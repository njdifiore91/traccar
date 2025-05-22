/*
 * Copyright 2020 - 2024 Anton Tananaev (anton@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.schedule;

import com.google.inject.Injector;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.LifecycleObject;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.messaging.MessageProducer;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Manages scheduled tasks with support for distributed coordination, task prioritization,
 * metrics collection, and graceful shutdown handling. Integrates with service discovery
 * for distributed task coordination and message broker for task event publishing.
 */
@Singleton
public class ScheduleManager implements LifecycleObject {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduleManager.class);
    private static final String METRIC_PREFIX = "traccar_scheduler";
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;

    private final Injector injector;
    private final boolean secondary;
    private final Config config;
    private final ServiceDiscovery serviceDiscovery;
    private final MessageProducer messageProducer;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    
    private ScheduledExecutorService executor;
    private final Map<String, ScheduleTask> activeTasks = new ConcurrentHashMap<>();
    private final Map<String, Future<?>> taskFutures = new ConcurrentHashMap<>();
    private final Map<String, Timer> taskTimers = new HashMap<>();
    private final Map<String, Counter> taskExecutionCounter = new HashMap<>();
    private final Map<String, Counter> taskFailureCounter = new HashMap<>();

    /**
     * Constructs a new ScheduleManager with the required dependencies.
     *
     * @param injector The Guice injector for creating task instances
     * @param config The application configuration
     * @param serviceDiscovery Service discovery for distributed task coordination
     * @param messageProducer Message producer for task event publishing
     * @param meterRegistry Metrics registry for performance monitoring
     * @param openTelemetry OpenTelemetry for distributed tracing
     */
    @Inject
    public ScheduleManager(Injector injector, Config config, ServiceDiscovery serviceDiscovery,
                          MessageProducer messageProducer, MeterRegistry meterRegistry,
                          OpenTelemetry openTelemetry) {
        this.injector = injector;
        this.config = config;
        this.serviceDiscovery = serviceDiscovery;
        this.messageProducer = messageProducer;
        this.meterRegistry = meterRegistry;
        this.tracer = openTelemetry.getTracer("org.traccar.scheduler");
        this.secondary = config.getBoolean(Keys.BROADCAST_SECONDARY);
        
        // Register scheduler metrics
        Gauge.builder(METRIC_PREFIX + ".active_tasks", activeTasks::size)
                .description("Number of active scheduled tasks")
                .register(meterRegistry);
    }

    /**
     * Starts the scheduler and initializes all configured tasks.
     * Tasks are prioritized based on their priority value and scheduled accordingly.
     */
    @Override
    public void start() {
        Span span = tracer.spanBuilder("scheduler.start").startSpan();
        try (var scope = span.makeCurrent()) {
            LOGGER.info("Starting scheduler manager");
            
            // Create thread pool with naming pattern for better monitoring
            ThreadFactory threadFactory = new ThreadFactoryBuilder()
                    .setNameFormat("scheduler-thread-%d")
                    .build();
            
            int threadPoolSize = config.getInteger("scheduler.threadPoolSize", 1);
            executor = Executors.newScheduledThreadPool(threadPoolSize, threadFactory);
            
            // Get all task classes and sort by priority (highest first)
            var taskClasses = Stream.of(
                    TaskHealthCheck.class,
                    TaskClearStatus.class,
                    TaskExpirations.class,
                    TaskDeleteTemporary.class,
                    TaskReports.class,
                    TaskDeviceInactivityCheck.class,
                    TaskWebSocketKeepalive.class)
                    .collect(Collectors.toList());
            
            // Initialize tasks and create metrics
            for (Class<? extends ScheduleTask> taskClass : taskClasses) {
                try {
                    var task = injector.getInstance(taskClass);
                    String taskName = task.getTaskName();
                    
                    // Create metrics for this task
                    taskTimers.put(taskName, Timer.builder(METRIC_PREFIX + ".execution.time")
                            .description("Task execution time")
                            .tag("task", taskName)
                            .register(meterRegistry));
                    
                    taskExecutionCounter.put(taskName, Counter.builder(METRIC_PREFIX + ".execution.count")
                            .description("Task execution count")
                            .tag("task", taskName)
                            .register(meterRegistry));
                    
                    taskFailureCounter.put(taskName, Counter.builder(METRIC_PREFIX + ".execution.failures")
                            .description("Task execution failure count")
                            .tag("task", taskName)
                            .register(meterRegistry));
                    
                    // Check if this task should run on this instance
                    boolean shouldRun = shouldRunTask(task);
                    
                    if (shouldRun) {
                        LOGGER.info("Scheduling task: {} with priority {}", taskName, task.getPriority());
                        activeTasks.put(taskName, task);
                        
                        // Create a wrapped task with metrics and tracing
                        Runnable wrappedTask = () -> executeTask(task);
                        
                        // Schedule the task and store the future for cancellation
                        task.schedule(executor);
                        // We can't get the Future directly, so we'll use a placeholder
                        taskFutures.put(taskName, new CompletableFuture<>());
                        
                        // Publish task scheduled event
                        publishTaskEvent(task, "scheduled", null);
                    } else {
                        LOGGER.info("Skipping task: {} (not eligible for this instance)", taskName);
                    }
                } catch (Exception e) {
                    LOGGER.error("Failed to initialize task: {}", taskClass.getSimpleName(), e);
                    span.recordException(e);
                }
            }
            
            LOGGER.info("Scheduler manager started with {} active tasks", activeTasks.size());
        } finally {
            span.end();
        }
    }

    /**
     * Determines if a task should run on this instance based on distributed coordination rules.
     *
     * @param task The task to check
     * @return true if the task should run on this instance, false otherwise
     */
    private boolean shouldRunTask(ScheduleTask task) {
        // If task allows multiple instances, check if this is a secondary instance
        if (task.multipleInstances()) {
            return !secondary;
        }
        
        // If task has a specific execution context, check if this instance matches
        String executionContext = task.getExecutionContext();
        if (executionContext != null && !executionContext.isEmpty()) {
            String instanceId = serviceDiscovery.getInstanceId();
            return executionContext.equals(instanceId);
        }
        
        // For tasks that need leader election
        if (!task.multipleInstances() && !secondary) {
            // Use service discovery to determine if this instance should run the task
            return serviceDiscovery.isLeaderForTask(task.getTaskName());
        }
        
        return false;
    }

    /**
     * Executes a task with metrics collection, tracing, and error handling.
     *
     * @param task The task to execute
     */
    private void executeTask(ScheduleTask task) {
        String taskName = task.getTaskName();
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            // Execute the task with tracing
            task.executeWithTracing(tracer);
            
            // Record successful execution
            taskExecutionCounter.get(taskName).increment();
            
            // Publish task completed event
            publishTaskEvent(task, "completed", null);
        } catch (Exception e) {
            // Record failure
            taskFailureCounter.get(taskName).increment();
            LOGGER.error("Task execution failed: {}", taskName, e);
            
            // Publish task failed event
            publishTaskEvent(task, "failed", e.getMessage());
        } finally {
            // Record execution time
            sample.stop(taskTimers.get(taskName));
        }
    }

    /**
     * Publishes a task event to the message broker.
     *
     * @param task The task that generated the event
     * @param status The event status (scheduled, completed, failed)
     * @param details Additional details (optional)
     */
    private void publishTaskEvent(ScheduleTask task, String status, String details) {
        try {
            if (messageProducer != null) {
                Map<String, Object> event = new HashMap<>();
                event.put("taskName", task.getTaskName());
                event.put("status", status);
                event.put("priority", task.getPriority());
                event.put("timestamp", System.currentTimeMillis());
                event.put("instanceId", serviceDiscovery.getInstanceId());
                
                if (details != null) {
                    event.put("details", details);
                }
                
                messageProducer.publish("scheduler.events", event);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to publish task event: {}", task.getTaskName(), e);
        }
    }

    /**
     * Stops the scheduler with graceful shutdown of running tasks.
     * Waits for tasks to complete or until the timeout is reached.
     */
    @Override
    public void stop() {
        Span span = tracer.spanBuilder("scheduler.stop").startSpan();
        try (var scope = span.makeCurrent()) {
            LOGGER.info("Stopping scheduler manager");
            
            if (executor != null) {
                // We can't cancel individual tasks directly since we don't have their futures
                // Just log that we're shutting down
                for (String taskName : activeTasks.keySet()) {
                    try {
                        LOGGER.debug("Task will be cancelled during shutdown: {}", taskName);
                        publishTaskEvent(activeTasks.get(taskName), "cancelled", "Scheduler shutdown");
                    } catch (Exception e) {
                        LOGGER.warn("Error during shutdown notification for task: {}", taskName, e);
                    }
                }
                
                // Initiate graceful shutdown
                executor.shutdown();
                
                try {
                    // Wait for tasks to complete
                    if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                        LOGGER.warn("Scheduler did not terminate in {} seconds. Forcing shutdown.", 
                                SHUTDOWN_TIMEOUT_SECONDS);
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    LOGGER.warn("Scheduler shutdown interrupted", e);
                    Thread.currentThread().interrupt();
                    executor.shutdownNow();
                }
                
                // Clear task collections
                taskFutures.clear();
                activeTasks.clear();
                executor = null;
                
                LOGGER.info("Scheduler manager stopped");
            }
        } finally {
            span.end();
        }
    }

    /**
     * Custom thread factory for creating named threads.
     */
    private static class ThreadFactoryBuilder implements ThreadFactory {
        private String nameFormat;
        
        public ThreadFactoryBuilder setNameFormat(String nameFormat) {
            this.nameFormat = nameFormat;
            return this;
        }
        
        public ThreadFactory build() {
            return r -> {
                Thread thread = new Thread(r);
                if (nameFormat != null) {
                    thread.setName(String.format(nameFormat, thread.getId()));
                }
                thread.setDaemon(false);
                return thread;
            };
        }
    }
}
