/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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

import com.google.inject.Inject;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.messaging.MessageBroker;
import org.traccar.messaging.MessageBroker.Topic;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Abstract base class for distributed scheduled tasks that need to be coordinated
 * across multiple service instances in a microservices environment.
 * <p>
 * This class provides:
 * - Service discovery integration for leader election
 * - Distributed tracing with OpenTelemetry
 * - Message broker integration for task event publishing
 * - Distributed locking mechanism for task coordination
 * - Metrics collection for monitoring
 */
public abstract class DistributedScheduleTask implements ScheduleTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedScheduleTask.class);

    private final ServiceDiscovery serviceDiscovery;
    private final MessageBroker messageBroker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final String taskName;
    private final String serviceId;

    private Timer executionTimer;
    private Timer lockAcquisitionTimer;

    /**
     * Creates a new distributed schedule task.
     *
     * @param serviceDiscovery Service discovery client for leader election
     * @param messageBroker Message broker for publishing task events
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry for monitoring
     */
    @Inject
    public DistributedScheduleTask(
            ServiceDiscovery serviceDiscovery,
            MessageBroker messageBroker,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.serviceDiscovery = serviceDiscovery;
        this.messageBroker = messageBroker;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.taskName = getClass().getSimpleName();
        this.serviceId = UUID.randomUUID().toString();
        initializeMetrics();
    }

    private void initializeMetrics() {
        executionTimer = Timer.builder("task.execution")
                .description("Time taken to execute the scheduled task")
                .tag("task", taskName)
                .register(meterRegistry);

        lockAcquisitionTimer = Timer.builder("task.lock.acquisition")
                .description("Time taken to acquire the distributed lock")
                .tag("task", taskName)
                .register(meterRegistry);

        // Register additional metrics
        meterRegistry.gauge("task.last.execution", Map.of("task", taskName), this, task -> 
                task.getLastExecutionTime() != null ? task.getLastExecutionTime().getEpochSecond() : 0);
    }

    /**
     * Gets the distributed lock for this task.
     * Implementations should provide a lock mechanism that works across service instances.
     *
     * @return A distributed lock instance
     */
    protected abstract Lock getDistributedLock();

    /**
     * Gets the lock timeout duration.
     * This is the maximum time to wait for acquiring the lock.
     *
     * @return Lock timeout duration
     */
    protected Duration getLockTimeout() {
        return Duration.ofSeconds(5);
    }

    /**
     * Gets the task execution timeout.
     * This is used for tracing and monitoring to detect long-running tasks.
     *
     * @return Task execution timeout duration
     */
    protected Duration getTaskTimeout() {
        return Duration.ofMinutes(5);
    }

    /**
     * Gets the last execution time of this task.
     * This is used for metrics and can be overridden by implementations to provide
     * persistent tracking of execution times.
     *
     * @return The last execution time or null if never executed
     */
    protected Instant getLastExecutionTime() {
        return null;
    }

    /**
     * Executes the task logic.
     * This method should be implemented by concrete task classes.
     *
     * @param traceContext The OpenTelemetry trace context for distributed tracing
     */
    protected abstract void executeTask(Context traceContext);

    /**
     * Determines if this service instance should execute the task.
     * Uses service discovery to implement leader election.
     *
     * @return true if this instance should execute the task, false otherwise
     */
    protected boolean shouldExecuteTask() {
        try {
            return serviceDiscovery.isLeader(taskName, serviceId);
        } catch (Exception e) {
            LOGGER.warn("Error determining task leadership, assuming execution responsibility", e);
            // Increment a metric counter for service discovery failures
            meterRegistry.counter("task.discovery.failures", "task", taskName).increment();
            return true; // Default to executing in case of service discovery failure
        }
    }

    /**
     * Publishes a task event to the message broker.
     *
     * @param eventType The type of event (started, completed, failed)
     * @param details Additional event details
     * @param traceContext The OpenTelemetry trace context for correlation
     */
    protected void publishTaskEvent(String eventType, Map<String, Object> details, Context traceContext) {
        try {
            Map<String, Object> event = Map.of(
                    "taskName", taskName,
                    "serviceId", serviceId,
                    "eventType", eventType,
                    "timestamp", Instant.now().toString(),
                    "details", details != null ? details : Map.of()
            );

            messageBroker.publish(Topic.TASK_EVENTS, event, traceContext);
        } catch (Exception e) {
            LOGGER.warn("Failed to publish task event", e);
        }
    }

    @Override
    public void run() {
        if (!shouldExecuteTask()) {
            LOGGER.debug("Skipping task execution as this instance is not the leader: {}", taskName);
            return;
        }

        // Create a new span for this task execution
        Span span = tracer.spanBuilder(taskName)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("task.type", "scheduled")
                .setAttribute("task.name", taskName)
                .setAttribute("service.id", serviceId)
                .startSpan();

        // Activate the span and get the context
        try (Scope scope = span.makeCurrent()) {
            Context traceContext = Context.current();

            // Publish task started event
            publishTaskEvent("started", null, traceContext);

            // Try to acquire the distributed lock
            Lock lock = getDistributedLock();
            boolean lockAcquired = false;

            try {
                // Record lock acquisition time
                lockAcquired = lockAcquisitionTimer.record(() -> {
                    try {
                        return lock.tryLock(getLockTimeout().toMillis(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                });

                if (!lockAcquired) {
                    LOGGER.warn("Failed to acquire lock for task: {}", taskName);
                    span.setStatus(StatusCode.ERROR, "Failed to acquire lock");
                    publishTaskEvent("failed", Map.of("reason", "lock_acquisition_timeout"), traceContext);
                    return;
                }

                // Record task execution time
                executionTimer.record(() -> {
                    try {
                        executeTask(traceContext);
                        span.setStatus(StatusCode.OK);
                        publishTaskEvent("completed", null, traceContext);
                    } catch (Exception e) {
                        LOGGER.error("Error executing task: {}", taskName, e);
                        span.recordException(e);
                        span.setStatus(StatusCode.ERROR, e.getMessage());
                        publishTaskEvent("failed", Map.of("reason", "execution_error", "error", e.getMessage()), traceContext);
                    }
                });
            } finally {
                if (lockAcquired) {
                    lock.unlock();
                }
            }
        } finally {
            span.end();
        }
    }

    @Override
    public boolean multipleInstances() {
        // This task can run on multiple instances, but coordination ensures
        // it only executes on one instance at a time
        return true;
    }
    
    /**
     * Schedules the task for execution.
     * Implementations should override this method to define the execution schedule.
     *
     * @param executor The executor service to schedule the task on
     */
    @Override
    public abstract void schedule(ScheduledExecutorService executor);
}