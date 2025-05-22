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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects and exposes metrics about scheduled task execution for monitoring and alerting.
 * Integrates with Prometheus to expose metrics such as task execution count, duration,
 * success rate, and last execution time.
 */
@Singleton
public class TaskMetrics {

    private final MeterRegistry registry;
    private final Map<String, AtomicLong> lastExecutionTimeMap;
    private final Map<String, AtomicLong> executionCountMap;
    private final Map<String, AtomicLong> successCountMap;
    private final Map<String, AtomicLong> failureCountMap;
    private final Map<String, Timer> executionTimerMap;

    /**
     * Constructs a new TaskMetrics instance with the provided MeterRegistry.
     *
     * @param registry the MeterRegistry to use for registering metrics
     */
    @Inject
    public TaskMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.lastExecutionTimeMap = new ConcurrentHashMap<>();
        this.executionCountMap = new ConcurrentHashMap<>();
        this.successCountMap = new ConcurrentHashMap<>();
        this.failureCountMap = new ConcurrentHashMap<>();
        this.executionTimerMap = new ConcurrentHashMap<>();
    }

    /**
     * Records the start of a task execution and returns a timer context that should be closed
     * when the task completes.
     *
     * @param task the task being executed
     * @return a timer sample that should be stopped when the task completes
     */
    public Timer.Sample recordTaskStart(ScheduleTask task) {
        String taskName = task.getTaskName();
        Tags tags = getTags(task);

        // Initialize metrics for this task if they don't exist
        initializeMetrics(taskName, tags);

        // Increment execution count
        executionCountMap.get(taskName).incrementAndGet();
        
        // Update last execution time
        lastExecutionTimeMap.get(taskName).set(System.currentTimeMillis());

        // Start timing the execution
        return Timer.start(registry);
    }

    /**
     * Records the successful completion of a task execution.
     *
     * @param task the task that completed successfully
     * @param sample the timer sample returned from recordTaskStart
     */
    public void recordTaskSuccess(ScheduleTask task, Timer.Sample sample) {
        String taskName = task.getTaskName();
        
        // Increment success count
        successCountMap.get(taskName).incrementAndGet();
        
        // Stop the timer and record the duration
        sample.stop(executionTimerMap.get(taskName));
    }

    /**
     * Records the failure of a task execution.
     *
     * @param task the task that failed
     * @param sample the timer sample returned from recordTaskStart
     * @param exception the exception that caused the failure
     */
    public void recordTaskFailure(ScheduleTask task, Timer.Sample sample, Throwable exception) {
        String taskName = task.getTaskName();
        
        // Increment failure count
        failureCountMap.get(taskName).incrementAndGet();
        
        // Stop the timer and record the duration
        sample.stop(executionTimerMap.get(taskName));
    }

    /**
     * Initializes metrics for a task if they don't already exist.
     *
     * @param taskName the name of the task
     * @param tags the tags to apply to the metrics
     */
    private void initializeMetrics(String taskName, Tags tags) {
        // Initialize last execution time gauge
        lastExecutionTimeMap.computeIfAbsent(taskName, k -> {
            AtomicLong value = new AtomicLong(0);
            Gauge.builder("task.last.execution.time", value, AtomicLong::get)
                    .tags(tags)
                    .description("Timestamp of the last execution of the task")
                    .register(registry);
            return value;
        });

        // Initialize execution count counter
        executionCountMap.computeIfAbsent(taskName, k -> {
            AtomicLong value = new AtomicLong(0);
            Counter.builder("task.execution.count")
                    .tags(tags)
                    .description("Total number of task executions")
                    .register(registry);
            return value;
        });

        // Initialize success count counter
        successCountMap.computeIfAbsent(taskName, k -> {
            AtomicLong value = new AtomicLong(0);
            Counter.builder("task.execution.success")
                    .tags(tags)
                    .description("Number of successful task executions")
                    .register(registry);
            return value;
        });

        // Initialize failure count counter
        failureCountMap.computeIfAbsent(taskName, k -> {
            AtomicLong value = new AtomicLong(0);
            Counter.builder("task.execution.failure")
                    .tags(tags)
                    .description("Number of failed task executions")
                    .register(registry);
            return value;
        });

        // Initialize execution timer
        executionTimerMap.computeIfAbsent(taskName, k -> {
            return Timer.builder("task.execution.duration")
                    .tags(tags)
                    .description("Duration of task execution")
                    .publishPercentiles(0.5, 0.95, 0.99) // Publish 50th, 95th, and 99th percentiles
                    .serviceLevelObjectives(
                            Duration.ofMillis(100),
                            Duration.ofMillis(500),
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(5))
                    .register(registry);
        });
    }

    /**
     * Creates tags for a task based on its properties.
     *
     * @param task the task to create tags for
     * @return the tags for the task
     */
    private Tags getTags(ScheduleTask task) {
        return Tags.of(
                Tag.of("task", task.getTaskName()),
                Tag.of("priority", String.valueOf(task.getPriority())),
                Tag.of("multipleInstances", String.valueOf(task.multipleInstances())),
                Tag.of("executionContext", task.getExecutionContext() != null ? task.getExecutionContext() : "default")
        );
    }

    /**
     * Registers a gauge that tracks the time since the last execution of a task.
     *
     * @param task the task to track
     */
    public void registerTimeSinceLastExecutionGauge(ScheduleTask task) {
        String taskName = task.getTaskName();
        Tags tags = getTags(task);

        // Initialize last execution time if not already initialized
        initializeMetrics(taskName, tags);

        // Register a gauge that calculates the time since the last execution
        Gauge.builder("task.time.since.last.execution", () -> {
            long lastExecutionTime = lastExecutionTimeMap.get(taskName).get();
            if (lastExecutionTime == 0) {
                return 0.0; // Task has never been executed
            }
            return (System.currentTimeMillis() - lastExecutionTime) / 1000.0; // Convert to seconds
        })
                .tags(tags)
                .description("Time in seconds since the last execution of the task")
                .register(registry);
    }

    /**
     * Registers a gauge that tracks the success rate of a task.
     *
     * @param task the task to track
     */
    public void registerSuccessRateGauge(ScheduleTask task) {
        String taskName = task.getTaskName();
        Tags tags = getTags(task);

        // Initialize counters if not already initialized
        initializeMetrics(taskName, tags);

        // Register a gauge that calculates the success rate
        Gauge.builder("task.success.rate", () -> {
            long totalCount = executionCountMap.get(taskName).get();
            if (totalCount == 0) {
                return 1.0; // No executions yet, assume 100% success rate
            }
            long successCount = successCountMap.get(taskName).get();
            return (double) successCount / totalCount;
        })
                .tags(tags)
                .description("Success rate of the task (0.0 to 1.0)")
                .register(registry);
    }

    /**
     * Creates a wrapper around a ScheduleTask that automatically records metrics.
     *
     * @param task the task to wrap
     * @return a wrapped task that records metrics
     */
    public ScheduleTask withMetrics(ScheduleTask task) {
        return new ScheduleTask() {
            @Override
            public void schedule(java.util.concurrent.ScheduledExecutorService executor) {
                task.schedule(executor);
            }

            @Override
            public boolean multipleInstances() {
                return task.multipleInstances();
            }

            @Override
            public int getPriority() {
                return task.getPriority();
            }

            @Override
            public String getTaskName() {
                return task.getTaskName();
            }

            @Override
            public String getExecutionContext() {
                return task.getExecutionContext();
            }

            @Override
            public void run() {
                Timer.Sample sample = recordTaskStart(task);
                try {
                    task.run();
                    recordTaskSuccess(task, sample);
                } catch (Throwable e) {
                    recordTaskFailure(task, sample, e);
                    throw e;
                }
            }
        };
    }
}