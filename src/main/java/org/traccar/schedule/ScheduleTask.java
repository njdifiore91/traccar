/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Interface for scheduled tasks that can be executed in a distributed environment.
 * Supports distributed execution context, OpenTelemetry tracing, task prioritization,
 * and execution metrics.
 */
public interface ScheduleTask extends Runnable {

    /**
     * Determines if multiple instances of this task can run simultaneously across
     * the distributed environment. If false, the task will use distributed locking
     * to ensure only one instance runs at a time.
     *
     * @return true if multiple instances can run simultaneously, false otherwise
     */
    default boolean multipleInstances() {
        return true;
    }

    /**
     * Gets the priority of this task. Higher values indicate higher priority.
     * Default priority is 0 (normal).
     *
     * @return the task priority (default: 0)
     */
    default int getPriority() {
        return 0;
    }

    /**
     * Gets the task name for identification in logs, metrics, and traces.
     * By default, returns the simple class name.
     *
     * @return the task name
     */
    default String getTaskName() {
        return this.getClass().getSimpleName();
    }

    /**
     * Gets the execution context for this task. This can be used to determine
     * which service instance should execute the task in a distributed environment.
     *
     * @return the execution context identifier, or null if the task can run on any instance
     */
    default String getExecutionContext() {
        return null;
    }

    /**
     * Schedules this task for execution using the provided executor service.
     * Implementations should configure the appropriate scheduling pattern
     * (fixed rate, fixed delay, cron-based, etc.).
     *
     * @param executor the executor service to use for scheduling
     */
    void schedule(ScheduledExecutorService executor);

    /**
     * Executes the task with distributed tracing support.
     * This method wraps the run() method with OpenTelemetry tracing.
     *
     * @param tracer the OpenTelemetry tracer to use for creating spans
     */
    default void executeWithTracing(Tracer tracer) {
        Span span = tracer.spanBuilder("ScheduleTask." + getTaskName())
                .setAttribute("task.name", getTaskName())
                .setAttribute("task.priority", getPriority())
                .setAttribute("task.multipleInstances", multipleInstances())
                .startSpan();
        
        try {
            span.makeCurrent();
            run();
        } catch (Exception e) {
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Publishes task execution events to the message broker.
     * This method can be called by implementations to notify other services
     * about task execution status.
     *
     * @param status the execution status (e.g., "started", "completed", "failed")
     * @param details additional details about the execution (optional)
     */
    default void publishTaskEvent(String status, String details) {
        // This is a default no-op implementation.
        // Concrete implementations should override this method to publish events
        // to the message broker (Kafka or RabbitMQ) as needed.
    }
}