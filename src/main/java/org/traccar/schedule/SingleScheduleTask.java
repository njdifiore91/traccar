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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.traccar.discovery.ServiceDiscoveryScheduler;
import org.traccar.messaging.MessageProducer;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * Abstract base class for scheduled tasks that should only run on a single instance
 * in a distributed environment. Uses service discovery for leader election and
 * distributed locking for task coordination.
 */
public abstract class SingleScheduleTask implements ScheduleTask {

    private final ServiceDiscoveryScheduler serviceDiscoveryScheduler;
    private final MessageProducer messageProducer;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;

    /**
     * Constructor for SingleScheduleTask.
     * 
     * @param serviceDiscoveryScheduler The scheduler that handles leader election
     * @param messageProducer The message producer for publishing task events
     * @param meterRegistry The meter registry for metrics collection
     * @param tracer The OpenTelemetry tracer for distributed tracing
     */
    protected SingleScheduleTask(
            ServiceDiscoveryScheduler serviceDiscoveryScheduler,
            MessageProducer messageProducer,
            MeterRegistry meterRegistry,
            Tracer tracer) {
        this.serviceDiscoveryScheduler = serviceDiscoveryScheduler;
        this.messageProducer = messageProducer;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
    }

    @Override
    public boolean multipleInstances() {
        return false;
    }

    @Override
    public void run() {
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder(getClass().getSimpleName() + ".run")
                .setParent(Context.current())
                .setAttribute("task.type", "scheduled")
                .setAttribute("task.name", getClass().getSimpleName())
                .startSpan();

        try {
            // Check if this instance is the leader
            if (serviceDiscoveryScheduler.isLeader(getClass().getSimpleName())) {
                // Publish task start event
                publishTaskEvent("start", Map.of("status", "started"));

                // Get distributed lock to ensure exclusive execution
                Lock lock = serviceDiscoveryScheduler.getLock(getClass().getSimpleName());
                boolean lockAcquired = lock.tryLock();

                if (lockAcquired) {
                    try {
                        // Record execution time with Micrometer
                        Timer.Sample sample = Timer.start(meterRegistry);
                        Instant startTime = Instant.now();

                        // Execute the actual task logic
                        executeTask();

                        // Record metrics
                        Duration executionTime = Duration.between(startTime, Instant.now());
                        sample.stop(Timer.builder("task.execution")
                                .tag("task.name", getClass().getSimpleName())
                                .tag("task.status", "success")
                                .register(meterRegistry));

                        // Publish task completion event
                        publishTaskEvent("complete", Map.of(
                                "status", "completed",
                                "executionTimeMs", executionTime.toMillis()));
                    } catch (Exception e) {
                        // Record failure metrics
                        meterRegistry.counter("task.errors", 
                                "task.name", getClass().getSimpleName(),
                                "error.type", e.getClass().getSimpleName())
                                .increment();

                        // Publish task error event
                        publishTaskEvent("error", Map.of(
                                "status", "failed",
                                "error.message", e.getMessage(),
                                "error.type", e.getClass().getSimpleName()));

                        // Re-throw the exception to be handled by the scheduler
                        throw e;
                    } finally {
                        // Always release the lock
                        lock.unlock();
                    }
                } else {
                    // Another instance is already executing this task
                    meterRegistry.counter("task.skipped", 
                            "task.name", getClass().getSimpleName(),
                            "reason", "lock-not-acquired")
                            .increment();

                    publishTaskEvent("skipped", Map.of(
                            "status", "skipped",
                            "reason", "lock-not-acquired"));
                }
            } else {
                // This instance is not the leader
                meterRegistry.counter("task.skipped", 
                        "task.name", getClass().getSimpleName(),
                        "reason", "not-leader")
                        .increment();

                publishTaskEvent("skipped", Map.of(
                        "status", "skipped",
                        "reason", "not-leader"));
            }
        } finally {
            // End the tracing span
            span.end();
        }
    }

    /**
     * Execute the actual task logic. This method should be implemented by subclasses.
     * 
     * @throws Exception if task execution fails
     */
    protected abstract void executeTask() throws Exception;

    /**
     * Publish a task event to the message broker.
     * 
     * @param eventType the type of event (start, complete, error, skipped)
     * @param attributes additional attributes for the event
     */
    private void publishTaskEvent(String eventType, Map<String, Object> attributes) {
        try {
            Map<String, Object> eventData = Map.of(
                    "taskName", getClass().getSimpleName(),
                    "eventType", eventType,
                    "timestamp", Instant.now().toString(),
                    "attributes", attributes);
            
            messageProducer.publish("task-events", eventData);
        } catch (Exception e) {
            // Log but don't fail the task if event publishing fails
            meterRegistry.counter("task.event.publish.errors",
                    "task.name", getClass().getSimpleName(),
                    "event.type", eventType)
                    .increment();
        }
    }
}