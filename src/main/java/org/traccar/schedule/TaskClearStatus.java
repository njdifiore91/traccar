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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.helper.model.DeviceUtil;
import org.traccar.messaging.MessageProducer;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Task to clear device status information.
 * Uses service discovery for leader election to ensure only one instance performs the operation.
 * Publishes status events to the message broker and collects metrics for monitoring.
 */
@Singleton
public class TaskClearStatus implements ScheduleTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskClearStatus.class);
    private static final String TASK_NAME = "status-clear";
    private static final long EXECUTION_INTERVAL = 24 * 60 * 60; // Run once per day

    private final ServiceDiscovery serviceDiscovery;
    private final Storage storage;
    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    
    private final Timer executionTimer;
    private final Counter successCounter;
    private final Counter failureCounter;

    /**
     * Constructs the task with required dependencies.
     *
     * @param serviceDiscovery Service discovery for leader election
     * @param storage Storage for device status operations
     * @param messageProducer Message broker producer for status events
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry for performance monitoring
     */
    @Inject
    public TaskClearStatus(
            ServiceDiscovery serviceDiscovery,
            Storage storage,
            MessageProducer messageProducer,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.serviceDiscovery = serviceDiscovery;
        this.storage = storage;
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.executionTimer = Timer.builder("traccar.task.status.clear.duration")
                .description("Time taken to clear device status")
                .register(meterRegistry);
        this.successCounter = Counter.builder("traccar.task.status.clear.success")
                .description("Number of successful status clear operations")
                .register(meterRegistry);
        this.failureCounter = Counter.builder("traccar.task.status.clear.failure")
                .description("Number of failed status clear operations")
                .register(meterRegistry);
        
        // Execute immediately on startup if this is the leader instance
        if (isLeader()) {
            LOGGER.info("Executing initial status clear operation as leader instance");
            executeStatusClear();
        }
    }

    /**
     * Schedules the task to run at fixed intervals.
     *
     * @param executor The executor service to schedule the task on
     */
    @Override
    public void schedule(ScheduledExecutorService executor) {
        executor.scheduleAtFixedRate(this, EXECUTION_INTERVAL, EXECUTION_INTERVAL, TimeUnit.SECONDS);
        LOGGER.info("Scheduled status clear task with interval {} seconds", EXECUTION_INTERVAL);
    }

    /**
     * Executes the task if this instance is the leader.
     */
    @Override
    public void run() {
        if (isLeader()) {
            LOGGER.debug("Executing scheduled status clear operation as leader instance");
            executeStatusClear();
        } else {
            LOGGER.trace("Skipping status clear operation as non-leader instance");
        }
    }

    /**
     * Determines if this instance is the leader using service discovery.
     *
     * @return true if this instance is the leader, false otherwise
     */
    private boolean isLeader() {
        try {
            return serviceDiscovery.isLeader(TASK_NAME);
        } catch (Exception e) {
            LOGGER.warn("Error determining leader status, assuming not leader", e);
            return false;
        }
    }

    /**
     * Executes the status clear operation with tracing and metrics.
     */
    private void executeStatusClear() {
        Span span = tracer.spanBuilder("status-clear-operation").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("task.name", TASK_NAME);
            
            executionTimer.record(() -> {
                try {
                    LOGGER.info("Clearing device status information");
                    DeviceUtil.resetStatus(storage);
                    
                    // Publish status clear event to message broker
                    messageProducer.publish("status-events", "status-cleared", null);
                    
                    successCounter.increment();
                    span.setAttribute("status.clear.success", true);
                    LOGGER.info("Successfully cleared device status information");
                } catch (StorageException e) {
                    failureCounter.increment();
                    span.setAttribute("status.clear.success", false);
                    span.recordException(e);
                    LOGGER.error("Failed to clear device status information", e);
                }
            });
        } finally {
            span.end();
        }
    }
}