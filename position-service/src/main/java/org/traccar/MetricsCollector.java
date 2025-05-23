/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.model.Position;

import java.util.concurrent.TimeUnit;

/**
 * Metrics collector for the Position Service.
 * 
 * This class collects and exposes metrics for monitoring the position service,
 * including position processing throughput, latency, and error rates. It uses
 * Micrometer to collect metrics and expose them to monitoring systems like
 * Prometheus.
 */
@Singleton
public class MetricsCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollector.class);

    private final MeterRegistry meterRegistry;
    private final Counter positionsReceivedCounter;
    private final Counter positionsProcessedCounter;
    private final Counter positionsErrorCounter;
    private final Timer positionProcessingTimer;

    /**
     * Constructs a new MetricsCollector with the necessary dependencies.
     *
     * @param config Configuration
     * @param meterRegistry Metrics registry
     */
    @Inject
    public MetricsCollector(Config config, MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Create counters for position processing metrics
        this.positionsReceivedCounter = meterRegistry.counter("position.received");
        this.positionsProcessedCounter = meterRegistry.counter("position.processed");
        this.positionsErrorCounter = meterRegistry.counter("position.error");
        
        // Create timer for position processing latency
        this.positionProcessingTimer = meterRegistry.timer("position.processing.time");
        
        LOGGER.info("Metrics collector initialized");
    }

    /**
     * Record that a position was received from the message broker.
     */
    public void recordPositionReceived() {
        positionsReceivedCounter.increment();
    }

    /**
     * Record that a position was successfully processed.
     */
    public void recordPositionProcessed() {
        positionsProcessedCounter.increment();
    }

    /**
     * Record that a position processing failed with an error.
     */
    public void recordPositionError() {
        positionsErrorCounter.increment();
    }

    /**
     * Record the time it took to process a position.
     *
     * @param startTimeNanos Start time in nanoseconds
     * @return Processing time in milliseconds
     */
    public long recordPositionProcessingTime(long startTimeNanos) {
        long processingTimeNanos = System.nanoTime() - startTimeNanos;
        positionProcessingTimer.record(processingTimeNanos, TimeUnit.NANOSECONDS);
        return TimeUnit.NANOSECONDS.toMillis(processingTimeNanos);
    }

    /**
     * Record metrics for a specific position.
     *
     * @param position Position to record metrics for
     */
    public void recordPositionMetrics(Position position) {
        // Record device-specific metrics
        meterRegistry.counter("position.device", "deviceId", String.valueOf(position.getDeviceId())).increment();
        
        // Record protocol-specific metrics if available
        if (position.getAttributes().containsKey(Position.KEY_PROTOCOL)) {
            String protocol = position.getAttributes().get(Position.KEY_PROTOCOL).toString();
            meterRegistry.counter("position.protocol", "protocol", protocol).increment();
        }
        
        // Record valid/invalid position metrics
        if (position.getValid()) {
            meterRegistry.counter("position.valid").increment();
        } else {
            meterRegistry.counter("position.invalid").increment();
        }
    }

    /**
     * Record handler-specific metrics.
     *
     * @param handlerName Handler name
     * @param success Whether the handler was successful
     * @param processingTimeNanos Processing time in nanoseconds
     */
    public void recordHandlerMetrics(String handlerName, boolean success, long processingTimeNanos) {
        // Record handler success/failure
        if (success) {
            meterRegistry.counter("handler.success", "handler", handlerName).increment();
        } else {
            meterRegistry.counter("handler.failure", "handler", handlerName).increment();
        }
        
        // Record handler processing time
        meterRegistry.timer("handler.processing.time", "handler", handlerName)
                .record(processingTimeNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Record outbox-related metrics.
     *
     * @param action Action (created, published, failed)
     * @param topic Topic
     */
    public void recordOutboxMetrics(String action, String topic) {
        meterRegistry.counter("outbox.message." + action, "topic", topic).increment();
    }

    /**
     * Record circuit breaker-related metrics.
     *
     * @param name Circuit breaker name
     * @param state Circuit breaker state
     */
    public void recordCircuitBreakerMetrics(String name, String state) {
        meterRegistry.counter("circuitbreaker.state", "name", name, "state", state).increment();
    }
}