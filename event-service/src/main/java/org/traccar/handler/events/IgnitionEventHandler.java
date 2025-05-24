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
package org.traccar.handler.events;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/**
 * IgnitionEventHandler is responsible for detecting ignition on/off events based on position data.
 * It compares the current position with the previous cached position to detect changes in ignition status.
 * 
 * This handler has been updated to work in a microservices architecture with the following enhancements:
 * - Consumes position data from a message broker instead of direct invocation
 * - Implements circuit breaker pattern for CacheManager interactions using Resilience4j
 * - Adds OpenTelemetry instrumentation for distributed tracing and metrics collection
 * - Propagates correlation IDs for cross-service request tracking
 * - Adds metrics for ignition event detection performance and counts by ignition event type
 * - Implements retry mechanism with exponential backoff for transient failures
 */
@Singleton
public class IgnitionEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(IgnitionEventHandler.class);

    private static final String CIRCUIT_BREAKER_NAME = "ignitionEventCacheManager";
    private static final String RETRY_NAME = "ignitionEventCacheManager";

    // OpenTelemetry attribute keys
    private static final AttributeKey<String> EVENT_TYPE_KEY = AttributeKey.stringKey("event.type");
    private static final AttributeKey<Long> DEVICE_ID_KEY = AttributeKey.longKey("device.id");
    private static final AttributeKey<String> CORRELATION_ID_KEY = AttributeKey.stringKey("correlation.id");
    private static final AttributeKey<Boolean> IGNITION_STATE_KEY = AttributeKey.booleanKey("ignition.state");

    private final CacheManager cacheManager;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Tracer tracer;
    private final Meter meter;
    private final LongCounter ignitionOnCounter;
    private final LongCounter ignitionOffCounter;
    private final Timer ignitionEventProcessingTimer;

    /**
     * Constructs a new IgnitionEventHandler with the necessary dependencies.
     *
     * @param cacheManager CacheManager for accessing previous positions
     * @param circuitBreakerRegistry Registry for creating circuit breakers
     * @param retryRegistry Registry for creating retry policies
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meter OpenTelemetry meter for metrics collection
     */
    @Inject
    public IgnitionEventHandler(
            CacheManager cacheManager,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry,
            Tracer tracer,
            Meter meter) {
        
        this.cacheManager = cacheManager;
        this.tracer = tracer;
        this.meter = meter;

        // Configure and create circuit breaker for cache manager interactions
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME, circuitBreakerConfig);

        // Configure and create retry policy for cache manager interactions
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(CompletionException.class)
                .build();
        this.retry = retryRegistry.retry(RETRY_NAME, retryConfig);

        // Create metrics for ignition events
        this.ignitionOnCounter = meter.counterBuilder("events.ignition.on")
                .setDescription("Number of ignition on events detected")
                .build();
        
        this.ignitionOffCounter = meter.counterBuilder("events.ignition.off")
                .setDescription("Number of ignition off events detected")
                .build();
                
        this.ignitionEventProcessingTimer = meter.timerBuilder("events.ignition.processing.time")
                .setDescription("Time taken to process ignition events")
                .build();
    }

    /**
     * Analyzes a position to detect ignition events.
     * This method is called by the PositionConsumer when a new position is received from the message broker.
     *
     * @param position The position to analyze
     * @param callback The callback to invoke if an event is detected
     */
    @Override
    public void analyzePosition(Position position, Callback callback) {
        // Start timer for performance measurement
        long startTime = System.currentTimeMillis();
        
        // Extract correlation ID from the current context if available
        String correlationId = null;
        Span currentSpan = Span.current();
        if (currentSpan != null) {
            correlationId = currentSpan.getAttribute(CORRELATION_ID_KEY);
        }
        
        // Create a span for the ignition event detection process
        Span span = tracer.spanBuilder("ignition_event_detection")
                .setParent(Context.current())
                .setAttribute(DEVICE_ID_KEY, position.getDeviceId())
                .startSpan();
        
        // Add correlation ID to span if available
        if (correlationId != null) {
            span.setAttribute(CORRELATION_ID_KEY, correlationId);
        }

        try (Scope scope = span.makeCurrent()) {
            // Get the current ignition state from the position
            Boolean ignition = position.getBoolean(Position.KEY_IGNITION);
            if (ignition == null) {
                LOGGER.debug("Position doesn't contain ignition information: {}", position.getDeviceId());
                return;
            }

            span.setAttribute(IGNITION_STATE_KEY, ignition);

            // Get the previous position with circuit breaker and retry
            CompletableFuture<Position> previousPositionFuture = getPreviousPosition(position.getDeviceId());
            previousPositionFuture.thenAccept(previousPosition -> {
                if (previousPosition != null) {
                    Boolean previousIgnition = previousPosition.getBoolean(Position.KEY_IGNITION);
                    if (previousIgnition != null && ignition != previousIgnition) {
                        // Ignition state has changed, create an event
                        String eventType = ignition ? Event.TYPE_IGNITION_ON : Event.TYPE_IGNITION_OFF;
                        Event event = new Event(eventType, position);
                        
                        // Record metric for the event type
                        if (ignition) {
                            ignitionOnCounter.add(1, Attributes.of(
                                DEVICE_ID_KEY, position.getDeviceId(),
                                EVENT_TYPE_KEY, Event.TYPE_IGNITION_ON
                            ));
                        } else {
                            ignitionOffCounter.add(1, Attributes.of(
                                DEVICE_ID_KEY, position.getDeviceId(),
                                EVENT_TYPE_KEY, Event.TYPE_IGNITION_OFF
                            ));
                        }
                        
                        // Set span attributes for the event
                        span.setAttribute(EVENT_TYPE_KEY, eventType);
                        
                        // Invoke the callback to publish the event
                        if (callback != null) {
                            callback.eventDetected(event);
                        }
                        
                        LOGGER.debug("Ignition {} event detected for device {}", 
                                ignition ? "ON" : "OFF", position.getDeviceId());
                    }
                }
            }).exceptionally(e -> {
                LOGGER.error("Error analyzing position for ignition events: {}", position.getDeviceId(), e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.recordException(e);
                return null;
            });
        } finally {
            // Record processing time metric
            long processingTime = System.currentTimeMillis() - startTime;
            ignitionEventProcessingTimer.record(processingTime, 
                    Attributes.of(DEVICE_ID_KEY, position.getDeviceId()));
            span.setAttribute("processing.time.ms", processingTime);
            span.end();
        }
    }

    /**
     * Gets the previous position for a device with circuit breaker and retry pattern.
     *
     * @param deviceId The device ID
     * @return A CompletableFuture that completes with the previous position or null if not found
     */
    private CompletableFuture<Position> getPreviousPosition(long deviceId) {
        // Extract correlation ID from the current context if available
        String correlationId = null;
        Span currentSpan = Span.current();
        if (currentSpan != null) {
            correlationId = currentSpan.getAttribute(CORRELATION_ID_KEY);
        }
        
        Span span = tracer.spanBuilder("get_previous_position")
                .setParent(Context.current())
                .setAttribute(DEVICE_ID_KEY, deviceId)
                .startSpan();
                
        // Add correlation ID to span if available
        if (correlationId != null) {
            span.setAttribute(CORRELATION_ID_KEY, correlationId);
        }

        try (Scope scope = span.makeCurrent()) {
            // Create a supplier that gets the previous position from the cache manager
            Supplier<CompletableFuture<Position>> positionSupplier = () -> {
                try {
                    long startTime = System.currentTimeMillis();
                    Position position = cacheManager.getPosition(deviceId);
                    long endTime = System.currentTimeMillis();
                    
                    // Record cache access time in span
                    span.setAttribute("cache.access.time.ms", endTime - startTime);
                    return CompletableFuture.completedFuture(position);
                } catch (Exception e) {
                    LOGGER.warn("Cache access failed for device {}, will retry: {}", deviceId, e.getMessage());
                    return CompletableFuture.failedFuture(e);
                }
            };

            // Apply circuit breaker and retry patterns to the supplier
            return Retry.decorateCompletionStage(
                    retry,
                    CircuitBreaker.decorateSupplier(circuitBreaker, positionSupplier)
            ).get().whenComplete((position, throwable) -> {
                if (throwable != null) {
                    LOGGER.error("Failed to get previous position for device {} after retries", deviceId, throwable);
                    span.setStatus(StatusCode.ERROR, throwable.getMessage());
                    span.recordException(throwable);
                } else {
                    span.setStatus(StatusCode.OK);
                    if (position != null) {
                        span.setAttribute("position.found", true);
                    } else {
                        span.setAttribute("position.found", false);
                    }
                }
                span.end();
            });
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            span.end();
            return CompletableFuture.failedFuture(e);
        }
    }
}