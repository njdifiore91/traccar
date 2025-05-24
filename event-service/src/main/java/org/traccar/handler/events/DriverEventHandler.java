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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Handler for driver change events.
 * 
 * This class detects when a driver ID changes between positions and generates
 * appropriate events. It includes circuit breaker pattern for resilience, OpenTelemetry
 * instrumentation for observability, and retry mechanisms for transient failures.
 */
public class DriverEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DriverEventHandler.class);
    
    private static final String CIRCUIT_BREAKER_NAME = "driverEventHandler";
    private static final String RETRY_NAME = "driverEventHandler";
    private static final String CORRELATION_ID_KEY = "correlationId";
    private static final String DRIVER_ID_ATTRIBUTE = "driverId";
    
    private final CacheManager cacheManager;
    private final Storage storage;
    private final CircuitBreaker cacheCircuitBreaker;
    private final Retry cacheRetry;
    private final Tracer tracer;
    
    // Metrics
    private final Timer processingTimer;
    private final Counter driverChangeEventCounter;
    private final Counter failedDetectionCounter;
    
    /**
     * Constructs the DriverEventHandler with necessary dependencies.
     *
     * @param cacheManager Cache manager for position data access
     * @param storage Storage for persistence operations
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Registry for metrics collection
     */
    @Inject
    public DriverEventHandler(
            CacheManager cacheManager,
            Storage storage,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        
        this.cacheManager = cacheManager;
        this.storage = storage;
        this.tracer = tracer;
        
        // Initialize circuit breaker
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(
                CircuitBreakerConfig.custom()
                        .failureRateThreshold(50)
                        .waitDurationInOpenState(Duration.ofSeconds(10))
                        .permittedNumberOfCallsInHalfOpenState(5)
                        .slidingWindowSize(10)
                        .recordExceptions(StorageException.class, TimeoutException.class)
                        .build());
        
        this.cacheCircuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME + "Cache");
        
        // Initialize retry mechanism
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(StorageException.class, TimeoutException.class)
                .enableExponentialBackoff(true)
                .exponentialBackoffMultiplier(2)
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.cacheRetry = retryRegistry.retry(RETRY_NAME + "Cache");
        
        // Initialize metrics
        this.processingTimer = Timer.builder("driver.change.processing.time")
                .description("Time taken to process position for driver change detection")
                .register(meterRegistry);
        
        this.driverChangeEventCounter = Counter.builder("driver.change.events.total")
                .description("Total number of driver change events detected")
                .register(meterRegistry);
        
        this.failedDetectionCounter = Counter.builder("driver.change.detection.failures")
                .description("Number of failed driver change detection attempts")
                .register(meterRegistry);
    }

    /**
     * Analyzes a position to detect driver change events.
     * 
     * This method is instrumented with OpenTelemetry for distributed tracing and
     * uses circuit breakers and retry mechanisms for resilience.
     *
     * @param position the position to analyze
     * @param correlationId the correlation ID for cross-service tracking
     * @return CompletableFuture with the event if detected, or null if no event
     */
    public CompletableFuture<Event> analyzePosition(Position position, String correlationId) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("driver.change.detect")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(CORRELATION_ID_KEY, correlationId)
                .setAttribute("deviceId", position.getDeviceId())
                .startSpan();
        
        // Use the timer to measure processing time
        return processingTimer.record(() -> {
            try (Scope scope = span.makeCurrent()) {
                // Add position attributes to the span for context
                if (position.hasAttribute(DRIVER_ID_ATTRIBUTE)) {
                    span.setAttribute("position.driverId", position.getString(DRIVER_ID_ATTRIBUTE));
                }
                span.setAttribute("position.time", position.getFixTime().getTime());
                
                return detectDriverChange(position, span, correlationId);
            } catch (Exception e) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                failedDetectionCounter.increment();
                LOGGER.error("Error detecting driver change for device {}: {}", 
                        position.getDeviceId(), e.getMessage(), e);
                return CompletableFuture.completedFuture(null);
            } finally {
                span.end();
            }
        });
    }

    /**
     * Core logic for driver change detection with resilience patterns applied.
     *
     * @param position the position to analyze
     * @param span the current tracing span
     * @param correlationId the correlation ID for cross-service tracking
     * @return CompletableFuture with the event if detected, or null if no event
     */
    private CompletableFuture<Event> detectDriverChange(Position position, Span span, String correlationId) {
        // Skip if position doesn't have driver ID attribute
        if (!position.hasAttribute(DRIVER_ID_ATTRIBUTE)) {
            span.addEvent("No driver ID attribute");
            return CompletableFuture.completedFuture(null);
        }
        
        String driverId = position.getString(DRIVER_ID_ATTRIBUTE);
        if (driverId == null || driverId.isEmpty()) {
            span.addEvent("Empty driver ID");
            return CompletableFuture.completedFuture(null);
        }
        
        // Get previous position with circuit breaker and retry for resilience
        Supplier<Position> previousPositionSupplier = () -> {
            Span positionSpan = tracer.spanBuilder("get.previous.position")
                    .setParent(Context.current())
                    .setAttribute(CORRELATION_ID_KEY, correlationId)
                    .startSpan();
            
            try {
                positionSpan.setAttribute("deviceId", position.getDeviceId());
                return cacheManager.getLastPosition(position.getDeviceId());
            } catch (Exception e) {
                positionSpan.recordException(e);
                positionSpan.setStatus(StatusCode.ERROR, e.getMessage());
                throw e;
            } finally {
                positionSpan.end();
            }
        };
        
        // Apply circuit breaker and retry patterns
        Position lastPosition;
        try {
            lastPosition = Retry.decorateSupplier(cacheRetry, 
                    CircuitBreaker.decorateSupplier(cacheCircuitBreaker, previousPositionSupplier))
                    .get();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Failed to get previous position: " + e.getMessage());
            failedDetectionCounter.increment();
            LOGGER.warn("Failed to get previous position for driver change detection: {}", e.getMessage());
            return CompletableFuture.completedFuture(null);
        }
        
        // If no previous position, we can't detect a change
        if (lastPosition == null) {
            span.addEvent("No previous position found");
            return CompletableFuture.completedFuture(null);
        }
        
        // Check if previous position had a driver ID
        if (!lastPosition.hasAttribute(DRIVER_ID_ATTRIBUTE)) {
            // First time we're seeing a driver ID
            span.setAttribute("driver.change.detected", true);
            span.setAttribute("driver.change.from", "none");
            span.setAttribute("driver.change.to", driverId);
            
            return createDriverChangeEvent(position, null, driverId, span, correlationId);
        }
        
        String previousDriverId = lastPosition.getString(DRIVER_ID_ATTRIBUTE);
        
        // Check if driver ID has changed
        if (!driverId.equals(previousDriverId)) {
            span.setAttribute("driver.change.detected", true);
            span.setAttribute("driver.change.from", previousDriverId);
            span.setAttribute("driver.change.to", driverId);
            
            return createDriverChangeEvent(position, previousDriverId, driverId, span, correlationId);
        } else {
            span.setAttribute("driver.change.detected", false);
            return CompletableFuture.completedFuture(null);
        }
    }
    
    /**
     * Creates and persists a driver change event.
     *
     * @param position the current position
     * @param oldDriverId the previous driver ID (may be null)
     * @param newDriverId the new driver ID
     * @param span the current tracing span
     * @param correlationId the correlation ID for cross-service tracking
     * @return CompletableFuture with the created event
     */
    private CompletableFuture<Event> createDriverChangeEvent(
            Position position, String oldDriverId, String newDriverId, Span span, String correlationId) {
        
        // Create driver change event
        Event event = new Event(Event.TYPE_DRIVER_CHANGED, position.getDeviceId(), position.getId());
        if (oldDriverId != null) {
            event.set("oldDriverId", oldDriverId);
        }
        event.set("driverId", newDriverId);
        event.set(Event.KEY_CORRELATION_ID, correlationId);
        
        // Save event with circuit breaker and retry
        Supplier<Event> eventSaveSupplier = () -> {
            Span saveSpan = tracer.spanBuilder("save.driver.change.event")
                    .setParent(Context.current())
                    .setAttribute(CORRELATION_ID_KEY, correlationId)
                    .startSpan();
            
            try {
                saveSpan.setAttribute("event.type", event.getType());
                saveSpan.setAttribute("event.deviceId", event.getDeviceId());
                
                storage.addObject(event, Map.of());
                driverChangeEventCounter.increment();
                return event;
            } catch (Exception e) {
                saveSpan.recordException(e);
                saveSpan.setStatus(StatusCode.ERROR, e.getMessage());
                throw new RuntimeException("Failed to save driver change event", e);
            } finally {
                saveSpan.end();
            }
        };
        
        try {
            Event savedEvent = Retry.decorateSupplier(cacheRetry,
                    CircuitBreaker.decorateSupplier(cacheCircuitBreaker, eventSaveSupplier))
                    .get();
            
            return CompletableFuture.completedFuture(savedEvent);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Failed to save event: " + e.getMessage());
            failedDetectionCounter.increment();
            LOGGER.error("Failed to save driver change event: {}", e.getMessage(), e);
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Processes a position message from the message broker.
     * This is the main entry point for the handler when receiving position data.
     *
     * @param position the position to process
     * @param correlationId the correlation ID for cross-service tracking
     * @return CompletableFuture with the event if detected, or null if no event
     */
    public CompletableFuture<Event> processPositionMessage(Position position, String correlationId) {
        Span rootSpan = tracer.spanBuilder("process.position.message")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(CORRELATION_ID_KEY, correlationId)
                .setAttribute("deviceId", position.getDeviceId())
                .startSpan();
        
        try (Scope scope = rootSpan.makeCurrent()) {
            return analyzePosition(position, correlationId)
                    .exceptionally(e -> {
                        rootSpan.recordException(e);
                        rootSpan.setStatus(StatusCode.ERROR, e.getMessage());
                        LOGGER.error("Unhandled exception in driver change detection: {}", e.getMessage(), e);
                        return null;
                    });
        } finally {
            rootSpan.end();
        }
    }
}