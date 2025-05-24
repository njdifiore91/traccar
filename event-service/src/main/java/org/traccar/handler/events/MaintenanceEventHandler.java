/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2016 - 2018 Andrey Kunitsyn (andrey@traccar.org)
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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Event;
import org.traccar.model.Maintenance;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.time.Duration;
import java.util.function.Supplier;

public class MaintenanceEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MaintenanceEventHandler.class);

    private final CacheManager cacheManager;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Timer maintenanceEventTimer;

    @Inject
    public MaintenanceEventHandler(CacheManager cacheManager, Tracer tracer, MeterRegistry meterRegistry) {
        this.cacheManager = cacheManager;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Configure circuit breaker for CacheManager interactions
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // When 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(10)) // Wait 10 seconds before trying again
                .slidingWindowSize(10) // Consider the last 10 calls
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("maintenanceEventHandler");
        
        // Configure retry with exponential backoff for transient failures
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Try up to 3 times
                .waitDuration(Duration.ofMillis(500)) // Start with 500ms delay
                .retryExceptions(Exception.class) // Retry on any exception
                .enableExponentialBackoff(true) // Use exponential backoff
                .exponentialBackoffMultiplier(2) // Double the wait time for each retry
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry("maintenanceEventHandler");
        
        // Initialize metrics
        this.maintenanceEventTimer = Timer.builder("maintenance.event.detection.time")
                .description("Time taken to detect maintenance events")
                .register(meterRegistry);
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("MaintenanceEventHandler.onPosition")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .startSpan();
        
        // Get the current correlation ID from the span context
        String correlationId = span.getSpanContext().getTraceId();
        LOGGER.debug("Processing position with correlation ID: {}", correlationId);
        
        try (Scope scope = span.makeCurrent()) {
            // Use timer to measure performance
            maintenanceEventTimer.record(() -> {
                try {
                    processPosition(position, callback, span);
                } catch (Exception e) {
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    LOGGER.warn("Error processing maintenance event with correlation ID {}: {}", 
                            correlationId, e.getMessage(), e);
                }
            });
        } finally {
            span.end();
        }
    }

    private void processPosition(Position position, Callback callback, Span parentSpan) {
        // Wrap CacheManager calls with circuit breaker and retry
        Supplier<Position> lastPositionSupplier = Retry.decorateSupplier(
                retry,
                CircuitBreaker.decorateSupplier(
                        circuitBreaker,
                        () -> {
                            Span span = tracer.spanBuilder("CacheManager.getPosition")
                                    .setParent(Context.current())
                                    .setSpanKind(SpanKind.CLIENT)
                                    .startSpan();
                            try (Scope scope = span.makeCurrent()) {
                                return cacheManager.getPosition(position.getDeviceId());
                            } finally {
                                span.end();
                            }
                        }
                )
        );

        Position lastPosition = lastPositionSupplier.get();
        if (lastPosition == null || position.getFixTime().compareTo(lastPosition.getFixTime()) < 0) {
            return;
        }

        // Wrap CacheManager calls with circuit breaker and retry
        Supplier<Iterable<Maintenance>> maintenanceSupplier = Retry.decorateSupplier(
                retry,
                CircuitBreaker.decorateSupplier(
                        circuitBreaker,
                        () -> {
                            Span span = tracer.spanBuilder("CacheManager.getDeviceObjects")
                                    .setParent(Context.current())
                                    .setSpanKind(SpanKind.CLIENT)
                                    .startSpan();
                            try (Scope scope = span.makeCurrent()) {
                                return cacheManager.getDeviceObjects(position.getDeviceId(), Maintenance.class);
                            } finally {
                                span.end();
                            }
                        }
                )
        );

        for (Maintenance maintenance : maintenanceSupplier.get()) {
            if (maintenance.getPeriod() != 0) {
                Span maintenanceSpan = tracer.spanBuilder("MaintenanceCheck")
                        .setParent(Context.current())
                        .setSpanKind(SpanKind.INTERNAL)
                        .setAttribute("maintenance.id", maintenance.getId())
                        .setAttribute("maintenance.type", maintenance.getType())
                        .setAttribute("maintenance.period", maintenance.getPeriod())
                        .startSpan();
                
                try (Scope scope = maintenanceSpan.makeCurrent()) {
                    double oldValue = getValue(lastPosition, maintenance.getType());
                    double newValue = getValue(position, maintenance.getType());
                    
                    maintenanceSpan.setAttribute("maintenance.oldValue", oldValue);
                    maintenanceSpan.setAttribute("maintenance.newValue", newValue);
                    maintenanceSpan.setAttribute("maintenance.start", maintenance.getStart());
                    
                    if (oldValue != 0.0 && newValue != 0.0 && newValue >= maintenance.getStart()) {
                        if (oldValue < maintenance.getStart()
                            || (long) ((oldValue - maintenance.getStart()) / maintenance.getPeriod())
                            < (long) ((newValue - maintenance.getStart()) / maintenance.getPeriod())) {
                            
                            // Record metric for maintenance event detection
                            meterRegistry.counter("maintenance.event.detected", 
                                    "type", maintenance.getType(), 
                                    "deviceId", String.valueOf(position.getDeviceId()))
                                    .increment();
                            
                            Event event = new Event(Event.TYPE_MAINTENANCE, position);
                            event.setMaintenanceId(maintenance.getId());
                            event.set(maintenance.getType(), newValue);
                            
                            // Propagate correlation ID to the event
                            String correlationId = Span.current().getSpanContext().getTraceId();
                            event.set("correlationId", correlationId);
                            
                            maintenanceSpan.setAttribute("event.detected", true);
                            LOGGER.info("Maintenance event detected: type={}, deviceId={}, correlationId={}", 
                                    maintenance.getType(), position.getDeviceId(), correlationId);
                            
                            callback.eventDetected(event);
                        } else {
                            maintenanceSpan.setAttribute("event.detected", false);
                        }
                    } else {
                        maintenanceSpan.setAttribute("event.detected", false);
                    }
                } finally {
                    maintenanceSpan.end();
                }
            }
        }
    }

    private double getValue(Position position, String type) {
        return switch (type) {
            case "serverTime" -> position.getServerTime().getTime();
            case "deviceTime" -> position.getDeviceTime().getTime();
            case "fixTime" -> position.getFixTime().getTime();
            default -> position.getDouble(type);
        };
    }
}