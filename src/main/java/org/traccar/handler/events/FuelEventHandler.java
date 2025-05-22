/*
 * Copyright 2017 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Keys;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.helper.model.PositionUtil;
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Fuel event handler that processes position data from the message broker
 * and publishes fuel level change events when detected.
 * Includes circuit breaker pattern for CacheManager interactions,
 * metrics collection for performance monitoring, and distributed tracing.
 */
@Singleton
public class FuelEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FuelEventHandler.class);
    
    private final CacheManager cacheManager;
    private final MessagePublisher messagePublisher;
    private final CircuitBreaker circuitBreaker;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final Timer processingTimer;
    
    /**
     * Creates a new FuelEventHandler with the specified dependencies.
     *
     * @param cacheManager The cache manager for accessing device and position data
     * @param messagePublisher The message publisher for sending events to the message broker
     * @param meterRegistry The meter registry for metrics collection
     * @param tracer The OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public FuelEventHandler(
            CacheManager cacheManager,
            MessagePublisher messagePublisher,
            MeterRegistry meterRegistry,
            Tracer tracer) {
        this.cacheManager = cacheManager;
        this.messagePublisher = messagePublisher;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        
        // Configure circuit breaker for CacheManager interactions
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("cacheManager");
        
        // Register circuit breaker events for monitoring
        this.circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker state changed: {}", event));
        
        // Create timer for measuring processing performance
        this.processingTimer = meterRegistry.timer("fuel.event.processing");
        
        // Register additional metrics
        meterRegistry.gauge("fuel.event.circuit_breaker.state", circuitBreaker, cb -> cb.getState().getOrder());
        meterRegistry.gauge("fuel.event.circuit_breaker.failure_rate", circuitBreaker, CircuitBreaker::getFailureRate);
    }

    /**
     * Processes a position update from the message broker.
     * This method is called by the message consumer when a new position is received.
     *
     * @param position The position data received from the message broker
     * @param correlationId The correlation ID for distributed tracing
     * @return A CompletableFuture that completes when processing is finished
     */
    public CompletableFuture<Void> processPositionUpdate(Position position, String correlationId) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("FuelEventHandler.processPositionUpdate")
                .setSpanKind(SpanKind.CONSUMER)
                .setParent(Context.current())
                .setAttribute("deviceId", position.getDeviceId())
                .setAttribute("correlationId", correlationId != null ? correlationId : "unknown")
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Use the timer to measure processing time
            return processingTimer.record(() -> {
                try {
                    // Process the position and detect fuel events
                    onPosition(position, event -> {
                        // Set trace context on the event
                        event.setSpanContext(Span.current().getSpanContext());
                        event.setCorrelationId(correlationId != null ? correlationId : UUID.randomUUID().toString());
                        event.setServiceOrigin("event-service");
                        
                        // Publish the event to the message broker
                        publishEvent(event);
                    });
                    
                    span.setStatus(StatusCode.OK);
                    return CompletableFuture.completedFuture(null);
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    LOGGER.error("Error processing position update", e);
                    return CompletableFuture.failedFuture(e);
                }
            });
        } finally {
            span.end();
        }
    }
    
    /**
     * Publishes an event to the message broker.
     *
     * @param event The event to publish
     */
    private void publishEvent(Event event) {
        Span span = tracer.spanBuilder("FuelEventHandler.publishEvent")
                .setSpanKind(SpanKind.PRODUCER)
                .setParent(Context.current())
                .setAttribute("eventType", event.getType())
                .setAttribute("deviceId", event.getDeviceId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            messagePublisher.publish("events", event.getType(), event);
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Failed to publish event", e);
        } finally {
            span.end();
        }
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Skip processing if position doesn't have fuel level information
        if (!position.hasAttribute(Position.KEY_FUEL_LEVEL)) {
            return;
        }
        
        // Execute with circuit breaker to prevent cascading failures
        try {
            // Get device with circuit breaker
            Device device = executeWithCircuitBreaker(() -> 
                    cacheManager.getObject(Device.class, position.getDeviceId()),
                    "getDevice");
            
            if (device == null) {
                return;
            }
            
            // Check if this is the latest position with circuit breaker
            boolean isLatest = executeWithCircuitBreaker(() -> 
                    PositionUtil.isLatest(cacheManager, position),
                    "isLatestPosition");
            
            if (!isLatest) {
                return;
            }

            // Get previous position with circuit breaker
            Position lastPosition = executeWithCircuitBreaker(() -> 
                    cacheManager.getPosition(position.getDeviceId()),
                    "getLastPosition");
            
            if (lastPosition != null && lastPosition.hasAttribute(Position.KEY_FUEL_LEVEL)) {
                double before = lastPosition.getDouble(Position.KEY_FUEL_LEVEL);
                double after = position.getDouble(Position.KEY_FUEL_LEVEL);
                double change = after - before;

                if (change > 0) {
                    // Get fuel increase threshold with circuit breaker
                    double threshold = executeWithCircuitBreaker(() -> 
                            AttributeUtil.lookup(cacheManager, Keys.EVENT_FUEL_INCREASE_THRESHOLD, position.getDeviceId()),
                            "getFuelIncreaseThreshold");
                    
                    if (threshold > 0 && change >= threshold) {
                        Event event = new Event(Event.TYPE_DEVICE_FUEL_INCREASE, position);
                        event.set("before", before);
                        event.set("after", after);
                        event.set("change", change);
                        callback.eventDetected(event);
                    }
                } else if (change < 0) {
                    // Get fuel drop threshold with circuit breaker
                    double threshold = executeWithCircuitBreaker(() -> 
                            AttributeUtil.lookup(cacheManager, Keys.EVENT_FUEL_DROP_THRESHOLD, position.getDeviceId()),
                            "getFuelDropThreshold");
                    
                    if (threshold > 0 && Math.abs(change) >= threshold) {
                        Event event = new Event(Event.TYPE_DEVICE_FUEL_DROP, position);
                        event.set("before", before);
                        event.set("after", after);
                        event.set("change", change);
                        callback.eventDetected(event);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error processing fuel level change", e);
            meterRegistry.counter("fuel.event.errors").increment();
        }
    }
    
    /**
     * Executes a function with circuit breaker protection.
     *
     * @param supplier The function to execute
     * @param operationName The name of the operation for metrics and logging
     * @param <T> The return type of the function
     * @return The result of the function
     */
    private <T> T executeWithCircuitBreaker(Supplier<T> supplier, String operationName) {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            return circuitBreaker.executeSupplier(supplier);
        } finally {
            sample.stop(meterRegistry.timer("fuel.event.cache_operation", "operation", operationName));
        }
    }
}