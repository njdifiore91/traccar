/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2017 Andrey Kunitsyn (andrey@traccar.org)
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
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Keys;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.helper.model.PositionUtil;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.reports.common.TripsConfig;
import org.traccar.session.cache.CacheManager;
import org.traccar.session.state.MotionProcessor;
import org.traccar.session.state.MotionState;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class MotionEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MotionEventHandler.class);
    
    // OpenTelemetry attribute keys
    private static final AttributeKey<String> DEVICE_ID_KEY = AttributeKey.stringKey("device.id");
    private static final AttributeKey<String> MOTION_STATE_KEY = AttributeKey.stringKey("motion.state");
    private static final AttributeKey<String> CORRELATION_ID_KEY = AttributeKey.stringKey("correlation.id");
    private static final AttributeKey<String> EVENT_TYPE_KEY = AttributeKey.stringKey("event.type");
    
    private final CacheManager cacheManager;
    private final Storage storage;
    private final Tracer tracer;
    private final TextMapPropagator propagator;
    private final MeterRegistry meterRegistry;
    
    // Circuit breakers
    private final CircuitBreaker cacheCircuitBreaker;
    private final CircuitBreaker storageCircuitBreaker;
    
    // Retry mechanisms
    private final Retry storageRetry;
    
    // Metrics
    private final Timer motionDetectionTimer;
    private final Counter motionStartCounter;
    private final Counter motionStopCounter;
    private final Counter motionDetectionErrorCounter;

    @Inject
    public MotionEventHandler(
            CacheManager cacheManager, 
            Storage storage, 
            Tracer tracer, 
            TextMapPropagator propagator,
            MeterRegistry meterRegistry) {
        this.cacheManager = cacheManager;
        this.storage = storage;
        this.tracer = tracer;
        this.propagator = propagator;
        this.meterRegistry = meterRegistry;
        
        // Initialize circuit breakers
        CircuitBreakerConfig cacheCircuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        
        CircuitBreakerConfig storageCircuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(3)
                .slidingWindowSize(10)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(
                Map.of("cache", cacheCircuitBreakerConfig, "storage", storageCircuitBreakerConfig));
        
        cacheCircuitBreaker = circuitBreakerRegistry.circuitBreaker("cache");
        storageCircuitBreaker = circuitBreakerRegistry.circuitBreaker("storage");
        
        // Initialize retry mechanism
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(StorageException.class)
                .enableExponentialBackoff(true)
                .exponentialBackoffMultiplier(2)
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        storageRetry = retryRegistry.retry("storage");
        
        // Initialize metrics
        motionDetectionTimer = Timer.builder("motion.detection.duration")
                .description("Time taken to detect motion events")
                .register(meterRegistry);
        
        motionStartCounter = Counter.builder("motion.events")
                .tag("type", "start")
                .description("Number of motion start events detected")
                .register(meterRegistry);
        
        motionStopCounter = Counter.builder("motion.events")
                .tag("type", "stop")
                .description("Number of motion stop events detected")
                .register(meterRegistry);
        
        motionDetectionErrorCounter = Counter.builder("motion.detection.errors")
                .description("Number of errors during motion detection")
                .register(meterRegistry);
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Extract correlation ID and create parent context
        Context parentContext = extractContextFromPosition(position);
        
        // Create a span for motion event detection
        Span span = tracer.spanBuilder("motion.event.detection")
                .setParent(parentContext)
                .setSpanKind(SpanKind.CONSUMER)
                .startSpan();
        
        // Add position attributes to span
        span.setAttribute(DEVICE_ID_KEY, String.valueOf(position.getDeviceId()));
        
        // Execute with the span in context
        try (Scope scope = span.makeCurrent()) {
            // Use timer to measure motion detection performance
            motionDetectionTimer.record(() -> {
                processPosition(position, callback, span);
            });
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            motionDetectionErrorCounter.increment();
            LOGGER.error("Error in motion event detection", e);
        } finally {
            span.end();
        }
    }
    
    private Context extractContextFromPosition(Position position) {
        // Create a getter for extracting context from position attributes
        TextMapGetter<Position> getter = new TextMapGetter<>() {
            @Override
            public Iterable<String> keys(Position carrier) {
                return carrier.getAttributes().keySet();
            }

            @Override
            public String get(Position carrier, String key) {
                return carrier.getString(key);
            }
        };
        
        // Extract context using the propagator
        return propagator.extract(Context.current(), position, getter);
    }

    private void processPosition(Position position, Callback callback, Span parentSpan) {
        long deviceId = position.getDeviceId();
        
        // Create a child span for device retrieval
        Span deviceSpan = tracer.spanBuilder("get.device")
                .setParent(Context.current().with(parentSpan))
                .startSpan();
        deviceSpan.setAttribute(DEVICE_ID_KEY, String.valueOf(deviceId));
        
        // Get device with circuit breaker
        Device device;
        try {
            device = cacheCircuitBreaker.executeSupplier(() -> cacheManager.getObject(Device.class, deviceId));
            deviceSpan.setStatus(StatusCode.OK);
        } catch (Exception e) {
            deviceSpan.setStatus(StatusCode.ERROR, "Failed to get device: " + e.getMessage());
            deviceSpan.recordException(e);
            LOGGER.warn("Circuit breaker prevented device retrieval", e);
            deviceSpan.end();
            return;
        } finally {
            deviceSpan.end();
        }
        
        if (device == null) {
            parentSpan.addEvent("Device not found");
            return;
        }
        
        // Check if position is latest
        Span positionCheckSpan = tracer.spanBuilder("check.position.latest")
                .setParent(Context.current().with(parentSpan))
                .startSpan();
        positionCheckSpan.setAttribute(DEVICE_ID_KEY, String.valueOf(deviceId));
        
        boolean isLatest;
        try {
            isLatest = cacheCircuitBreaker.executeSupplier(() -> PositionUtil.isLatest(cacheManager, position));
            positionCheckSpan.setStatus(StatusCode.OK);
        } catch (Exception e) {
            positionCheckSpan.setStatus(StatusCode.ERROR, "Failed to check if position is latest: " + e.getMessage());
            positionCheckSpan.recordException(e);
            LOGGER.warn("Circuit breaker prevented position check", e);
            positionCheckSpan.end();
            return;
        } finally {
            positionCheckSpan.end();
        }
        
        if (!isLatest) {
            parentSpan.addEvent("Position is not latest");
            return;
        }
        
        // Check if we should process invalid positions
        Span attributeCheckSpan = tracer.spanBuilder("check.process.invalid")
                .setParent(Context.current().with(parentSpan))
                .startSpan();
        attributeCheckSpan.setAttribute(DEVICE_ID_KEY, String.valueOf(deviceId));
        
        boolean processInvalid;
        try {
            processInvalid = cacheCircuitBreaker.executeSupplier(() -> 
                    AttributeUtil.lookup(cacheManager, Keys.EVENT_MOTION_PROCESS_INVALID_POSITIONS, deviceId));
            attributeCheckSpan.setStatus(StatusCode.OK);
        } catch (Exception e) {
            attributeCheckSpan.setStatus(StatusCode.ERROR, "Failed to check process invalid attribute: " + e.getMessage());
            attributeCheckSpan.recordException(e);
            LOGGER.warn("Circuit breaker prevented attribute check", e);
            attributeCheckSpan.end();
            return;
        } finally {
            attributeCheckSpan.end();
        }
        
        if (!processInvalid && !position.getValid()) {
            parentSpan.addEvent("Skipping invalid position");
            return;
        }

        // Process motion state
        Span motionProcessSpan = tracer.spanBuilder("process.motion.state")
                .setParent(Context.current().with(parentSpan))
                .startSpan();
        motionProcessSpan.setAttribute(DEVICE_ID_KEY, String.valueOf(deviceId));
        
        try {
            // Get trips config
            TripsConfig tripsConfig = new TripsConfig(new AttributeUtil.CacheProvider(cacheManager, deviceId));
            
            // Get motion state
            MotionState state = MotionState.fromDevice(device);
            motionProcessSpan.setAttribute(MOTION_STATE_KEY, String.valueOf(state.getMotionState()));
            
            // Update motion state
            MotionProcessor.updateState(state, position, position.getBoolean(Position.KEY_MOTION), tripsConfig);
            
            // If state changed, update device
            if (state.isChanged()) {
                motionProcessSpan.addEvent("Motion state changed");
                motionProcessSpan.setAttribute("motion.state.new", String.valueOf(state.getMotionState()));
                
                // Update device state
                state.toDevice(device);
                
                // Update device in storage with retry and circuit breaker
                try {
                    Supplier<Void> updateOperation = () -> {
                        try {
                            storage.updateObject(device, new Request(
                                    new Columns.Include("motionStreak", "motionState", "motionTime", "motionDistance"),
                                    new Condition.Equals("id", device.getId())));
                            return null;
                        } catch (StorageException e) {
                            throw new RuntimeException(e);
                        }
                    };
                    
                    // Apply retry with circuit breaker
                    Retry.decorateSupplier(storageRetry, 
                            CircuitBreaker.decorateSupplier(storageCircuitBreaker, updateOperation)).get();
                    
                    motionProcessSpan.addEvent("Device state updated in storage");
                } catch (Exception e) {
                    motionProcessSpan.setStatus(StatusCode.ERROR, "Failed to update device: " + e.getMessage());
                    motionProcessSpan.recordException(e);
                    LOGGER.warn("Update device motion error", e);
                }
            }
            
            // If event generated, send it
            if (state.getEvent() != null) {
                // Add correlation ID to event attributes
                Span currentSpan = Span.current();
                String correlationId = currentSpan.getSpanContext().getTraceId();
                
                state.getEvent().set("correlationId", correlationId);
                
                // Record metrics based on event type
                if (state.getEvent().getType().equals("deviceMoving")) {
                    motionStartCounter.increment();
                    motionProcessSpan.setAttribute(EVENT_TYPE_KEY, "deviceMoving");
                } else if (state.getEvent().getType().equals("deviceStopped")) {
                    motionStopCounter.increment();
                    motionProcessSpan.setAttribute(EVENT_TYPE_KEY, "deviceStopped");
                }
                
                // Send event
                callback.eventDetected(state.getEvent());
                motionProcessSpan.addEvent("Event sent: " + state.getEvent().getType());
            }
            
            motionProcessSpan.setStatus(StatusCode.OK);
        } catch (Exception e) {
            motionProcessSpan.setStatus(StatusCode.ERROR, e.getMessage());
            motionProcessSpan.recordException(e);
            motionDetectionErrorCounter.increment();
            LOGGER.error("Error processing motion state", e);
        } finally {
            motionProcessSpan.end();
        }
    }
}