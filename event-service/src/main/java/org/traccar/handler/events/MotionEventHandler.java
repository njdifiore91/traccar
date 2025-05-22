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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
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
import java.util.concurrent.TimeUnit;

public class MotionEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MotionEventHandler.class);

    private final CacheManager cacheManager;
    private final Storage storage;
    private final CircuitBreaker cacheCircuitBreaker;
    private final CircuitBreaker storageCircuitBreaker;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;

    @Inject
    public MotionEventHandler(CacheManager cacheManager, Storage storage, 
                             MeterRegistry meterRegistry, Tracer tracer) {
        this.cacheManager = cacheManager;
        this.storage = storage;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        
        // Configure circuit breakers
        CircuitBreakerConfig cacheConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        
        CircuitBreakerConfig storageConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(20))
                .permittedNumberOfCallsInHalfOpenState(3)
                .slidingWindowSize(10)
                .build();
        
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CircuitBreakerConfig.ofDefaults());
        this.cacheCircuitBreaker = registry.circuitBreaker("motionEventCache", cacheConfig);
        this.storageCircuitBreaker = registry.circuitBreaker("motionEventStorage", storageConfig);
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for motion event detection
        Span span = tracer.spanBuilder("motion.event.detection")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .setAttribute("positionId", String.valueOf(position.getId()))
                .startSpan();
        
        // Create a timer for performance measurement
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            // Extract correlation ID from context if available
            String correlationId = Span.current().getSpanContext().getTraceId();
            span.setAttribute("correlationId", correlationId);
            
            long deviceId = position.getDeviceId();
            
            // Use circuit breaker for cache access
            Device device = cacheCircuitBreaker.executeSupplier(() -> cacheManager.getObject(Device.class, deviceId));
            
            if (device == null) {
                span.setStatus(StatusCode.ERROR, "Device not found in cache");
                span.end();
                return;
            }
            
            // Use circuit breaker for position validation
            boolean isLatest = cacheCircuitBreaker.executeSupplier(() -> PositionUtil.isLatest(cacheManager, position));
            if (!isLatest) {
                span.setStatus(StatusCode.ERROR, "Position is not latest");
                span.end();
                return;
            }
            
            // Use circuit breaker for attribute lookup
            boolean processInvalid = cacheCircuitBreaker.executeSupplier(() -> 
                    AttributeUtil.lookup(cacheManager, Keys.EVENT_MOTION_PROCESS_INVALID_POSITIONS, deviceId));
            
            if (!processInvalid && !position.getValid()) {
                span.setStatus(StatusCode.ERROR, "Invalid position skipped");
                span.end();
                return;
            }

            // Use circuit breaker for trips config
            TripsConfig tripsConfig = cacheCircuitBreaker.executeSupplier(() -> 
                    new TripsConfig(new AttributeUtil.CacheProvider(cacheManager, deviceId)));
            
            span.addEvent("Processing motion state");
            MotionState state = MotionState.fromDevice(device);
            MotionProcessor.updateState(state, position, position.getBoolean(Position.KEY_MOTION), tripsConfig);
            
            if (state.isChanged()) {
                span.addEvent("Motion state changed");
                state.toDevice(device);
                try {
                    // Use circuit breaker for storage operations
                    storageCircuitBreaker.executeRunnable(() -> {
                        try {
                            storage.updateObject(device, new Request(
                                    new Columns.Include("motionStreak", "motionState", "motionTime", "motionDistance"),
                                    new Condition.Equals("id", device.getId())));
                        } catch (StorageException e) {
                            LOGGER.warn("Update device motion error", e);
                            throw new RuntimeException(e);
                        }
                    });
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, "Failed to update device motion state");
                    LOGGER.warn("Circuit breaker prevented storage update", e);
                }
            }
            
            if (state.getEvent() != null) {
                span.addEvent("Motion event detected", 
                        Span.current().getSpanContext().getTraceId(), 
                        Span.current().getSpanContext().getSpanId());
                span.setAttribute("eventType", state.getEvent().getType());
                
                // Add the correlation ID to the event for tracing
                state.getEvent().set("correlationId", correlationId);
                
                // Publish the event via callback
                callback.eventDetected(state.getEvent());
            }
            
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error processing motion event", e);
        } finally {
            // Record metrics for the operation
            sample.stop(meterRegistry.timer("motion.event.processing", 
                    "deviceId", String.valueOf(position.getDeviceId()),
                    "valid", String.valueOf(position.getValid())));
            span.end();
        }
    }
}