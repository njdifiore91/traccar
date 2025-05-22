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
import io.micrometer.core.instrument.Counter;
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
import org.traccar.messaging.MessageConsumer;
import org.traccar.messaging.MessageProducer;
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
import java.util.function.Supplier;

public class MotionEventHandler implements MessageConsumer<Position> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MotionEventHandler.class);

    private final CacheManager cacheManager;
    private final Storage storage;
    private final MessageProducer messageProducer;
    private final CircuitBreaker cacheCircuitBreaker;
    private final CircuitBreaker storageCircuitBreaker;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    
    private final Timer motionDetectionTimer;
    private final Counter motionEventsCounter;
    private final Counter motionStateChangesCounter;

    @Inject
    public MotionEventHandler(
            CacheManager cacheManager, 
            Storage storage, 
            MessageProducer messageProducer,
            CircuitBreakerRegistry circuitBreakerRegistry,
            MeterRegistry meterRegistry,
            Tracer tracer) {
        this.cacheManager = cacheManager;
        this.storage = storage;
        this.messageProducer = messageProducer;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        
        // Configure circuit breakers
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
        
        this.cacheCircuitBreaker = circuitBreakerRegistry.circuitBreaker("cacheManager", cacheCircuitBreakerConfig);
        this.storageCircuitBreaker = circuitBreakerRegistry.circuitBreaker("storage", storageCircuitBreakerConfig);
        
        // Register circuit breaker event listeners
        cacheCircuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Cache circuit breaker state changed from {} to {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));
        
        storageCircuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Storage circuit breaker state changed from {} to {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));
        
        // Initialize metrics
        motionDetectionTimer = Timer.builder("motion.detection.time")
                .description("Time taken to detect motion events")
                .register(meterRegistry);
        
        motionEventsCounter = Counter.builder("motion.events")
                .description("Number of motion events detected")
                .register(meterRegistry);
        
        motionStateChangesCounter = Counter.builder("motion.state.changes")
                .description("Number of motion state changes")
                .register(meterRegistry);
    }

    @Override
    public void consume(Position position, String correlationId) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("motion.event.detection")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("position.id", position.getId())
                .setAttribute("device.id", position.getDeviceId())
                .setAttribute("correlation.id", correlationId)
                .startSpan();
        
        // Attach the span to the current context
        try (Scope scope = span.makeCurrent()) {
            Timer.Sample sample = Timer.start(meterRegistry);
            
            try {
                processPosition(position, correlationId, span);
                span.setStatus(StatusCode.OK);
            } catch (Exception e) {
                LOGGER.error("Error processing position for motion detection", e);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
            } finally {
                sample.stop(motionDetectionTimer);
                span.end();
            }
        }
    }
    
    private void processPosition(Position position, String correlationId, Span parentSpan) {
        long deviceId = position.getDeviceId();
        
        // Create a child span for device retrieval
        Span deviceSpan = tracer.spanBuilder("get.device")
                .setParent(Context.current())
                .setAttribute("device.id", deviceId)
                .startSpan();
        
        Device device = null;
        try (Scope scope = deviceSpan.makeCurrent()) {
            // Use circuit breaker for cache access
            device = cacheCircuitBreaker.executeSupplier(() -> cacheManager.getObject(Device.class, deviceId));
            deviceSpan.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.warn("Failed to get device from cache", e);
            deviceSpan.recordException(e);
            deviceSpan.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            deviceSpan.end();
        }
        
        if (device == null || !isLatestPosition(position)) {
            return;
        }
        
        boolean processInvalid = getProcessInvalidAttribute(deviceId);
        if (!processInvalid && !position.getValid()) {
            return;
        }

        // Create a span for motion processing
        Span motionSpan = tracer.spanBuilder("process.motion")
                .setParent(Context.current())
                .setAttribute("device.id", deviceId)
                .startSpan();
        
        try (Scope scope = motionSpan.makeCurrent()) {
            TripsConfig tripsConfig = new TripsConfig(new AttributeUtil.CacheProvider(cacheManager, deviceId));
            MotionState state = MotionState.fromDevice(device);
            MotionProcessor.updateState(state, position, position.getBoolean(Position.KEY_MOTION), tripsConfig);
            
            if (state.isChanged()) {
                motionStateChangesCounter.increment();
                state.toDevice(device);
                updateDeviceMotionState(device);
            }
            
            if (state.getEvent() != null) {
                motionEventsCounter.increment();
                publishEvent(state.getEvent(), correlationId);
            }
            
            motionSpan.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.warn("Error processing motion state", e);
            motionSpan.recordException(e);
            motionSpan.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            motionSpan.end();
        }
    }
    
    private boolean isLatestPosition(Position position) {
        Span span = tracer.spanBuilder("check.latest.position")
                .setParent(Context.current())
                .setAttribute("position.id", position.getId())
                .setAttribute("device.id", position.getDeviceId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            boolean result = cacheCircuitBreaker.executeSupplier(() -> 
                    PositionUtil.isLatest(cacheManager, position));
            span.setStatus(StatusCode.OK);
            return result;
        } catch (Exception e) {
            LOGGER.warn("Failed to check if position is latest", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            return false; // Default to false if we can't determine
        } finally {
            span.end();
        }
    }
    
    private boolean getProcessInvalidAttribute(long deviceId) {
        Span span = tracer.spanBuilder("get.process.invalid.attribute")
                .setParent(Context.current())
                .setAttribute("device.id", deviceId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            boolean result = cacheCircuitBreaker.executeSupplier(() -> 
                    AttributeUtil.lookup(cacheManager, Keys.EVENT_MOTION_PROCESS_INVALID_POSITIONS, deviceId));
            span.setStatus(StatusCode.OK);
            return result;
        } catch (Exception e) {
            LOGGER.warn("Failed to get process invalid attribute", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            return false; // Default to false if we can't determine
        } finally {
            span.end();
        }
    }
    
    private void updateDeviceMotionState(Device device) {
        Span span = tracer.spanBuilder("update.device.motion")
                .setParent(Context.current())
                .setAttribute("device.id", device.getId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Supplier<Void> updateOperation = () -> {
                try {
                    storage.updateObject(device, new Request(
                            new Columns.Include("motionStreak", "motionState", "motionTime", "motionDistance"),
                            new Condition.Equals("id", device.getId())));
                    return null;
                } catch (StorageException e) {
                    throw new RuntimeException("Update device motion error", e);
                }
            };
            
            storageCircuitBreaker.executeSupplier(() -> updateOperation.get());
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.warn("Update device motion error", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }
    
    private void publishEvent(Object event, String correlationId) {
        Span span = tracer.spanBuilder("publish.motion.event")
                .setParent(Context.current())
                .setAttribute("correlation.id", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            messageProducer.publish("events", event, correlationId);
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.warn("Failed to publish motion event", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }
}