/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2018 Andrey Kunitsyn (andrey@traccar.org)
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
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.helper.model.PositionUtil;
import org.traccar.messaging.MessageConsumer;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHandler;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Geofence;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;
import org.traccar.session.state.OverspeedProcessor;
import org.traccar.session.state.OverspeedState;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.time.Duration;
import java.util.function.Supplier;

public class OverspeedEventHandler extends BaseEventHandler implements MessageHandler<Position> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OverspeedEventHandler.class);

    private final CacheManager cacheManager;
    private final Storage storage;
    private final MessageProducer messageProducer;
    private final CircuitBreaker cacheCircuitBreaker;
    private final CircuitBreaker storageCircuitBreaker;
    private final Tracer tracer;
    private final Meter meter;
    private final LongCounter overspeedEventsCounter;
    private final LongCounter overspeedDetectionErrorCounter;

    private final long minimalDuration;
    private final boolean preferLowest;
    private final double multiplier;

    @Inject
    public OverspeedEventHandler(
            Config config,
            CacheManager cacheManager,
            Storage storage,
            MessageProducer messageProducer,
            CircuitBreakerRegistry circuitBreakerRegistry,
            Tracer tracer,
            Meter meter) {
        this.cacheManager = cacheManager;
        this.storage = storage;
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.meter = meter;
        
        // Configure circuit breakers
        CircuitBreakerConfig cacheCircuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(3)
                .slidingWindowSize(10)
                .build();
        
        CircuitBreakerConfig storageCircuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(20))
                .permittedNumberOfCallsInHalfOpenState(2)
                .slidingWindowSize(10)
                .build();
        
        this.cacheCircuitBreaker = circuitBreakerRegistry.circuitBreaker("overspeed-cache", cacheCircuitBreakerConfig);
        this.storageCircuitBreaker = circuitBreakerRegistry.circuitBreaker("overspeed-storage", storageCircuitBreakerConfig);
        
        // Initialize metrics
        this.overspeedEventsCounter = meter.counterBuilder("overspeed.events.total")
                .setDescription("Total number of overspeed events detected")
                .build();
        
        this.overspeedDetectionErrorCounter = meter.counterBuilder("overspeed.detection.errors")
                .setDescription("Total number of errors during overspeed detection")
                .build();
        
        // Configuration parameters
        minimalDuration = config.getLong(Keys.EVENT_OVERSPEED_MINIMAL_DURATION) * 1000;
        preferLowest = config.getBoolean(Keys.EVENT_OVERSPEED_PREFER_LOWEST);
        multiplier = config.getDouble(Keys.EVENT_OVERSPEED_THRESHOLD_MULTIPLIER);
    }

    @Override
    public void handle(MessageEnvelope<Position> messageEnvelope) {
        // Extract the position from the message
        Position position = messageEnvelope.getPayload();
        
        // Extract trace context from message headers and create a span
        Context extractedContext = Context.current();
        Span span = tracer.spanBuilder("overspeed.detection")
                .setParent(extractedContext)
                .setSpanKind(SpanKind.CONSUMER)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("position.deviceId", position.getDeviceId());
            span.setAttribute("position.id", position.getId());
            
            // Process the position for overspeed detection
            analyzePosition(position, event -> {
                // Create a child span for event publishing
                Span eventSpan = tracer.spanBuilder("overspeed.event.publish")
                        .setParent(Context.current())
                        .setSpanKind(SpanKind.PRODUCER)
                        .startSpan();
                
                try (Scope eventScope = eventSpan.makeCurrent()) {
                    eventSpan.setAttribute("event.type", event.getType());
                    eventSpan.setAttribute("event.deviceId", event.getDeviceId());
                    
                    // Increment the counter for detected events
                    overspeedEventsCounter.add(1, Attributes.of(
                            AttributeKey.stringKey("event.type"), event.getType()));
                    
                    // Publish the event to the message broker
                    MessageEnvelope<Event> eventEnvelope = new MessageEnvelope<>(event);
                    // Propagate the trace context
                    messageProducer.send("events", eventEnvelope);
                    
                    eventSpan.setStatus(StatusCode.OK);
                } catch (Exception e) {
                    eventSpan.recordException(e);
                    eventSpan.setStatus(StatusCode.ERROR, "Failed to publish event: " + e.getMessage());
                    LOGGER.error("Failed to publish overspeed event", e);
                } finally {
                    eventSpan.end();
                }
            });
            
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Overspeed detection failed: " + e.getMessage());
            overspeedDetectionErrorCounter.add(1);
            LOGGER.error("Overspeed detection failed", e);
        } finally {
            span.end();
        }
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for the position processing
        Span span = tracer.spanBuilder("overspeed.process.position")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("position.deviceId", position.getDeviceId());
            
            long deviceId = position.getDeviceId();
            
            // Use circuit breaker for cache access
            Device device = cacheCircuitBreaker.executeSupplier(() -> 
                    cacheManager.getObject(Device.class, position.getDeviceId()));
            
            if (device == null) {
                span.addEvent("Device not found");
                return;
            }
            
            // Check if position is valid and latest
            boolean isLatest = cacheCircuitBreaker.executeSupplier(() -> 
                    PositionUtil.isLatest(cacheManager, position));
            
            if (!isLatest || !position.getValid()) {
                span.addEvent("Position not valid or not latest");
                return;
            }
            
            // Get speed limit with circuit breaker
            double speedLimit = cacheCircuitBreaker.executeSupplier(() -> 
                    AttributeUtil.lookup(cacheManager, Keys.EVENT_OVERSPEED_LIMIT, deviceId));
            
            span.setAttribute("speedLimit.initial", speedLimit);
            
            double positionSpeedLimit = position.getDouble(Position.KEY_SPEED_LIMIT);
            if (positionSpeedLimit > 0) {
                speedLimit = positionSpeedLimit;
                span.setAttribute("speedLimit.position", positionSpeedLimit);
            }
            
            double geofenceSpeedLimit = 0;
            long overspeedGeofenceId = 0;
            
            if (position.getGeofenceIds() != null) {
                for (long geofenceId : position.getGeofenceIds()) {
                    // Use circuit breaker for geofence lookup
                    Geofence geofence = cacheCircuitBreaker.executeSupplier(() -> 
                            cacheManager.getObject(Geofence.class, geofenceId));
                    
                    if (geofence != null) {
                        double currentSpeedLimit = geofence.getDouble(Keys.EVENT_OVERSPEED_LIMIT.getKey());
                        if (currentSpeedLimit > 0 && geofenceSpeedLimit == 0
                                || preferLowest && currentSpeedLimit < geofenceSpeedLimit
                                || !preferLowest && currentSpeedLimit > geofenceSpeedLimit) {
                            geofenceSpeedLimit = currentSpeedLimit;
                            overspeedGeofenceId = geofenceId;
                            span.setAttribute("speedLimit.geofence", geofenceSpeedLimit);
                            span.setAttribute("speedLimit.geofenceId", overspeedGeofenceId);
                        }
                    }
                }
            }
            
            if (geofenceSpeedLimit > 0) {
                speedLimit = geofenceSpeedLimit;
            }
            
            span.setAttribute("speedLimit.final", speedLimit);
            
            if (speedLimit == 0) {
                span.addEvent("No speed limit defined");
                return;
            }
            
            // Process overspeed state
            OverspeedState state = OverspeedState.fromDevice(device);
            OverspeedProcessor.updateState(state, position, speedLimit, multiplier, minimalDuration, overspeedGeofenceId);
            
            if (state.isChanged()) {
                state.toDevice(device);
                
                // Use circuit breaker for storage operations
                try {
                    storageCircuitBreaker.executeSupplier(() -> {
                        try {
                            storage.updateObject(device, new Request(
                                    new Columns.Include("overspeedState", "overspeedTime", "overspeedGeofenceId"),
                                    new Condition.Equals("id", device.getId())));
                            return true;
                        } catch (StorageException e) {
                            LOGGER.warn("Update device overspeed error", e);
                            throw new RuntimeException(e);
                        }
                    });
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, "Failed to update device state: " + e.getMessage());
                }
            }
            
            if (state.getEvent() != null) {
                span.addEvent("Overspeed event detected");
                callback.eventDetected(state.getEvent());
            }
            
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Position processing failed: " + e.getMessage());
            LOGGER.warn("Overspeed event handler failed", e);
        } finally {
            span.end();
        }
    }
}