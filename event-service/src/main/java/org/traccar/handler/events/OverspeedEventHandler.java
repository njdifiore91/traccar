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

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
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
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.helper.model.PositionUtil;
import org.traccar.messaging.EventProducer;
import org.traccar.model.Device;
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

import java.util.concurrent.TimeUnit;

public class OverspeedEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(OverspeedEventHandler.class);

    private final CacheManager cacheManager;
    private final Storage storage;
    private final EventProducer eventProducer;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    private final long minimalDuration;
    private final boolean preferLowest;
    private final double multiplier;

    // Metrics
    private final Timer overspeedDetectionTimer;
    private final Timer overspeedEventPublishTimer;

    @Inject
    public OverspeedEventHandler(Config config, CacheManager cacheManager, Storage storage, 
                                EventProducer eventProducer, Tracer tracer, MeterRegistry meterRegistry) {
        this.cacheManager = cacheManager;
        this.storage = storage;
        this.eventProducer = eventProducer;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        minimalDuration = config.getLong(Keys.EVENT_OVERSPEED_MINIMAL_DURATION) * 1000;
        preferLowest = config.getBoolean(Keys.EVENT_OVERSPEED_PREFER_LOWEST);
        multiplier = config.getDouble(Keys.EVENT_OVERSPEED_THRESHOLD_MULTIPLIER);
        
        // Initialize metrics
        overspeedDetectionTimer = Timer.builder("traccar.event.overspeed.detection")
                .description("Time taken to detect overspeed events")
                .register(meterRegistry);
        
        overspeedEventPublishTimer = Timer.builder("traccar.event.overspeed.publish")
                .description("Time taken to publish overspeed events")
                .register(meterRegistry);
        
        // Register additional counters
        meterRegistry.counter("traccar.event.overspeed.detected");
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for overspeed detection
        Span span = tracer.spanBuilder("overspeed.detection")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .setAttribute("positionId", String.valueOf(position.getId()))
                .setParent(Context.current())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Use timer to measure overspeed detection performance
            Timer.Sample sample = Timer.start(meterRegistry);
            
            processPosition(position, callback, span);
            
            // Record the time taken for overspeed detection
            sample.stop(overspeedDetectionTimer);
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.warn("Overspeed detection failed", e);
        } finally {
            span.end();
        }
    }

    @CircuitBreaker(name = "cacheManager", fallbackMethod = "fallbackProcessPosition")
    @Retry(name = "cacheManager")
    private void processPosition(Position position, Callback callback, Span parentSpan) {
        long deviceId = position.getDeviceId();
        Device device = getDevice(deviceId);
        if (device == null) {
            return;
        }
        if (!PositionUtil.isLatest(cacheManager, position) || !position.getValid()) {
            return;
        }

        double speedLimit = AttributeUtil.lookup(cacheManager, Keys.EVENT_OVERSPEED_LIMIT, deviceId);

        double positionSpeedLimit = position.getDouble(Position.KEY_SPEED_LIMIT);
        if (positionSpeedLimit > 0) {
            speedLimit = positionSpeedLimit;
        }

        double geofenceSpeedLimit = 0;
        long overspeedGeofenceId = 0;

        if (position.getGeofenceIds() != null) {
            for (long geofenceId : position.getGeofenceIds()) {
                Geofence geofence = getGeofence(geofenceId);
                if (geofence != null) {
                    double currentSpeedLimit = geofence.getDouble(Keys.EVENT_OVERSPEED_LIMIT.getKey());
                    if (currentSpeedLimit > 0 && geofenceSpeedLimit == 0
                            || preferLowest && currentSpeedLimit < geofenceSpeedLimit
                            || !preferLowest && currentSpeedLimit > geofenceSpeedLimit) {
                        geofenceSpeedLimit = currentSpeedLimit;
                        overspeedGeofenceId = geofenceId;
                    }
                }
            }
        }
        if (geofenceSpeedLimit > 0) {
            speedLimit = geofenceSpeedLimit;
        }

        if (speedLimit == 0) {
            return;
        }

        OverspeedState state = OverspeedState.fromDevice(device);
        OverspeedProcessor.updateState(state, position, speedLimit, multiplier, minimalDuration, overspeedGeofenceId);
        if (state.isChanged()) {
            state.toDevice(device);
            updateDeviceOverspeedState(device);
        }
        if (state.getEvent() != null) {
            // Increment the counter for detected overspeed events
            meterRegistry.counter("traccar.event.overspeed.detected").increment();
            
            // Create a child span for event publishing
            Span publishSpan = tracer.spanBuilder("overspeed.event.publish")
                    .setSpanKind(SpanKind.PRODUCER)
                    .setParent(Context.current())
                    .startSpan();
            
            try (Scope scope = publishSpan.makeCurrent()) {
                Timer.Sample publishSample = Timer.start(meterRegistry);
                
                // Add correlation ID to the event for distributed tracing
                state.getEvent().set("correlationId", Span.current().getSpanContext().getTraceId());
                
                // Use the callback to publish the event
                callback.eventDetected(state.getEvent());
                
                // Also publish to the message broker
                publishEvent(state.getEvent());
                
                publishSample.stop(overspeedEventPublishTimer);
                publishSpan.setStatus(StatusCode.OK);
            } catch (Exception e) {
                publishSpan.recordException(e);
                publishSpan.setStatus(StatusCode.ERROR, e.getMessage());
                LOGGER.warn("Failed to publish overspeed event", e);
            } finally {
                publishSpan.end();
            }
        }
    }

    @CircuitBreaker(name = "cacheManager")
    @Retry(name = "cacheManager")
    private Device getDevice(long deviceId) {
        return cacheManager.getObject(Device.class, deviceId);
    }

    @CircuitBreaker(name = "cacheManager")
    @Retry(name = "cacheManager")
    private Geofence getGeofence(long geofenceId) {
        return cacheManager.getObject(Geofence.class, geofenceId);
    }

    @CircuitBreaker(name = "storage", fallbackMethod = "fallbackUpdateDeviceOverspeedState")
    @Retry(name = "storage")
    private void updateDeviceOverspeedState(Device device) {
        try {
            storage.updateObject(device, new Request(
                    new Columns.Include("overspeedState", "overspeedTime", "overspeedGeofenceId"),
                    new Condition.Equals("id", device.getId())));
        } catch (StorageException e) {
            LOGGER.warn("Update device overspeed error", e);
            throw e; // Rethrow for circuit breaker and retry
        }
    }

    @CircuitBreaker(name = "eventProducer", fallbackMethod = "fallbackPublishEvent")
    @Retry(name = "eventProducer")
    private void publishEvent(org.traccar.model.Event event) {
        eventProducer.publish(event);
    }

    // Fallback methods for circuit breakers
    private void fallbackProcessPosition(Position position, Callback callback, Span parentSpan, Exception e) {
        LOGGER.warn("Using fallback for position processing due to: {}", e.getMessage());
        // No further processing in fallback mode
    }

    private void fallbackUpdateDeviceOverspeedState(Device device, Exception e) {
        LOGGER.warn("Using fallback for device state update due to: {}", e.getMessage());
        // State update will be retried on next position update
    }

    private void fallbackPublishEvent(org.traccar.model.Event event, Exception e) {
        LOGGER.warn("Using fallback for event publishing due to: {}", e.getMessage());
        // Event will still be processed through the callback mechanism
    }
}