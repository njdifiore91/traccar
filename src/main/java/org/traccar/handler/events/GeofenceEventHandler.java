/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.helper.model.PositionUtil;
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.Calendar;
import org.traccar.model.Event;
import org.traccar.model.Geofence;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class GeofenceEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeofenceEventHandler.class);
    private static final String CIRCUIT_BREAKER_NAME = "geofenceEventHandlerCache";
    private static final String TRACER_NAME = "org.traccar.handler.events.GeofenceEventHandler";
    private static final String TIMER_NAME = "geofence.event.detection";
    private static final String EVENT_TOPIC = "events";

    private final CacheManager cacheManager;
    private final CircuitBreaker circuitBreaker;
    private final Timer detectionTimer;
    private final Tracer tracer;
    private final MessagePublisher messagePublisher;

    @Inject
    public GeofenceEventHandler(
            CacheManager cacheManager,
            CircuitBreakerRegistry circuitBreakerRegistry,
            MeterRegistry meterRegistry,
            Tracer tracer,
            MessagePublisher messagePublisher) {
        this.cacheManager = cacheManager;
        this.tracer = tracer;
        this.messagePublisher = messagePublisher;
        
        // Configure and create circuit breaker for cache operations
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME, circuitBreakerConfig);
        
        // Create timer for measuring geofence detection performance
        this.detectionTimer = Timer.builder(TIMER_NAME)
                .description("Time taken to detect geofence events")
                .register(meterRegistry);
    }

    /**
     * Process a position update and detect geofence events
     * This method is maintained for backward compatibility with direct invocation
     */
    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("geofence.detect")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Record the operation timing
            detectionTimer.record(() -> {
                processGeofenceEvents(position, callback, span);
                return null;
            });
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.error("Error processing geofence events", e);
        } finally {
            span.end();
        }
    }

    /**
     * Process geofence events for a position
     */
    private void processGeofenceEvents(Position position, Callback callback, Span parentSpan) {
        // Check if this is the latest position
        if (!executeWithCircuitBreaker(() -> PositionUtil.isLatest(cacheManager, position))) {
            return;
        }

        List<Long> oldGeofences = new ArrayList<>();
        Position lastPosition = executeWithCircuitBreaker(() -> cacheManager.getPosition(position.getDeviceId()));
        if (lastPosition != null && lastPosition.getGeofenceIds() != null) {
            oldGeofences.addAll(lastPosition.getGeofenceIds());
        }

        List<Long> newGeofences = new ArrayList<>();
        if (position.getGeofenceIds() != null) {
            newGeofences.addAll(position.getGeofenceIds());
            newGeofences.removeAll(oldGeofences);
            oldGeofences.removeAll(position.getGeofenceIds());
        }

        // Process geofence exit events
        for (long geofenceId : oldGeofences) {
            Span exitSpan = tracer.spanBuilder("geofence.exit.detect")
                    .setParent(Context.current().with(parentSpan))
                    .setAttribute("geofenceId", String.valueOf(geofenceId))
                    .startSpan();
            
            try (Scope scope = exitSpan.makeCurrent()) {
                Geofence geofence = executeWithCircuitBreaker(() -> cacheManager.getObject(Geofence.class, geofenceId));
                if (geofence != null) {
                    long calendarId = geofence.getCalendarId();
                    Calendar calendar = calendarId != 0 ? 
                            executeWithCircuitBreaker(() -> cacheManager.getObject(Calendar.class, calendarId)) : null;
                    if (calendar == null || calendar.checkMoment(position.getFixTime())) {
                        Event event = new Event(Event.TYPE_GEOFENCE_EXIT, position);
                        event.setGeofenceId(geofenceId);
                        
                        // Set correlation ID for distributed tracing
                        String correlationId = Span.current().getSpanContext().getTraceId();
                        event.set("correlationId", correlationId);
                        
                        // Notify callback and publish to message broker
                        callback.eventDetected(event);
                        publishEvent(event, correlationId);
                    }
                }
            } finally {
                exitSpan.end();
            }
        }
        
        // Process geofence enter events
        for (long geofenceId : newGeofences) {
            Span enterSpan = tracer.spanBuilder("geofence.enter.detect")
                    .setParent(Context.current().with(parentSpan))
                    .setAttribute("geofenceId", String.valueOf(geofenceId))
                    .startSpan();
            
            try (Scope scope = enterSpan.makeCurrent()) {
                Geofence geofence = executeWithCircuitBreaker(() -> cacheManager.getObject(Geofence.class, geofenceId));
                if (geofence != null) {
                    long calendarId = geofence.getCalendarId();
                    Calendar calendar = calendarId != 0 ? 
                            executeWithCircuitBreaker(() -> cacheManager.getObject(Calendar.class, calendarId)) : null;
                    if (calendar == null || calendar.checkMoment(position.getFixTime())) {
                        Event event = new Event(Event.TYPE_GEOFENCE_ENTER, position);
                        event.setGeofenceId(geofenceId);
                        
                        // Set correlation ID for distributed tracing
                        String correlationId = Span.current().getSpanContext().getTraceId();
                        event.set("correlationId", correlationId);
                        
                        // Notify callback and publish to message broker
                        callback.eventDetected(event);
                        publishEvent(event, correlationId);
                    }
                }
            } finally {
                enterSpan.end();
            }
        }
    }
    
    /**
     * Execute a function with circuit breaker protection
     */
    private <T> T executeWithCircuitBreaker(Supplier<T> supplier) {
        return circuitBreaker.executeSupplier(supplier);
    }
    
    /**
     * Publish an event to the message broker
     */
    private void publishEvent(Event event, String correlationId) {
        Span span = tracer.spanBuilder("geofence.event.publish")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("eventType", event.getType())
                .setAttribute("correlationId", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            messagePublisher.publish(EVENT_TOPIC, event, correlationId);
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.error("Failed to publish geofence event", e);
        } finally {
            span.end();
        }
    }
}