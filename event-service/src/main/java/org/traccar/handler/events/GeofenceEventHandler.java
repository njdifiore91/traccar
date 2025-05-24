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
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.helper.model.PositionUtil;
import org.traccar.model.Calendar;
import org.traccar.model.Event;
import org.traccar.model.Geofence;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class GeofenceEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeofenceEventHandler.class);
    
    private static final String CIRCUIT_BREAKER_NAME = "geofenceEventHandler";
    private static final String RETRY_NAME = "geofenceEventHandler";
    
    private static final AttributeKey<String> CORRELATION_ID_KEY = AttributeKey.stringKey("correlation.id");
    private static final AttributeKey<String> DEVICE_ID_KEY = AttributeKey.stringKey("device.id");
    private static final AttributeKey<String> EVENT_TYPE_KEY = AttributeKey.stringKey("event.type");
    private static final AttributeKey<String> GEOFENCE_ID_KEY = AttributeKey.stringKey("geofence.id");
    
    private final CacheManager cacheManager;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Tracer tracer;
    private final Meter meter;
    
    private final LongCounter geofenceEnterCounter;
    private final LongCounter geofenceExitCounter;

    @Inject
    public GeofenceEventHandler(CacheManager cacheManager, Tracer tracer, Meter meter) {
        this.cacheManager = cacheManager;
        this.tracer = tracer;
        this.meter = meter;
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .slidingWindowSize(10)
                .permittedNumberOfCallsInHalfOpenState(5)
                .recordExceptions(Exception.class)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Initialize retry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(Exception.class)
                .enableExponentialBackoff(true)
                .exponentialBackoffMultiplier(2)
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry(RETRY_NAME);
        
        // Initialize metrics
        this.geofenceEnterCounter = meter.counterBuilder("geofence.enter.count")
                .setDescription("Number of geofence enter events detected")
                .build();
        
        this.geofenceExitCounter = meter.counterBuilder("geofence.exit.count")
                .setDescription("Number of geofence exit events detected")
                .build();
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Extract or generate correlation ID
        String correlationId = position.getString("correlationId");
        if (correlationId == null) {
            correlationId = java.util.UUID.randomUUID().toString();
        }
        
        // Create span for the geofence event detection process
        Span span = tracer.spanBuilder("geofence.event.detection")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(CORRELATION_ID_KEY, correlationId)
                .setAttribute(DEVICE_ID_KEY, String.valueOf(position.getDeviceId()))
                .startSpan();
        
        try (io.opentelemetry.context.Scope scope = span.makeCurrent()) {
            // Check if this is the latest position
            if (!isLatestPosition(position)) {
                span.addEvent("Position is not the latest, skipping");
                span.end();
                return;
            }
            
            // Process geofence events with circuit breaker and retry
            processGeofenceEvents(position, callback, correlationId, span);
            
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error processing geofence events: {}", e.getMessage(), e);
        } finally {
            span.end();
        }
    }
    
    private boolean isLatestPosition(Position position) {
        return Retry.decorateSupplier(retry, () -> 
            CircuitBreaker.decorateSupplier(circuitBreaker, () -> 
                PositionUtil.isLatest(cacheManager, position))
            .get())
        .get();
    }
    
    private void processGeofenceEvents(Position position, Callback callback, String correlationId, Span parentSpan) {
        Span span = tracer.spanBuilder("process.geofence.events")
                .setParent(Context.current().with(parentSpan))
                .setAttribute(CORRELATION_ID_KEY, correlationId)
                .setAttribute(DEVICE_ID_KEY, String.valueOf(position.getDeviceId()))
                .startSpan();
        
        try (io.opentelemetry.context.Scope scope = span.makeCurrent()) {
            // Get old geofences with circuit breaker and retry
            List<Long> oldGeofences = getOldGeofences(position);
            span.setAttribute("old.geofences.count", oldGeofences.size());
            
            // Get new geofences with circuit breaker and retry
            List<Long> newGeofences = getNewGeofences(position, oldGeofences);
            span.setAttribute("new.geofences.count", newGeofences.size());
            
            // Process exit events
            processExitEvents(position, callback, oldGeofences, correlationId, span);
            
            // Process enter events
            processEnterEvents(position, callback, newGeofences, correlationId, span);
            
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error processing geofence events with correlation ID {}: {}", 
                    correlationId, e.getMessage(), e);
        } finally {
            span.end();
        }
    }
    
    private List<Long> getOldGeofences(Position position) {
        return Retry.decorateSupplier(retry, () -> {
            return CircuitBreaker.decorateSupplier(circuitBreaker, () -> {
                List<Long> oldGeofences = new ArrayList<>();
                Position lastPosition = cacheManager.getPosition(position.getDeviceId());
                if (lastPosition != null && lastPosition.getGeofenceIds() != null) {
                    oldGeofences.addAll(lastPosition.getGeofenceIds());
                }
                return oldGeofences;
            }).get();
        }).get();
    }
    
    private List<Long> getNewGeofences(Position position, List<Long> oldGeofences) {
        return Retry.decorateSupplier(retry, () -> {
            return CircuitBreaker.decorateSupplier(circuitBreaker, () -> {
                List<Long> newGeofences = new ArrayList<>();
                if (position.getGeofenceIds() != null) {
                    newGeofences.addAll(position.getGeofenceIds());
                    newGeofences.removeAll(oldGeofences);
                    oldGeofences.removeAll(position.getGeofenceIds());
                }
                return newGeofences;
            }).get();
        }).get();
    }
    
    private void processExitEvents(Position position, Callback callback, List<Long> oldGeofences, 
                                  String correlationId, Span parentSpan) {
        for (long geofenceId : oldGeofences) {
            Span span = tracer.spanBuilder("process.geofence.exit")
                    .setParent(Context.current().with(parentSpan))
                    .setAttribute(CORRELATION_ID_KEY, correlationId)
                    .setAttribute(DEVICE_ID_KEY, String.valueOf(position.getDeviceId()))
                    .setAttribute(GEOFENCE_ID_KEY, String.valueOf(geofenceId))
                    .startSpan();
            
            try (io.opentelemetry.context.Scope scope = span.makeCurrent()) {
                Geofence geofence = getGeofence(geofenceId);
                if (geofence != null) {
                    long calendarId = geofence.getCalendarId();
                    Calendar calendar = calendarId != 0 ? getCalendar(calendarId) : null;
                    
                    if (calendar == null || checkCalendarMoment(calendar, position.getFixTime())) {
                        Event event = new Event(Event.TYPE_GEOFENCE_EXIT, position);
                        event.setGeofenceId(geofenceId);
                        event.set("correlationId", correlationId);
                        
                        span.setAttribute(EVENT_TYPE_KEY, Event.TYPE_GEOFENCE_EXIT);
                        span.addEvent("Geofence exit event detected");
                        
                        // Record metric
                        geofenceExitCounter.add(1, Attributes.of(
                                DEVICE_ID_KEY, String.valueOf(position.getDeviceId()),
                                GEOFENCE_ID_KEY, String.valueOf(geofenceId)));
                        
                        callback.eventDetected(event);
                    }
                }
            } catch (Exception e) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                LOGGER.error("Error processing geofence exit event with correlation ID {}: {}", 
                        correlationId, e.getMessage(), e);
            } finally {
                span.end();
            }
        }
    }
    
    private void processEnterEvents(Position position, Callback callback, List<Long> newGeofences, 
                                   String correlationId, Span parentSpan) {
        for (long geofenceId : newGeofences) {
            Span span = tracer.spanBuilder("process.geofence.enter")
                    .setParent(Context.current().with(parentSpan))
                    .setAttribute(CORRELATION_ID_KEY, correlationId)
                    .setAttribute(DEVICE_ID_KEY, String.valueOf(position.getDeviceId()))
                    .setAttribute(GEOFENCE_ID_KEY, String.valueOf(geofenceId))
                    .startSpan();
            
            try (io.opentelemetry.context.Scope scope = span.makeCurrent()) {
                long calendarId = getGeofenceCalendarId(geofenceId);
                Calendar calendar = calendarId != 0 ? getCalendar(calendarId) : null;
                
                if (calendar == null || checkCalendarMoment(calendar, position.getFixTime())) {
                    Event event = new Event(Event.TYPE_GEOFENCE_ENTER, position);
                    event.setGeofenceId(geofenceId);
                    event.set("correlationId", correlationId);
                    
                    span.setAttribute(EVENT_TYPE_KEY, Event.TYPE_GEOFENCE_ENTER);
                    span.addEvent("Geofence enter event detected");
                    
                    // Record metric
                    geofenceEnterCounter.add(1, Attributes.of(
                            DEVICE_ID_KEY, String.valueOf(position.getDeviceId()),
                            GEOFENCE_ID_KEY, String.valueOf(geofenceId)));
                    
                    callback.eventDetected(event);
                }
            } catch (Exception e) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                LOGGER.error("Error processing geofence enter event with correlation ID {}: {}", 
                        correlationId, e.getMessage(), e);
            } finally {
                span.end();
            }
        }
    }
    
    private Geofence getGeofence(long geofenceId) {
        return Retry.decorateSupplier(retry, () -> {
            return CircuitBreaker.decorateSupplier(circuitBreaker, () -> 
                cacheManager.getObject(Geofence.class, geofenceId))
            .get();
        }).get();
    }
    
    private long getGeofenceCalendarId(long geofenceId) {
        return Retry.decorateSupplier(retry, () -> {
            return CircuitBreaker.decorateSupplier(circuitBreaker, () -> {
                Geofence geofence = cacheManager.getObject(Geofence.class, geofenceId);
                return geofence != null ? geofence.getCalendarId() : 0;
            }).get();
        }).get();
    }
    
    private Calendar getCalendar(long calendarId) {
        return Retry.decorateSupplier(retry, () -> {
            return CircuitBreaker.decorateSupplier(circuitBreaker, () -> 
                cacheManager.getObject(Calendar.class, calendarId))
            .get();
        }).get();
    }
    
    private boolean checkCalendarMoment(Calendar calendar, long time) {
        return Retry.decorateSupplier(retry, () -> {
            return CircuitBreaker.decorateSupplier(circuitBreaker, () -> 
                calendar.checkMoment(time))
            .get();
        }).get();
    }
    
    // Fallback methods for circuit breaker
    private List<Long> getOldGeofencesFallback(Position position, Exception e) {
        LOGGER.warn("Circuit breaker open for getOldGeofences, using fallback", e);
        return Collections.emptyList();
    }
    
    private List<Long> getNewGeofencesFallback(Position position, List<Long> oldGeofences, Exception e) {
        LOGGER.warn("Circuit breaker open for getNewGeofences, using fallback", e);
        return Collections.emptyList();
    }
    
    private Geofence getGeofenceFallback(long geofenceId, Exception e) {
        LOGGER.warn("Circuit breaker open for getGeofence, using fallback", e);
        return null;
    }
    
    private Calendar getCalendarFallback(long calendarId, Exception e) {
        LOGGER.warn("Circuit breaker open for getCalendar, using fallback", e);
        return null;
    }
    
    private boolean checkCalendarMomentFallback(Calendar calendar, long time, Exception e) {
        LOGGER.warn("Circuit breaker open for checkCalendarMoment, using fallback", e);
        return false;
    }
}