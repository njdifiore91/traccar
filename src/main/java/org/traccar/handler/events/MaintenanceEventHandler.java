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

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
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
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.Event;
import org.traccar.model.Maintenance;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.concurrent.TimeUnit;

public class MaintenanceEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MaintenanceEventHandler.class);
    private static final String CIRCUIT_BREAKER_NAME = "maintenanceEventHandler";
    
    private final CacheManager cacheManager;
    private final MessagePublisher messagePublisher;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    
    private final Timer maintenanceDetectionTimer;

    @Inject
    public MaintenanceEventHandler(
            CacheManager cacheManager,
            MessagePublisher messagePublisher,
            MeterRegistry meterRegistry,
            Tracer tracer) {
        this.cacheManager = cacheManager;
        this.messagePublisher = messagePublisher;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        
        // Initialize metrics
        this.maintenanceDetectionTimer = Timer.builder("maintenance.detection.duration")
                .description("Time taken to detect maintenance events")
                .register(meterRegistry);
    }

    /**
     * Process a position message received from the message broker.
     * This method is called when a new position is received from the message broker.
     * 
     * @param position The position to process
     * @param correlationId The correlation ID for distributed tracing
     */
    public void processPosition(Position position, String correlationId) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("maintenance.process.position")
                .setSpanKind(SpanKind.CONSUMER)
                .setParent(Context.current().with(Span.current()))
                .setAttribute("deviceId", position.getDeviceId())
                .setAttribute("correlationId", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Use timer to measure performance
            maintenanceDetectionTimer.record(() -> {
                detectMaintenanceEvents(position, correlationId);
            });
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.error("Error processing position for maintenance events: {}", e.getMessage(), e);
        } finally {
            span.end();
        }
    }

    /**
     * Detect maintenance events based on the position data.
     * Uses circuit breaker pattern for CacheManager interactions.
     * 
     * @param position The position to check for maintenance events
     * @param correlationId The correlation ID for distributed tracing
     */
    @CircuitBreaker(name = CIRCUIT_BREAKER_NAME, fallbackMethod = "fallbackDetectMaintenanceEvents")
    private void detectMaintenanceEvents(Position position, String correlationId) {
        Position lastPosition = getLastPosition(position.getDeviceId());
        if (lastPosition == null || position.getFixTime().compareTo(lastPosition.getFixTime()) < 0) {
            return;
        }

        for (Maintenance maintenance : getDeviceMaintenances(position.getDeviceId())) {
            if (maintenance.getPeriod() != 0) {
                double oldValue = getValue(lastPosition, maintenance.getType());
                double newValue = getValue(position, maintenance.getType());
                if (oldValue != 0.0 && newValue != 0.0 && newValue >= maintenance.getStart()) {
                    if (oldValue < maintenance.getStart()
                        || (long) ((oldValue - maintenance.getStart()) / maintenance.getPeriod())
                        < (long) ((newValue - maintenance.getStart()) / maintenance.getPeriod())) {
                        
                        // Create maintenance event
                        Event event = new Event(Event.TYPE_MAINTENANCE, position);
                        event.setMaintenanceId(maintenance.getId());
                        event.set(maintenance.getType(), newValue);
                        
                        // Publish event to message broker
                        publishMaintenanceEvent(event, correlationId);
                    }
                }
            }
        }
    }
    
    /**
     * Fallback method for the circuit breaker.
     * Called when the circuit breaker is open or an exception occurs.
     * 
     * @param position The position being processed
     * @param correlationId The correlation ID for distributed tracing
     * @param e The exception that triggered the fallback
     */
    private void fallbackDetectMaintenanceEvents(Position position, String correlationId, Exception e) {
        LOGGER.warn("Circuit breaker triggered for maintenance event detection. Using fallback. Error: {}", 
                e.getMessage());
        
        // Record metric for circuit breaker activation
        meterRegistry.counter("maintenance.circuitbreaker.fallback").increment();
        
        // We can't process maintenance events without cache access, so we just log the issue
        Span.current().addEvent("maintenance_detection_fallback");
    }
    
    /**
     * Get the last position for a device with circuit breaker protection.
     * 
     * @param deviceId The device ID
     * @return The last position or null if not available
     */
    @CircuitBreaker(name = CIRCUIT_BREAKER_NAME)
    private Position getLastPosition(long deviceId) {
        return cacheManager.getPosition(deviceId);
    }
    
    /**
     * Get maintenance objects for a device with circuit breaker protection.
     * 
     * @param deviceId The device ID
     * @return Iterable of maintenance objects
     */
    @CircuitBreaker(name = CIRCUIT_BREAKER_NAME)
    private Iterable<Maintenance> getDeviceMaintenances(long deviceId) {
        return cacheManager.getDeviceObjects(deviceId, Maintenance.class);
    }
    
    /**
     * Publish a maintenance event to the message broker.
     * 
     * @param event The event to publish
     * @param correlationId The correlation ID for distributed tracing
     */
    private void publishMaintenanceEvent(Event event, String correlationId) {
        Span span = tracer.spanBuilder("maintenance.publish.event")
                .setSpanKind(SpanKind.PRODUCER)
                .setParent(Context.current().with(Span.current()))
                .setAttribute("eventType", event.getType())
                .setAttribute("deviceId", event.getDeviceId())
                .setAttribute("correlationId", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Record metric for maintenance events detected
            meterRegistry.counter("maintenance.events.detected").increment();
            
            // Publish to message broker with the correlation ID for tracing
            messagePublisher.publishEvent(event, correlationId);
            
            LOGGER.debug("Published maintenance event for device {}: {}", 
                    event.getDeviceId(), event.getMaintenanceId());
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.error("Failed to publish maintenance event: {}", e.getMessage(), e);
            
            // Record metric for publishing failures
            meterRegistry.counter("maintenance.events.publish.failures").increment();
        } finally {
            span.end();
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