/*
 * Copyright 2017 - 2024 Anton Tananaev (anton@traccar.org)
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
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.function.Consumer;

public class DriverEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DriverEventHandler.class);

    private final CacheManager cacheManager;
    private final CircuitBreaker circuitBreaker;
    private final MessageProducer messageProducer;
    private final Timer eventProcessingTimer;
    private final Tracer tracer;

    @Inject
    public DriverEventHandler(
            CacheManager cacheManager,
            CircuitBreakerRegistry circuitBreakerRegistry,
            MessageProducer messageProducer,
            MeterRegistry meterRegistry,
            Tracer tracer) {
        this.cacheManager = cacheManager;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("cacheManager");
        this.messageProducer = messageProducer;
        this.eventProcessingTimer = meterRegistry.timer("driver.event.processing");
        this.tracer = tracer;
    }

    /**
     * Process position data from message broker and detect driver change events
     * 
     * @param position Position data received from message broker
     * @param correlationId Correlation ID for distributed tracing
     */
    public void processPosition(Position position, String correlationId) {
        Span span = tracer.spanBuilder("DriverEventHandler.processPosition")
                .setSpanKind(SpanKind.CONSUMER)
                .setParent(Context.current().with(Span.current()))
                .setAttribute("position.deviceId", position.getDeviceId())
                .setAttribute("correlationId", correlationId)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            eventProcessingTimer.record(() -> {
                try {
                    // Use circuit breaker to protect against CacheManager failures
                    boolean isLatest = circuitBreaker.executeSupplier(() -> 
                        PositionUtil.isLatest(cacheManager, position));
                    
                    if (!isLatest) {
                        LOGGER.debug("Ignoring outdated position for device {}", position.getDeviceId());
                        return;
                    }
                    
                    String driverUniqueId = position.getString(Position.KEY_DRIVER_UNIQUE_ID);
                    if (driverUniqueId != null) {
                        String oldDriverUniqueId = null;
                        
                        // Use circuit breaker to protect against CacheManager failures
                        Position lastPosition = circuitBreaker.executeSupplier(() -> 
                            cacheManager.getPosition(position.getDeviceId()));
                            
                        if (lastPosition != null) {
                            oldDriverUniqueId = lastPosition.getString(Position.KEY_DRIVER_UNIQUE_ID);
                        }
                        
                        if (!driverUniqueId.equals(oldDriverUniqueId)) {
                            LOGGER.info("Driver changed for device {}: {} -> {}", 
                                position.getDeviceId(), oldDriverUniqueId, driverUniqueId);
                                
                            Event event = new Event(Event.TYPE_DRIVER_CHANGED, position);
                            event.set(Position.KEY_DRIVER_UNIQUE_ID, driverUniqueId);
                            
                            // Publish driver change event to message broker
                            publishEvent(event, correlationId);
                        }
                    }
                } catch (Exception e) {
                    span.recordException(e);
                    LOGGER.warn("Error processing driver event for device {}: {}", 
                        position.getDeviceId(), e.getMessage());
                }
            });
        } finally {
            span.end();
        }
    }
    
    /**
     * Publish driver change event to message broker
     * 
     * @param event The driver change event to publish
     * @param correlationId Correlation ID for distributed tracing
     */
    private void publishEvent(Event event, String correlationId) {
        Span span = tracer.spanBuilder("DriverEventHandler.publishEvent")
                .setSpanKind(SpanKind.PRODUCER)
                .setParent(Context.current())
                .setAttribute("event.type", event.getType())
                .setAttribute("event.deviceId", event.getDeviceId())
                .setAttribute("correlationId", correlationId)
                .startSpan();
                
        try (Scope scope = span.makeCurrent()) {
            messageProducer.publish("events", event, headers -> {
                // Propagate correlation ID for distributed tracing
                headers.put("correlationId", correlationId);
            });
            span.addEvent("Event published successfully");
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.error("Failed to publish driver change event: {}", e.getMessage());
        } finally {
            span.end();
        }
    }
}