/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.extension.annotations.WithSpan;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.messaging.MessageConsumer;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Event;
import org.traccar.model.Position;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/**
 * Handler for detecting media events from position data.
 * Consumes position data from message broker and publishes media events.
 */
@Singleton
public class MediaEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MediaEventHandler.class);

    private final MessageProducer messageProducer;
    private final MessageConsumer messageConsumer;
    private final Tracer tracer;
    private final TextMapPropagator propagator;
    private final MeterRegistry meterRegistry;
    private final CircuitBreaker circuitBreaker;
    
    private final Counter mediaEventsCounter;
    private final Timer mediaEventProcessingTimer;

    /**
     * Constructs a new MediaEventHandler with required dependencies.
     *
     * @param messageProducer For publishing media events to the message broker
     * @param messageConsumer For consuming position data from the message broker
     * @param tracer For distributed tracing
     * @param propagator For propagating trace context
     * @param meterRegistry For metrics collection
     * @param circuitBreakerRegistry For circuit breaker configuration
     */
    @Inject
    public MediaEventHandler(
            MessageProducer messageProducer,
            MessageConsumer messageConsumer,
            Tracer tracer,
            TextMapPropagator propagator,
            MeterRegistry meterRegistry,
            CircuitBreakerRegistry circuitBreakerRegistry) {
        this.messageProducer = messageProducer;
        this.messageConsumer = messageConsumer;
        this.tracer = tracer;
        this.propagator = propagator;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.mediaEventsCounter = Counter.builder("media.events.total")
                .description("Total number of media events detected")
                .register(meterRegistry);
        
        this.mediaEventProcessingTimer = Timer.builder("media.events.processing.time")
                .description("Time taken to process media events")
                .register(meterRegistry);
        
        // Configure circuit breaker for graceful degradation
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(java.time.Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(
                "mediaEventHandler", circuitBreakerConfig);
        
        // Subscribe to position updates from message broker
        subscribeToPositionUpdates();
    }

    /**
     * Subscribe to position updates from the message broker.
     */
    private void subscribeToPositionUpdates() {
        messageConsumer.subscribe("position-updates", message -> {
            if (message.getPayload() instanceof Position) {
                Position position = (Position) message.getPayload();
                
                // Extract trace context from message headers
                Map<String, String> headers = message.getHeaders();
                Context extractedContext = propagator.extract(Context.current(), headers, new TextMapGetter<Map<String, String>>() {
                    @Override
                    public Iterable<String> keys(Map<String, String> carrier) {
                        return carrier.keySet();
                    }

                    @Override
                    public String get(Map<String, String> carrier, String key) {
                        return carrier.get(key);
                    }
                });
                
                // Process position with the extracted context
                try (Scope scope = extractedContext.makeCurrent()) {
                    processPosition(position, headers);
                }
            }
        });
    }

    /**
     * Process a position update and detect media events.
     *
     * @param position The position to process
     * @param headers Message headers containing correlation IDs
     */
    @WithSpan(value = "processPosition", kind = SpanKind.CONSUMER)
    private void processPosition(Position position, Map<String, String> headers) {
        Span span = Span.current();
        span.setAttribute(SemanticAttributes.MESSAGING_SYSTEM, "position-processing");
        span.setAttribute("position.deviceId", position.getDeviceId());
        
        try {
            // Use circuit breaker to handle service degradation
            circuitBreaker.executeRunnable(() -> {
                Timer.Sample sample = Timer.start(meterRegistry);
                
                try {
                    Stream.of(Position.KEY_IMAGE, Position.KEY_VIDEO, Position.KEY_AUDIO)
                            .filter(position::hasAttribute)
                            .map(type -> {
                                Event event = new Event(Event.TYPE_MEDIA, position);
                                event.set("media", type);
                                event.set("file", position.getString(type));
                                return event;
                            })
                            .forEach(event -> publishMediaEvent(event, headers));
                    
                    span.setStatus(StatusCode.OK);
                } catch (Exception e) {
                    LOGGER.error("Error processing position for media events", e);
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                } finally {
                    sample.stop(mediaEventProcessingTimer);
                }
            });
        } catch (Exception e) {
            LOGGER.warn("Circuit breaker prevented media event processing due to service degradation", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Service degraded: " + e.getMessage());
        }
    }

    /**
     * Publish a media event to the message broker.
     *
     * @param event The event to publish
     * @param headers Message headers containing correlation IDs
     */
    @WithSpan(value = "publishMediaEvent", kind = SpanKind.PRODUCER)
    private void publishMediaEvent(Event event, Map<String, String> headers) {
        Span span = Span.current();
        span.setAttribute(SemanticAttributes.MESSAGING_SYSTEM, "event-publishing");
        span.setAttribute("event.type", event.getType());
        span.setAttribute("event.deviceId", event.getDeviceId());
        
        // Create a new map for headers to avoid modifying the input map
        Map<String, String> eventHeaders = new HashMap<>(headers);
        
        // Inject the current context into the headers for propagation
        propagator.inject(Context.current(), eventHeaders, (carrier, key, value) -> carrier.put(key, value));
        
        try {
            messageProducer.publish("media-events", event, eventHeaders);
            mediaEventsCounter.increment();
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.error("Failed to publish media event", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        }
    }

    /**
     * Legacy method maintained for backward compatibility.
     * This method is now deprecated as position data is consumed from the message broker.
     */
    @Override
    @Deprecated
    public void onPosition(Position position, Callback callback) {
        // This method is kept for backward compatibility but should not be used in new code
        LOGGER.warn("Legacy onPosition method called. Use message broker instead.");
        
        // Create a span for this legacy call
        Span span = tracer.spanBuilder("legacyOnPosition")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("position.deviceId", position.getDeviceId());
            
            Timer.Sample sample = Timer.start(meterRegistry);
            
            Stream.of(Position.KEY_IMAGE, Position.KEY_VIDEO, Position.KEY_AUDIO)
                    .filter(position::hasAttribute)
                    .map(type -> {
                        Event event = new Event(Event.TYPE_MEDIA, position);
                        event.set("media", type);
                        event.set("file", position.getString(type));
                        return event;
                    })
                    .forEach(event -> {
                        callback.eventDetected(event);
                        mediaEventsCounter.increment();
                    });
            
            sample.stop(mediaEventProcessingTimer);
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.error("Error in legacy onPosition method", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }
}