/*
 * Copyright 2021 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import jakarta.inject.Inject;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.UnitsConverter;
import org.traccar.messaging.MessageConsumer;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Handler for detecting behavior events from position data.
 * Consumes position data from message broker and publishes detected events.
 */
public class BehaviorEventHandler {

    private final double accelerationThreshold;
    private final double brakingThreshold;

    private final CacheManager cacheManager;
    private final MessageProducer messageProducer;
    private final CircuitBreaker circuitBreaker;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final TextMapPropagator propagator;

    private final Timer detectionTimer;
    private final Counter accelerationEventCounter;
    private final Counter brakingEventCounter;

    /**
     * Constructor for BehaviorEventHandler.
     * 
     * @param config Configuration for behavior thresholds
     * @param cacheManager Cache manager for retrieving previous positions
     * @param messageProducer Producer for publishing detected events
     * @param meterRegistry Registry for metrics collection
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param propagator OpenTelemetry context propagator
     */
    @Inject
    public BehaviorEventHandler(
            Config config,
            CacheManager cacheManager,
            MessageProducer messageProducer,
            MeterRegistry meterRegistry,
            Tracer tracer,
            TextMapPropagator propagator) {
        
        this.accelerationThreshold = config.getDouble(Keys.EVENT_BEHAVIOR_ACCELERATION_THRESHOLD);
        this.brakingThreshold = config.getDouble(Keys.EVENT_BEHAVIOR_BRAKING_THRESHOLD);
        this.cacheManager = cacheManager;
        this.messageProducer = messageProducer;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        this.propagator = propagator;

        // Initialize circuit breaker for cache manager interactions
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(3)
                .slidingWindowSize(10)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("cacheManager");

        // Initialize metrics
        this.detectionTimer = Timer.builder("behavior.event.detection.time")
                .description("Time taken to detect behavior events")
                .register(meterRegistry);
        
        this.accelerationEventCounter = Counter.builder("behavior.event.acceleration")
                .description("Number of acceleration events detected")
                .register(meterRegistry);
        
        this.brakingEventCounter = Counter.builder("behavior.event.braking")
                .description("Number of braking events detected")
                .register(meterRegistry);

        // Subscribe to position messages
        subscribeToPositionMessages();
    }

    /**
     * Subscribe to position messages from the message broker.
     */
    private void subscribeToPositionMessages() {
        // Implementation would depend on the specific message broker being used
        // This is a placeholder for the actual implementation
    }

    /**
     * Process a position message received from the message broker.
     * 
     * @param position The position to process
     * @param headers Message headers containing trace context
     */
    public void processPositionMessage(Position position, Map<String, String> headers) {
        // Extract trace context from message headers
        Context context = propagator.extract(Context.current(), headers, new TextMapGetter<Map<String, String>>() {
            @Override
            public Iterable<String> keys(Map<String, String> carrier) {
                return carrier.keySet();
            }

            @Override
            public String get(Map<String, String> carrier, String key) {
                return carrier.get(key);
            }
        });

        // Create a span for behavior event detection
        Span span = tracer.spanBuilder("behavior.event.detection")
                .setParent(context)
                .setSpanKind(SpanKind.CONSUMER)
                .startSpan();

        try {
            // Use the span's context for the detection operation
            io.opentelemetry.context.Scope scope = span.makeCurrent();
            try {
                // Record metrics for detection time
                detectionTimer.record(() -> detectBehaviorEvents(position));
            } finally {
                scope.close();
            }
        } finally {
            span.end();
        }
    }

    /**
     * Detect behavior events from a position.
     * 
     * @param position The position to check for behavior events
     */
    private void detectBehaviorEvents(Position position) {
        // Get previous position with circuit breaker protection
        Position lastPosition = getLastPositionWithCircuitBreaker(position.getDeviceId());
        
        if (lastPosition != null && !position.getFixTime().equals(lastPosition.getFixTime())) {
            // Add attributes to the current span
            Span span = Span.current();
            span.setAttribute("deviceId", String.valueOf(position.getDeviceId()));
            span.setAttribute("fixTime", position.getFixTime().toString());
            
            // Calculate acceleration
            double acceleration = UnitsConverter.mpsFromKnots(position.getSpeed() - lastPosition.getSpeed()) * 1000
                    / (position.getFixTime().getTime() - lastPosition.getFixTime().getTime());
            
            span.setAttribute("acceleration", acceleration);
            
            // Check for acceleration event
            if (accelerationThreshold != 0 && acceleration >= accelerationThreshold) {
                Event event = new Event(Event.TYPE_ALARM, position);
                event.set(Position.KEY_ALARM, Position.ALARM_ACCELERATION);
                publishEvent(event);
                accelerationEventCounter.increment();
                span.setAttribute("eventType", "acceleration");
            } 
            // Check for braking event
            else if (brakingThreshold != 0 && acceleration <= -brakingThreshold) {
                Event event = new Event(Event.TYPE_ALARM, position);
                event.set(Position.KEY_ALARM, Position.ALARM_BRAKING);
                publishEvent(event);
                brakingEventCounter.increment();
                span.setAttribute("eventType", "braking");
            }
        }
    }

    /**
     * Get the last position for a device with circuit breaker protection.
     * 
     * @param deviceId The device ID
     * @return The last position or null if not available
     */
    private Position getLastPositionWithCircuitBreaker(long deviceId) {
        Supplier<Position> positionSupplier = () -> cacheManager.getPosition(deviceId);
        return circuitBreaker.executeSupplier(positionSupplier);
    }

    /**
     * Publish an event to the message broker.
     * 
     * @param event The event to publish
     */
    private void publishEvent(Event event) {
        // Create a span for event publishing
        Span span = tracer.spanBuilder("behavior.event.publish")
                .setSpanKind(SpanKind.PRODUCER)
                .startSpan();
        
        try {
            io.opentelemetry.context.Scope scope = span.makeCurrent();
            try {
                // Add the current span context to the event for propagation
                // Implementation would depend on the specific message broker being used
                // This is a placeholder for the actual implementation
                messageProducer.publishEvent(event);
            } finally {
                scope.close();
            }
        } finally {
            span.end();
        }
    }
}