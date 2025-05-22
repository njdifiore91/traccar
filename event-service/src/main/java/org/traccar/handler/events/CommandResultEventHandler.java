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
import org.traccar.messaging.EventProducer;
import org.traccar.model.Event;
import org.traccar.model.Position;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Event handler for command result events.
 * <p>
 * This handler detects command result events from position data received via the message broker,
 * and publishes them to the events topic for further processing by the Notification Service.
 * <p>
 * It includes distributed tracing with OpenTelemetry, metrics collection with Micrometer,
 * and circuit breaker pattern with Resilience4j for graceful degradation during service unavailability.
 */
public class CommandResultEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandResultEventHandler.class);

    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final EventProducer eventProducer;
    private final CircuitBreaker circuitBreaker;

    private final Timer processingTimer;
    private final Counter commandResultEventsCounter;
    private final Counter failedEventsCounter;

    /**
     * Constructs a new CommandResultEventHandler with the necessary dependencies.
     *
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     * @param eventProducer Producer for publishing events to the message broker
     */
    @Inject
    public CommandResultEventHandler(Tracer tracer, MeterRegistry meterRegistry, EventProducer eventProducer) {
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.eventProducer = eventProducer;

        // Initialize metrics
        this.processingTimer = Timer.builder("command.result.processing.time")
                .description("Time taken to process command result events")
                .register(meterRegistry);
        
        this.commandResultEventsCounter = Counter.builder("command.result.events")
                .description("Number of command result events detected")
                .register(meterRegistry);
        
        this.failedEventsCounter = Counter.builder("command.result.events.failed")
                .description("Number of failed command result event processing attempts")
                .register(meterRegistry);

        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("commandResultEventHandler");
        
        // Register event consumer for circuit breaker state transitions
        this.circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker state changed: {}", event));
    }

    /**
     * Processes a position to detect command result events.
     * <p>
     * This method is called by the EventProcessingService when a position is received from the message broker.
     * It checks if the position contains a command result attribute, and if so, creates and publishes an event.
     *
     * @param position The position to process
     * @param callback The callback to invoke when an event is detected
     */
    @Override
    public void onPosition(Position position, Callback callback) {
        // Extract correlation ID from position for distributed tracing
        String correlationId = position.getAttributes().containsKey("correlationId") 
                ? (String) position.getAttributes().get("correlationId") 
                : "unknown";

        // Create a span for this operation
        Span span = tracer.spanBuilder("CommandResultEventHandler.onPosition")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("correlationId", correlationId)
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .startSpan();

        // Attach the span to the current context
        try (Scope scope = span.makeCurrent()) {
            // Use the timer to measure processing time
            processingTimer.record(() -> {
                try {
                    // Use circuit breaker to protect against failures
                    Supplier<Void> processPositionSupplier = () -> {
                        processPosition(position, callback, span, correlationId);
                        return null;
                    };

                    // Execute with circuit breaker
                    CircuitBreaker.decorateSupplier(circuitBreaker, processPositionSupplier).get();
                } catch (Exception e) {
                    failedEventsCounter.increment();
                    span.setStatus(StatusCode.ERROR);
                    span.recordException(e);
                    LOGGER.error("Failed to process command result event: {}", e.getMessage(), e);
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Internal method to process a position and detect command result events.
     *
     * @param position The position to process
     * @param callback The callback to invoke when an event is detected
     * @param span The current span for tracing
     * @param correlationId The correlation ID for distributed tracing
     */
    private void processPosition(Position position, Callback callback, Span span, String correlationId) {
        Object commandResult = position.getAttributes().get(Position.KEY_RESULT);
        if (commandResult != null) {
            span.addEvent("Command result found");
            span.setAttribute("command.result", commandResult.toString());

            // Create event with the correlation ID for tracing
            Event event = new Event(Event.TYPE_COMMAND_RESULT, position);
            event.set(Position.KEY_RESULT, (String) commandResult);
            
            // Add correlation ID to event attributes for tracing across services
            event.set("correlationId", correlationId);

            // Increment counter for detected events
            commandResultEventsCounter.increment();

            // Invoke callback which will publish the event to the message broker
            callback.eventDetected(event);

            span.addEvent("Event published");
        }
    }
}