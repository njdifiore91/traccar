/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.messaging;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.handler.events.BaseEventHandler;
import org.traccar.model.Event;
import org.traccar.model.Position;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Orchestrates the consumption of positions, processing through event handlers, and production of events.
 * This service initializes and manages the event handler pipeline, coordinates the flow of data from the
 * PositionConsumer through the event handlers to the EventProducer, and handles error conditions and retries.
 * It also manages the lifecycle of message processing components and implements graceful shutdown.
 */
@Singleton
public class EventProcessingService implements BaseEventHandler.Callback {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessingService.class);
    private static final String CIRCUIT_BREAKER_NAME = "eventProcessing";
    private static final String RETRY_NAME = "eventProcessing";
    private static final String TRACER_NAME = "event-processing-service";

    private final PositionConsumer positionConsumer;
    private final EventProducer eventProducer;
    private final List<BaseEventHandler> eventHandlers;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Tracer tracer;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    /**
     * Constructs the EventProcessingService with required dependencies.
     *
     * @param positionConsumer Consumer for position messages from the broker
     * @param eventProducer Producer for event messages to the broker
     * @param eventHandlers List of event handlers to process positions
     * @param circuitBreakerRegistry Registry for circuit breakers
     * @param retryRegistry Registry for retry configurations
     * @param tracer OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public EventProcessingService(
            PositionConsumer positionConsumer,
            EventProducer eventProducer,
            List<BaseEventHandler> eventHandlers,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry,
            Tracer tracer) {
        this.positionConsumer = positionConsumer;
        this.eventProducer = eventProducer;
        this.eventHandlers = eventHandlers;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        this.retry = retryRegistry.retry(RETRY_NAME);
        this.tracer = tracer;

        LOGGER.info("EventProcessingService initialized with {} event handlers", eventHandlers.size());
    }

    /**
     * Initializes the service and starts position consumption.
     * This method is called automatically after dependency injection is complete.
     */
    @PostConstruct
    public void init() {
        LOGGER.info("Starting EventProcessingService");
        positionConsumer.setPositionHandler(this::processPosition);
        positionConsumer.start();
        LOGGER.info("EventProcessingService started successfully");
    }

    /**
     * Gracefully shuts down the service, ensuring all in-flight messages are processed.
     * This method is called automatically before the container is destroyed.
     */
    @PreDestroy
    public void shutdown() {
        LOGGER.info("Shutting down EventProcessingService");
        running.set(false);

        try {
            // Allow time for in-flight messages to be processed
            boolean completed = shutdownLatch.await(30, TimeUnit.SECONDS);
            if (!completed) {
                LOGGER.warn("Shutdown timed out, some messages may not have been processed");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Shutdown interrupted", e);
        }

        positionConsumer.stop();
        LOGGER.info("EventProcessingService shutdown complete");
    }

    /**
     * Processes a position through all event handlers.
     * This method is called by the PositionConsumer when a new position is received.
     *
     * @param position The position to process
     * @param correlationId The correlation ID for distributed tracing
     * @return A CompletableFuture that completes when processing is done
     */
    public CompletableFuture<Void> processPosition(Position position, String correlationId) {
        if (!running.get()) {
            LOGGER.debug("Service is shutting down, skipping position processing");
            return CompletableFuture.completedFuture(null);
        }

        Span span = tracer.spanBuilder("process_position")
                .setSpanKind(SpanKind.CONSUMER)
                .setParent(Context.current().with(Span.current()))
                .setAttribute("deviceId", position.getDeviceId())
                .setAttribute("correlationId", correlationId)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            LOGGER.debug("Processing position for device {}, correlation ID: {}", 
                    position.getDeviceId(), correlationId);

            return circuitBreaker.executeCompletionStage(() -> {
                CompletableFuture<Void> future = new CompletableFuture<>();
                try {
                    // Process the position through all event handlers
                    for (BaseEventHandler handler : eventHandlers) {
                        Span handlerSpan = tracer.spanBuilder("event_handler_" + handler.getClass().getSimpleName())
                                .setParent(Context.current())
                                .startSpan();
                        try (Scope handlerScope = handlerSpan.makeCurrent()) {
                            handler.analyzePosition(position, this);
                        } catch (Exception e) {
                            handlerSpan.recordException(e);
                            LOGGER.warn("Error in event handler {}", handler.getClass().getSimpleName(), e);
                        } finally {
                            handlerSpan.end();
                        }
                    }
                    future.complete(null);
                } catch (Exception e) {
                    span.recordException(e);
                    LOGGER.error("Error processing position", e);
                    future.completeExceptionally(e);
                }
                return future;
            }).toCompletableFuture();
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.error("Circuit breaker error processing position", e);
            return CompletableFuture.failedFuture(e);
        } finally {
            span.end();
        }
    }

    /**
     * Callback method invoked by event handlers when an event is detected.
     * Implements the BaseEventHandler.Callback interface.
     *
     * @param event The detected event
     */
    @Override
    public void eventDetected(Event event) {
        if (!running.get()) {
            LOGGER.debug("Service is shutting down, skipping event publishing");
            return;
        }

        Span parentSpan = Span.current();
        Span span = tracer.spanBuilder("publish_event")
                .setSpanKind(SpanKind.PRODUCER)
                .setParent(Context.current())
                .setAttribute("eventType", event.getType())
                .setAttribute("deviceId", event.getDeviceId())
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            LOGGER.debug("Event detected: {} for device {}", event.getType(), event.getDeviceId());

            // Use retry pattern for publishing events
            Retry.decorateCompletionStage(
                retry,
                () -> eventProducer.publishEvent(event, span.getSpanContext().getTraceId())
            ).whenComplete((result, error) -> {
                if (error != null) {
                    span.recordException(error);
                    LOGGER.error("Failed to publish event after retries", error);
                } else {
                    LOGGER.debug("Event published successfully: {} for device {}", 
                            event.getType(), event.getDeviceId());
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.error("Error publishing event", e);
        } finally {
            span.end();
        }
    }

    /**
     * Signals that all in-flight messages have been processed.
     * This method should be called by the PositionConsumer when it has no more messages to process.
     */
    public void signalProcessingComplete() {
        if (!running.get()) {
            shutdownLatch.countDown();
        }
    }
    
    /**
     * Returns the current health status of the service.
     * This can be used by health check endpoints for orchestration platforms.
     *
     * @return true if the service is healthy, false otherwise
     */
    public boolean isHealthy() {
        return running.get() && 
               circuitBreaker.getState() != CircuitBreaker.State.OPEN &&
               positionConsumer.isHealthy() && 
               eventProducer.isHealthy();
    }
    
    /**
     * Returns the list of event handlers managed by this service.
     * This can be used for diagnostics and monitoring.
     *
     * @return The list of event handlers
     */
    public List<BaseEventHandler> getEventHandlers() {
        return eventHandlers;
    }
}