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
package org.traccar.handler.events;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.messaging.EventProducer;
import org.traccar.model.Event;
import org.traccar.model.Position;

import jakarta.inject.Inject;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Base class for all event handlers.
 * 
 * This abstract class provides common functionality for event detection and processing,
 * including distributed tracing, metrics collection, and asynchronous event publishing
 * to the message broker.
 * 
 * Key features:
 * - OpenTelemetry instrumentation for distributed tracing
 * - Micrometer metrics for performance monitoring
 * - Asynchronous event processing with CompletableFuture
 * - Correlation ID propagation across service boundaries
 * - Message broker integration for event publishing
 */
public abstract class BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseEventHandler.class);
    
    private static final AttributeKey<String> CORRELATION_ID_KEY = AttributeKey.stringKey("correlationId");
    
    protected Tracer tracer;
    protected MeterRegistry meterRegistry;
    protected EventProducer eventProducer;
    
    /**
     * Constructs a BaseEventHandler with dependencies.
     *
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Registry for metrics collection
     * @param eventProducer Producer for publishing events to message broker
     */
    @Inject
    public BaseEventHandler(Tracer tracer, MeterRegistry meterRegistry, EventProducer eventProducer) {
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.eventProducer = eventProducer;
    }
    
    /**
     * Analyzes a position to detect events.
     * 
     * This method is instrumented with OpenTelemetry for distributed tracing and
     * metrics collection for performance monitoring.
     *
     * @param position the position to analyze
     * @param correlationId the correlation ID for cross-service tracking
     * @return CompletableFuture with the event if detected, or null if no event
     */
    public CompletableFuture<Event> analyzePosition(Position position, String correlationId) {
        // Create a span for this operation
        Span span = tracer.spanBuilder(getEventHandlerName() + ".detect")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(CORRELATION_ID_KEY, correlationId)
                .setAttribute(AttributeKey.longKey("deviceId"), position.getDeviceId())
                .startSpan();
        
        // Use a timer to measure processing time
        Timer timer = meterRegistry.timer(getEventHandlerName() + ".processing.time");
        long startTime = System.nanoTime();
        
        CompletableFuture<Event> future = new CompletableFuture<>();
        try {
            try (Scope scope = span.makeCurrent()) {
                // Add position attributes to the span for context
                span.setAttribute(AttributeKey.longKey("position.time"), position.getFixTime().getTime());
                
                detectEvent(position, span, correlationId)
                    .thenAccept(event -> {
                        timer.record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                        future.complete(event);
                    })
                    .exceptionally(e -> {
                        timer.record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                        span.recordException(e);
                        span.setStatus(StatusCode.ERROR, e.getMessage());
                        LOGGER.error("Error detecting event for device {}: {}", 
                                position.getDeviceId(), e.getMessage(), e);
                        future.complete(null);
                        return null;
                    });
            } catch (Exception e) {
                timer.record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                LOGGER.error("Error detecting event for device {}: {}", 
                        position.getDeviceId(), e.getMessage(), e);
                future.complete(null);
            } finally {
                span.end();
            }
            
            return future;
        }
    }

    /**
     * Core logic for event detection to be implemented by subclasses.
     *
     * @param position the position to analyze
     * @param span the current tracing span
     * @param correlationId the correlation ID for cross-service tracking
     * @return CompletableFuture with the event if detected, or null if no event
     */
    protected abstract CompletableFuture<Event> detectEvent(Position position, Span span, String correlationId);
    
    /**
     * Returns the name of the event handler for use in metrics and tracing.
     * 
     * @return the name of the event handler
     */
    protected abstract String getEventHandlerName();
    
    /**
     * Processes a position message from the message broker.
     * This is the main entry point for handlers when receiving position data.
     *
     * @param position the position to process
     * @param correlationId the correlation ID for cross-service tracking
     * @return CompletableFuture with the event if detected, or null if no event
     */
    public CompletableFuture<Event> processPositionMessage(Position position, String correlationId) {
        Span rootSpan = tracer.spanBuilder("process.position.message")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(CORRELATION_ID_KEY, correlationId)
                .setAttribute(AttributeKey.longKey("deviceId"), position.getDeviceId())
                .setAttribute(AttributeKey.stringKey("handlerName"), getEventHandlerName())
                .startSpan();
        
        try (Scope scope = rootSpan.makeCurrent()) {
            return analyzePosition(position, correlationId)
                    .thenCompose(event -> {
                        if (event != null) {
                            return publishEvent(event, correlationId);
                        }
                        return CompletableFuture.completedFuture(null);
                    })
                    .exceptionally(e -> {
                        rootSpan.recordException(e);
                        rootSpan.setStatus(StatusCode.ERROR, e.getMessage());
                        LOGGER.error("Unhandled exception in event detection: {}", e.getMessage(), e);
                        return null;
                    });
        } finally {
            rootSpan.end();
        }
    }
    
    /**
     * Publishes an event to the message broker.
     * 
     * @param event the event to publish
     * @param correlationId the correlation ID for cross-service tracking
     * @return CompletableFuture with the published event
     */
    protected CompletableFuture<Event> publishEvent(Event event, String correlationId) {
        Span span = tracer.spanBuilder("publish.event")
                .setParent(Context.current())
                .setAttribute(CORRELATION_ID_KEY, correlationId)
                .setAttribute(AttributeKey.stringKey("event.type"), event.getType())
                .setAttribute(AttributeKey.longKey("event.deviceId"), event.getDeviceId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Ensure the correlation ID is set on the event
            event.set(Event.KEY_CORRELATION_ID, correlationId);
            
            // Publish the event to the message broker
            return eventProducer.publishEvent(event, correlationId)
                    .thenApply(publishedEvent -> {
                        span.addEvent("Event published successfully");
                        return publishedEvent;
                    })
                    .exceptionally(e -> {
                        span.recordException(e);
                        span.setStatus(StatusCode.ERROR, "Failed to publish event: " + e.getMessage());
                        LOGGER.error("Failed to publish event: {}", e.getMessage(), e);
                        return null;
                    });
        } finally {
            span.end();
        }
    }
    

}