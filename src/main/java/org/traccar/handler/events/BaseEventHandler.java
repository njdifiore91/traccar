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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.Event;
import org.traccar.model.Position;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Base class for all event handlers that provides common functionality for event processing.
 * Supports both synchronous and asynchronous event processing with distributed tracing and metrics.
 */
public abstract class BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseEventHandler.class);
    
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final MessagePublisher messagePublisher;
    private final boolean asyncProcessingEnabled;
    
    /**
     * Constructor for BaseEventHandler with required dependencies.
     * 
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     * @param messagePublisher Message broker publisher for async processing
     * @param asyncProcessingEnabled Flag to enable/disable async processing
     */
    protected BaseEventHandler(Tracer tracer, MeterRegistry meterRegistry, 
                             MessagePublisher messagePublisher, boolean asyncProcessingEnabled) {
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.messagePublisher = messagePublisher;
        this.asyncProcessingEnabled = asyncProcessingEnabled;
    }

    /**
     * Callback interface for event detection.
     */
    public interface Callback {
        /**
         * Called when an event is detected.
         * 
         * @param event The detected event
         */
        void eventDetected(Event event);
    }

    /**
     * Analyzes a position for potential events with distributed tracing and metrics.
     * 
     * @param position Position to analyze
     * @param callback Callback to invoke when an event is detected
     * @param correlationId Optional correlation ID for distributed tracing (can be null)
     */
    public void analyzePosition(Position position, Callback callback, String correlationId) {
        String traceId = correlationId != null ? correlationId : UUID.randomUUID().toString();
        
        Span span = tracer.spanBuilder("event.analyze." + getClass().getSimpleName())
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("position.id", position.getId())
                .setAttribute("position.deviceId", position.getDeviceId())
                .setAttribute("correlation.id", traceId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Timer.Sample sample = Timer.start(meterRegistry);
            
            try {
                if (asyncProcessingEnabled) {
                    // Asynchronous processing via message broker
                    processPositionAsync(position, callback, traceId);
                } else {
                    // Direct synchronous processing
                    onPosition(position, callback);
                }
                
                span.setStatus(StatusCode.OK);
            } catch (RuntimeException e) {
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.recordException(e);
                LOGGER.warn("Event handler failed", e);
                
                // Record error metric
                meterRegistry.counter("event.handler.error", 
                        "handler", getClass().getSimpleName(),
                        "error", e.getClass().getSimpleName()).increment();
            } finally {
                // Record execution time metric
                sample.stop(Timer.builder("event.handler.execution")
                        .tag("handler", getClass().getSimpleName())
                        .register(meterRegistry));
            }
        } finally {
            span.end();
        }
    }
    
    /**
     * Overloaded method for backward compatibility.
     * 
     * @param position Position to analyze
     * @param callback Callback to invoke when an event is detected
     */
    public void analyzePosition(Position position, Callback callback) {
        analyzePosition(position, callback, null);
    }

    /**
     * Process position asynchronously via message broker.
     * 
     * @param position Position to analyze
     * @param callback Callback to invoke when an event is detected
     * @param correlationId Correlation ID for distributed tracing
     * @return CompletableFuture representing the async operation
     */
    protected CompletableFuture<Void> processPositionAsync(Position position, Callback callback, String correlationId) {
        // Create a wrapper callback that will be used when the message is consumed
        AsyncEventCallback asyncCallback = new AsyncEventCallback(callback, correlationId);
        
        // Publish position to message broker for async processing
        // The actual implementation will depend on the message broker being used (Kafka/RabbitMQ)
        return messagePublisher.publishPositionForEventProcessing(position, correlationId)
                .exceptionally(ex -> {
                    LOGGER.error("Failed to publish position for async event processing", ex);
                    // Fall back to synchronous processing on publish failure
                    try {
                        onPosition(position, callback);
                    } catch (Exception e) {
                        LOGGER.error("Fallback synchronous processing also failed", e);
                    }
                    return null;
                });
    }

    /**
     * Event handlers implementation for position analysis.
     * This method should be implemented by concrete event handlers.
     * 
     * @param position Position to analyze
     * @param callback Callback to invoke when an event is detected
     */
    public abstract void onPosition(Position position, Callback callback);
    
    /**
     * Wrapper class for callbacks that preserves the correlation ID for distributed tracing.
     */
    protected class AsyncEventCallback implements Callback {
        private final Callback delegate;
        private final String correlationId;
        
        public AsyncEventCallback(Callback delegate, String correlationId) {
            this.delegate = delegate;
            this.correlationId = correlationId;
        }
        
        @Override
        public void eventDetected(Event event) {
            // Create a new span for the callback execution with the original correlation ID
            Span span = tracer.spanBuilder("event.callback." + BaseEventHandler.this.getClass().getSimpleName())
                    .setSpanKind(SpanKind.INTERNAL)
                    .setAttribute("event.id", event.getId())
                    .setAttribute("event.type", event.getType())
                    .setAttribute("correlation.id", correlationId)
                    .startSpan();
            
            try (Scope scope = span.makeCurrent()) {
                // Execute the original callback with tracing context
                delegate.eventDetected(event);
                span.setStatus(StatusCode.OK);
            } catch (RuntimeException e) {
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.recordException(e);
                LOGGER.error("Event callback execution failed", e);
                
                // Record error metric
                meterRegistry.counter("event.callback.error", 
                        "handler", BaseEventHandler.this.getClass().getSimpleName(),
                        "error", e.getClass().getSimpleName()).increment();
                
                // Publish to dead letter queue for later processing
                try {
                    messagePublisher.publishToDeadLetterQueue(event, correlationId, e);
                } catch (Exception dlqEx) {
                    LOGGER.error("Failed to publish to dead letter queue", dlqEx);
                }
            } finally {
                span.end();
            }
        }
    }
}