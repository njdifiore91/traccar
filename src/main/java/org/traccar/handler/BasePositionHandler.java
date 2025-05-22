/*
 * Copyright 2015 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.handler;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Counter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Position;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Base class for position handlers with support for:
 * - Asynchronous processing via message broker
 * - Distributed tracing using OpenTelemetry
 * - Metrics collection using Micrometer
 * - Circuit breaker for graceful degradation
 */
public abstract class BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasePositionHandler.class);

    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final MessageBrokerClient messageBrokerClient;
    private final CircuitBreaker circuitBreaker;
    private final boolean useDirectProcessing;
    
    // Metrics
    private final Timer processingTimer;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Counter fallbackCounter;

    /**
     * Callback interface for position processing completion notification.
     */
    public interface Callback {
        /**
         * Called when position processing is complete.
         * 
         * @param filtered true if position was filtered out, false otherwise
         */
        void processed(boolean filtered);
    }

    /**
     * Constructor with required dependencies.
     * 
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     * @param messageBrokerClient Client for message broker communication
     * @param useDirectProcessing Whether to use direct processing or service-based processing
     */
    protected BasePositionHandler(Tracer tracer, MeterRegistry meterRegistry, 
                                MessageBrokerClient messageBrokerClient, boolean useDirectProcessing) {
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.messageBrokerClient = messageBrokerClient;
        this.useDirectProcessing = useDirectProcessing;
        
        // Initialize metrics
        this.processingTimer = Timer.builder("position.processing.time")
                .description("Time taken to process a position")
                .tag("handler", getClass().getSimpleName())
                .register(meterRegistry);
        
        this.successCounter = Counter.builder("position.processing.success")
                .description("Number of successfully processed positions")
                .tag("handler", getClass().getSimpleName())
                .register(meterRegistry);
        
        this.failureCounter = Counter.builder("position.processing.failure")
                .description("Number of failed position processing attempts")
                .tag("handler", getClass().getSimpleName())
                .register(meterRegistry);
                
        this.fallbackCounter = Counter.builder("position.processing.fallback")
                .description("Number of times fallback processing was used")
                .tag("handler", getClass().getSimpleName())
                .register(meterRegistry);
        
        // Configure circuit breaker for service degradation
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        
        this.circuitBreaker = CircuitBreaker.of(getClass().getSimpleName() + "CircuitBreaker", circuitBreakerConfig);
    }

    /**
     * Process a position. This method should be implemented by subclasses.
     * 
     * @param position Position to process
     * @param callback Callback to notify when processing is complete
     */
    public abstract void onPosition(Position position, Callback callback);

    /**
     * Handle a position with tracing, metrics, and error handling.
     * 
     * @param position Position to handle
     * @param callback Callback to notify when handling is complete
     */
    public void handlePosition(Position position, Callback callback) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("handlePosition")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("position.deviceId", position.getDeviceId())
                .setAttribute("position.id", position.getId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Record metrics for this operation using a timer
            processingTimer.record(() -> {
                if (useDirectProcessing) {
                    processDirectly(position, callback, span);
                } else {
                    processViaService(position, callback, span);
                }
            });
        } catch (Exception e) {
            // Record the error in the span
            span.recordException(e)
                .setStatus(StatusCode.ERROR, e.getMessage());
            
            // Record metrics for failures
            failureCounter.increment();
            
            LOGGER.warn("Position handler failed", e);
            callback.processed(false);
            
            // End the span
            span.end();
        }
    }

    /**
     * Process a position directly (synchronously).
     * 
     * @param position Position to process
     * @param callback Callback to notify when processing is complete
     * @param span Current span for tracing
     */
    private void processDirectly(Position position, Callback callback, Span span) {
        span.addEvent("Processing position directly");
        
        try {
            onPosition(position, result -> {
                span.addEvent("Position processed directly");
                successCounter.increment();
                callback.processed(result);
                span.end();
            });
        } catch (RuntimeException e) {
            span.recordException(e)
                .setStatus(StatusCode.ERROR, e.getMessage());
            failureCounter.increment();
            LOGGER.warn("Direct position processing failed", e);
            callback.processed(false);
            span.end();
        }
    }

    /**
     * Process a position via service (asynchronously via message broker).
     * Uses circuit breaker for graceful degradation.
     * 
     * @param position Position to process
     * @param callback Callback to notify when processing is complete
     * @param span Current span for tracing
     */
    private void processViaService(Position position, Callback callback, Span span) {
        span.addEvent("Processing position via service");
        
        // Extract the current context for propagation
        Context context = Context.current();
        
        // Use circuit breaker to handle service unavailability
        Supplier<CompletableFuture<Boolean>> serviceCall = () -> {
            // Send position to message broker and return a future
            return messageBrokerClient.sendPosition(position, context);
        };
        
        try {
            // Execute with circuit breaker
            CompletableFuture<Boolean> future = circuitBreaker.executeSupplier(serviceCall);
            
            // Handle the result asynchronously
            future.thenAccept(result -> {
                span.addEvent("Position processed via service");
                successCounter.increment();
                callback.processed(result);
                span.end();
            }).exceptionally(e -> {
                span.recordException(e)
                    .setStatus(StatusCode.ERROR, e.getMessage());
                failureCounter.increment();
                LOGGER.warn("Service-based position processing failed", e);
                callback.processed(false);
                span.end();
                return null;
            });
        } catch (Exception e) {
            // Circuit breaker is open or other error occurred, fall back to direct processing
            span.addEvent("Circuit breaker open, falling back to direct processing");
            fallbackCounter.increment();
            LOGGER.warn("Service unavailable, falling back to direct processing", e);
            processDirectly(position, callback, span);
        }
    }

    /**
     * Interface for message broker client.
     * This would be implemented by a concrete class that interacts with RabbitMQ, Kafka, etc.
     */
    public interface MessageBrokerClient {
        /**
         * Send a position to the message broker.
         * 
         * @param position Position to send
         * @param context OpenTelemetry context for propagation
         * @return CompletableFuture that completes when the position is processed
         */
        CompletableFuture<Boolean> sendPosition(Position position, Context context);
    }
}