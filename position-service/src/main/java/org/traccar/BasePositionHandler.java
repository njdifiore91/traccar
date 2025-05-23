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
package org.traccar;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.traccar.model.Position;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Base class for position handlers in the Position Processing Service.
 * Adapted for microservice architecture with distributed tracing, resilience patterns,
 * and message broker integration.
 */
public abstract class BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasePositionHandler.class);
    
    private final Tracer tracer;
    private final Meter meter;
    private final CircuitBreaker circuitBreaker;
    private final Executor asyncExecutor;
    
    // Metrics for monitoring handler performance
    private final LongCounter processedPositionsCounter;
    private final LongCounter filteredPositionsCounter;
    private final LongCounter failedPositionsCounter;
    
    /**
     * Callback interface for position processing completion.
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
     * Constructor for BasePositionHandler.
     * 
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meter OpenTelemetry meter for metrics collection
     * @param circuitBreakerRegistry Registry for circuit breakers
     * @param asyncExecutor Executor for asynchronous processing
     */
    protected BasePositionHandler(Tracer tracer, Meter meter, 
                                CircuitBreakerRegistry circuitBreakerRegistry,
                                Executor asyncExecutor) {
        this.tracer = tracer;
        this.meter = meter;
        this.asyncExecutor = asyncExecutor;
        
        // Create a circuit breaker with default configuration
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
            .failureRateThreshold(50)
            .waitDurationInOpenState(Duration.ofSeconds(10))
            .permittedNumberOfCallsInHalfOpenState(5)
            .slidingWindowSize(10)
            .build();
        
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(getClass().getSimpleName(), config);
        
        // Initialize metrics
        this.processedPositionsCounter = meter.counterBuilder(getClass().getSimpleName() + ".processed")
            .setDescription("Number of positions processed by this handler")
            .build();
        
        this.filteredPositionsCounter = meter.counterBuilder(getClass().getSimpleName() + ".filtered")
            .setDescription("Number of positions filtered out by this handler")
            .build();
        
        this.failedPositionsCounter = meter.counterBuilder(getClass().getSimpleName() + ".failed")
            .setDescription("Number of positions that failed processing in this handler")
            .build();
    }
    
    /**
     * Abstract method to be implemented by concrete handlers for position processing.
     * 
     * @param position Position to process
     * @param callback Callback to invoke when processing is complete
     */
    public abstract void onPosition(Position position, Callback callback);
    
    /**
     * Process a position with circuit breaker protection and distributed tracing.
     * 
     * @param position Position to process
     * @param callback Callback to invoke when processing is complete
     */
    public void handlePosition(Position position, Callback callback) {
        // Create a span for this operation
        Span span = tracer.spanBuilder(getClass().getSimpleName() + ".handlePosition")
            .setAttribute("deviceId", position.getDeviceId())
            .setAttribute("positionId", position.getId())
            .startSpan();
        
        // Add trace context to MDC for logging
        try (Scope scope = span.makeCurrent()) {
            MDC.put("traceId", span.getSpanContext().getTraceId());
            MDC.put("spanId", span.getSpanContext().getSpanId());
            MDC.put("deviceId", String.valueOf(position.getDeviceId()));
            
            // Execute with circuit breaker protection
            try {
                circuitBreaker.executeRunnable(() -> {
                    try {
                        onPosition(position, new Callback() {
                            @Override
                            public void processed(boolean filtered) {
                                if (filtered) {
                                    filteredPositionsCounter.add(1);
                                    span.setAttribute("filtered", true);
                                } else {
                                    processedPositionsCounter.add(1);
                                    span.setAttribute("filtered", false);
                                }
                                callback.processed(filtered);
                            }
                        });
                    } catch (RuntimeException e) {
                        failedPositionsCounter.add(1);
                        span.recordException(e);
                        span.setAttribute("error", true);
                        LOGGER.warn("Position handler failed", e);
                        callback.processed(false);
                    }
                });
            } catch (Exception e) {
                // Circuit breaker is open or another issue occurred
                failedPositionsCounter.add(1);
                span.recordException(e);
                span.setAttribute("error", true);
                span.setAttribute("circuitBreakerState", circuitBreaker.getState().name());
                LOGGER.warn("Circuit breaker prevented position handling: {}", 
                    circuitBreaker.getState(), e);
                callback.processed(false);
            }
        } finally {
            MDC.remove("traceId");
            MDC.remove("spanId");
            MDC.remove("deviceId");
            span.end();
        }
    }
    
    /**
     * Process a position asynchronously with circuit breaker protection and distributed tracing.
     * 
     * @param position Position to process
     * @return CompletableFuture that completes when processing is done
     */
    public CompletableFuture<Boolean> handlePositionAsync(Position position) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        
        // Capture the current context for async propagation
        Context context = Context.current();
        
        // Submit task to executor
        asyncExecutor.execute(() -> {
            // Restore the context in the new thread
            try (Scope scope = context.makeCurrent()) {
                handlePosition(position, filtered -> future.complete(!filtered));
            }
        });
        
        return future;
    }
    
    /**
     * Execute a supplier with circuit breaker protection.
     * 
     * @param <T> Type of the result
     * @param supplier Supplier to execute
     * @return Result of the supplier
     */
    protected <T> T executeWithCircuitBreaker(Supplier<T> supplier) {
        return circuitBreaker.executeSupplier(supplier);
    }
    
    /**
     * Get the current state of the circuit breaker.
     * 
     * @return Current state of the circuit breaker
     */
    public CircuitBreaker.State getCircuitBreakerState() {
        return circuitBreaker.getState();
    }
}