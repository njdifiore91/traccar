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
package org.traccar.database;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Implements the circuit breaker pattern for database operations to prevent cascading failures
 * in the microservices architecture. It monitors database operation failures, trips the circuit
 * when failure thresholds are exceeded, and provides fallback mechanisms.
 */
public class DatabaseCircuitBreaker {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseCircuitBreaker.class);
    private static final String INSTRUMENTATION_NAME = "org.traccar.database.DatabaseCircuitBreaker";
    
    private final String name;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    
    // OpenTelemetry instrumentation
    private final Tracer tracer;
    private final Meter meter;
    private final LongCounter operationCounter;
    private final LongCounter failureCounter;
    
    /**
     * Creates a new DatabaseCircuitBreaker with default configuration.
     *
     * @param name The name of the circuit breaker, used for identification and metrics
     */
    public DatabaseCircuitBreaker(String name) {
        this(name, 
             CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10)
                .minimumNumberOfCalls(5)
                .permittedNumberOfCallsInHalfOpenState(3)
                .build(),
             RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .exponentialBackoff(Duration.ofMillis(500), Duration.ofSeconds(5), 2.0)
                .build());
    }
    
    /**
     * Creates a new DatabaseCircuitBreaker with custom configuration.
     *
     * @param name The name of the circuit breaker, used for identification and metrics
     * @param circuitBreakerConfig Custom circuit breaker configuration
     * @param retryConfig Custom retry configuration
     */
    public DatabaseCircuitBreaker(String name, CircuitBreakerConfig circuitBreakerConfig, RetryConfig retryConfig) {
        this.name = name;
        
        // Initialize OpenTelemetry
        tracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);
        meter = GlobalOpenTelemetry.getMeter(INSTRUMENTATION_NAME);
        operationCounter = meter.counterBuilder("database.operations")
                .setDescription("Number of database operations")
                .build();
        failureCounter = meter.counterBuilder("database.failures")
                .setDescription("Number of failed database operations")
                .build();
        
        // Initialize circuit breaker
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        circuitBreaker = circuitBreakerRegistry.circuitBreaker(name);
        
        // Initialize retry with exponential backoff
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        retry = retryRegistry.retry(name + "Retry");
        
        // Register event listeners for monitoring
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker {} state changed from {} to {}",
                        name, event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));
        
        retry.getEventPublisher()
                .onRetry(event -> LOGGER.debug("Retry attempt {} after {} ms for {}",
                        event.getNumberOfRetryAttempts(),
                        event.getWaitInterval().toMillis(),
                        name));
    }
    
    /**
     * Executes a supplier with circuit breaker and retry patterns.
     *
     * @param <T> The return type of the supplier
     * @param supplier The supplier to execute
     * @return The result of the supplier
     */
    public <T> T executeSupplier(Supplier<T> supplier) {
        return executeSupplier(supplier, null);
    }
    
    /**
     * Executes a supplier with circuit breaker and retry patterns, with a fallback value.
     *
     * @param <T> The return type of the supplier
     * @param supplier The supplier to execute
     * @param fallback The fallback value to return if the supplier fails
     * @return The result of the supplier or the fallback value
     */
    public <T> T executeSupplier(Supplier<T> supplier, T fallback) {
        Span span = tracer.spanBuilder("database." + name).startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("database.operation", name);
            
            operationCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), name));
            
            try {
                return circuitBreaker.executeSupplier(() -> {
                    return retry.executeSupplier(supplier);
                });
            } catch (Exception e) {
                failureCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), name));
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                
                if (fallback != null) {
                    LOGGER.warn("Operation {} failed, using fallback", name, e);
                    return fallback;
                }
                throw e;
            }
        } finally {
            span.end();
        }
    }
    
    /**
     * Executes a callable with circuit breaker and retry patterns.
     *
     * @param <T> The return type of the callable
     * @param callable The callable to execute
     * @return The result of the callable
     * @throws Exception If the callable throws an exception
     */
    public <T> T executeCallable(Callable<T> callable) throws Exception {
        return executeCallable(callable, null);
    }
    
    /**
     * Executes a callable with circuit breaker and retry patterns, with a fallback value.
     *
     * @param <T> The return type of the callable
     * @param callable The callable to execute
     * @param fallback The fallback value to return if the callable fails
     * @return The result of the callable or the fallback value
     * @throws Exception If the callable throws an exception and no fallback is provided
     */
    public <T> T executeCallable(Callable<T> callable, T fallback) throws Exception {
        Span span = tracer.spanBuilder("database." + name).startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("database.operation", name);
            
            operationCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), name));
            
            try {
                return circuitBreaker.executeCallable(() -> {
                    return retry.executeCallable(callable);
                });
            } catch (Exception e) {
                failureCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), name));
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                
                if (fallback != null) {
                    LOGGER.warn("Operation {} failed, using fallback", name, e);
                    return fallback;
                }
                throw e;
            }
        } finally {
            span.end();
        }
    }
    
    /**
     * Executes a runnable with circuit breaker and retry patterns.
     *
     * @param runnable The runnable to execute
     */
    public void executeRunnable(Runnable runnable) {
        Span span = tracer.spanBuilder("database." + name).startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("database.operation", name);
            
            operationCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), name));
            
            try {
                circuitBreaker.executeRunnable(() -> {
                    retry.executeRunnable(runnable);
                });
            } catch (Exception e) {
                failureCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), name));
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                throw e;
            }
        } finally {
            span.end();
        }
    }
    
    /**
     * Gets the current state of the circuit breaker.
     *
     * @return The current state of the circuit breaker
     */
    public CircuitBreaker.State getState() {
        return circuitBreaker.getState();
    }
    
    /**
     * Gets the name of the circuit breaker.
     *
     * @return The name of the circuit breaker
     */
    public String getName() {
        return name;
    }
}