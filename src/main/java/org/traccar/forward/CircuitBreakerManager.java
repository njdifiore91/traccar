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
package org.traccar.forward;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.registry.EntryAddedEvent;
import io.github.resilience4j.core.registry.EntryRemovedEvent;
import io.github.resilience4j.core.registry.EntryReplacedEvent;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.resilience4j.BulkheadMetrics;
import io.micrometer.core.instrument.binder.resilience4j.CircuitBreakerMetrics;
import io.micrometer.core.instrument.binder.resilience4j.RetryMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import jakarta.inject.Provider;
import java.io.IOException;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.Consumer;

/**
 * Provides circuit breaker functionality for external service calls in the forwarding subsystem.
 * Prevents cascading failures when downstream services are unavailable.
 * 
 * This class implements configurable circuit breakers with fallback strategies,
 * retry mechanisms with exponential backoff, and bulkhead patterns for resource isolation.
 */
@Singleton
public class CircuitBreakerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(CircuitBreakerManager.class);

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final BulkheadRegistry bulkheadRegistry;
    private final Provider<MeterRegistry> meterRegistryProvider;

    /**
     * Constructs a new CircuitBreakerManager with the specified configuration.
     *
     * @param config Configuration for circuit breakers, retries, and bulkheads
     * @param meterRegistryProvider Provider for meter registry for metrics collection
     */
    @Inject
    public CircuitBreakerManager(Config config, Provider<MeterRegistry> meterRegistryProvider) {
        this.meterRegistryProvider = meterRegistryProvider;
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(config.getFloat(Keys.FORWARD_CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD, 50.0f))
                .slowCallRateThreshold(config.getFloat(Keys.FORWARD_CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD, 50.0f))
                .slowCallDurationThreshold(Duration.ofMillis(
                        config.getLong(Keys.FORWARD_CIRCUIT_BREAKER_SLOW_CALL_DURATION_THRESHOLD, 5000)))
                .permittedNumberOfCallsInHalfOpenState(
                        config.getInteger(Keys.FORWARD_CIRCUIT_BREAKER_PERMITTED_CALLS_IN_HALF_OPEN, 10))
                .minimumNumberOfCalls(config.getInteger(Keys.FORWARD_CIRCUIT_BREAKER_MINIMUM_CALLS, 10))
                .waitDurationInOpenState(Duration.ofMillis(
                        config.getLong(Keys.FORWARD_CIRCUIT_BREAKER_WAIT_DURATION_IN_OPEN, 60000)))
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(config.getInteger(Keys.FORWARD_CIRCUIT_BREAKER_SLIDING_WINDOW_SIZE, 100))
                .recordExceptions(RuntimeException.class, TimeoutException.class, IOException.class)
                .build();

        // Configure retry
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(config.getInteger(Keys.FORWARD_RETRY_MAX_ATTEMPTS, 3))
                .waitDuration(Duration.ofMillis(config.getLong(Keys.FORWARD_RETRY_WAIT_DURATION, 1000)))
                .retryExceptions(RuntimeException.class, TimeoutException.class, IOException.class)
                .enableRandomizedWait(factor -> {
                    // Exponential backoff with jitter
                    long waitDuration = config.getLong(Keys.FORWARD_RETRY_WAIT_DURATION, 1000);
                    return Duration.ofMillis((long) (waitDuration * Math.pow(1.5, factor) * (1.0 + Math.random() * 0.1)));
                })
                .build();

        // Configure bulkhead
        BulkheadConfig bulkheadConfig = BulkheadConfig.custom()
                .maxConcurrentCalls(config.getInteger(Keys.FORWARD_BULKHEAD_MAX_CONCURRENT_CALLS, 25))
                .maxWaitDuration(Duration.ofMillis(config.getLong(Keys.FORWARD_BULKHEAD_MAX_WAIT_DURATION, 500)))
                .build();

        // Create registries
        circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        retryRegistry = RetryRegistry.of(retryConfig);
        bulkheadRegistry = BulkheadRegistry.of(bulkheadConfig);

        // Register event consumers for logging
        registerEventConsumers();

        // Register metrics if meter registry is available
        try {
            MeterRegistry meterRegistry = meterRegistryProvider.get();
            if (meterRegistry != null) {
                CircuitBreakerMetrics.ofCircuitBreakerRegistry(circuitBreakerRegistry).bindTo(meterRegistry);
                RetryMetrics.ofRetryRegistry(retryRegistry).bindTo(meterRegistry);
                BulkheadMetrics.ofBulkheadRegistry(bulkheadRegistry).bindTo(meterRegistry);
                LOGGER.debug("Resilience4j metrics registered with MeterRegistry");
            }
        } catch (Exception e) {
            LOGGER.debug("MeterRegistry not available for metrics registration: {}", e.getMessage());
        }
    }

    /**
     * Registers event consumers for logging circuit breaker, retry, and bulkhead events.
     * This provides visibility into the state and operation of the resilience mechanisms.
     */
    private void registerEventConsumers() {
        // Circuit breaker events
        circuitBreakerRegistry.getEventPublisher().onEntryAdded(event -> {
            CircuitBreaker circuitBreaker = event.getAddedEntry();
            circuitBreaker.getEventPublisher().onStateTransition(stateTransitionEvent -> 
                LOGGER.info("Circuit breaker '{}' changed state from {} to {}",
                    stateTransitionEvent.getCircuitBreakerName(),
                    stateTransitionEvent.getStateTransition().getFromState(),
                    stateTransitionEvent.getStateTransition().getToState()));
            circuitBreaker.getEventPublisher().onError(errorEvent -> 
                LOGGER.debug("Circuit breaker '{}' recorded error: {}",
                    errorEvent.getCircuitBreakerName(),
                    errorEvent.getThrowable().getMessage()));
            circuitBreaker.getEventPublisher().onSuccess(successEvent -> 
                LOGGER.trace("Circuit breaker '{}' recorded success, elapsed time: {}ms",
                    successEvent.getCircuitBreakerName(),
                    successEvent.getElapsedDuration().toMillis()));
        });

        // Retry events
        retryRegistry.getEventPublisher().onEntryAdded(event -> {
            Retry retry = event.getAddedEntry();
            retry.getEventPublisher().onRetry(retryEvent -> 
                LOGGER.debug("Retry '{}' attempt {} of {}",
                    retryEvent.getName(),
                    retryEvent.getNumberOfRetryAttempts(),
                    retryEvent.getMaxAttempts()));
            retry.getEventPublisher().onError(errorEvent -> 
                LOGGER.debug("Retry '{}' failed with error: {}",
                    errorEvent.getName(),
                    errorEvent.getLastThrowable().getMessage()));
        });

        // Bulkhead events
        bulkheadRegistry.getEventPublisher().onEntryAdded(event -> {
            Bulkhead bulkhead = event.getAddedEntry();
            bulkhead.getEventPublisher().onCallRejected(rejectedEvent -> 
                LOGGER.debug("Bulkhead '{}' rejected call due to overload",
                    rejectedEvent.getBulkheadName()));
        });
    }

    /**
     * Decorates a supplier with circuit breaker, retry, and bulkhead functionality.
     * The order of decoration is important: bulkhead first (to limit concurrent calls),
     * then retry (to retry failed calls), and finally circuit breaker (to prevent calls when service is down).
     *
     * @param name The name of the circuit breaker, retry, and bulkhead
     * @param supplier The supplier to decorate
     * @param <T> The type of the result
     * @return A decorated supplier with resilience patterns applied
     */
    public <T> Supplier<T> decorateSupplier(String name, Supplier<T> supplier) {
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(name);
        Retry retry = retryRegistry.retry(name);
        Bulkhead bulkhead = bulkheadRegistry.bulkhead(name);

        return Bulkhead.decorateSupplier(bulkhead,
                Retry.decorateSupplier(retry,
                        CircuitBreaker.decorateSupplier(circuitBreaker, supplier)));
    }

    /**
     * Decorates a supplier with circuit breaker, retry, and bulkhead functionality,
     * and provides a fallback supplier for when the primary supplier fails.
     *
     * @param name The name of the circuit breaker, retry, and bulkhead
     * @param supplier The supplier to decorate
     * @param fallback The fallback supplier to use when the primary supplier fails
     * @param <T> The type of the result
     * @return A decorated supplier with resilience patterns and fallback applied
     */
    public <T> Supplier<T> decorateSupplierWithFallback(String name, Supplier<T> supplier, Supplier<T> fallback) {
        return () -> {
            try {
                return decorateSupplier(name, supplier).get();
            } catch (Exception e) {
                LOGGER.warn("Executing fallback for '{}' due to: {}", name, e.getMessage());
                return fallback.get();
            }
        };
    }

    /**
     * Decorates a function with circuit breaker, retry, and bulkhead functionality.
     * The order of decoration is important: bulkhead first (to limit concurrent calls),
     * then retry (to retry failed calls), and finally circuit breaker (to prevent calls when service is down).
     *
     * @param name The name of the circuit breaker, retry, and bulkhead
     * @param function The function to decorate
     * @param <T> The type of the input to the function
     * @param <R> The type of the result of the function
     * @return A decorated function with resilience patterns applied
     */
    public <T, R> Function<T, R> decorateFunction(String name, Function<T, R> function) {
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(name);
        Retry retry = retryRegistry.retry(name);
        Bulkhead bulkhead = bulkheadRegistry.bulkhead(name);

        return Bulkhead.decorateFunction(bulkhead,
                Retry.decorateFunction(retry,
                        CircuitBreaker.decorateFunction(circuitBreaker, function)));
    }

    /**
     * Decorates a function with circuit breaker, retry, and bulkhead functionality,
     * and provides a fallback function for when the primary function fails.
     *
     * @param name The name of the circuit breaker, retry, and bulkhead
     * @param function The function to decorate
     * @param fallback The fallback function to use when the primary function fails
     * @param <T> The type of the input to the function
     * @param <R> The type of the result of the function
     * @return A decorated function with resilience patterns and fallback applied
     */
    public <T, R> Function<T, R> decorateFunctionWithFallback(String name, Function<T, R> function, Function<T, R> fallback) {
        return input -> {
            try {
                return decorateFunction(name, function).apply(input);
            } catch (Exception e) {
                LOGGER.warn("Executing fallback for '{}' due to: {}", name, e.getMessage());
                return fallback.apply(input);
            }
        };
    }

    /**
     * Gets the circuit breaker registry.
     *
     * @return The circuit breaker registry
     */
    public CircuitBreakerRegistry getCircuitBreakerRegistry() {
        return circuitBreakerRegistry;
    }

    /**
     * Gets the retry registry.
     *
     * @return The retry registry
     */
    public RetryRegistry getRetryRegistry() {
        return retryRegistry;
    }

    /**
     * Gets the bulkhead registry.
     *
     * @return The bulkhead registry
     */
    public BulkheadRegistry getBulkheadRegistry() {
        return bulkheadRegistry;
    }
    
    /**
     * Executes a supplier with resilience patterns applied.
     *
     * @param name The name of the circuit breaker, retry, and bulkhead
     * @param supplier The supplier to execute
     * @param <T> The type of the result
     * @return The result of the supplier
     */
    public <T> T executeSupplier(String name, Supplier<T> supplier) {
        return decorateSupplier(name, supplier).get();
    }
    
    /**
     * Executes a supplier with resilience patterns and fallback applied.
     *
     * @param name The name of the circuit breaker, retry, and bulkhead
     * @param supplier The supplier to execute
     * @param fallback The fallback supplier to use when the primary supplier fails
     * @param <T> The type of the result
     * @return The result of the supplier or fallback
     */
    public <T> T executeSupplierWithFallback(String name, Supplier<T> supplier, Supplier<T> fallback) {
        return decorateSupplierWithFallback(name, supplier, fallback).get();
    }
    
    /**
     * Executes a function with resilience patterns applied.
     *
     * @param name The name of the circuit breaker, retry, and bulkhead
     * @param function The function to execute
     * @param input The input to the function
     * @param <T> The type of the input to the function
     * @param <R> The type of the result of the function
     * @return The result of the function
     */
    public <T, R> R executeFunction(String name, Function<T, R> function, T input) {
        return decorateFunction(name, function).apply(input);
    }
    
    /**
     * Executes a function with resilience patterns and fallback applied.
     *
     * @param name The name of the circuit breaker, retry, and bulkhead
     * @param function The function to execute
     * @param fallback The fallback function to use when the primary function fails
     * @param input The input to the function
     * @param <T> The type of the input to the function
     * @param <R> The type of the result of the function
     * @return The result of the function or fallback
     */
    public <T, R> R executeFunctionWithFallback(String name, Function<T, R> function, Function<T, R> fallback, T input) {
        return decorateFunctionWithFallback(name, function, fallback).apply(input);
    }
    
    /**
     * Decorates a consumer with circuit breaker, retry, and bulkhead functionality.
     *
     * @param name The name of the circuit breaker, retry, and bulkhead
     * @param consumer The consumer to decorate
     * @param <T> The type of the input to the consumer
     * @return A decorated consumer with resilience patterns applied
     */
    public <T> Consumer<T> decorateConsumer(String name, Consumer<T> consumer) {
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(name);
        Retry retry = retryRegistry.retry(name);
        Bulkhead bulkhead = bulkheadRegistry.bulkhead(name);

        return Bulkhead.decorateConsumer(bulkhead,
                Retry.decorateConsumer(retry,
                        CircuitBreaker.decorateConsumer(circuitBreaker, consumer)));
    }
    
    /**
     * Decorates a consumer with circuit breaker, retry, and bulkhead functionality,
     * and provides a fallback consumer for when the primary consumer fails.
     *
     * @param name The name of the circuit breaker, retry, and bulkhead
     * @param consumer The consumer to decorate
     * @param fallback The fallback consumer to use when the primary consumer fails
     * @param <T> The type of the input to the consumer
     * @return A decorated consumer with resilience patterns and fallback applied
     */
    public <T> Consumer<T> decorateConsumerWithFallback(String name, Consumer<T> consumer, Consumer<T> fallback) {
        return input -> {
            try {
                decorateConsumer(name, consumer).accept(input);
            } catch (Exception e) {
                LOGGER.warn("Executing fallback for '{}' due to: {}", name, e.getMessage());
                fallback.accept(input);
            }
        };
    }
    
    /**
     * Executes a consumer with resilience patterns applied.
     *
     * @param name The name of the circuit breaker, retry, and bulkhead
     * @param consumer The consumer to execute
     * @param input The input to the consumer
     * @param <T> The type of the input to the consumer
     */
    public <T> void executeConsumer(String name, Consumer<T> consumer, T input) {
        decorateConsumer(name, consumer).accept(input);
    }
    
    /**
     * Executes a consumer with resilience patterns and fallback applied.
     *
     * @param name The name of the circuit breaker, retry, and bulkhead
     * @param consumer The consumer to execute
     * @param fallback The fallback consumer to use when the primary consumer fails
     * @param input The input to the consumer
     * @param <T> The type of the input to the consumer
     */
    public <T> void executeConsumerWithFallback(String name, Consumer<T> consumer, Consumer<T> fallback, T input) {
        decorateConsumerWithFallback(name, consumer, fallback).accept(input);
    }
}