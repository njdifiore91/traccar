/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.core.registry.EntryAddedEvent;
import io.github.resilience4j.core.registry.EntryRemovedEvent;
import io.github.resilience4j.core.registry.EntryReplacedEvent;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.traccar.config.Config;
import org.traccar.config.Keys;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages circuit breakers for external service calls to prevent cascading failures.
 * Implements Resilience4j circuit breakers, retry mechanisms, and bulkheads for fault tolerance.
 */
@Singleton
public class CircuitBreakerManager implements LifecycleObject {

    private static final Logger LOGGER = LoggerFactory.getLogger(CircuitBreakerManager.class);

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final BulkheadRegistry bulkheadRegistry;
    
    // Track decorated services for lifecycle management
    private final Map<String, Object> managedServices = new ConcurrentHashMap<>();

    /**
     * Initializes the CircuitBreakerManager with configuration from the Traccar config.
     *
     * @param config Traccar configuration
     */
    @Inject
    public CircuitBreakerManager(Config config) {
        // Configure default circuit breaker settings
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(config.getFloat(Keys.CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD, 50.0f))
                .slowCallRateThreshold(config.getFloat(Keys.CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD, 50.0f))
                .slowCallDurationThreshold(Duration.ofMillis(
                        config.getLong(Keys.CIRCUIT_BREAKER_SLOW_CALL_DURATION_THRESHOLD, 1000L)))
                .permittedNumberOfCallsInHalfOpenState(
                        config.getInteger(Keys.CIRCUIT_BREAKER_PERMITTED_CALLS_IN_HALF_OPEN, 10))
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(config.getInteger(Keys.CIRCUIT_BREAKER_SLIDING_WINDOW_SIZE, 100))
                .minimumNumberOfCalls(config.getInteger(Keys.CIRCUIT_BREAKER_MINIMUM_CALLS, 10))
                .waitDurationInOpenState(Duration.ofMillis(
                        config.getLong(Keys.CIRCUIT_BREAKER_WAIT_DURATION_IN_OPEN_STATE, 60000L)))
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .recordExceptions(Exception.class)
                .ignoreExceptions(IllegalArgumentException.class)
                .build();

        // Configure default retry settings
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(config.getInteger(Keys.CIRCUIT_BREAKER_RETRY_MAX_ATTEMPTS, 3))
                .waitDuration(Duration.ofMillis(config.getLong(Keys.CIRCUIT_BREAKER_RETRY_WAIT_DURATION, 1000L)))
                .retryExceptions(Exception.class)
                .ignoreExceptions(IllegalArgumentException.class)
                .retryOnResult(response -> response == null)
                .enableRandomizedWait()
                .randomizedWaitFactor(config.getFloat(Keys.CIRCUIT_BREAKER_RETRY_RANDOMIZED_WAIT_FACTOR, 0.5f))
                .build();

        // Configure default bulkhead settings
        BulkheadConfig bulkheadConfig = BulkheadConfig.custom()
                .maxConcurrentCalls(config.getInteger(Keys.CIRCUIT_BREAKER_BULKHEAD_MAX_CONCURRENT_CALLS, 25))
                .maxWaitDuration(Duration.ofMillis(
                        config.getLong(Keys.CIRCUIT_BREAKER_BULKHEAD_MAX_WAIT_DURATION, 0L)))
                .build();

        // Create registries with event consumers for monitoring
        circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig, new CircuitBreakerEventConsumer());
        retryRegistry = RetryRegistry.of(retryConfig, new RetryEventConsumer());
        bulkheadRegistry = BulkheadRegistry.of(bulkheadConfig, new BulkheadEventConsumer());

        // Log initialization
        LOGGER.info("Initialized CircuitBreakerManager with default configurations");
    }

    /**
     * Gets or creates a circuit breaker with the default configuration.
     *
     * @param name Name of the circuit breaker
     * @return CircuitBreaker instance
     */
    public CircuitBreaker getCircuitBreaker(String name) {
        return circuitBreakerRegistry.circuitBreaker(name);
    }

    /**
     * Gets or creates a circuit breaker with custom configuration.
     *
     * @param name Name of the circuit breaker
     * @param config Custom circuit breaker configuration
     * @return CircuitBreaker instance
     */
    public CircuitBreaker getCircuitBreaker(String name, CircuitBreakerConfig config) {
        return circuitBreakerRegistry.circuitBreaker(name, config);
    }

    /**
     * Gets or creates a retry with the default configuration.
     *
     * @param name Name of the retry
     * @return Retry instance
     */
    public Retry getRetry(String name) {
        return retryRegistry.retry(name);
    }

    /**
     * Gets or creates a retry with custom configuration.
     *
     * @param name Name of the retry
     * @param config Custom retry configuration
     * @return Retry instance
     */
    public Retry getRetry(String name, RetryConfig config) {
        return retryRegistry.retry(name, config);
    }

    /**
     * Gets or creates a bulkhead with the default configuration.
     *
     * @param name Name of the bulkhead
     * @return Bulkhead instance
     */
    public Bulkhead getBulkhead(String name) {
        return bulkheadRegistry.bulkhead(name);
    }

    /**
     * Gets or creates a bulkhead with custom configuration.
     *
     * @param name Name of the bulkhead
     * @param config Custom bulkhead configuration
     * @return Bulkhead instance
     */
    public Bulkhead getBulkhead(String name, BulkheadConfig config) {
        return bulkheadRegistry.bulkhead(name, config);
    }

    /**
     * Decorates a supplier with circuit breaker, retry, and bulkhead.
     *
     * @param <T> Type of the supplier result
     * @param name Name prefix for the resilience components
     * @param supplier Supplier to decorate
     * @return Decorated supplier with fault tolerance
     */
    public <T> Supplier<T> decorateSupplier(String name, Supplier<T> supplier) {
        Supplier<T> decorated = Bulkhead.decorateSupplier(
                getBulkhead(name + "Bulkhead"),
                Retry.decorateSupplier(
                        getRetry(name + "Retry"),
                        CircuitBreaker.decorateSupplier(
                                getCircuitBreaker(name + "CircuitBreaker"),
                                supplier)));
        
        // Track for lifecycle management
        managedServices.put(name, decorated);
        return decorated;
    }

    /**
     * Decorates a supplier with circuit breaker, retry, and bulkhead, providing a fallback.
     *
     * @param <T> Type of the supplier result
     * @param name Name prefix for the resilience components
     * @param supplier Supplier to decorate
     * @param fallback Fallback supplier to use when the call fails
     * @return Decorated supplier with fault tolerance and fallback
     */
    public <T> Supplier<T> decorateSupplierWithFallback(String name, Supplier<T> supplier, Supplier<T> fallback) {
        Supplier<T> decorated = () -> {
            try {
                return decorateSupplier(name, supplier).get();
            } catch (Exception e) {
                LOGGER.warn("Circuit breaker '{}' triggered fallback due to: {}", name, e.getMessage());
                return fallback.get();
            }
        };
        
        // Track for lifecycle management
        managedServices.put(name, decorated);
        return decorated;
    }

    /**
     * Decorates a function with circuit breaker, retry, and bulkhead.
     *
     * @param <T> Type of the function input
     * @param <R> Type of the function result
     * @param name Name prefix for the resilience components
     * @param function Function to decorate
     * @return Decorated function with fault tolerance
     */
    public <T, R> Function<T, R> decorateFunction(String name, Function<T, R> function) {
        Function<T, R> decorated = Bulkhead.decorateFunction(
                getBulkhead(name + "Bulkhead"),
                Retry.decorateFunction(
                        getRetry(name + "Retry"),
                        CircuitBreaker.decorateFunction(
                                getCircuitBreaker(name + "CircuitBreaker"),
                                function)));
        
        // Track for lifecycle management
        managedServices.put(name, decorated);
        return decorated;
    }

    /**
     * Decorates a function with circuit breaker, retry, and bulkhead, providing a fallback.
     *
     * @param <T> Type of the function input
     * @param <R> Type of the function result
     * @param name Name prefix for the resilience components
     * @param function Function to decorate
     * @param fallback Fallback function to use when the call fails
     * @return Decorated function with fault tolerance and fallback
     */
    public <T, R> Function<T, R> decorateFunctionWithFallback(
            String name, Function<T, R> function, Function<T, R> fallback) {
        Function<T, R> decorated = (T t) -> {
            try {
                return decorateFunction(name, function).apply(t);
            } catch (Exception e) {
                LOGGER.warn("Circuit breaker '{}' triggered fallback due to: {}", name, e.getMessage());
                return fallback.apply(t);
            }
        };
        
        // Track for lifecycle management
        managedServices.put(name, decorated);
        return decorated;
    }

    /**
     * Event consumer for circuit breaker events.
     */
    private static class CircuitBreakerEventConsumer implements RegistryEventConsumer<CircuitBreaker> {
        @Override
        public void onEntryAddedEvent(EntryAddedEvent<CircuitBreaker> entryAddedEvent) {
            entryAddedEvent.getAddedEntry().getEventPublisher()
                    .onStateTransition(event -> {
                        // Log state transition events
                        LOGGER.info("Circuit breaker '{}' changed state from {} to {}", 
                                event.getCircuitBreakerName(),
                                event.getStateTransition().getFromState(),
                                event.getStateTransition().getToState());
                    });
        }

        @Override
        public void onEntryRemovedEvent(EntryRemovedEvent<CircuitBreaker> entryRemoveEvent) {
            // Handle circuit breaker removal if needed
        }

        @Override
        public void onEntryReplacedEvent(EntryReplacedEvent<CircuitBreaker> entryReplacedEvent) {
            // Handle circuit breaker replacement if needed
        }
    }

    /**
     * Event consumer for retry events.
     */
    private static class RetryEventConsumer implements RegistryEventConsumer<Retry> {
        @Override
        public void onEntryAddedEvent(EntryAddedEvent<Retry> entryAddedEvent) {
            entryAddedEvent.getAddedEntry().getEventPublisher()
                    .onRetry(event -> {
                        // Log retry events
                        LOGGER.debug("Retry '{}' attempt #{}", 
                                event.getName(), 
                                event.getNumberOfRetryAttempts());
                    });
        }

        @Override
        public void onEntryRemovedEvent(EntryRemovedEvent<Retry> entryRemoveEvent) {
            // Handle retry removal if needed
        }

        @Override
        public void onEntryReplacedEvent(EntryReplacedEvent<Retry> entryReplacedEvent) {
            // Handle retry replacement if needed
        }
    }

    /**
     * Event consumer for bulkhead events.
     */
    private static class BulkheadEventConsumer implements RegistryEventConsumer<Bulkhead> {
        @Override
        public void onEntryAddedEvent(EntryAddedEvent<Bulkhead> entryAddedEvent) {
            entryAddedEvent.getAddedEntry().getEventPublisher()
                    .onCallRejected(event -> {
                        // Log bulkhead rejection events
                        LOGGER.warn("Bulkhead '{}' rejected call because it is full", 
                                event.getBulkheadName());
                    });
        }

        @Override
        public void onEntryRemovedEvent(EntryRemovedEvent<Bulkhead> entryRemoveEvent) {
            // Handle bulkhead removal if needed
        }

        @Override
        public void onEntryReplacedEvent(EntryReplacedEvent<Bulkhead> entryReplacedEvent) {
            // Handle bulkhead replacement if needed
        }
    }
    
    /**
     * Executes a supplier with circuit breaker, retry, and bulkhead protection.
     *
     * @param <T> Type of the supplier result
     * @param name Name prefix for the resilience components
     * @param supplier Supplier to execute
     * @return Result of the supplier execution
     */
    public <T> T executeSupplier(String name, Supplier<T> supplier) {
        return decorateSupplier(name, supplier).get();
    }
    
    /**
     * Executes a supplier with circuit breaker, retry, and bulkhead protection, providing a fallback.
     *
     * @param <T> Type of the supplier result
     * @param name Name prefix for the resilience components
     * @param supplier Supplier to execute
     * @param fallback Fallback supplier to use when the call fails
     * @return Result of the supplier execution or fallback
     */
    public <T> T executeSupplierWithFallback(String name, Supplier<T> supplier, Supplier<T> fallback) {
        return decorateSupplierWithFallback(name, supplier, fallback).get();
    }
    
    /**
     * Resets all circuit breakers to their initial state.
     */
    public void resetAllCircuitBreakers() {
        circuitBreakerRegistry.getAllCircuitBreakers().forEach(CircuitBreaker::reset);
        LOGGER.info("All circuit breakers have been reset");
    }
    
    /**
     * Lifecycle method called when the application starts.
     */
    @Override
    public void start() {
        LOGGER.info("CircuitBreakerManager started");
    }
    
    /**
     * Lifecycle method called when the application stops.
     * Resets all circuit breakers and cleans up resources.
     */
    @Override
    public void stop() {
        resetAllCircuitBreakers();
        managedServices.clear();
        LOGGER.info("CircuitBreakerManager stopped");
    }
}