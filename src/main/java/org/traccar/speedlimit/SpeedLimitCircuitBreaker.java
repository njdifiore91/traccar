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
package org.traccar.speedlimit;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Circuit breaker implementation for speed limit providers.
 * Implements resilience patterns to handle failures gracefully.
 */
@Singleton
public class SpeedLimitCircuitBreaker {

    private static final Logger LOGGER = Logger.getLogger(SpeedLimitCircuitBreaker.class.getName());

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;

    /**
     * Constructs a new SpeedLimitCircuitBreaker with default configuration.
     */
    @Inject
    public SpeedLimitCircuitBreaker() {
        // Configure circuit breaker with default settings
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before attempting to close
                .slidingWindowSize(10) // Consider last 10 calls for failure rate
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .recordExceptions(Exception.class) // Record all exceptions as failures
                .ignoreExceptions(SpeedLimitException.class) // Don't count SpeedLimitException as circuit breaker failures
                .build();

        // Configure retry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Maximum 3 attempts
                .waitDuration(Duration.ofMillis(500)) // Initial wait duration
                .retryExceptions(TimeoutException.class) // Retry on timeout
                .ignoreExceptions(SpeedLimitException.class) // Don't retry on SpeedLimitException
                .build();

        circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        retryRegistry = RetryRegistry.of(retryConfig);

        LOGGER.info("Initialized SpeedLimitCircuitBreaker");
    }

    /**
     * Executes a supplier with circuit breaker protection.
     *
     * @param supplier The supplier to execute
     * @param providerName The name of the speed limit provider
     * @param <T> The return type of the supplier
     * @return The result of the supplier
     * @throws Exception If the supplier throws an exception
     */
    public <T> T executeWithCircuitBreaker(Supplier<T> supplier, String providerName) throws Exception {
        CircuitBreaker circuitBreaker = getOrCreateCircuitBreaker(providerName);
        Retry retry = getOrCreateRetry(providerName);

        // Decorate the supplier with retry and circuit breaker
        Supplier<T> decoratedSupplier = CircuitBreaker.decorateSupplier(circuitBreaker, supplier);
        decoratedSupplier = Retry.decorateSupplier(retry, decoratedSupplier);

        try {
            return decoratedSupplier.get();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Circuit breaker caught exception from provider: " + providerName, e);
            throw e;
        }
    }

    /**
     * Gets the circuit breaker for a provider, creating it if it doesn't exist.
     *
     * @param providerName The name of the speed limit provider
     * @return The circuit breaker for the provider
     */
    public CircuitBreaker getOrCreateCircuitBreaker(String providerName) {
        return circuitBreakerRegistry.circuitBreaker(providerName);
    }

    /**
     * Gets the retry for a provider, creating it if it doesn't exist.
     *
     * @param providerName The name of the speed limit provider
     * @return The retry for the provider
     */
    public Retry getOrCreateRetry(String providerName) {
        return retryRegistry.retry(providerName);
    }

    /**
     * Gets the state of the circuit breaker for a provider.
     *
     * @param providerName The name of the speed limit provider
     * @return The state of the circuit breaker
     */
    public String getCircuitBreakerState(String providerName) {
        return getOrCreateCircuitBreaker(providerName).getState().name();
    }

    /**
     * Resets the circuit breaker for a provider.
     *
     * @param providerName The name of the speed limit provider
     */
    public void resetCircuitBreaker(String providerName) {
        getOrCreateCircuitBreaker(providerName).reset();
        LOGGER.info("Reset circuit breaker for provider: " + providerName);
    }

    /**
     * Gets the metrics for the circuit breaker of a provider.
     *
     * @param providerName The name of the speed limit provider
     * @return The metrics for the circuit breaker
     */
    public CircuitBreaker.Metrics getCircuitBreakerMetrics(String providerName) {
        return getOrCreateCircuitBreaker(providerName).getMetrics();
    }
}