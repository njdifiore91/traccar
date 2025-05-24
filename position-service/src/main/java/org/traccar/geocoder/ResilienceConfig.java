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
package org.traccar.geocoder;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Provides centralized configuration for resilience patterns used by geocoder implementations.
 * Configures circuit breakers, retry mechanisms with exponential backoff, and fallback strategies
 * for external geocoding service calls.
 */
@Singleton
public class ResilienceConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResilienceConfig.class);

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final MeterRegistry meterRegistry;

    /**
     * Creates a new ResilienceConfig with default settings.
     */
    @Inject
    public ResilienceConfig(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;

        // Configure circuit breaker defaults
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .slowCallRateThreshold(50) // Consider a call as slow when 50% of calls take longer than slowCallDurationThreshold
                .slowCallDurationThreshold(Duration.ofSeconds(5))
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before transitioning to half-open
                .permittedNumberOfCallsInHalfOpenState(10) // Allow 10 calls in half-open state
                .minimumNumberOfCalls(10) // Minimum number of calls before calculating failure rate
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(100) // Consider the last 100 calls
                .recordExceptions(
                        IOException.class,
                        TimeoutException.class,
                        ConnectException.class,
                        SocketTimeoutException.class)
                .build();

        // Configure retry defaults
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Maximum 3 attempts (1 initial + 2 retries)
                .waitDuration(Duration.ofMillis(500)) // Initial wait duration
                .retryExceptions(
                        IOException.class,
                        TimeoutException.class,
                        ConnectException.class,
                        SocketTimeoutException.class)
                .enableExponentialBackoff(true) // Use exponential backoff
                .exponentialBackoffMultiplier(2) // Double the wait time for each retry
                .enableRandomizedWait(true) // Add jitter to prevent thundering herd
                .randomizedWaitFactor(0.5) // Add up to 50% jitter
                .build();

        // Create registries with default configurations
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.retryRegistry = RetryRegistry.of(retryConfig);

        // Register metrics if MeterRegistry is available
        if (meterRegistry != null) {
            TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(circuitBreakerRegistry)
                    .bindTo(meterRegistry);
            LOGGER.info("Circuit breaker metrics registered with MeterRegistry");
        }
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
     * Gets the meter registry for metrics collection.
     *
     * @return The meter registry
     */
    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    /**
     * Configures a circuit breaker for a specific geocoder with custom settings.
     *
     * @param name The name of the geocoder
     * @param failureRateThreshold The failure rate threshold in percent
     * @param waitDurationInOpenState The wait duration in open state
     * @return The configured circuit breaker
     */
    public CircuitBreaker configureCircuitBreaker(String name, float failureRateThreshold, Duration waitDurationInOpenState) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(failureRateThreshold)
                .waitDurationInOpenState(waitDurationInOpenState)
                .build();

        return circuitBreakerRegistry.circuitBreaker(name, config);
    }

    /**
     * Configures a retry mechanism for a specific geocoder with custom settings.
     *
     * @param name The name of the geocoder
     * @param maxAttempts The maximum number of attempts
     * @param waitDuration The initial wait duration
     * @return The configured retry
     */
    public Retry configureRetry(String name, int maxAttempts, Duration waitDuration) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(maxAttempts)
                .waitDuration(waitDuration)
                .build();

        return retryRegistry.retry(name, config);
    }
}