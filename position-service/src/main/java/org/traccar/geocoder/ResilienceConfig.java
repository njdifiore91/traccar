/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.resilience4j.CircuitBreakerMetrics;
import io.micrometer.core.instrument.binder.resilience4j.RetryMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Provides centralized configuration for resilience patterns used by geocoder implementations.
 * Configures circuit breakers, retry mechanisms, and fallback strategies for external geocoding service calls.
 */
@Singleton
public class ResilienceConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResilienceConfig.class);

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final Map<String, CircuitBreaker> circuitBreakers = new HashMap<>();
    private final Map<String, Retry> retries = new HashMap<>();

    /**
     * Initialize resilience configuration with default settings and register with metrics.
     *
     * @param meterRegistry Metrics registry for monitoring resilience patterns
     */
    @Inject
    public ResilienceConfig(MeterRegistry meterRegistry) {
        // Configure default circuit breaker settings
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to trip circuit
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10) // Consider last 10 calls for failure rate
                .recordExceptions(
                        IOException.class,
                        ConnectException.class,
                        TimeoutException.class,
                        SocketTimeoutException.class)
                .build();

        // Configure default retry settings
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Try up to 3 times
                .waitDuration(Duration.ofMillis(500)) // Initial wait 500ms
                .retryExceptions(
                        IOException.class,
                        ConnectException.class,
                        TimeoutException.class,
                        SocketTimeoutException.class)
                .enableExponentialBackoff(true) // Use exponential backoff
                .exponentialBackoffMultiplier(2) // Double wait time each retry
                .enableRandomizedWait(true) // Add jitter to prevent thundering herd
                .randomizedWaitFactor(0.5) // 50% randomization factor
                .build();

        // Create registries with default configurations
        circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        retryRegistry = RetryRegistry.of(retryConfig);

        // Register with metrics
        CircuitBreakerMetrics.ofCircuitBreakerRegistry(circuitBreakerRegistry)
                .bindTo(meterRegistry);
        RetryMetrics.ofRetryRegistry(retryRegistry)
                .bindTo(meterRegistry);

        LOGGER.info("Initialized resilience configuration for geocoding services");
    }

    /**
     * Get or create a circuit breaker for a specific geocoder.
     * Uses provider-specific configuration if available, otherwise falls back to defaults.
     *
     * @param provider Geocoder provider name
     * @return CircuitBreaker instance for the provider
     */
    public CircuitBreaker getCircuitBreaker(String provider) {
        return circuitBreakers.computeIfAbsent(provider, name -> {
            // Create provider-specific configuration if needed
            CircuitBreakerConfig config = getProviderCircuitBreakerConfig(name);
            CircuitBreaker circuitBreaker = config != null
                    ? circuitBreakerRegistry.circuitBreaker(name, config)
                    : circuitBreakerRegistry.circuitBreaker(name);
            
            LOGGER.debug("Created circuit breaker for geocoder: {}", name);
            return circuitBreaker;
        });
    }

    /**
     * Get or create a retry for a specific geocoder.
     * Uses provider-specific configuration if available, otherwise falls back to defaults.
     *
     * @param provider Geocoder provider name
     * @return Retry instance for the provider
     */
    public Retry getRetry(String provider) {
        return retries.computeIfAbsent(provider, name -> {
            // Create provider-specific configuration if needed
            RetryConfig config = getProviderRetryConfig(name);
            Retry retry = config != null
                    ? retryRegistry.retry(name, config)
                    : retryRegistry.retry(name);
            
            LOGGER.debug("Created retry for geocoder: {}", name);
            return retry;
        });
    }

    /**
     * Get provider-specific circuit breaker configuration.
     * Override this method to provide custom configurations for specific providers.
     *
     * @param provider Geocoder provider name
     * @return Custom CircuitBreakerConfig or null to use defaults
     */
    protected CircuitBreakerConfig getProviderCircuitBreakerConfig(String provider) {
        // Provider-specific configurations
        switch (provider) {
            case "google":
                // Google has stricter rate limits, so we use a more conservative configuration
                return CircuitBreakerConfig.custom()
                        .failureRateThreshold(30) // More sensitive to failures
                        .waitDurationInOpenState(Duration.ofMinutes(1)) // Longer cool-down period
                        .permittedNumberOfCallsInHalfOpenState(3)
                        .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                        .slidingWindowSize(10)
                        .build();
            case "factual":
                // Factual-specific configuration
                return CircuitBreakerConfig.custom()
                        .failureRateThreshold(40)
                        .waitDurationInOpenState(Duration.ofSeconds(45))
                        .permittedNumberOfCallsInHalfOpenState(4)
                        .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                        .slidingWindowSize(10)
                        .build();
            default:
                // Use default configuration for other providers
                return null;
        }
    }

    /**
     * Get provider-specific retry configuration.
     * Override this method to provide custom configurations for specific providers.
     *
     * @param provider Geocoder provider name
     * @return Custom RetryConfig or null to use defaults
     */
    protected RetryConfig getProviderRetryConfig(String provider) {
        // Provider-specific configurations
        switch (provider) {
            case "google":
                // Google has stricter rate limits, so we use a more conservative retry policy
                return RetryConfig.custom()
                        .maxAttempts(2) // Fewer retries
                        .waitDuration(Duration.ofSeconds(1)) // Longer initial wait
                        .enableExponentialBackoff(true)
                        .exponentialBackoffMultiplier(2)
                        .build();
            case "factual":
                // Factual-specific configuration
                return RetryConfig.custom()
                        .maxAttempts(3)
                        .waitDuration(Duration.ofMillis(750)) // Longer initial wait
                        .enableExponentialBackoff(true)
                        .exponentialBackoffMultiplier(2)
                        .enableRandomizedWait(true)
                        .randomizedWaitFactor(0.3) // Less randomization
                        .build();
            default:
                // Use default configuration for other providers
                return null;
        }
    }
}