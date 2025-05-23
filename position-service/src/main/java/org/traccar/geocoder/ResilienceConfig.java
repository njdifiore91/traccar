/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides centralized configuration for resilience patterns used by geocoder implementations.
 * This includes circuit breakers, retry mechanisms, and fallback strategies.
 */
@Singleton
public class ResilienceConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResilienceConfig.class);

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final Map<String, CircuitBreaker> circuitBreakers = new ConcurrentHashMap<>();
    private final Map<String, Retry> retries = new ConcurrentHashMap<>();

    /**
     * Default constructor that initializes the circuit breaker and retry registries
     * with default configurations.
     */
    @Inject
    public ResilienceConfig() {
        // Create circuit breaker registry with default configuration
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to open circuit
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds in OPEN state
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10) // Count last 10 calls
                .minimumNumberOfCalls(5) // Minimum calls before calculating failure rate
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 calls in HALF_OPEN state
                .automaticTransitionFromOpenToHalfOpenEnabled(true) // Auto transition to HALF_OPEN
                .recordExceptions(Exception.class) // Record all exceptions as failures
                .build();

        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);

        // Create retry registry with default configuration
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Maximum 3 attempts
                .waitDuration(Duration.ofMillis(500)) // Initial wait duration
                .retryExceptions(Exception.class) // Retry on all exceptions
                .build();

        this.retryRegistry = RetryRegistry.of(retryConfig);
    }

    /**
     * Creates or retrieves a circuit breaker for the specified provider.
     * Each provider can have its own circuit breaker with custom configuration.
     *
     * @param providerName Name of the geocoding provider
     * @return CircuitBreaker instance for the provider
     */
    public CircuitBreaker createCircuitBreaker(String providerName) {
        return circuitBreakers.computeIfAbsent(providerName, name -> {
            LOGGER.info("Creating circuit breaker for geocoder provider: {}", name);
            
            // Provider-specific configurations can be applied here
            CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                    .failureRateThreshold(50)
                    .waitDurationInOpenState(Duration.ofSeconds(30))
                    .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                    .slidingWindowSize(10)
                    .minimumNumberOfCalls(5)
                    .permittedNumberOfCallsInHalfOpenState(3)
                    .automaticTransitionFromOpenToHalfOpenEnabled(true)
                    .recordExceptions(Exception.class);
            
            // Apply provider-specific customizations
            if ("tomtom".equals(name)) {
                // TomTom-specific configuration
                config.waitDurationInOpenState(Duration.ofSeconds(60)); // Longer wait for TomTom
            } else if ("google".equals(name)) {
                // Google-specific configuration
                config.failureRateThreshold(30); // More sensitive threshold for Google
            }
            
            return circuitBreakerRegistry.circuitBreaker(name, config.build());
        });
    }

    /**
     * Creates or retrieves a retry mechanism for the specified provider.
     * Each provider can have its own retry policy with custom configuration.
     *
     * @param providerName Name of the geocoding provider
     * @return Retry instance for the provider
     */
    public Retry createRetry(String providerName) {
        return retries.computeIfAbsent(providerName, name -> {
            LOGGER.info("Creating retry for geocoder provider: {}", name);
            
            // Provider-specific configurations can be applied here
            RetryConfig config = RetryConfig.custom()
                    .maxAttempts(3)
                    .waitDuration(Duration.ofMillis(500))
                    .retryExceptions(Exception.class);
            
            // Apply provider-specific customizations
            if ("tomtom".equals(name)) {
                // TomTom-specific configuration
                config.maxAttempts(4); // More retries for TomTom
            } else if ("google".equals(name)) {
                // Google-specific configuration
                config.waitDuration(Duration.ofSeconds(1)); // Longer wait between retries for Google
            }
            
            return retryRegistry.retry(name, config.build());
        });
    }
}