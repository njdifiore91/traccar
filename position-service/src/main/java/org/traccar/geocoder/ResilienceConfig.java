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

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Provides centralized configuration for resilience patterns used by geocoder implementations.
 * Configures circuit breakers, retry mechanisms with exponential backoff, and fallback strategies
 * for external geocoding service calls.
 */
public class ResilienceConfig {

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final Map<String, CircuitBreaker> circuitBreakers;
    private final Map<String, Retry> retries;
    private final MeterRegistry meterRegistry;

    /**
     * Creates a new ResilienceConfig with default settings.
     * 
     * @param meterRegistry The meter registry for metrics collection
     */
    public ResilienceConfig(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();
        this.retryRegistry = RetryRegistry.ofDefaults();
        this.circuitBreakers = new ConcurrentHashMap<>();
        this.retries = new ConcurrentHashMap<>();
        
        // Register metrics with Micrometer
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(circuitBreakerRegistry)
                .bindTo(meterRegistry);
    }

    /**
     * Creates a circuit breaker for a specific geocoding provider with default configuration.
     * 
     * @param providerName The name of the geocoding provider
     * @return The circuit breaker instance
     */
    public CircuitBreaker createCircuitBreaker(String providerName) {
        return circuitBreakers.computeIfAbsent(providerName, name -> {
            CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                    .failureRateThreshold(50) // 50% failure rate to open circuit
                    .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds in OPEN state
                    .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                    .slidingWindowSize(10) // Consider last 10 calls
                    .minimumNumberOfCalls(5) // Minimum calls before calculating failure rate
                    .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 calls in HALF_OPEN state
                    .build();
            return circuitBreakerRegistry.circuitBreaker(name, config);
        });
    }

    /**
     * Creates a circuit breaker for a specific geocoding provider with custom configuration.
     * 
     * @param providerName The name of the geocoding provider
     * @param failureRateThreshold Percentage threshold above which the circuit breaker should trip open
     * @param waitDurationInOpenState Duration to wait before transitioning from OPEN to HALF_OPEN
     * @param slidingWindowSize Number of calls to consider for failure rate calculation
     * @return The circuit breaker instance
     */
    public CircuitBreaker createCircuitBreaker(
            String providerName, 
            float failureRateThreshold, 
            Duration waitDurationInOpenState, 
            int slidingWindowSize) {
        
        return circuitBreakers.computeIfAbsent(providerName, name -> {
            CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                    .failureRateThreshold(failureRateThreshold)
                    .waitDurationInOpenState(waitDurationInOpenState)
                    .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                    .slidingWindowSize(slidingWindowSize)
                    .minimumNumberOfCalls(Math.max(1, slidingWindowSize / 2))
                    .permittedNumberOfCallsInHalfOpenState(Math.max(1, slidingWindowSize / 3))
                    .build();
            return circuitBreakerRegistry.circuitBreaker(name, config);
        });
    }

    /**
     * Creates a retry mechanism for a specific geocoding provider with default configuration.
     * 
     * @param providerName The name of the geocoding provider
     * @return The retry instance
     */
    public Retry createRetry(String providerName) {
        return retries.computeIfAbsent(providerName, name -> {
            RetryConfig config = RetryConfig.custom()
                    .maxAttempts(3) // Maximum 3 attempts
                    .waitDuration(Duration.ofMillis(500)) // Initial wait duration
                    .retryExceptions(Exception.class) // Retry on all exceptions
                    .ignoreExceptions(GeocoderException.class) // Don't retry on GeocoderException
                    .enableExponentialBackoff(true) // Use exponential backoff
                    .exponentialBackoffMultiplier(2) // Double the wait time for each retry
                    .build();
            return retryRegistry.retry(name, config);
        });
    }

    /**
     * Creates a retry mechanism for a specific geocoding provider with custom configuration.
     * 
     * @param providerName The name of the geocoding provider
     * @param maxAttempts Maximum number of retry attempts
     * @param waitDuration Initial wait duration before retrying
     * @param enableExponentialBackoff Whether to use exponential backoff
     * @return The retry instance
     */
    public Retry createRetry(
            String providerName, 
            int maxAttempts, 
            Duration waitDuration, 
            boolean enableExponentialBackoff) {
        
        return retries.computeIfAbsent(providerName, name -> {
            RetryConfig.Builder<Object> builder = RetryConfig.custom()
                    .maxAttempts(maxAttempts)
                    .waitDuration(waitDuration)
                    .retryExceptions(Exception.class)
                    .ignoreExceptions(GeocoderException.class);
            
            if (enableExponentialBackoff) {
                builder.enableExponentialBackoff(true)
                       .exponentialBackoffMultiplier(2);
            }
            
            return retryRegistry.retry(name, builder.build());
        });
    }

    /**
     * Executes a supplier with circuit breaker and retry protection.
     * 
     * @param <T> The return type of the supplier
     * @param providerName The name of the geocoding provider
     * @param supplier The supplier to execute
     * @param fallback The fallback supplier to use when the circuit is open or retries are exhausted
     * @return The result of the supplier or fallback
     */
    public <T> T executeWithResiliencePattern(
            String providerName, 
            Supplier<T> supplier, 
            Supplier<T> fallback) {
        
        CircuitBreaker circuitBreaker = createCircuitBreaker(providerName);
        Retry retry = createRetry(providerName);
        
        Supplier<T> decoratedSupplier = CircuitBreaker.decorateSupplier(circuitBreaker, supplier);
        decoratedSupplier = Retry.decorateSupplier(retry, decoratedSupplier);
        
        try {
            return decoratedSupplier.get();
        } catch (Exception e) {
            return fallback.get();
        }
    }

    /**
     * Gets the circuit breaker registry for advanced configuration.
     * 
     * @return The circuit breaker registry
     */
    public CircuitBreakerRegistry getCircuitBreakerRegistry() {
        return circuitBreakerRegistry;
    }

    /**
     * Gets the retry registry for advanced configuration.
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
}