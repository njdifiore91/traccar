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
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Provides centralized configuration for resilience patterns used by geocoder implementations.
 * This includes circuit breakers, retry mechanisms, and fallback strategies.
 */
@Component
public class ResilienceConfig {

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final Map<String, CircuitBreaker> circuitBreakers = new HashMap<>();
    private final Map<String, Retry> retries = new HashMap<>();

    @Autowired
    public ResilienceConfig(
            MeterRegistry meterRegistry,
            @Value("${geocoder.circuitBreaker.slidingWindowSize:100}") int slidingWindowSize,
            @Value("${geocoder.circuitBreaker.failureRateThreshold:50}") float failureRateThreshold,
            @Value("${geocoder.circuitBreaker.waitDurationInOpenState:30s}") Duration waitDurationInOpenState,
            @Value("${geocoder.circuitBreaker.permittedNumberOfCallsInHalfOpenState:10}") int permittedNumberOfCallsInHalfOpenState,
            @Value("${geocoder.retry.maxAttempts:3}") int maxAttempts,
            @Value("${geocoder.retry.waitDuration:1s}") Duration waitDuration) {

        // Configure default circuit breaker settings
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(slidingWindowSize)
                .failureRateThreshold(failureRateThreshold)
                .waitDurationInOpenState(waitDurationInOpenState)
                .permittedNumberOfCallsInHalfOpenState(permittedNumberOfCallsInHalfOpenState)
                .recordExceptions(IOException.class, TimeoutException.class, RuntimeException.class)
                .build();

        // Configure default retry settings with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(maxAttempts)
                .waitDuration(waitDuration)
                .retryExceptions(IOException.class, TimeoutException.class)
                .ignoreExceptions(IllegalArgumentException.class)
                .enableExponentialBackoff(true)
                .exponentialBackoffMultiplier(2)
                .enableRandomizedWait(true)
                .randomizedWaitFactor(0.5)
                .build();

        // Create registries with metrics integration
        circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        retryRegistry = RetryRegistry.of(retryConfig);

        // Register with metrics registry
        circuitBreakerRegistry.getEventPublisher().onEntryAdded(event -> 
                event.getAddedEntry().getEventPublisher().onStateTransition(state -> 
                        meterRegistry.counter(
                                "resilience4j.circuitbreaker.state", 
                                "name", event.getAddedEntry().getName(),
                                "state", state.getStateTransition().getToState().name())
                                .increment()));

        retryRegistry.getEventPublisher().onEntryAdded(event -> 
                event.getAddedEntry().getEventPublisher().onRetry(retry -> 
                        meterRegistry.counter(
                                "resilience4j.retry.calls", 
                                "name", event.getAddedEntry().getName(),
                                "kind", "retry")
                                .increment()));
    }

    /**
     * Gets a circuit breaker for the specified geocoder provider.
     * Creates a new one if it doesn't exist.
     *
     * @param provider the geocoder provider name (e.g., "geoapify", "google")
     * @return the circuit breaker instance
     */
    public CircuitBreaker getCircuitBreaker(String provider) {
        return circuitBreakers.computeIfAbsent(provider, 
                name -> circuitBreakerRegistry.circuitBreaker("geocoder." + name));
    }

    /**
     * Gets a retry for the specified geocoder provider.
     * Creates a new one if it doesn't exist.
     *
     * @param provider the geocoder provider name (e.g., "geoapify", "google")
     * @return the retry instance
     */
    public Retry getRetry(String provider) {
        return retries.computeIfAbsent(provider, 
                name -> retryRegistry.retry("geocoder." + name));
    }

    /**
     * Configures a provider-specific circuit breaker with custom settings.
     *
     * @param provider the geocoder provider name
     * @param config the custom circuit breaker configuration
     * @return the configured circuit breaker
     */
    public CircuitBreaker configureCircuitBreaker(String provider, CircuitBreakerConfig config) {
        CircuitBreaker circuitBreaker = CircuitBreaker.of("geocoder." + provider, config);
        circuitBreakers.put(provider, circuitBreaker);
        return circuitBreaker;
    }

    /**
     * Configures a provider-specific retry with custom settings.
     *
     * @param provider the geocoder provider name
     * @param config the custom retry configuration
     * @return the configured retry
     */
    public Retry configureRetry(String provider, RetryConfig config) {
        Retry retry = Retry.of("geocoder." + provider, config);
        retries.put(provider, retry);
        return retry;
    }

    /**
     * Gets the health status of all circuit breakers.
     *
     * @return a map of provider names to their circuit breaker states
     */
    public Map<String, CircuitBreaker.State> getCircuitBreakerStates() {
        Map<String, CircuitBreaker.State> states = new HashMap<>();
        circuitBreakers.forEach((provider, circuitBreaker) -> 
                states.put(provider, circuitBreaker.getState()));
        return states;
    }

    /**
     * Gets metrics for all circuit breakers.
     *
     * @return a map of provider names to their circuit breaker metrics
     */
    public Map<String, Map<String, Float>> getCircuitBreakerMetrics() {
        Map<String, Map<String, Float>> metrics = new HashMap<>();
        circuitBreakers.forEach((provider, circuitBreaker) -> {
            Map<String, Float> providerMetrics = new HashMap<>();
            providerMetrics.put("failureRate", circuitBreaker.getMetrics().getFailureRate());
            providerMetrics.put("slowCallRate", circuitBreaker.getMetrics().getSlowCallRate());
            providerMetrics.put("numberOfBufferedCalls", (float) circuitBreaker.getMetrics().getNumberOfBufferedCalls());
            providerMetrics.put("numberOfFailedCalls", (float) circuitBreaker.getMetrics().getNumberOfFailedCalls());
            providerMetrics.put("numberOfSlowCalls", (float) circuitBreaker.getMetrics().getNumberOfSlowCalls());
            metrics.put(provider, providerMetrics);
        });
        return metrics;
    }
}