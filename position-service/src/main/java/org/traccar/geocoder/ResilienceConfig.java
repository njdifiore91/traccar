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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Configuration class for resilience patterns used by geocoder implementations.
 * This class provides centralized configuration for circuit breakers and retry mechanisms.
 */
@Configuration
public class ResilienceConfig {

    @Value("${geocoder.circuitBreaker.failureRateThreshold:50}")
    private float failureRateThreshold;

    @Value("${geocoder.circuitBreaker.waitDurationInOpenState:30000}")
    private long waitDurationInOpenState;

    @Value("${geocoder.circuitBreaker.permittedNumberOfCallsInHalfOpenState:10}")
    private int permittedNumberOfCallsInHalfOpenState;

    @Value("${geocoder.circuitBreaker.slidingWindowSize:100}")
    private int slidingWindowSize;

    @Value("${geocoder.retry.maxAttempts:3}")
    private int maxRetryAttempts;

    @Value("${geocoder.retry.waitDuration:1000}")
    private long waitDuration;

    @Value("${geocoder.retry.enableExponentialBackoff:true}")
    private boolean enableExponentialBackoff;

    @Value("${geocoder.retry.exponentialBackoffMultiplier:2}")
    private double exponentialBackoffMultiplier;

    @Value("${geocoder.retry.enableRandomizedWait:true}")
    private boolean enableRandomizedWait;

    @Value("${geocoder.retry.randomizedWaitFactor:0.5}")
    private double randomizedWaitFactor;

    /**
     * Creates a CircuitBreakerRegistry with default configuration for geocoding services.
     *
     * @return The CircuitBreakerRegistry
     */
    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry() {
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(failureRateThreshold)
                .waitDurationInOpenState(Duration.ofMillis(waitDurationInOpenState))
                .permittedNumberOfCallsInHalfOpenState(permittedNumberOfCallsInHalfOpenState)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(slidingWindowSize)
                .recordExceptions(
                        IOException.class,
                        ConnectException.class,
                        TimeoutException.class,
                        SocketTimeoutException.class,
                        GeocoderException.class
                )
                .build();

        return CircuitBreakerRegistry.of(circuitBreakerConfig);
    }

    /**
     * Creates a RetryRegistry with default configuration for geocoding services.
     *
     * @return The RetryRegistry
     */
    @Bean
    public RetryRegistry retryRegistry() {
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(maxRetryAttempts)
                .waitDuration(Duration.ofMillis(waitDuration))
                .retryExceptions(
                        IOException.class,
                        ConnectException.class,
                        TimeoutException.class,
                        SocketTimeoutException.class
                )
                .ignoreExceptions(
                        GeocoderException.class
                )
                .enableExponentialBackoff(enableExponentialBackoff)
                .exponentialBackoffMultiplier(exponentialBackoffMultiplier)
                .enableRandomizedWait(enableRandomizedWait)
                .randomizedWaitFactor(randomizedWaitFactor)
                .build();

        return RetryRegistry.of(retryConfig);
    }

    /**
     * Creates a CircuitBreaker for the BingMapsGeocoder with custom configuration if needed.
     *
     * @param registry The CircuitBreakerRegistry
     * @return The CircuitBreaker for BingMapsGeocoder
     */
    @Bean
    public CircuitBreaker bingMapsGeocoderCircuitBreaker(CircuitBreakerRegistry registry) {
        return registry.circuitBreaker("bingMapsGeocoder");
    }

    /**
     * Creates a Retry for the BingMapsGeocoder with custom configuration if needed.
     *
     * @param registry The RetryRegistry
     * @return The Retry for BingMapsGeocoder
     */
    @Bean
    public Retry bingMapsGeocoderRetry(RetryRegistry registry) {
        return registry.retry("bingMapsGeocoder");
    }
}