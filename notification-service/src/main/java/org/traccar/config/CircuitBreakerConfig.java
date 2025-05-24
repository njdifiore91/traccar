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
package org.traccar.config;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Configuration class for circuit breakers in the Notification Service.
 * This class configures resilience patterns like circuit breakers, retries,
 * and time limiters to prevent cascading failures and improve system stability.
 */
@Configuration
public class CircuitBreakerConfig {

    @Value("${resilience.circuitbreaker.slidingWindowSize:100}")
    private int slidingWindowSize;

    @Value("${resilience.circuitbreaker.failureRateThreshold:50}")
    private float failureRateThreshold;

    @Value("${resilience.circuitbreaker.waitDurationInOpenState:60000}")
    private long waitDurationInOpenState;

    @Value("${resilience.circuitbreaker.permittedNumberOfCallsInHalfOpenState:10}")
    private int permittedNumberOfCallsInHalfOpenState;

    @Value("${resilience.retry.maxAttempts:3}")
    private int maxRetryAttempts;

    @Value("${resilience.retry.waitDuration:1000}")
    private long waitDuration;

    @Value("${resilience.timelimiter.timeoutDuration:5000}")
    private long timeoutDuration;

    /**
     * Configure circuit breaker registry with default settings.
     */
    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry(MeterRegistry meterRegistry) {
        io.github.resilience4j.circuitbreaker.CircuitBreakerConfig circuitBreakerConfig = 
                io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                .slidingWindowType(SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(slidingWindowSize)
                .failureRateThreshold(failureRateThreshold)
                .waitDurationInOpenState(Duration.ofMillis(waitDurationInOpenState))
                .permittedNumberOfCallsInHalfOpenState(permittedNumberOfCallsInHalfOpenState)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();

        // Create registry with custom configuration
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        
        // Register specific circuit breakers with custom configurations
        registry.circuitBreaker("emailNotificator", circuitBreakerConfig);
        registry.circuitBreaker("smsNotificator", circuitBreakerConfig);
        registry.circuitBreaker("pushNotificator", circuitBreakerConfig);
        registry.circuitBreaker("telegramNotificator", 
                io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.from(circuitBreakerConfig)
                .waitDurationInOpenState(Duration.ofMillis(120000)) // Longer wait for external services
                .build());
        
        // Register metrics
        registry.getEventPublisher().onStateTransition(
                event -> meterRegistry.counter(
                        "resilience4j.circuitbreaker.state", 
                        "name", event.getCircuitBreakerName(),
                        "state", event.getStateTransition().toString())
                .increment());
        
        return registry;
    }

    /**
     * Configure retry registry with default settings.
     */
    @Bean
    public RetryRegistry retryRegistry(MeterRegistry meterRegistry) {
        io.github.resilience4j.retry.RetryConfig retryConfig = 
                io.github.resilience4j.retry.RetryConfig.custom()
                .maxAttempts(maxRetryAttempts)
                .waitDuration(Duration.ofMillis(waitDuration))
                .retryExceptions(Exception.class)
                .ignoreExceptions(IllegalArgumentException.class, IllegalStateException.class)
                .build();

        RetryRegistry registry = RetryRegistry.of(retryConfig);
        
        // Register specific retry configurations
        registry.retry("notificationDelivery", retryConfig);
        registry.retry("externalServiceCall", 
                io.github.resilience4j.retry.RetryConfig.from(retryConfig)
                .waitDuration(Duration.ofMillis(2000)) // Longer wait for external services
                .build());
        
        // Register metrics
        registry.getEventPublisher().onRetry(
                event -> meterRegistry.counter(
                        "resilience4j.retry.calls", 
                        "name", event.getName(),
                        "successful", String.valueOf(event.getNumberOfRetryAttempts() < maxRetryAttempts))
                .increment());
        
        return registry;
    }

    /**
     * Configure time limiter registry with default settings.
     */
    @Bean
    public TimeLimiterRegistry timeLimiterRegistry(MeterRegistry meterRegistry) {
        io.github.resilience4j.timelimiter.TimeLimiterConfig timeLimiterConfig = 
                io.github.resilience4j.timelimiter.TimeLimiterConfig.custom()
                .timeoutDuration(Duration.ofMillis(timeoutDuration))
                .cancelRunningFuture(true)
                .build();

        TimeLimiterRegistry registry = TimeLimiterRegistry.of(timeLimiterConfig);
        
        // Register specific time limiter configurations
        registry.timeLimiter("notificationDelivery", timeLimiterConfig);
        registry.timeLimiter("externalServiceCall", 
                io.github.resilience4j.timelimiter.TimeLimiterConfig.from(timeLimiterConfig)
                .timeoutDuration(Duration.ofMillis(10000)) // Longer timeout for external services
                .build());
        
        // Register metrics
        registry.getEventPublisher().onTimeout(
                event -> meterRegistry.counter(
                        "resilience4j.timelimiter.timeout", 
                        "name", event.getTimeLimiterName())
                .increment());
        
        return registry;
    }
}