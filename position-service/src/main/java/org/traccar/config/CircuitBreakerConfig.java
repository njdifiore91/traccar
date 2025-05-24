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
package org.traccar.config;

import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.registry.EntryAddedEvent;
import io.github.resilience4j.core.registry.EntryRemovedEvent;
import io.github.resilience4j.core.registry.EntryReplacedEvent;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Configuration for Resilience4j circuit breaker patterns in the Position Processing Service.
 * This class defines circuit breaker configurations for different types of external service calls,
 * with appropriate thresholds, timeouts, and fallback mechanisms to prevent cascading failures
 * when external services are unavailable.
 */
@Configuration
public class CircuitBreakerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(CircuitBreakerConfig.class);

    /**
     * Creates a CircuitBreakerRegistry with event logging.
     * 
     * @param meterRegistry The meter registry for metrics collection
     * @return The circuit breaker registry
     */
    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry(MeterRegistry meterRegistry) {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(
                io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                        .slidingWindowSize(10)
                        .failureRateThreshold(50)
                        .waitDurationInOpenState(Duration.ofSeconds(10))
                        .permittedNumberOfCallsInHalfOpenState(5)
                        .slowCallRateThreshold(50)
                        .slowCallDurationThreshold(Duration.ofSeconds(1))
                        .build());
        
        registry.getEventPublisher()
                .onEntryAdded(event -> LOGGER.info("CircuitBreaker '{}' added", event.getAddedEntry().getName()))
                .onEntryRemoved(event -> LOGGER.info("CircuitBreaker '{}' removed", event.getRemovedEntry().getName()))
                .onEntryReplaced(event -> LOGGER.info("CircuitBreaker '{}' replaced", event.getNewEntry().getName()));
        
        return registry;
    }

    /**
     * Creates a RetryRegistry with exponential backoff configuration.
     * 
     * @param meterRegistry The meter registry for metrics collection
     * @return The retry registry
     */
    @Bean
    public RetryRegistry retryRegistry(MeterRegistry meterRegistry) {
        RetryRegistry registry = RetryRegistry.of(
                RetryConfig.custom()
                        .maxAttempts(3)
                        .waitDuration(Duration.ofMillis(500))
                        .retryExceptions(Exception.class)
                        .ignoreExceptions(IllegalArgumentException.class)
                        .build());
        
        registry.getEventPublisher()
                .onEntryAdded(event -> LOGGER.info("Retry '{}' added", event.getAddedEntry().getName()))
                .onEntryRemoved(event -> LOGGER.info("Retry '{}' removed", event.getRemovedEntry().getName()))
                .onEntryReplaced(event -> LOGGER.info("Retry '{}' replaced", event.getNewEntry().getName()));
        
        return registry;
    }

    /**
     * Creates a BulkheadRegistry to limit concurrent calls to external services.
     * 
     * @param meterRegistry The meter registry for metrics collection
     * @return The bulkhead registry
     */
    @Bean
    public BulkheadRegistry bulkheadRegistry(MeterRegistry meterRegistry) {
        BulkheadRegistry registry = BulkheadRegistry.of(
                BulkheadConfig.custom()
                        .maxConcurrentCalls(10)
                        .maxWaitDuration(Duration.ofMillis(500))
                        .build());
        
        registry.getEventPublisher()
                .onEntryAdded(event -> LOGGER.info("Bulkhead '{}' added", event.getAddedEntry().getName()))
                .onEntryRemoved(event -> LOGGER.info("Bulkhead '{}' removed", event.getRemovedEntry().getName()))
                .onEntryReplaced(event -> LOGGER.info("Bulkhead '{}' replaced", event.getNewEntry().getName()));
        
        return registry;
    }

    /**
     * Creates a RateLimiterRegistry to prevent overwhelming external services.
     * 
     * @param meterRegistry The meter registry for metrics collection
     * @return The rate limiter registry
     */
    @Bean
    public RateLimiterRegistry rateLimiterRegistry(MeterRegistry meterRegistry) {
        RateLimiterRegistry registry = RateLimiterRegistry.of(
                RateLimiterConfig.custom()
                        .limitRefreshPeriod(Duration.ofSeconds(1))
                        .limitForPeriod(50)
                        .timeoutDuration(Duration.ofMillis(500))
                        .build());
        
        registry.getEventPublisher()
                .onEntryAdded(event -> LOGGER.info("RateLimiter '{}' added", event.getAddedEntry().getName()))
                .onEntryRemoved(event -> LOGGER.info("RateLimiter '{}' removed", event.getRemovedEntry().getName()))
                .onEntryReplaced(event -> LOGGER.info("RateLimiter '{}' replaced", event.getNewEntry().getName()));
        
        return registry;
    }

    /**
     * Configures a circuit breaker for geocoding service calls.
     * 
     * @param registry The circuit breaker registry
     * @return The geocoding service circuit breaker
     */
    @Bean
    public CircuitBreaker geocodingServiceCircuitBreaker(CircuitBreakerRegistry registry) {
        return registry.circuitBreaker("geocodingService", 
                io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                        .slidingWindowSize(10)
                        .failureRateThreshold(50)
                        .waitDurationInOpenState(Duration.ofSeconds(30))
                        .permittedNumberOfCallsInHalfOpenState(3)
                        .slowCallRateThreshold(50)
                        .slowCallDurationThreshold(Duration.ofSeconds(2))
                        .build());
    }

    /**
     * Configures a circuit breaker for database service calls.
     * 
     * @param registry The circuit breaker registry
     * @return The database service circuit breaker
     */
    @Bean
    public CircuitBreaker databaseServiceCircuitBreaker(CircuitBreakerRegistry registry) {
        return registry.circuitBreaker("databaseService", 
                io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                        .slidingWindowSize(10)
                        .failureRateThreshold(40)
                        .waitDurationInOpenState(Duration.ofSeconds(20))
                        .permittedNumberOfCallsInHalfOpenState(5)
                        .slowCallRateThreshold(40)
                        .slowCallDurationThreshold(Duration.ofSeconds(1))
                        .build());
    }

    /**
     * Configures a circuit breaker for geolocation service calls.
     * 
     * @param registry The circuit breaker registry
     * @return The geolocation service circuit breaker
     */
    @Bean
    public CircuitBreaker geolocationServiceCircuitBreaker(CircuitBreakerRegistry registry) {
        return registry.circuitBreaker("geolocationService", 
                io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                        .slidingWindowSize(10)
                        .failureRateThreshold(50)
                        .waitDurationInOpenState(Duration.ofSeconds(30))
                        .permittedNumberOfCallsInHalfOpenState(3)
                        .slowCallRateThreshold(50)
                        .slowCallDurationThreshold(Duration.ofSeconds(2))
                        .build());
    }

    /**
     * Configures a circuit breaker for speed limit service calls.
     * 
     * @param registry The circuit breaker registry
     * @return The speed limit service circuit breaker
     */
    @Bean
    public CircuitBreaker speedLimitServiceCircuitBreaker(CircuitBreakerRegistry registry) {
        return registry.circuitBreaker("speedLimitService", 
                io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                        .slidingWindowSize(10)
                        .failureRateThreshold(50)
                        .waitDurationInOpenState(Duration.ofSeconds(30))
                        .permittedNumberOfCallsInHalfOpenState(3)
                        .slowCallRateThreshold(50)
                        .slowCallDurationThreshold(Duration.ofSeconds(2))
                        .build());
    }

    /**
     * Configures a circuit breaker for messaging service calls.
     * 
     * @param registry The circuit breaker registry
     * @return The messaging service circuit breaker
     */
    @Bean
    public CircuitBreaker messagingServiceCircuitBreaker(CircuitBreakerRegistry registry) {
        return registry.circuitBreaker("messagingService", 
                io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                        .slidingWindowSize(10)
                        .failureRateThreshold(30)
                        .waitDurationInOpenState(Duration.ofSeconds(15))
                        .permittedNumberOfCallsInHalfOpenState(5)
                        .slowCallRateThreshold(30)
                        .slowCallDurationThreshold(Duration.ofMillis(500))
                        .build());
    }

    /**
     * Registry event consumer for circuit breaker events to enable metrics collection.
     * 
     * @return The registry event consumer
     */
    @Bean
    public RegistryEventConsumer<CircuitBreaker> circuitBreakerEventConsumer() {
        return new RegistryEventConsumer<CircuitBreaker>() {
            @Override
            public void onEntryAddedEvent(EntryAddedEvent<CircuitBreaker> entryAddedEvent) {
                CircuitBreaker circuitBreaker = entryAddedEvent.getAddedEntry();
                circuitBreaker.getEventPublisher()
                        .onSuccess(event -> LOGGER.debug("Call success via circuit breaker '{}'", circuitBreaker.getName()))
                        .onError(event -> LOGGER.error("Call error via circuit breaker '{}': {}", 
                                circuitBreaker.getName(), event.getThrowable().getMessage()))
                        .onStateTransition(event -> LOGGER.info("Circuit breaker '{}' state changed from {} to {}", 
                                circuitBreaker.getName(), event.getStateTransition().getFromState(), 
                                event.getStateTransition().getToState()))
                        .onSlowCallRateExceeded(event -> LOGGER.warn("Slow call rate exceeded for circuit breaker '{}'", 
                                circuitBreaker.getName()))
                        .onFailureRateExceeded(event -> LOGGER.warn("Failure rate exceeded for circuit breaker '{}'", 
                                circuitBreaker.getName()));
            }

            @Override
            public void onEntryRemovedEvent(EntryRemovedEvent<CircuitBreaker> entryRemoveEvent) {
                // Do nothing
            }

            @Override
            public void onEntryReplacedEvent(EntryReplacedEvent<CircuitBreaker> entryReplacedEvent) {
                // Do nothing
            }
        };
    }
}