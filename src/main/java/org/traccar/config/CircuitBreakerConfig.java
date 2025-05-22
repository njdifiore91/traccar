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
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;

import java.util.function.Function;
import java.util.function.Supplier;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Duration;

/**
 * Configuration for resilience patterns in the microservices architecture.
 * This class provides configuration options for circuit breakers, bulkheads,
 * rate limiters, and retry mechanisms using the Resilience4j framework.
 */
@Singleton
public class CircuitBreakerConfig {

    private final Config config;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final BulkheadRegistry bulkheadRegistry;
    private final RateLimiterRegistry rateLimiterRegistry;
    private final RetryRegistry retryRegistry;

    /**
     * Constructs a new CircuitBreakerConfig with the provided Config instance.
     * Initializes all registries with default configurations.
     *
     * @param config The Config instance to retrieve configuration values from
     */
    @Inject
    public CircuitBreakerConfig(Config config) {
        this.config = config;
        this.circuitBreakerRegistry = createCircuitBreakerRegistry();
        this.bulkheadRegistry = createBulkheadRegistry();
        this.rateLimiterRegistry = createRateLimiterRegistry();
        this.retryRegistry = createRetryRegistry();
    }

    /**
     * Creates and configures the CircuitBreakerRegistry with default settings.
     *
     * @return The configured CircuitBreakerRegistry
     */
    private CircuitBreakerRegistry createCircuitBreakerRegistry() {
        CircuitBreakerConfig.Builder builder = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Default 50% failure rate threshold
                .slowCallRateThreshold(50) // Default 50% slow call rate threshold
                .slowCallDurationThreshold(Duration.ofSeconds(2)) // Default 2 seconds threshold for slow calls
                .permittedNumberOfCallsInHalfOpenState(10) // Default 10 calls in half-open state
                .slidingWindowSize(100) // Default sliding window size of 100 calls
                .minimumNumberOfCalls(10) // Default minimum number of calls before calculating failure rate
                .waitDurationInOpenState(Duration.ofSeconds(60)); // Default 60 seconds wait duration in open state

        return CircuitBreakerRegistry.of(builder.build());
    }

    /**
     * Creates and configures the BulkheadRegistry with default settings.
     *
     * @return The configured BulkheadRegistry
     */
    private BulkheadRegistry createBulkheadRegistry() {
        BulkheadConfig.Builder builder = BulkheadConfig.custom()
                .maxConcurrentCalls(25) // Default maximum of 25 concurrent calls
                .maxWaitDuration(Duration.ofMillis(500)); // Default maximum wait duration of 500ms

        return BulkheadRegistry.of(builder.build());
    }

    /**
     * Creates and configures the RateLimiterRegistry with default settings.
     *
     * @return The configured RateLimiterRegistry
     */
    private RateLimiterRegistry createRateLimiterRegistry() {
        RateLimiterConfig.Builder builder = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofSeconds(1)) // Default refresh period of 1 second
                .limitForPeriod(50) // Default limit of 50 calls per period
                .timeoutDuration(Duration.ofMillis(500)); // Default timeout duration of 500ms

        return RateLimiterRegistry.of(builder.build());
    }

    /**
     * Creates and configures the RetryRegistry with default settings.
     *
     * @return The configured RetryRegistry
     */
    private RetryRegistry createRetryRegistry() {
        RetryConfig.Builder builder = RetryConfig.custom()
                .maxAttempts(3) // Default maximum of 3 retry attempts
                .waitDuration(Duration.ofMillis(500)) // Default wait duration of 500ms between retries
                .retryExceptions(Exception.class) // Default retry on all exceptions
                .enableExponentialBackoff(true) // Enable exponential backoff by default
                .exponentialBackoffMultiplier(2); // Default multiplier of 2 for exponential backoff

        return RetryRegistry.of(builder.build());
    }

    /**
     * Gets the CircuitBreakerRegistry instance.
     *
     * @return The CircuitBreakerRegistry
     */
    public CircuitBreakerRegistry getCircuitBreakerRegistry() {
        return circuitBreakerRegistry;
    }

    /**
     * Gets the BulkheadRegistry instance.
     *
     * @return The BulkheadRegistry
     */
    public BulkheadRegistry getBulkheadRegistry() {
        return bulkheadRegistry;
    }

    /**
     * Gets the RateLimiterRegistry instance.
     *
     * @return The RateLimiterRegistry
     */
    public RateLimiterRegistry getRateLimiterRegistry() {
        return rateLimiterRegistry;
    }

    /**
     * Gets the RetryRegistry instance.
     *
     * @return The RetryRegistry
     */
    public RetryRegistry getRetryRegistry() {
        return retryRegistry;
    }
    
    /**
     * Decorates a function with a circuit breaker.
     *
     * @param <T> The type of the input to the function
     * @param <R> The type of the result of the function
     * @param circuitBreaker The circuit breaker to use
     * @param function The function to decorate
     * @return The decorated function
     */
    public <T, R> Function<T, R> decorateFunction(
            io.github.resilience4j.circuitbreaker.CircuitBreaker circuitBreaker,
            Function<T, R> function) {
        return io.github.resilience4j.circuitbreaker.CircuitBreaker.decorateFunction(circuitBreaker, function);
    }
    
    /**
     * Decorates a supplier with a circuit breaker.
     *
     * @param <T> The type of the result of the supplier
     * @param circuitBreaker The circuit breaker to use
     * @param supplier The supplier to decorate
     * @return The decorated supplier
     */
    public <T> Supplier<T> decorateSupplier(
            io.github.resilience4j.circuitbreaker.CircuitBreaker circuitBreaker,
            Supplier<T> supplier) {
        return io.github.resilience4j.circuitbreaker.CircuitBreaker.decorateSupplier(circuitBreaker, supplier);
    }
    
    /**
     * Decorates a supplier with a bulkhead.
     *
     * @param <T> The type of the result of the supplier
     * @param bulkhead The bulkhead to use
     * @param supplier The supplier to decorate
     * @return The decorated supplier
     */
    public <T> Supplier<T> decorateSupplierWithBulkhead(
            io.github.resilience4j.bulkhead.Bulkhead bulkhead,
            Supplier<T> supplier) {
        return io.github.resilience4j.bulkhead.Bulkhead.decorateSupplier(bulkhead, supplier);
    }
    
    /**
     * Decorates a supplier with a rate limiter.
     *
     * @param <T> The type of the result of the supplier
     * @param rateLimiter The rate limiter to use
     * @param supplier The supplier to decorate
     * @return The decorated supplier
     */
    public <T> Supplier<T> decorateSupplierWithRateLimiter(
            io.github.resilience4j.ratelimiter.RateLimiter rateLimiter,
            Supplier<T> supplier) {
        return io.github.resilience4j.ratelimiter.RateLimiter.decorateSupplier(rateLimiter, supplier);
    }
    
    /**
     * Decorates a supplier with a retry mechanism.
     *
     * @param <T> The type of the result of the supplier
     * @param retry The retry to use
     * @param supplier The supplier to decorate
     * @return The decorated supplier
     */
    public <T> Supplier<T> decorateSupplierWithRetry(
            io.github.resilience4j.retry.Retry retry,
            Supplier<T> supplier) {
        return io.github.resilience4j.retry.Retry.decorateSupplier(retry, supplier);
    }

    /**
     * Creates a custom CircuitBreakerConfig for a specific service.
     *
     * @param serviceName The name of the service to create a circuit breaker for
     * @return The CircuitBreakerConfig for the specified service
     */
    /**
     * Creates a custom CircuitBreakerConfig for a specific service.
     *
     * @param serviceName The name of the service to create a circuit breaker for
     * @return The CircuitBreakerConfig for the specified service
     */
    public io.github.resilience4j.circuitbreaker.CircuitBreaker createCircuitBreaker(String serviceName) {
        String configPrefix = "circuitbreaker." + serviceName + ".";
        
        CircuitBreakerConfig.Builder builder = CircuitBreakerConfig.custom()
                .failureRateThreshold(config.getFloat(new FloatConfigKey(configPrefix + "failureRateThreshold", 50.0f)))
                .slowCallRateThreshold(config.getFloat(new FloatConfigKey(configPrefix + "slowCallRateThreshold", 50.0f)))
                .slowCallDurationThreshold(Duration.ofMillis(config.getLong(new LongConfigKey(configPrefix + "slowCallDurationThreshold", 2000L))))
                .permittedNumberOfCallsInHalfOpenState(config.getInteger(new IntegerConfigKey(configPrefix + "permittedNumberOfCallsInHalfOpenState", 10)))
                .slidingWindowSize(config.getInteger(new IntegerConfigKey(configPrefix + "slidingWindowSize", 100)))
                .minimumNumberOfCalls(config.getInteger(new IntegerConfigKey(configPrefix + "minimumNumberOfCalls", 10)))
                .waitDurationInOpenState(Duration.ofMillis(config.getLong(new LongConfigKey(configPrefix + "waitDurationInOpenState", 60000L))));

        return circuitBreakerRegistry.circuitBreaker(serviceName, builder.build());
    }

    /**
     * Creates a custom Bulkhead for a specific service.
     *
     * @param serviceName The name of the service to create a bulkhead for
     * @return The Bulkhead for the specified service
     */
    public io.github.resilience4j.bulkhead.Bulkhead createBulkhead(String serviceName) {
        String configPrefix = "bulkhead." + serviceName + ".";
        
        BulkheadConfig.Builder builder = BulkheadConfig.custom()
                .maxConcurrentCalls(config.getInteger(new IntegerConfigKey(configPrefix + "maxConcurrentCalls", 25)))
                .maxWaitDuration(Duration.ofMillis(config.getLong(new LongConfigKey(configPrefix + "maxWaitDuration", 500L))));

        return bulkheadRegistry.bulkhead(serviceName, builder.build());
    }

    /**
     * Creates a custom RateLimiter for a specific service.
     *
     * @param serviceName The name of the service to create a rate limiter for
     * @return The RateLimiter for the specified service
     */
    public io.github.resilience4j.ratelimiter.RateLimiter createRateLimiter(String serviceName) {
        String configPrefix = "ratelimiter." + serviceName + ".";
        
        RateLimiterConfig.Builder builder = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofMillis(config.getLong(new LongConfigKey(configPrefix + "limitRefreshPeriod", 1000L))))
                .limitForPeriod(config.getInteger(new IntegerConfigKey(configPrefix + "limitForPeriod", 50)))
                .timeoutDuration(Duration.ofMillis(config.getLong(new LongConfigKey(configPrefix + "timeoutDuration", 500L))));

        return rateLimiterRegistry.rateLimiter(serviceName, builder.build());
    }

    /**
     * Creates a custom Retry for a specific service.
     *
     * @param serviceName The name of the service to create a retry for
     * @return The Retry for the specified service
     */
    public io.github.resilience4j.retry.Retry createRetry(String serviceName) {
        String configPrefix = "retry." + serviceName + ".";
        
        RetryConfig.Builder builder = RetryConfig.custom()
                .maxAttempts(config.getInteger(new IntegerConfigKey(configPrefix + "maxAttempts", 3)))
                .waitDuration(Duration.ofMillis(config.getLong(new LongConfigKey(configPrefix + "waitDuration", 500L))))
                .retryExceptions(Exception.class);

        boolean enableExponentialBackoff = config.getBoolean(new BooleanConfigKey(configPrefix + "enableExponentialBackoff", true));
        if (enableExponentialBackoff) {
            builder.enableExponentialBackoff(true)
                   .exponentialBackoffMultiplier(config.getDouble(new DoubleConfigKey(configPrefix + "exponentialBackoffMultiplier", 2.0)));
        }

        return retryRegistry.retry(serviceName, builder.build());
    }
}