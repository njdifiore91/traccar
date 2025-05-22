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
package org.traccar.geolocation;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent;
import io.github.resilience4j.core.registry.EntryAddedEvent;
import io.github.resilience4j.core.registry.EntryRemovedEvent;
import io.github.resilience4j.core.registry.EntryReplacedEvent;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Factory for creating and configuring circuit breakers for geolocation providers.
 * Implements resilience patterns to handle external service failures gracefully.
 * 
 * This factory provides:
 * - Consistent circuit breaker configuration across all geolocation providers
 * - Fallback strategies for when providers are unavailable
 * - Circuit breaker state monitoring and reporting
 * - Metrics collection for circuit breaker events
 */
@Singleton
public class GeolocationCircuitBreakerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeolocationCircuitBreakerFactory.class);

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final MeterRegistry meterRegistry;

    /**
     * Creates a new GeolocationCircuitBreakerFactory with default configuration.
     * 
     * @param meterRegistry Registry for collecting and exposing circuit breaker metrics
     */
    @Inject
    public GeolocationCircuitBreakerFactory(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Configure default circuit breaker settings
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // When 50% of calls fail
                .slowCallRateThreshold(50) // When 50% of calls are slow
                .slowCallDurationThreshold(Duration.ofSeconds(2)) // Call is considered slow if it takes more than 2 seconds
                .permittedNumberOfCallsInHalfOpenState(10) // Number of calls allowed in half-open state
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED) // Use count-based sliding window
                .slidingWindowSize(100) // Consider the last 100 calls
                .minimumNumberOfCalls(10) // Minimum calls before calculating failure rate
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before transitioning to half-open
                .recordExceptions(GeolocationException.class, Exception.class) // Record these exceptions as failures
                .build();

        // Create registry with event consumer for logging state changes
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig, new RegistryEventConsumer<CircuitBreaker>() {
            @Override
            public void onEntryAddedEvent(EntryAddedEvent<CircuitBreaker> entryAddedEvent) {
                CircuitBreaker circuitBreaker = entryAddedEvent.getAddedEntry();
                circuitBreaker.getEventPublisher()
                        .onStateTransition(event -> LOGGER.info("Circuit breaker '{}' state changed from {} to {}",
                                event.getCircuitBreakerName(), event.getStateTransition().getFromState(),
                                event.getStateTransition().getToState()));
                
                // Register additional event listeners for metrics and monitoring
                circuitBreaker.getEventPublisher().onSuccess(event -> 
                        LOGGER.debug("Call to '{}' succeeded in {}ms", 
                                event.getCircuitBreakerName(), event.getElapsedDuration().toMillis()));
                
                circuitBreaker.getEventPublisher().onError(event -> 
                        LOGGER.warn("Call to '{}' failed in {}ms: {}", 
                                event.getCircuitBreakerName(), event.getElapsedDuration().toMillis(), 
                                event.getThrowable().getMessage()));
                
                circuitBreaker.getEventPublisher().onSlowCallRateExceeded(event -> 
                        LOGGER.warn("Slow call rate for '{}' exceeded threshold: {}%", 
                                event.getCircuitBreakerName(), event.getSlowCallRate()));
                
                circuitBreaker.getEventPublisher().onFailureRateExceeded(event -> 
                        LOGGER.warn("Failure rate for '{}' exceeded threshold: {}%", 
                                event.getCircuitBreakerName(), event.getFailureRate()));
            }

            @Override
            public void onEntryRemovedEvent(EntryRemovedEvent<CircuitBreaker> entryRemoveEvent) {
                // Not used in this implementation
            }

            @Override
            public void onEntryReplacedEvent(EntryReplacedEvent<CircuitBreaker> entryReplacedEvent) {
                // Not used in this implementation
            }
        });
        
        // Register metrics for all circuit breakers
        CircuitBreakerRegistry.CircuitBreakerMetrics.ofCircuitBreakerRegistry(circuitBreakerRegistry)
                .bindTo(meterRegistry);
    }

    /**
     * Creates a circuit breaker for a specific geolocation provider.
     *
     * @param providerName Name of the geolocation provider
     * @return CircuitBreaker instance configured for the provider
     */
    public CircuitBreaker create(String providerName) {
        return circuitBreakerRegistry.circuitBreaker(providerName);
    }

    /**
     * Creates a circuit breaker with custom configuration for a specific geolocation provider.
     *
     * @param providerName Name of the geolocation provider
     * @param config Custom circuit breaker configuration
     * @return CircuitBreaker instance configured for the provider
     */
    public CircuitBreaker create(String providerName, CircuitBreakerConfig config) {
        return circuitBreakerRegistry.circuitBreaker(providerName, config);
    }

    /**
     * Decorates a geolocation provider function with a circuit breaker.
     *
     * @param providerName Name of the geolocation provider
     * @param function Function to be decorated with circuit breaker
     * @param <T> Input type
     * @param <R> Return type
     * @return Decorated function with circuit breaker protection
     */
    public <T, R> Function<T, R> decorateFunction(String providerName, Function<T, R> function) {
        CircuitBreaker circuitBreaker = create(providerName);
        return CircuitBreaker.decorateFunction(circuitBreaker, function);
    }

    /**
     * Decorates a geolocation provider function with a circuit breaker and fallback.
     *
     * @param providerName Name of the geolocation provider
     * @param function Function to be decorated with circuit breaker
     * @param fallback Fallback function to use when circuit is open
     * @param <T> Input type
     * @param <R> Return type
     * @return Decorated function with circuit breaker and fallback protection
     */
    public <T, R> Function<T, R> decorateFunctionWithFallback(
            String providerName, Function<T, R> function, Function<T, R> fallback) {
        CircuitBreaker circuitBreaker = create(providerName);
        return input -> {
            try {
                return circuitBreaker.executeSupplier(() -> function.apply(input));
            } catch (Exception e) {
                LOGGER.warn("Circuit breaker for provider {} is open or call failed, using fallback: {}",
                        providerName, e.getMessage());
                return fallback.apply(input);
            }
        };
    }

    /**
     * Decorates a geolocation provider supplier with a circuit breaker.
     *
     * @param providerName Name of the geolocation provider
     * @param supplier Supplier to be decorated with circuit breaker
     * @param <R> Return type
     * @return Decorated supplier with circuit breaker protection
     */
    public <R> Supplier<R> decorateSupplier(String providerName, Supplier<R> supplier) {
        CircuitBreaker circuitBreaker = create(providerName);
        return CircuitBreaker.decorateSupplier(circuitBreaker, supplier);
    }

    /**
     * Decorates a geolocation provider supplier with a circuit breaker and fallback.
     *
     * @param providerName Name of the geolocation provider
     * @param supplier Supplier to be decorated with circuit breaker
     * @param fallback Fallback supplier to use when circuit is open
     * @param <R> Return type
     * @return Decorated supplier with circuit breaker and fallback protection
     */
    public <R> Supplier<R> decorateSupplierWithFallback(
            String providerName, Supplier<R> supplier, Supplier<R> fallback) {
        CircuitBreaker circuitBreaker = create(providerName);
        return () -> {
            try {
                return circuitBreaker.executeSupplier(supplier);
            } catch (Exception e) {
                LOGGER.warn("Circuit breaker for provider {} is open or call failed, using fallback: {}",
                        providerName, e.getMessage());
                return fallback.get();
            }
        };
    }

    /**
     * Gets the current state of a circuit breaker.
     *
     * @param providerName Name of the geolocation provider
     * @return Current state of the circuit breaker
     */
    public CircuitBreaker.State getState(String providerName) {
        return circuitBreakerRegistry.circuitBreaker(providerName).getState();
    }

    /**
     * Resets a circuit breaker to its closed state.
     *
     * @param providerName Name of the geolocation provider
     */
    public void reset(String providerName) {
        circuitBreakerRegistry.circuitBreaker(providerName).reset();
        LOGGER.info("Circuit breaker for provider {} has been reset", providerName);
    }
    
    /**
     * Creates a custom circuit breaker configuration for specific provider needs.
     * 
     * @param failureRateThreshold Percentage threshold above which the circuit breaker should trip
     * @param slowCallDurationThreshold Duration threshold above which calls are considered slow
     * @param waitDurationInOpenState Duration to wait before transitioning from open to half-open
     * @return Custom circuit breaker configuration
     */
    public CircuitBreakerConfig createCustomConfig(
            float failureRateThreshold,
            Duration slowCallDurationThreshold,
            Duration waitDurationInOpenState) {
        
        return CircuitBreakerConfig.custom()
                .failureRateThreshold(failureRateThreshold)
                .slowCallRateThreshold(failureRateThreshold) // Use same threshold for simplicity
                .slowCallDurationThreshold(slowCallDurationThreshold)
                .permittedNumberOfCallsInHalfOpenState(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(100)
                .minimumNumberOfCalls(10)
                .waitDurationInOpenState(waitDurationInOpenState)
                .recordExceptions(GeolocationException.class, Exception.class)
                .build();
    }
}