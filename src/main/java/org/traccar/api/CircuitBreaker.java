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
package org.traccar.api;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.registry.EntryAddedEvent;
import io.github.resilience4j.core.registry.EntryRemovedEvent;
import io.github.resilience4j.core.registry.EntryReplacedEvent;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Circuit breaker implementation for resilient API calls to backend microservices.
 * <p>
 * This class provides a circuit breaker pattern implementation that monitors for failures
 * in service-to-service communication, automatically opens the circuit when failure thresholds
 * are exceeded, and provides fallback mechanisms for when services are unavailable.
 * <p>
 * The circuit breaker has three states:
 * <ul>
 *   <li>CLOSED: Normal operation, calls pass through to the service</li>
 *   <li>OPEN: Circuit is open, calls fail fast without reaching the service</li>
 *   <li>HALF_OPEN: Testing if the service has recovered by allowing a limited number of calls</li>
 * </ul>
 */
public class CircuitBreaker {

    private static final Logger LOGGER = LoggerFactory.getLogger(CircuitBreaker.class);

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final MeterRegistry meterRegistry;

    /**
     * Constructs a new CircuitBreaker with the specified meter registry for metrics collection.
     *
     * @param meterRegistry the meter registry for recording circuit breaker metrics
     */
    public CircuitBreaker(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();
        registerEventConsumer();
    }

    /**
     * Registers an event consumer to log circuit breaker state transitions.
     */
    private void registerEventConsumer() {
        RegistryEventConsumer<io.github.resilience4j.circuitbreaker.CircuitBreaker> eventConsumer = 
            new RegistryEventConsumer<>() {
                @Override
                public void onEntryAddedEvent(EntryAddedEvent<io.github.resilience4j.circuitbreaker.CircuitBreaker> entryAddedEvent) {
                    entryAddedEvent.getAddedEntry().getEventPublisher()
                        .onStateTransition(event -> LOGGER.info(event.toString()));
                }

                @Override
                public void onEntryRemovedEvent(EntryRemovedEvent<io.github.resilience4j.circuitbreaker.CircuitBreaker> entryRemoveEvent) {
                    // Not needed for this implementation
                }

                @Override
                public void onEntryReplacedEvent(EntryReplacedEvent<io.github.resilience4j.circuitbreaker.CircuitBreaker> entryReplacedEvent) {
                    // Not needed for this implementation
                }
            };

        circuitBreakerRegistry.getEventPublisher().onEntryAdded(eventConsumer::onEntryAddedEvent);
    }

    /**
     * Creates a circuit breaker with custom configuration.
     *
     * @param name                  the name of the circuit breaker
     * @param failureRateThreshold  the failure rate threshold in percentage
     * @param slowCallRateThreshold the slow call rate threshold in percentage
     * @param slowCallDuration      the duration threshold above which calls are considered slow
     * @param waitDurationInOpen    the wait duration in open state before transitioning to half-open
     * @param permittedCallsInHalfOpen the number of permitted calls when the circuit is half-open
     * @param slidingWindowSize     the sliding window size to calculate failure rate
     * @return a configured circuit breaker instance
     */
    public io.github.resilience4j.circuitbreaker.CircuitBreaker create(
            String name,
            float failureRateThreshold,
            float slowCallRateThreshold,
            Duration slowCallDuration,
            Duration waitDurationInOpen,
            int permittedCallsInHalfOpen,
            int slidingWindowSize) {

        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(failureRateThreshold)
                .slowCallRateThreshold(slowCallRateThreshold)
                .slowCallDurationThreshold(slowCallDuration)
                .waitDurationInOpenState(waitDurationInOpen)
                .permittedNumberOfCallsInHalfOpenState(permittedCallsInHalfOpen)
                .slidingWindowSize(slidingWindowSize)
                .build();

        io.github.resilience4j.circuitbreaker.CircuitBreaker circuitBreaker = 
                circuitBreakerRegistry.circuitBreaker(name, config);

        // Register metrics with Micrometer if available
        if (meterRegistry != null) {
            circuitBreaker.getEventPublisher()
                    .onSuccess(event -> meterRegistry.counter(name + ".success").increment())
                    .onError(event -> meterRegistry.counter(name + ".error").increment())
                    .onStateTransition(event -> meterRegistry.counter(
                            name + ".state." + event.getStateTransition().getToState()).increment());
        }

        return circuitBreaker;
    }

    /**
     * Creates a circuit breaker with default configuration.
     *
     * @param name the name of the circuit breaker
     * @return a configured circuit breaker instance with default settings
     */
    public io.github.resilience4j.circuitbreaker.CircuitBreaker create(String name) {
        return create(
                name,
                50.0f,                          // 50% failure rate threshold
                50.0f,                          // 50% slow call rate threshold
                Duration.ofSeconds(1),          // 1 second slow call duration threshold
                Duration.ofSeconds(30),         // 30 seconds wait duration in open state
                10,                             // 10 permitted calls in half-open state
                100);                           // 100 sliding window size
    }

    /**
     * Decorates a Supplier with a circuit breaker.
     *
     * @param circuitBreaker the circuit breaker to use
     * @param supplier       the supplier to decorate
     * @param fallback       the fallback function to use when the circuit is open
     * @param <T>            the type of the supplier result
     * @return a decorated supplier with circuit breaker functionality
     */
    public <T> Supplier<T> decorateSupplier(
            io.github.resilience4j.circuitbreaker.CircuitBreaker circuitBreaker,
            Supplier<T> supplier,
            Function<Exception, T> fallback) {

        return () -> {
            try {
                return circuitBreaker.decorateSupplier(supplier).get();
            } catch (Exception e) {
                LOGGER.warn("Circuit breaker '{}' fallback triggered: {}", 
                        circuitBreaker.getName(), e.getMessage());
                return fallback.apply(e);
            }
        };
    }

    /**
     * Decorates a Callable with a circuit breaker.
     *
     * @param circuitBreaker the circuit breaker to use
     * @param callable       the callable to decorate
     * @param fallback       the fallback function to use when the circuit is open
     * @param <T>            the type of the callable result
     * @return a decorated callable with circuit breaker functionality
     */
    public <T> Callable<T> decorateCallable(
            io.github.resilience4j.circuitbreaker.CircuitBreaker circuitBreaker,
            Callable<T> callable,
            Function<Exception, T> fallback) {

        return () -> {
            try {
                return circuitBreaker.decorateCallable(callable).call();
            } catch (Exception e) {
                LOGGER.warn("Circuit breaker '{}' fallback triggered: {}", 
                        circuitBreaker.getName(), e.getMessage());
                return fallback.apply(e);
            }
        };
    }

    /**
     * Decorates a CompletionStage with a circuit breaker.
     *
     * @param circuitBreaker the circuit breaker to use
     * @param supplier       the supplier of the CompletionStage to decorate
     * @param fallback       the fallback function to use when the circuit is open
     * @param <T>            the type of the CompletionStage result
     * @return a decorated CompletionStage with circuit breaker functionality
     */
    public <T> Supplier<CompletionStage<T>> decorateCompletionStage(
            io.github.resilience4j.circuitbreaker.CircuitBreaker circuitBreaker,
            Supplier<CompletionStage<T>> supplier,
            Function<Exception, T> fallback) {

        return () -> {
            try {
                return circuitBreaker.decorateCompletionStage(
                        () -> CompletableFuture.completedFuture(null), 
                        (ignored, throwable) -> supplier.get())
                        .apply(null, null);
            } catch (Exception e) {
                LOGGER.warn("Circuit breaker '{}' fallback triggered: {}", 
                        circuitBreaker.getName(), e.getMessage());
                return CompletableFuture.completedFuture(fallback.apply(e));
            }
        };
    }

    /**
     * Gets the current state of the circuit breaker.
     *
     * @param circuitBreaker the circuit breaker to check
     * @return the current state of the circuit breaker
     */
    public String getState(io.github.resilience4j.circuitbreaker.CircuitBreaker circuitBreaker) {
        return circuitBreaker.getState().name();
    }

    /**
     * Gets the failure rate of the circuit breaker.
     *
     * @param circuitBreaker the circuit breaker to check
     * @return the current failure rate as a percentage
     */
    public float getFailureRate(io.github.resilience4j.circuitbreaker.CircuitBreaker circuitBreaker) {
        return circuitBreaker.getMetrics().getFailureRate();
    }

    /**
     * Gets the slow call rate of the circuit breaker.
     *
     * @param circuitBreaker the circuit breaker to check
     * @return the current slow call rate as a percentage
     */
    public float getSlowCallRate(io.github.resilience4j.circuitbreaker.CircuitBreaker circuitBreaker) {
        return circuitBreaker.getMetrics().getSlowCallRate();
    }

    /**
     * Resets the circuit breaker to its original closed state, losing all metrics.
     *
     * @param circuitBreaker the circuit breaker to reset
     */
    public void reset(io.github.resilience4j.circuitbreaker.CircuitBreaker circuitBreaker) {
        circuitBreaker.reset();
    }

    /**
     * Forces the circuit breaker into an open state, preventing all calls.
     *
     * @param circuitBreaker the circuit breaker to force open
     */
    public void forceOpen(io.github.resilience4j.circuitbreaker.CircuitBreaker circuitBreaker) {
        circuitBreaker.transitionToForcedOpenState();
    }

    /**
     * Forces the circuit breaker into a closed state, allowing all calls.
     *
     * @param circuitBreaker the circuit breaker to force closed
     */
    public void forceClosed(io.github.resilience4j.circuitbreaker.CircuitBreaker circuitBreaker) {
        circuitBreaker.transitionToClosedState();
    }
}