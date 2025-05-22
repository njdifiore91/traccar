/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.notification;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.registry.EntryAddedEvent;
import io.github.resilience4j.core.registry.EntryRemovedEvent;
import io.github.resilience4j.core.registry.EntryReplacedEvent;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.function.Function;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Implements the circuit breaker pattern for external notification services to prevent cascading failures
 * when external services are degraded. Provides a unified interface for executing notification delivery
 * operations with proper failure handling, timeout management, and service health monitoring.
 */
@Singleton
public class CircuitBreakerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(CircuitBreakerManager.class);

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final Map<String, CircuitBreaker> circuitBreakers = new ConcurrentHashMap<>();
    private final Map<String, TimeLimiter> timeLimiters = new ConcurrentHashMap<>();
    private final MeterRegistry meterRegistry;
    private final TimeLimiterConfig timeLimiterConfig;
    private final ScheduledExecutorService scheduledExecutorService;

    /**
     * Creates a new CircuitBreakerManager with the specified configuration.
     *
     * @param config Configuration for circuit breaker settings
     * @param meterRegistry Registry for metrics collection
     */
    @Inject
    public CircuitBreakerManager(Config config, MeterRegistry meterRegistry) {
        LOGGER.info("Initializing CircuitBreakerManager for notification services");
        
        // Create a scheduled executor service for async operations
        this.scheduledExecutorService = Executors.newScheduledThreadPool(
                config.getInteger(Keys.NOTIFICATION_ASYNC_THREAD_POOL_SIZE, 5));
        this.meterRegistry = meterRegistry;

        // Configure default circuit breaker settings from configuration
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(config.getFloat(Keys.NOTIFICATION_CIRCUIT_BREAKER_FAILURE_THRESHOLD, 50.0f))
                .slowCallRateThreshold(config.getFloat(Keys.NOTIFICATION_CIRCUIT_BREAKER_SLOW_THRESHOLD, 50.0f))
                .slowCallDurationThreshold(Duration.ofMillis(
                        config.getLong(Keys.NOTIFICATION_CIRCUIT_BREAKER_SLOW_DURATION, 2000L)))
                .permittedNumberOfCallsInHalfOpenState(
                        config.getInteger(Keys.NOTIFICATION_CIRCUIT_BREAKER_HALF_OPEN_CALLS, 10))
                .minimumNumberOfCalls(config.getInteger(Keys.NOTIFICATION_CIRCUIT_BREAKER_MIN_CALLS, 5))
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(config.getInteger(Keys.NOTIFICATION_CIRCUIT_BREAKER_WINDOW_SIZE, 100))
                .waitDurationInOpenState(Duration.ofMillis(
                        config.getLong(Keys.NOTIFICATION_CIRCUIT_BREAKER_OPEN_DURATION, 60000L)))
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .recordExceptions(Exception.class)
                .ignoreExceptions(MessageException.class)
                .build();
        
        LOGGER.debug("Circuit breaker configuration: failureRateThreshold={}, slowCallRateThreshold={}, slowCallDurationThreshold={}, permittedNumberOfCallsInHalfOpenState={}, minimumNumberOfCalls={}, slidingWindowSize={}, waitDurationInOpenState={}",
                circuitBreakerConfig.getFailureRateThreshold(),
                circuitBreakerConfig.getSlowCallRateThreshold(),
                circuitBreakerConfig.getSlowCallDurationThreshold(),
                circuitBreakerConfig.getPermittedNumberOfCallsInHalfOpenState(),
                circuitBreakerConfig.getMinimumNumberOfCalls(),
                circuitBreakerConfig.getSlidingWindowSize(),
                circuitBreakerConfig.getWaitDurationInOpenState());

        // Configure time limiter for timeout handling
        timeLimiterConfig = TimeLimiterConfig.custom()
                .timeoutDuration(Duration.ofMillis(
                        config.getLong(Keys.NOTIFICATION_TIMEOUT_DURATION, 5000L)))
                .cancelRunningFuture(true)
                .build();
                
        // Create registry with event consumer for logging and metrics
        circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig, new CircuitBreakerEventConsumer());
    }

    /**
     * Executes a notification operation with circuit breaker protection.
     *
     * @param notificatorType The type of notificator (e.g., "sms", "email")
     * @param operation The notification operation to execute
     * @param <T> The return type of the operation
     * @return The result of the operation
     * @throws MessageException If the operation fails or the circuit is open
     */
    public <T> T executeWithCircuitBreaker(String notificatorType, Supplier<T> operation) throws MessageException {
        if (notificatorType == null || notificatorType.isEmpty()) {
            throw new MessageException("Notificator type cannot be null or empty");
        }
        CircuitBreaker circuitBreaker = getOrCreateCircuitBreaker(notificatorType);
        
        try {
            return circuitBreaker.decorateSupplier(operation).get();
        } catch (Exception e) {
            if (e instanceof CircuitBreaker.CallNotPermittedException) {
                throw new MessageException("Service unavailable: " + notificatorType + " circuit is open");
            } else if (e instanceof TimeoutException) {
                throw new MessageException("Service timeout: " + notificatorType);
            } else {
                throw new MessageException(e);
            }
        }
    }

    /**
     * Executes a notification operation with circuit breaker protection and a fallback.
     *
     * @param notificatorType The type of notificator (e.g., "sms", "email")
     * @param operation The notification operation to execute
     * @param fallback The fallback operation to execute if the primary operation fails
     * @param <T> The return type of the operation
     * @return The result of the operation or fallback
     */
    public <T> T executeWithFallback(String notificatorType, Supplier<T> operation, Supplier<T> fallback) {
        if (notificatorType == null || notificatorType.isEmpty()) {
            LOGGER.warn("Notificator type is null or empty, using fallback directly");
            return fallback.get();
        }
        CircuitBreaker circuitBreaker = getOrCreateCircuitBreaker(notificatorType);
        
        try {
            return circuitBreaker.decorateSupplier(operation).get();
        } catch (Exception e) {
            LOGGER.warn("Notification service {} failed, using fallback: {}", notificatorType, e.getMessage());
            return fallback.get();
        }
    }

    /**
     * Gets the circuit breaker for the specified notificator type or creates a new one if it doesn't exist.
     *
     * @param notificatorType The type of notificator
     * @return The circuit breaker for the specified notificator type
     */
    private CircuitBreaker getOrCreateCircuitBreaker(String notificatorType) {
        return circuitBreakers.computeIfAbsent(notificatorType, type -> {
            CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(type);
            
            // Register metrics for this circuit breaker
            circuitBreaker.getEventPublisher()
                    .onSuccess(event -> updateMetrics(type, "success"))
                    .onError(event -> updateMetrics(type, "error"))
                    .onStateTransition(event -> {
                        LOGGER.info("Circuit breaker {} state changed from {} to {}",
                                type, event.getStateTransition().getFromState(),
                                event.getStateTransition().getToState());
                        updateMetrics(type, "state", event.getStateTransition().getToState().ordinal());
                    });
            
            return circuitBreaker;
        });
    }
    
    /**
     * Gets the time limiter for the specified notificator type or creates a new one if it doesn't exist.
     *
     * @param notificatorType The type of notificator
     * @return The time limiter for the specified notificator type
     */
    private TimeLimiter getOrCreateTimeLimiter(String notificatorType) {
        return timeLimiters.computeIfAbsent(notificatorType, type -> {
            TimeLimiter timeLimiter = TimeLimiter.of(timeLimiterConfig);
            LOGGER.debug("Created time limiter for notificator type: {}", type);
            return timeLimiter;
        });
    }

    /**
     * Updates metrics for the specified notificator type.
     *
     * @param notificatorType The type of notificator
     * @param metricType The type of metric to update
     */
    private void updateMetrics(String notificatorType, String metricType) {
        meterRegistry.counter("notification.circuit_breaker." + metricType, "type", notificatorType).increment();
    }

    /**
     * Updates metrics for the specified notificator type with a value.
     *
     * @param notificatorType The type of notificator
     * @param metricType The type of metric to update
     * @param value The value to set
     */
    private void updateMetrics(String notificatorType, String metricType, double value) {
        meterRegistry.gauge("notification.circuit_breaker." + metricType, value, "type", notificatorType);
    }

    /**
     * Gets the current state of the circuit breaker for the specified notificator type.
     *
     * @param notificatorType The type of notificator
     * @return The current state of the circuit breaker
     */
    public CircuitBreaker.State getCircuitBreakerState(String notificatorType) {
        CircuitBreaker circuitBreaker = circuitBreakers.get(notificatorType);
        return circuitBreaker != null ? circuitBreaker.getState() : CircuitBreaker.State.CLOSED;
    }

    /**
     * Resets the circuit breaker for the specified notificator type.
     *
     * @param notificatorType The type of notificator
     */
    public void resetCircuitBreaker(String notificatorType) {
        CircuitBreaker circuitBreaker = circuitBreakers.get(notificatorType);
        if (circuitBreaker != null) {
            circuitBreaker.reset();
            LOGGER.info("Circuit breaker {} has been reset", notificatorType);
        }
    }

    /**
     * Event consumer for circuit breaker events.
     */
    private static class CircuitBreakerEventConsumer implements RegistryEventConsumer<CircuitBreaker> {

        @Override
        public void onEntryAddedEvent(EntryAddedEvent<CircuitBreaker> entryAddedEvent) {
            entryAddedEvent.getAddedEntry().getEventPublisher()
                    .onStateTransition(event -> LOGGER.info("Circuit breaker {} state changed from {} to {}",
                            entryAddedEvent.getAddedEntry().getName(),
                            event.getStateTransition().getFromState(),
                            event.getStateTransition().getToState()));
        }

        @Override
        public void onEntryRemovedEvent(EntryRemovedEvent<CircuitBreaker> entryRemoveEvent) {
            // Not used
        }

        @Override
        public void onEntryReplacedEvent(EntryReplacedEvent<CircuitBreaker> entryReplacedEvent) {
            // Not used
        }
    }
    
    /**
     * Executes a notification operation with circuit breaker protection that takes an input parameter.
     *
     * @param notificatorType The type of notificator (e.g., "sms", "email")
     * @param operation The notification operation to execute
     * @param input The input parameter for the operation
     * @param <T> The return type of the operation
     * @param <R> The input type for the operation
     * @return The result of the operation
     * @throws MessageException If the operation fails or the circuit is open
     */
    public <T, R> T executeWithCircuitBreaker(String notificatorType, Function<R, T> operation, R input) throws MessageException {
        if (notificatorType == null || notificatorType.isEmpty()) {
            throw new MessageException("Notificator type cannot be null or empty");
        }
        
        CircuitBreaker circuitBreaker = getOrCreateCircuitBreaker(notificatorType);
        
        try {
            return circuitBreaker.decorateFunction(operation).apply(input);
        } catch (Exception e) {
            if (e instanceof CircuitBreaker.CallNotPermittedException) {
                throw new MessageException("Service unavailable: " + notificatorType + " circuit is open");
            } else if (e instanceof TimeoutException) {
                throw new MessageException("Service timeout: " + notificatorType);
            } else {
                throw new MessageException(e);
            }
        }
    }
    
    /**
     * Executes a notification operation with circuit breaker protection and a fallback that takes an input parameter.
     *
     * @param notificatorType The type of notificator (e.g., "sms", "email")
     * @param operation The notification operation to execute
     * @param fallback The fallback operation to execute if the primary operation fails
     * @param input The input parameter for the operation
     * @param <T> The return type of the operation
     * @param <R> The input type for the operation
     * @return The result of the operation or fallback
     */
    public <T, R> T executeWithFallback(String notificatorType, Function<R, T> operation, Function<R, T> fallback, R input) {
        if (notificatorType == null || notificatorType.isEmpty()) {
            LOGGER.warn("Notificator type is null or empty, using fallback directly");
            return fallback.apply(input);
        }
        
        CircuitBreaker circuitBreaker = getOrCreateCircuitBreaker(notificatorType);
        
        try {
            return circuitBreaker.decorateFunction(operation).apply(input);
        } catch (Exception e) {
            LOGGER.warn("Notification service {} failed, using fallback: {}", notificatorType, e.getMessage());
            return fallback.apply(input);
        }
    }
    
    /**
     * Executes a notification operation asynchronously with circuit breaker and timeout protection.
     *
     * @param notificatorType The type of notificator (e.g., "sms", "email")
     * @param operation The notification operation to execute
     * @param <T> The return type of the operation
     * @return A CompletableFuture that will complete with the result of the operation
     */
    public <T> CompletableFuture<T> executeAsync(String notificatorType, Supplier<T> operation) {
        if (notificatorType == null || notificatorType.isEmpty()) {
            CompletableFuture<T> future = new CompletableFuture<>();
            future.completeExceptionally(new MessageException("Notificator type cannot be null or empty"));
            return future;
        }
        
        CircuitBreaker circuitBreaker = getOrCreateCircuitBreaker(notificatorType);
        TimeLimiter timeLimiter = getOrCreateTimeLimiter(notificatorType);
        
        Supplier<CompletableFuture<T>> futureSupplier = () -> CompletableFuture.supplyAsync(operation);
        return timeLimiter.executeCompletionStage(
                scheduledExecutorService,
                CircuitBreaker.decorateCompletionStage(
                        circuitBreaker,
                        futureSupplier
                )
        ).toCompletableFuture();
    }
    
    /**
     * Executes a notification operation asynchronously with circuit breaker, timeout protection, and fallback.
     *
     * @param notificatorType The type of notificator (e.g., "sms", "email")
     * @param operation The notification operation to execute
     * @param fallback The fallback operation to execute if the primary operation fails
     * @param <T> The return type of the operation
     * @return A CompletableFuture that will complete with the result of the operation or fallback
     */
    public <T> CompletableFuture<T> executeAsyncWithFallback(String notificatorType, Supplier<T> operation, Supplier<T> fallback) {
        CompletableFuture<T> future = executeAsync(notificatorType, operation);
        return future.exceptionally(ex -> {
            LOGGER.warn("Async notification service {} failed, using fallback: {}", notificatorType, ex.getMessage());
            return fallback.get();
        });
    }
    
    /**
     * Shuts down the circuit breaker manager and releases resources.
     */
    public void shutdown() {
        LOGGER.info("Shutting down CircuitBreakerManager");
        if (scheduledExecutorService != null && !scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdown();
        }
    }
}