/*
 * Copyright 2022 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.database;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.traccar.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.storage.query.Request;
import org.traccar.storage.Storage;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Implements the circuit breaker pattern for database operations to prevent cascading failures
 * in the microservices architecture. It monitors database operation failures, trips the circuit
 * when failure thresholds are exceeded, and provides fallback mechanisms.
 * <p>
 * The circuit breaker has three states:
 * - CLOSED: Normal operation, calls pass through with monitoring
 * - OPEN: Circuit tripped, calls fail fast without reaching the database
 * - HALF-OPEN: Testing period, limited calls allowed to check if the underlying issue is resolved
 * <p>
 * This class integrates with OpenTelemetry for distributed tracing and exposes metrics for monitoring.
 */
@Singleton
public class DatabaseCircuitBreaker {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseCircuitBreaker.class);

    private static final String CIRCUIT_BREAKER_NAME = "database";
    private static final String RETRY_NAME = "database";

    private static final AttributeKey<String> OPERATION_KEY = AttributeKey.stringKey("database.operation");
    private static final AttributeKey<String> CIRCUIT_STATE_KEY = AttributeKey.stringKey("circuit.state");
    private static final AttributeKey<Boolean> SUCCESS_KEY = AttributeKey.booleanKey("operation.success");

    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Tracer tracer;
    private final Meter meter;
    private final LongCounter successCounter;
    private final LongCounter failureCounter;
    private final LongCounter timeoutCounter;
    private final Storage storage;

    /**
     * Constructs a new DatabaseCircuitBreaker with the specified configuration.
     *
     * @param config The application configuration
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param meter The OpenTelemetry meter for metrics collection
     */
    @Inject
    public DatabaseCircuitBreaker(Config config, Tracer tracer, Meter meter, Storage storage) {
        this.storage = storage;
        this.tracer = tracer;
        this.meter = meter;

        // Configure circuit breaker based on application configuration
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(config.getFloat(Keys.DATABASE_CIRCUIT_BREAKER_FAILURE_THRESHOLD, 50.0f))
                .slowCallRateThreshold(config.getFloat(Keys.DATABASE_CIRCUIT_BREAKER_SLOW_THRESHOLD, 50.0f))
                .slowCallDurationThreshold(Duration.ofMillis(
                        config.getLong(Keys.DATABASE_CIRCUIT_BREAKER_SLOW_DURATION, 1000L)))
                .waitDurationInOpenState(Duration.ofMillis(
                        config.getLong(Keys.DATABASE_CIRCUIT_BREAKER_WAIT_DURATION, 10000L)))
                .permittedNumberOfCallsInHalfOpenState(
                        config.getInteger(Keys.DATABASE_CIRCUIT_BREAKER_PERMITTED_CALLS, 10))
                .minimumNumberOfCalls(config.getInteger(Keys.DATABASE_CIRCUIT_BREAKER_MINIMUM_CALLS, 10))
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(config.getInteger(Keys.DATABASE_CIRCUIT_BREAKER_WINDOW_SIZE, 100))
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .recordExceptions(Exception.class)
                .build();

        // Configure retry policy with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(config.getInteger(Keys.DATABASE_RETRY_MAX_ATTEMPTS, 3))
                .waitDuration(Duration.ofMillis(config.getLong(Keys.DATABASE_RETRY_WAIT_DURATION, 1000L)))
                .retryExceptions(Exception.class)
                .ignoreExceptions(TimeoutException.class)
                .enableExponentialBackoff(true)
                .exponentialBackoffMultiplier(config.getDouble(Keys.DATABASE_RETRY_BACKOFF_MULTIPLIER, 2.0))
                .failAfterMaxAttempts(true)
                .build();

        // Create circuit breaker and retry instances
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);

        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        this.retry = retryRegistry.retry(RETRY_NAME);

        // Register event listeners for logging and metrics
        registerEventListeners();

        // Initialize metrics
        this.successCounter = meter.counterBuilder("database.operations.success")
                .setDescription("Number of successful database operations")
                .build();

        this.failureCounter = meter.counterBuilder("database.operations.failure")
                .setDescription("Number of failed database operations")
                .build();

        this.timeoutCounter = meter.counterBuilder("database.operations.timeout")
                .setDescription("Number of timed out database operations")
                .build();

        // Register circuit breaker state gauge
        meter.gaugeBuilder("database.circuit.state")
                .setDescription("Current state of the database circuit breaker (0=CLOSED, 1=OPEN, 2=HALF_OPEN)")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    CircuitBreaker.State state = circuitBreaker.getState();
                    measurement.record(stateToValue(state));
                });
    }

    /**
     * Executes a database operation with circuit breaker and retry protection.
     *
     * @param operation The name of the database operation for tracing and metrics
     * @param supplier The database operation to execute
     * @param <T> The return type of the database operation
     * @return The result of the database operation
     * @throws Exception If the operation fails and cannot be retried or the circuit is open
     */
    public <T> T executeOperation(String operation, Supplier<T> supplier) throws Exception {
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            throw new StorageException("Database circuit breaker is open");
        }
        Span span = tracer.spanBuilder("database." + operation)
                .setAttribute(OPERATION_KEY, operation)
                .setAttribute(CIRCUIT_STATE_KEY, circuitBreaker.getState().name())
                .startSpan();

        try (var scope = span.makeCurrent()) {
            // Wrap the operation with retry and circuit breaker
            return Retry.decorateSupplier(retry, 
                    CircuitBreaker.decorateSupplier(circuitBreaker, () -> {
                        try {
                            T result = supplier.get();
                            successCounter.add(1, Attributes.of(OPERATION_KEY, operation));
                            span.setAttribute(SUCCESS_KEY, true);
                            return result;
                        } catch (Exception e) {
                            if (e instanceof TimeoutException) {
                                timeoutCounter.add(1, Attributes.of(OPERATION_KEY, operation));
                                span.setAttribute("timeout", true);
                            }
                            failureCounter.add(1, Attributes.of(OPERATION_KEY, operation));
                            span.setAttribute(SUCCESS_KEY, false);
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            throw e;
                        }
                    })).get();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Executes a database operation that throws checked exceptions with circuit breaker and retry protection.
     *
     * @param operation The name of the database operation for tracing and metrics
     * @param callable The database operation to execute
     * @param <T> The return type of the database operation
     * @return The result of the database operation
     * @throws Exception If the operation fails and cannot be retried or the circuit is open
     */
    public <T> T executeCheckedOperation(String operation, Callable<T> callable) throws Exception {
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            throw new StorageException("Database circuit breaker is open");
        }
        Span span = tracer.spanBuilder("database." + operation)
                .setAttribute(OPERATION_KEY, operation)
                .setAttribute(CIRCUIT_STATE_KEY, circuitBreaker.getState().name())
                .startSpan();

        try (var scope = span.makeCurrent()) {
            // Wrap the operation with retry and circuit breaker
            return Retry.decorateCallable(retry, 
                    CircuitBreaker.decorateCallable(circuitBreaker, () -> {
                        try {
                            T result = callable.call();
                            successCounter.add(1, Attributes.of(OPERATION_KEY, operation));
                            span.setAttribute(SUCCESS_KEY, true);
                            return result;
                        } catch (Exception e) {
                            if (e instanceof TimeoutException) {
                                timeoutCounter.add(1, Attributes.of(OPERATION_KEY, operation));
                                span.setAttribute("timeout", true);
                            }
                            failureCounter.add(1, Attributes.of(OPERATION_KEY, operation));
                            span.setAttribute(SUCCESS_KEY, false);
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            throw e;
                        }
                    })).call();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Executes a database operation with circuit breaker and retry protection, with a fallback value.
     *
     * @param operation The name of the database operation for tracing and metrics
     * @param supplier The database operation to execute
     * @param fallback The fallback value to return if the operation fails
     * @param <T> The return type of the database operation
     * @return The result of the database operation or the fallback value
     */
    public <T> T executeWithFallback(String operation, Supplier<T> supplier, T fallback) {
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            LOGGER.warn("Database circuit breaker is open, using fallback value for operation: {}", operation);
            return fallback;
        }
        try {
            return executeOperation(operation, supplier);
        } catch (Exception e) {
            LOGGER.warn("Database operation '{}' failed, using fallback value", operation, e);
            return fallback;
        }
    }

    /**
     * Gets the current state of the circuit breaker.
     *
     * @return The current state of the circuit breaker
     */
    public CircuitBreaker.State getState() {
        return circuitBreaker.getState();
    }

    /**
     * Resets the circuit breaker to its closed state.
     * This can be useful for manual recovery after fixing an issue.
     */
    public void reset() {
        LOGGER.info("Manually resetting database circuit breaker");
        circuitBreaker.reset();
    }
    
    /**
     * Executes a database query with circuit breaker protection.
     *
     * @param operation The name of the database operation for tracing and metrics
     * @param query The database query to execute
     * @param <T> The return type of the database query
     * @return The result of the database query
     * @throws StorageException If the query fails or the circuit is open
     */
    public <T> T executeQuery(String operation, DatabaseQuery<T> query) throws StorageException {
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            throw new StorageException("Database circuit breaker is open");
        }
        try {
            return executeCheckedOperation(operation, () -> {
                try {
                    return query.execute(storage);
                } catch (StorageException e) {
                    throw e;
                } catch (Exception e) {
                    throw new StorageException(e);
                }
            });
        } catch (StorageException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    /**
     * Functional interface for database queries.
     *
     * @param <T> The return type of the query
     */
    @FunctionalInterface
    public interface DatabaseQuery<T> {
        /**
         * Executes a database query using the provided storage.
         *
         * @param storage The storage to use for the query
         * @return The result of the query
         * @throws StorageException If the query fails
         */
        T execute(Storage storage) throws StorageException;
    }
    
    /**
     * Executes a database get operation with circuit breaker protection.
     *
     * @param entityClass The entity class to get
     * @param request The database request
     * @param <T> The type of the entity
     * @return The entity or null if not found
     * @throws StorageException If the operation fails or the circuit is open
     */
    public <T> T getObject(Class<T> entityClass, Request request) throws StorageException {
        return executeQuery("getObject." + entityClass.getSimpleName(), 
                storage -> storage.getObject(entityClass, request));
    }
    
    /**
     * Executes a database get collection operation with circuit breaker protection.
     *
     * @param entityClass The entity class to get
     * @param request The database request
     * @param <T> The type of the entity
     * @return The collection of entities
     * @throws StorageException If the operation fails or the circuit is open
     */
    public <T> Collection<T> getObjects(Class<T> entityClass, Request request) throws StorageException {
        return executeQuery("getObjects." + entityClass.getSimpleName(), 
                storage -> storage.getObjects(entityClass, request));
    }
    
    /**
     * Executes a database add operation with circuit breaker protection.
     *
     * @param entityClass The entity class to add
     * @param entity The entity to add
     * @param request The database request
     * @param <T> The type of the entity
     * @return The ID of the added entity
     * @throws StorageException If the operation fails or the circuit is open
     */
    public <T> long addObject(Class<T> entityClass, T entity, Request request) throws StorageException {
        return executeQuery("addObject." + entityClass.getSimpleName(), 
                storage -> storage.addObject(entity, request));
    }
    
    /**
     * Executes a database update operation with circuit breaker protection.
     *
     * @param entityClass The entity class to update
     * @param entity The entity to update
     * @param request The database request
     * @param <T> The type of the entity
     * @throws StorageException If the operation fails or the circuit is open
     */
    public <T> void updateObject(Class<T> entityClass, T entity, Request request) throws StorageException {
        executeQuery("updateObject." + entityClass.getSimpleName(), 
                storage -> {
                    storage.updateObject(entity, request);
                    return null;
                });
    }
    
    /**
     * Executes a database remove operation with circuit breaker protection.
     *
     * @param entityClass The entity class to remove
     * @param request The database request
     * @param <T> The type of the entity
     * @throws StorageException If the operation fails or the circuit is open
     */
    public <T> void removeObject(Class<T> entityClass, Request request) throws StorageException {
        executeQuery("removeObject." + entityClass.getSimpleName(), 
                storage -> {
                    storage.removeObject(entityClass, request);
                    return null;
                });
    }

    /**
     * Registers event listeners for the circuit breaker to log state transitions and events.
     */
    private void registerEventListeners() {
        // Log state transitions
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    LOGGER.info("Database circuit breaker state changed from {} to {}",
                            event.getStateTransition().getFromState(),
                            event.getStateTransition().getToState());
                });

        // Log success and failure events
        circuitBreaker.getEventPublisher()
                .onSuccess(event -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Database operation succeeded in {}ms", event.getElapsedDuration().toMillis());
                    }
                });

        circuitBreaker.getEventPublisher()
                .onError(event -> {
                    LOGGER.warn("Database operation failed after {}ms: {}",
                            event.getElapsedDuration().toMillis(),
                            event.getThrowable().getMessage());
                });

        // Log retry events
        retry.getEventPublisher()
                .onRetry(event -> {
                    LOGGER.warn("Retrying database operation after failure (attempt {}/{}): {}",
                            event.getNumberOfRetryAttempts(),
                            retry.getRetryConfig().getMaxAttempts(),
                            event.getLastThrowable().getMessage());
                });
    }

    /**
     * Converts a circuit breaker state to a numeric value for metrics.
     *
     * @param state The circuit breaker state
     * @return A numeric value representing the state (0=CLOSED, 1=OPEN, 2=HALF_OPEN)
     */
    private long stateToValue(CircuitBreaker.State state) {
        return switch (state) {
            case CLOSED -> 0;
            case OPEN -> 1;
            case HALF_OPEN -> 2;
            case DISABLED -> -1;
            case FORCED_OPEN -> -2;
            case METRICS_ONLY -> -3;
        };
    }
}