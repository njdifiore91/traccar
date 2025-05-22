/*
 * Copyright 2015 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.handler;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.database.StatisticsManager;
import org.traccar.messaging.PositionProducer;
import org.traccar.model.Position;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Request;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Database handler for storing position data in the database and publishing to message broker.
 * Implements circuit breaker pattern for database operations and includes distributed tracing and metrics.
 */
public class DatabaseHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseHandler.class);

    private final Storage storage;
    private final StatisticsManager statisticsManager;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final PositionProducer positionProducer;
    
    // Metrics
    private final Timer databaseOperationTimer;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Counter circuitBreakerOpenCounter;

    /**
     * Constructs a new DatabaseHandler with the necessary dependencies.
     *
     * @param storage The storage implementation for database operations
     * @param statisticsManager The statistics manager for tracking message statistics
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param meterRegistry The Micrometer registry for metrics collection
     * @param positionProducer The message broker producer for publishing positions
     */
    @Inject
    public DatabaseHandler(
            Storage storage,
            StatisticsManager statisticsManager,
            Tracer tracer,
            MeterRegistry meterRegistry,
            PositionProducer positionProducer) {
        this.storage = storage;
        this.statisticsManager = statisticsManager;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.positionProducer = positionProducer;
        
        // Initialize metrics
        this.databaseOperationTimer = Timer.builder("database.operation.duration")
                .description("Time taken for database operations")
                .tag("operation", "store_position")
                .register(meterRegistry);
        this.successCounter = Counter.builder("database.operation.success")
                .description("Number of successful database operations")
                .tag("operation", "store_position")
                .register(meterRegistry);
        this.failureCounter = Counter.builder("database.operation.failure")
                .description("Number of failed database operations")
                .tag("operation", "store_position")
                .register(meterRegistry);
        this.circuitBreakerOpenCounter = Counter.builder("database.circuit_breaker.open")
                .description("Number of times the circuit breaker opened")
                .tag("operation", "store_position")
                .register(meterRegistry);
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit breaker if 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .slidingWindowSize(10) // Consider the last 10 calls for failure rate calculation
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .recordExceptions(StorageException.class, TimeoutException.class) // Record these exceptions as failures
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("databaseOperations");
        
        // Add a state transition listener to track circuit breaker state changes
        this.circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    if (event.getStateTransition() == CircuitBreaker.StateTransition.CLOSED_TO_OPEN) {
                        LOGGER.warn("Circuit breaker opened due to failure rate threshold exceeded");
                        circuitBreakerOpenCounter.increment();
                    } else if (event.getStateTransition() == CircuitBreaker.StateTransition.OPEN_TO_HALF_OPEN) {
                        LOGGER.info("Circuit breaker transitioned from OPEN to HALF_OPEN");
                    } else if (event.getStateTransition() == CircuitBreaker.StateTransition.HALF_OPEN_TO_CLOSED) {
                        LOGGER.info("Circuit breaker closed, service recovered");
                    }
                });
        
        // Configure retry
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Try up to 3 times
                .waitDuration(Duration.ofMillis(500)) // Wait 500ms between retries
                .retryExceptions(StorageException.class) // Retry on storage exceptions
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry("databaseOperations");
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for the database operation
        Span span = tracer.spanBuilder("database.store_position")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .setAttribute("protocol", position.getProtocol())
                .startSpan();
        
        // Attach the span to the current context
        try (Scope scope = span.makeCurrent()) {
            // Wrap the database operation with circuit breaker and retry
            Supplier<Long> databaseOperation = () -> {
                try {
                    // Use timer to measure database operation duration
                    return databaseOperationTimer.record(() -> {
                        try {
                            // Store position in database
                            long id = storage.addObject(position, new Request(new Columns.Exclude("id")));
                            // Record success in statistics and metrics
                            statisticsManager.registerMessageStored(position.getDeviceId(), position.getProtocol());
                            successCounter.increment();
                            return id;
                        } catch (Exception e) {
                            // Record failure in metrics
                            failureCounter.increment();
                            throw new StorageException(e);
                        }
                    });
                } catch (Exception e) {
                    // Propagate the exception for retry/circuit breaker handling
                    throw new StorageException(e);
                }
            };
            
            try {
                // Execute the database operation with resilience patterns
                Long id = Retry.decorateSupplier(retry, 
                        CircuitBreaker.decorateSupplier(circuitBreaker, databaseOperation))
                        .get();
                
                // Set the generated ID on the position
                position.setId(id);
                
                // Mark the span as successful
                span.setStatus(StatusCode.OK);
                
                // Publish the position to the message broker asynchronously
                CompletableFuture.runAsync(() -> {
                    try {
                        positionProducer.sendPosition(position);
                    } catch (Exception e) {
                        LOGGER.warn("Failed to publish position to message broker", e);
                    }
                });
                
            } catch (Exception e) {
                // Handle circuit breaker open or other exceptions
                if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
                    LOGGER.warn("Circuit breaker is open, database operations are temporarily disabled");
                    // Implement graceful degradation - still publish to message broker for eventual consistency
                    CompletableFuture.runAsync(() -> {
                        try {
                            positionProducer.sendPosition(position);
                        } catch (Exception ex) {
                            LOGGER.warn("Failed to publish position to message broker during circuit breaker open", ex);
                        }
                    });
                } else {
                    LOGGER.warn("Failed to store position", e);
                }
                
                // Mark the span as failed with error details
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.recordException(e);
            }
        } finally {
            // End the span regardless of success or failure
            span.end();
            
            // Always call the callback to continue processing
            callback.processed(false);
        }
    }
}