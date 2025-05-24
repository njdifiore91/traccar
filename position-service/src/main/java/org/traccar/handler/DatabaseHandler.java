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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
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
import java.util.function.Supplier;

/**
 * Database handler for persisting position data and publishing to message broker.
 * Implements resilience patterns for database operations.
 */
public class DatabaseHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseHandler.class);

    private final Storage storage;
    private final StatisticsManager statisticsManager;
    private final PositionProducer positionProducer;
    private final Tracer tracer;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;

    /**
     * Constructs a new DatabaseHandler with the specified dependencies.
     *
     * @param storage The storage implementation for database operations
     * @param statisticsManager The statistics manager for tracking metrics
     * @param positionProducer The producer for publishing positions to message broker
     * @param tracer The OpenTelemetry tracer for instrumentation
     */
    @Inject
    public DatabaseHandler(Storage storage, StatisticsManager statisticsManager, 
                          PositionProducer positionProducer, Tracer tracer) {
        this.storage = storage;
        this.statisticsManager = statisticsManager;
        this.positionProducer = positionProducer;
        this.tracer = tracer;
        
        // Configure circuit breaker for database operations
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(10)) // Wait 10 seconds before attempting again
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .slidingWindowSize(10) // Consider last 10 calls for failure rate
                .recordExceptions(StorageException.class) // Record storage exceptions
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("databaseStorage");
        
        // Configure retry with exponential backoff for transient failures
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Maximum 3 attempts
                .waitDuration(Duration.ofMillis(500)) // Initial wait of 500ms
                .retryExceptions(StorageException.class) // Retry on storage exceptions
                .enableExponentialBackoff(true) // Enable exponential backoff
                .exponentialBackoffMultiplier(2) // Multiply wait time by 2 for each retry
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry("databaseStorage");
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for database operation
        Span span = tracer.spanBuilder("database.storePosition").startSpan();
        span.setAttribute("deviceId", String.valueOf(position.getDeviceId()));
        span.setAttribute("protocol", position.getProtocol());
        
        try (Scope scope = span.makeCurrent()) {
            // Implement idempotent processing by checking if position already exists
            // This is important for at-least-once delivery semantics in a message-based architecture
            if (position.getId() > 0) {
                LOGGER.debug("Position already has ID {}, skipping database storage", position.getId());
                publishPosition(position, callback);
                span.setStatus(StatusCode.OK);
                return;
            }
            
            // Use circuit breaker and retry patterns for database operation
            Supplier<Long> storePositionSupplier = () -> {
                try {
                    return storage.addObject(position, new Request(new Columns.Exclude("id")));
                } catch (StorageException e) {
                    LOGGER.warn("Failed to store position", e);
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, "Failed to store position: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            };
            
            // Apply circuit breaker and retry patterns
            try {
                Long positionId = retry.executeSupplier(
                    circuitBreaker.decorateSupplier(storePositionSupplier));
                
                position.setId(positionId);
                statisticsManager.registerMessageStored(position.getDeviceId(), position.getProtocol());
                span.setStatus(StatusCode.OK);
                
                // Publish position to message broker for downstream services
                publishPosition(position, callback);
            } catch (Exception e) {
                LOGGER.error("Failed to store position after retries", e);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, "Failed to store position after retries: " + e.getMessage());
                callback.processed(false);
            }
        } finally {
            span.end();
        }
    }
    
    /**
     * Publishes the position to the message broker for downstream services.
     *
     * @param position The position to publish
     * @param callback The callback to invoke after processing
     */
    private void publishPosition(Position position, Callback callback) {
        Span span = tracer.spanBuilder("messaging.publishPosition").startSpan();
        span.setAttribute("deviceId", String.valueOf(position.getDeviceId()));
        span.setAttribute("positionId", String.valueOf(position.getId()));
        
        try (Scope scope = span.makeCurrent()) {
            positionProducer.sendPosition(position);
            span.setStatus(StatusCode.OK);
            LOGGER.debug("Published position with ID {} to message broker", position.getId());
        } catch (Exception e) {
            LOGGER.warn("Failed to publish position to message broker", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Failed to publish position: " + e.getMessage());
        } finally {
            span.end();
            callback.processed(false);
        }
    }
}