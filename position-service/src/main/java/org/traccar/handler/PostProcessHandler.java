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
package org.traccar.handler;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.helper.model.PositionUtil;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * PostProcessHandler is responsible for processing position updates in the Traccar position-service.
 * It verifies if a position is the latest for its device, updates the device's positionId in the database,
 * refreshes the cache, and publishes the enriched position to the message broker for downstream services.
 */
public class PostProcessHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessHandler.class);
    private static final String CIRCUIT_BREAKER_NAME = "postProcessStorage";
    private static final String RETRY_NAME = "postProcessStorageRetry";
    private static final String POSITIONS_TOPIC = "enriched-positions";
    
    private final CacheManager cacheManager;
    private final Storage storage;
    private final MessageProducer messageProducer;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Tracer tracer;

    /**
     * Constructs a new PostProcessHandler with the necessary dependencies.
     *
     * @param cacheManager    The cache manager for position data
     * @param storage         The storage interface for database operations
     * @param messageProducer The message producer for publishing to the message broker
     */
    @Inject
    public PostProcessHandler(CacheManager cacheManager, Storage storage, MessageProducer messageProducer) {
        this.cacheManager = cacheManager;
        this.storage = storage;
        this.messageProducer = messageProducer;
        
        // Configure circuit breaker for database operations
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Configure retry with exponential backoff for transient database failures
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(StorageException.class)
                .exponentialBackoff(Duration.ofMillis(500), Duration.ofSeconds(2), 2.0)
                .build();
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry(RETRY_NAME);
        
        // Initialize OpenTelemetry tracer
        this.tracer = GlobalOpenTelemetry.getTracer("org.traccar.handler.PostProcessHandler");
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("PostProcessHandler.onPosition")
                .setAttribute("deviceId", position.getDeviceId())
                .setAttribute("positionId", position.getId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Check if this position is the latest for the device
            if (PositionUtil.isLatest(cacheManager, position)) {
                span.addEvent("Position is latest for device");
                
                // Create device update object
                Device updatedDevice = new Device();
                updatedDevice.setId(position.getDeviceId());
                updatedDevice.setPositionId(position.getId());
                
                // Define database update request
                Request request = new Request(
                        new Columns.Include("positionId"),
                        new Condition.Equals("id", updatedDevice.getId()));
                
                // Execute database update with circuit breaker and retry patterns
                Supplier<Void> databaseOperation = () -> {
                    try {
                        storage.updateObject(updatedDevice, request);
                        return null;
                    } catch (StorageException e) {
                        span.recordException(e);
                        span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, "Database update failed");
                        LOGGER.warn("Failed to update device position", e);
                        throw new RuntimeException("Failed to update device position", e);
                    }
                };
                
                try {
                    // Apply circuit breaker and retry patterns
                    Supplier<Void> decoratedOperation = CircuitBreaker.decorateSupplier(circuitBreaker, 
                            Retry.decorateSupplier(retry, databaseOperation));
                    decoratedOperation.get();
                    
                    // Update cache with the new position
                    cacheManager.updatePosition(position);
                    span.addEvent("Cache updated with new position");
                    
                    // Publish position to message broker for downstream services
                    publishPosition(position, span);
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, "Failed to process position");
                    LOGGER.error("Error in position post-processing", e);
                }
            } else {
                span.addEvent("Position is not the latest for device");
            }
        } finally {
            span.end();
            callback.processed(false);
        }
    }
    
    /**
     * Publishes the position to the message broker for downstream services.
     *
     * @param position The position to publish
     * @param span     The current tracing span
     */
    private void publishPosition(Position position, Span span) {
        try {
            // Add idempotency key to ensure at-least-once delivery semantics
            String idempotencyKey = String.format("%d-%d", position.getDeviceId(), position.getId());
            
            // Publish to the enriched-positions topic
            messageProducer.publish(POSITIONS_TOPIC, idempotencyKey, position);
            span.addEvent("Position published to message broker");
            LOGGER.debug("Published position {} for device {} to message broker", 
                    position.getId(), position.getDeviceId());
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, "Failed to publish position");
            LOGGER.error("Failed to publish position to message broker", e);
        }
    }
}