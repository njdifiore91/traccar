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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.helper.model.PositionUtil;
import org.traccar.messaging.MessageBroker;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.session.ConnectionManager;
import org.traccar.session.cache.CacheManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class PostProcessHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessHandler.class);
    private static final String POSITION_TOPIC = "position.updates";
    private static final String CIRCUIT_BREAKER_NAME = "postProcessStorage";
    private static final String RETRY_NAME = "postProcessStorageRetry";

    private final CacheManager cacheManager;
    private final Storage storage;
    private final ConnectionManager connectionManager;
    private final MessageBroker messageBroker;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    
    // Metrics
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Timer processingTimer;

    @Inject
    public PostProcessHandler(
            CacheManager cacheManager, 
            Storage storage, 
            ConnectionManager connectionManager,
            MessageBroker messageBroker,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.cacheManager = cacheManager;
        this.storage = storage;
        this.connectionManager = connectionManager;
        this.messageBroker = messageBroker;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME, circuitBreakerConfig);
        
        // Configure retry
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(StorageException.class)
                .build();
        this.retry = retryRegistry.retry(RETRY_NAME, retryConfig);
        
        // Initialize metrics
        this.successCounter = meterRegistry.counter("post.process.success");
        this.failureCounter = meterRegistry.counter("post.process.failure");
        this.processingTimer = meterRegistry.timer("post.process.time");
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for tracing
        Span span = tracer.spanBuilder("PostProcessHandler.onPosition")
                .setParent(Context.current())
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .setAttribute("positionId", String.valueOf(position.getId()))
                .startSpan();
        
        try {
            // Record processing time
            processingTimer.record(() -> {
                processPosition(position, callback, span);
            });
        } finally {
            span.end();
        }
    }
    
    private void processPosition(Position position, Callback callback, Span parentSpan) {
        try {
            if (PositionUtil.isLatest(cacheManager, position)) {
                Span span = tracer.spanBuilder("PostProcessHandler.updateDevice")
                        .setParent(Context.current().with(parentSpan))
                        .startSpan();
                
                try {
                    // Update device position ID with circuit breaker and retry
                    updateDevicePositionId(position);
                    
                    // Update cache and notify clients
                    cacheManager.updatePosition(position);
                    connectionManager.updatePosition(true, position);
                    
                    // Publish position update to message broker for asynchronous processing
                    publishPositionUpdate(position, span);
                    
                    // Record success metric
                    successCounter.increment();
                } catch (Exception e) {
                    // Record failure metric
                    failureCounter.increment();
                    span.recordException(e);
                    throw e;
                } finally {
                    span.end();
                }
            }
        } catch (Exception error) {
            LOGGER.warn("Failed to process position", error);
            parentSpan.recordException(error);
        }
        callback.processed(false);
    }
    
    private void updateDevicePositionId(Position position) {
        Device updatedDevice = new Device();
        updatedDevice.setId(position.getDeviceId());
        updatedDevice.setPositionId(position.getId());
        
        // Use circuit breaker and retry pattern for database operation
        Supplier<Void> storageOperation = Retry.decorateSupplier(retry, () -> {
            try {
                storage.updateObject(updatedDevice, new Request(
                        new Columns.Include("positionId"),
                        new Condition.Equals("id", updatedDevice.getId())));
                return null;
            } catch (StorageException e) {
                LOGGER.debug("Retrying database operation", e);
                throw e;
            }
        });
        
        try {
            // Execute with circuit breaker for fault tolerance
            CircuitBreaker.decorateSupplier(circuitBreaker, storageOperation).get();
        } catch (Exception e) {
            LOGGER.warn("Circuit breaker prevented database operation", e);
            // Graceful degradation - continue with cache updates even if database is unavailable
            // The position will still be processed and available in memory
        }
    }
    
    private void publishPositionUpdate(Position position, Span parentSpan) {
        Span span = tracer.spanBuilder("PostProcessHandler.publishPositionUpdate")
                .setParent(Context.current().with(parentSpan))
                .startSpan();
        
        try {
            // Asynchronously publish position update to message broker
            CompletableFuture.runAsync(() -> {
                try {
                    messageBroker.publish(POSITION_TOPIC, position);
                    LOGGER.debug("Published position update to broker: {}", position.getId());
                } catch (Exception e) {
                    LOGGER.warn("Failed to publish position update to broker", e);
                }
            });
        } finally {
            span.end();
        }
    }
}