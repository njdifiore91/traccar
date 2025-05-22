/*
 * Copyright 2012 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.geocoder.Geocoder;
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class GeocoderHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeocoderHandler.class);

    private final Geocoder geocoder;
    private final CacheManager cacheManager;
    private final boolean ignorePositions;
    private final boolean processInvalidPositions;
    private final int reuseDistance;
    private final MessagePublisher messagePublisher;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Timer geocodingRequestTimer;
    private final Counter geocodingSuccessCounter;
    private final Counter geocodingFailureCounter;
    private final Counter geocodingCacheHitCounter;
    private final Counter geocodingCircuitBreakerOpenCounter;

    @Inject
    public GeocoderHandler(
            Config config,
            Geocoder geocoder,
            CacheManager cacheManager,
            MessagePublisher messagePublisher,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.geocoder = geocoder;
        this.cacheManager = cacheManager;
        this.messagePublisher = messagePublisher;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        ignorePositions = config.getBoolean(Keys.GEOCODER_IGNORE_POSITIONS);
        processInvalidPositions = config.getBoolean(Keys.GEOCODER_PROCESS_INVALID_POSITIONS);
        reuseDistance = config.getInteger(Keys.GEOCODER_REUSE_DISTANCE, 0);
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(config.getFloat(Keys.GEOCODER_CIRCUIT_BREAKER_FAILURE_RATE, 50.0f))
                .waitDurationInOpenState(Duration.ofSeconds(
                        config.getInteger(Keys.GEOCODER_CIRCUIT_BREAKER_OPEN_DURATION, 60)))
                .permittedNumberOfCallsInHalfOpenState(
                        config.getInteger(Keys.GEOCODER_CIRCUIT_BREAKER_PERMITTED_CALLS, 10))
                .slidingWindowSize(config.getInteger(Keys.GEOCODER_CIRCUIT_BREAKER_WINDOW_SIZE, 100))
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        circuitBreaker = circuitBreakerRegistry.circuitBreaker("geocoder");
        
        // Configure retry
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(config.getInteger(Keys.GEOCODER_RETRY_COUNT, 3))
                .waitDuration(Duration.ofMillis(config.getInteger(Keys.GEOCODER_RETRY_DELAY, 1000)))
                .retryExceptions(Exception.class)
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        retry = retryRegistry.retry("geocoder");
        
        // Initialize metrics
        geocodingRequestTimer = Timer.builder("geocoder.request.duration")
                .description("Time taken to perform geocoding requests")
                .register(meterRegistry);
        
        geocodingSuccessCounter = Counter.builder("geocoder.request.success")
                .description("Number of successful geocoding requests")
                .register(meterRegistry);
        
        geocodingFailureCounter = Counter.builder("geocoder.request.failure")
                .description("Number of failed geocoding requests")
                .register(meterRegistry);
        
        geocodingCacheHitCounter = Counter.builder("geocoder.cache.hit")
                .description("Number of geocoding cache hits")
                .register(meterRegistry);
        
        geocodingCircuitBreakerOpenCounter = Counter.builder("geocoder.circuit_breaker.open")
                .description("Number of times the geocoder circuit breaker was open")
                .register(meterRegistry);
        
        // Register circuit breaker state change listener for metrics
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    if (event.getStateTransition() == CircuitBreaker.StateTransition.CLOSED_TO_OPEN) {
                        geocodingCircuitBreakerOpenCounter.increment();
                        LOGGER.warn("Geocoder circuit breaker transitioned from CLOSED to OPEN");
                    }
                });
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        if (!ignorePositions && (processInvalidPositions || position.getValid())) {
            // Create a span for this operation
            Span span = tracer.spanBuilder("geocoder.getAddress")
                    .setAttribute("position.deviceId", position.getDeviceId())
                    .setAttribute("position.id", position.getId())
                    .setAttribute("position.latitude", position.getLatitude())
                    .setAttribute("position.longitude", position.getLongitude())
                    .startSpan();
            
            try (Scope scope = span.makeCurrent()) {
                // Check if we can reuse address from previous position
                if (reuseDistance != 0) {
                    Position lastPosition = cacheManager.getPosition(position.getDeviceId());
                    if (lastPosition != null && lastPosition.getAddress() != null
                            && position.getDouble(Position.KEY_DISTANCE) <= reuseDistance) {
                        position.setAddress(lastPosition.getAddress());
                        geocodingCacheHitCounter.increment();
                        span.addEvent("cache.hit");
                        span.setStatus(StatusCode.OK);
                        callback.processed(false);
                        return;
                    }
                }

                // Wrap geocoding operation with circuit breaker and retry
                Supplier<CompletableFuture<String>> geocodingSupplier = () -> {
                    CompletableFuture<String> future = new CompletableFuture<>();
                    
                    Timer.Sample sample = Timer.start(meterRegistry);
                    
                    geocoder.getAddress(position.getLatitude(), position.getLongitude(),
                            new Geocoder.ReverseGeocoderCallback() {
                                @Override
                                public void onSuccess(String address) {
                                    sample.stop(geocodingRequestTimer);
                                    geocodingSuccessCounter.increment();
                                    span.addEvent("geocoder.success");
                                    future.complete(address);
                                }

                                @Override
                                public void onFailure(Throwable e) {
                                    sample.stop(geocodingRequestTimer);
                                    geocodingFailureCounter.increment();
                                    span.recordException(e);
                                    span.addEvent("geocoder.failure");
                                    future.completeExceptionally(e);
                                }
                            });
                    
                    return future;
                };
                
                // Apply circuit breaker and retry patterns
                Supplier<CompletableFuture<String>> decoratedSupplier = Retry.decorateCompletionStage(
                        retry,
                        CircuitBreaker.decorateCompletionStage(
                                circuitBreaker,
                                ctx -> geocodingSupplier.get()));
                
                // Execute the geocoding operation with resilience patterns
                decoratedSupplier.get()
                        .whenComplete((address, throwable) -> {
                            if (throwable != null) {
                                handleGeocodingFailure(position, callback, span, throwable);
                            } else {
                                position.setAddress(address);
                                span.setStatus(StatusCode.OK);
                                callback.processed(false);
                            }
                        });
            } catch (Exception e) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.end();
                LOGGER.warn("Geocoding failed", e);
                callback.processed(false);
            }
        } else {
            callback.processed(false);
        }
    }
    
    private void handleGeocodingFailure(Position position, Callback callback, Span span, Throwable throwable) {
        LOGGER.warn("Geocoding failed", throwable);
        
        // Publish geocoding failure event to message broker for monitoring
        try {
            messagePublisher.publish("geocoder.failures", 
                    String.format("{\"deviceId\":%d,\"positionId\":%d,\"error\":\"%s\"}", 
                            position.getDeviceId(), position.getId(), 
                            throwable.getMessage().replace("\"", "\\\"")));
        } catch (Exception e) {
            LOGGER.warn("Failed to publish geocoding failure event", e);
        }
        
        // Set status and end the span
        span.setStatus(StatusCode.ERROR, throwable.getMessage());
        span.end();
        
        // Continue processing without address
        callback.processed(false);
    }
}