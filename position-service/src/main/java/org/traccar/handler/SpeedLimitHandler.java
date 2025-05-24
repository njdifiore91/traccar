/*
 * Copyright 2020 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Position;
import org.traccar.speedlimit.SpeedLimitProvider;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class SpeedLimitHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpeedLimitHandler.class);

    private final SpeedLimitProvider speedLimitProvider;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    
    private final Timer speedLimitRequestTimer;
    private final Counter speedLimitSuccessCounter;
    private final Counter speedLimitFailureCounter;

    @Inject
    public SpeedLimitHandler(
            SpeedLimitProvider speedLimitProvider,
            MeterRegistry meterRegistry,
            Tracer tracer) {
        this.speedLimitProvider = speedLimitProvider;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        
        // Initialize metrics
        this.speedLimitRequestTimer = Timer.builder("speedlimit.request.duration")
                .description("Time taken to retrieve speed limit data")
                .register(meterRegistry);
        this.speedLimitSuccessCounter = Counter.builder("speedlimit.request.success")
                .description("Number of successful speed limit retrievals")
                .register(meterRegistry);
        this.speedLimitFailureCounter = Counter.builder("speedlimit.request.failure")
                .description("Number of failed speed limit retrievals")
                .register(meterRegistry);
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .slidingWindowSize(10) // Consider last 10 calls for failure rate calculation
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 calls in half-open state
                .recordExceptions(Exception.class) // Record all exceptions as failures
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("speedLimitService");
        
        // Configure retry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Try up to 3 times
                .waitDuration(Duration.ofMillis(500)) // Initial wait of 500ms
                .retryExceptions(Exception.class) // Retry on all exceptions except circuit breaker exceptions
                .ignoreExceptions(io.github.resilience4j.circuitbreaker.CallNotPermittedException.class)
                .exponentialBackoff(Duration.ofMillis(500), Duration.ofSeconds(5), 2.0) // Exponential backoff
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry("speedLimitRetry");
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("speedlimit.retrieve")
                .setAttribute("position.latitude", position.getLatitude())
                .setAttribute("position.longitude", position.getLongitude())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Decorate the speed limit retrieval with retry and circuit breaker
            Supplier<Void> speedLimitRetrieval = Retry.decorateSupplier(retry, () -> {
                return CircuitBreaker.decorateSupplier(circuitBreaker, () -> {
                    return speedLimitRequestTimer.record(() -> {
                        try {
                            speedLimitProvider.getSpeedLimit(position.getLatitude(), position.getLongitude(),
                                    new SpeedLimitProvider.SpeedLimitProviderCallback() {
                                        @Override
                                        public void onSuccess(double speedLimit) {
                                            position.set(Position.KEY_SPEED_LIMIT, speedLimit);
                                            speedLimitSuccessCounter.increment();
                                            span.setAttribute("speedlimit.value", speedLimit);
                                            span.setStatus(StatusCode.OK);
                                            callback.processed(false);
                                        }

                                        @Override
                                        public void onFailure(Throwable e) {
                                            LOGGER.warn("Speed limit provider failed", e);
                                            speedLimitFailureCounter.increment();
                                            span.recordException(e);
                                            span.setStatus(StatusCode.ERROR, e.getMessage());
                                            throw new RuntimeException("Speed limit provider failed", e);
                                        }
                                    });
                            return null; // Return null as we're using callbacks
                        } catch (Exception e) {
                            LOGGER.warn("Speed limit retrieval failed", e);
                            speedLimitFailureCounter.increment();
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            throw e;
                        }
                    });
                });
            });

            try {
                speedLimitRetrieval.get();
            } catch (Exception e) {
                // If all retries fail or circuit is open, we still need to call the callback
                LOGGER.error("Speed limit retrieval failed after retries or circuit breaker opened", e);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, "Failed after retries: " + e.getMessage());
                callback.processed(false);
            }
        } finally {
            span.end();
        }
    }
}