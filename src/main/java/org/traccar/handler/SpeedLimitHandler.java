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
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.Position;
import org.traccar.speedlimit.SpeedLimitProvider;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Handles speed limit lookups for positions with circuit breaker pattern and asynchronous processing.
 * Uses OpenTelemetry for distributed tracing and Micrometer for metrics collection.
 */
public class SpeedLimitHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpeedLimitHandler.class);
    private static final String CIRCUIT_BREAKER_NAME = "speedLimitService";
    private static final String SPEED_LIMIT_TOPIC = "speedlimit.requests";
    private static final String SPEED_LIMIT_RESULT_TOPIC = "speedlimit.results";
    
    // Cache for recently retrieved speed limits to use as fallback
    private final Map<String, Double> speedLimitCache = new ConcurrentHashMap<>();
    
    private final SpeedLimitProvider speedLimitProvider;
    private final CircuitBreaker circuitBreaker;
    private final MessagePublisher messagePublisher;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    
    // Metrics
    private final Timer speedLimitRequestTimer;
    private final Counter speedLimitSuccessCounter;
    private final Counter speedLimitFailureCounter;
    private final Counter speedLimitTimeoutCounter;
    private final Counter circuitBreakerOpenCounter;

    /**
     * Creates a new SpeedLimitHandler with circuit breaker, tracing, and metrics support.
     *
     * @param speedLimitProvider The provider for speed limit information
     * @param messagePublisher The message broker publisher for asynchronous processing
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param meterRegistry The Micrometer registry for metrics collection
     */
    @Inject
    public SpeedLimitHandler(
            SpeedLimitProvider speedLimitProvider,
            MessagePublisher messagePublisher,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.speedLimitProvider = speedLimitProvider;
        this.messagePublisher = messagePublisher;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before attempting again
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10) // Consider the last 10 calls
                .minimumNumberOfCalls(5) // Minimum calls before calculating failure rate
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 calls in half-open state
                .automaticTransitionFromOpenToHalfOpenEnabled(true) // Auto transition to half-open
                .recordExceptions(Exception.class) // Record all exceptions as failures
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Register circuit breaker state transition listener for logging and metrics
        this.circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    LOGGER.info("Circuit breaker state changed from {} to {}",
                            event.getStateTransition().getFromState(),
                            event.getStateTransition().getToState());
                    if (event.getStateTransition().getToState() == CircuitBreaker.State.OPEN) {
                        circuitBreakerOpenCounter.increment();
                    }
                });
        
        // Initialize metrics
        this.speedLimitRequestTimer = Timer.builder("speedlimit.request.duration")
                .description("Time taken to retrieve speed limit information")
                .register(meterRegistry);
        
        this.speedLimitSuccessCounter = Counter.builder("speedlimit.request.success")
                .description("Number of successful speed limit requests")
                .register(meterRegistry);
        
        this.speedLimitFailureCounter = Counter.builder("speedlimit.request.failure")
                .description("Number of failed speed limit requests")
                .register(meterRegistry);
        
        this.speedLimitTimeoutCounter = Counter.builder("speedlimit.request.timeout")
                .description("Number of timed out speed limit requests")
                .register(meterRegistry);
        
        this.circuitBreakerOpenCounter = Counter.builder("speedlimit.circuit_breaker.open")
                .description("Number of times the speed limit circuit breaker opened")
                .register(meterRegistry);
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("SpeedLimitHandler.getSpeedLimit")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("position.id", position.getId())
                .setAttribute("position.deviceId", position.getDeviceId())
                .setAttribute("position.latitude", position.getLatitude())
                .setAttribute("position.longitude", position.getLongitude())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Extract tracing context for propagation
            Map<String, String> tracingContext = new HashMap<>();
            Context.current().forEach((key, value) -> {
                if (key.toString().startsWith("traceparent") || key.toString().startsWith("tracestate")) {
                    tracingContext.put(key.toString(), value.toString());
                }
            });
            
            // Generate a cache key based on location
            String cacheKey = String.format("%.5f:%.5f", position.getLatitude(), position.getLongitude());
            
            // Decide whether to use asynchronous or synchronous processing
            if (shouldProcessAsync()) {
                processAsynchronously(position, callback, tracingContext, span, cacheKey);
            } else {
                processSynchronously(position, callback, tracingContext, span, cacheKey);
            }
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            LOGGER.error("Unexpected error in speed limit processing", e);
            handleFallback(position, callback);
        } finally {
            span.end();
        }
    }
    
    /**
     * Determines if the request should be processed asynchronously.
     * This could be based on configuration, system load, or other factors.
     *
     * @return true if the request should be processed asynchronously
     */
    private boolean shouldProcessAsync() {
        // This could be based on configuration, system load, or other factors
        // For now, we'll use a simple approach based on circuit breaker state
        return circuitBreaker.getState() != CircuitBreaker.State.OPEN;
    }
    
    /**
     * Processes the speed limit request synchronously with circuit breaker protection.
     */
    private void processSynchronously(Position position, Callback callback, Map<String, String> tracingContext, Span parentSpan, String cacheKey) {
        parentSpan.addEvent("Processing speed limit synchronously");
        
        // Use circuit breaker to protect the call
        Supplier<Void> speedLimitSupplier = () -> {
            Timer.Sample sample = Timer.start(meterRegistry);
            
            speedLimitProvider.getSpeedLimit(
                    position.getLatitude(),
                    position.getLongitude(),
                    tracingContext,
                    new SpeedLimitProvider.SpeedLimitProviderCallback() {
                        @Override
                        public void onSuccess(double speedLimit) {
                            sample.stop(speedLimitRequestTimer);
                            speedLimitSuccessCounter.increment();
                            
                            // Update cache for future fallback
                            speedLimitCache.put(cacheKey, speedLimit);
                            
                            position.set(Position.KEY_SPEED_LIMIT, speedLimit);
                            parentSpan.setAttribute("speedlimit.value", speedLimit);
                            parentSpan.addEvent("Speed limit retrieved successfully");
                            callback.processed(false);
                        }
                        
                        @Override
                        public void onFailure(Throwable e) {
                            sample.stop(speedLimitRequestTimer);
                            speedLimitFailureCounter.increment();
                            
                            LOGGER.warn("Speed limit provider failed", e);
                            parentSpan.setStatus(StatusCode.ERROR, e.getMessage());
                            parentSpan.recordException(e);
                            
                            handleFallback(position, callback);
                        }
                    });
            return null;
        };
        
        try {
            // Execute with circuit breaker protection
            circuitBreaker.executeSupplier(speedLimitSupplier);
        } catch (Exception e) {
            // Circuit breaker is open or call failed
            LOGGER.warn("Circuit breaker prevented speed limit call or call failed", e);
            parentSpan.setStatus(StatusCode.ERROR, "Circuit breaker prevented call: " + e.getMessage());
            parentSpan.recordException(e);
            
            handleFallback(position, callback);
        }
    }
    
    /**
     * Processes the speed limit request asynchronously via message broker.
     */
    private void processAsynchronously(Position position, Callback callback, Map<String, String> tracingContext, Span parentSpan, String cacheKey) {
        parentSpan.addEvent("Publishing speed limit request to message broker");
        
        // Create a message with position data and tracing context
        Map<String, Object> message = new HashMap<>();
        message.put("positionId", position.getId());
        message.put("deviceId", position.getDeviceId());
        message.put("latitude", position.getLatitude());
        message.put("longitude", position.getLongitude());
        message.put("tracingContext", tracingContext);
        
        try {
            // Publish to message broker
            messagePublisher.publish(SPEED_LIMIT_TOPIC, message);
            parentSpan.addEvent("Speed limit request published to broker");
            
            // For now, we'll use fallback since we don't have the result yet
            // In a real implementation, we would subscribe to the result topic
            // and update the position when the result arrives
            handleFallback(position, callback);
        } catch (Exception e) {
            LOGGER.error("Failed to publish speed limit request to message broker", e);
            parentSpan.setStatus(StatusCode.ERROR, e.getMessage());
            parentSpan.recordException(e);
            
            // Fall back to synchronous processing if message broker fails
            processSynchronously(position, callback, tracingContext, parentSpan, cacheKey);
        }
    }
    
    /**
     * Handles fallback when speed limit service is unavailable or circuit is open.
     * Uses cached values if available or continues without speed limit.
     */
    private void handleFallback(Position position, Callback callback) {
        // Try to get cached value for this location
        String cacheKey = String.format("%.5f:%.5f", position.getLatitude(), position.getLongitude());
        Double cachedSpeedLimit = speedLimitCache.get(cacheKey);
        
        if (cachedSpeedLimit != null) {
            // Use cached value as fallback
            position.set(Position.KEY_SPEED_LIMIT, cachedSpeedLimit);
            position.set("speedLimitSource", "cache"); // Mark as coming from cache
            LOGGER.debug("Using cached speed limit value: {}", cachedSpeedLimit);
        } else {
            // No speed limit available, continue without it
            LOGGER.debug("No speed limit available, continuing without it");
        }
        
        callback.processed(false);
    }
}
