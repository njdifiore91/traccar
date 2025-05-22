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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.database.StatisticsManager;
import org.traccar.geolocation.GeolocationProvider;
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class GeolocationHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeolocationHandler.class);

    private final GeolocationProvider geolocationProvider;
    private final CacheManager cacheManager;
    private final StatisticsManager statisticsManager;
    private final boolean processInvalidPositions;
    private final boolean reuse;
    private final boolean requireWifi;
    
    // Circuit breaker for geolocation service calls
    private final CircuitBreaker circuitBreaker;
    
    // OpenTelemetry tracer for distributed tracing
    private final Tracer tracer;
    
    // Micrometer metrics
    private final Timer geolocationRequestTimer;
    private final Counter geolocationSuccessCounter;
    private final Counter geolocationFailureCounter;
    private final Counter geolocationFallbackCounter;
    
    // Message broker for asynchronous processing
    private final MessagePublisher messagePublisher;

    public GeolocationHandler(
            Config config, GeolocationProvider geolocationProvider, CacheManager cacheManager,
            StatisticsManager statisticsManager, Tracer tracer, MeterRegistry meterRegistry,
            MessagePublisher messagePublisher) {
        this.geolocationProvider = geolocationProvider;
        this.cacheManager = cacheManager;
        this.statisticsManager = statisticsManager;
        this.tracer = tracer;
        this.messagePublisher = messagePublisher;
        processInvalidPositions = config.getBoolean(Keys.GEOLOCATION_PROCESS_INVALID_POSITIONS);
        reuse = config.getBoolean(Keys.GEOLOCATION_REUSE);
        requireWifi = config.getBoolean(Keys.GEOLOCATION_REQUIRE_WIFI);
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to trip circuit
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .slidingWindowSize(10) // Consider last 10 calls for failure rate
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 calls in half-open state
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("geolocationService");
        
        // Initialize metrics
        this.geolocationRequestTimer = Timer.builder("geolocation.request.duration")
                .description("Time taken for geolocation requests")
                .register(meterRegistry);
        
        this.geolocationSuccessCounter = Counter.builder("geolocation.request.success")
                .description("Number of successful geolocation requests")
                .register(meterRegistry);
        
        this.geolocationFailureCounter = Counter.builder("geolocation.request.failure")
                .description("Number of failed geolocation requests")
                .register(meterRegistry);
        
        this.geolocationFallbackCounter = Counter.builder("geolocation.request.fallback")
                .description("Number of fallback geolocation responses")
                .register(meterRegistry);
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("GeolocationHandler.onPosition")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            if ((position.getOutdated() || processInvalidPositions && !position.getValid())
                    && position.getNetwork() != null
                    && (!requireWifi || position.getNetwork().getWifiAccessPoints() != null)) {
                
                span.setAttribute("hasNetwork", true);
                
                if (reuse) {
                    Position lastPosition = cacheManager.getPosition(position.getDeviceId());
                    if (lastPosition != null && position.getNetwork().equals(lastPosition.getNetwork())) {
                        span.setAttribute("reuseCache", true);
                        updatePosition(
                                position, lastPosition.getLatitude(), lastPosition.getLongitude(),
                                lastPosition.getAccuracy());
                        span.setStatus(StatusCode.OK);
                        callback.processed(false);
                        return;
                    }
                }

                if (statisticsManager != null) {
                    statisticsManager.registerGeolocationRequest();
                }
                
                // Use circuit breaker to handle geolocation service calls
                Supplier<CompletableFuture<Void>> geolocationSupplier = () -> {
                    CompletableFuture<Void> future = new CompletableFuture<>();
                    
                    // Create a child span for the geolocation request
                    Span geolocationSpan = tracer.spanBuilder("GeolocationProvider.getLocation")
                            .setParent(Context.current())
                            .setSpanKind(SpanKind.CLIENT)
                            .startSpan();
                    
                    try (Scope geolocationScope = geolocationSpan.makeCurrent()) {
                        geolocationSpan.setAttribute("networkType", position.getNetwork().getType());
                        
                        // Time the geolocation request
                        Timer.Sample sample = Timer.start();
                        
                        geolocationProvider.getLocation(position.getNetwork(),
                                new GeolocationProvider.LocationProviderCallback() {
                            @Override
                            public void onSuccess(double latitude, double longitude, double accuracy) {
                                sample.stop(geolocationRequestTimer);
                                geolocationSuccessCounter.increment();
                                
                                geolocationSpan.setAttribute("latitude", latitude);
                                geolocationSpan.setAttribute("longitude", longitude);
                                geolocationSpan.setAttribute("accuracy", accuracy);
                                geolocationSpan.setStatus(StatusCode.OK);
                                geolocationSpan.end();
                                
                                updatePosition(position, latitude, longitude, accuracy);
                                future.complete(null);
                                callback.processed(false);
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                sample.stop(geolocationRequestTimer);
                                geolocationFailureCounter.increment();
                                
                                LOGGER.warn("Geolocation network error", e);
                                geolocationSpan.recordException(e);
                                geolocationSpan.setStatus(StatusCode.ERROR, e.getMessage());
                                geolocationSpan.end();
                                
                                future.completeExceptionally(e);
                                callback.processed(false);
                            }
                        });
                    }
                    
                    return future;
                };
                
                // Execute with circuit breaker
                try {
                    circuitBreaker.executeSupplier(geolocationSupplier);
                } catch (Exception e) {
                    // Circuit is open or call failed, use fallback
                    geolocationFallbackCounter.increment();
                    span.setAttribute("circuitBreaker.state", circuitBreaker.getState().name());
                    span.recordException(e);
                    
                    LOGGER.warn("Geolocation service unavailable, using fallback", e);
                    handleGeolocationFallback(position, span);
                    callback.processed(false);
                }
            } else {
                span.setAttribute("hasNetwork", false);
                span.setStatus(StatusCode.OK, "No network information to process");
                callback.processed(false);
            }
        } finally {
            span.end();
        }
    }
    
    private void handleGeolocationFallback(Position position, Span span) {
        // Try to use last known position from cache as fallback
        Position lastPosition = cacheManager.getPosition(position.getDeviceId());
        if (lastPosition != null && lastPosition.getValid()) {
            span.setAttribute("fallback", "lastKnownPosition");
            updatePosition(
                    position, lastPosition.getLatitude(), lastPosition.getLongitude(),
                    lastPosition.getAccuracy() * 2); // Increase accuracy to indicate less precision
        } else {
            // No fallback available, mark position as invalid
            span.setAttribute("fallback", "none");
            position.setValid(false);
        }
    }

    private void updatePosition(Position position, double latitude, double longitude, double accuracy) {
        position.set(Position.KEY_APPROXIMATE, true);
        position.setValid(true);
        position.setFixTime(position.getDeviceTime());
        position.setLatitude(latitude);
        position.setLongitude(longitude);
        position.setAccuracy(accuracy);
        position.setAltitude(0);
        position.setSpeed(0);
        position.setCourse(0);
    }
}