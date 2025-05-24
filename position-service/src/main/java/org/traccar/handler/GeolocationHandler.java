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
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.geolocation.GeolocationProvider;
import org.traccar.metrics.EnrichmentMetrics;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Handles network-based geolocation for positions that are outdated or invalid.
 * Uses external geolocation services to determine approximate position based on
 * network information (cell towers, WiFi access points).
 */
@Component
public class GeolocationHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeolocationHandler.class);
    private static final String CIRCUIT_BREAKER_NAME = "geolocationService";
    private static final String RETRY_NAME = "geolocationService";
    private static final String SPAN_NAME = "geolocation.lookup";

    private final GeolocationProvider geolocationProvider;
    private final CacheManager cacheManager;
    private final EnrichmentMetrics enrichmentMetrics;
    private final Tracer tracer;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final boolean processInvalidPositions;
    private final boolean reuse;
    private final boolean requireWifi;

    /**
     * Constructs a new GeolocationHandler with the specified dependencies.
     *
     * @param config The configuration provider
     * @param geolocationProvider The geolocation service provider
     * @param cacheManager The cache manager for position caching
     * @param enrichmentMetrics Metrics collector for enrichment operations
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param circuitBreakerRegistry Registry for circuit breakers
     * @param retryRegistry Registry for retry configurations
     */
    @Autowired
    public GeolocationHandler(
            Config config, 
            GeolocationProvider geolocationProvider, 
            CacheManager cacheManager,
            EnrichmentMetrics enrichmentMetrics,
            Tracer tracer,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry) {
        this.geolocationProvider = geolocationProvider;
        this.cacheManager = cacheManager;
        this.enrichmentMetrics = enrichmentMetrics;
        this.tracer = tracer;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        this.retry = retryRegistry.retry(RETRY_NAME);
        this.processInvalidPositions = config.getBoolean(Keys.GEOLOCATION_PROCESS_INVALID_POSITIONS);
        this.reuse = config.getBoolean(Keys.GEOLOCATION_REUSE);
        this.requireWifi = config.getBoolean(Keys.GEOLOCATION_REQUIRE_WIFI);
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Check if position needs geolocation
        if ((position.getOutdated() || processInvalidPositions && !position.getValid())
                && position.getNetwork() != null
                && (!requireWifi || position.getNetwork().getWifiAccessPoints() != null)) {
            
            // Create a span for this operation
            Span span = tracer.spanBuilder(SPAN_NAME)
                    .setParent(Context.current())
                    .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                    .setAttribute("hasWifi", String.valueOf(position.getNetwork().getWifiAccessPoints() != null))
                    .setAttribute("hasCellTowers", String.valueOf(position.getNetwork().getCellTowers() != null))
                    .startSpan();
            
            try {
                // Check cache for reusable position
                if (reuse) {
                    Position lastPosition = cacheManager.getPosition(position.getDeviceId());
                    if (lastPosition != null && position.getNetwork().equals(lastPosition.getNetwork())) {
                        span.addEvent("cache.hit");
                        enrichmentMetrics.recordGeolocationCacheHit();
                        updatePosition(
                                position, lastPosition.getLatitude(), lastPosition.getLongitude(),
                                lastPosition.getAccuracy());
                        callback.processed(false);
                        return;
                    }
                    span.addEvent("cache.miss");
                    enrichmentMetrics.recordGeolocationCacheMiss();
                }

                // Start metrics timer
                enrichmentMetrics.recordGeolocationRequest();
                long startTime = System.currentTimeMillis();

                // Create a CompletableFuture for the geolocation operation
                CompletableFuture<Void> future = new CompletableFuture<>();
                
                // Execute geolocation with circuit breaker and retry
                Supplier<CompletableFuture<Void>> geolocationSupplier = () -> {
                    CompletableFuture<Void> result = new CompletableFuture<>();
                    
                    geolocationProvider.getLocation(position.getNetwork(),
                            new GeolocationProvider.LocationProviderCallback() {
                                @Override
                                public void onSuccess(double latitude, double longitude, double accuracy) {
                                    span.addEvent("geolocation.success");
                                    span.setAttribute("latitude", latitude);
                                    span.setAttribute("longitude", longitude);
                                    span.setAttribute("accuracy", accuracy);
                                    
                                    updatePosition(position, latitude, longitude, accuracy);
                                    
                                    // Record metrics
                                    long duration = System.currentTimeMillis() - startTime;
                                    enrichmentMetrics.recordGeolocationSuccess(duration);
                                    
                                    result.complete(null);
                                }

                                @Override
                                public void onFailure(Throwable e) {
                                    span.recordException(e);
                                    span.addEvent("geolocation.failure");
                                    
                                    // Record metrics
                                    long duration = System.currentTimeMillis() - startTime;
                                    enrichmentMetrics.recordGeolocationFailure(duration);
                                    
                                    result.completeExceptionally(e);
                                }
                            });
                    
                    return result;
                };
                
                // Apply circuit breaker and retry patterns
                Retry.decorateCompletionStage(
                    retry,
                    CircuitBreaker.decorateSupplier(circuitBreaker, geolocationSupplier)
                ).get()
                .whenComplete((result, error) -> {
                    if (error != null) {
                        LOGGER.warn("Geolocation network error after retries", error);
                        span.recordException(error);
                        span.addEvent("geolocation.failure.final");
                    }
                    callback.processed(false);
                    span.end();
                    future.complete(null);
                });
                
                // Wait for the operation to complete
                future.join();
                
            } catch (Exception e) {
                LOGGER.error("Unexpected error in geolocation processing", e);
                span.recordException(e);
                span.end();
                callback.processed(false);
            }
        } else {
            callback.processed(false);
        }
    }

    /**
     * Updates the position with the geolocation results.
     *
     * @param position The position to update
     * @param latitude The resolved latitude
     * @param longitude The resolved longitude
     * @param accuracy The accuracy of the geolocation in meters
     */
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