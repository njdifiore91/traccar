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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.geocoder.Geocoder;
import org.traccar.geocoder.GeocoderMetrics;
import org.traccar.model.Position;
import org.traccar.position.cache.PositionCache;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Position handler for enriching positions with address information through geocoding.
 * Implements resilience patterns for geocoding service interactions and provides monitoring.
 */
public class GeocoderHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeocoderHandler.class);
    
    private static final String SPAN_NAME = "geocoder.getAddress";
    private static final AttributeKey<Double> ATTR_LATITUDE = AttributeKey.doubleKey("geocoder.latitude");
    private static final AttributeKey<Double> ATTR_LONGITUDE = AttributeKey.doubleKey("geocoder.longitude");
    private static final AttributeKey<Boolean> ATTR_CACHE_HIT = AttributeKey.booleanKey("geocoder.cache.hit");
    private static final AttributeKey<Boolean> ATTR_REUSED = AttributeKey.booleanKey("geocoder.address.reused");

    private final Geocoder geocoder;
    private final PositionCache positionCache;
    private final boolean ignorePositions;
    private final boolean processInvalidPositions;
    private final int reuseDistance;
    private final Tracer tracer;

    /**
     * Constructs a new GeocoderHandler with the specified configuration and dependencies.
     *
     * @param config Configuration for the handler
     * @param geocoder Geocoder implementation to use for address lookup
     * @param positionCache Cache for position data
     * @param tracer OpenTelemetry tracer for instrumentation
     */
    public GeocoderHandler(Config config, Geocoder geocoder, PositionCache positionCache, Tracer tracer) {
        this.geocoder = geocoder;
        this.positionCache = positionCache;
        this.tracer = tracer;
        
        // Configure circuit breaker and retry policy for geocoder
        geocoder.configureCircuitBreaker(5, 30000); // 5 failures to open, 30s reset timeout
        geocoder.configureRetryPolicy(3, 1000, 10000); // 3 retries, 1s initial delay, 10s max delay
        
        // Read configuration settings
        ignorePositions = config.getBoolean(Keys.GEOCODER_IGNORE_POSITIONS);
        processInvalidPositions = config.getBoolean(Keys.GEOCODER_PROCESS_INVALID_POSITIONS);
        reuseDistance = config.getInteger(Keys.GEOCODER_REUSE_DISTANCE, 0);
        
        LOGGER.info("GeocoderHandler initialized with ignorePositions={}, processInvalidPositions={}, reuseDistance={}",
                ignorePositions, processInvalidPositions, reuseDistance);
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        if (ignorePositions || (!processInvalidPositions && !position.getValid())) {
            callback.processed(false);
            return;
        }

        // Create a span for this geocoding operation
        Span span = tracer.spanBuilder(SPAN_NAME)
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute(ATTR_LATITUDE, position.getLatitude())
                .setAttribute(ATTR_LONGITUDE, position.getLongitude())
                .startSpan();
        
        try {
            // Check if we can reuse the address from a previous position
            if (reuseDistance > 0) {
                Position lastPosition = positionCache.getPosition(position.getDeviceId());
                if (lastPosition != null && lastPosition.getAddress() != null
                        && position.getDouble(Position.KEY_DISTANCE) <= reuseDistance) {
                    position.setAddress(lastPosition.getAddress());
                    
                    // Record metrics and tracing for address reuse
                    span.setAttribute(ATTR_REUSED, true);
                    span.setStatus(StatusCode.OK);
                    span.end();
                    
                    callback.processed(false);
                    return;
                }
            }
            
            // Set attribute to indicate we're not reusing an address
            span.setAttribute(ATTR_REUSED, false);
            
            // Use the context to propagate the span
            Context context = Context.current().with(span);
            
            // Get address asynchronously with circuit breaker and retry
            CompletableFuture<String> addressFuture = geocoder.getAddress(
                    position.getLatitude(), position.getLongitude(), true);
            
            addressFuture.whenComplete((address, throwable) -> {
                try {
                    if (throwable != null) {
                        LOGGER.warn("Geocoding failed", throwable);
                        span.recordException(throwable);
                        span.setStatus(StatusCode.ERROR, throwable.getMessage());
                    } else {
                        position.setAddress(address);
                        span.setStatus(StatusCode.OK);
                    }
                } finally {
                    // Log metrics periodically
                    logMetricsIfNeeded();
                    span.end();
                    callback.processed(false);
                }
            });
        } catch (Exception e) {
            LOGGER.error("Error in geocoder handler", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.end();
            callback.processed(false);
        }
    }
    
    // Track last time metrics were logged to avoid excessive logging
    private long lastMetricsLogTime = 0;
    private static final long METRICS_LOG_INTERVAL = TimeUnit.MINUTES.toMillis(5);
    
    /**
     * Logs geocoder metrics if enough time has passed since the last log
     */
    private void logMetricsIfNeeded() {
        long now = System.currentTimeMillis();
        if (now - lastMetricsLogTime > METRICS_LOG_INTERVAL) {
            lastMetricsLogTime = now;
            GeocoderMetrics metrics = geocoder.getMetrics();
            if (metrics != null) {
                LOGGER.info("Geocoder metrics - Success rate: {}%, Avg response time: {}ms, Cache hit rate: {}%, Requests: {}, Circuit breaker trips: {}",
                        String.format("%.2f", metrics.getSuccessRate()),
                        metrics.getAverageResponseTimeMs(),
                        String.format("%.2f", metrics.getCacheHitRate()),
                        metrics.getTotalRequests(),
                        metrics.getCircuitBreakerTrips());
            }
        }
    }
}