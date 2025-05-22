/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.inject.Inject;
import org.traccar.config.Config;
import org.traccar.helper.model.GeofenceUtil;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class GeofenceHandler extends BasePositionHandler {

    private final Config config;
    private final CacheManager cacheManager;
    private final CircuitBreaker circuitBreaker;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    
    private final Counter geofenceRequestsCounter;
    private final Counter geofenceSuccessCounter;
    private final Counter geofenceFailureCounter;
    private final Counter circuitBreakerOpenCounter;
    private final Timer geofenceLookupTimer;

    @Inject
    public GeofenceHandler(
            Config config, 
            CacheManager cacheManager, 
            CircuitBreakerRegistry circuitBreakerRegistry,
            MeterRegistry meterRegistry,
            Tracer tracer) {
        this.config = config;
        this.cacheManager = cacheManager;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .recordExceptions(Exception.class)
                .build();
        
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("geofenceLookup", circuitBreakerConfig);
        
        // Initialize metrics
        this.geofenceRequestsCounter = Counter.builder("geofence.requests")
                .description("Number of geofence lookup requests")
                .register(meterRegistry);
        
        this.geofenceSuccessCounter = Counter.builder("geofence.success")
                .description("Number of successful geofence lookups")
                .register(meterRegistry);
        
        this.geofenceFailureCounter = Counter.builder("geofence.failure")
                .description("Number of failed geofence lookups")
                .register(meterRegistry);
        
        this.circuitBreakerOpenCounter = Counter.builder("geofence.circuit_breaker.open")
                .description("Number of times the circuit breaker opened")
                .register(meterRegistry);
        
        this.geofenceLookupTimer = Timer.builder("geofence.lookup.time")
                .description("Time taken for geofence lookups")
                .register(meterRegistry);
        
        // Register circuit breaker state transition listener
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    if (event.getStateTransition() == CircuitBreaker.StateTransition.CLOSED_TO_OPEN) {
                        circuitBreakerOpenCounter.increment();
                    }
                });
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("geofence.lookup")
                .setParent(Context.current())
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .setAttribute("latitude", position.getLatitude())
                .setAttribute("longitude", position.getLongitude())
                .startSpan();
        
        try {
            // Increment request counter
            geofenceRequestsCounter.increment();
            
            // Execute geofence lookup with circuit breaker and timer
            Supplier<List<Long>> geofenceLookupSupplier = () -> {
                return geofenceLookupTimer.record(() -> {
                    return GeofenceUtil.getCurrentGeofences(config, cacheManager, position);
                });
            };
            
            // Execute with circuit breaker and fallback
            List<Long> geofenceIds = circuitBreaker.executeSupplier(() -> {
                try {
                    List<Long> ids = geofenceLookupSupplier.get();
                    geofenceSuccessCounter.increment();
                    span.setAttribute("geofenceCount", ids.size());
                    return ids;
                } catch (Exception e) {
                    geofenceFailureCounter.increment();
                    span.recordException(e);
                    span.setAttribute("error", true);
                    throw e;
                }
            });
            
            if (!geofenceIds.isEmpty()) {
                position.setGeofenceIds(geofenceIds);
            }
            
            // Process asynchronously if needed
            if (config.getBoolean("geofence.async", false)) {
                CompletableFuture.runAsync(() -> {
                    // Additional async processing can be done here
                    // This would typically involve sending to a message broker
                });
            }
            
            callback.processed(false);
        } catch (Exception e) {
            // Fallback in case of circuit breaker open or other failures
            span.recordException(e);
            span.setAttribute("error", true);
            span.setAttribute("fallback", true);
            
            // Graceful degradation - use empty geofence list
            position.setGeofenceIds(Collections.emptyList());
            callback.processed(false);
        } finally {
            span.end();
        }
    }

    /**
     * Fallback method for geofence lookup when the circuit breaker is open
     * or when an exception occurs during the lookup process.
     * 
     * @param position The position to check
     * @return An empty list of geofence IDs
     */
    private List<Long> fallbackGeofenceLookup(Position position) {
        return Collections.emptyList();
    }
}