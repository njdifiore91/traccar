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
package org.traccar.geocoder;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.database.StatisticsManager;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Circuit breaker implementation for geocoding services.
 * Wraps a geocoder implementation with Resilience4j circuit breaker to provide fault tolerance.
 */
public class GeocoderCircuitBreaker implements Geocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeocoderCircuitBreaker.class);

    private final Geocoder geocoder;
    private final CircuitBreaker circuitBreaker;
    private final String fallbackAddress;
    private final ScheduledExecutorService scheduler;

    /**
     * Constructs a new GeocoderCircuitBreaker with default configuration.
     *
     * @param geocoder The geocoder implementation to wrap
     */
    public GeocoderCircuitBreaker(Geocoder geocoder) {
        this(geocoder, null, null);
    }

    /**
     * Constructs a new GeocoderCircuitBreaker with custom configuration.
     *
     * @param geocoder The geocoder implementation to wrap
     * @param config   Custom circuit breaker configuration (null for defaults)
     * @param fallbackAddress Fallback address to use when circuit is open (null for empty string)
     */
    public GeocoderCircuitBreaker(Geocoder geocoder, CircuitBreakerConfig config, String fallbackAddress) {
        this.geocoder = geocoder;
        this.fallbackAddress = fallbackAddress != null ? fallbackAddress : "";
        this.scheduler = Executors.newSingleThreadScheduledExecutor();

        // Create circuit breaker with default or custom configuration
        CircuitBreakerConfig circuitBreakerConfig = config != null ? config : CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10) // Consider the last 10 calls
                .minimumNumberOfCalls(5) // Minimum calls before calculating failure rate
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 calls in half-open state
                .automaticTransitionFromOpenToHalfOpenEnabled(true) // Auto transition to half-open
                .recordExceptions(GeocoderException.class, Exception.class) // Record these exceptions as failures
                .build();

        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = registry.circuitBreaker("geocoder");

        // Add state transition listener for logging
        this.circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.warn("Geocoder circuit breaker state changed from {} to {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));

        // Schedule periodic health checks
        this.scheduler.scheduleAtFixedRate(this::checkHealth, 1, 1, TimeUnit.MINUTES);
    }

    /**
     * Performs a health check on the wrapped geocoder.
     * Updates circuit breaker metrics based on the result.
     */
    private void checkHealth() {
        try {
            if (geocoder.isHealthy()) {
                // Record a success to help close the circuit if it's open
                circuitBreaker.onSuccess(0);
            } else {
                // Record a failure to help open the circuit if it's closed
                circuitBreaker.onError(0, new GeocoderException("Geocoder health check failed"));
            }
        } catch (Exception e) {
            LOGGER.warn("Error during geocoder health check", e);
            circuitBreaker.onError(0, e);
        }
    }

    @Override
    public String getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        // If callback is provided, use asynchronous approach
        if (callback != null) {
            CompletableFuture.supplyAsync(() -> {
                try {
                    return circuitBreaker.executeSupplier(() -> 
                        geocoder.getAddress(latitude, longitude, null));
                } catch (Exception e) {
                    LOGGER.warn("Circuit breaker prevented geocoder call or geocoder failed", e);
                    return fallbackAddress;
                }
            }).thenAccept(address -> {
                if (address != null) {
                    callback.onSuccess(address);
                } else {
                    callback.onSuccess(fallbackAddress);
                }
            }).exceptionally(e -> {
                callback.onFailure(e);
                return null;
            });
            return null;
        } else {
            // Synchronous approach
            try {
                return circuitBreaker.executeSupplier(() -> 
                    geocoder.getAddress(latitude, longitude, null));
            } catch (Exception e) {
                LOGGER.warn("Circuit breaker prevented geocoder call or geocoder failed", e);
                return fallbackAddress;
            }
        }
    }

    @Override
    public void setStatisticsManager(StatisticsManager statisticsManager) {
        geocoder.setStatisticsManager(statisticsManager);
    }

    @Override
    public void registerWithServiceDiscovery(String serviceId, Map<String, String> metadata) {
        // Add circuit breaker information to metadata
        Map<String, String> enhancedMetadata = new HashMap<>(metadata);
        enhancedMetadata.put("circuitBreaker", "true");
        enhancedMetadata.put("circuitBreakerState", circuitBreaker.getState().name());
        geocoder.registerWithServiceDiscovery(serviceId, enhancedMetadata);
    }

    @Override
    public void deregisterFromServiceDiscovery(String serviceId) {
        geocoder.deregisterFromServiceDiscovery(serviceId);
    }

    @Override
    public Map<String, Double> getMetrics() {
        Map<String, Double> metrics = new HashMap<>(geocoder.getMetrics());
        
        // Add circuit breaker metrics
        metrics.put("circuitbreaker.failure.rate", circuitBreaker.getMetrics().getFailureRate());
        metrics.put("circuitbreaker.slow.call.rate", circuitBreaker.getMetrics().getSlowCallRate());
        metrics.put("circuitbreaker.number.of.failed.calls", (double) circuitBreaker.getMetrics().getNumberOfFailedCalls());
        metrics.put("circuitbreaker.number.of.slow.calls", (double) circuitBreaker.getMetrics().getNumberOfSlowCalls());
        metrics.put("circuitbreaker.number.of.successful.calls", (double) circuitBreaker.getMetrics().getNumberOfSuccessfulCalls());
        metrics.put("circuitbreaker.number.of.not.permitted.calls", (double) circuitBreaker.getMetrics().getNumberOfNotPermittedCalls());
        
        return metrics;
    }

    @Override
    public boolean isHealthy() {
        // Consider healthy if circuit is not open
        return circuitBreaker.getState() != CircuitBreaker.State.OPEN && geocoder.isHealthy();
    }

    @Override
    public Map<String, Object> getHealthDetails() {
        Map<String, Object> details = new HashMap<>(geocoder.getHealthDetails());
        
        // Add circuit breaker health details
        details.put("circuitBreakerState", circuitBreaker.getState().name());
        details.put("failureRate", circuitBreaker.getMetrics().getFailureRate());
        details.put("slowCallRate", circuitBreaker.getMetrics().getSlowCallRate());
        
        // Update overall status based on circuit breaker state
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            details.put("status", "DOWN");
            details.put("reason", "Circuit breaker is open");
        } else if (circuitBreaker.getState() == CircuitBreaker.State.HALF_OPEN) {
            details.put("status", "DEGRADED");
            details.put("reason", "Circuit breaker is half-open");
        }
        
        return details;
    }

    @Override
    public void setTraceContext(String traceId, String spanId, boolean sampled) {
        geocoder.setTraceContext(traceId, spanId, sampled);
    }

    @Override
    public void configureFromEnvironment(String environmentPrefix) {
        geocoder.configureFromEnvironment(environmentPrefix);
    }

    @Override
    public void initializeForContainer(Map<String, String> containerConfig) {
        geocoder.initializeForContainer(containerConfig);
    }

    /**
     * Gets the current state of the circuit breaker.
     *
     * @return The current circuit breaker state
     */
    public CircuitBreaker.State getCircuitBreakerState() {
        return circuitBreaker.getState();
    }

    /**
     * Gets the underlying circuit breaker instance for advanced configuration.
     *
     * @return The circuit breaker instance
     */
    public CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    /**
     * Resets the circuit breaker to its initial closed state.
     * This will clear all metrics and return the circuit breaker to its closed state.
     */
    public void resetCircuitBreaker() {
        circuitBreaker.reset();
    }

    /**
     * Closes and cleans up resources used by this circuit breaker.
     * Should be called when the geocoder is no longer needed.
     */
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
    }
}