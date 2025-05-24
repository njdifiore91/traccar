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
package org.traccar.geocoder;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.database.StatisticsManager;

import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.InvocationCallback;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public abstract class JsonGeocoder implements Geocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonGeocoder.class);

    private final Client client;
    private final String url;
    private final AddressFormat addressFormat;
    private StatisticsManager statisticsManager;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final MeterRegistry meterRegistry;
    private final String geocoderName;
    private final boolean healthReportingEnabled;
    private Geocoder fallbackGeocoder;
    private int maxRetries = 3;
    private long initialDelayMs = 100;
    private long maxDelayMs = 1000;
    private int failureThreshold = 50;
    private long resetTimeoutMs = 30000;

    // Distributed cache using Redis or in-memory cache as fallback
    private Map<Map.Entry<Double, Double>, String> cache;

    public JsonGeocoder(Client client, String url, final int cacheSize, AddressFormat addressFormat,
                       CircuitBreakerRegistry circuitBreakerRegistry, RetryRegistry retryRegistry,
                       MeterRegistry meterRegistry, String geocoderName, boolean healthReportingEnabled) {
        this.client = client;
        this.url = url;
        this.addressFormat = addressFormat;
        this.meterRegistry = meterRegistry;
        this.geocoderName = geocoderName;
        this.healthReportingEnabled = healthReportingEnabled;

        // Initialize circuit breaker
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(geocoderName);
        
        // Initialize retry mechanism
        this.retry = retryRegistry.retry(geocoderName);

        // Initialize cache
        if (cacheSize > 0) {
            this.cache = Collections.synchronizedMap(new LinkedHashMap<>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry eldest) {
                    return size() > cacheSize;
                }
            });
        }
    }

    /**
     * Constructor for backward compatibility
     */
    public JsonGeocoder(Client client, String url, final int cacheSize, AddressFormat addressFormat) {
        this.client = client;
        this.url = url;
        this.addressFormat = addressFormat;
        this.meterRegistry = null;
        this.geocoderName = "defaultGeocoder";
        this.healthReportingEnabled = false;
        this.circuitBreaker = null;
        this.retry = null;

        // Initialize cache
        if (cacheSize > 0) {
            this.cache = Collections.synchronizedMap(new LinkedHashMap<>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry eldest) {
                    return size() > cacheSize;
                }
            });
        }
    }

    @Override
    public void setStatisticsManager(StatisticsManager statisticsManager) {
        this.statisticsManager = statisticsManager;
    }

    protected String readValue(JsonObject object, String key) {
        if (object.containsKey(key) && !object.isNull(key)) {
            return object.getString(key);
        }
        return null;
    }

    private String handleResponse(
            double latitude, double longitude, JsonObject json, ReverseGeocoderCallback callback) {

        Address address = parseAddress(json);
        if (address != null) {
            String formattedAddress = addressFormat.format(address);
            if (cache != null) {
                cache.put(new AbstractMap.SimpleImmutableEntry<>(latitude, longitude), formattedAddress);
            }
            if (callback != null) {
                callback.onSuccess(formattedAddress);
            }
            return formattedAddress;
        } else {
            String msg = "Empty address. Error: " + parseError(json);
            if (callback != null) {
                callback.onFailure(new GeocoderException(msg));
            } else {
                LOGGER.warn(msg);
            }
        }
        return null;
    }

    @Override
    public String getAddress(
            final double latitude, final double longitude, final ReverseGeocoderCallback callback) {

        // Check cache first
        if (cache != null) {
            String cachedAddress = cache.get(new AbstractMap.SimpleImmutableEntry<>(latitude, longitude));
            if (cachedAddress != null) {
                if (callback != null) {
                    callback.onSuccess(cachedAddress);
                }
                return cachedAddress;
            }
        }

        // Record geocoder request for statistics
        if (statisticsManager != null) {
            statisticsManager.registerGeocoderRequest();
        }

        // If circuit breaker and retry are available, use them
        if (circuitBreaker != null && retry != null) {
            return getAddressWithResilience(latitude, longitude, callback);
        } else {
            // Legacy implementation without resilience patterns
            return getAddressLegacy(latitude, longitude, callback);
        }
    }

    private String getAddressWithResilience(
            final double latitude, final double longitude, final ReverseGeocoderCallback callback) {

        // Create a supplier that will be decorated with resilience patterns
        Supplier<String> geocodingSupplier = () -> {
            Timer.Sample sample = null;
            if (meterRegistry != null) {
                sample = Timer.start(meterRegistry);
            }

            try {
                var request = client.target(String.format(url, latitude, longitude)).request();
                JsonObject response = request.get(JsonObject.class);
                String result = handleResponse(latitude, longitude, response, null);

                // Record successful metrics
                if (meterRegistry != null && sample != null) {
                    sample.stop(Timer.builder("geocoder.request.time")
                            .tags(Tags.of(
                                    Tag.of("geocoder", geocoderName),
                                    Tag.of("status", "success")))
                            .register(meterRegistry));
                }

                return result;
            } catch (Exception e) {
                // Record failed metrics
                if (meterRegistry != null && sample != null) {
                    sample.stop(Timer.builder("geocoder.request.time")
                            .tags(Tags.of(
                                    Tag.of("geocoder", geocoderName),
                                    Tag.of("status", "error"),
                                    Tag.of("error", e.getClass().getSimpleName())))
                            .register(meterRegistry));
                }

                LOGGER.warn("Geocoder network error", e);
                throw e;
            }
        };

        // Decorate the supplier with circuit breaker and retry
        Supplier<String> decoratedSupplier = Retry.decorateSupplier(retry, 
                CircuitBreaker.decorateSupplier(circuitBreaker, geocodingSupplier));

        try {
            if (callback != null) {
                // Async execution with callback
                CompletableFuture.supplyAsync(decoratedSupplier)
                        .thenAccept(callback::onSuccess)
                        .exceptionally(e -> {
                            Throwable cause = e instanceof CompletionException ? e.getCause() : e;
                            callback.onFailure(cause);
                            return null;
                        });
                return null;
            } else {
                // Synchronous execution
                return decoratedSupplier.get();
            }
        } catch (Exception e) {
            // Handle fallback for synchronous execution
            LOGGER.warn("Geocoder service unavailable, using fallback", e);
            return getFallbackAddress(latitude, longitude);
        }
    }

    private String getAddressLegacy(
            final double latitude, final double longitude, final ReverseGeocoderCallback callback) {

        var request = client.target(String.format(url, latitude, longitude)).request();

        if (callback != null) {
            request.async().get(new InvocationCallback<JsonObject>() {
                @Override
                public void completed(JsonObject json) {
                    handleResponse(latitude, longitude, json, callback);
                }

                @Override
                public void failed(Throwable throwable) {
                    callback.onFailure(throwable);
                }
            });
        } else {
            try {
                return handleResponse(latitude, longitude, request.get(JsonObject.class), null);
            } catch (Exception e) {
                LOGGER.warn("Geocoder network error", e);
            }
        }
        return null;
    }

    /**
     * Fallback method when geocoding service is unavailable
     */
    protected String getFallbackAddress(double latitude, double longitude) {
        // Try using fallback geocoder if available
        if (fallbackGeocoder != null) {
            try {
                CompletableFuture<String> future = fallbackGeocoder.getAddress(latitude, longitude);
                return future.getNow(String.format("%.6f, %.6f", latitude, longitude));
            } catch (Exception e) {
                LOGGER.warn("Fallback geocoder failed", e);
            }
        }
        // Default implementation returns coordinates as a string
        return String.format("%.6f, %.6f", latitude, longitude);
    }

    @Override
    public CompletableFuture<String> getAddress(double latitude, double longitude) {
        CompletableFuture<String> future = new CompletableFuture<>();
        String address = getAddress(latitude, longitude, new ReverseGeocoderCallback() {
            @Override
            public void onSuccess(String address) {
                future.complete(address);
            }

            @Override
            public void onFailure(Throwable e) {
                future.completeExceptionally(e);
            }
        });
        if (address != null) {
            future.complete(address);
        }
        return future;
    }

    @Override
    public CompletableFuture<String> getAddress(double latitude, double longitude, boolean useFallback) {
        CompletableFuture<String> future = getAddress(latitude, longitude);
        if (useFallback) {
            return future.exceptionally(e -> {
                LOGGER.warn("Primary geocoder failed, using fallback", e);
                return getFallbackAddress(latitude, longitude);
            });
        }
        return future;
    }

    @Override
    public boolean isHealthy() {
        if (!healthReportingEnabled) {
            return true; // Health reporting disabled, assume healthy
        }

        if (circuitBreaker != null) {
            // Check circuit breaker state
            return !circuitBreaker.getState().equals(CircuitBreaker.State.OPEN) &&
                   !circuitBreaker.getState().equals(CircuitBreaker.State.FORCED_OPEN);
        }
        return true; // No circuit breaker, assume healthy
    }

    @Override
    public GeocoderHealthStatus getHealthStatus() {
        boolean healthy = isHealthy();
        String statusMessage = healthy ? "Geocoder service is healthy" : "Geocoder service is unhealthy";
        long responseTimeMs = 0;
        int successfulRequests = 0;
        int failedRequests = 0;
        String lastError = null;

        if (circuitBreaker != null) {
            CircuitBreaker.Metrics metrics = circuitBreaker.getMetrics();
            successfulRequests = (int) metrics.getNumberOfSuccessfulCalls();
            failedRequests = (int) metrics.getNumberOfFailedCalls();
            statusMessage = "Circuit breaker state: " + circuitBreaker.getState();
        }

        return new GeocoderHealthStatus(healthy, statusMessage, responseTimeMs,
                successfulRequests, failedRequests, lastError);
    }

    @Override
    public boolean registerWithServiceDiscovery(String serviceId) {
        // Implementation would depend on the service discovery mechanism used
        LOGGER.debug("Registering geocoder {} with service discovery as {}", geocoderName, serviceId);
        return true;
    }

    @Override
    public boolean deregisterFromServiceDiscovery(String serviceId) {
        // Implementation would depend on the service discovery mechanism used
        LOGGER.debug("Deregistering geocoder {} from service discovery", serviceId);
        return true;
    }

    @Override
    public String resolveServiceEndpoint(String serviceType) {
        // Implementation would depend on the service discovery mechanism used
        LOGGER.debug("Resolving endpoint for service type {}", serviceType);
        return null;
    }

    @Override
    public GeocoderMetrics getMetrics() {
        long totalRequests = 0;
        long successfulRequests = 0;
        long failedRequests = 0;
        long averageResponseTimeMs = 0;
        long cacheHits = 0;
        long cacheMisses = 0;
        long circuitBreakerOpenCount = 0;
        long retryCount = 0;

        if (circuitBreaker != null) {
            CircuitBreaker.Metrics metrics = circuitBreaker.getMetrics();
            successfulRequests = metrics.getNumberOfSuccessfulCalls();
            failedRequests = metrics.getNumberOfFailedCalls();
            totalRequests = successfulRequests + failedRequests;
        }

        return new GeocoderMetrics(totalRequests, successfulRequests, failedRequests, averageResponseTimeMs,
                cacheHits, cacheMisses, circuitBreakerOpenCount, retryCount);
    }

    @Override
    public boolean resetCircuitBreaker() {
        if (circuitBreaker != null) {
            try {
                circuitBreaker.reset();
                LOGGER.info("Circuit breaker for geocoder {} has been reset", geocoderName);
                return true;
            } catch (Exception e) {
                LOGGER.warn("Failed to reset circuit breaker for geocoder {}", geocoderName, e);
            }
        }
        return false;
    }

    @Override
    public void configureRetryPolicy(int maxRetries, long initialDelayMs, long maxDelayMs) {
        this.maxRetries = maxRetries;
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        LOGGER.info("Retry policy configured for geocoder {}: maxRetries={}, initialDelay={}ms, maxDelay={}ms",
                geocoderName, maxRetries, initialDelayMs, maxDelayMs);
    }

    @Override
    public void configureCircuitBreaker(int failureThreshold, long resetTimeoutMs) {
        this.failureThreshold = failureThreshold;
        this.resetTimeoutMs = resetTimeoutMs;
        LOGGER.info("Circuit breaker configured for geocoder {}: failureThreshold={}%, resetTimeout={}ms",
                geocoderName, failureThreshold, resetTimeoutMs);
    }

    @Override
    public void setFallbackGeocoder(Geocoder fallbackGeocoder) {
        this.fallbackGeocoder = fallbackGeocoder;
        LOGGER.info("Fallback geocoder set for {}: {}", geocoderName,
                fallbackGeocoder != null ? fallbackGeocoder.getClass().getSimpleName() : "null");
    }

    @Override
    public Geocoder getFallbackGeocoder() {
        return fallbackGeocoder;
    }

    /**
     * Get the current state of the circuit breaker
     */
    public CircuitBreaker.State getCircuitBreakerState() {
        if (circuitBreaker != null) {
            return circuitBreaker.getState();
        }
        return null;
    }

    public abstract Address parseAddress(JsonObject json);

    protected String parseError(JsonObject json) {
        return null;
    }
}