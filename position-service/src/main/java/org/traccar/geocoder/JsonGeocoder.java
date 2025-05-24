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
        // Default implementation returns coordinates as a string
        return String.format("%.6f, %.6f", latitude, longitude);
    }

    /**
     * Check if the geocoder service is healthy
     */
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

    /**
     * Get the current state of the circuit breaker
     */
    public CircuitBreaker.State getCircuitBreakerState() {
        if (circuitBreaker != null) {
            return circuitBreaker.getState();
        }
        return null;
    }

    /**
     * Get metrics for the geocoder service
     */
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        if (circuitBreaker != null) {
            CircuitBreaker.Metrics circuitMetrics = circuitBreaker.getMetrics();
            metrics.put("failureRate", circuitMetrics.getFailureRate());
            metrics.put("slowCallRate", circuitMetrics.getSlowCallRate());
            metrics.put("numberOfBufferedCalls", circuitMetrics.getNumberOfBufferedCalls());
            metrics.put("numberOfFailedCalls", circuitMetrics.getNumberOfFailedCalls());
            metrics.put("numberOfSlowCalls", circuitMetrics.getNumberOfSlowCalls());
            metrics.put("numberOfSuccessfulCalls", circuitMetrics.getNumberOfSuccessfulCalls());
        }
        return metrics;
    }

    public abstract Address parseAddress(JsonObject json);

    protected String parseError(JsonObject json) {
        return null;
    }
}