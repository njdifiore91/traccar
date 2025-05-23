/*
 * Copyright 2012 - 2023 Anton Tananaev (anton@traccar.org)
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
import io.github.resilience4j.retry.Retry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonString;
import jakarta.ws.rs.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Google reverse geocoder implementation with circuit breaker, retry, and metrics support.
 * This implementation is designed for the microservices architecture with resilience patterns
 * and distributed caching.
 */
public class GoogleGeocoder extends JsonGeocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleGeocoder.class);
    
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final MeterRegistry meterRegistry;
    private final Timer requestTimer;
    private final String providerName = "google";

    private static String formatUrl(String url, String key, String language) {
        if (url == null) {
            url = "https://maps.googleapis.com/maps/api/geocode/json";
        }
        url += "?latlng=%f,%f&key=" + key;
        if (language != null) {
            url += "&language=" + language;
        }
        return url;
    }

    /**
     * Creates a new GoogleGeocoder with resilience patterns and metrics.
     *
     * @param client JAX-RS client for HTTP requests
     * @param url Base URL for the Google API (optional, defaults to official endpoint)
     * @param key Google API key
     * @param language Language for the response (optional)
     * @param cacheSize Size of the local cache
     * @param resilienceConfig Configuration for circuit breaker and retry patterns
     * @param meterRegistry Registry for metrics collection
     */
    public GoogleGeocoder(Client client, String url, String key, String language, int cacheSize,
                          ResilienceConfig resilienceConfig, MeterRegistry meterRegistry) {
        super(client, formatUrl(url, key, language), cacheSize, new AddressFormat());
        
        // Initialize circuit breaker with provider-specific configuration
        this.circuitBreaker = resilienceConfig.createCircuitBreaker(providerName);
        
        // Initialize retry with provider-specific configuration
        this.retry = resilienceConfig.createRetry(providerName);
        
        // Initialize metrics
        this.meterRegistry = meterRegistry;
        this.requestTimer = Timer.builder("geocoder.request")
                .tag("provider", providerName)
                .description("Time taken for geocoding requests")
                .register(meterRegistry);
        
        // Register circuit breaker state metrics
        CircuitBreaker.EventPublisher eventPublisher = circuitBreaker.getEventPublisher();
        eventPublisher.onStateTransition(event -> {
            LOGGER.info("Google geocoder circuit breaker state changed from {} to {}", 
                    event.getStateTransition().getFromState(),
                    event.getStateTransition().getToState());
            meterRegistry.counter("geocoder.circuitbreaker.state", 
                    "provider", providerName, 
                    "state", event.getStateTransition().getToState().name())
                    .increment();
        });
        
        // Register retry metrics
        Retry.EventPublisher retryEventPublisher = retry.getEventPublisher();
        retryEventPublisher.onRetry(event -> 
                meterRegistry.counter("geocoder.retry.count", 
                        "provider", providerName)
                        .increment());
    }

    /**
     * Overrides the getAddress method to apply circuit breaker and retry patterns.
     */
    @Override
    public Address getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        // Create a resilient supplier that will be executed with circuit breaker and retry
        Supplier<Address> addressSupplier = () -> {
            Timer.Sample sample = Timer.start(meterRegistry);
            try {
                Address result = super.getAddress(latitude, longitude, null);
                sample.stop(requestTimer);
                return result;
            } catch (Exception e) {
                sample.stop(meterRegistry.counter("geocoder.error", 
                        "provider", providerName, 
                        "type", e.getClass().getSimpleName()));
                LOGGER.warn("Error getting address from Google: {}", e.getMessage());
                throw e;
            }
        };

        // Apply circuit breaker and retry patterns
        Supplier<Address> resilientSupplier = Retry.decorateSupplier(retry, 
                CircuitBreaker.decorateSupplier(circuitBreaker, addressSupplier));

        if (callback != null) {
            // Asynchronous execution with callback
            CompletableFuture.supplyAsync(resilientSupplier)
                    .thenAccept(address -> {
                        if (address != null) {
                            callback.onSuccess(address);
                        } else {
                            callback.onFailure(new GeocoderException("No address found"));
                        }
                    })
                    .exceptionally(e -> {
                        LOGGER.warn("Failed to get address from Google", e);
                        callback.onFailure(new GeocoderException(e));
                        return null;
                    });
            return null;
        } else {
            // Synchronous execution
            try {
                return resilientSupplier.get();
            } catch (Exception e) {
                LOGGER.warn("Failed to get address from Google", e);
                return getFallbackAddress(latitude, longitude);
            }
        }
    }

    /**
     * Provides a fallback address when the Google service is unavailable.
     * This is part of the graceful degradation strategy.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @return A basic address with coordinates or null based on configuration
     */
    private Address getFallbackAddress(double latitude, double longitude) {
        // In a real implementation, this could check a local database or another source
        // For now, we'll just return a basic address with the coordinates
        Address address = new Address();
        address.setFormattedAddress(String.format("%.6f, %.6f", latitude, longitude));
        meterRegistry.counter("geocoder.fallback", "provider", providerName).increment();
        return address;
    }

    @Override
    public Address parseAddress(JsonObject json) {
        JsonArray results = json.getJsonArray("results");
        if (results != null && !results.isEmpty()) {
            JsonObject result = results.getJsonObject(0);
            Address address = new Address();

            JsonArray components = result.getJsonArray("address_components");
            if (components != null) {
                for (JsonObject component : components.getValuesAs(JsonObject.class)) {
                    String value = component.getString("long_name");
                    JsonArray types = component.getJsonArray("types");
                    if (types != null) {
                        for (JsonString type : types.getValuesAs(JsonString.class)) {
                            switch (type.getString()) {
                                case "street_number":
                                    address.setHouse(value);
                                    break;
                                case "route":
                                    address.setStreet(value);
                                    break;
                                case "locality":
                                    address.setSettlement(value);
                                    break;
                                case "administrative_area_level_2":
                                    address.setDistrict(value);
                                    break;
                                case "administrative_area_level_1":
                                    address.setState(value);
                                    break;
                                case "country":
                                    address.setCountry(value.toUpperCase());
                                    break;
                                case "postal_code":
                                    address.setPostcode(value);
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                }
            }

            // Get formatted address if components didn't provide enough information
            if (address.getFormattedAddress() == null && result.containsKey("formatted_address")) {
                address.setFormattedAddress(result.getString("formatted_address"));
            }

            return address;
        }

        return null;
    }

    @Override
    protected String parseError(JsonObject json) {
        if (json.containsKey("error_message")) {
            return json.getString("error_message");
        }
        if (json.containsKey("status")) {
            return json.getString("status");
        }
        return null;
    }

    /**
     * Reports the current health status of the geocoder.
     * Used by the health check system for service discovery and monitoring.
     *
     * @return true if the circuit breaker is closed (service is healthy), false otherwise
     */
    public boolean isHealthy() {
        return circuitBreaker.getState() == CircuitBreaker.State.CLOSED;
    }
}