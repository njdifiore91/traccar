/*
 * Copyright 2015 - 2023 Anton Tananaev (anton@traccar.org)
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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonString;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.InvocationCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Mapbox geocoder implementation with circuit breaker, retry, and distributed cache support.
 * This implementation is designed for the microservices architecture with resilience patterns.
 */
@Singleton
public class MapboxGeocoder extends JsonGeocoder implements Geocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapboxGeocoder.class);
    
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final MeterRegistry meterRegistry;
    private final Counter requestCounter;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Counter fallbackCounter;
    
    private volatile boolean healthy = true;

    /**
     * Creates a new MapboxGeocoder with the specified parameters and resilience configuration.
     *
     * @param client JAX-RS client for HTTP requests
     * @param url URL template with placeholders for coordinates
     * @param key Mapbox API key
     * @param cacheSize Size of the local cache (0 to disable)
     * @param addressFormat Format for address display
     * @param resilienceConfig Resilience configuration for circuit breaker and retry
     * @param meterRegistry Registry for metrics collection
     */
    @Inject
    public MapboxGeocoder(
            Client client, String url, String key, int cacheSize,
            AddressFormat addressFormat, ResilienceConfig resilienceConfig,
            MeterRegistry meterRegistry) {
        super(client, url + "?access_token=" + key, cacheSize, addressFormat);
        
        this.meterRegistry = meterRegistry;
        this.circuitBreaker = resilienceConfig.createCircuitBreaker("mapbox");
        this.retry = resilienceConfig.createRetry("mapbox");
        
        // Register metrics for monitoring
        this.requestCounter = Counter.builder("geocoder.requests")
                .tag("provider", "mapbox")
                .description("Number of geocoding requests")
                .register(meterRegistry);
        
        this.successCounter = Counter.builder("geocoder.success")
                .tag("provider", "mapbox")
                .description("Number of successful geocoding requests")
                .register(meterRegistry);
        
        this.failureCounter = Counter.builder("geocoder.failures")
                .tag("provider", "mapbox")
                .description("Number of failed geocoding requests")
                .register(meterRegistry);
        
        this.fallbackCounter = Counter.builder("geocoder.fallbacks")
                .tag("provider", "mapbox")
                .description("Number of fallback responses used")
                .register(meterRegistry);
        
        // Register circuit breaker state gauge
        meterRegistry.gauge("geocoder.circuit.state", 
                circuitBreaker, 
                cb -> cb.getState().getOrder());
        
        // Add circuit breaker event listeners for health monitoring
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    if (event.getStateTransition() == CircuitBreaker.StateTransition.CLOSED_TO_OPEN) {
                        LOGGER.warn("Mapbox geocoder circuit breaker opened");
                        healthy = false;
                    } else if (event.getStateTransition() == CircuitBreaker.StateTransition.OPEN_TO_CLOSED
                            || event.getStateTransition() == CircuitBreaker.StateTransition.HALF_OPEN_TO_CLOSED) {
                        LOGGER.info("Mapbox geocoder circuit breaker closed");
                        healthy = true;
                    }
                });
    }

    /**
     * Gets a human-readable address for the specified coordinates with resilience patterns.
     * Uses circuit breaker and retry mechanisms for fault tolerance.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param callback Optional callback for asynchronous execution
     * @return Address object if executed synchronously, null if using callback
     */
    @Override
    public Address getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        requestCounter.increment();
        
        // If using callback (asynchronous mode)
        if (callback != null) {
            // Create a decorated supplier with circuit breaker and retry
            Supplier<CompletableFuture<Address>> decoratedSupplier = CircuitBreaker.decorateSupplier(
                    circuitBreaker,
                    () -> {
                        CompletableFuture<Address> future = new CompletableFuture<>();
                        
                        // Call the parent implementation with a custom callback
                        super.getAddress(latitude, longitude, new ReverseGeocoderCallback() {
                            @Override
                            public void onSuccess(Address address) {
                                successCounter.increment();
                                future.complete(address);
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                failureCounter.increment();
                                future.completeExceptionally(e);
                            }
                        });
                        
                        return future;
                    });
            
            // Apply retry to the decorated supplier
            Supplier<CompletableFuture<Address>> retryingSupplier = Retry.decorateSupplier(
                    retry, decoratedSupplier);
            
            try {
                // Execute the decorated supplier and handle the result
                retryingSupplier.get()
                        .thenAccept(address -> callback.onSuccess(address))
                        .exceptionally(e -> {
                            // Provide fallback when all retries fail
                            fallbackCounter.increment();
                            Address fallbackAddress = createFallbackAddress(latitude, longitude);
                            callback.onSuccess(fallbackAddress);
                            return null;
                        });
                
                return null;
            } catch (Exception e) {
                // Handle any unexpected exceptions
                LOGGER.error("Error executing geocoding request", e);
                fallbackCounter.increment();
                Address fallbackAddress = createFallbackAddress(latitude, longitude);
                callback.onSuccess(fallbackAddress);
                return null;
            }
        } else {
            // Synchronous execution
            try {
                // Create a decorated supplier with circuit breaker and retry
                Supplier<Address> decoratedSupplier = CircuitBreaker.decorateSupplier(
                        circuitBreaker,
                        () -> super.getAddress(latitude, longitude, null));
                
                // Apply retry to the decorated supplier
                Supplier<Address> retryingSupplier = Retry.decorateSupplier(
                        retry, decoratedSupplier);
                
                // Execute the decorated supplier
                Address address = retryingSupplier.get();
                if (address != null) {
                    successCounter.increment();
                }
                return address;
            } catch (Exception e) {
                // Handle any exceptions
                LOGGER.error("Error executing geocoding request", e);
                failureCounter.increment();
                fallbackCounter.increment();
                return createFallbackAddress(latitude, longitude);
            }
        }
    }

    /**
     * Creates a fallback address when the geocoding service is unavailable.
     * This provides graceful degradation instead of failing completely.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @return Basic address with coordinates
     */
    private Address createFallbackAddress(double latitude, double longitude) {
        Address address = new Address();
        address.setLatitude(latitude);
        address.setLongitude(longitude);
        address.setFormattedAddress(String.format("%.6f, %.6f", latitude, longitude));
        return address;
    }

    /**
     * Parses the JSON response from Mapbox into an Address object.
     *
     * @param json JSON response from the service
     * @return Address object or null if parsing failed
     */
    @Override
    public Address parseAddress(JsonObject json) {
        JsonArray features = json.getJsonArray("features");
        if (features != null && !features.isEmpty()) {
            Address address = new Address();
            
            JsonObject feature = features.getJsonObject(0);
            JsonObject context = feature.getJsonObject("context");
            JsonObject properties = feature.getJsonObject("properties");
            
            if (properties != null) {
                String formattedAddress = properties.getString("place_name", null);
                if (formattedAddress != null) {
                    address.setFormattedAddress(formattedAddress);
                }
            }
            
            if (context != null) {
                JsonArray addressComponents = context.getJsonArray("address_components");
                if (addressComponents != null) {
                    for (int i = 0; i < addressComponents.size(); i++) {
                        JsonObject component = addressComponents.getJsonObject(i);
                        String type = component.getString("type", "");
                        String value = component.getString("text", "");
                        
                        switch (type) {
                            case "country":
                                address.setCountry(value);
                                break;
                            case "region":
                                address.setState(value);
                                break;
                            case "district":
                            case "place":
                                address.setDistrict(value);
                                break;
                            case "locality":
                            case "neighborhood":
                                address.setSettlement(value);
                                break;
                            case "address":
                                String houseNumber = component.getString("house_number", "");
                                String street = component.getString("street", "");
                                if (!houseNumber.isEmpty() && !street.isEmpty()) {
                                    address.setStreet(houseNumber + " " + street);
                                } else if (!street.isEmpty()) {
                                    address.setStreet(street);
                                }
                                break;
                            case "postcode":
                                address.setPostcode(value);
                                break;
                            default:
                                break;
                        }
                    }
                }
            }
            
            return address;
        }
        
        return null;
    }

    /**
     * Parses error information from the JSON response.
     *
     * @param json JSON response from the service
     * @return Error message or null if no error was found
     */
    @Override
    protected String parseError(JsonObject json) {
        if (json.containsKey("message")) {
            return json.getString("message");
        }
        return null;
    }
    
    /**
     * Reports the health status of the geocoder.
     * Used by the health check system for service discovery and monitoring.
     *
     * @return true if the geocoder is healthy and operational, false otherwise
     */
    public boolean isHealthy() {
        return healthy && circuitBreaker.getState() != CircuitBreaker.State.OPEN;
    }
}