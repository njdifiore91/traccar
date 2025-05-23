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
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.function.Supplier;

/**
 * Reverse geocoder using MapTiler API.
 * Implements circuit breaker pattern, fallback strategies, and distributed caching
 * for microservices architecture.
 */
@Singleton
public class MapTilerGeocoder extends JsonGeocoder implements GeocoderHealthIndicator.HealthCheck {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapTilerGeocoder.class);
    private static final String GEOCODER_NAME = "maptiler";

    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final MeterRegistry meterRegistry;
    private final Timer requestTimer;

    /**
     * Creates a new MapTilerGeocoder with the specified parameters.
     *
     * @param client JAX-RS client for HTTP requests
     * @param url URL template with placeholders for coordinates
     * @param key MapTiler API key
     * @param cacheSize Size of the local cache (0 to disable)
     * @param resilienceConfig Resilience configuration for circuit breaker and retry
     * @param meterRegistry Registry for metrics collection
     */
    @Inject
    public MapTilerGeocoder(
            Client client, String url, String key, int cacheSize,
            ResilienceConfig resilienceConfig, MeterRegistry meterRegistry) {
        super(client, url + "?key=" + key, cacheSize, new AddressFormat());
        this.circuitBreaker = resilienceConfig.createCircuitBreaker(GEOCODER_NAME);
        this.retry = resilienceConfig.createRetry(GEOCODER_NAME);
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.requestTimer = Timer.builder("geocoder.request")
                .tag("provider", GEOCODER_NAME)
                .description("Time taken for MapTiler geocoding requests")
                .register(meterRegistry);
        
        LOGGER.info("Initialized MapTiler geocoder with circuit breaker and retry");
    }

    /**
     * Parses the JSON response from MapTiler into an Address object.
     *
     * @param json JSON response from the service
     * @return Address object or null if parsing failed
     */
    @Override
    public Address parseAddress(JsonObject json) {
        // Wrap the parsing logic with circuit breaker and retry
        Supplier<Address> addressSupplier = () -> {
            try {
                JsonArray features = json.getJsonArray("features");
                if (features != null && !features.isEmpty()) {
                    JsonObject feature = features.getJsonObject(0);
                    JsonObject properties = feature.getJsonObject("properties");

                    Address address = new Address();

                    if (properties.containsKey("country")) {
                        address.setCountry(properties.getString("country"));
                    }
                    if (properties.containsKey("country_code")) {
                        address.setCountryCode(properties.getString("country_code"));
                    }
                    if (properties.containsKey("region")) {
                        address.setState(properties.getString("region"));
                    }
                    if (properties.containsKey("locality")) {
                        address.setSettlement(properties.getString("locality"));
                    }
                    if (properties.containsKey("district")) {
                        address.setDistrict(properties.getString("district"));
                    }
                    if (properties.containsKey("street")) {
                        address.setStreet(properties.getString("street"));
                    }
                    if (properties.containsKey("housenumber")) {
                        address.setHouse(properties.getString("housenumber"));
                    }
                    if (properties.containsKey("postcode")) {
                        address.setPostcode(properties.getString("postcode"));
                    }

                    return address;
                }
                return null;
            } catch (Exception e) {
                LOGGER.warn("MapTiler address parsing error", e);
                meterRegistry.counter("geocoder.error", "provider", GEOCODER_NAME, "type", "parsing").increment();
                return null;
            }
        };

        // Apply circuit breaker and retry patterns
        return Retry.decorateSupplier(retry, 
                CircuitBreaker.decorateSupplier(circuitBreaker, addressSupplier))
                .get();
    }

    /**
     * Parses error information from the JSON response.
     *
     * @param json JSON response from the service
     * @return Error message or null if no error was found
     */
    @Override
    protected String parseError(JsonObject json) {
        if (json.containsKey("error")) {
            return json.getString("error");
        }
        return null;
    }

    /**
     * Gets a human-readable address for the specified coordinates.
     * Overrides the parent method to add metrics and circuit breaker functionality.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param callback Optional callback for asynchronous execution
     * @return Address object if executed synchronously, null if using callback
     */
    @Override
    public Address getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            // Track request count
            meterRegistry.counter("geocoder.requests", "provider", GEOCODER_NAME).increment();
            
            // Check if circuit breaker is open
            if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
                LOGGER.warn("Circuit breaker is OPEN for MapTiler geocoder, using fallback");
                meterRegistry.counter("geocoder.circuitbreaker.open", "provider", GEOCODER_NAME).increment();
                return handleFallback(latitude, longitude, callback);
            }
            
            // Proceed with normal geocoding
            return super.getAddress(latitude, longitude, callback);
        } finally {
            sample.stop(requestTimer);
        }
    }

    /**
     * Fallback strategy when the circuit breaker is open or an error occurs.
     * Returns a minimal address with just the coordinates.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param callback Optional callback for asynchronous execution
     * @return Minimal address with coordinates
     */
    private Address handleFallback(double latitude, double longitude, ReverseGeocoderCallback callback) {
        // Create a minimal address with just the coordinates
        Address fallbackAddress = new Address();
        fallbackAddress.setFormattedAddress(String.format("%.6f, %.6f", latitude, longitude));
        
        // Track fallback usage
        meterRegistry.counter("geocoder.fallback", "provider", GEOCODER_NAME).increment();
        
        if (callback != null) {
            callback.onSuccess(fallbackAddress);
            return null;
        }
        return fallbackAddress;
    }

    /**
     * Reports the health status of the geocoder.
     * Used by the health check system for service discovery and monitoring.
     *
     * @return true if the geocoder is healthy and operational, false otherwise
     */
    @Override
    public boolean isHealthy() {
        return circuitBreaker.getState() != CircuitBreaker.State.OPEN;
    }

    /**
     * Gets the name of this geocoder provider.
     *
     * @return Provider name
     */
    @Override
    public String getName() {
        return GEOCODER_NAME;
    }

    /**
     * Gets detailed health information about this geocoder.
     *
     * @return Health information as a string
     */
    @Override
    public String getHealthDetails() {
        return String.format("State: %s, Failure Rate: %.2f%%, Calls: %d",
                circuitBreaker.getState(),
                circuitBreaker.getMetrics().getFailureRate(),
                circuitBreaker.getMetrics().getNumberOfCalls());
    }
}