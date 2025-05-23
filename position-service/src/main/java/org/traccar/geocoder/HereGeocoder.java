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
import io.micrometer.core.instrument.Timer;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonString;
import jakarta.ws.rs.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.database.StatisticsManager;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * HERE Maps geocoder implementation.
 * Supports both the legacy Geocoder API and the newer Geocoding and Search API v7.
 * Implements resilience patterns for microservices architecture.
 */
@Singleton
public class HereGeocoder extends JsonGeocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(HereGeocoder.class);
    
    private static final String LEGACY_URL = "https://reverse.geocoder.api.here.com/6.2/reversegeocode.json"
            + "?app_id=%s&app_code=%s&mode=retrieveAddresses"
            + "&prox=%f,%f,50&maxresults=1&gen=9&language=en-US";
    
    private static final String API_V7_URL = "https://revgeocode.search.hereapi.com/v1/revgeocode"
            + "?apiKey=%s&at=%f,%f&lang=en-US";

    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final MeterRegistry meterRegistry;
    private final Timer requestTimer;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Counter fallbackCounter;
    private final boolean useApiV7;
    private final String apiKey;
    private final String appId;
    private final String appCode;
    private boolean healthy = true;

    /**
     * Creates a new HERE geocoder with the specified parameters.
     *
     * @param client JAX-RS client for HTTP requests
     * @param useApiV7 Whether to use the newer Geocoding and Search API v7
     * @param apiKey API key for Geocoding and Search API v7
     * @param appId App ID for legacy Geocoder API
     * @param appCode App Code for legacy Geocoder API
     * @param cacheSize Size of the local cache (0 to disable)
     * @param resilienceConfig Resilience configuration for circuit breaker and retry
     * @param meterRegistry Registry for metrics collection
     */
    @Inject
    public HereGeocoder(
            Client client, boolean useApiV7, String apiKey, String appId, String appCode, int cacheSize,
            ResilienceConfig resilienceConfig, MeterRegistry meterRegistry) {
        super(client, useApiV7 
                ? String.format(API_V7_URL, apiKey)
                : String.format(LEGACY_URL, appId, appCode),
                cacheSize, new AddressFormat());
        
        this.useApiV7 = useApiV7;
        this.apiKey = apiKey;
        this.appId = appId;
        this.appCode = appCode;
        this.meterRegistry = meterRegistry;
        
        // Create circuit breaker and retry with provider-specific configuration
        this.circuitBreaker = resilienceConfig.createCircuitBreaker("here");
        this.retry = resilienceConfig.createRetry("here");
        
        // Register metrics for monitoring
        this.requestTimer = Timer.builder("geocoder.request.duration")
                .tag("provider", "here")
                .description("Time taken for geocoding requests")
                .register(meterRegistry);
        
        this.successCounter = Counter.builder("geocoder.request.success")
                .tag("provider", "here")
                .description("Number of successful geocoding requests")
                .register(meterRegistry);
        
        this.failureCounter = Counter.builder("geocoder.request.failure")
                .tag("provider", "here")
                .description("Number of failed geocoding requests")
                .register(meterRegistry);
        
        this.fallbackCounter = Counter.builder("geocoder.request.fallback")
                .tag("provider", "here")
                .description("Number of times fallback was used")
                .register(meterRegistry);
        
        // Register circuit breaker state gauge
        meterRegistry.gauge("geocoder.circuit.state", 
                circuitBreaker, cb -> cb.getState().getOrder());
    }

    /**
     * Gets a human-readable address for the specified coordinates.
     * Implements circuit breaker and retry patterns for resilience.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param callback Optional callback for asynchronous execution
     * @return Address object if executed synchronously, null if using callback
     */
    @Override
    public Address getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        // If callback is provided, use asynchronous execution
        if (callback != null) {
            CompletableFuture.supplyAsync(() -> {
                try {
                    // Apply circuit breaker and retry patterns
                    return Retry.decorateSupplier(retry, 
                            CircuitBreaker.decorateSupplier(circuitBreaker, 
                                    () -> getAddressWithFallback(latitude, longitude, null)))
                            .get();
                } catch (Exception e) {
                    failureCounter.increment();
                    LOGGER.warn("HERE geocoder request failed", e);
                    return null;
                }
            }).thenAccept(address -> {
                if (address != null) {
                    callback.onSuccess(address);
                } else {
                    callback.onFailure(new GeocoderException("HERE geocoder failed to return an address"));
                }
            });
            return null;
        } else {
            // Synchronous execution
            try {
                // Apply circuit breaker and retry patterns with timer
                return requestTimer.record(() -> 
                        Retry.decorateSupplier(retry, 
                                CircuitBreaker.decorateSupplier(circuitBreaker, 
                                        () -> getAddressWithFallback(latitude, longitude, null)))
                                .get());
            } catch (Exception e) {
                failureCounter.increment();
                LOGGER.warn("HERE geocoder request failed", e);
                return null;
            }
        }
    }

    /**
     * Gets an address with fallback strategy if the primary request fails.
     * This method is called within the circuit breaker and retry decorators.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param callback Optional callback for asynchronous execution
     * @return Address object or null if failed
     */
    private Address getAddressWithFallback(double latitude, double longitude, ReverseGeocoderCallback callback) {
        try {
            // Attempt to get address using the configured API
            Address address = super.getAddress(latitude, longitude, null);
            if (address != null) {
                successCounter.increment();
                healthy = true;
                return address;
            }
            throw new GeocoderException("No address returned");
        } catch (Exception e) {
            // If using API v7 and it fails, try falling back to legacy API
            if (useApiV7 && appId != null && appCode != null) {
                LOGGER.info("Falling back to legacy HERE geocoder API");
                fallbackCounter.increment();
                try {
                    String url = String.format(LEGACY_URL, appId, appCode, latitude, longitude);
                    JsonObject json = client.target(url).request().get(JsonObject.class);
                    Address address = parseLegacyResponse(json);
                    if (address != null) {
                        successCounter.increment();
                        healthy = true;
                        return address;
                    }
                } catch (Exception fallbackException) {
                    LOGGER.warn("HERE geocoder fallback failed", fallbackException);
                    healthy = false;
                }
            } else {
                healthy = false;
            }
            throw new GeocoderException(e);
        }
    }

    /**
     * Parses the JSON response from the HERE geocoder API v7.
     *
     * @param json JSON response from the service
     * @return Address object or null if parsing failed
     */
    @Override
    public Address parseAddress(JsonObject json) {
        if (useApiV7) {
            return parseApiV7Response(json);
        } else {
            return parseLegacyResponse(json);
        }
    }

    /**
     * Parses the JSON response from the HERE Geocoding and Search API v7.
     *
     * @param json JSON response from the service
     * @return Address object or null if parsing failed
     */
    private Address parseApiV7Response(JsonObject json) {
        JsonArray items = json.getJsonArray("items");
        if (items != null && !items.isEmpty()) {
            JsonObject item = items.getJsonObject(0);
            JsonObject address = item.getJsonObject("address");
            if (address != null) {
                Address result = new Address();
                
                result.setCountry(address.getString("countryName", null));
                result.setCountryCode(address.getString("countryCode", null));
                result.setState(address.getString("state", null));
                result.setCounty(address.getString("county", null));
                result.setDistrict(address.getString("district", null));
                result.setCity(address.getString("city", null));
                result.setStreet(address.getString("street", null));
                result.setHouse(address.getString("houseNumber", null));
                result.setPostcode(address.getString("postalCode", null));
                
                // Set formatted address if available
                if (address.containsKey("label")) {
                    result.setFormattedAddress(address.getString("label"));
                }
                
                return result;
            }
        }
        return null;
    }

    /**
     * Parses the JSON response from the legacy HERE Geocoder API.
     *
     * @param json JSON response from the service
     * @return Address object or null if parsing failed
     */
    private Address parseLegacyResponse(JsonObject json) {
        JsonObject response = json.getJsonObject("Response");
        if (response != null) {
            JsonArray results = response.getJsonArray("View");
            if (results != null && !results.isEmpty()) {
                JsonArray locations = results.getJsonObject(0).getJsonArray("Result");
                if (locations != null && !locations.isEmpty()) {
                    JsonObject location = locations.getJsonObject(0).getJsonObject("Location");
                    JsonObject addressDetails = location.getJsonObject("Address");
                    if (addressDetails != null) {
                        Address address = new Address();
                        
                        address.setCountry(addressDetails.getString("Country", null));
                        address.setCountryCode(addressDetails.getString("Country", null));
                        address.setState(addressDetails.getString("State", null));
                        address.setCounty(addressDetails.getString("County", null));
                        address.setDistrict(addressDetails.getString("District", null));
                        address.setCity(addressDetails.getString("City", null));
                        address.setStreet(addressDetails.getString("Street", null));
                        address.setHouse(addressDetails.getString("HouseNumber", null));
                        address.setPostcode(addressDetails.getString("PostalCode", null));
                        
                        // Set formatted address
                        if (addressDetails.containsKey("Label")) {
                            address.setFormattedAddress(addressDetails.getString("Label"));
                        }
                        
                        return address;
                    }
                }
            }
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
        if (useApiV7) {
            // Parse error from API v7 response
            if (json.containsKey("status")) {
                return json.getString("status");
            } else if (json.containsKey("title")) {
                return json.getString("title");
            }
        } else {
            // Parse error from legacy API response
            JsonObject response = json.getJsonObject("Response");
            if (response != null && response.containsKey("MetaInfo")) {
                JsonObject metaInfo = response.getJsonObject("MetaInfo");
                if (metaInfo.containsKey("StatusCode")) {
                    return metaInfo.getString("StatusCode");
                }
            }
        }
        return null;
    }

    /**
     * Reports the health status of the geocoder.
     * Used by the health check system for service discovery and monitoring.
     *
     * @return true if the geocoder is healthy and operational, false otherwise
     */
    @Override
    public boolean isHealthy() {
        return healthy && !circuitBreaker.getState().equals(CircuitBreaker.State.OPEN);
    }

    /**
     * Sets the distributed cache manager for cross-service caching.
     *
     * @param distributedCacheManager Distributed cache manager instance
     */
    public void setDistributedCacheManager(JsonGeocoder.DistributedCacheManager distributedCacheManager) {
        super.setDistributedCacheManager(distributedCacheManager);
    }

    /**
     * Sets the statistics manager for metrics collection.
     *
     * @param statisticsManager Statistics manager instance
     */
    @Override
    public void setStatisticsManager(StatisticsManager statisticsManager) {
        super.setStatisticsManager(statisticsManager);
    }
}