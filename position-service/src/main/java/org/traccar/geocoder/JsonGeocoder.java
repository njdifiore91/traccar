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

import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.InvocationCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.database.StatisticsManager;

import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Abstract base class for JSON-based geocoders.
 * Updated to support distributed caching and resilience patterns for microservices architecture.
 */
public abstract class JsonGeocoder implements Geocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonGeocoder.class);

    private final Client client;
    private final String url;
    private final AddressFormat addressFormat;

    private Map<Map.Entry<Double, Double>, Address> cacheLocal;
    private DistributedCacheManager distributedCacheManager;
    private StatisticsManager statisticsManager;

    /**
     * Creates a new JsonGeocoder with the specified parameters.
     *
     * @param client JAX-RS client for HTTP requests
     * @param url URL template with placeholders for coordinates
     * @param cacheSize Size of the local cache (0 to disable)
     * @param addressFormat Format for address display
     */
    public JsonGeocoder(Client client, String url, int cacheSize, AddressFormat addressFormat) {
        this.client = client;
        this.url = url;
        this.addressFormat = addressFormat;

        if (cacheSize > 0) {
            // Create a local LRU cache with the specified size
            this.cacheLocal = new LinkedHashMap<Map.Entry<Double, Double>, Address>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Map.Entry<Double, Double>, Address> eldest) {
                    return size() > cacheSize;
                }
            };
            this.cacheLocal = java.util.Collections.synchronizedMap(this.cacheLocal);
        }
    }

    /**
     * Sets the distributed cache manager for cross-service caching.
     *
     * @param distributedCacheManager Distributed cache manager instance
     */
    public void setDistributedCacheManager(DistributedCacheManager distributedCacheManager) {
        this.distributedCacheManager = distributedCacheManager;
    }

    @Override
    public void setStatisticsManager(StatisticsManager statisticsManager) {
        this.statisticsManager = statisticsManager;
    }

    /**
     * Gets a human-readable address for the specified coordinates.
     * Checks both local and distributed caches before making an HTTP request.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param callback Optional callback for asynchronous execution
     * @return Address object if executed synchronously, null if using callback
     */
    @Override
    public Address getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        Map.Entry<Double, Double> key = new AbstractMap.SimpleImmutableEntry<>(latitude, longitude);
        Address cachedAddress = null;

        // Check local cache first
        if (cacheLocal != null) {
            cachedAddress = cacheLocal.get(key);
        }

        // If not in local cache, check distributed cache
        if (cachedAddress == null && distributedCacheManager != null) {
            String cacheKey = String.format("%s-%f-%f", getClass().getSimpleName(), latitude, longitude);
            cachedAddress = distributedCacheManager.getAddress(cacheKey);
            
            // If found in distributed cache, add to local cache
            if (cachedAddress != null && cacheLocal != null) {
                cacheLocal.put(key, cachedAddress);
            }
        }

        // If found in either cache, return or invoke callback
        if (cachedAddress != null) {
            if (callback != null) {
                callback.onSuccess(cachedAddress);
                return null;
            }
            return cachedAddress;
        }

        // Register statistics if available
        if (statisticsManager != null) {
            statisticsManager.registerGeocoderRequest();
        }

        // Format the URL with coordinates
        String formattedUrl = String.format(url, latitude, longitude);

        if (callback != null) {
            // Asynchronous execution with callback
            client.target(formattedUrl).request().async().get(new InvocationCallback<JsonObject>() {
                @Override
                public void completed(JsonObject json) {
                    try {
                        handleResponse(latitude, longitude, json, callback);
                    } catch (Exception e) {
                        failed(e);
                    }
                }

                @Override
                public void failed(Throwable throwable) {
                    callback.onFailure(throwable);
                }
            });
            return null;
        } else {
            // Synchronous execution
            try {
                JsonObject json = client.target(formattedUrl).request().get(JsonObject.class);
                return handleResponse(latitude, longitude, json, null);
            } catch (Exception e) {
                LOGGER.warn("Geocoder network error", e);
                return null;
            }
        }
    }

    /**
     * Handles the JSON response from the geocoding service.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param json JSON response from the service
     * @param callback Optional callback for asynchronous execution
     * @return Address object if executed synchronously, null if using callback
     */
    private Address handleResponse(double latitude, double longitude, JsonObject json, ReverseGeocoderCallback callback) {
        Address address = parseAddress(json);
        Map.Entry<Double, Double> key = new AbstractMap.SimpleImmutableEntry<>(latitude, longitude);
        
        if (address != null) {
            if (addressFormat != null) {
                address.setFormattedAddress(addressFormat.format(address));
            }
            
            // Cache the result locally
            if (cacheLocal != null) {
                cacheLocal.put(key, address);
            }
            
            // Cache the result in distributed cache
            if (distributedCacheManager != null) {
                String cacheKey = String.format("%s-%f-%f", getClass().getSimpleName(), latitude, longitude);
                distributedCacheManager.putAddress(cacheKey, address);
            }
            
            if (callback != null) {
                callback.onSuccess(address);
            }
        } else {
            String error = parseError(json);
            if (error != null) {
                LOGGER.warn("Geocoder error: {}", error);
            } else {
                LOGGER.warn("Empty geocoder response");
            }
            if (callback != null) {
                callback.onFailure(new GeocoderException(error));
            }
        }
        
        return address;
    }

    /**
     * Parses the JSON response into an Address object.
     * Must be implemented by subclasses for each specific geocoding service.
     *
     * @param json JSON response from the service
     * @return Address object or null if parsing failed
     */
    public abstract Address parseAddress(JsonObject json);

    /**
     * Parses error information from the JSON response.
     * Can be overridden by subclasses for service-specific error handling.
     *
     * @param json JSON response from the service
     * @return Error message or null if no error was found
     */
    protected String parseError(JsonObject json) {
        return null;
    }

    /**
     * Safely reads a string value from a JSON object.
     *
     * @param json JSON object to read from
     * @param key Key to read
     * @return String value or null if not found
     */
    protected String readValue(JsonObject json, String key) {
        if (json.containsKey(key) && !json.isNull(key)) {
            return json.getString(key);
        }
        return null;
    }

    /**
     * Interface for distributed cache management.
     * Implementations should provide Redis or other distributed cache integration.
     */
    public interface DistributedCacheManager {
        Address getAddress(String key);
        void putAddress(String key, Address address);
    }
}