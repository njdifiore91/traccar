/*
 * Copyright 2014 - 2015 Stefaan Van Dooren (stefaan.vandooren@gmail.com)
 * Copyright 2017 Anton Tananaev (anton@traccar.org)
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
import io.micrometer.core.instrument.Timer;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.InvocationCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * BingMapsGeocoder is a geocoder implementation that uses the Bing Maps API for reverse geocoding.
 * This implementation includes circuit breaker, retry, and distributed caching capabilities.
 */
public class BingMapsGeocoder extends JsonGeocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(BingMapsGeocoder.class);
    private static final String CIRCUIT_BREAKER_NAME = "bingMapsGeocoder";
    private static final String RETRY_NAME = "bingMapsGeocoder";
    private static final String CACHE_PREFIX = "geocoder:bingmaps:";
    private static final int CACHE_EXPIRATION_HOURS = 24;

    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final RedisTemplate<String, String> redisTemplate;
    private final MeterRegistry meterRegistry;
    private final Timer requestTimer;

    /**
     * Constructs a BingMapsGeocoder with circuit breaker, retry, and distributed cache capabilities.
     *
     * @param client The HTTP client to use for API requests
     * @param url The base URL for the Bing Maps API
     * @param key The API key for Bing Maps
     * @param cacheSize The size of the local cache (if used as fallback)
     * @param addressFormat The format to use for addresses
     * @param circuitBreakerRegistry The circuit breaker registry
     * @param retryRegistry The retry registry
     * @param redisTemplate The Redis template for distributed caching
     * @param meterRegistry The meter registry for metrics collection
     */
    public BingMapsGeocoder(Client client, String url, String key, int cacheSize, AddressFormat addressFormat,
                           CircuitBreakerRegistry circuitBreakerRegistry, RetryRegistry retryRegistry,
                           RedisTemplate<String, String> redisTemplate, MeterRegistry meterRegistry) {
        super(client, url + "/Locations/%f,%f?key=" + key + "&include=ciso2", cacheSize, addressFormat);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        this.retry = retryRegistry.retry(RETRY_NAME);
        this.redisTemplate = redisTemplate;
        this.meterRegistry = meterRegistry;
        this.requestTimer = Timer.builder("geocoder.bingmaps.request.time")
                .description("Time taken for Bing Maps geocoding requests")
                .register(meterRegistry);
        
        // Register additional metrics
        meterRegistry.gauge("geocoder.bingmaps.circuit_breaker.state", circuitBreaker, 
                cb -> cb.getState().getOrder());
        meterRegistry.gauge("geocoder.bingmaps.circuit_breaker.failure_rate", circuitBreaker, 
                CircuitBreaker::getFailureRate);
        meterRegistry.counter("geocoder.bingmaps.requests.total");
    }

    /**
     * Gets the address for the given coordinates using the Bing Maps API.
     * This implementation includes circuit breaker, retry, and distributed caching capabilities.
     *
     * @param latitude The latitude coordinate
     * @param longitude The longitude coordinate
     * @param callback The callback to invoke with the result
     * @return The address string, or null if the address could not be determined
     */
    @Override
    public String getAddress(final double latitude, final double longitude, final ReverseGeocoderCallback callback) {
        // Create cache key for Redis
        final String cacheKey = CACHE_PREFIX + latitude + ":" + longitude;
        
        // Check distributed cache first
        String cachedAddress = null;
        try {
            cachedAddress = redisTemplate.opsForValue().get(cacheKey);
        } catch (Exception e) {
            LOGGER.warn("Error accessing distributed cache", e);
            // Continue with local cache or API call if distributed cache fails
        }
        
        if (cachedAddress != null) {
            meterRegistry.counter("geocoder.bingmaps.cache.hit").increment();
            if (callback != null) {
                callback.onSuccess(cachedAddress);
            }
            return cachedAddress;
        }
        
        meterRegistry.counter("geocoder.bingmaps.cache.miss").increment();
        
        // If circuit breaker is open, try local cache before failing
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            LOGGER.warn("Circuit breaker is OPEN, using fallback for coordinates: {}, {}", latitude, longitude);
            meterRegistry.counter("geocoder.bingmaps.circuit_breaker.open").increment();
            
            // Try local cache as fallback
            String localCachedAddress = getFromLocalCache(latitude, longitude);
            if (localCachedAddress != null) {
                if (callback != null) {
                    callback.onSuccess(localCachedAddress);
                }
                return localCachedAddress;
            }
            
            // If no cached address is available, return a default message
            String fallbackAddress = "Unknown location (service unavailable)";
            if (callback != null) {
                callback.onSuccess(fallbackAddress);
            }
            return fallbackAddress;
        }
        
        // Wrap the API call with circuit breaker and retry
        Supplier<CompletableFuture<String>> geocodingSupplier = () -> {
            CompletableFuture<String> future = new CompletableFuture<>();
            Timer.Sample sample = Timer.start(meterRegistry);
            
            try {
                meterRegistry.counter("geocoder.bingmaps.requests.total").increment();
                
                var request = getClient().target(String.format(getUrl(), latitude, longitude)).request();
                
                request.async().get(new InvocationCallback<JsonObject>() {
                    @Override
                    public void completed(JsonObject json) {
                        sample.stop(requestTimer);
                        meterRegistry.counter("geocoder.bingmaps.requests.success").increment();
                        
                        String address = handleResponse(latitude, longitude, json, null);
                        if (address != null) {
                            // Store in distributed cache
                            try {
                                redisTemplate.opsForValue().set(cacheKey, address, CACHE_EXPIRATION_HOURS, TimeUnit.HOURS);
                            } catch (Exception e) {
                                LOGGER.warn("Failed to store address in distributed cache", e);
                            }
                            
                            if (callback != null) {
                                callback.onSuccess(address);
                            }
                            future.complete(address);
                        } else {
                            Exception exception = new GeocoderException("Failed to parse address from response");
                            if (callback != null) {
                                callback.onFailure(exception);
                            }
                            future.completeExceptionally(exception);
                        }
                    }
                    
                    @Override
                    public void failed(Throwable throwable) {
                        sample.stop(requestTimer);
                        meterRegistry.counter("geocoder.bingmaps.requests.error").increment();
                        LOGGER.warn("Geocoding request failed", throwable);
                        
                        if (callback != null) {
                            callback.onFailure(throwable);
                        }
                        future.completeExceptionally(throwable);
                    }
                });
            } catch (Exception e) {
                sample.stop(requestTimer);
                meterRegistry.counter("geocoder.bingmaps.requests.error").increment();
                LOGGER.warn("Error executing geocoding request", e);
                
                if (callback != null) {
                    callback.onFailure(e);
                }
                future.completeExceptionally(e);
            }
            
            return future;
        };
        
        // Apply circuit breaker and retry patterns
        try {
            CompletableFuture<String> future = Retry.decorateCompletionStage(
                    retry,
                    CircuitBreaker.decorateSupplier(circuitBreaker, geocodingSupplier)
            ).get();
            
            // If callback is provided, the result will be handled asynchronously
            if (callback == null) {
                try {
                    return future.join();
                } catch (Exception e) {
                    LOGGER.warn("Failed to get address after retries", e);
                    return getFallbackAddress(latitude, longitude);
                }
            }
            
            return null; // Async operation in progress, result will be delivered via callback
        } catch (Exception e) {
            LOGGER.warn("Circuit breaker prevented request execution", e);
            meterRegistry.counter("geocoder.bingmaps.circuit_breaker.prevented").increment();
            
            if (callback != null) {
                callback.onFailure(e);
            }
            return getFallbackAddress(latitude, longitude);
        }
    }
    
    /**
     * Gets an address from the local cache if available.
     *
     * @param latitude The latitude coordinate
     * @param longitude The longitude coordinate
     * @return The cached address, or null if not found
     */
    private String getFromLocalCache(double latitude, double longitude) {
        Map<Map.Entry<Double, Double>, String> cache = getCache();
        if (cache != null) {
            return cache.get(new AbstractMap.SimpleImmutableEntry<>(latitude, longitude));
        }
        return null;
    }
    
    /**
     * Gets a fallback address when the geocoding service is unavailable.
     *
     * @param latitude The latitude coordinate
     * @param longitude The longitude coordinate
     * @return A fallback address string
     */
    private String getFallbackAddress(double latitude, double longitude) {
        // Try local cache first
        String cachedAddress = getFromLocalCache(latitude, longitude);
        if (cachedAddress != null) {
            return cachedAddress;
        }
        
        // Return coordinates as fallback
        return String.format("%.6f, %.6f", latitude, longitude);
    }

    /**
     * Parses the address from the Bing Maps API response.
     *
     * @param json The JSON response from the API
     * @return The parsed address, or null if parsing failed
     */
    @Override
    public Address parseAddress(JsonObject json) {
        JsonArray result = json.getJsonArray("resourceSets");
        if (result != null) {
            JsonObject location =
                    result.getJsonObject(0).getJsonArray("resources").getJsonObject(0).getJsonObject("address");
            if (location != null) {
                Address address = new Address();
                if (location.containsKey("addressLine")) {
                    address.setStreet(location.getString("addressLine"));
                }
                if (location.containsKey("locality")) {
                    address.setSettlement(location.getString("locality"));
                }
                if (location.containsKey("adminDistrict2")) {
                    address.setDistrict(location.getString("adminDistrict2"));
                }
                if (location.containsKey("adminDistrict")) {
                    address.setState(location.getString("adminDistrict"));
                }
                if (location.containsKey("countryRegionIso2")) {
                    address.setCountry(location.getString("countryRegionIso2").toUpperCase());
                }
                if (location.containsKey("postalCode")) {
                    address.setPostcode(location.getString("postalCode"));
                }
                if (location.containsKey("formattedAddress")) {
                    address.setFormattedAddress(location.getString("formattedAddress"));
                }
                return address;
            }
        }
        return null;
    }
    
    /**
     * Parses error information from the API response.
     *
     * @param json The JSON response from the API
     * @return The error message, or null if no error information is available
     */
    @Override
    protected String parseError(JsonObject json) {
        if (json.containsKey("errorDetails")) {
            JsonArray errorDetails = json.getJsonArray("errorDetails");
            if (errorDetails != null && !errorDetails.isEmpty()) {
                return errorDetails.getString(0);
            }
        }
        return super.parseError(json);
    }
    
    /**
     * Gets the client used for API requests.
     *
     * @return The HTTP client
     */
    protected Client getClient() {
        return super.client;
    }
    
    /**
     * Gets the URL used for API requests.
     *
     * @return The URL template
     */
    protected String getUrl() {
        return super.url;
    }
    
    /**
     * Gets the local cache used as a fallback.
     *
     * @return The local cache map
     */
    protected Map<Map.Entry<Double, Double>, String> getCache() {
        return super.cache;
    }
    
    /**
     * Handles the response from the API.
     *
     * @param latitude The latitude coordinate
     * @param longitude The longitude coordinate
     * @param json The JSON response from the API
     * @param callback The callback to invoke with the result
     * @return The address string, or null if the address could not be determined
     */
    protected String handleResponse(double latitude, double longitude, JsonObject json, ReverseGeocoderCallback callback) {
        return super.handleResponse(latitude, longitude, json, callback);
    }
}