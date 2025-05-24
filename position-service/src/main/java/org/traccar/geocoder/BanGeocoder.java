/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.InvocationCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Geocoder implementation for the French BAN (Base Adresse Nationale) API.
 * This geocoder uses the adresse.data.gouv.fr API to geocode addresses in France.
 */
@Service
public class BanGeocoder extends JsonGeocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(BanGeocoder.class);
    private static final String CACHE_NAME = "banGeocoderCache";
    private static final String FALLBACK_ADDRESS = "Address not found";

    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final MeterRegistry meterRegistry;
    private final Cache cache;

    /**
     * Constructs a BanGeocoder with the specified parameters.
     *
     * @param client The HTTP client
     * @param url The URL template for the BAN API
     * @param cacheSize The cache size
     * @param addressFormat The address format
     * @param circuitBreaker The circuit breaker for resilience
     * @param retry The retry mechanism for resilience
     * @param meterRegistry The meter registry for metrics
     * @param cacheManager The cache manager for distributed caching
     */
    @Autowired
    public BanGeocoder(
            Client client,
            @Qualifier("banGeocoderUrl") String url,
            @Qualifier("geocoderCacheSize") int cacheSize,
            AddressFormat addressFormat,
            @Qualifier("banGeocoderCircuitBreaker") CircuitBreaker circuitBreaker,
            @Qualifier("banGeocoderRetry") Retry retry,
            MeterRegistry meterRegistry,
            CacheManager cacheManager) {
        super(client, url, 0, addressFormat); // Set cache size to 0 as we'll use Spring Cache
        this.circuitBreaker = circuitBreaker;
        this.retry = retry;
        this.meterRegistry = meterRegistry;
        this.cache = cacheManager.getCache(CACHE_NAME);
    }

    /**
     * Parses the address from the BAN API response.
     *
     * @param json The JSON response from the BAN API
     * @return The parsed address
     */
    @Override
    public Address parseAddress(JsonObject json) {
        JsonArray features = json.getJsonArray("features");
        if (features != null && !features.isEmpty()) {
            JsonObject feature = features.getJsonObject(0);
            JsonObject properties = feature.getJsonObject("properties");
            
            if (properties != null) {
                Address address = new Address();
                
                address.setFormattedAddress(properties.getString("label", null));
                address.setHouse(properties.getString("housenumber", null));
                address.setStreet(properties.getString("street", null));
                address.setSettlement(properties.getString("city", null));
                address.setPostcode(properties.getString("postcode", null));
                address.setCountry("FR"); // BAN only covers France
                
                // Extract context information (department, region)
                String context = properties.getString("context", "");
                if (!context.isEmpty()) {
                    String[] parts = context.split(",");
                    if (parts.length >= 2) {
                        address.setDistrict(parts[0].trim()); // Department code
                        address.setState(parts[parts.length - 1].trim()); // Region
                    }
                }
                
                return address;
            }
        }
        return null;
    }

    /**
     * Extracts error information from the BAN API response.
     *
     * @param json The JSON response from the BAN API
     * @return The error message or null if no error is found
     */
    @Override
    protected String parseError(JsonObject json) {
        if (json.containsKey("message")) {
            return json.getString("message");
        }
        return null;
    }

    /**
     * Gets the address for the specified coordinates with resilience patterns applied.
     *
     * @param latitude The latitude
     * @param longitude The longitude
     * @param callback The callback for asynchronous processing
     * @return The address or null if not found
     */
    @Override
    public String getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        // Check distributed cache first
        Map.Entry<Double, Double> key = new AbstractMap.SimpleImmutableEntry<>(latitude, longitude);
        String cachedAddress = getCachedAddress(key);
        if (cachedAddress != null) {
            if (callback != null) {
                callback.onSuccess(cachedAddress);
            }
            return cachedAddress;
        }

        // Record metrics
        Timer.Sample sample = Timer.start(meterRegistry);
        meterRegistry.counter("geocoder.ban.requests").increment();

        if (callback != null) {
            // Asynchronous processing with resilience patterns
            CompletableFuture.supplyAsync(() -> {
                return executeWithResilience(() -> {
                    return super.getAddress(latitude, longitude, null);
                });
            }).thenAccept(address -> {
                sample.stop(meterRegistry.timer("geocoder.ban.request.time"));
                if (address != null) {
                    cacheAddress(key, address);
                    callback.onSuccess(address);
                    meterRegistry.counter("geocoder.ban.success").increment();
                } else {
                    String fallback = getFallbackAddress(latitude, longitude);
                    callback.onSuccess(fallback);
                    meterRegistry.counter("geocoder.ban.fallback").increment();
                }
            }).exceptionally(e -> {
                meterRegistry.counter("geocoder.ban.error").increment();
                callback.onFailure(e);
                return null;
            });
            return null;
        } else {
            // Synchronous processing with resilience patterns
            try {
                String address = executeWithResilience(() -> {
                    return super.getAddress(latitude, longitude, null);
                });
                
                sample.stop(meterRegistry.timer("geocoder.ban.request.time"));
                
                if (address != null) {
                    cacheAddress(key, address);
                    meterRegistry.counter("geocoder.ban.success").increment();
                    return address;
                } else {
                    String fallback = getFallbackAddress(latitude, longitude);
                    meterRegistry.counter("geocoder.ban.fallback").increment();
                    return fallback;
                }
            } catch (Exception e) {
                meterRegistry.counter("geocoder.ban.error").increment();
                LOGGER.warn("BAN geocoder error", e);
                return getFallbackAddress(latitude, longitude);
            }
        }
    }

    /**
     * Executes the supplier with circuit breaker and retry patterns applied.
     *
     * @param supplier The supplier to execute
     * @return The result of the supplier or null if an exception occurs
     */
    private String executeWithResilience(Supplier<String> supplier) {
        return Retry.decorateSupplier(retry, 
                CircuitBreaker.decorateSupplier(circuitBreaker, supplier))
                .get();
    }

    /**
     * Gets the address from the distributed cache.
     *
     * @param key The cache key
     * @return The cached address or null if not found
     */
    private String getCachedAddress(Map.Entry<Double, Double> key) {
        if (cache != null) {
            Cache.ValueWrapper value = cache.get(key);
            if (value != null) {
                return (String) value.get();
            }
        }
        return null;
    }

    /**
     * Caches the address in the distributed cache.
     *
     * @param key The cache key
     * @param address The address to cache
     */
    private void cacheAddress(Map.Entry<Double, Double> key, String address) {
        if (cache != null && address != null) {
            cache.put(key, address);
        }
    }

    /**
     * Gets a fallback address when geocoding fails.
     *
     * @param latitude The latitude
     * @param longitude The longitude
     * @return The fallback address
     */
    private String getFallbackAddress(double latitude, double longitude) {
        return FALLBACK_ADDRESS + " (" + latitude + ", " + longitude + ")";
    }
}