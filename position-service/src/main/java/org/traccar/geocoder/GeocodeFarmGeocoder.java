/*
 * Copyright 2016 - 2025 Anton Tananaev (anton@traccar.org)
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
import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;

public class GeocodeFarmGeocoder extends JsonGeocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeocodeFarmGeocoder.class);
    private static final String PROVIDER_NAME = "geocodefarm";

    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final CacheManager cacheManager;
    private final MeterRegistry meterRegistry;

    private static String formatUrl(String key, String language) {
        String url = "https://api.geocode.farm/reverse/?lat=%f&lon=%f";
        if (key != null) {
            url += "&key=" + key;
        }
        if (language != null) {
            url += "&lang=" + language;
        }
        return url;
    }

    public GeocodeFarmGeocoder(
            Client client, String key, String language, int cacheSize, AddressFormat addressFormat,
            ResilienceConfig resilienceConfig, CacheManager cacheManager, MeterRegistry meterRegistry) {
        super(client, formatUrl(key, language), 0, addressFormat); // Set cache size to 0 as we'll use distributed cache
        this.circuitBreaker = resilienceConfig.getCircuitBreaker(PROVIDER_NAME);
        this.retry = resilienceConfig.getRetry(PROVIDER_NAME);
        this.cacheManager = cacheManager;
        this.meterRegistry = meterRegistry;
        
        // Register metrics for monitoring
        meterRegistry.gauge("geocoder." + PROVIDER_NAME + ".state", 
                circuitBreaker, cb -> cb.getState().getOrder());
        meterRegistry.counter("geocoder." + PROVIDER_NAME + ".calls");
        meterRegistry.counter("geocoder." + PROVIDER_NAME + ".errors");
    }

    @Override
    public String getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        // Record metrics
        meterRegistry.counter("geocoder." + PROVIDER_NAME + ".calls").increment();
        
        // Check distributed cache first
        String cachedAddress = getCachedAddress(latitude, longitude);
        if (cachedAddress != null) {
            if (callback != null) {
                callback.onSuccess(cachedAddress);
            }
            return cachedAddress;
        }

        // Apply resilience patterns (circuit breaker and retry) to the geocoding request
        try {
            return Retry.decorateSupplier(retry, 
                    CircuitBreaker.decorateSupplier(circuitBreaker, 
                            () -> executeGeocodingRequest(latitude, longitude, callback)))
                    .get();
        } catch (Exception e) {
            meterRegistry.counter("geocoder." + PROVIDER_NAME + ".errors").increment();
            LOGGER.warn("Geocoding failed after retries", e);
            
            // Execute fallback strategy
            String fallbackAddress = getFallbackAddress(latitude, longitude);
            if (callback != null) {
                if (fallbackAddress != null) {
                    callback.onSuccess(fallbackAddress);
                } else {
                    callback.onFailure(e);
                }
            }
            return fallbackAddress;
        }
    }

    private String executeGeocodingRequest(double latitude, double longitude, ReverseGeocoderCallback callback) {
        // Delegate to the parent implementation but handle caching separately
        String address = super.getAddress(latitude, longitude, null);
        
        // If address was found, store in distributed cache
        if (address != null) {
            cacheAddress(latitude, longitude, address);
            if (callback != null) {
                callback.onSuccess(address);
            }
        }
        
        return address;
    }

    private String getCachedAddress(double latitude, double longitude) {
        Cache cache = cacheManager.getCache("geocoder");
        if (cache != null) {
            Map.Entry<Double, Double> key = new AbstractMap.SimpleImmutableEntry<>(latitude, longitude);
            return Optional.ofNullable(cache.get(key, String.class)).orElse(null);
        }
        return null;
    }

    private void cacheAddress(double latitude, double longitude, String address) {
        Cache cache = cacheManager.getCache("geocoder");
        if (cache != null) {
            Map.Entry<Double, Double> key = new AbstractMap.SimpleImmutableEntry<>(latitude, longitude);
            cache.put(key, address);
        }
    }

    private String getFallbackAddress(double latitude, double longitude) {
        // Simple fallback strategy: return coordinates as string
        return String.format("%.6f, %.6f", latitude, longitude);
    }

    @Override
    public Address parseAddress(JsonObject json) {
        Address address = new Address();

        try {
            JsonObject result = json.getJsonObject("RESULTS").getJsonObject("result").getJsonObject("0");

            if (result.containsKey("formatted_address")) {
                address.setFormattedAddress(result.getString("formatted_address"));
            }
            if (result.containsKey("house_number")) {
                address.setHouse(result.getString("house_number"));
            }
            if (result.containsKey("street_name")) {
                address.setStreet(result.getString("street_name"));
            }
            if (result.containsKey("locality")) {
                address.setSettlement(result.getString("locality"));
            }
            if (result.containsKey("admin_1")) {
                address.setState(result.getString("admin_1"));
            }
            if (result.containsKey("country")) {
                address.setCountry(result.getString("country"));
            }
            if (result.containsKey("postal_code")) {
                address.setPostcode(result.getString("postal_code"));
            }
        } catch (Exception e) {
            meterRegistry.counter("geocoder." + PROVIDER_NAME + ".parse.errors").increment();
            LOGGER.warn("Error parsing geocoding response", e);
            return null;
        }

        return address;
    }

    @Override
    protected String parseError(JsonObject json) {
        try {
            if (json.containsKey("error")) {
                return json.getString("error");
            }
        } catch (Exception e) {
            LOGGER.warn("Error parsing error response", e);
        }
        return null;
    }

    /**
     * Reports the health status of this geocoder for service health checks
     * @return true if the geocoder is operational, false otherwise
     */
    public boolean isHealthy() {
        return !circuitBreaker.getState().equals(CircuitBreaker.State.OPEN);
    }
}