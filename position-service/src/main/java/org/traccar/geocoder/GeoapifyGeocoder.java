/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
import jakarta.ws.rs.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class GeoapifyGeocoder extends JsonGeocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeoapifyGeocoder.class);
    
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final RedisTemplate<String, String> redisTemplate;
    private final MeterRegistry meterRegistry;
    private final String cacheKeyPrefix = "geocoder:geoapify:";
    private final int cacheTtlSeconds;
    
    // Metrics
    private final Timer requestTimer;
    private final Counter requestCounter;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Counter fallbackCounter;
    private final Counter cacheHitCounter;
    private final Counter cacheMissCounter;

    private static String formatUrl(String key, String language) {
        String url = "https://api.geoapify.com/v1/geocode/reverse?format=json&lat=%f&lon=%f";
        if (key != null) {
            url += "&apiKey=" + key;
        }
        if (language != null) {
            url += "&lang=" + language;
        }
        return url;
    }

    public GeoapifyGeocoder(
            Client client, 
            String key, 
            String language, 
            int cacheSize, 
            AddressFormat addressFormat,
            ResilienceConfig resilienceConfig,
            RedisTemplate<String, String> redisTemplate,
            MeterRegistry meterRegistry,
            int cacheTtlSeconds) {
        
        super(client, formatUrl(key, language), cacheSize, addressFormat);
        
        this.circuitBreaker = resilienceConfig.getCircuitBreaker("geoapify");
        this.retry = resilienceConfig.getRetry("geoapify");
        this.redisTemplate = redisTemplate;
        this.meterRegistry = meterRegistry;
        this.cacheTtlSeconds = cacheTtlSeconds;
        
        // Initialize metrics
        this.requestTimer = Timer.builder("geocoder.geoapify.request.time")
                .description("Time taken for Geoapify geocoding requests")
                .register(meterRegistry);
        this.requestCounter = Counter.builder("geocoder.geoapify.request.count")
                .description("Number of Geoapify geocoding requests")
                .register(meterRegistry);
        this.successCounter = Counter.builder("geocoder.geoapify.request.success")
                .description("Number of successful Geoapify geocoding requests")
                .register(meterRegistry);
        this.failureCounter = Counter.builder("geocoder.geoapify.request.failure")
                .description("Number of failed Geoapify geocoding requests")
                .register(meterRegistry);
        this.fallbackCounter = Counter.builder("geocoder.geoapify.request.fallback")
                .description("Number of fallback responses for Geoapify geocoding")
                .register(meterRegistry);
        this.cacheHitCounter = Counter.builder("geocoder.geoapify.cache.hit")
                .description("Number of cache hits for Geoapify geocoding")
                .register(meterRegistry);
        this.cacheMissCounter = Counter.builder("geocoder.geoapify.cache.miss")
                .description("Number of cache misses for Geoapify geocoding")
                .register(meterRegistry);
    }

    @Override
    public String getAddress(final double latitude, final double longitude, final ReverseGeocoderCallback callback) {
        requestCounter.increment();
        
        // Check distributed cache first
        String cacheKey = cacheKeyPrefix + latitude + ":" + longitude;
        String cachedAddress = redisTemplate.opsForValue().get(cacheKey);
        
        if (cachedAddress != null) {
            cacheHitCounter.increment();
            if (callback != null) {
                callback.onSuccess(cachedAddress);
            }
            return cachedAddress;
        }
        
        cacheMissCounter.increment();
        
        // Define the geocoding operation with resilience patterns
        Supplier<String> geocodingOperation = Retry.decorateSupplier(retry, 
            CircuitBreaker.decorateSupplier(circuitBreaker, () -> {
                Timer.Sample sample = Timer.start(meterRegistry);
                try {
                    String result = executeGeocodingRequest(latitude, longitude, callback);
                    successCounter.increment();
                    sample.stop(requestTimer);
                    
                    // Cache the result in Redis if successful
                    if (result != null) {
                        redisTemplate.opsForValue().set(cacheKey, result, cacheTtlSeconds, TimeUnit.SECONDS);
                    }
                    
                    return result;
                } catch (Exception e) {
                    failureCounter.increment();
                    sample.stop(requestTimer);
                    LOGGER.warn("Geoapify geocoding request failed", e);
                    throw e;
                }
            }));
        
        try {
            return geocodingOperation.get();
        } catch (Exception e) {
            fallbackCounter.increment();
            String fallbackAddress = getFallbackAddress(latitude, longitude);
            if (callback != null) {
                callback.onSuccess(fallbackAddress);
            }
            return fallbackAddress;
        }
    }
    
    private String executeGeocodingRequest(double latitude, double longitude, ReverseGeocoderCallback callback) {
        return super.getAddress(latitude, longitude, callback);
    }
    
    private String getFallbackAddress(double latitude, double longitude) {
        // Implement a simple fallback strategy that returns coordinates as a string
        return String.format("%.6f, %.6f", latitude, longitude);
    }

    @Override
    public Address parseAddress(JsonObject json) {
        JsonArray results = json.getJsonArray("results");
        if (results != null && results.size() > 0) {
            JsonObject result = results.getJsonObject(0);

            Address address = new Address();

            if (json.containsKey("formatted")) {
                address.setFormattedAddress(json.getString("formatted"));
            }

            if (result.containsKey("housenumber")) {
                address.setHouse(result.getString("housenumber"));
            }
            if (result.containsKey("street")) {
                address.setStreet(result.getString("street"));
            }
            if (result.containsKey("suburb")) {
                address.setSuburb(result.getString("suburb"));
            }
            if (result.containsKey("city")) {
                address.setSettlement(result.getString("city"));
            }
            if (result.containsKey("district")) {
                address.setDistrict(result.getString("district"));
            }
            if (result.containsKey("state")) {
                address.setState(result.getString("state"));
            }
            if (result.containsKey("country_code")) {
                address.setCountry(result.getString("country_code").toUpperCase());
            }
            if (result.containsKey("postcode")) {
                address.setPostcode(result.getString("postcode"));
            }

            return address;
        }

        return null;
    }
    
    @Override
    protected String parseError(JsonObject json) {
        if (json.containsKey("error")) {
            return json.getString("error");
        }
        return super.parseError(json);
    }
    
    /**
     * Reports the current health status of the geocoder service
     * @return true if the circuit breaker is closed (service is healthy), false otherwise
     */
    public boolean isHealthy() {
        return circuitBreaker.getState() == CircuitBreaker.State.CLOSED;
    }
    
    /**
     * Gets the current state of the circuit breaker
     * @return the current circuit breaker state
     */
    public CircuitBreaker.State getCircuitBreakerState() {
        return circuitBreaker.getState();
    }
    
    /**
     * Gets metrics about the geocoder service
     * @return a map of metric names to values
     */
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = Map.of(
            "requestCount", requestCounter.count(),
            "successCount", successCounter.count(),
            "failureCount", failureCounter.count(),
            "fallbackCount", fallbackCounter.count(),
            "cacheHitCount", cacheHitCounter.count(),
            "cacheMissCount", cacheMissCounter.count(),
            "circuitBreakerState", circuitBreaker.getState().name(),
            "failureRate", circuitBreaker.getMetrics().getFailureRate(),
            "slowCallRate", circuitBreaker.getMetrics().getSlowCallRate()
        );
        return metrics;
    }
}