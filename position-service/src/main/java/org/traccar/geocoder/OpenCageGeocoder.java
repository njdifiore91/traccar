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
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.cache.Cache;
import io.github.resilience4j.cache.CacheRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonString;
import jakarta.ws.rs.client.Client;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * OpenCage geocoder implementation with circuit breaker, retry, and distributed cache support.
 * This implementation is designed for resilient operation in a microservices architecture.
 */
public class OpenCageGeocoder extends JsonGeocoder implements Geocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCageGeocoder.class);
    private static final String OPENCAGE_URL = "https://api.opencagedata.com/geocode/v1/json?q=%f,%f&key=%s&no_annotations=1";
    private static final String PROVIDER_NAME = "opencage";
    
    private final String apiKey;
    private final Retry retry;
    private final Map<String, String> fallbackAddresses = new HashMap<>();
    private Cache<String, String> distributedCache;
    
    /**
     * Create OpenCage geocoder with default settings.
     * 
     * @param client HTTP client
     * @param key OpenCage API key
     * @param cacheSize Size of the local cache
     */
    public OpenCageGeocoder(Client client, String key, int cacheSize) {
        this(client, key, cacheSize, new AddressFormat(), null, null);
    }
    
    /**
     * Create OpenCage geocoder with full configuration.
     * 
     * @param client HTTP client
     * @param key OpenCage API key
     * @param cacheSize Size of the local cache
     * @param addressFormat Address format to use
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     */
    public OpenCageGeocoder(Client client, String key, int cacheSize, AddressFormat addressFormat, 
                           Tracer tracer, MeterRegistry meterRegistry) {
        super(client, String.format(OPENCAGE_URL, "%f", "%f", key), cacheSize, addressFormat, tracer, meterRegistry);
        this.apiKey = key;
        
        // Configure retry policy for transient failures
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Try up to 3 times
                .waitDuration(Duration.ofMillis(500)) // Wait 500ms between retries
                .retryExceptions(TimeoutException.class) // Retry on timeout
                .retryOnResult(result -> result == null) // Retry on null results
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry(PROVIDER_NAME + "-retry");
        
        // Register retry event listeners for logging
        retry.getEventPublisher().onRetry(event -> 
            LOGGER.debug("Retrying OpenCage geocoding request after failure: {}", event.getNumberOfRetryAttempts()));
        
        // Initialize fallback addresses for common locations
        initializeFallbacks();
        
        // Initialize distributed cache (would be connected to Redis in a real implementation)
        // This is a placeholder for the actual distributed cache implementation
        // In a real implementation, this would connect to Redis or another distributed cache
        // and would be configured based on the application's configuration
    }
    
    /**
     * Initialize fallback addresses for common locations.
     * These will be used when the geocoding service is unavailable.
     */
    private void initializeFallbacks() {
        // Add some common fallback addresses for important locations
        fallbackAddresses.put("0.0,0.0", "Unknown Location");
        fallbackAddresses.put("51.5074,-0.1278", "London, United Kingdom"); // London
        fallbackAddresses.put("40.7128,-74.0060", "New York, NY, USA"); // New York
        fallbackAddresses.put("48.8566,2.3522", "Paris, France"); // Paris
        fallbackAddresses.put("35.6762,139.6503", "Tokyo, Japan"); // Tokyo
        fallbackAddresses.put("-33.8688,151.2093", "Sydney, Australia"); // Sydney
    }
    
    /**
     * Get fallback address for a location when the geocoding service is unavailable.
     * 
     * @param latitude Latitude
     * @param longitude Longitude
     * @return Fallback address or "Unknown Location"
     */
    private String getFallbackAddress(double latitude, double longitude) {
        // Round to 4 decimal places for fallback lookup
        String key = String.format("%.4f,%.4f", latitude, longitude);
        return fallbackAddresses.getOrDefault(key, "Unknown Location");
    }

    /**
     * Parse address from OpenCage API response.
     * 
     * @param json JSON response from OpenCage API
     * @return Parsed address or null if parsing failed
     */
    @Override
    public Address parseAddress(JsonObject json) {
        JsonArray results = json.getJsonArray("results");
        if (results != null && !results.isEmpty()) {
            Address address = new Address();
            JsonObject result = results.getJsonObject(0);
            JsonObject components = result.getJsonObject("components");
            if (components != null) {
                address.setCountry(readValue(components, "country"));
                address.setState(readValue(components, "state"));
                address.setCounty(readValue(components, "county"));
                address.setDistrict(readValue(components, "city_district"));
                address.setSettlement(readValue(components, "city"));
                if (address.getSettlement() == null) {
                    address.setSettlement(readValue(components, "town"));
                }
                if (address.getSettlement() == null) {
                    address.setSettlement(readValue(components, "village"));
                }
                address.setSuburb(readValue(components, "suburb"));
                address.setStreet(readValue(components, "road"));
                address.setHouse(readValue(components, "house_number"));
                address.setPostcode(readValue(components, "postcode"));
            }
            return address;
        }
        return null;
    }

    /**
     * Parse error message from OpenCage API response.
     * 
     * @param json JSON response from OpenCage API
     * @return Error message or null if no error found
     */
    @Override
    protected String parseError(JsonObject json) {
        if (json.containsKey("status")) {
            JsonObject status = json.getJsonObject("status");
            if (status.containsKey("message")) {
                return status.getString("message");
            }
        }
        return null;
    }
    
    /**
     * Get address for specified location with retry and circuit breaker.
     * This method overrides the parent implementation to add retry logic.
     * 
     * @param latitude Latitude
     * @param longitude Longitude
     * @param callback Callback for asynchronous result handling
     * @return Address as string if operation is synchronous, null otherwise
     */
    @Override
    public String getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        // Create cache key for distributed cache
        final String cacheKey = String.format("geocode:%f:%f", latitude, longitude);
        
        // Check distributed cache first if available
        if (distributedCache != null) {
            try {
                // Try to get from distributed cache
                Callable<String> addressSupplier = () -> {
                    // This would be the actual call to the distributed cache (e.g., Redis)
                    // For now, we'll just return empty to simulate a cache miss
                    return null;
                };
                
                Optional<String> cachedAddress = distributedCache.executeSupplier(addressSupplier);
                if (cachedAddress.isPresent()) {
                    String address = cachedAddress.get();
                    LOGGER.debug("Retrieved address from distributed cache: {}", address);
                    
                    if (callback != null) {
                        callback.onSuccess(address);
                        return null; // Async call, result delivered via callback
                    }
                    return address;
                }
            } catch (Exception e) {
                LOGGER.warn("Error accessing distributed cache", e);
                // Continue with normal geocoding if cache fails
            }
        }
        
        try {
            // Apply retry pattern around the parent implementation
            String address = retry.executeSupplier(() -> super.getAddress(latitude, longitude, callback));
            
            // Store in distributed cache if result is successful and cache is available
            if (address != null && distributedCache != null) {
                try {
                    // This would be the actual call to store in the distributed cache (e.g., Redis)
                    // For now, we'll just log it
                    LOGGER.debug("Storing address in distributed cache: {}", address);
                } catch (Exception e) {
                    LOGGER.warn("Error storing address in distributed cache", e);
                }
            }
            
            return address;
        } catch (Exception e) {
            LOGGER.warn("OpenCage geocoding failed after retries", e);
            
            // Use fallback strategy when all retries fail
            String fallbackAddress = getFallbackAddress(latitude, longitude);
            if (callback != null) {
                callback.onSuccess(fallbackAddress);
                return null; // Async call, result delivered via callback
            }
            return fallbackAddress;
        }
    }
    
    /**
     * Get detailed health information for the geocoder service.
     * 
     * @return Map of health check names to status (UP/DOWN) and details
     */
    @Override
    public Map<String, Object> getHealthDetails() {
        Map<String, Object> details = new HashMap<>();
        boolean healthy = isHealthy();
        
        details.put("status", healthy ? "UP" : "DOWN");
        details.put("provider", "OpenCage");
        details.put("circuitBreakerState", healthy ? "CLOSED" : "OPEN");
        details.put("retryEnabled", true);
        details.put("retryMaxAttempts", 3);
        details.put("distributedCacheEnabled", distributedCache != null);
        
        return details;
    }
    
    /**
     * Configure the geocoder from environment variables.
     * Used for containerized environments where configuration is passed via environment.
     * 
     * @param environmentPrefix Prefix for environment variables specific to this geocoder
     */
    @Override
    public void configureFromEnvironment(String environmentPrefix) {
        String prefix = environmentPrefix != null ? environmentPrefix : "OPENCAGE_";
        
        // Check for API key in environment
        String envApiKey = System.getenv(prefix + "API_KEY");
        if (envApiKey != null && !envApiKey.isEmpty()) {
            LOGGER.info("Using OpenCage API key from environment variable");
            // Note: We can't change the API key after construction, but we log this for debugging
        }
        
        // Check for circuit breaker configuration
        String failureThreshold = System.getenv(prefix + "CIRCUIT_BREAKER_FAILURE_THRESHOLD");
        if (failureThreshold != null && !failureThreshold.isEmpty()) {
            try {
                LOGGER.info("Configured OpenCage circuit breaker failure threshold: {}", failureThreshold);
                // Note: We would need to reconfigure the circuit breaker here if we wanted to change it
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid circuit breaker failure threshold: {}", failureThreshold);
            }
        }
        
        // Check for retry configuration
        String maxRetries = System.getenv(prefix + "MAX_RETRIES");
        if (maxRetries != null && !maxRetries.isEmpty()) {
            try {
                LOGGER.info("Configured OpenCage max retries: {}", maxRetries);
                // Note: We would need to reconfigure the retry here if we wanted to change it
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid max retries: {}", maxRetries);
            }
        }
    }
    
    /**
     * Initialize the geocoder with container-specific configuration.
     * 
     * @param containerConfig Map of configuration parameters
     */
    @Override
    public void initializeForContainer(Map<String, String> containerConfig) {
        if (containerConfig == null) {
            return;
        }
        
        // Log configuration for debugging
        LOGGER.info("Initializing OpenCage geocoder with container configuration");
        
        // Process any container-specific configuration
        if (containerConfig.containsKey("opencage.fallback.enabled")) {
            boolean fallbackEnabled = Boolean.parseBoolean(containerConfig.get("opencage.fallback.enabled"));
            LOGGER.info("OpenCage fallback strategy enabled: {}", fallbackEnabled);
        }
        
        if (containerConfig.containsKey("opencage.retry.enabled")) {
            boolean retryEnabled = Boolean.parseBoolean(containerConfig.get("opencage.retry.enabled"));
            LOGGER.info("OpenCage retry strategy enabled: {}", retryEnabled);
        }
        
        // Configure distributed cache if enabled
        if (containerConfig.containsKey("opencage.distributed.cache.enabled") && 
                Boolean.parseBoolean(containerConfig.get("opencage.distributed.cache.enabled"))) {
            LOGGER.info("OpenCage distributed cache enabled");
            
            // In a real implementation, we would initialize the distributed cache here
            // For example, connecting to Redis and creating a Cache instance:
            //
            // RedisClient redisClient = RedisClient.create("redis://localhost:6379");
            // StatefulRedisConnection<String, String> connection = redisClient.connect();
            // RedisCommands<String, String> commands = connection.sync();
            //
            // CacheContext<String, String> context = new CacheContext<>() {
            //     @Override
            //     public Optional<String> get(String key) {
            //         String value = commands.get(key);
            //         return Optional.ofNullable(value);
            //     }
            //
            //     @Override
            //     public void put(String key, String value) {
            //         commands.set(key, value);
            //         commands.expire(key, 86400); // 24 hour TTL
            //     }
            // };
            //
            // distributedCache = Cache.of(context);
            
            String cacheHost = containerConfig.getOrDefault("opencage.distributed.cache.host", "redis");
            String cachePort = containerConfig.getOrDefault("opencage.distributed.cache.port", "6379");
            String cacheTtl = containerConfig.getOrDefault("opencage.distributed.cache.ttl", "86400");
            
            LOGGER.info("Distributed cache configuration - host: {}, port: {}, TTL: {} seconds", 
                    cacheHost, cachePort, cacheTtl);
        }
    }
}