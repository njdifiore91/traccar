/*
 * Copyright 2014 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2024 - 2024 Matjaž Črnko (m.crnko@txt.i)
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
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Geocoder implementation that uses a JSON API to perform geocoding.
 * This implementation includes circuit breaker, retry policies, metrics collection,
 * and distributed caching for microservices architecture.
 */
public class GeocodeJsonGeocoder extends JsonGeocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeocodeJsonGeocoder.class);

    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final MeterRegistry meterRegistry;
    private final Map<Map.Entry<Double, Double>, String> cache;
    private final RedisTemplate<String, String> redisTemplate;
    private final Timer geocodingRequestTimer;
    private final Counter geocodingSuccessCounter;
    private final Counter geocodingFailureCounter;
    private final Counter geocodingFallbackCounter;
    private final Counter cacheHitCounter;
    private final Counter cacheMissCounter;

    private static String formatUrl(String url, String key, String language) {
        if (url == null) {
            url = "https://photon.komoot.io/reverse";
        }
        url += "?lat=%f&lon=%f";
        if (key != null) {
            url += "&key=" + key;
        }
        if (language != null) {
            url += "&lang=" + language;
        }
        return url;
    }

    /**
     * Creates a new instance of the GeocodeJsonGeocoder with circuit breaker, metrics, and distributed cache.
     *
     * @param client HTTP client for making API requests
     * @param url Base URL for the geocoding service
     * @param key API key for the geocoding service
     * @param language Language preference for geocoding results
     * @param cacheSize Size of the local cache (will be ignored as we use Redis)
     * @param addressFormat Format for address display
     * @param meterRegistry Registry for metrics collection
     * @param redisTemplate Redis template for distributed caching
     */
    public GeocodeJsonGeocoder(
            Client client, String url, String key, String language, int cacheSize, AddressFormat addressFormat,
            MeterRegistry meterRegistry, RedisTemplate<String, String> redisTemplate) {
        super(client, formatUrl(url, key, language), 0, addressFormat); // Set local cache size to 0 as we'll use Redis
        this.meterRegistry = meterRegistry;
        this.redisTemplate = redisTemplate;
        this.cache = super.getCache(); // Keep reference to parent's cache for compatibility

        // Initialize metrics
        this.geocodingRequestTimer = Timer.builder("geocoding.request.duration")
                .description("Time taken to complete geocoding requests")
                .tag("geocoder", "json")
                .register(meterRegistry);
        
        this.geocodingSuccessCounter = Counter.builder("geocoding.requests.success")
                .description("Number of successful geocoding requests")
                .tag("geocoder", "json")
                .register(meterRegistry);
        
        this.geocodingFailureCounter = Counter.builder("geocoding.requests.failure")
                .description("Number of failed geocoding requests")
                .tag("geocoder", "json")
                .register(meterRegistry);
        
        this.geocodingFallbackCounter = Counter.builder("geocoding.requests.fallback")
                .description("Number of times fallback was used for geocoding")
                .tag("geocoder", "json")
                .register(meterRegistry);
        
        this.cacheHitCounter = Counter.builder("geocoding.cache.hits")
                .description("Number of cache hits for geocoding requests")
                .tag("geocoder", "json")
                .register(meterRegistry);
        
        this.cacheMissCounter = Counter.builder("geocoding.cache.misses")
                .description("Number of cache misses for geocoding requests")
                .tag("geocoder", "json")
                .register(meterRegistry);

        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .slidingWindowSize(10) // Consider last 10 calls for failure rate calculation
                .recordExceptions(Exception.class) // Record all exceptions as failures
                .build();

        this.circuitBreaker = CircuitBreaker.of("geocodingCircuitBreaker", circuitBreakerConfig);
        
        // Register circuit breaker state transition listener for metrics
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    LOGGER.info("Circuit breaker state changed from {} to {}", 
                            event.getStateTransition().getFromState(),
                            event.getStateTransition().getToState());
                    
                    // Update gauge for circuit breaker state
                    meterRegistry.gauge("geocoding.circuitbreaker.state", 
                            circuitBreaker, 
                            cb -> cb.getState().getOrder());
                });

        // Configure retry policy
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Maximum 3 attempts
                .waitDuration(Duration.ofMillis(500)) // Initial wait duration
                .retryExceptions(TimeoutException.class) // Retry on timeout
                .ignoreExceptions(GeocoderException.class) // Don't retry on geocoder exceptions
                .build();

        this.retry = Retry.of("geocodingRetry", retryConfig);
    }

    @Override
    public String getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        // First check distributed Redis cache
        String cacheKey = "geocoder:" + latitude + ":" + longitude;
        String cachedAddress = redisTemplate.opsForValue().get(cacheKey);
        
        if (cachedAddress != null) {
            cacheHitCounter.increment();
            if (callback != null) {
                callback.onSuccess(cachedAddress);
            }
            return cachedAddress;
        }
        cacheMissCounter.increment();
        
        // If not in Redis cache, check local cache
        Map.Entry<Double, Double> cacheKey = new AbstractMap.SimpleImmutableEntry<>(latitude, longitude);
        String localCachedAddress = cache != null ? cache.get(cacheKey) : null;
        
        if (localCachedAddress != null) {
            if (callback != null) {
                callback.onSuccess(localCachedAddress);
            }
            cacheHitCounter.increment();
            // Store in Redis for other instances with a TTL of 24 hours
            String redisCacheKey = "geocoder:" + latitude + ":" + longitude;
            redisTemplate.opsForValue().set(redisCacheKey, localCachedAddress, Duration.ofHours(24));
            return localCachedAddress;
        }

        // Apply circuit breaker and retry patterns
        Supplier<String> geocodingSupplier = () -> {
            try {
                return geocodingRequestTimer.record(() -> {
                    String address = performGeocodingRequest(latitude, longitude, callback);
                    if (address != null) {
                        // Store in Redis cache with a TTL of 24 hours
                        redisTemplate.opsForValue().set(cacheKey, address, Duration.ofHours(24));
                        geocodingSuccessCounter.increment();
                    }
                    return address;
                });
            } catch (Exception e) {
                geocodingFailureCounter.increment();
                LOGGER.warn("Geocoding request failed", e);
                throw e;
            }
        };

        // Apply retry and circuit breaker patterns
        Supplier<String> retryingSupplier = Retry.decorateSupplier(retry, geocodingSupplier);
        Supplier<String> resilientSupplier = CircuitBreaker.decorateSupplier(circuitBreaker, retryingSupplier);

        try {
            return resilientSupplier.get();
        } catch (Exception e) {
            // Fallback strategy when circuit is open or all retries failed
            geocodingFallbackCounter.increment();
            LOGGER.warn("Using fallback for geocoding request", e);
            return handleFallback(latitude, longitude, callback);
        }
    }



    /**
     * Handles fallback when geocoding service is unavailable.
     * This provides a graceful degradation of service when the geocoding API is down.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param callback Callback for async processing
     * @return A fallback address representation
     */
    /**
     * Handles fallback when geocoding service is unavailable.
     * This provides a graceful degradation of service when the geocoding API is down.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param callback Callback for async processing
     * @return A fallback address representation
     */
    private String handleFallback(double latitude, double longitude, ReverseGeocoderCallback callback) {
        // Simple fallback: return coordinates as string
        String fallbackAddress = String.format("%.6f, %.6f", latitude, longitude);
        
        if (callback != null) {
            callback.onSuccess(fallbackAddress);
        }
        
        // Store the fallback in Redis with a shorter TTL
        String redisCacheKey = "geocoder:" + latitude + ":" + longitude;
        redisTemplate.opsForValue().set(redisCacheKey, fallbackAddress, Duration.ofMinutes(5));
        
        return fallbackAddress;
    }

    /**
     * Parses the JSON response from the geocoding service into an Address object.
     * 
     * @param json The JSON response from the geocoding service
     * @return The parsed Address object or null if parsing failed
     */
    @Override
    public Address parseAddress(JsonObject json) {
        JsonArray features = json.getJsonArray("features");
        if (!features.isEmpty()) {
            Address address = new Address();
            JsonObject properties = features.getJsonObject(0).getJsonObject("properties");

            if (properties.containsKey("label")) {
                address.setFormattedAddress(properties.getString("label"));
            }
            if (properties.containsKey("housenumber")) {
                address.setHouse(properties.getString("housenumber"));
            }
            if (properties.containsKey("street")) {
                address.setStreet(properties.getString("street"));
            }
            if (properties.containsKey("city")) {
                address.setSettlement(properties.getString("city"));
            }
            if (properties.containsKey("district")) {
                address.setDistrict(properties.getString("district"));
            }
            if (properties.containsKey("state")) {
                address.setState(properties.getString("state"));
            }
            if (properties.containsKey("countrycode")) {
                address.setCountry(properties.getString("countrycode").toUpperCase());
            }
            if (properties.containsKey("postcode")) {
                address.setPostcode(properties.getString("postcode"));
            }

            return address;
        }
        return null;
    }

    /**
     * Performs geocoding request without using the cache.
     * This is used by our resilient implementation to separate the API call from caching logic.
     * 
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param callback Callback for async processing
     * @return The geocoded address or null if not found/error
     */
    private String performGeocodingRequest(double latitude, double longitude, ReverseGeocoderCallback callback) {
        if (getStatisticsManager() != null) {
            getStatisticsManager().registerGeocoderRequest();
        }

        var request = getClient().target(String.format(getUrl(), latitude, longitude)).request();

        if (callback != null) {
            request.async().get(new jakarta.ws.rs.client.InvocationCallback<JsonObject>() {
                @Override
                public void completed(JsonObject json) {
                    Address address = parseAddress(json);
                    String formattedAddress = null;
                    if (address != null) {
                        formattedAddress = getAddressFormat().format(address);
                        if (cache != null) {
                            cache.put(new AbstractMap.SimpleImmutableEntry<>(latitude, longitude), formattedAddress);
                        }
                        if (callback != null) {
                            callback.onSuccess(formattedAddress);
                        }
                        geocodingSuccessCounter.increment();
                    } else {
                        String msg = "Empty address. Error: " + (json != null ? json.toString() : "null");
                        if (callback != null) {
                            callback.onFailure(new GeocoderException(msg));
                        } else {
                            LOGGER.warn(msg);
                        }
                    }

                }

                @Override
                public void failed(Throwable throwable) {
                    geocodingFailureCounter.increment();
                    callback.onFailure(throwable);
                }
            });
            return null; // Async call, result will be delivered via callback
        } else {
            try {
                JsonObject json = request.get(JsonObject.class);
                Address address = parseAddress(json);
                if (address != null) {
                    String formattedAddress = getAddressFormat().format(address);
                    return formattedAddress;
                }
            } catch (Exception e) {
                LOGGER.warn("Geocoder network error", e);
                throw e; // Rethrow to be handled by circuit breaker
            }
        }
        return null;
    }
}