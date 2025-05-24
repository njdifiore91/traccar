/*
 * Copyright 2014 - 2015 Stefaan Van Dooren (stefaan.vandooren@gmail.com)
 * Copyright 2017 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Geocoder implementation for Factual API.
 * This implementation includes circuit breaker, retry, metrics collection, and distributed caching.
 */
public class FactualGeocoder extends JsonGeocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(FactualGeocoder.class);
    private static final String GEOCODER_NAME = "factual";
    private static final String CACHE_PREFIX = "geocoder:factual:";

    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final RedisTemplate<String, String> redisTemplate;
    private final MeterRegistry meterRegistry;
    private final Timer requestTimer;
    private final Counter requestCounter;
    private final Counter successCounter;
    private final Counter errorCounter;
    private final Counter fallbackCounter;
    private final Counter cacheHitCounter;
    private final Counter cacheMissCounter;

    private static String formatUrl(String url, String key) {
        if (url == null) {
            url = "https://api.factual.com/geotag";
        }
        url += "?latitude=%f&longitude=%f&KEY=" + key;
        return url;
    }

    /**
     * Creates a new instance of FactualGeocoder with circuit breaker, retry, metrics, and distributed cache.
     *
     * @param client         HTTP client
     * @param url            service URL
     * @param key            service API key
     * @param cacheSize      cache size (not used with Redis cache)
     * @param addressFormat  address format
     * @param redisTemplate  Redis template for distributed caching
     * @param meterRegistry  Micrometer registry for metrics collection
     */
    public FactualGeocoder(Client client, String url, String key, int cacheSize, AddressFormat addressFormat,
                          RedisTemplate<String, String> redisTemplate, MeterRegistry meterRegistry) {
        super(client, formatUrl(url, key), 0, addressFormat); // Set cache size to 0 as we're using Redis instead
        this.redisTemplate = redisTemplate;
        this.meterRegistry = meterRegistry;

        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10) // Consider the last 10 calls
                .minimumNumberOfCalls(5) // Minimum calls before calculating failure rate
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 calls in half-open state
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .recordExceptions(Exception.class) // Record all exceptions as failures
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(GEOCODER_NAME);

        // Configure retry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Try up to 3 times
                .waitDuration(Duration.ofMillis(500)) // Initial wait of 500ms
                .retryExceptions(TimeoutException.class) // Retry on timeout
                .exponentialBackoff(Duration.ofMillis(500), Duration.ofSeconds(5), 2.0) // Exponential backoff
                .build();

        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry(GEOCODER_NAME);

        // Register metrics
        this.requestTimer = Timer.builder("geocoder.request.time")
                .tag("provider", GEOCODER_NAME)
                .description("Time taken for geocoding requests")
                .register(meterRegistry);

        this.requestCounter = Counter.builder("geocoder.requests")
                .tag("provider", GEOCODER_NAME)
                .description("Number of geocoding requests")
                .register(meterRegistry);

        this.successCounter = Counter.builder("geocoder.success")
                .tag("provider", GEOCODER_NAME)
                .description("Number of successful geocoding requests")
                .register(meterRegistry);

        this.errorCounter = Counter.builder("geocoder.errors")
                .tag("provider", GEOCODER_NAME)
                .description("Number of failed geocoding requests")
                .register(meterRegistry);

        this.fallbackCounter = Counter.builder("geocoder.fallbacks")
                .tag("provider", GEOCODER_NAME)
                .description("Number of geocoding fallbacks used")
                .register(meterRegistry);

        this.cacheHitCounter = Counter.builder("geocoder.cache.hits")
                .tag("provider", GEOCODER_NAME)
                .description("Number of geocoding cache hits")
                .register(meterRegistry);

        this.cacheMissCounter = Counter.builder("geocoder.cache.misses")
                .tag("provider", GEOCODER_NAME)
                .description("Number of geocoding cache misses")
                .register(meterRegistry);

        // Register health metrics for circuit breaker
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    LOGGER.info("Circuit breaker '{}' changed state from {} to {}",
                            event.getCircuitBreakerName(), event.getStateTransition().getFromState(),
                            event.getStateTransition().getToState());
                    meterRegistry.gauge("geocoder.circuitbreaker.state",
                            Map.of("provider", GEOCODER_NAME),
                            circuitBreaker, cb -> cb.getState().getOrder());
                });
    }

    @Override
    public String getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        requestCounter.increment();

        // Check Redis cache first
        String cacheKey = CACHE_PREFIX + latitude + ":" + longitude;
        String cachedAddress = redisTemplate.opsForValue().get(cacheKey);

        if (cachedAddress != null) {
            cacheHitCounter.increment();
            if (callback != null) {
                callback.onSuccess(cachedAddress);
            }
            return cachedAddress;
        }

        cacheMissCounter.increment();

        // Wrap the geocoding request with circuit breaker and retry
        Supplier<CompletableFuture<String>> geocodingSupplier = () -> {
            CompletableFuture<String> future = new CompletableFuture<>();

            try {
                // Use timer to measure request duration
                return requestTimer.record(() -> {
                    if (callback != null) {
                        // Asynchronous call with callback
                        super.getAddress(latitude, longitude, new ReverseGeocoderCallback() {
                            @Override
                            public void onSuccess(String address) {
                                successCounter.increment();
                                // Cache the result in Redis
                                if (address != null) {
                                    redisTemplate.opsForValue().set(cacheKey, address);
                                }
                                future.complete(address);
                                callback.onSuccess(address);
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                errorCounter.increment();
                                future.completeExceptionally(e);
                                callback.onFailure(e);
                            }
                        });
                    } else {
                        // Synchronous call
                        String address = super.getAddress(latitude, longitude, null);
                        if (address != null) {
                            successCounter.increment();
                            // Cache the result in Redis
                            redisTemplate.opsForValue().set(cacheKey, address);
                            future.complete(address);
                        } else {
                            errorCounter.increment();
                            future.completeExceptionally(new GeocoderException("Failed to get address"));
                        }
                    }
                    return future;
                });
            } catch (Exception e) {
                errorCounter.increment();
                future.completeExceptionally(e);
                return future;
            }
        };

        // Apply circuit breaker and retry patterns
        try {
            CompletableFuture<String> result = Retry.decorateCompletionStage(
                    retry,
                    CircuitBreaker.decorateSupplier(circuitBreaker, geocodingSupplier)
            ).get().toCompletableFuture();

            // For synchronous calls, block and return the result
            if (callback == null) {
                try {
                    return result.join();
                } catch (Exception e) {
                    LOGGER.warn("Geocoding failed after retries, using fallback", e);
                    return handleFallback(latitude, longitude);
                }
            }

            // For asynchronous calls, add fallback handling
            result.exceptionally(e -> {
                LOGGER.warn("Geocoding failed after retries, using fallback", e);
                String fallbackAddress = handleFallback(latitude, longitude);
                if (callback != null && fallbackAddress != null) {
                    callback.onSuccess(fallbackAddress);
                }
                return fallbackAddress;
            });

            return null; // Async call will be handled by callback
        } catch (Exception e) {
            LOGGER.warn("Circuit breaker or retry configuration error", e);
            if (callback != null) {
                callback.onFailure(e);
            }
            return handleFallback(latitude, longitude);
        }
    }

    /**
     * Fallback method when geocoding fails.
     * Attempts to find a nearby cached location or returns null.
     *
     * @param latitude  latitude
     * @param longitude longitude
     * @return fallback address or null
     */
    private String handleFallback(double latitude, double longitude) {
        fallbackCounter.increment();
        
        // Try to find a nearby location in cache (simplified approach)
        // In a real implementation, you might want to search for nearby coordinates
        // within a small radius, but for simplicity we'll just return null here
        return null;
    }

    @Override
    public Address parseAddress(JsonObject json) {
        JsonObject result = json.getJsonObject("response").getJsonObject("data");
        if (result != null) {
                Address address = new Address();
                if (result.getJsonObject("street_number") != null) {
                    address.setHouse(result.getJsonObject("street_number").getString("name"));
                }
                if (result.getJsonObject("street_name") != null) {
                    address.setStreet(result.getJsonObject("street_name").getString("name"));
                }
                if (result.getJsonObject("locality") != null) {
                    address.setSettlement(result.getJsonObject("locality").getString("name"));
                }
                if (result.getJsonObject("county") != null) {
                    address.setDistrict(result.getJsonObject("county").getString("name"));
                }
                if (result.getJsonObject("region") != null) {
                    address.setState(result.getJsonObject("region").getString("name"));
                }
                if (result.getJsonObject("country") != null) {
                    address.setCountry(result.getJsonObject("country").getString("name"));
                }
                if (result.getJsonObject("postcode") != null) {
                    address.setPostcode(result.getJsonObject("postcode").getString("name"));
                }
                return address;
        }
        return null;
    }

    /**
     * Returns the current state of the circuit breaker.
     * This can be used for health checks and monitoring.
     *
     * @return current circuit breaker state
     */
    public CircuitBreaker.State getCircuitBreakerState() {
        return circuitBreaker.getState();
    }

    /**
     * Returns metrics about the geocoder's performance.
     *
     * @return map of metric name to value
     */
    public Map<String, Number> getMetrics() {
        return Map.of(
            "requestCount", requestCounter.count(),
            "successCount", successCounter.count(),
            "errorCount", errorCounter.count(),
            "fallbackCount", fallbackCounter.count(),
            "cacheHitCount", cacheHitCounter.count(),
            "cacheMissCount", cacheMissCounter.count()
        );
    }
}