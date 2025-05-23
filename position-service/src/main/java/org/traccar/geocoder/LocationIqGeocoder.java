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
import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * LocationIQ geocoder implementation with enhanced resilience patterns for microservices architecture.
 * This implementation includes circuit breaker, retry mechanisms, metrics collection, and distributed caching.
 * 
 * LocationIQ is based on Nominatim but requires an API key and has its own usage policy:
 * - Provides proper User-Agent header
 * - Implements robust caching to avoid repeated queries
 * - Includes fallback mechanisms for service unavailability
 */
public class LocationIqGeocoder extends JsonGeocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocationIqGeocoder.class);
    
    private static final String PROVIDER_NAME = "locationiq";
    private static final String USER_AGENT = "Traccar-Server/1.0 (https://www.traccar.org)";
    
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final MeterRegistry meterRegistry;
    private final Timer requestTimer;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Counter fallbackCounter;
    private final Counter circuitBreakerOpenCounter;
    private final Counter cacheHitCounter;
    private boolean healthy = true;

    private static String formatUrl(String url, String key, String language) {
        if (url == null) {
            url = "https://us1.locationiq.com/v1/reverse.php";
        }
        url += "?format=json&lat=%f&lon=%f&zoom=18&addressdetails=1";
        if (key != null) {
            url += "&key=" + key;
        }
        if (language != null) {
            url += "&accept-language=" + language;
        }
        return url;
    }

    /**
     * Creates a new LocationIqGeocoder with the specified parameters and resilience configuration.
     *
     * @param client JAX-RS client for HTTP requests
     * @param url URL template with placeholders for coordinates
     * @param key API key for the service (required)
     * @param language Preferred language for results
     * @param cacheSize Size of the local cache (0 to disable)
     * @param addressFormat Format for address display
     * @param resilienceConfig Resilience configuration for circuit breaker and retry
     * @param meterRegistry Registry for metrics collection
     * @param distributedCacheManager Distributed cache manager for cross-service caching
     */
    @Inject
    public LocationIqGeocoder(
            Client client, 
            @Named("geocoder.url") String url,
            @Named("geocoder.key") String key, 
            @Named("geocoder.language") String language,
            @Named("geocoder.cacheSize") int cacheSize, 
            AddressFormat addressFormat,
            ResilienceConfig resilienceConfig,
            MeterRegistry meterRegistry,
            JsonGeocoder.DistributedCacheManager distributedCacheManager) {
        super(client, formatUrl(url, key, language), cacheSize, addressFormat);
        
        if (key == null) {
            throw new IllegalArgumentException("LocationIQ geocoder requires an API key");
        }
        
        // Register User-Agent header filter
        client.register(new ClientRequestFilter() {
            @Override
            public void filter(ClientRequestContext requestContext) {
                requestContext.getHeaders().add("User-Agent", USER_AGENT);
            }
        });
        
        // Set up distributed cache
        if (distributedCacheManager != null) {
            setDistributedCacheManager(distributedCacheManager);
        }
        
        // Set up resilience patterns
        this.circuitBreaker = resilienceConfig.createCircuitBreaker(PROVIDER_NAME);
        this.retry = resilienceConfig.createRetry(PROVIDER_NAME);
        
        // Configure circuit breaker state transitions listener
        this.circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    if (event.getStateTransition() == CircuitBreaker.StateTransition.CLOSED_TO_OPEN) {
                        LOGGER.warn("Circuit breaker for LocationIQ geocoder opened due to failure rate threshold");
                        healthy = false;
                        circuitBreakerOpenCounter.increment();
                    } else if (event.getStateTransition() == CircuitBreaker.StateTransition.OPEN_TO_CLOSED
                            || event.getStateTransition() == CircuitBreaker.StateTransition.HALF_OPEN_TO_CLOSED) {
                        LOGGER.info("Circuit breaker for LocationIQ geocoder closed");
                        healthy = true;
                    }
                });
        
        // Set up metrics
        this.meterRegistry = meterRegistry;
        this.requestTimer = Timer.builder("geocoder.request.duration")
                .tag("provider", PROVIDER_NAME)
                .description("Time taken for geocoding requests")
                .register(meterRegistry);
        this.successCounter = Counter.builder("geocoder.request.success")
                .tag("provider", PROVIDER_NAME)
                .description("Number of successful geocoding requests")
                .register(meterRegistry);
        this.failureCounter = Counter.builder("geocoder.request.failure")
                .tag("provider", PROVIDER_NAME)
                .description("Number of failed geocoding requests")
                .register(meterRegistry);
        this.fallbackCounter = Counter.builder("geocoder.request.fallback")
                .tag("provider", PROVIDER_NAME)
                .description("Number of times fallback was used")
                .register(meterRegistry);
        this.circuitBreakerOpenCounter = Counter.builder("geocoder.circuit_breaker.open")
                .tag("provider", PROVIDER_NAME)
                .description("Number of times circuit breaker opened")
                .register(meterRegistry);
        this.cacheHitCounter = Counter.builder("geocoder.cache.hit")
                .tag("provider", PROVIDER_NAME)
                .description("Number of cache hits")
                .register(meterRegistry);
        
        // Register gauge for circuit breaker state
        meterRegistry.gauge("geocoder.circuit_breaker.state", 
                circuitBreaker, 
                cb -> cb.getState().getOrder());
    }
    
    /**
     * Gets a human-readable address for the specified coordinates with resilience patterns.
     * This implementation adds circuit breaker, retry, and metrics collection.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param callback Optional callback for asynchronous execution
     * @return Address object if executed synchronously, null if using callback
     */
    @Override
    public Address getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        // Check cache first (handled by superclass)
        Address cachedAddress = super.getAddress(latitude, longitude, null);
        if (cachedAddress != null) {
            cacheHitCounter.increment();
            if (callback != null) {
                callback.onSuccess(cachedAddress);
                return null;
            }
            return cachedAddress;
        }
        
        // If circuit breaker is open, use fallback immediately
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            LOGGER.debug("Circuit breaker open, using fallback for LocationIQ geocoder");
            fallbackCounter.increment();
            return handleFallback(latitude, longitude, callback);
        }

        // Create a supplier that will be decorated with resilience patterns
        Supplier<Address> geocodingSupplier = () -> {
            Timer.Sample sample = Timer.start(meterRegistry);
            try {
                Address address = executeGeocodingRequest(latitude, longitude, callback != null);
                sample.stop(requestTimer);
                successCounter.increment();
                return address;
            } catch (Exception e) {
                sample.stop(requestTimer);
                failureCounter.increment();
                LOGGER.warn("LocationIQ geocoding request failed", e);
                throw e;
            }
        };

        // Apply resilience patterns
        Supplier<Address> resilientSupplier = Retry.decorateSupplier(retry, 
                CircuitBreaker.decorateSupplier(circuitBreaker, geocodingSupplier));

        try {
            // Execute the request with resilience patterns
            Address address = resilientSupplier.get();
            
            // Handle asynchronous callback if provided
            if (callback != null && address != null) {
                callback.onSuccess(address);
                return null;
            }
            
            return address;
        } catch (Exception e) {
            LOGGER.error("Geocoding request failed after retries and triggered circuit breaker", e);
            return handleFallback(latitude, longitude, callback);
        }
    }

    /**
     * Executes the actual geocoding request to the LocationIQ service.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param isAsync Whether this is an asynchronous request
     * @return Address object or null if request failed
     */
    private Address executeGeocodingRequest(double latitude, double longitude, boolean isAsync) {
        if (isAsync) {
            // For async requests, we need to return null and handle via CompletableFuture
            CompletableFuture<Address> future = new CompletableFuture<>();
            String formattedUrl = String.format(getUrl(), latitude, longitude);
            
            getClient().target(formattedUrl).request().async().get(new jakarta.ws.rs.client.InvocationCallback<JsonObject>() {
                @Override
                public void completed(JsonObject json) {
                    try {
                        Address address = parseAddress(json);
                        if (address != null) {
                            // Cache the result (handled by superclass)
                            Map.Entry<Double, Double> key = new AbstractMap.SimpleImmutableEntry<>(latitude, longitude);
                            handleResponse(latitude, longitude, json, null);
                            future.complete(address);
                        } else {
                            future.completeExceptionally(new GeocoderException("Empty response from LocationIQ"));
                        }
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                }

                @Override
                public void failed(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
            
            try {
                // Wait for the async operation with a timeout
                return future.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException("Geocoding request timed out or failed", e);
            }
        } else {
            // For synchronous requests, use the parent implementation
            return super.getAddress(latitude, longitude, null);
        }
    }

    /**
     * Handles fallback when geocoding fails or circuit breaker is open.
     * This provides a graceful degradation strategy.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param callback Optional callback for asynchronous execution
     * @return Fallback address or null
     */
    private Address handleFallback(double latitude, double longitude, ReverseGeocoderCallback callback) {
        fallbackCounter.increment();
        LOGGER.debug("Using fallback for LocationIQ geocoder at {}, {}", latitude, longitude);
        
        // Create a minimal fallback address with just coordinates
        Address fallbackAddress = new Address();
        fallbackAddress.setFormattedAddress(String.format("%.6f, %.6f", latitude, longitude));
        
        if (callback != null) {
            callback.onSuccess(fallbackAddress);
            return null;
        }
        return fallbackAddress;
    }

    /**
     * Parses the JSON response from LocationIQ into an Address object.
     * LocationIQ uses the same response format as Nominatim.
     *
     * @param json JSON response from the service
     * @return Address object or null if parsing failed
     */
    @Override
    public Address parseAddress(JsonObject json) {
        JsonObject result = json.getJsonObject("address");

        if (result != null) {
            Address address = new Address();

            if (json.containsKey("display_name")) {
                address.setFormattedAddress(json.getString("display_name"));
            }

            if (result.containsKey("house_number")) {
                address.setHouse(result.getString("house_number"));
            }
            if (result.containsKey("road")) {
                address.setStreet(result.getString("road"));
            }
            if (result.containsKey("suburb")) {
                address.setSuburb(result.getString("suburb"));
            }

            if (result.containsKey("village")) {
                address.setSettlement(result.getString("village"));
            } else if (result.containsKey("town")) {
                address.setSettlement(result.getString("town"));
            } else if (result.containsKey("city")) {
                address.setSettlement(result.getString("city"));
            }

            if (result.containsKey("state_district")) {
                address.setDistrict(result.getString("state_district"));
            } else if (result.containsKey("region")) {
                address.setDistrict(result.getString("region"));
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

    /**
     * Parses error information from the JSON response.
     * LocationIQ provides error details in the 'error' field.
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
     * Reports the health status of the geocoder.
     * Used by the health check system for service discovery and monitoring.
     *
     * @return true if the geocoder is healthy and operational, false otherwise
     */
    @Override
    public boolean isHealthy() {
        return healthy;
    }

    /**
     * Gets the URL template used for geocoding requests.
     *
     * @return URL template string
     */
    protected String getUrl() {
        return super.url;
    }

    /**
     * Gets the JAX-RS client used for HTTP requests.
     *
     * @return Client instance
     */
    protected Client getClient() {
        return super.client;
    }

    /**
     * Helper method to handle response caching and processing.
     * Used by the async implementation.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param json JSON response from the service
     * @param callback Optional callback for asynchronous execution
     * @return Address object or null
     */
    private Address handleResponse(double latitude, double longitude, JsonObject json, ReverseGeocoderCallback callback) {
        Address address = parseAddress(json);
        
        if (address != null) {
            if (getAddressFormat() != null) {
                address.setFormattedAddress(getAddressFormat().format(address));
            }
            
            // Cache the result in distributed cache
            if (getDistributedCacheManager() != null) {
                String cacheKey = String.format("%s-%f-%f", getClass().getSimpleName(), latitude, longitude);
                getDistributedCacheManager().putAddress(cacheKey, address);
            }
            
            if (callback != null) {
                callback.onSuccess(address);
            }
        } else {
            String error = parseError(json);
            if (error != null) {
                LOGGER.warn("LocationIQ geocoder error: {}", error);
            } else {
                LOGGER.warn("Empty LocationIQ geocoder response");
            }
            if (callback != null) {
                callback.onFailure(new GeocoderException(error));
            }
        }
        
        return address;
    }

    /**
     * Gets the address format used for formatting addresses.
     *
     * @return AddressFormat instance
     */
    protected AddressFormat getAddressFormat() {
        return super.addressFormat;
    }

    /**
     * Gets the distributed cache manager used for cross-service caching.
     *
     * @return DistributedCacheManager instance
     */
    protected DistributedCacheManager getDistributedCacheManager() {
        return super.distributedCacheManager;
    }
}