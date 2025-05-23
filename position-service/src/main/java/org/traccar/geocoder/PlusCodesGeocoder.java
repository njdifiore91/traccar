/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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

import com.google.openlocationcode.OpenLocationCode;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

/**
 * Plus Codes geocoder implementation using Google's Open Location Code library.
 * This implementation includes circuit breaker, retry, and metrics collection.
 */
public class PlusCodesGeocoder implements Geocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlusCodesGeocoder.class);

    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Timer geocodingTimer;
    private final ConcurrentMap<String, Address> cache;

    /**
     * Create Plus Codes geocoder.
     *
     * @param meterRegistry Meter registry for metrics collection
     */
    public PlusCodesGeocoder(MeterRegistry meterRegistry) {
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .recordExceptions(Exception.class)
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        circuitBreaker = circuitBreakerRegistry.circuitBreaker("plusCodesGeocoder");
        
        // Register circuit breaker event listeners for logging
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker state changed from {} to {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()))
                .onError(event -> LOGGER.warn("Circuit breaker recorded error: {}", event.getThrowable().getMessage()));

        // Configure retry policy
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(TimeoutException.class)
                .build();

        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        retry = retryRegistry.retry("plusCodesGeocoder");

        // Register metrics
        geocodingTimer = meterRegistry.timer("geocoder.pluscodes.request.time");
        meterRegistry.gauge("geocoder.pluscodes.circuit_breaker.state", circuitBreaker, cb -> cb.getState().getOrder());
        meterRegistry.gauge("geocoder.pluscodes.cache.size", this, pg -> pg.cache.size());

        // Initialize cache
        cache = new ConcurrentHashMap<>();
    }

    @Override
    public Address getAddress(double latitude, double longitude, int zoom) {
        // Generate Plus Code from coordinates
        String plusCode = generatePlusCode(latitude, longitude);
        
        // Check cache first
        Address cachedAddress = cache.get(plusCode);
        if (cachedAddress != null) {
            return cachedAddress;
        }

        // Use timer to measure geocoding performance
        return geocodingTimer.record(() -> {
            try {
                // Apply circuit breaker and retry patterns
                Address address = CircuitBreaker.decorateSupplier(circuitBreaker,
                        () -> Retry.decorateSupplier(retry,
                                () -> geocodeWithPlusCode(plusCode, latitude, longitude)).get()).get();
                
                // Cache the result
                if (address != null) {
                    cache.put(plusCode, address);
                }
                
                return address;
            } catch (Exception e) {
                LOGGER.warn("Plus Codes geocoding failed: {}", e.getMessage());
                return getFallbackAddress(latitude, longitude);
            }
        });
    }

    @Override
    public CompletableFuture<Address> getAddressAsync(double latitude, double longitude, int zoom) {
        return CompletableFuture.supplyAsync(() -> getAddress(latitude, longitude, zoom));
    }

    /**
     * Generate a Plus Code from latitude and longitude coordinates.
     *
     * @param latitude  Latitude coordinate
     * @param longitude Longitude coordinate
     * @return Plus Code string
     */
    private String generatePlusCode(double latitude, double longitude) {
        try {
            // Generate a full Plus Code with 10 digits precision
            return OpenLocationCode.encode(latitude, longitude);
        } catch (Exception e) {
            LOGGER.error("Failed to generate Plus Code: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Geocode a location using Plus Code.
     *
     * @param plusCode  Plus Code to geocode
     * @param latitude  Original latitude (for fallback)
     * @param longitude Original longitude (for fallback)
     * @return Address object or null if geocoding failed
     */
    private Address geocodeWithPlusCode(String plusCode, double latitude, double longitude) {
        if (plusCode == null) {
            return null;
        }

        try {
            // Decode the Plus Code to get the area information
            OpenLocationCode.CodeArea codeArea = OpenLocationCode.decode(plusCode);
            
            // Create an address with the Plus Code as the formatted address
            Address address = new Address();
            address.setFormattedAddress(plusCode);
            
            // Set the center coordinates of the code area
            address.setLatitude(codeArea.getCenterLatitude());
            address.setLongitude(codeArea.getCenterLongitude());
            
            return address;
        } catch (Exception e) {
            LOGGER.warn("Failed to decode Plus Code: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Provide a fallback address when geocoding fails.
     *
     * @param latitude  Original latitude
     * @param longitude Original longitude
     * @return Basic address with coordinates
     */
    private Address getFallbackAddress(double latitude, double longitude) {
        Address address = new Address();
        address.setLatitude(latitude);
        address.setLongitude(longitude);
        address.setFormattedAddress(String.format("%.6f, %.6f", latitude, longitude));
        return address;
    }
}