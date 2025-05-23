/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.io.StringReader;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MapTilerGeocoderTest {

    private Client client;
    private WebTarget webTarget;
    private Invocation.Builder requestBuilder;
    private ResilienceConfig resilienceConfig;
    private MeterRegistry meterRegistry;

    @BeforeEach
    public void setUp() {
        client = mock(Client.class);
        webTarget = mock(WebTarget.class);
        requestBuilder = mock(Invocation.Builder.class);

        when(client.target(anyString())).thenReturn(webTarget);
        when(webTarget.request()).thenReturn(requestBuilder);

        // Create a real ResilienceConfig with test-specific settings
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(
                CircuitBreakerConfig.custom()
                        .failureRateThreshold(50)
                        .waitDurationInOpenState(Duration.ofMillis(100)) // Short duration for tests
                        .slidingWindowSize(5)
                        .minimumNumberOfCalls(1) // Only need 1 call for testing
                        .build());

        RetryRegistry retryRegistry = RetryRegistry.of(
                RetryConfig.custom()
                        .maxAttempts(2)
                        .waitDuration(Duration.ofMillis(50))
                        .build());

        resilienceConfig = new ResilienceConfig(circuitBreakerRegistry, retryRegistry);
        meterRegistry = new SimpleMeterRegistry();
    }

    @Test
    public void testParseValidResponse() {
        // Sample response from MapTiler API
        String response = "{\"features\":[{\"properties\":{\"country\":\"Switzerland\",\"country_code\":\"ch\",\"region\":\"Zurich\",\"locality\":\"Zurich\",\"district\":\"Kreis 1\",\"street\":\"Bahnhofstrasse\",\"housenumber\":\"1\",\"postcode\":\"8001\"}}]}";
        JsonReader reader = Json.createReader(new StringReader(response));
        JsonObject json = reader.readObject();

        when(requestBuilder.get(ArgumentMatchers.<Class<JsonObject>>any())).thenReturn(json);

        MapTilerGeocoder geocoder = new MapTilerGeocoder(
                client, "https://api.maptiler.com/geocoding/{0},{1}.json", "test_key", 0,
                resilienceConfig, meterRegistry);

        Address address = geocoder.getAddress(47.3769, 8.5417, null);

        assertNotNull(address);
        assertEquals("Switzerland", address.getCountry());
        assertEquals("ch", address.getCountryCode());
        assertEquals("Zurich", address.getState());
        assertEquals("Zurich", address.getSettlement());
        assertEquals("Kreis 1", address.getDistrict());
        assertEquals("Bahnhofstrasse", address.getStreet());
        assertEquals("1", address.getHouse());
        assertEquals("8001", address.getPostcode());
    }

    @Test
    public void testParseEmptyResponse() {
        // Empty response with no features
        String response = "{\"features\":[]}";
        JsonReader reader = Json.createReader(new StringReader(response));
        JsonObject json = reader.readObject();

        when(requestBuilder.get(ArgumentMatchers.<Class<JsonObject>>any())).thenReturn(json);

        MapTilerGeocoder geocoder = new MapTilerGeocoder(
                client, "https://api.maptiler.com/geocoding/{0},{1}.json", "test_key", 0,
                resilienceConfig, meterRegistry);

        Address address = geocoder.getAddress(47.3769, 8.5417, null);

        assertNull(address);
    }

    @Test
    public void testParseError() {
        // Error response
        String response = "{\"error\":\"Invalid API key\"}";
        JsonReader reader = Json.createReader(new StringReader(response));
        JsonObject json = reader.readObject();

        when(requestBuilder.get(ArgumentMatchers.<Class<JsonObject>>any())).thenReturn(json);

        MapTilerGeocoder geocoder = new MapTilerGeocoder(
                client, "https://api.maptiler.com/geocoding/{0},{1}.json", "test_key", 0,
                resilienceConfig, meterRegistry);

        Address address = geocoder.getAddress(47.3769, 8.5417, null);

        assertNull(address);
    }

    @Test
    public void testFallbackWhenCircuitBreakerOpen() {
        // Force circuit breaker to open
        CircuitBreaker circuitBreaker = resilienceConfig.createCircuitBreaker("maptiler");
        circuitBreaker.transitionToOpenState();

        MapTilerGeocoder geocoder = new MapTilerGeocoder(
                client, "https://api.maptiler.com/geocoding/{0},{1}.json", "test_key", 0,
                resilienceConfig, meterRegistry);

        Address address = geocoder.getAddress(47.3769, 8.5417, null);

        // Should return fallback address with coordinates
        assertNotNull(address);
        assertEquals("47.376900, 8.541700", address.getFormattedAddress());
    }
}