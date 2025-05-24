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
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.health.Status;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class GeocoderTest {

    private ResilienceConfig resilienceConfig;
    private MeterRegistry meterRegistry;

    @Mock
    private Client client;

    @Mock
    private WebTarget webTarget;

    @Mock
    private Invocation.Builder requestBuilder;

    @BeforeEach
    public void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        resilienceConfig = new ResilienceConfig(meterRegistry);

        // Setup mock client
        when(client.target(anyString())).thenReturn(webTarget);
        when(webTarget.request()).thenReturn(requestBuilder);
    }

    /**
     * Test class that extends JsonGeocoder for testing purposes
     */
    private static class TestGeocoder extends JsonGeocoder {

        public TestGeocoder(Client client, String url, int cacheSize, AddressFormat addressFormat,
                           CircuitBreakerRegistry circuitBreakerRegistry, RetryRegistry retryRegistry,
                           MeterRegistry meterRegistry) {
            super(client, url, cacheSize, addressFormat, circuitBreakerRegistry, retryRegistry,
                    meterRegistry, "testGeocoder", true);
        }

        @Override
        public Address parseAddress(JsonObject json) {
            Address address = new Address();
            if (json.containsKey("address")) {
                address.setFormattedAddress(json.getString("address"));
            }
            return address;
        }
    }

    /**
     * Test class that implements Geocoder for testing fallback functionality
     */
    private static class FallbackGeocoder implements Geocoder {

        @Override
        public CompletableFuture<String> getAddress(double latitude, double longitude) {
            return CompletableFuture.completedFuture("Fallback Address");
        }

        @Override
        public CompletableFuture<String> getAddress(double latitude, double longitude, boolean useFallback) {
            return getAddress(latitude, longitude);
        }

        @Override
        public boolean isHealthy() {
            return true;
        }

        @Override
        public GeocoderHealthStatus getHealthStatus() {
            return new GeocoderHealthStatus(true, "Fallback Geocoder", 100);
        }

        @Override
        public boolean registerWithServiceDiscovery(String serviceId) {
            return true;
        }

        @Override
        public boolean deregisterFromServiceDiscovery(String serviceId) {
            return true;
        }

        @Override
        public String resolveServiceEndpoint(String serviceType) {
            return "http://fallback-geocoder.example.com";
        }

        @Override
        public GeocoderMetrics getMetrics() {
            return new GeocoderMetrics(0, 0, 0, 0);
        }

        @Override
        public boolean resetCircuitBreaker() {
            return true;
        }

        @Override
        public void configureRetryPolicy(int maxRetries, long initialDelayMs, long maxDelayMs) {
            // No-op for test implementation
        }

        @Override
        public void configureCircuitBreaker(int failureThreshold, long resetTimeoutMs) {
            // No-op for test implementation
        }

        @Override
        public void setFallbackGeocoder(Geocoder fallbackGeocoder) {
            // No-op for test implementation
        }

        @Override
        public Geocoder getFallbackGeocoder() {
            return null;
        }
    }

    @Test
    public void testSuccessfulGeocoding() throws ExecutionException, InterruptedException {
        // Setup mock response
        JsonObject response = Json.createObjectBuilder()
                .add("address", "123 Test Street, Test City")
                .build();
        when(requestBuilder.get(JsonObject.class)).thenReturn(response);

        // Create geocoder with resilience config
        TestGeocoder geocoder = new TestGeocoder(
                client,
                "http://test.geocoder.com/reverse?lat=%.6f&lon=%.6f",
                10,
                new AddressFormat(),
                resilienceConfig.getCircuitBreakerRegistry(),
                resilienceConfig.getRetryRegistry(),
                meterRegistry);

        // Test geocoding
        CompletableFuture<String> future = geocoder.getAddress(1.0, 1.0);
        String address = future.get();

        // Verify
        assertEquals("123 Test Street, Test City", address);
        verify(client).target(contains("http://test.geocoder.com/reverse"));
    }

    @Test
    public void testCircuitBreakerFunctionality() throws Exception {
        // Configure circuit breaker with low threshold for testing
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(2)
                .minimumNumberOfCalls(2)
                .waitDurationInOpenState(Duration.ofMillis(500))
                .build();

        CircuitBreakerRegistry testRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        CircuitBreaker circuitBreaker = testRegistry.circuitBreaker("testGeocoder");

        // Configure retry with no retries for this test
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(1)
                .build();
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);

        // Setup mock to throw exceptions
        when(requestBuilder.get(JsonObject.class)).thenThrow(new RuntimeException("Service unavailable"));

        // Create geocoder with test circuit breaker
        TestGeocoder geocoder = new TestGeocoder(
                client,
                "http://test.geocoder.com/reverse?lat=%.6f&lon=%.6f",
                10,
                new AddressFormat(),
                testRegistry,
                retryRegistry,
                meterRegistry);

        // First call - should fail but circuit still closed
        CompletableFuture<String> future1 = geocoder.getAddress(1.0, 1.0);
        assertThrows(Exception.class, () -> future1.get(1, TimeUnit.SECONDS));
        assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getState());

        // Second call - should fail and open the circuit
        CompletableFuture<String> future2 = geocoder.getAddress(1.0, 1.0);
        assertThrows(Exception.class, () -> future2.get(1, TimeUnit.SECONDS));
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getState());

        // Third call - circuit is open, should fail fast
        long startTime = System.currentTimeMillis();
        CompletableFuture<String> future3 = geocoder.getAddress(1.0, 1.0);
        assertThrows(Exception.class, () -> future3.get(1, TimeUnit.SECONDS));
        long endTime = System.currentTimeMillis();

        // Verify fast failure (should be much less than 1 second)
        assertTrue((endTime - startTime) < 100, "Circuit breaker should fail fast when open");

        // Wait for circuit to transition to half-open
        Thread.sleep(600); // Wait longer than waitDurationInOpenState

        // Setup mock to return success now
        JsonObject response = Json.createObjectBuilder()
                .add("address", "123 Test Street, Test City")
                .build();
        when(requestBuilder.get(JsonObject.class)).thenReturn(response);

        // Next call should succeed and close the circuit
        CompletableFuture<String> future4 = geocoder.getAddress(1.0, 1.0);
        assertEquals("123 Test Street, Test City", future4.get(1, TimeUnit.SECONDS));

        // Circuit should be closed again after successful call
        assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getState());
    }

    @Test
    public void testRetryMechanismWithExponentialBackoff() throws Exception {
        // Configure retry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(100))
                .enableExponentialBackoff(true)
                .exponentialBackoffMultiplier(2.0)
                .build();
        RetryRegistry testRetryRegistry = RetryRegistry.of(retryConfig);

        // Use default circuit breaker config
        CircuitBreakerRegistry testCircuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();

        // Counter to track number of attempts
        AtomicInteger attemptCount = new AtomicInteger(0);

        // Setup mock to fail twice then succeed
        when(requestBuilder.get(JsonObject.class)).thenAnswer(invocation -> {
            int count = attemptCount.incrementAndGet();
            if (count < 3) {
                throw new RuntimeException("Temporary failure");
            } else {
                return Json.createObjectBuilder()
                        .add("address", "123 Test Street, Test City")
                        .build();
            }
        });

        // Create geocoder with test retry config
        TestGeocoder geocoder = new TestGeocoder(
                client,
                "http://test.geocoder.com/reverse?lat=%.6f&lon=%.6f",
                10,
                new AddressFormat(),
                testCircuitBreakerRegistry,
                testRetryRegistry,
                meterRegistry);

        // Test geocoding with retry
        long startTime = System.currentTimeMillis();
        CompletableFuture<String> future = geocoder.getAddress(1.0, 1.0);
        String address = future.get(2, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();

        // Verify
        assertEquals("123 Test Street, Test City", address);
        assertEquals(3, attemptCount.get(), "Should have attempted 3 times");

        // Verify timing - should be at least the sum of wait durations (100ms + 200ms)
        long minExpectedDuration = 300; // 100ms initial wait + 200ms second wait
        assertTrue((endTime - startTime) >= minExpectedDuration,
                "Retry should have exponential backoff delay");
    }

    @Test
    public void testFallbackStrategy() throws Exception {
        // Configure circuit breaker to open immediately
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(1)
                .minimumNumberOfCalls(1)
                .build();

        CircuitBreakerRegistry testRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        RetryRegistry retryRegistry = RetryRegistry.ofDefaults();

        // Setup mock to throw exception
        when(requestBuilder.get(JsonObject.class)).thenThrow(new RuntimeException("Service unavailable"));

        // Create primary geocoder
        TestGeocoder primaryGeocoder = new TestGeocoder(
                client,
                "http://test.geocoder.com/reverse?lat=%.6f&lon=%.6f",
                10,
                new AddressFormat(),
                testRegistry,
                retryRegistry,
                meterRegistry);

        // Create fallback geocoder
        Geocoder fallbackGeocoder = new FallbackGeocoder();

        // Set fallback geocoder
        primaryGeocoder.setFallbackGeocoder(fallbackGeocoder);

        // First call to open the circuit
        CompletableFuture<String> future1 = primaryGeocoder.getAddress(1.0, 1.0);
        assertThrows(Exception.class, () -> future1.get(1, TimeUnit.SECONDS));

        // Second call should use fallback
        CompletableFuture<String> future2 = primaryGeocoder.getAddress(1.0, 1.0, true);
        String address = future2.get(1, TimeUnit.SECONDS);

        // Verify fallback was used
        assertEquals("Fallback Address", address);
    }

    @Test
    public void testDistributedCache() throws Exception {
        // Setup mock response
        JsonObject response = Json.createObjectBuilder()
                .add("address", "123 Test Street, Test City")
                .build();
        when(requestBuilder.get(JsonObject.class)).thenReturn(response);

        // Create geocoder with cache
        TestGeocoder geocoder = new TestGeocoder(
                client,
                "http://test.geocoder.com/reverse?lat=%.6f&lon=%.6f",
                10, // Enable caching with size 10
                new AddressFormat(),
                resilienceConfig.getCircuitBreakerRegistry(),
                resilienceConfig.getRetryRegistry(),
                meterRegistry);

        // First call - should hit the service
        CompletableFuture<String> future1 = geocoder.getAddress(1.0, 1.0);
        String address1 = future1.get();
        assertEquals("123 Test Street, Test City", address1);

        // Reset mock to verify it's not called again
        reset(requestBuilder);
        when(requestBuilder.get(JsonObject.class)).thenThrow(new RuntimeException("Should not be called"));

        // Second call with same coordinates - should use cache
        CompletableFuture<String> future2 = geocoder.getAddress(1.0, 1.0);
        String address2 = future2.get();
        assertEquals("123 Test Street, Test City", address2);

        // Verify the service was not called again
        verify(requestBuilder, never()).get(JsonObject.class);
    }

    @Test
    public void testMetricsCollection() throws Exception {
        // Setup mock response
        JsonObject response = Json.createObjectBuilder()
                .add("address", "123 Test Street, Test City")
                .build();
        when(requestBuilder.get(JsonObject.class)).thenReturn(response);

        // Create geocoder with metrics
        TestGeocoder geocoder = new TestGeocoder(
                client,
                "http://test.geocoder.com/reverse?lat=%.6f&lon=%.6f",
                10,
                new AddressFormat(),
                resilienceConfig.getCircuitBreakerRegistry(),
                resilienceConfig.getRetryRegistry(),
                meterRegistry);

        // Make a successful call
        CompletableFuture<String> future = geocoder.getAddress(1.0, 1.0);
        future.get();

        // Verify metrics were recorded
        assertTrue(meterRegistry.find("geocoder.request.time").timer() != null,
                "Geocoder request time metric should be registered");

        // Check circuit breaker metrics
        Map<String, Object> metrics = geocoder.getMetrics();
        assertNotNull(metrics);
        assertTrue(metrics.containsKey("numberOfSuccessfulCalls"));
        assertEquals(1L, metrics.get("numberOfSuccessfulCalls"));
    }

    @Test
    public void testGeocoderHealthIndicator() throws Exception {
        // Setup mock response
        JsonObject response = Json.createObjectBuilder()
                .add("address", "123 Test Street, Test City")
                .build();
        when(requestBuilder.get(JsonObject.class)).thenReturn(response);

        // Create geocoder
        TestGeocoder geocoder = new TestGeocoder(
                client,
                "http://test.geocoder.com/reverse?lat=%.6f&lon=%.6f",
                10,
                new AddressFormat(),
                resilienceConfig.getCircuitBreakerRegistry(),
                resilienceConfig.getRetryRegistry(),
                meterRegistry);

        // Create health indicator
        GeocoderHealthIndicator healthIndicator = new GeocoderHealthIndicator(geocoder, meterRegistry);

        // Check health
        assertEquals(Status.UP, healthIndicator.health().getStatus());

        // Now make the geocoder fail
        reset(requestBuilder);
        when(requestBuilder.get(JsonObject.class)).thenThrow(new RuntimeException("Service unavailable"));

        // Check health again
        assertEquals(Status.DOWN, healthIndicator.health().getStatus());
    }
}