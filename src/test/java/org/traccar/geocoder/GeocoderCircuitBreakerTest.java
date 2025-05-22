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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.traccar.database.StatisticsManager;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.*;

public class GeocoderCircuitBreakerTest {

    @Mock
    private Geocoder mockGeocoder;

    @Mock
    private StatisticsManager mockStatisticsManager;

    private GeocoderCircuitBreaker geocoderCircuitBreaker;
    private AutoCloseable closeable;

    @BeforeEach
    public void setUp() {
        closeable = MockitoAnnotations.openMocks(this);

        // Configure circuit breaker with fast testing settings
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofMillis(500)) // Short duration for testing
                .slidingWindowSize(5)
                .minimumNumberOfCalls(2)
                .permittedNumberOfCallsInHalfOpenState(2)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();

        geocoderCircuitBreaker = new GeocoderCircuitBreaker(mockGeocoder, config, "Fallback Address");
        geocoderCircuitBreaker.setStatisticsManager(mockStatisticsManager);
    }

    @AfterEach
    public void tearDown() throws Exception {
        geocoderCircuitBreaker.close();
        closeable.close();
    }

    @Test
    public void testSuccessfulGeocoding() {
        // Setup
        when(mockGeocoder.getAddress(anyDouble(), anyDouble(), any())).thenReturn("Test Address");

        // Execute
        String result = geocoderCircuitBreaker.getAddress(1.0, 1.0, null);

        // Verify
        assertEquals("Test Address", result);
        verify(mockGeocoder).getAddress(1.0, 1.0, null);
        assertEquals(CircuitBreaker.State.CLOSED, geocoderCircuitBreaker.getCircuitBreakerState());
    }

    @Test
    public void testFailedGeocoding() {
        // Setup
        when(mockGeocoder.getAddress(anyDouble(), anyDouble(), any()))
                .thenThrow(new GeocoderException("Test Exception"));

        // Execute
        String result = geocoderCircuitBreaker.getAddress(1.0, 1.0, null);

        // Verify
        assertEquals("Fallback Address", result);
        verify(mockGeocoder).getAddress(1.0, 1.0, null);
    }

    @Test
    public void testCircuitBreakerOpens() {
        // Setup - make the geocoder fail consistently
        when(mockGeocoder.getAddress(anyDouble(), anyDouble(), any()))
                .thenThrow(new GeocoderException("Test Exception"));

        // Execute - call enough times to open the circuit
        for (int i = 0; i < 5; i++) {
            geocoderCircuitBreaker.getAddress(1.0, 1.0, null);
        }

        // Verify
        assertEquals(CircuitBreaker.State.OPEN, geocoderCircuitBreaker.getCircuitBreakerState());

        // Reset call count
        clearInvocations(mockGeocoder);

        // Execute - one more call when circuit is open
        String result = geocoderCircuitBreaker.getAddress(1.0, 1.0, null);

        // Verify - should use fallback without calling geocoder
        assertEquals("Fallback Address", result);
        verify(mockGeocoder, never()).getAddress(anyDouble(), anyDouble(), any());
    }

    @Test
    public void testAsyncGeocoding() throws Exception {
        // Setup
        when(mockGeocoder.getAddress(anyDouble(), anyDouble(), any()))
                .thenReturn("Async Test Address");

        // Create a latch to wait for async completion
        CountDownLatch latch = new CountDownLatch(1);
        final String[] result = new String[1];

        // Execute
        geocoderCircuitBreaker.getAddress(1.0, 1.0, new Geocoder.ReverseGeocoderCallback() {
            @Override
            public void onSuccess(String address) {
                result[0] = address;
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable e) {
                fail("Should not fail");
                latch.countDown();
            }
        });

        // Wait for async completion
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        // Verify
        assertEquals("Async Test Address", result[0]);
    }

    @Test
    public void testAsyncGeocodingFailure() throws Exception {
        // Setup
        when(mockGeocoder.getAddress(anyDouble(), anyDouble(), any()))
                .thenThrow(new GeocoderException("Async Test Exception"));

        // Create a latch to wait for async completion
        CountDownLatch latch = new CountDownLatch(1);
        final String[] result = new String[1];

        // Execute
        geocoderCircuitBreaker.getAddress(1.0, 1.0, new Geocoder.ReverseGeocoderCallback() {
            @Override
            public void onSuccess(String address) {
                result[0] = address;
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable e) {
                result[0] = "Error: " + e.getMessage();
                latch.countDown();
            }
        });

        // Wait for async completion
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        // Verify - should get fallback address
        assertEquals("Fallback Address", result[0]);
    }

    @Test
    public void testHealthCheck() {
        // Setup
        when(mockGeocoder.isHealthy()).thenReturn(true);

        // Execute
        boolean healthy = geocoderCircuitBreaker.isHealthy();

        // Verify
        assertTrue(healthy);
        Map<String, Object> healthDetails = geocoderCircuitBreaker.getHealthDetails();
        assertEquals("CLOSED", healthDetails.get("circuitBreakerState"));
    }

    @Test
    public void testMetrics() {
        // Setup - make a successful call
        when(mockGeocoder.getAddress(anyDouble(), anyDouble(), any())).thenReturn("Test Address");
        geocoderCircuitBreaker.getAddress(1.0, 1.0, null);

        // Execute
        Map<String, Double> metrics = geocoderCircuitBreaker.getMetrics();

        // Verify
        assertNotNull(metrics);
        assertTrue(metrics.containsKey("circuitbreaker.number.of.successful.calls"));
        assertEquals(1.0, metrics.get("circuitbreaker.number.of.successful.calls"));
    }

    @Test
    public void testCircuitBreakerReset() {
        // Setup - make the geocoder fail consistently to open the circuit
        when(mockGeocoder.getAddress(anyDouble(), anyDouble(), any()))
                .thenThrow(new GeocoderException("Test Exception"));

        // Execute - call enough times to open the circuit
        for (int i = 0; i < 5; i++) {
            geocoderCircuitBreaker.getAddress(1.0, 1.0, null);
        }

        // Verify circuit is open
        assertEquals(CircuitBreaker.State.OPEN, geocoderCircuitBreaker.getCircuitBreakerState());

        // Reset the circuit breaker
        geocoderCircuitBreaker.resetCircuitBreaker();

        // Verify circuit is closed again
        assertEquals(CircuitBreaker.State.CLOSED, geocoderCircuitBreaker.getCircuitBreakerState());
    }

    @Test
    public void testServiceDiscoveryIntegration() {
        // Setup
        Map<String, String> metadata = Map.of("version", "1.0");

        // Execute
        geocoderCircuitBreaker.registerWithServiceDiscovery("geocoder-service", metadata);

        // Verify - should add circuit breaker info to metadata
        verify(mockGeocoder).registerWithServiceDiscovery(eq("geocoder-service"), argThat(m -> 
                m.containsKey("circuitBreaker") && 
                m.containsKey("circuitBreakerState") &&
                m.get("version").equals("1.0")));
    }
}