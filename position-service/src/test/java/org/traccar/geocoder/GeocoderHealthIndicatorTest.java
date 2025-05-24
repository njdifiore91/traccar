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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link GeocoderHealthIndicator} that verify health monitoring functionality
 * for geocoding services in the microservices architecture.
 */
@ExtendWith(MockitoExtension.class)
public class GeocoderHealthIndicatorTest {

    @Mock
    private Geocoder geocoder;

    @Mock
    private MeterRegistry meterRegistry;

    @Mock
    private Counter counter;

    @Mock
    private Timer timer;

    private GeocoderHealthIndicator healthIndicator;

    @BeforeEach
    public void setUp() {
        // Mock the geocoder class name for provider name extraction
        when(geocoder.getClass()).thenReturn((Class) MockGeocoder.class);
        
        // Mock meter registry gauge method
        when(meterRegistry.gauge(eq("geocoder.health.status"), anyList(), any(), any()))
                .thenReturn(1.0);
        
        // Create the health indicator with mocked dependencies
        healthIndicator = new GeocoderHealthIndicator(geocoder, meterRegistry);
    }

    @Test
    public void testHealthCheckSuccess() {
        // Arrange
        mockGeocoderSuccess("123 Test Street, Test City");

        // Act
        Health health = healthIndicator.health();

        // Assert
        assertEquals(Status.UP, health.getStatus());
        assertEquals("mock", health.getDetails().get("provider"));
        assertTrue(health.getDetails().containsKey("testCoordinates"));
        assertTrue(health.getDetails().containsKey("responseTime"));
        
        // Verify status is tracked correctly
        assertEquals(Status.UP, healthIndicator.getCurrentStatus());
        assertEquals("", healthIndicator.getLastError());
    }

    @Test
    public void testHealthCheckFailureWithEmptyAddress() {
        // Arrange
        mockGeocoderSuccess(""); // Empty address

        // Act
        Health health = healthIndicator.health();

        // Assert
        assertEquals(Status.DOWN, health.getStatus());
        assertEquals("mock", health.getDetails().get("provider"));
        assertEquals("Empty address returned from geocoder", health.getDetails().get("error"));
        
        // Verify status is tracked correctly
        assertEquals(Status.DOWN, healthIndicator.getCurrentStatus());
        assertEquals("Empty address returned from geocoder", healthIndicator.getLastError());
    }

    @Test
    public void testHealthCheckFailureWithException() {
        // Arrange
        mockGeocoderFailure(new RuntimeException("Service unavailable"));

        // Act
        Health health = healthIndicator.health();

        // Assert
        assertEquals(Status.DOWN, health.getStatus());
        assertEquals("mock", health.getDetails().get("provider"));
        assertEquals("Service unavailable", health.getDetails().get("error"));
        
        // Verify status is tracked correctly
        assertEquals(Status.DOWN, healthIndicator.getCurrentStatus());
        assertEquals("Service unavailable", healthIndicator.getLastError());
    }

    @Test
    public void testHealthCheckFailureWithTimeout() {
        // Arrange
        // Create a health indicator with a very short timeout
        healthIndicator = new GeocoderHealthIndicator(geocoder, meterRegistry)
                .withTimeout(1); // 1ms timeout
        
        // Mock geocoder to simulate timeout by not calling any callback
        doAnswer(invocation -> {
            // Don't call any callback to simulate timeout
            return null;
        }).when(geocoder).getAddress(anyDouble(), anyDouble(), any(Geocoder.ReverseGeocoderCallback.class));

        // Act
        Health health = healthIndicator.health();

        // Assert
        assertEquals(Status.DOWN, health.getStatus());
        assertTrue(health.getDetails().containsKey("error"));
        String error = (String) health.getDetails().get("error");
        assertTrue(error.contains("Timeout") || error.contains("timeout") || error.contains("timed out"),
                "Error should mention timeout: " + error);
    }

    @Test
    public void testCustomTestCoordinates() {
        // Arrange
        double testLat = 51.5074;
        double testLon = -0.1278;
        healthIndicator = healthIndicator.withTestCoordinates(testLat, testLon);
        mockGeocoderSuccess("London, UK");

        // Act
        Health health = healthIndicator.health();

        // Assert
        assertEquals(Status.UP, health.getStatus());
        String coordinates = (String) health.getDetails().get("testCoordinates");
        assertTrue(coordinates.contains(String.valueOf(testLat)) && coordinates.contains(String.valueOf(testLon)),
                "Custom coordinates should be used in health check");
    }

    @Test
    public void testCustomTimeout() {
        // Arrange
        long customTimeout = 10000; // 10 seconds
        healthIndicator = healthIndicator.withTimeout(customTimeout);
        mockGeocoderSuccess("Test Address");

        // Act
        Health health = healthIndicator.health();

        // Assert
        assertEquals(Status.UP, health.getStatus());
        String responseTime = (String) health.getDetails().get("responseTime");
        assertTrue(responseTime.contains(String.valueOf(customTimeout)),
                "Custom timeout should be reflected in health check");
    }

    @Test
    public void testMetricsRegistration() {
        // Verify that metrics are registered with the meter registry
        ArgumentCaptor<List<Tag>> tagsCaptor = ArgumentCaptor.forClass(List.class);
        
        verify(meterRegistry).gauge(eq("geocoder.health.status"), tagsCaptor.capture(), any(), any());
        
        // Verify provider tag is included
        List<Tag> tags = tagsCaptor.getValue();
        boolean hasProviderTag = tags.stream()
                .anyMatch(tag -> "provider".equals(tag.getKey()) && "mock".equals(tag.getValue()));
        
        assertTrue(hasProviderTag, "Metrics should include provider tag");
    }

    @Test
    public void testStatusChangeMetrics() {
        // Arrange
        when(meterRegistry.counter(eq("geocoder.health.status.change"), anyList()))
                .thenReturn(counter);
        
        // First check - success
        mockGeocoderSuccess("Test Address");
        healthIndicator.health();
        
        // Second check - failure (should trigger status change metric)
        mockGeocoderFailure(new RuntimeException("Service unavailable"));
        healthIndicator.health();
        
        // Verify status change counter was incremented
        verify(counter).increment();
        
        // Third check - success again (should trigger recovery metric)
        mockGeocoderSuccess("Test Address");
        healthIndicator.health();
        
        // Verify recovery counter was incremented
        verify(counter, times(2)).increment();
    }

    @Test
    public void testProviderNameExtraction() {
        // Test with different geocoder implementations
        when(geocoder.getClass()).thenReturn((Class) GoogleGeocoder.class);
        healthIndicator = new GeocoderHealthIndicator(geocoder, meterRegistry);
        mockGeocoderSuccess("Test Address");
        
        Health health = healthIndicator.health();
        assertEquals("google", health.getDetails().get("provider"));
        
        // Test with a non-standard named geocoder
        when(geocoder.getClass()).thenReturn((Class) CustomGeocoder.class);
        healthIndicator = new GeocoderHealthIndicator(geocoder, meterRegistry);
        mockGeocoderSuccess("Test Address");
        
        health = healthIndicator.health();
        assertEquals("custom", health.getDetails().get("provider"));
    }

    /**
     * Helper method to mock successful geocoder response
     */
    private void mockGeocoderSuccess(String address) {
        doAnswer(invocation -> {
            Geocoder.ReverseGeocoderCallback callback = invocation.getArgument(2);
            callback.onSuccess(address);
            return null;
        }).when(geocoder).getAddress(anyDouble(), anyDouble(), any(Geocoder.ReverseGeocoderCallback.class));
    }

    /**
     * Helper method to mock failed geocoder response
     */
    private void mockGeocoderFailure(Throwable error) {
        doAnswer(invocation -> {
            Geocoder.ReverseGeocoderCallback callback = invocation.getArgument(2);
            callback.onFailure(error);
            return null;
        }).when(geocoder).getAddress(anyDouble(), anyDouble(), any(Geocoder.ReverseGeocoderCallback.class));
    }

    /**
     * Mock geocoder classes for testing provider name extraction
     */
    private static class MockGeocoder {}
    private static class GoogleGeocoder {}
    private static class CustomGeocoder {}
}