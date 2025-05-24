package org.traccar.geolocation;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.model.Network;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the GeolocationMetricsCollector class.
 * 
 * These tests validate that the metrics collector correctly records success rates,
 * response times, and error rates for different geolocation providers.
 */
@ExtendWith(MockitoExtension.class)
public class GeolocationMetricsCollectorTest {

    private GeolocationMetricsCollector metricsCollector;
    
    private InMemoryMetricReader metricReader;
    private SdkMeterProvider meterProvider;
    private Meter meter;
    
    @Mock
    private LongCounter requestCounter;
    
    @Mock
    private LongCounter errorCounter;
    
    @Mock
    private Network network;
    
    @Mock
    private GeolocationProvider.LocationProviderCallback callback;
    
    @BeforeEach
    public void setUp() {
        // Set up the in-memory metric reader for testing
        metricReader = InMemoryMetricReader.create();
        meterProvider = SdkMeterProvider.builder()
                .registerMetricReader(metricReader)
                .build();
        meter = meterProvider.get(GeolocationMetricsCollector.class.getName());
        
        // Create the metrics collector with the test meter provider
        metricsCollector = new GeolocationMetricsCollector(meterProvider);
    }
    
    @Test
    public void testMetricsRegistration() {
        // Verify that the metrics collector registers all required metrics
        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        
        // Check that the required metrics are registered
        boolean foundRequestsMetric = false;
        boolean foundErrorsMetric = false;
        boolean foundDurationMetric = false;
        
        for (MetricData metric : metrics) {
            switch (metric.getName()) {
                case "geolocation.requests":
                    foundRequestsMetric = true;
                    break;
                case "geolocation.errors":
                    foundErrorsMetric = true;
                    break;
                case "geolocation.duration":
                    foundDurationMetric = true;
                    break;
            }
        }
        
        assertTrue(foundRequestsMetric, "Geolocation requests metric should be registered");
        assertTrue(foundErrorsMetric, "Geolocation errors metric should be registered");
        assertTrue(foundDurationMetric, "Geolocation duration metric should be registered");
    }
    
    @Test
    public void testRecordSuccessfulRequest() {
        // Create a mock GeolocationMetricsCollector with mocked counters
        GeolocationMetricsCollector collector = createMockCollector();
        
        // Record a successful request
        collector.recordRequest("google", true, 150.0);
        
        // Verify that the request counter was incremented with success=true
        verify(requestCounter).add(1, Attributes.of(
                AttributeKey.stringKey("provider"), "google",
                AttributeKey.booleanKey("success"), true));
        
        // Verify that the error counter was not incremented
        verify(errorCounter, times(0)).add(eq(1L), any(Attributes.class));
    }
    
    @Test
    public void testRecordFailedRequest() {
        // Create a mock GeolocationMetricsCollector with mocked counters
        GeolocationMetricsCollector collector = createMockCollector();
        
        // Record a failed request
        collector.recordRequest("opencellid", false, 75.0);
        
        // Verify that the request counter was incremented with success=false
        verify(requestCounter).add(1, Attributes.of(
                AttributeKey.stringKey("provider"), "opencellid",
                AttributeKey.booleanKey("success"), false));
        
        // Verify that the error counter was incremented
        verify(errorCounter).add(1, Attributes.of(
                AttributeKey.stringKey("provider"), "opencellid")));
    }
    
    @Test
    public void testRecordRequestWithDifferentProviders() {
        // Create a mock GeolocationMetricsCollector with mocked counters
        GeolocationMetricsCollector collector = createMockCollector();
        
        // Record requests for different providers
        collector.recordRequest("google", true, 120.0);
        collector.recordRequest("unwired", true, 180.0);
        collector.recordRequest("opencellid", false, 90.0);
        
        // Verify that the request counter was incremented for each provider with correct attributes
        verify(requestCounter).add(1, Attributes.of(
                AttributeKey.stringKey("provider"), "google",
                AttributeKey.booleanKey("success"), true));
        
        verify(requestCounter).add(1, Attributes.of(
                AttributeKey.stringKey("provider"), "unwired",
                AttributeKey.booleanKey("success"), true));
        
        verify(requestCounter).add(1, Attributes.of(
                AttributeKey.stringKey("provider"), "opencellid",
                AttributeKey.booleanKey("success"), false));
        
        // Verify that the error counter was incremented only for the failed request
        verify(errorCounter, times(1)).add(eq(1L), any(Attributes.class));
        verify(errorCounter).add(1, Attributes.of(
                AttributeKey.stringKey("provider"), "opencellid")));
    }
    
    @Test
    public void testWrapGeolocationProvider() {
        // Create a mock GeolocationProvider
        GeolocationProvider mockProvider = mock(GeolocationProvider.class);
        
        // Wrap the provider with metrics collection
        GeolocationProvider wrappedProvider = metricsCollector.wrapGeolocationProvider(mockProvider, "google");
        
        // Verify that the wrapped provider is not null
        assertNotNull(wrappedProvider);
        
        // Call getLocation on the wrapped provider
        wrappedProvider.getLocation(network, callback);
        
        // Verify that the original provider's getLocation method was called
        verify(mockProvider).getLocation(eq(network), any(GeolocationProvider.LocationProviderCallback.class));
    }
    
    @Test
    public void testSuccessfulCallbackInWrappedProvider() {
        // Create a mock GeolocationProvider that calls onSuccess immediately
        GeolocationProvider mockProvider = mock(GeolocationProvider.class);
        when(mockProvider.getLocation(any(Network.class), any(GeolocationProvider.LocationProviderCallback.class)))
                .thenAnswer(invocation -> {
                    GeolocationProvider.LocationProviderCallback cb = invocation.getArgument(1);
                    cb.onSuccess(10.0, 20.0, 30.0);
                    return null;
                });
        
        // Create a real metrics collector with the test meter provider
        GeolocationMetricsCollector collector = new GeolocationMetricsCollector(meterProvider);
        
        // Wrap the provider with metrics collection
        GeolocationProvider wrappedProvider = collector.wrapGeolocationProvider(mockProvider, "google");
        
        // Create a mock callback for verification
        GeolocationProvider.LocationProviderCallback mockCallback = mock(GeolocationProvider.LocationProviderCallback.class);
        
        // Call getLocation on the wrapped provider
        wrappedProvider.getLocation(network, mockCallback);
        
        // Verify that the original callback's onSuccess method was called with the correct parameters
        verify(mockCallback).onSuccess(10.0, 20.0, 30.0);
        
        // Verify that metrics were recorded
        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        assertNotNull(metrics);
        assertTrue(metrics.size() > 0, "Metrics should have been recorded");
    }
    
    @Test
    public void testFailedCallbackInWrappedProvider() {
        // Create a mock GeolocationProvider that calls onFailure immediately
        GeolocationProvider mockProvider = mock(GeolocationProvider.class);
        Throwable testError = new GeolocationException("Test error");
        when(mockProvider.getLocation(any(Network.class), any(GeolocationProvider.LocationProviderCallback.class)))
                .thenAnswer(invocation -> {
                    GeolocationProvider.LocationProviderCallback cb = invocation.getArgument(1);
                    cb.onFailure(testError);
                    return null;
                });
        
        // Create a real metrics collector with the test meter provider
        GeolocationMetricsCollector collector = new GeolocationMetricsCollector(meterProvider);
        
        // Wrap the provider with metrics collection
        GeolocationProvider wrappedProvider = collector.wrapGeolocationProvider(mockProvider, "google");
        
        // Create a mock callback for verification
        GeolocationProvider.LocationProviderCallback mockCallback = mock(GeolocationProvider.LocationProviderCallback.class);
        
        // Call getLocation on the wrapped provider
        wrappedProvider.getLocation(network, mockCallback);
        
        // Verify that the original callback's onFailure method was called with the correct parameters
        verify(mockCallback).onFailure(testError);
        
        // Verify that metrics were recorded
        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        assertNotNull(metrics);
        assertTrue(metrics.size() > 0, "Metrics should have been recorded");
    }
    
    /**
     * Helper method to create a mock GeolocationMetricsCollector with mocked counters
     */
    private GeolocationMetricsCollector createMockCollector() {
        MeterProvider mockMeterProvider = mock(MeterProvider.class);
        Meter mockMeter = mock(Meter.class);
        
        when(mockMeterProvider.get(any())).thenReturn(mockMeter);
        when(mockMeter.counterBuilder(eq("geolocation.requests"))).thenAnswer(invocation -> {
            LongCounter.Builder builder = mock(LongCounter.Builder.class);
            when(builder.setDescription(any())).thenReturn(builder);
            when(builder.setUnit(any())).thenReturn(builder);
            when(builder.build()).thenReturn(requestCounter);
            return builder;
        });
        
        when(mockMeter.counterBuilder(eq("geolocation.errors"))).thenAnswer(invocation -> {
            LongCounter.Builder builder = mock(LongCounter.Builder.class);
            when(builder.setDescription(any())).thenReturn(builder);
            when(builder.setUnit(any())).thenReturn(builder);
            when(builder.build()).thenReturn(errorCounter);
            return builder;
        });
        
        when(mockMeter.histogramBuilder(eq("geolocation.duration"))).thenAnswer(invocation -> {
            io.opentelemetry.api.metrics.DoubleHistogram.Builder builder = 
                    mock(io.opentelemetry.api.metrics.DoubleHistogram.Builder.class);
            when(builder.setDescription(any())).thenReturn(builder);
            when(builder.setUnit(any())).thenReturn(builder);
            io.opentelemetry.api.metrics.DoubleHistogram histogram = 
                    mock(io.opentelemetry.api.metrics.DoubleHistogram.class);
            when(builder.build()).thenReturn(histogram);
            return builder;
        });
        
        return new GeolocationMetricsCollector(mockMeterProvider);
    }
}