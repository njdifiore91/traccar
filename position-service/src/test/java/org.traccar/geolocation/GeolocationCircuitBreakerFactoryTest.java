package org.traccar.geolocation;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.metrics.CircuitBreakerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.traccar.model.Network;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class GeolocationCircuitBreakerFactoryTest {

    private GeolocationCircuitBreakerFactory circuitBreakerFactory;
    
    @Mock
    private GeolocationProvider geolocationProvider;
    
    @Mock
    private GeolocationProvider fallbackProvider;
    
    private MeterRegistry meterRegistry;
    private CircuitBreakerRegistry circuitBreakerRegistry;
    
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        
        // Create a meter registry for metrics collection
        meterRegistry = new SimpleMeterRegistry();
        
        // Create a circuit breaker registry with custom configuration
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofMillis(1000))
                .slidingWindowSize(10)
                .minimumNumberOfCalls(5)
                .permittedNumberOfCallsInHalfOpenState(3)
                .build();
        
        circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        
        // Register metrics with the registry
        CircuitBreakerMetrics.ofCircuitBreakerRegistry(meterRegistry, circuitBreakerRegistry);
        
        // Create the factory under test
        circuitBreakerFactory = new GeolocationCircuitBreakerFactory(circuitBreakerRegistry, fallbackProvider);
    }
    
    @Test
    public void testCreateCircuitBreaker() {
        // When
        CircuitBreaker circuitBreaker = circuitBreakerFactory.create("testProvider");
        
        // Then
        assertNotNull("Circuit breaker should not be null", circuitBreaker);
        assertEquals("Circuit breaker should have the correct name", "testProvider", circuitBreaker.getName());
        assertEquals("Circuit breaker should be in CLOSED state initially", CircuitBreaker.State.CLOSED, circuitBreaker.getState());
    }
    
    @Test
    public void testCircuitBreakerStateTransition() throws Exception {
        // Given
        CircuitBreaker circuitBreaker = circuitBreakerFactory.create("transitionTest");
        doThrow(new GeolocationException("Service unavailable")).when(geolocationProvider).getLocation(any(), any());
        
        // When - simulate multiple failures to trip the circuit breaker
        for (int i = 0; i < 10; i++) {
            try {
                circuitBreaker.executeSupplier(() -> {
                    try {
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        geolocationProvider.getLocation(new Network(), new GeolocationProvider.LocationProviderCallback() {
                            @Override
                            public void onSuccess(double latitude, double longitude, double accuracy) {
                                future.complete(null);
                            }
                            
                            @Override
                            public void onFailure(Throwable e) {
                                future.completeExceptionally(e);
                            }
                        });
                        future.get();
                        return true;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (Exception ignored) {
                // Expected exceptions
            }
        }
        
        // Then
        assertEquals("Circuit breaker should transition to OPEN state after failures", 
                CircuitBreaker.State.OPEN, circuitBreaker.getState());
    }
    
    @Test
    public void testFallbackStrategy() {
        // Given
        CircuitBreaker circuitBreaker = circuitBreakerFactory.create("fallbackTest");
        circuitBreaker.transitionToOpenState(); // Force the circuit breaker to open
        
        // Setup the fallback provider to return a successful response
        doAnswer(invocation -> {
            GeolocationProvider.LocationProviderCallback callback = invocation.getArgument(1);
            callback.onSuccess(1.0, 2.0, 3.0);
            return null;
        }).when(fallbackProvider).getLocation(any(), any());
        
        // When - create a decorated supplier that uses the circuit breaker with fallback
        Supplier<CompletableFuture<LocationResult>> decoratedSupplier = circuitBreakerFactory.decorateSupplier(
                geolocationProvider, new Network());
        
        // Then - verify that the fallback is used when the circuit is open
        try {
            CompletableFuture<LocationResult> future = decoratedSupplier.get();
            LocationResult result = future.get();
            
            assertNotNull("Result should not be null", result);
            assertEquals("Latitude should match fallback", 1.0, result.getLatitude(), 0.001);
            assertEquals("Longitude should match fallback", 2.0, result.getLongitude(), 0.001);
            assertEquals("Accuracy should match fallback", 3.0, result.getAccuracy(), 0.001);
            
            // Verify the fallback provider was called
            verify(fallbackProvider).getLocation(any(), any());
            // Verify the primary provider was not called (due to open circuit)
            verify(geolocationProvider, never()).getLocation(any(), any());
        } catch (InterruptedException | ExecutionException e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }
    
    @Test
    public void testMetricsCollection() {
        // Given
        CircuitBreaker circuitBreaker = circuitBreakerFactory.create("metricsTest");
        
        // When - record some successful and failed calls
        for (int i = 0; i < 3; i++) {
            circuitBreaker.onSuccess(100); // Record successful calls with 100ms response time
        }
        
        for (int i = 0; i < 2; i++) {
            circuitBreaker.onError(100, new Exception("Test exception")); // Record failed calls
        }
        
        // Then - verify metrics are collected
        assertEquals("Should record correct number of successful calls", 3, 
                circuitBreaker.getMetrics().getNumberOfSuccessfulCalls());
        assertEquals("Should record correct number of failed calls", 2, 
                circuitBreaker.getMetrics().getNumberOfFailedCalls());
        assertEquals("Should calculate correct failure rate", 40.0f, 
                circuitBreaker.getMetrics().getFailureRate(), 0.01);
        
        // Verify metrics are available in the meter registry
        assertNotNull("Circuit breaker metrics should be registered", 
                meterRegistry.find("resilience4j.circuitbreaker.calls").tag("name", "metricsTest").counter());
    }
    
    /**
     * Helper class to represent a geolocation result
     */
    private static class LocationResult {
        private final double latitude;
        private final double longitude;
        private final double accuracy;
        
        public LocationResult(double latitude, double longitude, double accuracy) {
            this.latitude = latitude;
            this.longitude = longitude;
            this.accuracy = accuracy;
        }
        
        public double getLatitude() {
            return latitude;
        }
        
        public double getLongitude() {
            return longitude;
        }
        
        public double getAccuracy() {
            return accuracy;
        }
    }
}