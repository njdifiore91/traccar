package org.traccar.geocoder;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.*;

/**
 * Tests resilience patterns for external geocoding services in the position service,
 * including circuit breakers, retry mechanisms, and fallback strategies.
 * 
 * This test verifies that the system can handle temporary service outages, slow responses,
 * and complete service failures gracefully without impacting the overall system stability.
 */
public class GeocoderResilienceTest {

    @RegisterExtension
    static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();
    
    private Tracer tracer;
    private Geocoder geocoder;
    private Geocoder fallbackGeocoder;
    private CircuitBreakerRegistry circuitBreakerRegistry;
    private RetryRegistry retryRegistry;
    private CircuitBreaker circuitBreaker;
    private Retry retry;
    
    @BeforeEach
    public void setUp() {
        // Set up OpenTelemetry tracer for distributed tracing tests
        tracer = otelTesting.getOpenTelemetry().getTracer("geocoder-resilience-test");
        
        // Create mock geocoders
        geocoder = Mockito.mock(Geocoder.class);
        fallbackGeocoder = Mockito.mock(Geocoder.class);
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .slidingWindowSize(10)    // Consider the last 10 calls
                .minimumNumberOfCalls(5)  // Require at least 5 calls before calculating failure rate
                .waitDurationInOpenState(Duration.ofSeconds(5)) // Wait 5 seconds in OPEN state
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 calls in HALF_OPEN state
                .build();
        
        circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        circuitBreaker = circuitBreakerRegistry.circuitBreaker("geocoder-service");
        
        // Configure retry mechanism
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Try up to 3 times
                .waitDuration(Duration.ofMillis(100)) // Initial wait time
                .retryExceptions(RuntimeException.class, TimeoutException.class) // Retry on these exceptions
                .enableExponentialBackoff(true) // Use exponential backoff
                .exponentialBackoffMultiplier(2.0) // Double wait time after each attempt
                .build();
        
        retryRegistry = RetryRegistry.of(retryConfig);
        retry = retryRegistry.retry("geocoder-service");
    }
    
    /**
     * Tests that the circuit breaker prevents cascading failures when the geocoding service is down.
     */
    @Test
    public void testCircuitBreakerPreventsFailureCascade() {
        // Configure geocoder to always fail
        when(geocoder.getAddress(anyDouble(), anyDouble()))
                .thenThrow(new RuntimeException("Service unavailable"));
        
        // Configure fallback geocoder to return a default address
        when(fallbackGeocoder.getAddress(anyDouble(), anyDouble()))
                .thenReturn("Fallback Address");
        
        // Create a resilient geocoder function with circuit breaker and fallback
        Supplier<String> resilientGeocoder = CircuitBreaker.decorateSupplier(
                circuitBreaker,
                () -> geocoder.getAddress(1.0, 1.0)
        );
        
        Supplier<String> withFallback = () -> {
            try {
                return resilientGeocoder.get();
            } catch (Exception e) {
                return fallbackGeocoder.getAddress(1.0, 1.0);
            }
        };
        
        // Call the service enough times to open the circuit
        for (int i = 0; i < 10; i++) {
            String result = withFallback.get();
            assertEquals("Fallback Address", result, "Should return fallback address");
        }
        
        // Verify the circuit is now open
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getState(), 
                "Circuit should be OPEN after multiple failures");
        
        // Verify that the primary geocoder was called only until the circuit opened
        // and then the fallback was used directly
        verify(geocoder, atLeast(5)).getAddress(1.0, 1.0);
        verify(fallbackGeocoder, atLeast(5)).getAddress(1.0, 1.0);
    }
    
    /**
     * Tests that retry mechanism works with exponential backoff for transient errors.
     */
    @Test
    public void testRetryWithExponentialBackoff() {
        // Configure geocoder to fail twice and then succeed
        when(geocoder.getAddress(anyDouble(), anyDouble()))
                .thenThrow(new RuntimeException("Temporary failure"))
                .thenThrow(new RuntimeException("Temporary failure"))
                .thenReturn("123 Success Street");
        
        // Create a resilient geocoder function with retry
        Supplier<String> resilientGeocoder = Retry.decorateSupplier(
                retry,
                () -> geocoder.getAddress(1.0, 1.0)
        );
        
        // Call the service
        long startTime = System.currentTimeMillis();
        String result = resilientGeocoder.get();
        long endTime = System.currentTimeMillis();
        
        // Verify the result
        assertEquals("123 Success Street", result, "Should return successful result after retries");
        
        // Verify the geocoder was called 3 times (initial + 2 retries)
        verify(geocoder, times(3)).getAddress(1.0, 1.0);
        
        // Verify that the operation took at least the expected backoff time
        // Initial attempt + 100ms wait + second attempt + 200ms wait + third attempt
        long minExpectedDuration = 300; // 100ms + 200ms minimum wait time
        assertTrue(endTime - startTime >= minExpectedDuration, 
                "Operation should take at least the backoff time");
    }
    
    /**
     * Tests fallback strategies when geocoding service is completely unavailable.
     */
    @Test
    public void testFallbackStrategy() {
        // Configure primary geocoder to always fail
        when(geocoder.getAddress(anyDouble(), anyDouble()))
                .thenThrow(new RuntimeException("Service completely down"));
        
        // Configure fallback geocoder to return a default address
        when(fallbackGeocoder.getAddress(anyDouble(), anyDouble()))
                .thenReturn("Default Address");
        
        // Create a resilient geocoder with circuit breaker, retry and fallback
        Supplier<String> resilientGeocoder = Retry.decorateSupplier(
                retry,
                CircuitBreaker.decorateSupplier(
                        circuitBreaker,
                        () -> geocoder.getAddress(1.0, 1.0)
                )
        );
        
        Supplier<String> withFallback = () -> {
            try {
                return resilientGeocoder.get();
            } catch (Exception e) {
                return fallbackGeocoder.getAddress(1.0, 1.0);
            }
        };
        
        // Call the service
        String result = withFallback.get();
        
        // Verify the result
        assertEquals("Default Address", result, "Should return fallback address");
        
        // Verify the primary geocoder was called multiple times (due to retry)
        verify(geocoder, times(3)).getAddress(1.0, 1.0);
        
        // Verify the fallback geocoder was called once
        verify(fallbackGeocoder, times(1)).getAddress(1.0, 1.0);
    }
    
    /**
     * Tests timeout handling and graceful degradation for slow responses.
     */
    @Test
    public void testTimeoutHandling() throws Exception {
        // Configure geocoder to delay response
        when(geocoder.getAddress(anyDouble(), anyDouble()))
                .thenAnswer(invocation -> {
                    Thread.sleep(2000); // Simulate a 2-second delay
                    return "Delayed Address";
                });
        
        // Configure fallback geocoder
        when(fallbackGeocoder.getAddress(anyDouble(), anyDouble()))
                .thenReturn("Timeout Fallback Address");
        
        // Create a future with timeout
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            return geocoder.getAddress(1.0, 1.0);
        });
        
        // Try to get result with a 1-second timeout
        String result;
        try {
            result = future.orTimeout(1, java.util.concurrent.TimeUnit.SECONDS).get();
        } catch (Exception e) {
            // Use fallback on timeout
            result = fallbackGeocoder.getAddress(1.0, 1.0);
        }
        
        // Verify the result
        assertEquals("Timeout Fallback Address", result, "Should return fallback address on timeout");
        
        // Verify the fallback geocoder was called
        verify(fallbackGeocoder, times(1)).getAddress(1.0, 1.0);
    }
    
    /**
     * Tests distributed tracing across service boundaries during failures.
     */
    @Test
    public void testDistributedTracingDuringFailures() {
        // Configure geocoder to fail
        when(geocoder.getAddress(anyDouble(), anyDouble()))
                .thenThrow(new RuntimeException("Service failure for tracing test"));
        
        // Configure fallback geocoder
        when(fallbackGeocoder.getAddress(anyDouble(), anyDouble()))
                .thenReturn("Traced Fallback Address");
        
        // Create a span for the test
        Span parentSpan = tracer.spanBuilder("geocode-request")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        
        try {
            // Use the span as the current context
            Context context = Context.current().with(parentSpan);
            
            // Execute within the context
            String result = context.with(() -> {
                // Create a child span for the geocoder call
                Span childSpan = tracer.spanBuilder("geocoder-service-call")
                        .setParent(context)
                        .setSpanKind(SpanKind.CLIENT)
                        .startSpan();
                
                try {
                    // Try to get address from primary geocoder
                    String address;
                    try {
                        address = geocoder.getAddress(1.0, 1.0);
                        childSpan.setStatus(StatusCode.OK);
                    } catch (Exception e) {
                        // Record the error in the span
                        childSpan.recordException(e);
                        childSpan.setStatus(StatusCode.ERROR, e.getMessage());
                        
                        // Use fallback
                        address = fallbackGeocoder.getAddress(1.0, 1.0);
                        childSpan.setAttribute("fallback.used", true);
                    }
                    return address;
                } finally {
                    childSpan.end();
                }
            });
            
            // Verify the result
            assertEquals("Traced Fallback Address", result, "Should return fallback address");
            
        } finally {
            parentSpan.end();
        }
        
        // Verify spans were created correctly
        otelTesting.assertTraces().hasTracesSatisfyingExactly(trace -> 
            trace.hasSpansSatisfyingExactly(
                parentSpan -> parentSpan.hasName("geocode-request"),
                childSpan -> childSpan.hasName("geocoder-service-call")
                    .hasAttribute("fallback.used", true)
                    .hasStatus(StatusCode.ERROR)
            )
        );
    }
    
    /**
     * Tests that circuit breaker can recover after services become available again.
     */
    @Test
    public void testCircuitBreakerRecovery() {
        // Configure geocoder to fail initially and then recover
        when(geocoder.getAddress(anyDouble(), anyDouble()))
                .thenThrow(new RuntimeException("Service down"))
                .thenThrow(new RuntimeException("Service down"))
                .thenThrow(new RuntimeException("Service down"))
                .thenThrow(new RuntimeException("Service down"))
                .thenThrow(new RuntimeException("Service down"))
                .thenReturn("Recovered Address");
        
        // Configure fallback geocoder
        when(fallbackGeocoder.getAddress(anyDouble(), anyDouble()))
                .thenReturn("Circuit Open Fallback");
        
        // Create a resilient geocoder function with circuit breaker and fallback
        Supplier<String> resilientGeocoder = CircuitBreaker.decorateSupplier(
                circuitBreaker,
                () -> geocoder.getAddress(1.0, 1.0)
        );
        
        Supplier<String> withFallback = () -> {
            try {
                return resilientGeocoder.get();
            } catch (Exception e) {
                return fallbackGeocoder.getAddress(1.0, 1.0);
            }
        };
        
        // Call the service enough times to open the circuit
        for (int i = 0; i < 10; i++) {
            withFallback.get();
        }
        
        // Verify the circuit is now open
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getState(), 
                "Circuit should be OPEN after multiple failures");
        
        // Wait for the circuit to transition to half-open
        try {
            Thread.sleep(5000); // Wait for waitDurationInOpenState (5 seconds)
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Circuit should now be half-open
        assertEquals(CircuitBreaker.State.HALF_OPEN, circuitBreaker.getState(), 
                "Circuit should be HALF_OPEN after wait duration");
        
        // Make successful calls to close the circuit
        for (int i = 0; i < 3; i++) { // permittedNumberOfCallsInHalfOpenState is 3
            String result = withFallback.get();
            assertEquals("Recovered Address", result, "Should return successful result");
        }
        
        // Circuit should now be closed again
        assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getState(), 
                "Circuit should be CLOSED after successful calls");
    }
}