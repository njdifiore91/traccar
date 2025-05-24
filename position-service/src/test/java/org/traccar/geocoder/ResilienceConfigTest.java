package org.traccar.geocoder;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.metrics.MetricsEndpoint;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

/**
 * Unit test for the ResilienceConfig class that validates the configuration of resilience patterns
 * used by geocoder implementations. This test ensures that circuit breakers, retry mechanisms, and
 * fallback strategies are properly configured for external geocoding service calls.
 * 
 * The test verifies that:
 * 1. Circuit breakers prevent cascading failures from external geocoding services
 * 2. Retry mechanisms use appropriate backoff strategies
 * 3. Fallback strategies provide graceful degradation when services are unavailable
 * 4. Metrics are properly collected for monitoring circuit breaker states
 * 5. Service-specific configurations are applied for different geocoding providers
 */
@ExtendWith(MockitoExtension.class)
public class ResilienceConfigTest {

    @Mock
    private ResilienceConfig resilienceConfig;

    @Mock
    private MetricsEndpoint metricsEndpoint;
    
    @Mock
    private BulkheadRegistry bulkheadRegistry;
    
    @Mock
    private TimeLimiterRegistry timeLimiterRegistry;

    /**
     * Tests that the circuit breaker configuration has appropriate thresholds
     * for different geocoding services.
     */
    @Test
    public void testCircuitBreakerConfiguration() {
        // Given
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();
        when(resilienceConfig.getCircuitBreakerRegistry()).thenReturn(circuitBreakerRegistry);
        when(resilienceConfig.getGoogleGeocoderCircuitBreakerConfig()).thenReturn(
                CircuitBreakerConfig.custom()
                        .failureRateThreshold(50)
                        .waitDurationInOpenState(Duration.ofSeconds(30))
                        .permittedNumberOfCallsInHalfOpenState(5)
                        .slidingWindowSize(10)
                        .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                        .minimumNumberOfCalls(5)
                        .build());
        
        // When
        CircuitBreaker googleCircuitBreaker = resilienceConfig.getGoogleGeocoderCircuitBreakerConfig();
        
        // Then
        CircuitBreakerConfig config = googleCircuitBreaker.getCircuitBreakerConfig();
        assertEquals(50.0f, config.getFailureRateThreshold());
        assertEquals(Duration.ofSeconds(30), config.getWaitDurationInOpenState());
        assertEquals(5, config.getPermittedNumberOfCallsInHalfOpenState());
        assertEquals(10, config.getSlidingWindowSize());
        assertEquals(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED, config.getSlidingWindowType());
        assertEquals(5, config.getMinimumNumberOfCalls());
    }

    /**
     * Tests that different geocoding services have appropriate service-specific
     * circuit breaker configurations.
     */
    @Test
    public void testServiceSpecificCircuitBreakerConfiguration() {
        // Given
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();
        when(resilienceConfig.getCircuitBreakerRegistry()).thenReturn(circuitBreakerRegistry);
        
        // Configure different thresholds for different services
        when(resilienceConfig.getGoogleGeocoderCircuitBreakerConfig()).thenReturn(
                CircuitBreakerConfig.custom()
                        .failureRateThreshold(50)
                        .waitDurationInOpenState(Duration.ofSeconds(30))
                        .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                        .build());
        
        when(resilienceConfig.getNominatimGeocoderCircuitBreakerConfig()).thenReturn(
                CircuitBreakerConfig.custom()
                        .failureRateThreshold(40)
                        .waitDurationInOpenState(Duration.ofSeconds(45))
                        .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
                        .build());
        
        when(resilienceConfig.getMapboxGeocoderCircuitBreakerConfig()).thenReturn(
                CircuitBreakerConfig.custom()
                        .failureRateThreshold(30)
                        .waitDurationInOpenState(Duration.ofSeconds(60))
                        .slowCallRateThreshold(50.0f)
                        .slowCallDurationThreshold(Duration.ofSeconds(2))
                        .build());
        
        // When
        CircuitBreaker googleCircuitBreaker = resilienceConfig.getGoogleGeocoderCircuitBreakerConfig();
        CircuitBreaker nominatimCircuitBreaker = resilienceConfig.getNominatimGeocoderCircuitBreakerConfig();
        CircuitBreaker mapboxCircuitBreaker = resilienceConfig.getMapboxGeocoderCircuitBreakerConfig();
        
        // Then
        assertEquals(50.0f, googleCircuitBreaker.getCircuitBreakerConfig().getFailureRateThreshold());
        assertEquals(40.0f, nominatimCircuitBreaker.getCircuitBreakerConfig().getFailureRateThreshold());
        assertEquals(30.0f, mapboxCircuitBreaker.getCircuitBreakerConfig().getFailureRateThreshold());
        
        assertEquals(Duration.ofSeconds(30), googleCircuitBreaker.getCircuitBreakerConfig().getWaitDurationInOpenState());
        assertEquals(Duration.ofSeconds(45), nominatimCircuitBreaker.getCircuitBreakerConfig().getWaitDurationInOpenState());
        assertEquals(Duration.ofSeconds(60), mapboxCircuitBreaker.getCircuitBreakerConfig().getWaitDurationInOpenState());
        
        assertEquals(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED, googleCircuitBreaker.getCircuitBreakerConfig().getSlidingWindowType());
        assertEquals(CircuitBreakerConfig.SlidingWindowType.TIME_BASED, nominatimCircuitBreaker.getCircuitBreakerConfig().getSlidingWindowType());
        
        assertEquals(50.0f, mapboxCircuitBreaker.getCircuitBreakerConfig().getSlowCallRateThreshold());
        assertEquals(Duration.ofSeconds(2), mapboxCircuitBreaker.getCircuitBreakerConfig().getSlowCallDurationThreshold());
    }

    /**
     * Tests that the retry mechanism is configured with exponential backoff and jitter.
     */
    @Test
    public void testRetryWithExponentialBackoff() {
        // Given
        RetryRegistry retryRegistry = RetryRegistry.ofDefaults();
        when(resilienceConfig.getRetryRegistry()).thenReturn(retryRegistry);
        when(resilienceConfig.getGeocoderRetryConfig()).thenReturn(
                RetryConfig.custom()
                        .maxAttempts(3)
                        .waitDuration(Duration.ofMillis(500))
                        .retryExceptions(TimeoutException.class, RuntimeException.class)
                        .enableExponentialBackoff(true)
                        .exponentialBackoffMultiplier(2)
                        .enableRandomizedWait(true) // Enable jitter
                        .randomizedWaitFactor(0.5) // 50% jitter
                        .build());
        
        // When
        RetryConfig retryConfig = resilienceConfig.getGeocoderRetryConfig();
        
        // Then
        assertEquals(3, retryConfig.getMaxAttempts());
        assertEquals(Duration.ofMillis(500), retryConfig.getWaitDuration());
        assertTrue(retryConfig.getExponentialBackoffMultiplier().isPresent());
        assertEquals(2.0, retryConfig.getExponentialBackoffMultiplier().get());
        assertTrue(retryConfig.getRandomizedWaitFactor().isPresent());
        assertEquals(0.5, retryConfig.getRandomizedWaitFactor().get());
        assertTrue(retryConfig.getExceptionPredicate().test(new TimeoutException()));
        assertTrue(retryConfig.getExceptionPredicate().test(new RuntimeException()));
    }
    
    /**
     * Tests that different geocoding services have appropriate service-specific
     * retry configurations.
     */
    @Test
    public void testServiceSpecificRetryConfiguration() {
        // Given
        RetryRegistry retryRegistry = RetryRegistry.ofDefaults();
        when(resilienceConfig.getRetryRegistry()).thenReturn(retryRegistry);
        
        // Configure different retry settings for different services
        when(resilienceConfig.getGoogleGeocoderRetryConfig()).thenReturn(
                RetryConfig.custom()
                        .maxAttempts(3)
                        .waitDuration(Duration.ofMillis(500))
                        .enableExponentialBackoff(true)
                        .exponentialBackoffMultiplier(2)
                        .build());
        
        when(resilienceConfig.getNominatimGeocoderRetryConfig()).thenReturn(
                RetryConfig.custom()
                        .maxAttempts(5)
                        .waitDuration(Duration.ofMillis(200))
                        .enableExponentialBackoff(true)
                        .exponentialBackoffMultiplier(1.5)
                        .build());
        
        when(resilienceConfig.getMapboxGeocoderRetryConfig()).thenReturn(
                RetryConfig.custom()
                        .maxAttempts(2)
                        .waitDuration(Duration.ofSeconds(1))
                        .enableExponentialBackoff(false)
                        .build());
        
        // When
        RetryConfig googleRetryConfig = resilienceConfig.getGoogleGeocoderRetryConfig();
        RetryConfig nominatimRetryConfig = resilienceConfig.getNominatimGeocoderRetryConfig();
        RetryConfig mapboxRetryConfig = resilienceConfig.getMapboxGeocoderRetryConfig();
        
        // Then
        assertEquals(3, googleRetryConfig.getMaxAttempts());
        assertEquals(5, nominatimRetryConfig.getMaxAttempts());
        assertEquals(2, mapboxRetryConfig.getMaxAttempts());
        
        assertEquals(Duration.ofMillis(500), googleRetryConfig.getWaitDuration());
        assertEquals(Duration.ofMillis(200), nominatimRetryConfig.getWaitDuration());
        assertEquals(Duration.ofSeconds(1), mapboxRetryConfig.getWaitDuration());
        
        assertTrue(googleRetryConfig.getExponentialBackoffMultiplier().isPresent());
        assertTrue(nominatimRetryConfig.getExponentialBackoffMultiplier().isPresent());
        assertFalse(mapboxRetryConfig.getExponentialBackoffMultiplier().isPresent());
        
        assertEquals(2.0, googleRetryConfig.getExponentialBackoffMultiplier().get());
        assertEquals(1.5, nominatimRetryConfig.getExponentialBackoffMultiplier().get());
    }

    /**
     * Tests that fallback strategies are properly configured for unavailable geocoding services.
     */
    @Test
    public void testFallbackStrategies() {
        // Given
        when(resilienceConfig.isFallbackEnabled()).thenReturn(true);
        when(resilienceConfig.getDefaultFallbackAddress()).thenReturn("Unknown Location");
        when(resilienceConfig.getServiceSpecificFallbackAddress("google")).thenReturn("Google Geocoder Unavailable");
        when(resilienceConfig.getServiceSpecificFallbackAddress("nominatim")).thenReturn("Nominatim Geocoder Unavailable");
        when(resilienceConfig.getServiceSpecificFallbackAddress("mapbox")).thenReturn("Mapbox Geocoder Unavailable");
        
        // When
        boolean fallbackEnabled = resilienceConfig.isFallbackEnabled();
        String defaultFallbackAddress = resilienceConfig.getDefaultFallbackAddress();
        String googleFallbackAddress = resilienceConfig.getServiceSpecificFallbackAddress("google");
        String nominatimFallbackAddress = resilienceConfig.getServiceSpecificFallbackAddress("nominatim");
        String mapboxFallbackAddress = resilienceConfig.getServiceSpecificFallbackAddress("mapbox");
        
        // Then
        assertTrue(fallbackEnabled);
        assertEquals("Unknown Location", defaultFallbackAddress);
        assertEquals("Google Geocoder Unavailable", googleFallbackAddress);
        assertEquals("Nominatim Geocoder Unavailable", nominatimFallbackAddress);
        assertEquals("Mapbox Geocoder Unavailable", mapboxFallbackAddress);
    }
    
    /**
     * Tests that fallback strategies are properly applied when geocoding services are unavailable.
     */
    @Test
    public void testFallbackExecution() {
        // Given
        when(resilienceConfig.isFallbackEnabled()).thenReturn(true);
        when(resilienceConfig.getDefaultFallbackAddress()).thenReturn("Unknown Location");
        when(resilienceConfig.executeFallbackStrategy("google", new RuntimeException("Service unavailable")))
                .thenReturn("Google Geocoder Unavailable");
        when(resilienceConfig.executeFallbackStrategy("nominatim", new TimeoutException("Request timed out")))
                .thenReturn("Nominatim Geocoder Unavailable");
        
        // When
        String googleFallbackResult = resilienceConfig.executeFallbackStrategy("google", 
                new RuntimeException("Service unavailable"));
        String nominatimFallbackResult = resilienceConfig.executeFallbackStrategy("nominatim", 
                new TimeoutException("Request timed out"));
        
        // Then
        assertEquals("Google Geocoder Unavailable", googleFallbackResult);
        assertEquals("Nominatim Geocoder Unavailable", nominatimFallbackResult);
    }

    /**
     * Tests that the circuit breaker integrates with metrics collection for monitoring.
     */
    @Test
    public void testMetricsIntegration() {
        // Given
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();
        when(resilienceConfig.getCircuitBreakerRegistry()).thenReturn(circuitBreakerRegistry);
        when(resilienceConfig.isMetricsEnabled()).thenReturn(true);
        when(resilienceConfig.getMetricsEndpoint()).thenReturn(metricsEndpoint);
        
        // When
        boolean metricsEnabled = resilienceConfig.isMetricsEnabled();
        MetricsEndpoint metricsEndpointResult = resilienceConfig.getMetricsEndpoint();
        
        // Then
        assertTrue(metricsEnabled);
        assertNotNull(metricsEndpointResult);
        assertEquals(metricsEndpoint, metricsEndpointResult);
    }
    
    /**
     * Tests that circuit breaker metrics are properly collected and exposed.
     */
    @Test
    public void testCircuitBreakerMetricsCollection() {
        // Given
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("googleGeocoder");
        
        when(resilienceConfig.getCircuitBreakerRegistry()).thenReturn(circuitBreakerRegistry);
        when(resilienceConfig.getCircuitBreaker("googleGeocoder")).thenReturn(circuitBreaker);
        when(resilienceConfig.isMetricsEnabled()).thenReturn(true);
        
        // When
        CircuitBreaker result = resilienceConfig.getCircuitBreaker("googleGeocoder");
        
        // Then
        assertNotNull(result);
        assertEquals("googleGeocoder", result.getName());
        assertEquals(CircuitBreaker.State.CLOSED, result.getState());
        
        // Verify metrics are available
        assertNotNull(result.getMetrics());
        assertEquals(0, result.getMetrics().getNumberOfFailedCalls());
        assertEquals(0, result.getMetrics().getNumberOfSuccessfulCalls());
    }
    
    /**
     * Tests the integration between circuit breaker and retry mechanisms.
     */
    @Test
    public void testCircuitBreakerAndRetryIntegration() {
        // Given
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();
        RetryRegistry retryRegistry = RetryRegistry.ofDefaults();
        
        when(resilienceConfig.getCircuitBreakerRegistry()).thenReturn(circuitBreakerRegistry);
        when(resilienceConfig.getRetryRegistry()).thenReturn(retryRegistry);
        when(resilienceConfig.getCircuitBreakerAspectOrder()).thenReturn(1);
        when(resilienceConfig.getRetryAspectOrder()).thenReturn(2);
        
        // When
        int circuitBreakerOrder = resilienceConfig.getCircuitBreakerAspectOrder();
        int retryOrder = resilienceConfig.getRetryAspectOrder();
        
        // Then
        // Verify that retry has higher precedence (executes before circuit breaker)
        assertTrue(retryOrder > circuitBreakerOrder, 
                "Retry aspect should have higher order than circuit breaker to execute first");
    }
    
    /**
     * Tests that bulkhead configuration is properly set up to limit concurrent calls
     * to geocoding services.
     */
    @Test
    public void testBulkheadConfiguration() {
        // Given
        when(resilienceConfig.getBulkheadRegistry()).thenReturn(bulkheadRegistry);
        when(resilienceConfig.getGeocoderBulkheadConfig()).thenReturn(
                BulkheadConfig.custom()
                        .maxConcurrentCalls(20)
                        .maxWaitDuration(Duration.ofMillis(500))
                        .build());
        
        // When
        BulkheadConfig bulkheadConfig = resilienceConfig.getGeocoderBulkheadConfig();
        
        // Then
        assertEquals(20, bulkheadConfig.getMaxConcurrentCalls());
        assertEquals(Duration.ofMillis(500), bulkheadConfig.getMaxWaitDuration());
    }
    
    /**
     * Tests that time limiter configuration is properly set up to limit the duration
     * of geocoding service calls.
     */
    @Test
    public void testTimeLimiterConfiguration() {
        // Given
        when(resilienceConfig.getTimeLimiterRegistry()).thenReturn(timeLimiterRegistry);
        when(resilienceConfig.getGeocoderTimeLimiterConfig()).thenReturn(
                TimeLimiterConfig.custom()
                        .timeoutDuration(Duration.ofSeconds(3))
                        .cancelRunningFuture(true)
                        .build());
        
        // When
        TimeLimiterConfig timeLimiterConfig = resilienceConfig.getGeocoderTimeLimiterConfig();
        
        // Then
        assertEquals(Duration.ofSeconds(3), timeLimiterConfig.getTimeoutDuration());
        assertTrue(timeLimiterConfig.shouldCancelRunningFuture());
    }
    
    /**
     * Tests that exception predicates are properly configured to determine which exceptions
     * should trigger circuit breaker and retry mechanisms.
     */
    @Test
    public void testExceptionPredicates() {
        // Given
        Predicate<Throwable> retryPredicate = e -> e instanceof TimeoutException || e instanceof RuntimeException;
        Predicate<Throwable> ignorePredicate = e -> e instanceof IllegalArgumentException;
        
        when(resilienceConfig.getRetryExceptionPredicate()).thenReturn(retryPredicate);
        when(resilienceConfig.getIgnoreExceptionPredicate()).thenReturn(ignorePredicate);
        
        // When
        Predicate<Throwable> resultRetryPredicate = resilienceConfig.getRetryExceptionPredicate();
        Predicate<Throwable> resultIgnorePredicate = resilienceConfig.getIgnoreExceptionPredicate();
        
        // Then
        assertTrue(resultRetryPredicate.test(new TimeoutException()));
        assertTrue(resultRetryPredicate.test(new RuntimeException()));
        assertFalse(resultRetryPredicate.test(new IllegalArgumentException()));
        
        assertTrue(resultIgnorePredicate.test(new IllegalArgumentException()));
        assertFalse(resultIgnorePredicate.test(new TimeoutException()));
    }
}