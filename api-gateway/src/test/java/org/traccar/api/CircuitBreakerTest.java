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
package org.traccar.api;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerEvent;
import io.github.resilience4j.core.registry.EntryAddedEvent;
import io.github.resilience4j.core.registry.EntryRemovedEvent;
import io.github.resilience4j.core.registry.EntryReplacedEvent;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class CircuitBreakerTest {

    private static final String SERVICE_NAME = "testService";
    private static final int FAILURE_THRESHOLD_PERCENTAGE = 50;
    private static final int WAIT_DURATION_IN_OPEN_STATE_MS = 1000;
    private static final int PERMITTED_CALLS_IN_HALF_OPEN = 2;
    private static final int SLIDING_WINDOW_SIZE = 10;
    private static final int MINIMUM_CALLS_FOR_THRESHOLD = 5;
    
    private CircuitBreakerRegistry circuitBreakerRegistry;
    private CircuitBreaker circuitBreaker;
    private MeterRegistry meterRegistry;
    
    @Mock
    private BackendService backendService;
    
    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        
        // Create a custom CircuitBreakerConfig
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(FAILURE_THRESHOLD_PERCENTAGE)
                .waitDurationInOpenState(Duration.ofMillis(WAIT_DURATION_IN_OPEN_STATE_MS))
                .permittedNumberOfCallsInHalfOpenState(PERMITTED_CALLS_IN_HALF_OPEN)
                .slidingWindowSize(SLIDING_WINDOW_SIZE)
                .minimumNumberOfCalls(MINIMUM_CALLS_FOR_THRESHOLD)
                .build();
        
        // Create a CircuitBreakerRegistry with the custom config
        circuitBreakerRegistry = CircuitBreakerRegistry.of(config);
        
        // Create a CircuitBreaker with the registry
        circuitBreaker = circuitBreakerRegistry.circuitBreaker(SERVICE_NAME);
        
        // Setup metrics registry
        meterRegistry = new SimpleMeterRegistry();
    }
    
    /**
     * Test that the circuit breaker starts in CLOSED state
     */
    @Test
    public void testInitialState() {
        assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getState());
    }
    
    /**
     * Test that the circuit breaker transitions to OPEN state after failure threshold is reached
     */
    @Test
    public void testCircuitOpenAfterFailures() {
        // Setup backend service to throw exceptions
        when(backendService.process()).thenThrow(new RuntimeException("Service unavailable"));
        
        // Execute calls through the circuit breaker
        for (int i = 0; i < MINIMUM_CALLS_FOR_THRESHOLD + 1; i++) {
            try {
                circuitBreaker.executeSupplier(() -> backendService.process());
                fail("Expected exception was not thrown");
            } catch (Exception e) {
                // Expected exception
            }
        }
        
        // Verify circuit breaker is now OPEN
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getState());
    }
    
    /**
     * Test that the circuit breaker stays CLOSED if failures are below threshold
     */
    @Test
    public void testCircuitStaysClosedBelowThreshold() {
        // Setup backend service to succeed and fail alternately
        when(backendService.process())
                .thenReturn("Success")
                .thenReturn("Success")
                .thenThrow(new RuntimeException("Service unavailable"))
                .thenReturn("Success");
        
        // Execute calls through the circuit breaker
        for (int i = 0; i < 4; i++) {
            try {
                circuitBreaker.executeSupplier(() -> backendService.process());
            } catch (Exception e) {
                // Expected for one call
            }
        }
        
        // Verify circuit breaker is still CLOSED
        assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getState());
    }
    
    /**
     * Test that the circuit breaker transitions to HALF_OPEN state after wait duration
     */
    @Test
    public void testCircuitHalfOpenAfterWaitDuration() throws InterruptedException {
        // First, make the circuit OPEN
        when(backendService.process()).thenThrow(new RuntimeException("Service unavailable"));
        
        for (int i = 0; i < MINIMUM_CALLS_FOR_THRESHOLD + 1; i++) {
            try {
                circuitBreaker.executeSupplier(() -> backendService.process());
            } catch (Exception e) {
                // Expected exceptions
            }
        }
        
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getState());
        
        // Wait for the circuit to transition to HALF_OPEN
        Thread.sleep(WAIT_DURATION_IN_OPEN_STATE_MS + 100);
        
        // Verify circuit breaker is now HALF_OPEN
        assertEquals(CircuitBreaker.State.HALF_OPEN, circuitBreaker.getState());
    }
    
    /**
     * Test that the circuit breaker closes after successful calls in HALF_OPEN state
     */
    @Test
    public void testCircuitClosesAfterSuccessInHalfOpen() throws InterruptedException {
        // First, make the circuit OPEN
        when(backendService.process()).thenThrow(new RuntimeException("Service unavailable"));
        
        for (int i = 0; i < MINIMUM_CALLS_FOR_THRESHOLD + 1; i++) {
            try {
                circuitBreaker.executeSupplier(() -> backendService.process());
            } catch (Exception e) {
                // Expected exceptions
            }
        }
        
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getState());
        
        // Wait for the circuit to transition to HALF_OPEN
        Thread.sleep(WAIT_DURATION_IN_OPEN_STATE_MS + 100);
        
        // Now make the service return successfully
        when(backendService.process()).thenReturn("Success");
        
        // Execute successful calls in HALF_OPEN state
        for (int i = 0; i < PERMITTED_CALLS_IN_HALF_OPEN; i++) {
            String result = circuitBreaker.executeSupplier(() -> backendService.process());
            assertEquals("Success", result);
        }
        
        // Verify circuit breaker is now CLOSED
        assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getState());
    }
    
    /**
     * Test that the circuit breaker reopens after failed calls in HALF_OPEN state
     */
    @Test
    public void testCircuitReopensAfterFailureInHalfOpen() throws InterruptedException {
        // First, make the circuit OPEN
        when(backendService.process()).thenThrow(new RuntimeException("Service unavailable"));
        
        for (int i = 0; i < MINIMUM_CALLS_FOR_THRESHOLD + 1; i++) {
            try {
                circuitBreaker.executeSupplier(() -> backendService.process());
            } catch (Exception e) {
                // Expected exceptions
            }
        }
        
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getState());
        
        // Wait for the circuit to transition to HALF_OPEN
        Thread.sleep(WAIT_DURATION_IN_OPEN_STATE_MS + 100);
        
        // Keep the service failing
        when(backendService.process()).thenThrow(new RuntimeException("Service still unavailable"));
        
        // Execute a call in HALF_OPEN state
        try {
            circuitBreaker.executeSupplier(() -> backendService.process());
            fail("Expected exception was not thrown");
        } catch (Exception e) {
            // Expected exception
        }
        
        // Verify circuit breaker is back to OPEN
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getState());
    }
    
    /**
     * Test that fallback responses are provided when the circuit is open
     */
    @Test
    public void testFallbackWhenCircuitOpen() {
        // First, make the circuit OPEN
        when(backendService.process()).thenThrow(new RuntimeException("Service unavailable"));
        
        for (int i = 0; i < MINIMUM_CALLS_FOR_THRESHOLD + 1; i++) {
            try {
                circuitBreaker.executeSupplier(() -> backendService.process());
            } catch (Exception e) {
                // Expected exceptions
            }
        }
        
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getState());
        
        // Now try with fallback
        String result = circuitBreaker.decorateSupplier(() -> backendService.process())
                .recover(throwable -> "Fallback Response")
                .get();
        
        assertEquals("Fallback Response", result);
    }
    
    /**
     * Test that fallback responses are provided when the service throws exceptions
     */
    @Test
    public void testFallbackWhenServiceFails() {
        when(backendService.process()).thenThrow(new RuntimeException("Service error"));
        
        String result = circuitBreaker.decorateSupplier(() -> backendService.process())
                .recover(throwable -> "Fallback Response")
                .get();
        
        assertEquals("Fallback Response", result);
    }
    
    /**
     * Test that circuit breaker metrics are exposed for monitoring
     */
    @Test
    public void testCircuitBreakerMetrics() {
        // Register the circuit breaker with the meter registry
        io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
                .ofCircuitBreakerRegistry(circuitBreakerRegistry)
                .bindTo(meterRegistry);
        
        // Execute some successful calls
        when(backendService.process()).thenReturn("Success");
        
        for (int i = 0; i < 5; i++) {
            circuitBreaker.executeSupplier(() -> backendService.process());
        }
        
        // Verify metrics are recorded
        assertNotNull(meterRegistry.get("resilience4j.circuitbreaker.calls").counter());
        assertTrue(meterRegistry.get("resilience4j.circuitbreaker.calls")
                .tag("kind", "successful")
                .tag("name", SERVICE_NAME)
                .counter().count() > 0);
    }
    
    /**
     * Test asynchronous execution with circuit breaker
     */
    @Test
    public void testAsyncExecution() throws ExecutionException, InterruptedException, TimeoutException {
        when(backendService.process()).thenReturn("Async Success");
        
        Supplier<CompletableFuture<String>> futureSupplier = 
                CircuitBreaker.decorateSupplier(circuitBreaker, 
                        () -> CompletableFuture.supplyAsync(() -> backendService.process()));
        
        CompletableFuture<String> future = futureSupplier.get();
        String result = future.get(1, TimeUnit.SECONDS);
        
        assertEquals("Async Success", result);
    }
    
    /**
     * Test that circuit breaker events are published
     */
    @Test
    public void testCircuitBreakerEvents() {
        // Create an event consumer to track events
        TestCircuitBreakerEventConsumer eventConsumer = new TestCircuitBreakerEventConsumer();
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CircuitBreakerConfig.ofDefaults(), eventConsumer);
        CircuitBreaker cb = registry.circuitBreaker("eventTestService");
        
        // Register an event consumer on the circuit breaker
        cb.getEventPublisher().onStateTransition(event -> 
                eventConsumer.onCircuitBreakerEvent(event));
        
        // Make the circuit breaker open
        when(backendService.process()).thenThrow(new RuntimeException("Service unavailable"));
        
        for (int i = 0; i < 10; i++) {
            try {
                cb.executeSupplier(() -> backendService.process());
            } catch (Exception e) {
                // Expected exceptions
            }
        }
        
        // Verify that state transition events were published
        assertTrue(eventConsumer.getEventCount() > 0);
    }
    
    /**
     * Mock backend service interface
     */
    interface BackendService {
        String process();
    }
    
    /**
     * Test event consumer to track circuit breaker events
     */
    static class TestCircuitBreakerEventConsumer implements RegistryEventConsumer<CircuitBreaker> {
        private int eventCount = 0;
        
        @Override
        public void onEntryAddedEvent(EntryAddedEvent<CircuitBreaker> entryAddedEvent) {
            // Not used in this test
        }
        
        @Override
        public void onEntryRemovedEvent(EntryRemovedEvent<CircuitBreaker> entryRemoveEvent) {
            // Not used in this test
        }
        
        @Override
        public void onEntryReplacedEvent(EntryReplacedEvent<CircuitBreaker> entryReplacedEvent) {
            // Not used in this test
        }
        
        public void onCircuitBreakerEvent(CircuitBreakerEvent event) {
            eventCount++;
        }
        
        public int getEventCount() {
            return eventCount;
        }
    }
}