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
package org.traccar.notificators;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.MessageException;
import org.traccar.notification.NotificationMessage;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class NotificatorCircuitBreakerTest {

    @Mock
    private Config config;

    @Mock
    private Notificator notificator;

    private NotificatorCircuitBreaker circuitBreaker;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        
        // Configure default values for circuit breaker
        when(config.getFloat(eq(Keys.NOTIFICATION_CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD), anyFloat()))
                .thenReturn(50.0f);
        when(config.getFloat(eq(Keys.NOTIFICATION_CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD), anyFloat()))
                .thenReturn(50.0f);
        when(config.getLong(eq(Keys.NOTIFICATION_CIRCUIT_BREAKER_SLOW_CALL_DURATION_THRESHOLD), anyLong()))
                .thenReturn(2000L);
        when(config.getInteger(eq(Keys.NOTIFICATION_CIRCUIT_BREAKER_PERMITTED_CALLS_IN_HALF_OPEN), anyInt()))
                .thenReturn(10);
        when(config.getInteger(eq(Keys.NOTIFICATION_CIRCUIT_BREAKER_MINIMUM_CALLS), anyInt()))
                .thenReturn(10);
        when(config.getInteger(eq(Keys.NOTIFICATION_CIRCUIT_BREAKER_SLIDING_WINDOW_SIZE), anyInt()))
                .thenReturn(100);
        when(config.getLong(eq(Keys.NOTIFICATION_CIRCUIT_BREAKER_WAIT_DURATION_IN_OPEN_STATE), anyLong()))
                .thenReturn(60L);
        when(config.getBoolean(eq(Keys.NOTIFICATION_CIRCUIT_BREAKER_STORE_FOR_RETRY), anyBoolean()))
                .thenReturn(true);
        when(config.getBoolean(eq(Keys.NOTIFICATION_CIRCUIT_BREAKER_USE_ALTERNATIVE_CHANNEL), anyBoolean()))
                .thenReturn(false);
        
        // Mock notificator class name
        when(notificator.getClass()).thenReturn((Class) NotificatorMail.class);
        
        circuitBreaker = new NotificatorCircuitBreaker(config, new SimpleMeterRegistry());
    }

    @Test
    public void testGetCircuitBreaker() {
        CircuitBreaker cb = circuitBreaker.getCircuitBreaker("mail");
        assertNotNull(cb);
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        
        // Test that we get the same instance for the same channel
        CircuitBreaker cb2 = circuitBreaker.getCircuitBreaker("mail");
        assertSame(cb, cb2);
        
        // Test that we get a different instance for a different channel
        CircuitBreaker cb3 = circuitBreaker.getCircuitBreaker("sms");
        assertNotSame(cb, cb3);
    }

    @Test
    public void testExecuteNotification() throws MessageException {
        User user = new User();
        NotificationMessage message = new NotificationMessage();
        Event event = new Event();
        Position position = new Position();
        
        // Test successful notification
        circuitBreaker.executeNotification(notificator, user, message, event, position);
        verify(notificator).send(user, message, event, position);
    }

    @Test
    public void testExecuteNotificationWithException() throws MessageException {
        User user = new User();
        NotificationMessage message = new NotificationMessage();
        Event event = new Event();
        Position position = new Position();
        
        // Configure notificator to throw an exception
        doThrow(new MessageException("Test exception")).when(notificator).send(any(), any(), any(), any());
        
        // Test that the exception is propagated
        assertThrows(MessageException.class, () -> {
            circuitBreaker.executeNotification(notificator, user, message, event, position);
        });
    }

    @Test
    public void testCircuitBreakerTrip() throws MessageException {
        // Create a circuit breaker with a very low threshold for testing
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(2)
                .minimumNumberOfCalls(2)
                .waitDurationInOpenState(Duration.ofMillis(100))
                .build();
        
        CircuitBreaker cb = CircuitBreaker.of("test", config);
        
        // Force the circuit breaker to open
        cb.onError(0, new RuntimeException());
        cb.onError(0, new RuntimeException());
        
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
    }

    @Test
    public void testIsHealthy() {
        // Initially all circuit breakers should be healthy
        assertTrue(circuitBreaker.isHealthy());
        
        // Get a circuit breaker and force it to open state
        CircuitBreaker cb = circuitBreaker.getCircuitBreaker("test");
        cb.transitionToOpenState();
        
        // Now the system should not be healthy
        assertFalse(circuitBreaker.isHealthy());
    }

    @Test
    public void testGetCircuitBreakerState() {
        // Get a circuit breaker
        CircuitBreaker cb = circuitBreaker.getCircuitBreaker("test");
        assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getCircuitBreakerState("test"));
        
        // Force it to open state
        cb.transitionToOpenState();
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getCircuitBreakerState("test"));
        
        // Test non-existent circuit breaker
        assertNull(circuitBreaker.getCircuitBreakerState("nonexistent"));
    }

    @Test
    public void testResetCircuitBreaker() {
        // Get a circuit breaker and force it to open state
        CircuitBreaker cb = circuitBreaker.getCircuitBreaker("test");
        cb.transitionToOpenState();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        
        // Reset it
        circuitBreaker.resetCircuitBreaker("test");
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
    }

    @Test
    public void testGetCircuitBreakerMetrics() {
        // Get a circuit breaker
        CircuitBreaker cb = circuitBreaker.getCircuitBreaker("test");
        
        // Record some successful calls
        cb.onSuccess(100);
        cb.onSuccess(100);
        
        // Get metrics
        var metrics = circuitBreaker.getCircuitBreakerMetrics("test");
        assertNotNull(metrics);
        assertEquals("CLOSED", metrics.get("state"));
        assertEquals(2, metrics.get("numberOfSuccessfulCalls"));
        assertEquals(0, metrics.get("numberOfFailedCalls"));
        
        // Test non-existent circuit breaker
        assertNull(circuitBreaker.getCircuitBreakerMetrics("nonexistent"));
    }
}