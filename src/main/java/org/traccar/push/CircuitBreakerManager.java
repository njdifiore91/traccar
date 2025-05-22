/*
 * Copyright 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.push;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
// Import for circuit breaker functionality
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerEvent;
import com.google.firebase.messaging.BatchResponse;
import com.google.firebase.messaging.MulticastMessage;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.resilience4j.CircuitBreakerMetrics;
import io.micrometer.core.instrument.binder.resilience4j.RetryMetrics;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.logging.Level;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Implements circuit breaker patterns for resilient push notification delivery.
 * This class wraps external service calls (like Firebase Cloud Messaging) with circuit breakers
 * to prevent cascading failures when external services are degraded or unavailable.
 * It provides fallback mechanisms, configurable retry policies, and metrics collection for circuit breaker states.
 *
 * The circuit breaker pattern helps to prevent cascading failures by stopping calls to failing services,
 * allowing them time to recover, and providing fallback mechanisms for graceful degradation.
 */
@Singleton
public class CircuitBreakerManager {

    private static final Logger LOGGER = Logger.getLogger(CircuitBreakerManager.class.getName());
    private static final String FIREBASE_SERVICE = "firebase";
    
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;

    /**
     * Constructs a new CircuitBreakerManager with default configurations.
     *
     * @param meterRegistry The meter registry for metrics collection
     * @param tracer The OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public CircuitBreakerManager(MeterRegistry meterRegistry, Tracer tracer) {
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        
        // Configure circuit breaker with default settings
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before attempting to close
                .slidingWindowSize(10) // Consider the last 10 calls for failure rate calculation
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .automaticTransitionFromOpenToHalfOpenEnabled(true) // Auto transition to half-open
                .build();
        
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        
        // Configure retry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Maximum 3 retry attempts
                .waitDuration(Duration.ofMillis(500)) // Initial wait duration
                .retryExceptions(Exception.class) // Retry on all exceptions
                .exponentialBackoff(2, Duration.ofMillis(500), Duration.ofSeconds(5)) // Exponential backoff
                .build();
        
        this.retryRegistry = RetryRegistry.of(retryConfig);
        
        // Register metrics for monitoring
        CircuitBreakerMetrics.ofCircuitBreakerRegistry(circuitBreakerRegistry)
                .bindTo(meterRegistry);
        RetryMetrics.ofRetryRegistry(retryRegistry)
                .bindTo(meterRegistry);
        
        // Initialize Firebase circuit breaker with event consumer for tracing
        CircuitBreaker firebaseCircuitBreaker = circuitBreakerRegistry.circuitBreaker(FIREBASE_SERVICE);
        registerEventConsumer(firebaseCircuitBreaker);
    }

    /**
     * Creates or retrieves a circuit breaker for the specified service.
     *
     * @param serviceName The name of the service to create a circuit breaker for
     * @return The circuit breaker instance
     */
    public CircuitBreaker getCircuitBreaker(String serviceName) {
        return circuitBreakerRegistry.circuitBreaker(serviceName);
    }

    /**
     * Creates or retrieves a retry for the specified service.
     *
     * @param serviceName The name of the service to create a retry for
     * @return The retry instance
     */
    public Retry getRetry(String serviceName) {
        return retryRegistry.retry(serviceName);
    }

    /**
     * Executes a callable with circuit breaker and retry protection.
     *
     * @param serviceName The name of the service being called
     * @param callable The callable to execute
     * @param fallback The fallback supplier to use when the circuit is open
     * @param <T> The return type of the callable
     * @return The result of the callable or fallback
     */
    public <T> T executeWithFallback(String serviceName, Callable<T> callable, Supplier<T> fallback) {
        CircuitBreaker circuitBreaker = getCircuitBreaker(serviceName);
        Retry retry = getRetry(serviceName);
        
        try {
            // Decorate the callable with retry and circuit breaker
            Callable<T> decoratedCallable = Retry.decorateCallable(retry, callable);
            decoratedCallable = CircuitBreaker.decorateCallable(circuitBreaker, decoratedCallable);
            
            return decoratedCallable.call();
        } catch (Exception e) {
            // If circuit is open or call fails after retries, use fallback
            return fallback.get();
        }
    }

    /**
     * Executes a callable with circuit breaker and retry protection without a fallback.
     * This will throw exceptions if the circuit is open or all retries fail.
     *
     * @param serviceName The name of the service being called
     * @param callable The callable to execute
     * @param <T> The return type of the callable
     * @return The result of the callable
     * @throws Exception If the call fails and circuit breaker is open
     */
    public <T> T execute(String serviceName, Callable<T> callable) throws Exception {
        CircuitBreaker circuitBreaker = getCircuitBreaker(serviceName);
        Retry retry = getRetry(serviceName);
        
        // Decorate the callable with retry and circuit breaker
        Callable<T> decoratedCallable = Retry.decorateCallable(retry, callable);
        decoratedCallable = CircuitBreaker.decorateCallable(circuitBreaker, decoratedCallable);
        
        return decoratedCallable.call();
    }

    /**
     * Configures a custom circuit breaker for a specific service.
     *
     * @param serviceName The name of the service
     * @param config The custom circuit breaker configuration
     * @return The configured circuit breaker
     */
    public CircuitBreaker configureCircuitBreaker(String serviceName, CircuitBreakerConfig config) {
        return circuitBreakerRegistry.circuitBreaker(serviceName, config);
    }

    /**
     * Configures a custom retry for a specific service.
     *
     * @param serviceName The name of the service
     * @param config The custom retry configuration
     * @return The configured retry
     */
    public Retry configureRetry(String serviceName, RetryConfig config) {
        return retryRegistry.retry(serviceName, config);
    }

    /**
     * Resets the circuit breaker for a service, clearing all metrics and returning it to the closed state.
     *
     * @param serviceName The name of the service whose circuit breaker should be reset
     */
    public void resetCircuitBreaker(String serviceName) {
        CircuitBreaker circuitBreaker = getCircuitBreaker(serviceName);
        circuitBreaker.reset();
    }
    
    /**
     * Registers an event consumer for the circuit breaker to enable tracing and logging.
     *
     * @param circuitBreaker The circuit breaker to register events for
     */
    private void registerEventConsumer(CircuitBreaker circuitBreaker) {
        circuitBreaker.getEventPublisher().onEvent(event -> {
            Span span = tracer.spanBuilder("CircuitBreaker." + event.getEventType().name())
                    .setAttribute("circuitbreaker.name", event.getCircuitBreakerName())
                    .setAttribute("circuitbreaker.event.type", event.getEventType().name())
                    .startSpan();
            
            try {
                if (event instanceof CircuitBreakerOnStateTransitionEvent) {
                    CircuitBreakerOnStateTransitionEvent transitionEvent = 
                            (CircuitBreakerOnStateTransitionEvent) event;
                    
                    span.setAttribute("circuitbreaker.from.state", transitionEvent.getStateTransition().getFromState().name());
                    span.setAttribute("circuitbreaker.to.state", transitionEvent.getStateTransition().getToState().name());
                    
                    LOGGER.log(Level.INFO, "Circuit breaker {0} state changed from {1} to {2}", 
                            new Object[]{event.getCircuitBreakerName(), 
                                    transitionEvent.getStateTransition().getFromState(),
                                    transitionEvent.getStateTransition().getToState()});
                } else {
                    LOGGER.log(Level.FINE, "Circuit breaker {0} event: {1}", 
                            new Object[]{event.getCircuitBreakerName(), event.getEventType()});
                }
            } finally {
                span.end();
            }
        });
    }
    
    /**
     * Wraps a Firebase client with circuit breaker and retry protection.
     *
     * @param firebaseClient The Firebase client to wrap
     * @return A wrapped Firebase client with circuit breaker protection
     */
    public FirebaseClient wrapFirebaseClient(FirebaseClient firebaseClient) {
        return new FirebaseClientWithCircuitBreaker(firebaseClient, this);
    }
    
    /**
     * Inner class that wraps a FirebaseClient with circuit breaker protection.
     */
    private static class FirebaseClientWithCircuitBreaker implements FirebaseClient {
        private final FirebaseClient delegate;
        private final CircuitBreakerManager circuitBreakerManager;
        
        public FirebaseClientWithCircuitBreaker(FirebaseClient delegate, CircuitBreakerManager circuitBreakerManager) {
            this.delegate = delegate;
            this.circuitBreakerManager = circuitBreakerManager;
        }
        
        @Override
        public FirebaseClient getInstance() {
            return this;
        }
        
        @Override
        public BatchResponse sendEachForMulticast(MulticastMessage message) throws Exception {
            Span span = circuitBreakerManager.tracer.spanBuilder("Firebase.sendEachForMulticast")
                    .setAttribute("firebase.message.type", message.getClass().getSimpleName())
                    .setAttribute("firebase.tokens.count", message.getTokens().size())
                    .startSpan();
            
            try {
                return circuitBreakerManager.execute(FIREBASE_SERVICE, 
                        () -> delegate.getInstance().sendEachForMulticast(message));
            } catch (Exception e) {
                span.recordException(e);
                LOGGER.log(Level.WARNING, "Firebase push notification failed", e);
                throw e;
            } finally {
                span.end();
            }
        }
    }
}