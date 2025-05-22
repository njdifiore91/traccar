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

import com.google.firebase.messaging.MulticastMessage;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.model.Command;
import org.traccar.model.Device;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

public class PushCommandManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushCommandManager.class);
    private static final String CIRCUIT_BREAKER_NAME = "pushCommand";
    private static final String RETRY_NAME = "pushCommandRetry";
    private static final String CORRELATION_ID_KEY = "correlation-id";

    private final FirebaseClient firebaseClient;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final ServiceDiscovery serviceDiscovery;
    
    private final Counter pushCommandAttempts;
    private final Counter pushCommandSuccess;
    private final Counter pushCommandFailure;
    private final Timer pushCommandDuration;

    /**
     * Constructor for PushCommandManager with circuit breaker, retry, metrics, and tracing support.
     *
     * @param firebaseClient Firebase client for sending push notifications
     * @param meterRegistry Metrics registry for collecting performance metrics
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param serviceDiscovery Service discovery for locating push notification services
     */
    public PushCommandManager(FirebaseClient firebaseClient, MeterRegistry meterRegistry, 
                             Tracer tracer, ServiceDiscovery serviceDiscovery) {
        this.firebaseClient = firebaseClient;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        this.serviceDiscovery = serviceDiscovery;
        
        // Initialize circuit breaker with custom configuration
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .slidingWindowSize(10) // Consider last 10 calls for failure rate calculation
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Register circuit breaker state transition listener for logging
        this.circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker '{}' changed state from {} to {}",
                        event.getCircuitBreakerName(), event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));
        
        // Initialize retry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Maximum 3 attempts
                .waitDuration(Duration.ofMillis(500)) // Initial wait duration
                .retryExceptions(Exception.class) // Retry on all exceptions
                .exponentialBackoff(Duration.ofMillis(500), Duration.ofSeconds(5), 2.0) // Exponential backoff
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry(RETRY_NAME);
        
        // Initialize metrics
        this.pushCommandAttempts = Counter.builder("push.command.attempts")
                .description("Number of push command attempts")
                .register(meterRegistry);
        
        this.pushCommandSuccess = Counter.builder("push.command.success")
                .description("Number of successful push commands")
                .register(meterRegistry);
        
        this.pushCommandFailure = Counter.builder("push.command.failure")
                .description("Number of failed push commands")
                .register(meterRegistry);
        
        this.pushCommandDuration = Timer.builder("push.command.duration")
                .description("Duration of push command execution")
                .register(meterRegistry);
    }

    /**
     * Sends a command to a device using push notification with circuit breaker, retry, metrics, and tracing.
     *
     * @param device Device to send command to
     * @param command Command to send
     * @throws Exception if sending fails after retries or circuit breaker is open
     */
    public void sendCommand(Device device, Command command) throws Exception {
        sendCommand(device, command, generateCorrelationId());
    }
    
    /**
     * Sends a command to a device using push notification with circuit breaker, retry, metrics, and tracing.
     * This overload accepts a correlation ID for request tracing across service boundaries.
     *
     * @param device Device to send command to
     * @param command Command to send
     * @param correlationId Correlation ID for request tracing
     * @throws Exception if sending fails after retries or circuit breaker is open
     */
    public void sendCommand(Device device, Command command, String correlationId) throws Exception {
        // Create span for tracing
        Span span = tracer.spanBuilder("push.command.send")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("device.id", device.getId())
                .setAttribute("command.type", command.getType())
                .setAttribute(CORRELATION_ID_KEY, correlationId)
                .startSpan();
        
        // Use the span as the current context
        try (Scope scope = span.makeCurrent()) {
            // Record metrics for attempt
            pushCommandAttempts.increment();
            
            // Check if device has notification tokens
            if (!device.hasAttribute("notificationTokens")) {
                span.setStatus(StatusCode.ERROR, "Missing device notification tokens");
                pushCommandFailure.increment();
                throw new RuntimeException("Missing device notification tokens");
            }
            
            // Prepare tokens and message
            List<String> registrationTokens = new ArrayList<>(
                    Arrays.asList(device.getString("notificationTokens").split("[, ]")));
            
            // Add correlation ID to message data for tracing across services
            MulticastMessage message = MulticastMessage.builder()
                    .putData("command", command.getType())
                    .putData(CORRELATION_ID_KEY, correlationId)
                    .addAllTokens(registrationTokens)
                    .build();
            
            // Use retry with circuit breaker to send the message
            try {
                // Measure execution time
                pushCommandDuration.record(() -> {
                    try {
                        // Try to use service discovery for push service if available
                        if (serviceDiscovery != null && serviceDiscovery.isServiceAvailable("push-service")) {
                            String pushServiceUrl = serviceDiscovery.getServiceUrl("push-service");
                            span.setAttribute("push.service.url", pushServiceUrl);
                            LOGGER.debug("Using push service at {}", pushServiceUrl);
                            // Implementation for service-based push would go here
                            // For now, fall back to direct Firebase client
                        }
                        
                        // Execute with retry and circuit breaker
                        Callable<Void> sendMessageCallable = () -> {
                            var result = firebaseClient.getInstance().sendEachForMulticast(message);
                            if (result.getFailureCount() > 0) {
                                throw new RuntimeException("Failed to send device push: " + 
                                        result.getFailureCount() + " failures out of " + 
                                        result.getSuccessCount() + " total");
                            }
                            return null;
                        };
                        
                        // Apply circuit breaker and retry patterns
                        Callable<Void> decoratedCallable = CircuitBreaker.decorateCallable(
                                circuitBreaker, Retry.decorateCallable(retry, sendMessageCallable));
                        
                        // Execute the call
                        decoratedCallable.call();
                        
                        // Record success metric
                        pushCommandSuccess.increment();
                        span.setStatus(StatusCode.OK);
                        
                    } catch (Exception e) {
                        // Record failure metric
                        pushCommandFailure.increment();
                        span.setStatus(StatusCode.ERROR, e.getMessage());
                        span.recordException(e);
                        throw new RuntimeException("Failed to send push command", e);
                    }
                });
            } catch (Exception e) {
                LOGGER.error("Error sending push command with correlation ID {}: {}", 
                        correlationId, e.getMessage(), e);
                throw e;
            }
        } finally {
            // End the span
            span.end();
        }
    }
    
    /**
     * Generates a unique correlation ID for request tracing.
     *
     * @return A unique correlation ID string
     */
    private String generateCorrelationId() {
        return UUID.randomUUID().toString();
    }
}
