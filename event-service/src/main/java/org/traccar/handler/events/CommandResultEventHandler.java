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
package org.traccar.handler.events;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Handler for command result events.
 * 
 * This class detects when a position contains command result information and generates
 * appropriate events. It includes circuit breaker pattern for resilience, OpenTelemetry
 * instrumentation for observability, and retry mechanisms for transient failures.
 */
public class CommandResultEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandResultEventHandler.class);
    
    private static final String CIRCUIT_BREAKER_NAME = "commandResultEventHandler";
    private static final String RETRY_NAME = "commandResultEventHandler";
    private static final String CORRELATION_ID_KEY = "correlationId";
    
    // Position attribute keys
    private static final String RESULT_KEY = "result";
    private static final String COMMAND_KEY = "command";
    
    // OpenTelemetry attribute keys
    private static final AttributeKey<String> DEVICE_ID_KEY = AttributeKey.stringKey("device.id");
    private static final AttributeKey<String> COMMAND_KEY_ATTR = AttributeKey.stringKey("command");
    private static final AttributeKey<String> RESULT_KEY_ATTR = AttributeKey.stringKey("result");
    private static final AttributeKey<String> CORRELATION_ID_ATTR = AttributeKey.stringKey("correlation.id");
    
    private final CacheManager cacheManager;
    private final Storage storage;
    private final CircuitBreaker cacheCircuitBreaker;
    private final CircuitBreaker storageCircuitBreaker;
    private final Retry cacheRetry;
    private final Retry storageRetry;
    private final Tracer tracer;
    
    // Metrics
    private final Timer processingTimer;
    private final Counter commandResultEventCounter;
    private final Counter failedDetectionCounter;
    
    /**
     * Constructs the CommandResultEventHandler with necessary dependencies.
     *
     * @param cacheManager Cache manager for device data access
     * @param storage Storage for persistence operations
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Registry for metrics collection
     */
    @Inject
    public CommandResultEventHandler(
            CacheManager cacheManager,
            Storage storage,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        
        this.cacheManager = cacheManager;
        this.storage = storage;
        this.tracer = tracer;
        
        // Initialize circuit breakers
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .recordExceptions(StorageException.class, TimeoutException.class)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.cacheCircuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME + "Cache");
        this.storageCircuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME + "Storage");
        
        // Initialize retry mechanisms
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(StorageException.class, TimeoutException.class)
                .enableExponentialBackoff(true)
                .exponentialBackoffMultiplier(2)
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.cacheRetry = retryRegistry.retry(RETRY_NAME + "Cache");
        this.storageRetry = retryRegistry.retry(RETRY_NAME + "Storage");
        
        // Initialize metrics
        this.processingTimer = Timer.builder("command.result.processing.time")
                .description("Time taken to process position for command result detection")
                .register(meterRegistry);
        
        this.commandResultEventCounter = Counter.builder("command.result.events.total")
                .description("Total number of command result events detected")
                .register(meterRegistry);
        
        this.failedDetectionCounter = Counter.builder("command.result.detection.failures")
                .description("Number of failed command result detection attempts")
                .register(meterRegistry);
    }

    /**
     * Analyzes a position to detect command result events.
     * 
     * This method is instrumented with OpenTelemetry for distributed tracing and
     * uses circuit breakers and retry mechanisms for resilience.
     *
     * @param position the position to analyze
     * @param correlationId the correlation ID for cross-service tracking
     * @return CompletableFuture with the event if detected, or null if no event
     */
    public CompletableFuture<Event> analyzePosition(Position position, String correlationId) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("command.result.detect")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(CORRELATION_ID_ATTR, correlationId)
                .setAttribute(DEVICE_ID_KEY, String.valueOf(position.getDeviceId()))
                .startSpan();
        
        // Use the timer to measure processing time
        return processingTimer.record(() -> {
            try (Scope scope = span.makeCurrent()) {
                return detectCommandResult(position, span, correlationId);
            } catch (Exception e) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                failedDetectionCounter.increment();
                LOGGER.error("Error detecting command result for device {}: {}", 
                        position.getDeviceId(), e.getMessage(), e);
                return CompletableFuture.completedFuture(null);
            } finally {
                span.end();
            }
        });
    }

    /**
     * Core logic for command result detection with resilience patterns applied.
     *
     * @param position the position to analyze
     * @param span the current tracing span
     * @param correlationId the correlation ID for cross-service tracking
     * @return CompletableFuture with the event if detected, or null if no event
     */
    private CompletableFuture<Event> detectCommandResult(Position position, Span span, String correlationId) {
        // Check if position has command result attributes
        if (!position.hasAttribute(RESULT_KEY)) {
            span.addEvent("No command result attribute");
            return CompletableFuture.completedFuture(null);
        }
        
        // Extract command result information
        String result = position.getString(RESULT_KEY);
        String command = position.getString(COMMAND_KEY);
        
        // Add command result attributes to span for context
        span.setAttribute(RESULT_KEY_ATTR, result != null ? result : "null");
        span.setAttribute(COMMAND_KEY_ATTR, command != null ? command : "null");
        
        // Create command result event
        Event event = new Event(Event.TYPE_COMMAND_RESULT, position.getDeviceId(), position.getId());
        event.set(RESULT_KEY, result);
        if (command != null) {
            event.set(COMMAND_KEY, command);
        }
        event.set(Event.KEY_CORRELATION_ID, correlationId);
        
        // Save event with circuit breaker and retry
        Supplier<Event> eventSaveSupplier = () -> {
            Span saveSpan = tracer.spanBuilder("save.command.result.event")
                    .setParent(Context.current())
                    .setAttribute(CORRELATION_ID_ATTR, correlationId)
                    .startSpan();
            
            try {
                saveSpan.setAttribute("event.type", event.getType());
                saveSpan.setAttribute("event.deviceId", event.getDeviceId());
                
                storage.addObject(event, Map.of());
                commandResultEventCounter.increment();
                return event;
            } catch (Exception e) {
                saveSpan.recordException(e);
                saveSpan.setStatus(StatusCode.ERROR, e.getMessage());
                throw new RuntimeException("Failed to save command result event", e);
            } finally {
                saveSpan.end();
            }
        };
        
        try {
            Event savedEvent = Retry.decorateSupplier(storageRetry,
                    CircuitBreaker.decorateSupplier(storageCircuitBreaker, eventSaveSupplier))
                    .get();
            
            span.setAttribute("event.saved", true);
            return CompletableFuture.completedFuture(savedEvent);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Failed to save event: " + e.getMessage());
            failedDetectionCounter.increment();
            LOGGER.error("Failed to save command result event: {}", e.getMessage(), e);
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Processes a position message from the message broker.
     * This is the main entry point for the handler when receiving position data.
     *
     * @param position the position to process
     * @param correlationId the correlation ID for cross-service tracking
     * @return CompletableFuture with the event if detected, or null if no event
     */
    public CompletableFuture<Event> processPositionMessage(Position position, String correlationId) {
        Span rootSpan = tracer.spanBuilder("process.position.message")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(CORRELATION_ID_ATTR, correlationId)
                .setAttribute(DEVICE_ID_KEY, String.valueOf(position.getDeviceId()))
                .startSpan();
        
        try (Scope scope = rootSpan.makeCurrent()) {
            return analyzePosition(position, correlationId)
                    .exceptionally(e -> {
                        rootSpan.recordException(e);
                        rootSpan.setStatus(StatusCode.ERROR, e.getMessage());
                        LOGGER.error("Unhandled exception in command result detection: {}", e.getMessage(), e);
                        return null;
                    });
        } finally {
            rootSpan.end();
        }
    }
}