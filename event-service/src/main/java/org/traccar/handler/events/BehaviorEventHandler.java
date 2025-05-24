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
package org.traccar.handler.events;

import com.google.inject.Inject;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class BehaviorEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(BehaviorEventHandler.class);

    private static final AttributeKey<String> BEHAVIOR_TYPE_KEY = AttributeKey.stringKey("behavior.type");
    private static final AttributeKey<String> DEVICE_ID_KEY = AttributeKey.stringKey("device.id");
    private static final AttributeKey<String> CORRELATION_ID_KEY = AttributeKey.stringKey("correlation.id");

    private final CacheManager cacheManager;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Map<String, Timer> behaviorTimers = new ConcurrentHashMap<>();

    /**
     * Initialize behavior event handler
     *
     * @param cacheManager Cache manager for device information
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry for performance monitoring
     */
    @Inject
    public BehaviorEventHandler(
            CacheManager cacheManager,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.cacheManager = cacheManager;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;

        // Configure circuit breaker for CacheManager interactions
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Trip circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(10)) // Wait 10 seconds before attempting again
                .slidingWindowSize(10) // Consider last 10 calls for failure rate
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .minimumNumberOfCalls(5) // Minimum calls before calculating failure rate
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("behaviorEventCacheManager");

        // Configure retry with exponential backoff for transient failures
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Maximum 3 retry attempts
                .waitDuration(Duration.ofMillis(500)) // Initial wait duration
                .retryExceptions(Exception.class) // Retry on all exceptions
                .exponentialBackoff(Duration.ofMillis(500), Duration.ofSeconds(5), 2.0) // Exponential backoff
                .build();

        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry("behaviorEventRetry");

        // Register circuit breaker and retry events for monitoring
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker state changed: {}", event.getStateTransition()));

        retry.getEventPublisher()
                .onRetry(event -> LOGGER.info("Retry attempt: {}, elapsed time: {}", 
                        event.getNumberOfRetryAttempts(), event.getElapsedDuration()));
    }

    /**
     * Analyze position for behavior events
     *
     * @param position Position to analyze
     * @param device Device associated with the position
     * @return True if position was analyzed successfully
     */
    @Override
    protected boolean analyzePosition(Position position, Device device) {
        String correlationId = position.getString("correlationId");
        if (correlationId == null) {
            correlationId = "unknown-" + System.currentTimeMillis();
        }

        // Create a span for behavior event detection
        Span span = tracer.spanBuilder("behavior.event.detection")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(CORRELATION_ID_KEY, correlationId)
                .setAttribute(DEVICE_ID_KEY, String.valueOf(device.getId()))
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            // Check for harsh acceleration
            if (position.hasAttribute(Position.KEY_ACCELERATION) && 
                    position.getDouble(Position.KEY_ACCELERATION) > 0.5) {
                return handleBehaviorEvent(position, device, Event.TYPE_BEHAVIOR_HARSH_ACCELERATION, correlationId, span);
            }

            // Check for harsh braking
            if (position.hasAttribute(Position.KEY_ACCELERATION) && 
                    position.getDouble(Position.KEY_ACCELERATION) < -0.5) {
                return handleBehaviorEvent(position, device, Event.TYPE_BEHAVIOR_HARSH_BRAKING, correlationId, span);
            }

            // Check for harsh cornering
            if (position.hasAttribute(Position.KEY_ROTATION) && 
                    Math.abs(position.getDouble(Position.KEY_ROTATION)) > 0.3) {
                return handleBehaviorEvent(position, device, Event.TYPE_BEHAVIOR_HARSH_CORNERING, correlationId, span);
            }

            span.setStatus(StatusCode.OK);
            return false; // No behavior events detected
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error analyzing position for behavior events: {}", e.getMessage(), e);
            return false;
        } finally {
            span.end();
        }
    }

    /**
     * Handle a detected behavior event
     *
     * @param position Position data
     * @param device Device information
     * @param eventType Type of behavior event
     * @param correlationId Correlation ID for request tracking
     * @param parentSpan Parent span for tracing
     * @return True if event was handled successfully
     */
    private boolean handleBehaviorEvent(Position position, Device device, String eventType, 
                                       String correlationId, Span parentSpan) {
        // Create a child span for the specific behavior event
        Span span = tracer.spanBuilder("behavior.event." + eventType)
                .setParent(Context.current().with(parentSpan))
                .setAttribute(BEHAVIOR_TYPE_KEY, eventType)
                .setAttribute(DEVICE_ID_KEY, String.valueOf(device.getId()))
                .setAttribute(CORRELATION_ID_KEY, correlationId)
                .startSpan();

        // Record metrics for behavior event detection
        Timer timer = getOrCreateTimer(eventType);
        Timer.Sample sample = Timer.start(meterRegistry);

        try (Scope scope = span.makeCurrent()) {
            // Create and process the event with circuit breaker and retry pattern
            Event event = new Event(Event.TYPE_BEHAVIOR, position.getDeviceId(), position.getId());
            event.set("type", eventType);
            event.set("correlationId", correlationId);

            // Use circuit breaker and retry pattern for processing the event
            boolean result = Retry.decorateSupplier(retry, 
                    CircuitBreaker.decorateSupplier(circuitBreaker, 
                            () -> processEventWithCache(event, position, device, correlationId, span)))
                    .get();

            span.setStatus(StatusCode.OK);
            return result;
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Failed to process behavior event: {}", e.getMessage(), e);
            return false;
        } finally {
            sample.stop(timer);
            span.end();
            
            // Increment counter for behavior event type
            meterRegistry.counter("behavior.events", "type", eventType).increment();
        }
    }

    /**
     * Process event with cache manager access
     *
     * @param event Event to process
     * @param position Position data
     * @param device Device information
     * @param correlationId Correlation ID for request tracking
     * @param span Current span for tracing
     * @return True if event was processed successfully
     */
    private boolean processEventWithCache(Event event, Position position, Device device, 
                                         String correlationId, Span span) {
        try {
            // Get device-specific behavior settings from cache
            Map<String, Object> deviceAttributes = cacheManager.getDeviceObjects(device.getId());
            if (deviceAttributes != null) {
                // Apply device-specific thresholds if available
                applyDeviceThresholds(event, deviceAttributes);
            }

            // Add additional context to the event
            event.set("speed", position.getSpeed());
            event.set("correlationId", correlationId);

            // Process the event through the callback
            return analyzeEvent(event, position);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Failed to process event with cache: " + e.getMessage());
            throw e; // Re-throw for circuit breaker and retry to handle
        }
    }

    /**
     * Apply device-specific thresholds to the event
     *
     * @param event Event to modify
     * @param deviceAttributes Device attributes from cache
     */
    private void applyDeviceThresholds(Event event, Map<String, Object> deviceAttributes) {
        String eventType = event.getString("type");
        if (eventType == null) {
            return;
        }

        switch (eventType) {
            case Event.TYPE_BEHAVIOR_HARSH_ACCELERATION:
                if (deviceAttributes.containsKey("accelerationThreshold")) {
                    event.set("threshold", deviceAttributes.get("accelerationThreshold"));
                }
                break;
            case Event.TYPE_BEHAVIOR_HARSH_BRAKING:
                if (deviceAttributes.containsKey("brakingThreshold")) {
                    event.set("threshold", deviceAttributes.get("brakingThreshold"));
                }
                break;
            case Event.TYPE_BEHAVIOR_HARSH_CORNERING:
                if (deviceAttributes.containsKey("corneringThreshold")) {
                    event.set("threshold", deviceAttributes.get("corneringThreshold"));
                }
                break;
            default:
                // No specific threshold for this event type
                break;
        }
    }

    /**
     * Get or create a timer for measuring behavior event processing time
     *
     * @param eventType Type of behavior event
     * @return Timer for the specified event type
     */
    private Timer getOrCreateTimer(String eventType) {
        return behaviorTimers.computeIfAbsent(eventType, 
                type -> Timer.builder("behavior.event.processing")
                        .tag("type", type)
                        .description("Time taken to process behavior events")
                        .register(meterRegistry));
    }
}