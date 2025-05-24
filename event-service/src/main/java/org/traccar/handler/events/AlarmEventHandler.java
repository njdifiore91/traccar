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
import org.traccar.messaging.EventProducer;
import org.traccar.model.Device;
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
 * Handler for alarm events.
 * 
 * This class detects when a device reports an alarm and generates appropriate events.
 * It includes circuit breaker pattern for resilience, OpenTelemetry instrumentation
 * for observability, and retry mechanisms for transient failures.
 */
public class AlarmEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlarmEventHandler.class);
    
    private static final String CIRCUIT_BREAKER_NAME = "alarmEventHandler";
    private static final String RETRY_NAME = "alarmEventHandler";
    private static final String CORRELATION_ID_KEY = "correlationId";
    
    private final CacheManager cacheManager;
    private final Storage storage;
    private final CircuitBreaker cacheCircuitBreaker;
    private final CircuitBreaker storageCircuitBreaker;
    private final Retry cacheRetry;
    private final Retry storageRetry;
    private final Tracer tracer;
    
    // Metrics
    private final Timer processingTimer;
    private final Counter alarmEventCounter;
    private final Counter failedDetectionCounter;
    private final Map<String, Counter> alarmTypeCounters;
    
    /**
     * Constructs the AlarmEventHandler with necessary dependencies.
     *
     * @param cacheManager Cache manager for device data access
     * @param storage Storage for persistence operations
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Registry for metrics collection
     * @param eventProducer Producer for publishing events to message broker
     */
    @Inject
    public AlarmEventHandler(
            CacheManager cacheManager,
            Storage storage,
            Tracer tracer,
            MeterRegistry meterRegistry,
            EventProducer eventProducer) {
        
        super(tracer, meterRegistry, eventProducer);
        
        this.cacheManager = cacheManager;
        this.storage = storage;
        this.tracer = tracer;
        
        // Initialize circuit breakers
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(
                CircuitBreakerConfig.custom()
                        .failureRateThreshold(50)
                        .waitDurationInOpenState(Duration.ofSeconds(10))
                        .permittedNumberOfCallsInHalfOpenState(5)
                        .slidingWindowSize(10)
                        .recordExceptions(StorageException.class, TimeoutException.class)
                        .build());
        
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
        this.processingTimer = Timer.builder("alarm.processing.time")
                .description("Time taken to process position for alarm detection")
                .register(meterRegistry);
        
        this.alarmEventCounter = Counter.builder("alarm.events.total")
                .description("Total number of alarm events detected")
                .register(meterRegistry);
        
        this.failedDetectionCounter = Counter.builder("alarm.detection.failures")
                .description("Number of failed alarm detection attempts")
                .register(meterRegistry);
        
        // Create counters for specific alarm types
        this.alarmTypeCounters = Map.of(
                Position.ALARM_GENERAL, Counter.builder("alarm.events.type")
                        .description("Number of alarm events by type")
                        .tag("type", Position.ALARM_GENERAL)
                        .register(meterRegistry),
                Position.ALARM_SOS, Counter.builder("alarm.events.type")
                        .description("Number of alarm events by type")
                        .tag("type", Position.ALARM_SOS)
                        .register(meterRegistry),
                Position.ALARM_OVERSPEED, Counter.builder("alarm.events.type")
                        .description("Number of alarm events by type")
                        .tag("type", Position.ALARM_OVERSPEED)
                        .register(meterRegistry)
                // Additional alarm types can be added here
        );
    }

    @Override
    protected String getEventHandlerName() {
        return "alarm";
    }

    @Override
    protected CompletableFuture<Event> detectEvent(Position position, Span span, String correlationId) {
        // Skip position if no alarm is present
        if (!position.hasAttribute(Position.KEY_ALARM)) {
            span.addEvent("No alarm data available");
            return CompletableFuture.completedFuture(null);
        }
        
        // Get the alarm type
        String alarm = position.getString(Position.KEY_ALARM);
        span.setAttribute("alarm.type", alarm);
        
        // Get device with circuit breaker and retry for resilience
        Supplier<Device> deviceSupplier = () -> {
            Span deviceSpan = tracer.spanBuilder("get.device.info")
                    .setParent(Context.current())
                    .setAttribute(CORRELATION_ID_KEY, correlationId)
                    .startSpan();
            
            try {
                deviceSpan.setAttribute("deviceId", position.getDeviceId());
                return cacheManager.getObject(Device.class, position.getDeviceId());
            } catch (Exception e) {
                deviceSpan.recordException(e);
                deviceSpan.setStatus(StatusCode.ERROR, e.getMessage());
                throw new RuntimeException("Failed to get device", e);
            } finally {
                deviceSpan.end();
            }
        };
        
        // Apply circuit breaker and retry patterns
        Device device;
        try {
            device = Retry.decorateSupplier(cacheRetry, 
                    CircuitBreaker.decorateSupplier(cacheCircuitBreaker, deviceSupplier))
                    .get();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Failed to get device: " + e.getMessage());
            failedDetectionCounter.increment();
            LOGGER.warn("Failed to get device for alarm detection: {}", e.getMessage());
            return CompletableFuture.completedFuture(null);
        }
        
        if (device == null) {
            span.addEvent("Device not found");
            return CompletableFuture.completedFuture(null);
        }
        
        // Create alarm event
        Event event = new Event(Event.TYPE_ALARM, position.getDeviceId(), position.getId());
        event.set(Position.KEY_ALARM, alarm);
        event.set(Event.KEY_CORRELATION_ID, correlationId);
        
        // Save event with circuit breaker and retry
        Supplier<Event> eventSaveSupplier = () -> {
            Span saveSpan = tracer.spanBuilder("save.alarm.event")
                    .setParent(Context.current())
                    .setAttribute(CORRELATION_ID_KEY, correlationId)
                    .startSpan();
            
            try {
                saveSpan.setAttribute("event.type", event.getType());
                saveSpan.setAttribute("event.deviceId", event.getDeviceId());
                saveSpan.setAttribute("alarm.type", alarm);
                
                storage.addObject(event, Map.of());
                alarmEventCounter.increment();
                
                // Increment specific alarm type counter if available
                Counter typeCounter = alarmTypeCounters.get(alarm);
                if (typeCounter != null) {
                    typeCounter.increment();
                }
                
                return event;
            } catch (Exception e) {
                saveSpan.recordException(e);
                saveSpan.setStatus(StatusCode.ERROR, e.getMessage());
                throw new RuntimeException("Failed to save alarm event", e);
            } finally {
                saveSpan.end();
            }
        };
        
        try {
            Event savedEvent = Retry.decorateSupplier(storageRetry,
                    CircuitBreaker.decorateSupplier(storageCircuitBreaker, eventSaveSupplier))
                    .get();
            
            return CompletableFuture.completedFuture(savedEvent);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Failed to save event: " + e.getMessage());
            failedDetectionCounter.increment();
            LOGGER.error("Failed to save alarm event: {}", e.getMessage(), e);
            return CompletableFuture.completedFuture(null);
        }
    }
}