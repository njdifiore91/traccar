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

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.micrometer.core.instrument.Counter;
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
import io.opentelemetry.context.propagation.TextMapGetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.inject.Singleton;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Handler for fuel level events.
 * 
 * This class processes position data from the message broker to detect fuel-related events
 * such as fuel drops (potential theft) and refills. It uses circuit breakers for resilience
 * and implements retry mechanisms for transient failures.
 */
@Singleton
public class FuelEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FuelEventHandler.class);
    
    private static final String FUEL_LEVEL_KEY = "fuel";
    private static final String FUEL_THRESHOLD_KEY = "event.fuel.threshold";
    private static final String FUEL_DROP_EVENT = "fuelDrop";
    private static final String FUEL_REFILL_EVENT = "fuelRefill";
    
    private final double fuelThreshold; // percentage change to trigger event
    
    private final CacheManager cacheManager;
    private final EventProcessor eventProcessor;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    
    // Metrics
    private final Timer fuelEventProcessingTimer;
    private final Counter fuelDropCounter;
    private final Counter fuelRefillCounter;
    private final Counter fuelErrorCounter;
    
    // In-memory cache for last known fuel levels when cache manager fails
    private final Map<Long, Double> fallbackFuelLevels = new ConcurrentHashMap<>();

    @Autowired
    public FuelEventHandler(CacheManager cacheManager, 
                           EventProcessor eventProcessor,
                           Tracer tracer,
                           MeterRegistry meterRegistry,
                           @Value("${" + FUEL_THRESHOLD_KEY + ":10.0}") double fuelThreshold) {
        this.fuelThreshold = fuelThreshold;
        this.cacheManager = cacheManager;
        this.eventProcessor = eventProcessor;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.fuelEventProcessingTimer = Timer.builder("fuel.event.processing.time")
                .description("Time taken to process fuel events")
                .register(meterRegistry);
        
        this.fuelDropCounter = Counter.builder("fuel.event.count")
                .tag("type", "drop")
                .description("Number of fuel drop events detected")
                .register(meterRegistry);
        
        this.fuelRefillCounter = Counter.builder("fuel.event.count")
                .tag("type", "refill")
                .description("Number of fuel refill events detected")
                .register(meterRegistry);
        
        this.fuelErrorCounter = Counter.builder("fuel.event.errors")
                .description("Number of errors during fuel event processing")
                .register(meterRegistry);
    }

    /**
     * Processes position data from the message broker to detect fuel events.
     * 
     * @param position The position data containing fuel information
     */
    @KafkaListener(topics = "${kafka.topics.enriched-positions}", groupId = "${kafka.consumer.groups.fuel-events}")
    public void processPosition(Position position) {
        String correlationId = position.getCorrelationId();
        Span span = tracer.spanBuilder("FuelEventHandler.processPosition")
                .setParent(Context.current().with(Span.current()))
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(AttributeKey.longKey("deviceId"), position.getDeviceId())
                .setAttribute(AttributeKey.stringKey("correlationId"), correlationId)
                .setAttribute(AttributeKey.stringKey("service.name"), "event-service")
                .setAttribute(AttributeKey.stringKey("event.type"), "fuel")
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            LOGGER.debug("Processing position for fuel events: deviceId={}, correlationId={}", 
                    position.getDeviceId(), correlationId);
            
            // Use timer to measure processing time
            fuelEventProcessingTimer.record(() -> {
                try {
                    processPositionInternal(position);
                } catch (Exception e) {
                    LOGGER.error("Error in processPositionInternal for deviceId={}: {}", 
                            position.getDeviceId(), e.getMessage(), e);
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    fuelErrorCounter.increment();
                    throw e; // Re-throw to be handled by the outer try-catch
                }
            });
        } catch (Exception e) {
            LOGGER.error("Error processing fuel event for deviceId={}: {}", 
                    position.getDeviceId(), e.getMessage(), e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            fuelErrorCounter.increment();
        } finally {
            span.end();
        }
    }

    private void processPositionInternal(Position position) {
        if (!position.hasAttribute(FUEL_LEVEL_KEY)) {
            return; // Skip positions without fuel data
        }
        
        long deviceId = position.getDeviceId();
        double currentFuel = position.getDouble(FUEL_LEVEL_KEY);
        
        // Get previous fuel level with circuit breaker protection
        Double previousFuel = getPreviousFuelLevel(deviceId);
        
        // Store current fuel level for future comparisons
        storeFuelLevel(deviceId, currentFuel);
        
        // Skip if we don't have a previous reading to compare
        if (previousFuel == null) {
            return;
        }
        
        // Calculate fuel change
        double fuelChange = currentFuel - previousFuel;
        double fuelChangePercent = Math.abs(fuelChange) * 100.0 / previousFuel;
        
        // Check if change exceeds threshold
        if (fuelChangePercent >= fuelThreshold) {
            if (fuelChange < 0) {
                // Fuel drop detected
                LOGGER.info("Fuel drop detected: deviceId={}, change={}%, from={} to={}", 
                        deviceId, String.format("%.2f", fuelChangePercent), previousFuel, currentFuel);
                createFuelEvent(position, FUEL_DROP_EVENT, fuelChange);
                fuelDropCounter.increment();
            } else {
                // Fuel refill detected
                LOGGER.info("Fuel refill detected: deviceId={}, change={}%, from={} to={}", 
                        deviceId, String.format("%.2f", fuelChangePercent), previousFuel, currentFuel);
                createFuelEvent(position, FUEL_REFILL_EVENT, fuelChange);
                fuelRefillCounter.increment();
            }
        }
    }

    /**
     * Gets the previous fuel level for a device with circuit breaker protection.
     * Falls back to in-memory cache if the cache manager fails.
     * 
     * @param deviceId The device ID
     * @return The previous fuel level or null if not available
     */
    @CircuitBreaker(name = "cacheManager", fallbackMethod = "getPreviousFuelLevelFallback")
    @Retry(name = "cacheManager", fallbackMethod = "getPreviousFuelLevelFallback")
    private Double getPreviousFuelLevel(long deviceId) {
        String key = "fuel:" + deviceId;
        Double value = cacheManager.get(key, Double.class);
        
        // Update fallback cache if we got a value
        if (value != null) {
            fallbackFuelLevels.put(deviceId, value);
        }
        
        return value;
    }

    /**
     * Fallback method for getting previous fuel level when cache manager fails.
     * Uses in-memory map as a fallback cache.
     * 
     * @param deviceId The device ID
     * @param e The exception that triggered the fallback
     * @return The previous fuel level from fallback cache or null if not available
     */
    private Double getPreviousFuelLevelFallback(long deviceId, Exception e) {
        LOGGER.warn("Using fallback for fuel level cache: deviceId={}, error={}", deviceId, e.getMessage());
        Span span = Span.current();
        span.addEvent("cache_fallback_used", Attributes.of(
                AttributeKey.longKey("deviceId"), deviceId,
                AttributeKey.stringKey("error"), e.getMessage()));
        return fallbackFuelLevels.get(deviceId);
    }

    /**
     * Stores the current fuel level in the cache with circuit breaker protection.
     * 
     * @param deviceId The device ID
     * @param fuelLevel The current fuel level
     */
    @CircuitBreaker(name = "cacheManager", fallbackMethod = "storeFuelLevelFallback")
    @Retry(name = "cacheManager", fallbackMethod = "storeFuelLevelFallback")
    private void storeFuelLevel(long deviceId, double fuelLevel) {
        String key = "fuel:" + deviceId;
        cacheManager.set(key, fuelLevel);
        
        // Update fallback cache
        fallbackFuelLevels.put(deviceId, fuelLevel);
    }

    /**
     * Fallback method for storing fuel level when cache manager fails.
     * Updates only the in-memory fallback cache.
     * 
     * @param deviceId The device ID
     * @param fuelLevel The current fuel level
     * @param e The exception that triggered the fallback
     */
    private void storeFuelLevelFallback(long deviceId, double fuelLevel, Exception e) {
        LOGGER.warn("Using fallback for storing fuel level: deviceId={}, error={}", deviceId, e.getMessage());
        Span span = Span.current();
        span.addEvent("cache_store_fallback_used", Attributes.of(
                AttributeKey.longKey("deviceId"), deviceId,
                AttributeKey.doubleKey("fuelLevel"), fuelLevel,
                AttributeKey.stringKey("error"), e.getMessage()));
        fallbackFuelLevels.put(deviceId, fuelLevel);
    }

    /**
     * Creates and processes a fuel event.
     * 
     * @param position The position data
     * @param eventType The type of fuel event (fuelDrop or fuelRefill)
     * @param fuelChange The amount of fuel change
     */
    private void createFuelEvent(Position position, String eventType, double fuelChange) {
        Span span = tracer.spanBuilder("FuelEventHandler.createFuelEvent")
                .setParent(Context.current())
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(AttributeKey.longKey("deviceId"), position.getDeviceId())
                .setAttribute(AttributeKey.stringKey("correlationId"), position.getCorrelationId())
                .setAttribute(AttributeKey.stringKey("eventType"), eventType)
                .setAttribute(AttributeKey.doubleKey("fuelChange"), fuelChange)
                .startSpan();
                
        try (Scope scope = span.makeCurrent()) {
            Event event = new Event(eventType, position);
            event.set("fuelChange", fuelChange);
            
            // Propagate correlation ID for cross-service tracking
            event.setCorrelationId(position.getCorrelationId());
            
            // Process the event (will be published to the message broker)
            eventProcessor.processEvent(event);
            
            LOGGER.debug("Created and processed {} event: deviceId={}, fuelChange={}, correlationId={}",
                    eventType, position.getDeviceId(), fuelChange, position.getCorrelationId());
        } catch (Exception e) {
            LOGGER.error("Error creating fuel event: {}", e.getMessage(), e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            fuelErrorCounter.increment();
        } finally {
            span.end();
        }
    }
}