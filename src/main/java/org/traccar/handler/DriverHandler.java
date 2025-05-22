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
package org.traccar.handler;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Driver;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Handler for associating driver information with positions.
 * Includes circuit breaker pattern for resilient driver lookups,
 * distributed tracing, and metrics collection.
 */
public class DriverHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DriverHandler.class);

    private final CacheManager cacheManager;
    private final boolean useLinkedDriver;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final MessageProducer messageProducer;
    
    private final Counter driverAssignmentCounter;
    private final Counter driverAssignmentFailureCounter;
    private final Timer driverLookupTimer;

    /**
     * Constructs a new DriverHandler with the necessary dependencies.
     *
     * @param config Configuration for the handler
     * @param cacheManager Cache manager for driver lookups
     * @param circuitBreakerRegistry Registry for circuit breakers
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     * @param messageProducer Message broker producer for async processing
     */
    @Inject
    public DriverHandler(
            Config config,
            CacheManager cacheManager,
            CircuitBreakerRegistry circuitBreakerRegistry,
            Tracer tracer,
            MeterRegistry meterRegistry,
            MessageProducer messageProducer) {
        
        this.cacheManager = cacheManager;
        this.useLinkedDriver = config.getBoolean(Keys.PROCESSING_USE_LINKED_DRIVER);
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.messageProducer = messageProducer;
        
        // Configure circuit breaker for driver lookups
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("driverLookup", circuitBreakerConfig);
        
        // Initialize metrics
        this.driverAssignmentCounter = Counter.builder("driver.assignments")
                .description("Number of driver assignments to positions")
                .register(meterRegistry);
        
        this.driverAssignmentFailureCounter = Counter.builder("driver.assignments.failures")
                .description("Number of failed driver assignments")
                .register(meterRegistry);
        
        this.driverLookupTimer = Timer.builder("driver.lookup.time")
                .description("Time taken to look up driver information")
                .register(meterRegistry);
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("driver.lookup")
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .startSpan();
        
        try {
            Context context = Context.current().with(span);
            
            if (useLinkedDriver && !position.hasAttribute(Position.KEY_DRIVER_UNIQUE_ID)) {
                // Use circuit breaker to protect against cache failures
                Supplier<Set<Driver>> driverLookupSupplier = () -> {
                    // Use timer to measure lookup performance
                    return driverLookupTimer.record(() -> {
                        try {
                            return cacheManager.getDeviceObjects(position.getDeviceId(), Driver.class);
                        } catch (Exception e) {
                            LOGGER.warn("Error looking up driver for device {}: {}", 
                                    position.getDeviceId(), e.getMessage());
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            driverAssignmentFailureCounter.increment();
                            throw e;
                        }
                    });
                };
                
                // Execute with circuit breaker and fallback
                Set<Driver> drivers;
                try {
                    drivers = circuitBreaker.executeSupplier(driverLookupSupplier);
                } catch (Exception e) {
                    // Graceful degradation during cache unavailability
                    LOGGER.warn("Circuit breaker execution failed, using fallback for device {}", position.getDeviceId());
                    drivers = driverLookupFallback(position.getDeviceId());
                }
                
                if (!drivers.isEmpty()) {
                    Driver driver = drivers.iterator().next();
                    position.set(Position.KEY_DRIVER_UNIQUE_ID, driver.getUniqueId());
                    driverAssignmentCounter.increment();
                    span.setAttribute("driver.id", driver.getUniqueId());
                    
                    // Asynchronously publish driver assignment event to message broker
                    CompletableFuture.runAsync(() -> {
                        try {
                            Map<String, Object> eventData = new HashMap<>();
                            eventData.put("deviceId", position.getDeviceId());
                            eventData.put("driverId", driver.getUniqueId());
                            eventData.put("positionId", position.getId());
                            eventData.put("timestamp", position.getDeviceTime().getTime());
                            
                            messageProducer.publish("driver.assignments", eventData);
                        } catch (Exception e) {
                            LOGGER.error("Failed to publish driver assignment event", e);
                        }
                    });
                }
            }
            
            span.setStatus(StatusCode.OK);
            callback.processed(false);
            
        } catch (Exception e) {
            // Graceful degradation during cache unavailability
            LOGGER.error("Error processing driver information", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            callback.processed(false);
        } finally {
            span.end();
        }
    }

    /**
     * Fallback method for driver lookup when circuit breaker is open
     * @param deviceId The device ID to look up drivers for
     * @return An empty set of drivers
     */
    private Set<Driver> driverLookupFallback(long deviceId) {
        LOGGER.warn("Circuit breaker open, using fallback for driver lookup for device {}", deviceId);
        return Collections.emptySet();
    }
}