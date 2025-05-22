/*
 * Copyright 2017 Anton Tananaev (anton@traccar.org)
 * Copyright 2017 Andrey Kunitsyn (andrey@traccar.org)
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
package org.traccar.api.resource;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.LongCounter;

import org.traccar.api.ExtendedObjectResource;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.model.Driver;

import java.time.Duration;

/**
 * REST endpoint for Driver operations.
 * Enhanced with service discovery, circuit breaker, distributed tracing, and metrics.
 */
@Path("drivers")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DriverResource extends ExtendedObjectResource<Driver> {

    private final ServiceDiscovery serviceDiscovery;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final LongCounter requestCounter;
    
    /**
     * Constructor with dependency injection for resilience and observability components.
     * 
     * @param serviceDiscovery Service discovery client for locating backend services
     * @param circuitBreakerRegistry Registry for circuit breaker configuration
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meter OpenTelemetry meter for metrics collection
     */
    @Inject
    public DriverResource(
            ServiceDiscovery serviceDiscovery,
            CircuitBreakerRegistry circuitBreakerRegistry,
            Tracer tracer,
            Meter meter) {
        super(Driver.class, "name");
        
        this.serviceDiscovery = serviceDiscovery;
        this.tracer = tracer;
        
        // Configure circuit breaker with custom settings
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds in OPEN state
                .slidingWindowSize(10) // Consider last 10 calls for failure rate
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in HALF_OPEN state
                .build();
        
        // Create or get circuit breaker for this resource
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("driverResource", circuitBreakerConfig);
        
        // Create metrics counter for tracking API calls
        this.requestCounter = meter.counterBuilder("driver.requests")
                .setDescription("Number of requests to the Driver resource")
                .build();
    }
    
    /**
     * Override the base method to add circuit breaker, tracing, and metrics.
     * This is a template method that will be applied to all resource operations.
     * Specific implementations can be added for individual methods as needed.
     */
    // This method assumes ExtendedObjectResource has a beforeExecute method to override
    // If not, this would need to be implemented differently, such as by overriding specific HTTP methods
    @Override
    protected void beforeExecute() {
        // Create and start a new span for tracing
        Span span = tracer.spanBuilder("DriverResource.execute")
                .setAttribute("resource.type", "driver")
                .startSpan();
        
        try {
            // Increment request counter for metrics
            requestCounter.add(1);
            
            // Use service discovery to locate any required backend services
            // This is a placeholder - actual service discovery would be implemented
            // based on the specific services needed by this resource
            String serviceUrl = serviceDiscovery.findService("driver-service");
            
            // Execute operations through the circuit breaker to provide resilience
            // This is a demonstration of how circuit breaker would be used
            // Actual implementation would wrap specific service calls
            circuitBreaker.executeRunnable(() -> {
                // Service operations would be executed here
                // For example: driverService.validateDriver(...);
            });
            
            // Additional pre-execution logic can be added here
        } catch (Exception e) {
            // Record exception in the current span
            span.recordException(e);
            throw e;
        } finally {
            // End the span
            span.end();
        }
    }
}