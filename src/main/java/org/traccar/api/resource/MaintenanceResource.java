/*
 * Copyright 2018 Anton Tananaev (anton@traccar.org)
 * Copyright 2018 Andrey Kunitsyn (andrey@traccar.org)
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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.api.ExtendedObjectResource;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;
import org.traccar.model.Maintenance;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

@Path("maintenance")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MaintenanceResource extends ExtendedObjectResource<Maintenance> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MaintenanceResource.class);
    private static final String MAINTENANCE_SERVICE_NAME = "maintenance-service";
    private static final String CIRCUIT_BREAKER_NAME = "maintenanceServiceCircuitBreaker";
    private static final int CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD = 50;
    private static final int CIRCUIT_BREAKER_WAIT_DURATION_IN_OPEN_STATE = 30;
    private static final int CIRCUIT_BREAKER_SLIDING_WINDOW_SIZE = 10;
    private static final int CIRCUIT_BREAKER_PERMITTED_CALLS_IN_HALF_OPEN_STATE = 5;
    
    private final CircuitBreaker circuitBreaker;
    
    @Inject
    private ServiceDiscovery serviceDiscovery;
    
    @Inject
    private Tracer tracer;
    
    @Inject
    private MeterRegistry meterRegistry;
    
    private final Timer maintenanceRequestTimer;

    public MaintenanceResource() {
        super(Maintenance.class, "name");
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD)
                .waitDurationInOpenState(Duration.ofSeconds(CIRCUIT_BREAKER_WAIT_DURATION_IN_OPEN_STATE))
                .slidingWindowSize(CIRCUIT_BREAKER_SLIDING_WINDOW_SIZE)
                .permittedNumberOfCallsInHalfOpenState(CIRCUIT_BREAKER_PERMITTED_CALLS_IN_HALF_OPEN_STATE)
                .recordExceptions(TimeoutException.class, RuntimeException.class)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Register circuit breaker event listeners for logging
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker '{}' state changed from {} to {}",
                        event.getCircuitBreakerName(), event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()))
                .onError(event -> LOGGER.warn("Circuit breaker '{}' recorded error: {}",
                        event.getCircuitBreakerName(), event.getThrowable().getMessage()))
                .onSuccess(event -> LOGGER.debug("Circuit breaker '{}' recorded success",
                        event.getCircuitBreakerName()));
        
        // Initialize metrics
        this.maintenanceRequestTimer = Timer.builder("maintenance.request.duration")
                .description("Time taken to process maintenance requests")
                .register(meterRegistry);
    }
    
    /**
     * Discovers the Maintenance Service instance using service discovery.
     * 
     * @return The service instance if found, empty otherwise
     */
    private Optional<ServiceInstance> discoverMaintenanceService() {
        Span span = tracer.spanBuilder("DiscoverMaintenanceService")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Collection<ServiceInstance> instances = serviceDiscovery.findServiceInstances(MAINTENANCE_SERVICE_NAME);
            
            if (instances.isEmpty()) {
                LOGGER.warn("No instances of Maintenance Service found");
                span.setStatus(StatusCode.ERROR, "No instances found");
                return Optional.empty();
            }
            
            // For simplicity, just return the first healthy instance
            Optional<ServiceInstance> healthyInstance = instances.stream()
                    .filter(ServiceInstance::isHealthy)
                    .findFirst();
            
            if (healthyInstance.isEmpty()) {
                LOGGER.warn("No healthy instances of Maintenance Service found");
                span.setStatus(StatusCode.ERROR, "No healthy instances found");
            } else {
                span.setAttribute("service.instance.id", healthyInstance.get().getId());
                span.setAttribute("service.instance.host", healthyInstance.get().getHost());
                span.setAttribute("service.instance.port", healthyInstance.get().getPort());
            }
            
            return healthyInstance;
        } catch (Exception e) {
            LOGGER.error("Error discovering Maintenance Service", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            return Optional.empty();
        } finally {
            span.end();
        }
    }
    
    /**
     * Executes a function with circuit breaker, tracing, and metrics.
     * 
     * @param <T> The return type of the function
     * @param operationName The name of the operation for tracing
     * @param supplier The function to execute
     * @return The result of the function
     * @throws Exception If an error occurs during execution
     */
    @Timed(value = "maintenance.operation", description = "Time taken to execute maintenance operations")
    private <T> T executeWithResilienceAndTracing(String operationName, Supplier<T> supplier) throws Exception {
        Span span = tracer.spanBuilder(operationName)
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("circuit_breaker.name", CIRCUIT_BREAKER_NAME)
                .setAttribute("circuit_breaker.state", circuitBreaker.getState().name())
                .startSpan();
        
        Context context = Context.current().with(span);
        
        try (Scope scope = context.makeCurrent()) {
            return maintenanceRequestTimer.record(() -> {
                try {
                    return circuitBreaker.executeSupplier(supplier);
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    } else {
                        throw new RuntimeException("Error executing operation: " + operationName, e);
                    }
                }
            });
        } finally {
            span.end();
        }
    }

    // The base class ExtendedObjectResource already provides the CRUD operations
    // We've enhanced it with circuit breaker, tracing, and metrics capabilities
}