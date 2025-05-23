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
package org.traccar;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.storage.Storage;

import java.util.HashMap;
import java.util.Map;

/**
 * Health check controller for the Position Service.
 * 
 * This controller exposes health check endpoints for Kubernetes liveness and readiness probes.
 * The liveness probe checks if the service is running, and the readiness probe checks if the
 * service is ready to handle requests, including checking the health of dependencies like
 * the database and message broker.
 */
@Path("/health")
public class HealthController {

    private static final Logger LOGGER = LoggerFactory.getLogger(HealthController.class);

    private final Storage storage;
    private final CircuitBreakerRegistry circuitBreakerRegistry;

    /**
     * Constructs a new HealthController with the necessary dependencies.
     *
     * @param storage Storage for checking database connectivity
     * @param circuitBreakerRegistry Registry for checking circuit breaker status
     */
    @Inject
    public HealthController(Storage storage, CircuitBreakerRegistry circuitBreakerRegistry) {
        this.storage = storage;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
    }

    /**
     * Liveness probe endpoint for Kubernetes.
     * 
     * This endpoint checks if the service is running. It always returns 200 OK
     * if the service is able to handle the request.
     *
     * @return 200 OK if the service is running
     */
    @GET
    @Path("/liveness")
    @Produces(MediaType.APPLICATION_JSON)
    public Response liveness() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("timestamp", System.currentTimeMillis());
        
        return Response.ok(response).build();
    }

    /**
     * Readiness probe endpoint for Kubernetes.
     * 
     * This endpoint checks if the service is ready to handle requests, including
     * checking the health of dependencies like the database and message broker.
     *
     * @return 200 OK if the service is ready, 503 Service Unavailable otherwise
     */
    @GET
    @Path("/readiness")
    @Produces(MediaType.APPLICATION_JSON)
    public Response readiness() {
        Map<String, Object> response = new HashMap<>();
        Map<String, Object> components = new HashMap<>();
        boolean isReady = true;
        
        // Check database connectivity
        try {
            storage.getObjects(OutboxMessage.class, null);
            components.put("database", Map.of("status", "UP"));
        } catch (Exception e) {
            LOGGER.error("Database health check failed: {}", e.getMessage());
            components.put("database", Map.of(
                    "status", "DOWN",
                    "error", e.getMessage()));
            isReady = false;
        }
        
        // Check circuit breaker status
        Map<String, Object> circuitBreakers = new HashMap<>();
        for (CircuitBreaker circuitBreaker : circuitBreakerRegistry.getAllCircuitBreakers()) {
            String name = circuitBreaker.getName();
            CircuitBreaker.State state = circuitBreaker.getState();
            
            circuitBreakers.put(name, Map.of("state", state.name()));
            
            // If any circuit breaker is OPEN, the service is not ready
            if (state == CircuitBreaker.State.OPEN) {
                LOGGER.warn("Circuit breaker {} is OPEN", name);
                isReady = false;
            }
        }
        components.put("circuitBreakers", circuitBreakers);
        
        // Build the response
        response.put("status", isReady ? "UP" : "DOWN");
        response.put("components", components);
        response.put("timestamp", System.currentTimeMillis());
        
        return isReady
                ? Response.ok(response).build()
                : Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(response).build();
    }

    /**
     * Detailed health check endpoint.
     * 
     * This endpoint provides detailed health information about the service and its dependencies.
     *
     * @return 200 OK with detailed health information
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response health() {
        Map<String, Object> response = new HashMap<>();
        Map<String, Object> components = new HashMap<>();
        boolean isHealthy = true;
        
        // Check database connectivity
        try {
            storage.getObjects(OutboxMessage.class, null);
            components.put("database", Map.of("status", "UP"));
        } catch (Exception e) {
            LOGGER.error("Database health check failed: {}", e.getMessage());
            components.put("database", Map.of(
                    "status", "DOWN",
                    "error", e.getMessage()));
            isHealthy = false;
        }
        
        // Check circuit breaker status
        Map<String, Object> circuitBreakers = new HashMap<>();
        for (CircuitBreaker circuitBreaker : circuitBreakerRegistry.getAllCircuitBreakers()) {
            String name = circuitBreaker.getName();
            CircuitBreaker.State state = circuitBreaker.getState();
            CircuitBreaker.Metrics metrics = circuitBreaker.getMetrics();
            
            circuitBreakers.put(name, Map.of(
                    "state", state.name(),
                    "failureRate", metrics.getFailureRate(),
                    "slowCallRate", metrics.getSlowCallRate(),
                    "numberOfBufferedCalls", metrics.getNumberOfBufferedCalls(),
                    "numberOfFailedCalls", metrics.getNumberOfFailedCalls(),
                    "numberOfSlowCalls", metrics.getNumberOfSlowCalls(),
                    "numberOfSuccessfulCalls", metrics.getNumberOfSuccessfulCalls()));
            
            // If any circuit breaker is OPEN, the service is not healthy
            if (state == CircuitBreaker.State.OPEN) {
                isHealthy = false;
            }
        }
        components.put("circuitBreakers", circuitBreakers);
        
        // Add JVM metrics
        Runtime runtime = Runtime.getRuntime();
        Map<String, Object> jvm = new HashMap<>();
        jvm.put("totalMemory", runtime.totalMemory());
        jvm.put("freeMemory", runtime.freeMemory());
        jvm.put("maxMemory", runtime.maxMemory());
        jvm.put("availableProcessors", runtime.availableProcessors());
        components.put("jvm", jvm);
        
        // Build the response
        response.put("status", isHealthy ? "UP" : "DOWN");
        response.put("components", components);
        response.put("timestamp", System.currentTimeMillis());
        
        return Response.ok(response).build();
    }
}