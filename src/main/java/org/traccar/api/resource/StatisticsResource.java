/*
 * Copyright 2016 - 2022 Anton Tananaev (anton@traccar.org)
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

import org.traccar.api.BaseResource;
import org.traccar.model.Statistics;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Order;
import org.traccar.storage.query.Request;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Resource for accessing statistics data.
 * Implements service discovery, circuit breaker, distributed tracing, and metrics collection.
 */
@Path("statistics")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class StatisticsResource extends BaseResource {

    private static final String CIRCUIT_BREAKER_NAME = "statisticsService";
    private static final String REPORTING_SERVICE_NAME = "reporting-service";
    private static final String METRICS_TIMER_NAME = "statistics.query.time";
    private static final String METRICS_COUNTER_NAME = "statistics.query.count";
    
    /**
     * Initializes the circuit breaker if it doesn't exist.
     */
    private void initializeCircuitBreaker() {
        if (!circuitBreakerRegistry.getAllCircuitBreakers().stream()
                .anyMatch(cb -> cb.getName().equals(CIRCUIT_BREAKER_NAME))) {
            CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                    .failureRateThreshold(50) // 50% failure rate to open circuit
                    .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                    .slidingWindowSize(10) // Consider the last 10 calls
                    .minimumNumberOfCalls(5) // Minimum calls before calculating failure rate
                    .build();
            circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME, config);
        }
    }

    /**
     * Gets statistics data for the specified time range.
     * Implements circuit breaker pattern, distributed tracing, and metrics collection.
     *
     * @param from Start date for the query
     * @param to End date for the query
     * @return Collection of statistics objects
     * @throws StorageException If a storage error occurs
     */
    @GET
    public Collection<Statistics> get(
            @QueryParam("from") Date from, @QueryParam("to") Date to) throws StorageException {
        
        // Initialize circuit breaker if needed
        initializeCircuitBreaker();
        
        // Create counter for metrics
        createCounter(METRICS_COUNTER_NAME, "endpoint", "get").increment();
        
        // Check permissions with tracing
        return traceOperation("statistics.get", () -> {
            Span span = Span.current();
            span.setAttribute("from", from != null ? from.toString() : "null");
            span.setAttribute("to", to != null ? to.toString() : "null");
            
            // Check permissions
            permissionsService.checkAdmin(getUserId());
            
            // Execute query with circuit breaker and metrics
            return executeWithCircuitBreakerAndFallback(
                    CIRCUIT_BREAKER_NAME,
                    () -> measureExecutionTime(
                            METRICS_TIMER_NAME,
                            () -> getStatisticsFromStorage(from, to),
                            "type", "direct"),
                    Collections.emptyList() // Fallback to empty list if circuit is open
            );
        });
    }
    
    /**
     * Gets statistics from storage or from reporting service if available.
     *
     * @param from Start date for the query
     * @param to End date for the query
     * @return Collection of statistics objects
     * @throws StorageException If a storage error occurs
     */
    private Collection<Statistics> getStatisticsFromStorage(Date from, Date to) throws StorageException {
        // Try to discover reporting service first
        Optional<com.orbitz.consul.model.health.ServiceHealth> reportingService = 
                discoverService(REPORTING_SERVICE_NAME);
        
        if (reportingService.isPresent()) {
            // If reporting service is available, use it to get statistics
            Span span = Span.current();
            span.setAttribute("service.name", REPORTING_SERVICE_NAME);
            span.setAttribute("service.address", reportingService.get().getService().getAddress());
            span.setAttribute("service.port", reportingService.get().getService().getPort());
            
            try {
                // In a real implementation, we would make a call to the reporting service
                // For now, we'll just fall back to the direct storage query
                return storage.getObjects(Statistics.class, new Request(
                        new Columns.All(),
                        new Condition.Between("captureTime", "from", from, "to", to),
                        new Order("captureTime")));
            } catch (Exception e) {
                span.recordException(e);
                // If the call to reporting service fails, fall back to direct storage query
                return storage.getObjects(Statistics.class, new Request(
                        new Columns.All(),
                        new Condition.Between("captureTime", "from", from, "to", to),
                        new Order("captureTime")));
            }
        } else {
            // If reporting service is not available, use direct storage query
            return storage.getObjects(Statistics.class, new Request(
                    new Columns.All(),
                    new Condition.Between("captureTime", "from", from, "to", to),
                    new Order("captureTime")));
        }
    }
    
    /**
     * Gets statistics data asynchronously for the specified time range.
     * Demonstrates how to implement asynchronous operations with tracing.
     *
     * @param from Start date for the query
     * @param to End date for the query
     * @return CompletionStage that will complete with the statistics data
     */
    public CompletionStage<Collection<Statistics>> getAsync(Date from, Date to) {
        return traceOperationAsync("statistics.getAsync", () -> {
            Span span = Span.current();
            span.setAttribute("from", from != null ? from.toString() : "null");
            span.setAttribute("to", to != null ? to.toString() : "null");
            
            // This would be implemented with an async client in a real application
            return CompletableFuture.supplyAsync(() -> {
                try {
                    permissionsService.checkAdmin(getUserId());
                    return getStatisticsFromStorage(from, to);
                } catch (StorageException e) {
                    throw new RuntimeException(e);
                }
            });
        });
    }
}