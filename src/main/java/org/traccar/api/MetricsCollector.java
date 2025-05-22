/*
 * Copyright 2022 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.api;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.Response;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Collects and exposes API performance metrics for monitoring and alerting.
 * Tracks request rates, response times, error rates, and circuit breaker states
 * using OpenTelemetry, and exposes these metrics through a /metrics endpoint for
 * Prometheus scraping.
 */
@Singleton
public class MetricsCollector {

    private final MeterRegistry meterRegistry;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final OpenTelemetry openTelemetry;
    private final Meter meter;
    
    private final Map<String, Timer> endpointTimers = new ConcurrentHashMap<>();
    private final Map<String, Counter> errorCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> requestCounters = new ConcurrentHashMap<>();
    private final LongCounter totalRequestsCounter;
    private final LongCounter totalErrorsCounter;
    
    /**
     * Initializes the metrics collector with required dependencies.
     *
     * @param meterRegistry Micrometer registry for exposing metrics to Prometheus
     * @param circuitBreakerRegistry Registry containing all circuit breakers
     * @param openTelemetry OpenTelemetry API for metrics collection
     */
    @Inject
    public MetricsCollector(
            MeterRegistry meterRegistry,
            CircuitBreakerRegistry circuitBreakerRegistry,
            OpenTelemetry openTelemetry) {
        this.meterRegistry = meterRegistry;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.openTelemetry = openTelemetry;
        this.meter = openTelemetry.getMeter("org.traccar.api");
        
        // Initialize JVM and system metrics
        new JvmMemoryMetrics().bindTo(meterRegistry);
        new JvmThreadMetrics().bindTo(meterRegistry);
        new ProcessorMetrics().bindTo(meterRegistry);
        
        // Initialize global counters
        totalRequestsCounter = meter.counterBuilder("api.requests.total")
                .setDescription("Total number of API requests")
                .build();
        
        totalErrorsCounter = meter.counterBuilder("api.errors.total")
                .setDescription("Total number of API errors")
                .build();
        
        // Initialize circuit breaker metrics
        initializeCircuitBreakerMetrics();
    }
    
    /**
     * Initializes metrics for monitoring circuit breaker states.
     */
    private void initializeCircuitBreakerMetrics() {
        circuitBreakerRegistry.getAllCircuitBreakers().forEach(circuitBreaker -> {
            String name = circuitBreaker.getName();
            
            // Register gauge for circuit breaker state
            Gauge.builder("circuit_breaker.state", circuitBreaker, cb -> cb.getState().getOrder())
                    .tag("name", name)
                    .description("Circuit breaker state (0-CLOSED, 1-OPEN, 2-HALF_OPEN)")
                    .register(meterRegistry);
            
            // Register gauge for failure rate
            Gauge.builder("circuit_breaker.failure_rate", circuitBreaker, CircuitBreaker::getFailureRate)
                    .tag("name", name)
                    .description("Circuit breaker failure rate percentage")
                    .register(meterRegistry);
            
            // Register gauge for slow call rate
            Gauge.builder("circuit_breaker.slow_call_rate", circuitBreaker, CircuitBreaker::getSlowCallRate)
                    .tag("name", name)
                    .description("Circuit breaker slow call rate percentage")
                    .register(meterRegistry);
            
            // Register counters for successful and failed calls
            Gauge.builder("circuit_breaker.successful_calls", circuitBreaker, cb -> cb.getMetrics().getNumberOfSuccessfulCalls())
                    .tag("name", name)
                    .description("Number of successful calls through circuit breaker")
                    .register(meterRegistry);
            
            Gauge.builder("circuit_breaker.failed_calls", circuitBreaker, cb -> cb.getMetrics().getNumberOfFailedCalls())
                    .tag("name", name)
                    .description("Number of failed calls through circuit breaker")
                    .register(meterRegistry);
        });
    }
    
    /**
     * Records metrics for an API request.
     *
     * @param requestContext The container request context
     */
    public void recordRequest(ContainerRequestContext requestContext) {
        String endpoint = requestContext.getUriInfo().getPath();
        String method = requestContext.getMethod();
        String metricKey = method + "_" + endpoint;
        
        // Increment request counter
        requestCounters.computeIfAbsent(metricKey, k -> Counter.builder("api.requests")
                .tag("endpoint", endpoint)
                .tag("method", method)
                .description("Number of requests to this endpoint")
                .register(meterRegistry))
                .increment();
        
        totalRequestsCounter.add(1, Attributes.builder()
                .put("endpoint", endpoint)
                .put("method", method)
                .build());
    }
    
    /**
     * Records metrics for an API response, including response time and status code.
     *
     * @param requestContext The container request context
     * @param responseContext The container response context
     * @param startTime The request start time in nanoseconds
     */
    public void recordResponse(
            ContainerRequestContext requestContext,
            ContainerResponseContext responseContext,
            long startTime) {
        
        String endpoint = requestContext.getUriInfo().getPath();
        String method = requestContext.getMethod();
        int statusCode = responseContext.getStatus();
        String metricKey = method + "_" + endpoint;
        long duration = System.nanoTime() - startTime;
        
        // Record response time
        endpointTimers.computeIfAbsent(metricKey, k -> Timer.builder("api.request.duration")
                .tag("endpoint", endpoint)
                .tag("method", method)
                .description("API request duration")
                .register(meterRegistry))
                .record(duration, TimeUnit.NANOSECONDS);
        
        // Record error metrics if status code is 4xx or 5xx
        if (statusCode >= Response.Status.BAD_REQUEST.getStatusCode()) {
            String errorKey = metricKey + "_" + statusCode;
            errorCounters.computeIfAbsent(errorKey, k -> Counter.builder("api.errors")
                    .tag("endpoint", endpoint)
                    .tag("method", method)
                    .tag("status", String.valueOf(statusCode))
                    .description("Number of error responses")
                    .register(meterRegistry))
                    .increment();
            
            totalErrorsCounter.add(1, Attributes.builder()
                    .put("endpoint", endpoint)
                    .put("method", method)
                    .put("status", String.valueOf(statusCode))
                    .build());
        }
    }
    
    /**
     * Records metrics for WebSocket connections.
     *
     * @param action The action (connect, disconnect, message)
     */
    public void recordWebSocketMetric(String action) {
        Counter.builder("websocket.events")
                .tag("action", action)
                .description("WebSocket connection events")
                .register(meterRegistry)
                .increment();
        
        meter.counterBuilder("websocket.events.total")
                .setDescription("Total WebSocket events")
                .build()
                .add(1, Attributes.builder()
                        .put("action", action)
                        .build());
    }
    
    /**
     * Updates circuit breaker metrics when a circuit breaker state changes.
     *
     * @param circuitBreaker The circuit breaker that changed state
     */
    public void updateCircuitBreakerMetrics(CircuitBreaker circuitBreaker) {
        // This method can be called when circuit breaker state changes
        // The gauges will automatically reflect the new state on the next scrape
        // No additional action needed as gauges pull current values when scraped
    }
}