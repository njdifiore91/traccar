/*
 * Copyright 2015 - 2019 Anton Tananaev (anton@traccar.org)
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
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Error handler for REST API resources that includes distributed tracing information,
 * metrics collection, and implements circuit breaker pattern for error handling.
 */
public class ResourceErrorHandler implements ExceptionMapper<Exception> {

    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final CircuitBreaker circuitBreaker;
    private final Timer responseTimer;
    
    @Inject
    public ResourceErrorHandler(MeterRegistry meterRegistry, Tracer tracer) {
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        
        // Initialize circuit breaker with custom configuration
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before attempting to close
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .slidingWindowSize(10) // Consider last 10 calls for failure rate calculation
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("apiErrorHandler");
        
        // Initialize response timer for metrics
        this.responseTimer = Timer.builder("api.error.response.time")
                .description("Time taken to process error responses")
                .register(meterRegistry);
    }

    @Override
    public Response toResponse(Exception exception) {
        // Start timing the error response
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            // Record the exception in the circuit breaker
            return circuitBreaker.executeSupplier(() -> {
                // Get current span from OpenTelemetry context
                Span currentSpan = Span.current();
                
                // Mark the span as error and add exception details
                currentSpan.setStatus(StatusCode.ERROR);
                currentSpan.recordException(exception);
                
                // Get trace and span IDs for the response
                String traceId = currentSpan.getSpanContext().getTraceId();
                String spanId = currentSpan.getSpanContext().getSpanId();
                
                StringWriter stringWriter = new StringWriter();
                PrintWriter printWriter = new PrintWriter(stringWriter);
                exception.printStackTrace(printWriter);
                
                // Create error response with tracing information
                Map<String, String> errorMetadata = new HashMap<>();
                errorMetadata.put("traceId", traceId);
                errorMetadata.put("spanId", spanId);
                errorMetadata.put("service", "api-gateway");
                
                // Record metrics for the error
                Counter.builder("api.errors")
                        .tag("exception", exception.getClass().getSimpleName())
                        .tag("service", "api-gateway")
                        .register(meterRegistry)
                        .increment();
                
                if (exception instanceof WebApplicationException webException) {
                    return Response.fromResponse(webException.getResponse())
                            .entity(stringWriter.toString())
                            .header("X-Trace-ID", traceId)
                            .header("X-Span-ID", spanId)
                            .header("X-Error-Metadata", errorMetadata.toString())
                            .build();
                } else {
                    return Response.status(Response.Status.BAD_REQUEST)
                            .entity(stringWriter.toString())
                            .header("X-Trace-ID", traceId)
                            .header("X-Span-ID", spanId)
                            .header("X-Error-Metadata", errorMetadata.toString())
                            .build();
                }
            });
        } catch (Exception e) {
            // Circuit breaker is open or another error occurred during error handling
            // Provide a fallback response with minimal information
            Span currentSpan = Span.current();
            String traceId = currentSpan.getSpanContext().getTraceId();
            String spanId = currentSpan.getSpanContext().getSpanId();
            
            // Record circuit breaker metrics
            Counter.builder("api.circuit_breaker.fallback")
                    .tag("service", "api-gateway")
                    .register(meterRegistry)
                    .increment();
            
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("Service temporarily unavailable. Please try again later.")
                    .header("X-Trace-ID", traceId)
                    .header("X-Span-ID", spanId)
                    .build();
        } finally {
            // Record the time taken to process the error
            sample.stop(responseTimer);
        }
    }
}