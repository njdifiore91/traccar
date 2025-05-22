/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.forward;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.InvocationCallback;
import jakarta.ws.rs.core.Response;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class EventForwarderJson implements EventForwarder {

    private final String url;
    private final String header;
    private final Client client;
    private final ServiceDiscovery serviceDiscovery;
    
    // OpenTelemetry components
    private final Tracer tracer;
    private final Meter meter;
    private final LongCounter requestCounter;
    private final LongCounter successCounter;
    private final LongCounter errorCounter;
    
    // Resilience4j components
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    
    // TextMapSetter for propagating trace context
    private static final TextMapSetter<jakarta.ws.rs.client.Invocation.Builder> SETTER = 
            (carrier, key, value) -> carrier.header(key, value);

    public EventForwarderJson(Config config, Client client) {
        this.client = client;
        this.url = config.getString(Keys.EVENT_FORWARD_URL);
        this.header = config.getString(Keys.EVENT_FORWARD_HEADERS);
        
        // Initialize service discovery
        this.serviceDiscovery = new ServiceDiscovery(config);
        
        // Initialize OpenTelemetry components
        this.tracer = GlobalOpenTelemetry.getTracer("org.traccar.forward.EventForwarderJson");
        this.meter = GlobalOpenTelemetry.getMeter("org.traccar.forward.EventForwarderJson");
        
        // Create metrics
        this.requestCounter = meter.counterBuilder("event_forwarder_requests")
                .setDescription("Number of event forwarding requests")
                .build();
        this.successCounter = meter.counterBuilder("event_forwarder_success")
                .setDescription("Number of successful event forwarding requests")
                .build();
        this.errorCounter = meter.counterBuilder("event_forwarder_errors")
                .setDescription("Number of failed event forwarding requests")
                .build();
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // When 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .slidingWindowSize(10) // Consider the last 10 calls
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .build();
        this.circuitBreaker = CircuitBreaker.of("eventForwarderCircuitBreaker", circuitBreakerConfig);
        
        // Configure retry
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Try up to 3 times
                .waitDuration(Duration.ofMillis(500)) // Wait 500ms between retries
                .retryExceptions(RuntimeException.class) // Retry on RuntimeException
                .build();
        this.retry = Retry.of("eventForwarderRetry", retryConfig);
    }

    @Override
    public void forward(EventData eventData, ResultHandler resultHandler) {
        // Create a span for the forwarding operation
        Span span = tracer.spanBuilder("event_forward")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        
        // Increment request counter
        requestCounter.add(1);
        
        try (Scope scope = span.makeCurrent()) {
            // Add event details to span
            span.setAttribute("event.id", eventData.getEvent().getId());
            span.setAttribute("event.type", eventData.getEvent().getType());
            if (eventData.getDevice() != null) {
                span.setAttribute("device.id", eventData.getDevice().getId());
                span.setAttribute("device.uniqueId", eventData.getDevice().getUniqueId());
            }
            
            // Generate correlation ID if not present
            String correlationId = UUID.randomUUID().toString();
            span.setAttribute("correlation.id", correlationId);
            
            // Resolve endpoint using service discovery
            String resolvedUrl = serviceDiscovery.resolveEndpoint(url);
            span.setAttribute("http.url", resolvedUrl);
            
            // Create request builder
            var requestBuilder = client.target(resolvedUrl).request();
            
            // Add headers
            if (header != null && !header.isEmpty()) {
                for (String line: header.split("\\r?\\n")) {
                    String[] values = line.split(":", 2);
                    requestBuilder.header(values[0].trim(), values[1].trim());
                }
            }
            
            // Add correlation ID header
            requestBuilder.header("X-Correlation-ID", correlationId);
            
            // Propagate trace context
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                    .inject(Context.current(), requestBuilder, SETTER);
            
            // Create a CompletableFuture for the result
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            
            // Wrap the HTTP call with circuit breaker and retry
            Supplier<CompletableFuture<Boolean>> decoratedSupplier = Retry.decorateCompletionStage(
                    retry,
                    CircuitBreaker.decorateSupplier(circuitBreaker, () -> {
                        // Execute the HTTP request
                        requestBuilder.async().post(Entity.json(eventData), new InvocationCallback<Response>() {
                            @Override
                            public void completed(Response response) {
                                if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
                                    span.setStatus(StatusCode.OK);
                                    span.setAttribute("http.status_code", response.getStatus());
                                    successCounter.add(1);
                                    future.complete(true);
                                } else {
                                    int code = response.getStatusInfo().getStatusCode();
                                    span.setStatus(StatusCode.ERROR, "HTTP code " + code);
                                    span.setAttribute("http.status_code", code);
                                    errorCounter.add(1);
                                    future.completeExceptionally(new RuntimeException("HTTP code " + code));
                                }
                            }

                            @Override
                            public void failed(Throwable throwable) {
                                span.setStatus(StatusCode.ERROR, throwable.getMessage());
                                span.recordException(throwable);
                                errorCounter.add(1);
                                future.completeExceptionally(throwable);
                            }
                        });
                        return future;
                    })
            ).get();
            
            // Handle the final result
            decoratedSupplier.whenComplete((result, throwable) -> {
                if (throwable != null) {
                    resultHandler.onResult(false, throwable);
                } else {
                    resultHandler.onResult(result, null);
                }
                span.end();
            });
            
            // Set a timeout for the operation
            future.orTimeout(30, TimeUnit.SECONDS);
            
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            errorCounter.add(1);
            resultHandler.onResult(false, e);
            span.end();
        }
    }
    
    /**
     * Simple service discovery implementation.
     * In a real-world scenario, this would integrate with a service registry like Consul, Eureka, etc.
     */
    private static class ServiceDiscovery {
        private final Config config;
        
        public ServiceDiscovery(Config config) {
            this.config = config;
        }
        
        /**
         * Resolves the endpoint URL using service discovery.
         * This is a simplified implementation that could be extended to use a real service registry.
         * 
         * @param url The configured URL or service name
         * @return The resolved endpoint URL
         */
        public String resolveEndpoint(String url) {
            // In a real implementation, this would look up the service in a registry
            // For now, we just return the configured URL
            return url;
        }
    }
}