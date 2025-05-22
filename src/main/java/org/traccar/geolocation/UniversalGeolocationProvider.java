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
package org.traccar.geolocation;

import org.traccar.model.Network;

import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.InvocationCallback;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * Universal geolocation provider that supports various geolocation services.
 * Implements circuit breaker pattern for resilience and includes distributed tracing.
 */
public class UniversalGeolocationProvider implements GeolocationProvider {

    private final Client client;
    private final String url;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final GeolocationMetricsCollector metricsCollector;
    
    /**
     * Creates a new instance of the UniversalGeolocationProvider.
     *
     * @param client HTTP client for making requests
     * @param url Base URL for the geolocation service
     * @param key API key for the geolocation service
     * @param circuitBreakerFactory Factory for creating circuit breakers
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param metricsCollector Collector for geolocation metrics
     */
    @Inject
    public UniversalGeolocationProvider(
            Client client, 
            @Named("geolocation.url") String url, 
            @Named("geolocation.key") String key,
            GeolocationCircuitBreakerFactory circuitBreakerFactory,
            Tracer tracer,
            GeolocationMetricsCollector metricsCollector) {
        this.client = client;
        this.url = url + "?key=" + key;
        this.circuitBreaker = circuitBreakerFactory.createCircuitBreaker("universalGeolocation");
        this.tracer = tracer;
        this.metricsCollector = metricsCollector;
    }

    @Override
    public void getLocation(Network network, final LocationProviderCallback callback) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("UniversalGeolocationProvider.getLocation")
                .setAttribute("provider.type", "universal")
                .setAttribute("network.mcc", network.getMcc() != null ? network.getMcc() : "")
                .setAttribute("network.mnc", network.getMnc() != null ? network.getMnc() : "")
                .startSpan();
        
        // Start metrics timer
        long startTime = System.currentTimeMillis();
        
        try {
            // Use circuit breaker to execute the request
            circuitBreaker.executeRunnable(() -> {
                // Make the HTTP request within the circuit breaker context
                client.target(url).request().async().post(Entity.json(network), new InvocationCallback<JsonObject>() {
                    @Override
                    public void completed(JsonObject json) {
                        try {
                            if (json.containsKey("error")) {
                                String errorMessage = json.getJsonObject("error").getString("message");
                                GeolocationException exception = new GeolocationException(errorMessage);
                                
                                // Record metrics for failure
                                metricsCollector.recordFailure("universal", System.currentTimeMillis() - startTime);
                                
                                // Record error in span
                                span.recordException(exception);
                                span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, errorMessage);
                                
                                callback.onFailure(exception);
                            } else {
                                JsonObject location = json.getJsonObject("location");
                                double latitude = location.getJsonNumber("lat").doubleValue();
                                double longitude = location.getJsonNumber("lng").doubleValue();
                                double accuracy = json.getJsonNumber("accuracy").doubleValue();
                                
                                // Record metrics for success
                                metricsCollector.recordSuccess("universal", System.currentTimeMillis() - startTime);
                                
                                // Add location data to span
                                span.setAttribute("location.latitude", latitude);
                                span.setAttribute("location.longitude", longitude);
                                span.setAttribute("location.accuracy", accuracy);
                                
                                callback.onSuccess(latitude, longitude, accuracy);
                            }
                        } finally {
                            // End the span
                            span.end();
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        try {
                            // Record metrics for failure
                            metricsCollector.recordFailure("universal", System.currentTimeMillis() - startTime);
                            
                            // Record error in span
                            span.recordException(throwable);
                            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, throwable.getMessage());
                            
                            callback.onFailure(throwable);
                        } finally {
                            // End the span
                            span.end();
                        }
                    }
                });
            });
        } catch (Exception e) {
            // This will be called if the circuit breaker is open or another exception occurs
            try {
                // Record metrics for failure
                metricsCollector.recordFailure("universal", System.currentTimeMillis() - startTime);
                
                // Record error in span
                span.recordException(e);
                span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
                
                callback.onFailure(e);
            } finally {
                // End the span
                span.end();
            }
        }
    }
}