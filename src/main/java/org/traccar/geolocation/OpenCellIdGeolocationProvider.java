/*
 * Copyright 2015 - 2022 Anton Tananaev (anton@traccar.org)
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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.traccar.model.CellTower;
import org.traccar.model.Network;

import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.InvocationCallback;

import javax.inject.Inject;

/**
 * OpenCellId geolocation provider.
 * Uses the OpenCellId API to determine device location based on cell tower information.
 */
public class OpenCellIdGeolocationProvider implements GeolocationProvider {

    private final Client client;
    private final String url;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final GeolocationMetricsCollector metricsCollector;

    /**
     * Initialize the OpenCellId geolocation provider.
     *
     * @param client The HTTP client to use for requests
     * @param url The base URL for the OpenCellId API (can be null for default)
     * @param key The API key for OpenCellId
     * @param circuitBreakerFactory Factory for creating circuit breakers
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param metricsCollector Collector for geolocation metrics
     * @param serviceManager Service manager for service discovery
     */
    @Inject
    public OpenCellIdGeolocationProvider(
            Client client, 
            String url, 
            String key, 
            GeolocationCircuitBreakerFactory circuitBreakerFactory,
            Tracer tracer,
            GeolocationMetricsCollector metricsCollector,
            GeolocationServiceManager serviceManager) {
        this.client = client;
        this.tracer = tracer;
        this.metricsCollector = metricsCollector;
        
        // Use service discovery to get the URL if not provided
        if (url == null) {
            url = serviceManager.getServiceUrl("opencellid");
            if (url == null) {
                url = "http://opencellid.org/cell/get";
            }
        }
        this.url = url + "?format=json&mcc=%d&mnc=%d&lac=%d&cellid=%d&key=" + key;
        
        // Create a circuit breaker for this provider
        this.circuitBreaker = circuitBreakerFactory.create("opencellid");
    }

    @Override
    public void getLocation(Network network, final LocationProviderCallback callback) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("opencellid.getLocation").startSpan();
        try (Scope scope = span.makeCurrent()) {
            // Add attributes to the span
            span.setAttribute("provider", "opencellid");
            
            if (network.getCellTowers() != null && !network.getCellTowers().isEmpty()) {
                // Start metrics collection
                metricsCollector.recordRequestStarted("opencellid");
                
                CellTower cellTower = network.getCellTowers().iterator().next();
                String request = String.format(url, cellTower.getMobileCountryCode(), cellTower.getMobileNetworkCode(),
                        cellTower.getLocationAreaCode(), cellTower.getCellId());
                
                // Add cell tower information to the span
                span.setAttribute("cell.mcc", cellTower.getMobileCountryCode());
                span.setAttribute("cell.mnc", cellTower.getMobileNetworkCode());
                span.setAttribute("cell.lac", cellTower.getLocationAreaCode());
                span.setAttribute("cell.cid", cellTower.getCellId());
                span.setAttribute("request.url", request);
                
                // Execute the request with circuit breaker protection
                try {
                    circuitBreaker.executeRunnable(() -> {
                        client.target(request).request().async().get(new InvocationCallback<JsonObject>() {
                            @Override
                            public void completed(JsonObject json) {
                                try {
                                    if (json.containsKey("lat") && json.containsKey("lon")) {
                                        double latitude = json.getJsonNumber("lat").doubleValue();
                                        double longitude = json.getJsonNumber("lon").doubleValue();
                                        
                                        // Record success metrics
                                        metricsCollector.recordRequestSuccess("opencellid");
                                        
                                        // Add result to span
                                        span.setAttribute("location.latitude", latitude);
                                        span.setAttribute("location.longitude", longitude);
                                        span.setStatus(StatusCode.OK);
                                        span.end();
                                        
                                        callback.onSuccess(latitude, longitude, 0);
                                    } else {
                                        String errorMessage = "Coordinates are missing";
                                        if (json.containsKey("error")) {
                                            errorMessage = json.getString("error");
                                            if (json.containsKey("code")) {
                                                errorMessage += " (" + json.getInt("code") + ")";
                                            }
                                        }
                                        
                                        // Record failure metrics
                                        metricsCollector.recordRequestFailure("opencellid", errorMessage);
                                        
                                        // Add error to span
                                        span.setAttribute("error", errorMessage);
                                        span.setStatus(StatusCode.ERROR, errorMessage);
                                        span.end();
                                        
                                        callback.onFailure(new GeolocationException(errorMessage));
                                    }
                                } catch (Exception e) {
                                    failed(e);
                                }
                            }

                            @Override
                            public void failed(Throwable throwable) {
                                // Record failure metrics
                                metricsCollector.recordRequestFailure("opencellid", throwable.getMessage());
                                
                                // Add error to span
                                span.recordException(throwable);
                                span.setStatus(StatusCode.ERROR, throwable.getMessage());
                                span.end();
                                
                                callback.onFailure(throwable);
                            }
                        });
                    });
                } catch (Exception e) {
                    // Circuit breaker is open or other error occurred
                    metricsCollector.recordRequestFailure("opencellid", "Circuit breaker error: " + e.getMessage());
                    
                    // Add circuit breaker error to span
                    span.recordException(e);
                    span.setAttribute("circuit_breaker.state", circuitBreaker.getState().name());
                    span.setStatus(StatusCode.ERROR, "Circuit breaker error: " + e.getMessage());
                    span.end();
                    
                    callback.onFailure(new GeolocationException("Circuit breaker error: " + e.getMessage()));
                }
            } else {
                String errorMessage = "No network information";
                
                // Record failure metrics
                metricsCollector.recordRequestFailure("opencellid", errorMessage);
                
                // Add error to span
                span.setAttribute("error", errorMessage);
                span.setStatus(StatusCode.ERROR, errorMessage);
                span.end();
                
                callback.onFailure(new GeolocationException(errorMessage));
            }
        }
    }
}