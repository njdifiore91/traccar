/*
 * Copyright 2015 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.geocoder;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.database.StatisticsManager;

import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.InvocationCallback;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public abstract class JsonGeocoder implements Geocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonGeocoder.class);
    private static final String GEOCODER_NAME = "geocoder";
    private static final String GEOCODER_CIRCUIT_BREAKER = "geocoder-circuit-breaker";

    private final Client client;
    private final String url;
    private final AddressFormat addressFormat;
    private StatisticsManager statisticsManager;
    
    // Circuit breaker for resilient geocoding
    private final CircuitBreaker circuitBreaker;
    
    // OpenTelemetry tracer for distributed tracing
    private final Tracer tracer;
    
    // Micrometer metrics
    private final Timer requestTimer;
    private final Counter requestCounter;
    private final Counter successCounter;
    private final Counter errorCounter;
    private final Counter circuitBreakerOpenCounter;
    
    // Health status
    private volatile boolean healthy = true;

    private Map<Map.Entry<Double, Double>, String> cache;

    public JsonGeocoder(Client client, String url, final int cacheSize, AddressFormat addressFormat) {
        this(client, url, cacheSize, addressFormat, null, null);
    }
    
    public JsonGeocoder(Client client, String url, final int cacheSize, AddressFormat addressFormat, 
                       Tracer tracer, MeterRegistry meterRegistry) {
        this.client = client;
        this.url = url;
        this.addressFormat = addressFormat;
        
        // Initialize circuit breaker with default configuration
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to open circuit
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .slidingWindowSize(10) // Consider last 10 calls for failure rate
                .permittedNumberOfCallsInHalfOpenState(2) // Allow 2 test calls when half-open
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(GEOCODER_CIRCUIT_BREAKER);
        
        // Register circuit breaker state transition listener for health status updates
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    if (event.getStateTransition() == CircuitBreaker.StateTransition.CLOSED_TO_OPEN) {
                        LOGGER.warn("Geocoder circuit breaker opened due to failure rate threshold");
                        healthy = false;
                        if (meterRegistry != null) {
                            circuitBreakerOpenCounter.increment();
                        }
                    } else if (event.getStateTransition() == CircuitBreaker.StateTransition.OPEN_TO_CLOSED
                            || event.getStateTransition() == CircuitBreaker.StateTransition.HALF_OPEN_TO_CLOSED) {
                        LOGGER.info("Geocoder circuit breaker closed, service recovered");
                        healthy = true;
                    }
                });
        
        // Initialize OpenTelemetry tracer
        this.tracer = tracer;
        
        // Initialize Micrometer metrics
        if (meterRegistry != null) {
            this.requestTimer = Timer.builder("geocoder.request.duration")
                    .description("Time taken to complete geocoding requests")
                    .tag("provider", getClass().getSimpleName())
                    .register(meterRegistry);
            
            this.requestCounter = Counter.builder("geocoder.requests.total")
                    .description("Total number of geocoding requests")
                    .tag("provider", getClass().getSimpleName())
                    .register(meterRegistry);
            
            this.successCounter = Counter.builder("geocoder.requests.success")
                    .description("Number of successful geocoding requests")
                    .tag("provider", getClass().getSimpleName())
                    .register(meterRegistry);
            
            this.errorCounter = Counter.builder("geocoder.requests.error")
                    .description("Number of failed geocoding requests")
                    .tag("provider", getClass().getSimpleName())
                    .register(meterRegistry);
            
            this.circuitBreakerOpenCounter = Counter.builder("geocoder.circuit_breaker.open")
                    .description("Number of times the geocoder circuit breaker opened")
                    .tag("provider", getClass().getSimpleName())
                    .register(meterRegistry);
        } else {
            this.requestTimer = null;
            this.requestCounter = null;
            this.successCounter = null;
            this.errorCounter = null;
            this.circuitBreakerOpenCounter = null;
        }
        
        if (cacheSize > 0) {
            this.cache = Collections.synchronizedMap(new LinkedHashMap<>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry eldest) {
                    return size() > cacheSize;
                }
            });
        }
    }

    @Override
    public void setStatisticsManager(StatisticsManager statisticsManager) {
        this.statisticsManager = statisticsManager;
    }
    
    /**
     * Check if the geocoder service is healthy
     * @return true if the service is healthy, false otherwise
     */
    public boolean isHealthy() {
        return healthy && circuitBreaker.getState() != CircuitBreaker.State.OPEN;
    }

    protected String readValue(JsonObject object, String key) {
        if (object.containsKey(key) && !object.isNull(key)) {
            return object.getString(key);
        }
        return null;
    }

    private String handleResponse(
            double latitude, double longitude, JsonObject json, ReverseGeocoderCallback callback, Span span) {

        Address address = parseAddress(json);
        if (address != null) {
            String formattedAddress = addressFormat.format(address);
            if (cache != null) {
                cache.put(new AbstractMap.SimpleImmutableEntry<>(latitude, longitude), formattedAddress);
            }
            if (callback != null) {
                callback.onSuccess(formattedAddress);
            }
            
            // Record success metrics
            if (successCounter != null) {
                successCounter.increment();
            }
            
            // Add address information to span
            if (span != null) {
                span.setAttribute("geocoder.address.found", true);
                span.setAttribute("geocoder.address.formatted", formattedAddress);
                span.setStatus(StatusCode.OK);
            }
            
            return formattedAddress;
        } else {
            String msg = "Empty address. Error: " + parseError(json);
            if (callback != null) {
                callback.onFailure(new GeocoderException(msg));
            } else {
                LOGGER.warn(msg);
            }
            
            // Record error metrics
            if (errorCounter != null) {
                errorCounter.increment();
            }
            
            // Add error information to span
            if (span != null) {
                span.setAttribute("geocoder.address.found", false);
                span.setAttribute("geocoder.error", msg);
                span.setStatus(StatusCode.ERROR, msg);
            }
        }
        return null;
    }

    @Override
    public String getAddress(
            final double latitude, final double longitude, final ReverseGeocoderCallback callback) {

        // Start metrics collection
        if (requestCounter != null) {
            requestCounter.increment();
        }
        
        // Create a span for distributed tracing
        Span span = null;
        if (tracer != null) {
            span = tracer.spanBuilder("geocoder.reverse")
                    .setSpanKind(SpanKind.CLIENT)
                    .setAttribute("geocoder.latitude", latitude)
                    .setAttribute("geocoder.longitude", longitude)
                    .setAttribute("geocoder.provider", getClass().getSimpleName())
                    .startSpan();
        }
        
        try {
            // Check cache first
            if (cache != null) {
                String cachedAddress = cache.get(new AbstractMap.SimpleImmutableEntry<>(latitude, longitude));
                if (cachedAddress != null) {
                    if (callback != null) {
                        callback.onSuccess(cachedAddress);
                    }
                    
                    // Record cache hit in span
                    if (span != null) {
                        span.setAttribute("geocoder.cache.hit", true);
                        span.setAttribute("geocoder.address.formatted", cachedAddress);
                        span.setStatus(StatusCode.OK);
                    }
                    
                    // Record success metrics
                    if (successCounter != null) {
                        successCounter.increment();
                    }
                    
                    return cachedAddress;
                } else if (span != null) {
                    span.setAttribute("geocoder.cache.hit", false);
                }
            }

            if (statisticsManager != null) {
                statisticsManager.registerGeocoderRequest();
            }
            
            // Create the request
            final var request = client.target(String.format(url, latitude, longitude)).request();
            
            // Use circuit breaker pattern for resilient geocoding
            if (callback != null) {
                // Asynchronous request with circuit breaker
                Supplier<CompletableFuture<String>> geocodingSupplier = () -> {
                    CompletableFuture<String> future = new CompletableFuture<>();
                    
                    request.async().get(new InvocationCallback<JsonObject>() {
                        @Override
                        public void completed(JsonObject json) {
                            String result = handleResponse(latitude, longitude, json, callback, span);
                            future.complete(result);
                        }

                        @Override
                        public void failed(Throwable throwable) {
                            // Record error in span
                            if (span != null) {
                                span.recordException(throwable);
                                span.setStatus(StatusCode.ERROR, throwable.getMessage());
                            }
                            
                            // Record error metrics
                            if (errorCounter != null) {
                                errorCounter.increment();
                            }
                            
                            callback.onFailure(throwable);
                            future.completeExceptionally(throwable);
                        }
                    });
                    
                    return future;
                };
                
                // Execute with circuit breaker
                try {
                    circuitBreaker.executeCompletionStage(geocodingSupplier).toCompletableFuture();
                } catch (Exception e) {
                    // Circuit breaker is open or other error
                    if (span != null) {
                        span.recordException(e);
                        span.setAttribute("geocoder.circuit_breaker.open", 
                                circuitBreaker.getState() == CircuitBreaker.State.OPEN);
                        span.setStatus(StatusCode.ERROR, e.getMessage());
                    }
                    
                    if (errorCounter != null) {
                        errorCounter.increment();
                    }
                    
                    callback.onFailure(e);
                }
                
                return null; // Async call, result will be delivered via callback
            } else {
                // Synchronous request with circuit breaker and timer
                Timer.Sample sample = null;
                if (requestTimer != null) {
                    sample = Timer.start();
                }
                
                try {
                    // Execute with circuit breaker
                    return circuitBreaker.executeSupplier(() -> {
                        try {
                            return handleResponse(latitude, longitude, request.get(JsonObject.class), null, span);
                        } catch (Exception e) {
                            // Record error in span
                            if (span != null) {
                                span.recordException(e);
                                span.setStatus(StatusCode.ERROR, e.getMessage());
                            }
                            
                            LOGGER.warn("Geocoder network error", e);
                            throw e; // Rethrow for circuit breaker to handle
                        }
                    });
                } catch (Exception e) {
                    // Circuit breaker is open or other error
                    if (span != null) {
                        span.recordException(e);
                        span.setAttribute("geocoder.circuit_breaker.open", 
                                circuitBreaker.getState() == CircuitBreaker.State.OPEN);
                        span.setStatus(StatusCode.ERROR, e.getMessage());
                    }
                    
                    if (errorCounter != null) {
                        errorCounter.increment();
                    }
                    
                    LOGGER.warn("Geocoder error: {}", e.getMessage());
                    return null;
                } finally {
                    // Record timing metrics
                    if (sample != null && requestTimer != null) {
                        sample.stop(requestTimer);
                    }
                }
            }
        } finally {
            // End the span
            if (span != null) {
                span.end();
            }
        }
    }

    public abstract Address parseAddress(JsonObject json);

    protected String parseError(JsonObject json) {
        return null;
    }

}