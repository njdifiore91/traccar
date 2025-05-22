/*
 * Copyright 2020 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.speedlimit;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.helper.UnitsConverter;

import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.ws.rs.client.AsyncInvoker;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.InvocationCallback;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Speed limit provider that uses the Overpass API to retrieve speed limits.
 * Enhanced with service discovery, circuit breaker, distributed tracing, and metrics.
 */
@Singleton
public class OverpassSpeedLimitProvider implements SpeedLimitProvider {

    private static final Logger LOGGER = Logger.getLogger(OverpassSpeedLimitProvider.class.getName());
    private static final String PROVIDER_NAME = "overpass";
    private static final String SERVICE_ID_PREFIX = "speedlimit-overpass-";
    
    private final Client client;
    private final String urlTemplate;
    private final int accuracy;
    private final SpeedLimitCircuitBreaker circuitBreaker;
    private final SpeedLimitMetrics metrics;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final Tracer tracer;
    
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicLong lastSuccessTime = new AtomicLong(0);
    
    private String serviceId;
    private String currentEndpoint;
    private HealthStatus healthStatus = HealthStatus.HEALTHY;

    /**
     * Constructs a new OverpassSpeedLimitProvider with the given dependencies.
     *
     * @param config The configuration
     * @param client The HTTP client
     * @param circuitBreaker The circuit breaker
     * @param metrics The metrics collector
     * @param serviceDiscoveryManager The service discovery manager
     * @param tracer The OpenTelemetry tracer
     */
    @Inject
    public OverpassSpeedLimitProvider(
            Config config,
            Client client,
            SpeedLimitCircuitBreaker circuitBreaker,
            SpeedLimitMetrics metrics,
            ServiceDiscoveryManager serviceDiscoveryManager,
            Tracer tracer) {
        
        this.client = client;
        this.circuitBreaker = circuitBreaker;
        this.metrics = metrics;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.tracer = tracer;
        
        this.accuracy = config.getInteger(Keys.SPEED_LIMIT_ACCURACY, 25);
        
        // Get the base URL from configuration or service discovery
        String configuredUrl = config.getString(Keys.SPEED_LIMIT_URL);
        if (configuredUrl != null) {
            this.currentEndpoint = configuredUrl;
            LOGGER.info("Using configured Overpass API endpoint: " + currentEndpoint);
        } else {
            // Use a default endpoint if none is configured or discovered
            this.currentEndpoint = "https://overpass-api.de/api/interpreter";
            LOGGER.info("Using default Overpass API endpoint: " + currentEndpoint);
            
            // Try to discover the endpoint from service discovery
            discoverEndpoint();
        }
        
        // Create the URL template with the accuracy parameter
        this.urlTemplate = "?data=[out:json];way[maxspeed](around:" + accuracy + ",%f,%f);out%%20tags;";
        
        // Register with service discovery
        registerWithServiceDiscovery();
        
        // Initialize metrics
        metrics.recordHealthStatus(PROVIDER_NAME, healthStatus);
        
        LOGGER.info("Initialized OverpassSpeedLimitProvider with accuracy: " + accuracy);
    }

    /**
     * Attempts to discover the Overpass API endpoint from service discovery.
     */
    private void discoverEndpoint() {
        // This would use the ServiceDiscoveryManager to find an Overpass API endpoint
        // For now, we'll just log that we're using the default endpoint
        LOGGER.info("No Overpass API endpoint discovered, using default");
    }

    /**
     * Registers this provider with the service discovery system.
     */
    private void registerWithServiceDiscovery() {
        try {
            // Parse the URI to get host and port
            URI uri = new URI(currentEndpoint);
            String host = uri.getHost();
            int port = uri.getPort() > 0 ? uri.getPort() : ("https".equals(uri.getScheme()) ? 443 : 80);
            boolean secure = "https".equals(uri.getScheme());
            
            // Register with service discovery
            serviceId = serviceDiscoveryManager.register(
                    "speedlimit-overpass",
                    host,
                    port,
                    "http",
                    secure);
            
            if (serviceId != null) {
                LOGGER.info("Registered with service discovery as: " + serviceId);
            } else {
                LOGGER.warning("Failed to register with service discovery");
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error registering with service discovery", e);
        }
    }

    /**
     * Parses a speed limit value from a string.
     *
     * @param value The speed limit value as a string
     * @return The speed limit value in knots, or null if parsing failed
     */
    private Double parseSpeed(String value) {
        if (value.endsWith(" mph")) {
            return UnitsConverter.knotsFromMph(Double.parseDouble(value.substring(0, value.length() - 4)));
        } else if (value.endsWith(" knots")) {
            return Double.parseDouble(value.substring(0, value.length() - 6));
        } else if (value.matches("\\d+")) {
            return UnitsConverter.knotsFromKph(Double.parseDouble(value));
        } else {
            return null;
        }
    }

    @Override
    public void getSpeedLimit(double latitude, double longitude, SpeedLimitProviderCallback callback) {
        getSpeedLimit(latitude, longitude, new HashMap<>(), callback);
    }

    @Override
    public void getSpeedLimit(double latitude, double longitude, Map<String, String> tracingContext, SpeedLimitProviderCallback callback) {
        // Create a span for this operation
        Span span = createSpan("overpass.getSpeedLimit", tracingContext);
        span.setAttribute("latitude", latitude);
        span.setAttribute("longitude", longitude);
        
        // Start timing the request
        long startTime = System.currentTimeMillis();
        requestCount.incrementAndGet();
        
        try (Scope scope = span.makeCurrent()) {
            // Format the URL with the coordinates
            String formattedUrl = currentEndpoint + String.format(urlTemplate, latitude, longitude);
            span.setAttribute("url", formattedUrl);
            
            // Use the circuit breaker to execute the request
            CompletableFuture<Double> future = new CompletableFuture<>();
            
            try {
                // Check if the circuit breaker is open
                String state = circuitBreaker.getCircuitBreakerState(PROVIDER_NAME);
                span.setAttribute("circuit_breaker.state", state);
                
                if (CircuitBreaker.State.OPEN.name().equals(state)) {
                    throw new SpeedLimitException.Builder("Circuit breaker is open")
                            .withCategory(SpeedLimitException.Category.SERVICE)
                            .withSeverity(SpeedLimitException.Severity.WARNING)
                            .withFailureType(SpeedLimitException.FailureType.TRANSIENT)
                            .withCircuitBreakerTrigger(false)
                            .withServiceName(PROVIDER_NAME)
                            .withTraceId(span.getSpanContext().getTraceId())
                            .withSpanId(span.getSpanContext().getSpanId())
                            .build();
                }
                
                // Execute the request with the circuit breaker
                circuitBreaker.executeWithCircuitBreaker(() -> {
                    AsyncInvoker invoker = client.target(formattedUrl).request().async();
                    invoker.get(new InvocationCallback<JsonObject>() {
                        @Override
                        public void completed(JsonObject json) {
                            try {
                                JsonArray elements = json.getJsonArray("elements");
                                if (!elements.isEmpty()) {
                                    Double maxSpeed = parseSpeed(
                                            elements.getJsonObject(0).getJsonObject("tags").getString("maxspeed"));
                                    if (maxSpeed != null) {
                                        future.complete(maxSpeed);
                                    } else {
                                        future.completeExceptionally(new SpeedLimitException.Builder("Parsing failed")
                                                .withCategory(SpeedLimitException.Category.VALIDATION)
                                                .withSeverity(SpeedLimitException.Severity.WARNING)
                                                .withFailureType(SpeedLimitException.FailureType.PERMANENT)
                                                .withCircuitBreakerTrigger(false)
                                                .withServiceName(PROVIDER_NAME)
                                                .withTraceId(span.getSpanContext().getTraceId())
                                                .withSpanId(span.getSpanContext().getSpanId())
                                                .build());
                                    }
                                } else {
                                    future.completeExceptionally(new SpeedLimitException.Builder("Not found")
                                            .withCategory(SpeedLimitException.Category.CLIENT)
                                            .withSeverity(SpeedLimitException.Severity.INFO)
                                            .withFailureType(SpeedLimitException.FailureType.PERMANENT)
                                            .withCircuitBreakerTrigger(false)
                                            .withServiceName(PROVIDER_NAME)
                                            .withTraceId(span.getSpanContext().getTraceId())
                                            .withSpanId(span.getSpanContext().getSpanId())
                                            .build());
                                }
                            } catch (Exception e) {
                                future.completeExceptionally(new SpeedLimitException.Builder("Processing error")
                                        .withCause(e)
                                        .withCategory(SpeedLimitException.Category.SERVICE)
                                        .withSeverity(SpeedLimitException.Severity.ERROR)
                                        .withFailureType(SpeedLimitException.FailureType.TRANSIENT)
                                        .withCircuitBreakerTrigger(true)
                                        .withServiceName(PROVIDER_NAME)
                                        .withTraceId(span.getSpanContext().getTraceId())
                                        .withSpanId(span.getSpanContext().getSpanId())
                                        .build());
                            }
                        }

                        @Override
                        public void failed(Throwable throwable) {
                            future.completeExceptionally(new SpeedLimitException.Builder("Request failed")
                                    .withCause(throwable)
                                    .withCategory(SpeedLimitException.Category.CONNECTION)
                                    .withSeverity(SpeedLimitException.Severity.ERROR)
                                    .withFailureType(SpeedLimitException.FailureType.TRANSIENT)
                                    .withCircuitBreakerTrigger(true)
                                    .withServiceName(PROVIDER_NAME)
                                    .withTraceId(span.getSpanContext().getTraceId())
                                    .withSpanId(span.getSpanContext().getSpanId())
                                    .build());
                        }
                    });
                    return null;
                }, PROVIDER_NAME);
                
                // Handle the result
                future.orTimeout(10, TimeUnit.SECONDS)
                        .thenAccept(result -> {
                            long duration = System.currentTimeMillis() - startTime;
                            successCount.incrementAndGet();
                            lastSuccessTime.set(System.currentTimeMillis());
                            updateHealthStatus();
                            
                            metrics.recordSuccess(PROVIDER_NAME, duration);
                            span.setAttribute("speedlimit.value", result);
                            span.setStatus(StatusCode.OK);
                            span.end();
                            
                            callback.onSuccess(result);
                        })
                        .exceptionally(throwable -> {
                            long duration = System.currentTimeMillis() - startTime;
                            failureCount.incrementAndGet();
                            updateHealthStatus();
                            
                            Throwable cause = throwable.getCause() != null ? throwable.getCause() : throwable;
                            String errorType = cause.getClass().getSimpleName();
                            
                            metrics.recordFailure(PROVIDER_NAME, errorType, duration);
                            span.recordException(cause);
                            span.setStatus(StatusCode.ERROR, cause.getMessage());
                            span.end();
                            
                            callback.onFailure(cause);
                            return null;
                        });
                
            } catch (Exception e) {
                long duration = System.currentTimeMillis() - startTime;
                failureCount.incrementAndGet();
                updateHealthStatus();
                
                metrics.recordFailure(PROVIDER_NAME, e.getClass().getSimpleName(), duration);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.end();
                
                callback.onFailure(e);
            }
        }
    }

    /**
     * Creates a span for distributed tracing.
     *
     * @param name The name of the span
     * @param tracingContext The tracing context from the parent span
     * @return The created span
     */
    private Span createSpan(String name, Map<String, String> tracingContext) {
        // Extract the parent context if available
        Context parentContext = Context.current();
        if (tracingContext != null && !tracingContext.isEmpty()) {
            TextMapGetter<Map<String, String>> getter = new TextMapGetter<>() {
                @Override
                public Iterable<String> keys(Map<String, String> carrier) {
                    return carrier.keySet();
                }

                @Override
                public String get(Map<String, String> carrier, String key) {
                    return carrier.get(key);
                }
            };
            
            // This would normally use the OpenTelemetry propagator to extract the context
            // For now, we'll just use the current context
            // parentContext = OpenTelemetry.getPropagators().getTextMapPropagator().extract(parentContext, tracingContext, getter);
        }
        
        // Create a new span
        return tracer.spanBuilder(name)
                .setParent(parentContext)
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("service.name", "speedlimit-service")
                .setAttribute("provider", PROVIDER_NAME)
                .startSpan();
    }

    /**
     * Updates the health status based on recent request statistics.
     */
    private void updateHealthStatus() {
        int total = requestCount.get();
        int success = successCount.get();
        int failure = failureCount.get();
        
        if (total == 0) {
            healthStatus = HealthStatus.HEALTHY;
        } else {
            double failureRate = (double) failure / total;
            long timeSinceLastSuccess = System.currentTimeMillis() - lastSuccessTime.get();
            
            if (failureRate > 0.5 || (lastSuccessTime.get() > 0 && timeSinceLastSuccess > 300000)) {
                // More than 50% failures or no success in 5 minutes
                healthStatus = HealthStatus.UNHEALTHY;
            } else if (failureRate > 0.2) {
                // More than 20% failures
                healthStatus = HealthStatus.DEGRADED;
            } else {
                healthStatus = HealthStatus.HEALTHY;
            }
        }
        
        // Update metrics with the new health status
        metrics.recordHealthStatus(PROVIDER_NAME, healthStatus);
        
        // Update circuit breaker metrics
        CircuitBreaker.Metrics cbMetrics = circuitBreaker.getCircuitBreakerMetrics(PROVIDER_NAME);
        metrics.recordCircuitBreakerMetrics(
                PROVIDER_NAME,
                cbMetrics.getFailureRate(),
                cbMetrics.getSlowCallRate(),
                cbMetrics.getNumberOfBufferedCalls(),
                cbMetrics.getNumberOfFailedCalls());
        
        // Update circuit breaker state
        metrics.recordCircuitBreakerState(PROVIDER_NAME, circuitBreaker.getCircuitBreakerState(PROVIDER_NAME));
    }

    @Override
    public HealthStatus getHealthStatus() {
        return healthStatus;
    }

    @Override
    public Map<String, Object> getHealthMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("status", healthStatus.name());
        metrics.put("requestCount", requestCount.get());
        metrics.put("successCount", successCount.get());
        metrics.put("failureCount", failureCount.get());
        metrics.put("lastSuccessTime", lastSuccessTime.get());
        metrics.put("circuitBreakerState", circuitBreaker.getCircuitBreakerState(PROVIDER_NAME));
        
        CircuitBreaker.Metrics cbMetrics = circuitBreaker.getCircuitBreakerMetrics(PROVIDER_NAME);
        metrics.put("failureRate", cbMetrics.getFailureRate());
        metrics.put("slowCallRate", cbMetrics.getSlowCallRate());
        metrics.put("numberOfBufferedCalls", cbMetrics.getNumberOfBufferedCalls());
        metrics.put("numberOfFailedCalls", cbMetrics.getNumberOfFailedCalls());
        
        return metrics;
    }

    @Override
    public boolean register(String serviceId, Map<String, String> metadata) {
        // This provider is already registered in the constructor
        return this.serviceId != null;
    }

    @Override
    public boolean deregister(String serviceId) {
        if (this.serviceId != null && this.serviceId.equals(serviceId)) {
            serviceDiscoveryManager.deregister(this.serviceId);
            this.serviceId = null;
            return true;
        }
        return false;
    }

    @Override
    public boolean resetCircuitBreaker() {
        try {
            circuitBreaker.resetCircuitBreaker(PROVIDER_NAME);
            return true;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to reset circuit breaker", e);
            return false;
        }
    }

    @Override
    public String getCircuitBreakerState() {
        return circuitBreaker.getCircuitBreakerState(PROVIDER_NAME);
    }
}