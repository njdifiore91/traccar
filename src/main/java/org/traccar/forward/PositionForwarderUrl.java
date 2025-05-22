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
package org.traccar.forward;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.forward.ResultHandler.ErrorInfo;
import org.traccar.helper.Checksum;
import org.traccar.model.Device;
import org.traccar.model.Position;

import jakarta.inject.Inject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.InvocationCallback;
import jakarta.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Calendar;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Position forwarder that sends position data to a URL endpoint with enhanced resilience,
 * distributed tracing, and metrics collection capabilities.
 */
public class PositionForwarderUrl implements PositionForwarder {

    private final String urlTemplate;
    private final String header;

    private final Client client;
    private final ObjectMapper objectMapper;
    private final CircuitBreakerManager circuitBreakerManager;
    private final TracingManager tracingManager;
    private final MetricsManager metricsManager;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Timer requestTimer;
    
    private static final String CIRCUIT_BREAKER_NAME = "position-forwarder-url";
    private static final String CORRELATION_ID_HEADER = "X-Correlation-ID";
    private static final String TRACE_ID_HEADER = "X-Trace-ID";
    private static final String SPAN_ID_HEADER = "X-Span-ID";
    
    /**
     * TextMapSetter implementation for adding tracing context to HTTP headers.
     */
    private static final TextMapSetter<Map<String, Object>> SETTER = new TextMapSetter<>() {
        @Override
        public void set(Map<String, Object> carrier, String key, String value) {
            if (carrier != null) {
                carrier.put(key, value);
            }
        }
    };

    /**
     * Constructs a new PositionForwarderUrl with the specified dependencies.
     *
     * @param config                  Configuration provider
     * @param client                  HTTP client for making requests
     * @param objectMapper            JSON object mapper
     * @param circuitBreakerManager   Manager for circuit breaker functionality
     * @param tracingManager          Manager for distributed tracing
     * @param metricsManager          Manager for metrics collection
     * @param serviceDiscoveryManager Manager for service discovery
     * @param meterRegistry           Registry for metrics collection
     */
    @Inject
    public PositionForwarderUrl(
            Config config,
            Client client,
            ObjectMapper objectMapper,
            CircuitBreakerManager circuitBreakerManager,
            TracingManager tracingManager,
            MetricsManager metricsManager,
            ServiceDiscoveryManager serviceDiscoveryManager,
            MeterRegistry meterRegistry) {
        
        this.client = client;
        this.objectMapper = objectMapper;
        this.urlTemplate = config.getString(Keys.FORWARD_URL);
        this.header = config.getString(Keys.FORWARD_HEADER);
        this.circuitBreakerManager = circuitBreakerManager;
        this.tracingManager = tracingManager;
        this.metricsManager = metricsManager;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        
        // Create circuit breaker for HTTP requests
        this.circuitBreaker = circuitBreakerManager.createCircuitBreaker(
                CIRCUIT_BREAKER_NAME,
                CircuitBreakerConfig.custom()
                        .failureRateThreshold(50) // Open circuit when 50% of calls fail
                        .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                        .slidingWindowSize(10) // Consider the last 10 calls
                        .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 calls in half-open state
                        .build());
        
        // Create retry configuration for transient failures
        this.retry = circuitBreakerManager.createRetry(
                CIRCUIT_BREAKER_NAME,
                RetryConfig.custom()
                        .maxAttempts(3) // Try up to 3 times
                        .waitDuration(Duration.ofMillis(500)) // Start with 500ms delay
                        .retryExceptions(UnsupportedEncodingException.class, JsonProcessingException.class)
                        .retryOnResult(response -> response instanceof Response && ((Response) response).getStatus() >= 500)
                        .build());
        
        // Create timer for measuring request latency
        this.requestTimer = meterRegistry.timer("position.forwarder.url.request");
    }

    @Override
    public void forward(PositionData positionData, ResultHandler resultHandler) {
        // Start the timer for this operation
        long startTime = System.currentTimeMillis();
        
        // Create a new span for this forwarding operation
        Span span = tracingManager.startSpan("position.forwarder.url.forward", SpanKind.CLIENT);
        Map<String, String> traceContext = new HashMap<>();
        
        try {
            // Add trace context to the span
            span.setAttribute("positionId", positionData.getPosition().getId());
            span.setAttribute("deviceId", positionData.getPosition().getDeviceId());
            span.setAttribute("protocol", positionData.getPosition().getProtocol());
            
            // Extract trace context for propagation
            tracingManager.extractContextToMap(Context.current(), traceContext);
            
            // Check if circuit breaker is open
            if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
                span.setStatus(StatusCode.ERROR, "Circuit breaker is open");
                ErrorInfo errorInfo = new ErrorInfo(
                        "CIRCUIT_OPEN",
                        "Circuit breaker is open for URL forwarding",
                        new RuntimeException("Circuit breaker is open"),
                        Map.of("service", CIRCUIT_BREAKER_NAME));
                
                resultHandler.onCircuitBreakerStateChange(true, CIRCUIT_BREAKER_NAME, 
                        Map.of("failureRate", circuitBreaker.getMetrics().getFailureRate()));
                resultHandler.onResultAsync(false, errorInfo, traceContext);
                return;
            }
            
            // Resolve the URL using service discovery if needed
            String url = resolveUrl(positionData);
            span.setAttribute("url", url);
            
            // Prepare the request with headers
            Map<String, Object> headerMap = new HashMap<>();
            var requestBuilder = client.target(url).request();
            
            // Add custom headers if specified
            if (header != null && !header.isEmpty()) {
                for (String line : header.split("\\r?\\n")) {
                    String[] values = line.split(":", 2);
                    if (values.length == 2) {
                        String headerName = values[0].trim();
                        String headerValue = values[1].trim();
                        requestBuilder.header(headerName, headerValue);
                        headerMap.put(headerName, headerValue);
                    }
                }
            }
            
            // Add correlation ID and trace context headers
            String correlationId = tracingManager.getCurrentCorrelationId();
            if (correlationId != null) {
                requestBuilder.header(CORRELATION_ID_HEADER, correlationId);
                headerMap.put(CORRELATION_ID_HEADER, correlationId);
            }
            
            // Inject trace context into headers
            tracingManager.injectContext(Context.current(), headerMap, SETTER);
            
            // Add trace headers from the headerMap to the request
            headerMap.forEach((key, value) -> {
                if (value instanceof String) {
                    requestBuilder.header(key, (String) value);
                }
            });
            
            // Execute the HTTP request with circuit breaker and retry
            Supplier<CompletableFuture<Response>> decoratedSupplier = CircuitBreaker.decorateSupplier(
                    circuitBreaker,
                    () -> {
                        CompletableFuture<Response> future = new CompletableFuture<>();
                        try {
                            requestBuilder.async().get(new InvocationCallback<Response>() {
                                @Override
                                public void completed(Response response) {
                                    future.complete(response);
                                }

                                @Override
                                public void failed(Throwable throwable) {
                                    future.completeExceptionally(throwable);
                                }
                            });
                        } catch (Exception e) {
                            future.completeExceptionally(e);
                        }
                        return future;
                    });
            
            // Apply retry mechanism
            Retry.decorateCompletionStage(
                    retry,
                    scheduler -> CompletableFuture.supplyAsync(decoratedSupplier).thenCompose(f -> f)
            ).get().whenComplete((response, throwable) -> {
                long duration = System.currentTimeMillis() - startTime;
                
                if (throwable != null) {
                    // Handle exception
                    span.recordException(throwable);
                    span.setStatus(StatusCode.ERROR, throwable.getMessage());
                    
                    // Record metrics
                    Map<String, String> metadataMap = new HashMap<>();
                    metadataMap.put("operation", "forward");
                    metadataMap.put("success", "false");
                    metadataMap.put("errorType", throwable.getClass().getSimpleName());
                    resultHandler.recordMetrics("position.forwarder.url", duration, false, metadataMap);
                    
                    // Create structured error info
                    ErrorInfo errorInfo = new ErrorInfo(
                            "HTTP_REQUEST_FAILED",
                            "Failed to forward position data: " + throwable.getMessage(),
                            throwable,
                            metadataMap);
                    
                    // Complete the operation
                    resultHandler.onResultAsync(false, errorInfo, traceContext);
                } else {
                    // Handle successful response
                    boolean success = response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL;
                    span.setAttribute("http.status_code", response.getStatus());
                    
                    if (success) {
                        span.setStatus(StatusCode.OK);
                    } else {
                        span.setStatus(StatusCode.ERROR, "HTTP status code: " + response.getStatus());
                    }
                    
                    // Record metrics
                    Map<String, String> metadataMap = new HashMap<>();
                    metadataMap.put("operation", "forward");
                    metadataMap.put("success", String.valueOf(success));
                    metadataMap.put("statusCode", String.valueOf(response.getStatus()));
                    resultHandler.recordMetrics("position.forwarder.url", duration, success, metadataMap);
                    
                    if (success) {
                        resultHandler.onResultAsync(true, (ErrorInfo) null, traceContext);
                    } else {
                        int code = response.getStatus();
                        ErrorInfo errorInfo = new ErrorInfo(
                                "HTTP_ERROR_RESPONSE",
                                "HTTP error code: " + code,
                                new RuntimeException("HTTP code " + code),
                                metadataMap);
                        resultHandler.onResultAsync(false, errorInfo, traceContext);
                    }
                    
                    // Close the response to release resources
                    response.close();
                }
                
                // End the span
                span.end();
            });
            
        } catch (Exception e) {
            // Handle unexpected exceptions
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.end();
            
            long duration = System.currentTimeMillis() - startTime;
            Map<String, String> metadataMap = new HashMap<>();
            metadataMap.put("operation", "forward");
            metadataMap.put("success", "false");
            metadataMap.put("errorType", e.getClass().getSimpleName());
            resultHandler.recordMetrics("position.forwarder.url", duration, false, metadataMap);
            
            ErrorInfo errorInfo = new ErrorInfo(
                    "UNEXPECTED_ERROR",
                    "Unexpected error during forwarding: " + e.getMessage(),
                    e,
                    metadataMap);
            
            resultHandler.onResultAsync(false, errorInfo, traceContext);
        }
    }

    /**
     * Resolves the URL for the position data, applying service discovery if needed.
     *
     * @param positionData The position data to forward
     * @return The resolved URL string
     * @throws UnsupportedEncodingException If URL encoding fails
     * @throws JsonProcessingException If JSON processing fails
     */
    private String resolveUrl(PositionData positionData) throws UnsupportedEncodingException, JsonProcessingException {
        String url = formatRequest(positionData);
        
        // Apply service discovery if the URL contains service placeholders
        if (url.contains("{service:")) {
            for (String servicePlaceholder : url.split("\\{service:")) {
                if (servicePlaceholder.contains("}")) {
                    String serviceName = servicePlaceholder.substring(0, servicePlaceholder.indexOf('}'));
                    String serviceUrl = serviceDiscoveryManager.resolveServiceUrl(serviceName);
                    if (serviceUrl != null) {
                        url = url.replace("{service:" + serviceName + "}", serviceUrl);
                    }
                }
            }
        }
        
        return url;
    }

    /**
     * Formats the request URL with position data.
     *
     * @param positionData The position data to include in the URL
     * @return The formatted URL string
     * @throws UnsupportedEncodingException If URL encoding fails
     * @throws JsonProcessingException If JSON processing fails
     */
    public String formatRequest(PositionData positionData) throws UnsupportedEncodingException, JsonProcessingException {
        Position position = positionData.getPosition();
        Device device = positionData.getDevice();

        String request = urlTemplate
                .replace("{name}", URLEncoder.encode(device.getName(), StandardCharsets.UTF_8))
                .replace("{uniqueId}", device.getUniqueId())
                .replace("{status}", device.getStatus())
                .replace("{deviceId}", String.valueOf(position.getDeviceId()))
                .replace("{protocol}", String.valueOf(position.getProtocol()))
                .replace("{deviceTime}", String.valueOf(position.getDeviceTime().getTime()))
                .replace("{fixTime}", String.valueOf(position.getFixTime().getTime()))
                .replace("{valid}", String.valueOf(position.getValid()))
                .replace("{latitude}", String.valueOf(position.getLatitude()))
                .replace("{longitude}", String.valueOf(position.getLongitude()))
                .replace("{altitude}", String.valueOf(position.getAltitude()))
                .replace("{speed}", String.valueOf(position.getSpeed()))
                .replace("{course}", String.valueOf(position.getCourse()))
                .replace("{accuracy}", String.valueOf(position.getAccuracy()))
                .replace("{statusCode}", calculateStatus(position));

        if (position.getAddress() != null) {
            request = request.replace(
                    "{address}", URLEncoder.encode(position.getAddress(), StandardCharsets.UTF_8));
        }

        if (request.contains("{attributes}")) {
            String attributes = objectMapper.writeValueAsString(position.getAttributes());
            request = request.replace(
                    "{attributes}", URLEncoder.encode(attributes, StandardCharsets.UTF_8));
        }

        if (request.contains("{gprmc}")) {
            request = request.replace("{gprmc}", formatSentence(position));
        }

        return request;
    }

    /**
     * Formats a GPRMC sentence from position data.
     *
     * @param position The position to format
     * @return The formatted GPRMC sentence
     */
    private static String formatSentence(Position position) {
        StringBuilder s = new StringBuilder("$GPRMC,");

        try (Formatter f = new Formatter(s, Locale.ENGLISH)) {
            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ENGLISH);
            calendar.setTimeInMillis(position.getFixTime().getTime());

            f.format("%1$tH%1$tM%1$tS.%1$tL,A,", calendar);

            double lat = position.getLatitude();
            double lon = position.getLongitude();

            f.format("%02d%07.4f,%c,", (int) Math.abs(lat), Math.abs(lat) % 1 * 60, lat < 0 ? 'S' : 'N');
            f.format("%03d%07.4f,%c,", (int) Math.abs(lon), Math.abs(lon) % 1 * 60, lon < 0 ? 'W' : 'E');

            f.format("%.2f,%.2f,", position.getSpeed(), position.getCourse());
            f.format("%1$td%1$tm%1$ty,,", calendar);
        }

        s.append(Checksum.nmea(s.substring(1)));

        return s.toString();
    }

    /**
     * Calculates an OpenGTS status code from position data.
     *
     * @param position The position to calculate status for
     * @return The OpenGTS status code
     */
    private String calculateStatus(Position position) {
        if (position.hasAttribute(Position.KEY_ALARM)) {
            return "0xF841"; // STATUS_PANIC_ON
        } else if (position.getSpeed() < 1.0) {
            return "0xF020"; // STATUS_LOCATION
        } else {
            return "0xF11C"; // STATUS_MOTION_MOVING
        }
    }

    /**
     * Service discovery manager interface for resolving service URLs.
     */
    public interface ServiceDiscoveryManager {
        /**
         * Resolves a service name to its URL.
         *
         * @param serviceName The name of the service to resolve
         * @return The resolved service URL or null if not found
         */
        String resolveServiceUrl(String serviceName);
    }
}