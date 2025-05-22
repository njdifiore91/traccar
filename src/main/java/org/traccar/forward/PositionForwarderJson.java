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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.session.cache.CacheManager;
import org.traccar.discovery.ServiceDiscovery;

import jakarta.inject.Inject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.InvocationCallback;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class PositionForwarderJson implements PositionForwarder {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionForwarderJson.class);
    private static final String CORRELATION_ID_HEADER = "X-Correlation-ID";
    private static final String FORWARD_SERVICE_NAME_KEY = "forward.serviceName";

    private final String header;
    private final Client client;
    private final ObjectMapper objectMapper;
    private final CacheManager cacheManager;
    private final ServiceDiscovery serviceDiscovery;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    
    // Metrics
    private final Timer requestTimer;
    private final Counter successCounter;
    private final Counter failureCounter;

    @Inject
    public PositionForwarderJson(Config config, Client client, ObjectMapper objectMapper, 
                               CacheManager cacheManager, ServiceDiscovery serviceDiscovery,
                               MeterRegistry meterRegistry) {
        this.client = client;
        this.objectMapper = objectMapper;
        this.cacheManager = cacheManager;
        this.serviceDiscovery = serviceDiscovery;
        this.header = config.getString(Keys.FORWARD_HEADER);
        
        // Initialize metrics
        this.requestTimer = Timer.builder("position.forward.request.duration")
                .description("Time taken to forward position data")
                .register(meterRegistry);
        this.successCounter = Counter.builder("position.forward.success")
                .description("Number of successful position forwards")
                .register(meterRegistry);
        this.failureCounter = Counter.builder("position.forward.failure")
                .description("Number of failed position forwards")
                .register(meterRegistry);
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("positionForwarder");
        
        // Configure retry mechanism
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(RuntimeException.class)
                .build();
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry("positionForwarder");
        
        // Register metrics for circuit breaker and retry
        circuitBreaker.getEventPublisher()
                .onSuccess(event -> successCounter.increment())
                .onError(event -> failureCounter.increment());
    }

    @Override
    public void forward(PositionData positionData, ResultHandler resultHandler) {
        // Generate correlation ID for tracing
        final String correlationId = UUID.randomUUID().toString();
        LOGGER.debug("Starting position forward with correlationId: {}", correlationId);
        
        // Resolve endpoint using service discovery or fallback to device attribute
        String url = resolveEndpoint(positionData);
        if (url.isBlank()) {
            resultHandler.onResult(true, null);
            return;
        }

        try {
            // Prepare the request
            var requestBuilder = client.target(url).request();

            // Add correlation ID header for distributed tracing
            requestBuilder.header(CORRELATION_ID_HEADER, correlationId);
            
            MediaType mediaType = MediaType.APPLICATION_JSON_TYPE;
            if (header != null && !header.isEmpty()) {
                for (String line: header.split("\\r?\\n")) {
                    String[] values = line.split(":", 2);
                    if (values.length == 2) {
                        String headerName = values[0].trim();
                        String headerValue = values[1].trim();
                        if (headerName.equals(HttpHeaders.CONTENT_TYPE)) {
                            mediaType = MediaType.valueOf(headerValue);
                        } else {
                            requestBuilder.header(headerName, headerValue);
                        }
                    }
                }
            }

            final String jsonPayload = objectMapper.writeValueAsString(positionData);
            final Entity<String> entity = Entity.entity(jsonPayload, mediaType);
            final MediaType finalMediaType = mediaType;
            
            // Create a supplier that will be decorated with circuit breaker and retry
            Supplier<Response> httpCallSupplier = () -> {
                Timer.Sample sample = Timer.start();
                try {
                    LOGGER.debug("Executing HTTP request with correlationId: {}", correlationId);
                    // Create a new request builder for each attempt to avoid stale state
                    var freshRequestBuilder = client.target(url).request()
                            .header(CORRELATION_ID_HEADER, correlationId);
                    
                    // Re-apply headers if needed
                    if (header != null && !header.isEmpty()) {
                        for (String line: header.split("\\r?\\n")) {
                            String[] values = line.split(":", 2);
                            if (values.length == 2) {
                                String headerName = values[0].trim();
                                String headerValue = values[1].trim();
                                if (!headerName.equals(HttpHeaders.CONTENT_TYPE)) {
                                    freshRequestBuilder.header(headerName, headerValue);
                                }
                            }
                        }
                    }
                    
                    // Execute the request synchronously for the circuit breaker/retry
                    Response response = freshRequestBuilder.post(Entity.entity(jsonPayload, finalMediaType));
                    sample.stop(requestTimer);
                    return response;
                } catch (Exception e) {
                    sample.stop(requestTimer);
                    LOGGER.warn("HTTP request failed with correlationId: {}, error: {}", correlationId, e.getMessage());
                    throw new RuntimeException("HTTP request failed", e);
                }
            };
            
            // Decorate the supplier with circuit breaker and retry
            Supplier<Response> decoratedSupplier = CircuitBreaker.decorateSupplier(circuitBreaker, httpCallSupplier);
            decoratedSupplier = Retry.decorateSupplier(retry, decoratedSupplier);
            
            // Execute the decorated supplier asynchronously
            CompletableFuture.supplyAsync(decoratedSupplier)
                .thenAccept(response -> {
                    try {
                        if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
                            LOGGER.debug("Position forward successful with correlationId: {}", correlationId);
                            resultHandler.onResult(true, null);
                        } else {
                            int code = response.getStatusInfo().getStatusCode();
                            LOGGER.warn("Position forward failed with HTTP code {} and correlationId: {}", 
                                    code, correlationId);
                            resultHandler.onResult(false, new RuntimeException("HTTP code " + code));
                        }
                    } finally {
                        // Always close the response to release resources
                        response.close();
                    }
                })
                .exceptionally(throwable -> {
                    LOGGER.warn("Position forward failed with correlationId: {}, error: {}", 
                            correlationId, throwable.getMessage());
                    resultHandler.onResult(false, throwable);
                    return null;
                });
                
        } catch (JsonProcessingException e) {
            failureCounter.increment();
            LOGGER.error("JSON processing error with correlationId: {}", correlationId, e);
            resultHandler.onResult(false, e);
        } catch (Exception e) {
            failureCounter.increment();
            LOGGER.error("Unexpected error during position forward with correlationId: {}", correlationId, e);
            resultHandler.onResult(false, e);
        }
    }
    
    private String resolveEndpoint(PositionData positionData) {
        // First try to resolve via service discovery
        try {
            String serviceName = AttributeUtil.lookup(cacheManager, FORWARD_SERVICE_NAME_KEY, positionData.getDevice().getId());
            if (serviceName != null && !serviceName.isBlank()) {
                String resolvedUrl = serviceDiscovery.resolveService(serviceName);
                if (resolvedUrl != null && !resolvedUrl.isBlank()) {
                    LOGGER.debug("Resolved endpoint {} via service discovery for device {}", 
                            resolvedUrl, positionData.getDevice().getId());
                    return resolvedUrl;
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Error resolving service via discovery for device {}, falling back to direct URL", 
                    positionData.getDevice().getId(), e);
        }
        
        // Fallback to direct URL from device attributes
        return AttributeUtil.lookup(cacheManager, Keys.FORWARD_URL, positionData.getDevice().getId());
    }
}