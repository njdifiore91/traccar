/*
 * Copyright 2018 - 2023 Anton Tananaev (anton@traccar.org)
 * Copyright 2018 Andrey Kunitsyn (andrey@traccar.org)
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
package org.traccar.sms;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.decorators.Decorators;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Tag;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.DataConverter;
import org.traccar.notification.MessageException;
import org.traccar.helper.LogAction;
import org.traccar.discovery.ServiceDiscoveryManager;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.Collections;

/**
 * HTTP SMS client implementation with resilience patterns (circuit breaker, retry),
 * distributed tracing, metrics collection, and service discovery integration.
 */
public class HttpSmsClient implements SmsManager {

    private static final Logger LOGGER = Logger.getLogger(HttpSmsClient.class.getName());
    private static final String CIRCUIT_BREAKER_NAME = "smsHttpClient";
    private static final String RETRY_NAME = "smsHttpClientRetry";
    private static final String METRIC_NAME = "sms.http.client";
    private static final String CORRELATION_ID_HEADER = "X-Correlation-ID";
    
    private final Client client;
    private final String url;
    private final String authorizationHeader;
    private final String authorization;
    private final String template;
    private final MediaType mediaType;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final boolean useServiceDiscovery;
    
    private static final TextMapSetter<Map<String, String>> SETTER = new TextMapSetter<>() {
        @Override
        public void set(Map<String, String> carrier, String key, String value) {
            carrier.put(key, value);
        }
    };

    /**
     * Constructs a new HTTP SMS client with resilience patterns.
     *
     * @param config Configuration parameters
     * @param client HTTP client for making requests
     * @param meterRegistry Registry for metrics collection
     * @param openTelemetry OpenTelemetry for distributed tracing
     * @param serviceDiscoveryManager Service discovery for endpoint resolution
     */
    public HttpSmsClient(Config config, Client client, MeterRegistry meterRegistry, 
                        OpenTelemetry openTelemetry, ServiceDiscoveryManager serviceDiscoveryManager) {
        this.client = client;
        this.meterRegistry = meterRegistry;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.tracer = openTelemetry.getTracer("org.traccar.sms.HttpSmsClient");
        
        // Service discovery configuration
        this.useServiceDiscovery = config.getBoolean(Keys.SMS_HTTP_USE_SERVICE_DISCOVERY, false);
        String serviceName = config.getString(Keys.SMS_HTTP_SERVICE_NAME, "sms-service");
        
        // Resolve URL - either from service discovery or direct configuration
        if (useServiceDiscovery && serviceDiscoveryManager != null) {
            this.url = serviceDiscoveryManager.getServiceUrl(serviceName);
            LOGGER.info("Using service discovery for SMS HTTP client. Service: " + serviceName + ", URL: " + url);
        } else {
            this.url = config.getString(Keys.SMS_HTTP_URL);
            LOGGER.info("Using configured URL for SMS HTTP client: " + url);
        }
        
        // Authorization configuration
        authorizationHeader = config.getString(Keys.SMS_HTTP_AUTHORIZATION_HEADER);
        if (config.hasKey(Keys.SMS_HTTP_AUTHORIZATION)) {
            authorization = config.getString(Keys.SMS_HTTP_AUTHORIZATION);
        } else {
            String user = config.getString(Keys.SMS_HTTP_USER);
            String password = config.getString(Keys.SMS_HTTP_PASSWORD);
            if (user != null && password != null) {
                authorization = "Basic "
                        + DataConverter.printBase64((user + ":" + password).getBytes(StandardCharsets.UTF_8));
            } else {
                authorization = null;
            }
        }
        
        // Template and media type configuration
        template = config.getString(Keys.SMS_HTTP_TEMPLATE).trim();
        if (template.charAt(0) == '<') {
            mediaType = MediaType.APPLICATION_XML_TYPE;
        } else if (template.charAt(0) == '{' || template.charAt(0) == '[') {
            mediaType = MediaType.APPLICATION_JSON_TYPE;
        } else {
            mediaType = MediaType.APPLICATION_FORM_URLENCODED_TYPE;
        }
        
        // Circuit breaker configuration
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(config.getFloat(Keys.SMS_HTTP_CIRCUIT_BREAKER_FAILURE_THRESHOLD, 50.0f))
                .waitDurationInOpenState(Duration.ofMillis(
                        config.getLong(Keys.SMS_HTTP_CIRCUIT_BREAKER_WAIT_DURATION, 10000)))
                .slidingWindowSize(config.getInteger(Keys.SMS_HTTP_CIRCUIT_BREAKER_SLIDING_WINDOW_SIZE, 10))
                .permittedNumberOfCallsInHalfOpenState(
                        config.getInteger(Keys.SMS_HTTP_CIRCUIT_BREAKER_PERMITTED_HALF_OPEN_CALLS, 5))
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Retry configuration
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(config.getInteger(Keys.SMS_HTTP_RETRY_MAX_ATTEMPTS, 3))
                .waitDuration(Duration.ofMillis(config.getLong(Keys.SMS_HTTP_RETRY_WAIT_DURATION, 1000)))
                .retryExceptions(MessageException.class)
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry(RETRY_NAME);
        
        LOGGER.info("Initialized HttpSmsClient with circuit breaker and retry mechanisms");
    }
    
    /**
     * Prepares a value for inclusion in the payload based on the media type.
     *
     * @param value The value to prepare
     * @return The prepared value
     * @throws UnsupportedEncodingException If encoding fails
     */
    private String prepareValue(String value) throws UnsupportedEncodingException {
        if (mediaType == MediaType.APPLICATION_FORM_URLENCODED_TYPE) {
            return URLEncoder.encode(value, StandardCharsets.UTF_8);
        }
        if (mediaType == MediaType.APPLICATION_JSON_TYPE) {
            return new String(JsonStringEncoder.getInstance().quoteAsString(value));
        }
        return value;
    }

    /**
     * Prepares the payload for the SMS request by replacing placeholders with actual values.
     *
     * @param phone The phone number
     * @param message The message content
     * @return The prepared payload
     */
    private String preparePayload(String phone, String message) {
        try {
            return template
                    .replace("{phone}", prepareValue(phone))
                    .replace("{message}", prepareValue(message));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a request builder with appropriate headers including correlation ID for tracing.
     *
     * @param correlationId The correlation ID for tracing
     * @return The configured request builder
     */
    private Invocation.Builder getRequestBuilder(String correlationId) {
        Invocation.Builder builder = client.target(url).request();
        if (authorization != null) {
            builder = builder.header(authorizationHeader, authorization);
        }
        
        // Add correlation ID for tracing
        if (correlationId != null && !correlationId.isEmpty()) {
            builder = builder.header(CORRELATION_ID_HEADER, correlationId);
        }
        
        return builder;
    }
    
    /**
     * Fallback method when SMS sending fails
     * 
     * @param phone The phone number
     * @param message The message content
     * @param command Whether this is a command message
     * @param exception The exception that caused the failure
     * @return Always throws MessageException with enhanced information
     * @throws MessageException with enhanced error information
     */
    private String fallback(String phone, String message, boolean command, Throwable exception) throws MessageException {
        String errorMessage = "SMS sending failed";
        if (exception instanceof MessageException) {
            errorMessage += ": " + exception.getMessage();
        } else {
            errorMessage += " due to unexpected error: " + exception.getMessage();
        }
        
        LOGGER.warning(errorMessage);
        throw new MessageException(errorMessage);
    }

    /**
     * Masks a phone number for privacy in logs and metrics
     * 
     * @param phoneNumber The phone number to mask
     * @return Masked phone number (e.g., +1234****789)
     */
    private String maskPhoneNumber(String phoneNumber) {
        if (phoneNumber == null || phoneNumber.length() <= 6) {
            return phoneNumber;
        }
        
        int visibleDigits = Math.min(3, phoneNumber.length() / 3);
        return phoneNumber.substring(0, visibleDigits) + 
               "****" + 
               phoneNumber.substring(phoneNumber.length() - visibleDigits);
    }
    
    /**
     * Sends an SMS message with resilience patterns, tracing, and metrics.
     *
     * @param phone The phone number to send the message to
     * @param message The message content
     * @param command Whether this is a command message
     * @throws MessageException If the message cannot be sent
     */
    @Override
    public void sendMessage(String phone, String message, boolean command) throws MessageException {
        // Create a span for tracing
        Span span = tracer.spanBuilder("send_sms")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("phone.number.masked", maskPhoneNumber(phone))
                .setAttribute("message.length", message.length())
                .setAttribute("command", command)
                .startSpan();
        
        // Generate correlation ID from span
        String correlationId = span.getSpanContext().getTraceId();
        
        // Create context for propagation
        Context context = Context.current().with(span);
        Map<String, String> headers = new HashMap<>();
        openTelemetry.getPropagators().getTextMapPropagator().inject(context, headers, SETTER);
        
        // Create timer for metrics
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = context.makeCurrent()) {
            // Decorate the SMS sending operation with circuit breaker and retry
            Supplier<String> sendSmsSupplier = () -> {
                try (Response response = getRequestBuilder(correlationId)
                        .post(Entity.entity(preparePayload(phone, message), mediaType))) {
                    
                    int statusCode = response.getStatus();
                    span.setAttribute("http.status_code", statusCode);
                    
                    if (statusCode / 100 != 2) {
                        String errorResponse = response.readEntity(String.class);
                        span.setStatus(StatusCode.ERROR, "HTTP error: " + statusCode);
                        span.setAttribute("error.message", errorResponse);
                        throw new MessageException(errorResponse);
                    }
                    
                    return "Success";
                }
            };
            
            // Apply circuit breaker and retry patterns
            String result = Decorators.ofSupplier(sendSmsSupplier)
                    .withCircuitBreaker(circuitBreaker)
                    .withRetry(retry)
                    .withFallback(throwable -> {
                        try {
                            return fallback(phone, message, command, throwable);
                        } catch (MessageException e) {
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            throw new RuntimeException(e);
                        }
                    })
                    .get();
            
            // Record success metrics
            sample.stop(Timer.builder(METRIC_NAME)
                    .description("Time taken to send SMS messages")
                    .tags(Collections.singletonList(Tag.of("outcome", "success")))
                    .register(meterRegistry));
            
            span.setStatus(StatusCode.OK);
            LOGGER.info("Successfully sent SMS to " + maskPhoneNumber(phone) + ", correlation ID: " + correlationId);
            
        } catch (Exception e) {
            // Record failure metrics
            sample.stop(Timer.builder(METRIC_NAME)
                    .description("Time taken to send SMS messages")
                    .tags(Collections.singletonList(Tag.of("outcome", "failure")))
                    .register(meterRegistry));
            
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }