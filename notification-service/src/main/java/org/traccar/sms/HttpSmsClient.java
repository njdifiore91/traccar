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
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.notification.MessageException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * HTTP-based SMS client implementation for the Notification Service.
 * Sends SMS messages via HTTP POST requests to configured endpoints.
 * Includes circuit breaker patterns for resilience and distributed tracing.
 */
@Singleton
public class HttpSmsClient implements SmsManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSmsClient.class);
    private static final String CIRCUIT_BREAKER_NAME = "smsHttpClient";
    private static final String DEAD_LETTER_TOPIC = "notification.sms.deadletter";

    private final Client client;
    private final String url;
    private final String authorizationHeader;
    private final String authorization;
    private final String template;
    private final MediaType mediaType;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final DeadLetterService deadLetterService;
    private final HealthCheckService healthCheckService;

    /**
     * Constructs a new HttpSmsClient with the specified dependencies.
     *
     * @param config Configuration provider for SMS settings
     * @param client HTTP client for making requests
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry for circuit breaker monitoring
     * @param deadLetterService Service for handling failed deliveries
     * @param healthCheckService Service for health monitoring
     */
    @Inject
    public HttpSmsClient(
            @Named("notification") NotificationConfig config,
            Client client,
            Tracer tracer,
            MeterRegistry meterRegistry,
            DeadLetterService deadLetterService,
            HealthCheckService healthCheckService) {
        this.client = client;
        this.tracer = tracer;
        this.deadLetterService = deadLetterService;
        this.healthCheckService = healthCheckService;

        // Load configuration from notification service config
        url = config.getString("sms.http.url");
        authorizationHeader = config.getString("sms.http.authorization.header", "Authorization");
        
        if (config.hasKey("sms.http.authorization")) {
            authorization = config.getString("sms.http.authorization");
        } else {
            String user = config.getString("sms.http.user");
            String password = config.getString("sms.http.password");
            if (user != null && password != null) {
                authorization = "Basic " + java.util.Base64.getEncoder()
                        .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
            } else {
                authorization = null;
            }
        }
        
        template = config.getString("sms.http.template").trim();
        if (template.charAt(0) == '<') {
            mediaType = MediaType.APPLICATION_XML_TYPE;
        } else if (template.charAt(0) == '{' || template.charAt(0) == '[') {
            mediaType = MediaType.APPLICATION_JSON_TYPE;
        } else {
            mediaType = MediaType.APPLICATION_FORM_URLENCODED_TYPE;
        }

        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .slidingWindowSize(10) // Consider last 10 calls for failure rate
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 test calls when half-open
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Register circuit breaker metrics
        CircuitBreaker.EventPublisher eventPublisher = circuitBreaker.getEventPublisher();
        eventPublisher.onStateTransition(event -> {
            LOGGER.info("SMS HTTP client circuit breaker state changed from {} to {}", 
                    event.getStateTransition().getFromState(), 
                    event.getStateTransition().getToState());
        });
        
        // Register health check
        healthCheckService.registerHealthCheck("sms-http-client", this::checkHealth);
        
        LOGGER.info("Initialized HTTP SMS client with URL: {}", url);
    }

    /**
     * Prepares a value for inclusion in the request payload based on the media type.
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
     * Prepares the payload for the HTTP request by replacing placeholders with actual values.
     *
     * @param phone The phone number to send the SMS to
     * @param message The message content
     * @return The prepared payload
     */
    private String preparePayload(String phone, String message) {
        try {
            return template
                    .replace("{phone}", prepareValue(phone))
                    .replace("{message}", prepareValue(message));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Failed to prepare SMS payload", e);
        }
    }

    /**
     * Creates and configures a request builder for the HTTP request.
     *
     * @return The configured request builder
     */
    private Invocation.Builder getRequestBuilder() {
        Invocation.Builder builder = client.target(url).request();
        if (authorization != null) {
            builder = builder.header(authorizationHeader, authorization);
        }
        return builder;
    }

    /**
     * Sends an SMS message via HTTP, with circuit breaker protection and distributed tracing.
     *
     * @param phone The phone number to send the SMS to
     * @param message The message content
     * @param command Whether this is a command message
     * @throws MessageException If the message cannot be sent
     */
    @Override
    public void sendMessage(String phone, String message, boolean command) throws MessageException {
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("sms.send")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("sms.phone", phone)
                .setAttribute("sms.command", command)
                .setAttribute("sms.provider", "http")
                .startSpan();
        
        String correlationId = span.getSpanContext().getTraceId();
        LOGGER.debug("Sending SMS to {} with correlation ID: {}", phone, correlationId);
        
        try {
            // Execute with circuit breaker protection
            Supplier<Response> httpRequestSupplier = () -> {
                String payload = preparePayload(phone, message);
                return getRequestBuilder()
                        .header("X-Correlation-ID", correlationId)
                        .post(Entity.entity(payload, mediaType));
            };
            
            Response response = circuitBreaker.executeSupplier(httpRequestSupplier);
            
            if (response.getStatus() / 100 != 2) {
                String errorMessage = response.readEntity(String.class);
                LOGGER.error("SMS delivery failed with status {}: {}", response.getStatus(), errorMessage);
                
                // Send to dead letter queue for later retry
                SmsMessage failedMessage = new SmsMessage(phone, message, command, correlationId, errorMessage);
                deadLetterService.sendToDeadLetterQueue(DEAD_LETTER_TOPIC, failedMessage);
                
                throw new MessageException(errorMessage);
            }
            
            LOGGER.info("Successfully sent SMS to {} with correlation ID: {}", phone, correlationId);
            
        } catch (Exception e) {
            LOGGER.error("Failed to send SMS to {}: {}", phone, e.getMessage(), e);
            
            // Send to dead letter queue for later retry if not already handled
            if (!(e instanceof MessageException)) {
                SmsMessage failedMessage = new SmsMessage(phone, message, command, correlationId, e.getMessage());
                deadLetterService.sendToDeadLetterQueue(DEAD_LETTER_TOPIC, failedMessage);
            }
            
            if (e instanceof MessageException) {
                throw (MessageException) e;
            } else {
                throw new MessageException(e);
            }
        } finally {
            span.end();
        }
    }
    
    /**
     * Health check implementation for the SMS HTTP client.
     * 
     * @return true if the circuit breaker is closed (healthy), false otherwise
     */
    private boolean checkHealth() {
        CircuitBreaker.State state = circuitBreaker.getState();
        boolean isHealthy = state == CircuitBreaker.State.CLOSED;
        
        if (!isHealthy) {
            LOGGER.warn("SMS HTTP client circuit breaker is in {} state", state);
        }
        
        return isHealthy;
    }
    
    /**
     * Data class for SMS messages to be sent to the dead letter queue.
     */
    private static class SmsMessage {
        private final String phone;
        private final String message;
        private final boolean command;
        private final String correlationId;
        private final String errorMessage;
        
        public SmsMessage(String phone, String message, boolean command, String correlationId, String errorMessage) {
            this.phone = phone;
            this.message = message;
            this.command = command;
            this.correlationId = correlationId;
            this.errorMessage = errorMessage;
        }
        
        public String getPhone() {
            return phone;
        }
        
        public String getMessage() {
            return message;
        }
        
        public boolean isCommand() {
            return command;
        }
        
        public String getCorrelationId() {
            return correlationId;
        }
        
        public String getErrorMessage() {
            return errorMessage;
        }
    }
}