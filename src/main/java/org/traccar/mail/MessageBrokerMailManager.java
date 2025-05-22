/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.mail;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.User;
import org.traccar.notification.PropertiesProvider;

import jakarta.inject.Inject;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeBodyPart;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Implementation of MailManager that publishes email messages to a message broker
 * for asynchronous processing by the Notification Service.
 */
public class MessageBrokerMailManager implements MailManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBrokerMailManager.class);
    private static final String EMAIL_TOPIC = "email-out";
    private static final String CIRCUIT_BREAKER_NAME = "emailBroker";
    private static final String METRIC_PREFIX = "mail.broker";

    private final Config config;
    private final MessagePublisher messagePublisher;
    private final CircuitBreaker circuitBreaker;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    private final Counter emailRequestCounter;
    private final Counter emailSuccessCounter;
    private final Counter emailFailureCounter;
    private final Timer emailPublishTimer;

    /**
     * Interface for publishing messages to a broker.
     * This abstraction allows for different message broker implementations.
     */
    public interface MessagePublisher {
        CompletableFuture<Void> publish(String topic, Map<String, String> headers, byte[] payload);
        boolean isHealthy();
    }

    /**
     * TextMapSetter implementation for OpenTelemetry context propagation.
     */
    private static class HeaderSetter implements TextMapSetter<Map<String, String>> {
        @Override
        public void set(Map<String, String> carrier, String key, String value) {
            carrier.put(key, value);
        }
    }

    @Inject
    public MessageBrokerMailManager(
            Config config,
            MessagePublisher messagePublisher,
            ObjectMapper objectMapper,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.config = config;
        this.messagePublisher = messagePublisher;
        this.objectMapper = objectMapper;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;

        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);

        // Initialize metrics
        this.emailRequestCounter = Counter.builder(METRIC_PREFIX + ".requests")
                .description("Total number of email requests")
                .register(meterRegistry);
        this.emailSuccessCounter = Counter.builder(METRIC_PREFIX + ".success")
                .description("Number of successful email publications")
                .register(meterRegistry);
        this.emailFailureCounter = Counter.builder(METRIC_PREFIX + ".failures")
                .description("Number of failed email publications")
                .register(meterRegistry);
        this.emailPublishTimer = Timer.builder(METRIC_PREFIX + ".publish.time")
                .description("Time taken to publish email messages")
                .register(meterRegistry);
    }

    @Override
    public boolean getEmailEnabled() {
        return config.getBoolean(Keys.MAIL_BROKER_ENABLED, false);
    }

    @Override
    public void sendMessage(User user, boolean system, String subject, String body) throws MessagingException {
        sendMessage(user, system, subject, body, null);
    }

    @Override
    public void sendMessage(
            User user, boolean system, String subject, String body, MimeBodyPart attachment) throws MessagingException {

        if (!getEmailEnabled()) {
            throw new MessagingException("Email broker is not enabled");
        }

        emailRequestCounter.increment();

        try {
            // Create email message payload
            Map<String, Object> emailMessage = new HashMap<>();
            emailMessage.put("userId", user.getId());
            emailMessage.put("userEmail", user.getEmail());
            emailMessage.put("system", system);
            emailMessage.put("subject", subject);
            emailMessage.put("body", body);

            // Get SMTP properties if available
            Properties properties = null;
            if (!config.getBoolean(Keys.MAIL_SMTP_IGNORE_USER_CONFIG)) {
                properties = getProperties(new PropertiesProvider(user));
            }
            if (properties == null && (system || !config.getBoolean(Keys.MAIL_SMTP_SYSTEM_ONLY))) {
                properties = getProperties(new PropertiesProvider(config));
            }

            if (properties != null) {
                emailMessage.put("smtpProperties", properties);
            }

            // Handle attachment if present
            if (attachment != null) {
                try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                    attachment.writeTo(outputStream);
                    emailMessage.put("attachmentData", Base64.getEncoder().encodeToString(outputStream.toByteArray()));
                    emailMessage.put("attachmentContentType", attachment.getContentType());
                    if (attachment.getFileName() != null) {
                        emailMessage.put("attachmentFileName", attachment.getFileName());
                    }
                } catch (IOException e) {
                    throw new MessagingException("Failed to process attachment", e);
                }
            }

            // Serialize message to JSON
            byte[] payload;
            try {
                payload = objectMapper.writeValueAsBytes(emailMessage);
            } catch (IOException e) {
                throw new MessagingException("Failed to serialize email message", e);
            }

            // Generate correlation ID for tracing
            String correlationId = UUID.randomUUID().toString();

            // Create message headers with tracing context
            Map<String, String> headers = new HashMap<>();
            headers.put("content-type", "application/json");
            headers.put("correlation-id", correlationId);

            // Create and start a new span for the email publication
            Span span = tracer.spanBuilder("publish_email")
                    .setSpanKind(SpanKind.PRODUCER)
                    .setAttribute("email.correlation_id", correlationId)
                    .setAttribute("email.user_id", user.getId())
                    .setAttribute("email.subject", subject)
                    .startSpan();

            // Propagate the current context (including the new span) to the message headers
            tracer.getOpenTelemetry().getPropagators().getTextMapPropagator()
                    .inject(Context.current().with(span), headers, new HeaderSetter());

            try {
                // Use circuit breaker to publish message
                Supplier<CompletableFuture<Void>> publishSupplier = () -> {
                    Timer.Sample sample = Timer.start(meterRegistry);
                    return messagePublisher.publish(EMAIL_TOPIC, headers, payload)
                            .whenComplete((result, error) -> {
                                sample.stop(emailPublishTimer);
                                if (error == null) {
                                    emailSuccessCounter.increment();
                                    LOGGER.debug("Email message published successfully: {}", correlationId);
                                } else {
                                    emailFailureCounter.increment();
                                    LOGGER.error("Failed to publish email message: {}", correlationId, error);
                                }
                            });
                };

                CompletableFuture<Void> future = circuitBreaker.executeCompletionStage(publishSupplier).toCompletableFuture();

                // Wait for the message to be published with a timeout
                try {
                    future.get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new MessagingException("Failed to publish email message to broker", e);
                }
            } finally {
                span.end();
            }
        } catch (MessagingException e) {
            emailFailureCounter.increment();
            throw e;
        }
    }

    /**
     * Extract SMTP properties from a provider.
     * This method is similar to the one in SmtpMailManager to maintain compatibility.
     */
    private static Properties getProperties(PropertiesProvider provider) {
        String host = provider.getString(Keys.MAIL_SMTP_HOST);
        if (host != null) {
            Properties properties = new Properties();
            properties.put("host", host);
            properties.put("port", String.valueOf(provider.getInteger(Keys.MAIL_SMTP_PORT)));
            properties.put("protocol", provider.getString(Keys.MAIL_TRANSPORT_PROTOCOL));

            Boolean starttlsEnable = provider.getBoolean(Keys.MAIL_SMTP_STARTTLS_ENABLE);
            if (starttlsEnable != null) {
                properties.put("starttlsEnable", String.valueOf(starttlsEnable));
            }

            Boolean starttlsRequired = provider.getBoolean(Keys.MAIL_SMTP_STARTTLS_REQUIRED);
            if (starttlsRequired != null) {
                properties.put("starttlsRequired", String.valueOf(starttlsRequired));
            }

            Boolean sslEnable = provider.getBoolean(Keys.MAIL_SMTP_SSL_ENABLE);
            if (sslEnable != null) {
                properties.put("sslEnable", String.valueOf(sslEnable));
            }

            String sslTrust = provider.getString(Keys.MAIL_SMTP_SSL_TRUST);
            if (sslTrust != null) {
                properties.put("sslTrust", sslTrust);
            }

            String sslProtocols = provider.getString(Keys.MAIL_SMTP_SSL_PROTOCOLS);
            if (sslProtocols != null) {
                properties.put("sslProtocols", sslProtocols);
            }

            String username = provider.getString(Keys.MAIL_SMTP_USERNAME);
            if (username != null) {
                properties.put("username", username);
            }

            String password = provider.getString(Keys.MAIL_SMTP_PASSWORD);
            if (password != null) {
                properties.put("password", password);
            }

            String from = provider.getString(Keys.MAIL_SMTP_FROM);
            if (from != null) {
                properties.put("from", from);
            }

            String fromName = provider.getString(Keys.MAIL_SMTP_FROM_NAME);
            if (fromName != null) {
                properties.put("fromName", fromName);
            }

            return properties;
        }
        return null;
    }

    /**
     * Check if the message broker connection is healthy.
     * This can be used for health checks in the microservices architecture.
     *
     * @return true if the broker connection is healthy, false otherwise
     */
    public boolean isHealthy() {
        return messagePublisher.isHealthy() && !circuitBreaker.getState().equals(CircuitBreaker.State.OPEN);
    }

    /**
     * Simple data class to hold SMTP properties.
     */
    private static class Properties {
        private final Map<String, String> properties = new HashMap<>();

        public void put(String key, String value) {
            properties.put(key, value);
        }

        public Map<String, String> getProperties() {
            return properties;
        }
    }
}