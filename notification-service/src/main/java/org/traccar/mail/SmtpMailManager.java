/*
 * Copyright 2016 - 2022 Anton Tananaev (anton@traccar.org)
 * Copyright 2017 - 2018 Andrey Kunitsyn (andrey@traccar.org)
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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.ConfigKey;
import org.traccar.config.Keys;
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.User;
import org.traccar.notification.PropertiesProvider;

import jakarta.mail.BodyPart;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public final class SmtpMailManager implements MailManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmtpMailManager.class);
    private static final String CONTENT_TYPE = "text/html; charset=utf-8";
    private static final String CORRELATION_ID_HEADER = "X-Correlation-ID";
    private static final String DEAD_LETTER_TOPIC = "email-dead-letter";

    private final Config config;
    private final MeterRegistry meterRegistry;
    private final MessagePublisher messagePublisher;
    private final Tracer tracer;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    
    private final Counter emailSuccessCounter;
    private final Counter emailFailureCounter;
    private final Timer emailSendTimer;

    public SmtpMailManager(Config config, MeterRegistry meterRegistry, MessagePublisher messagePublisher, Tracer tracer) {
        this.config = config;
        this.meterRegistry = meterRegistry;
        this.messagePublisher = messagePublisher;
        this.tracer = tracer;
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before attempting to close
                .slidingWindowSize(10) // Consider last 10 calls for failure rate
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 test calls when half-open
                .recordExceptions(MessagingException.class, TimeoutException.class)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("smtpCircuitBreaker");
        
        // Initialize retry mechanism
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Maximum 3 attempts
                .waitDuration(Duration.ofSeconds(1)) // Initial wait duration
                .exponentialBackoff(2, Duration.ofSeconds(10)) // Exponential backoff with max 10 seconds
                .retryExceptions(MessagingException.class, TimeoutException.class)
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry("smtpRetry");
        
        // Initialize metrics
        this.emailSuccessCounter = meterRegistry.counter("email.send.success");
        this.emailFailureCounter = meterRegistry.counter("email.send.failure");
        this.emailSendTimer = meterRegistry.timer("email.send.time");
        
        // Register circuit breaker events for metrics
        circuitBreaker.getEventPublisher()
                .onSuccess(event -> emailSuccessCounter.increment())
                .onError(event -> {
                    emailFailureCounter.increment();
                    LOGGER.error("Email sending failed: {}", event.getThrowable().getMessage());
                });
    }

    private static void copyBooleanProperty(
            Properties properties, PropertiesProvider provider, ConfigKey<Boolean> key) {
        Boolean value = provider.getBoolean(key);
        if (value != null) {
            properties.put(key.getKey(), String.valueOf(value));
        }
    }

    private static void copyStringProperty(
            Properties properties, PropertiesProvider provider, ConfigKey<String> key) {
        String value = provider.getString(key);
        if (value != null) {
            properties.put(key.getKey(), value);
        }
    }

    private static Properties getProperties(PropertiesProvider provider) {
        String host = provider.getString(Keys.MAIL_SMTP_HOST);
        if (host != null) {
            Properties properties = new Properties();

            properties.put(Keys.MAIL_TRANSPORT_PROTOCOL.getKey(), provider.getString(Keys.MAIL_TRANSPORT_PROTOCOL));
            properties.put(Keys.MAIL_SMTP_HOST.getKey(), host);
            properties.put(Keys.MAIL_SMTP_PORT.getKey(), String.valueOf(provider.getInteger(Keys.MAIL_SMTP_PORT)));

            copyBooleanProperty(properties, provider, Keys.MAIL_SMTP_STARTTLS_ENABLE);
            copyBooleanProperty(properties, provider, Keys.MAIL_SMTP_STARTTLS_REQUIRED);
            copyBooleanProperty(properties, provider, Keys.MAIL_SMTP_SSL_ENABLE);
            copyStringProperty(properties, provider, Keys.MAIL_SMTP_SSL_TRUST);
            copyStringProperty(properties, provider, Keys.MAIL_SMTP_SSL_PROTOCOLS);
            copyStringProperty(properties, provider, Keys.MAIL_SMTP_USERNAME);
            copyStringProperty(properties, provider, Keys.MAIL_SMTP_PASSWORD);
            copyStringProperty(properties, provider, Keys.MAIL_SMTP_FROM);
            copyStringProperty(properties, provider, Keys.MAIL_SMTP_FROM_NAME);

            return properties;
        }
        return null;
    }

    public boolean getEmailEnabled() {
        return config.hasKey(Keys.MAIL_SMTP_HOST);
    }

    @Override
    public void sendMessage(
            User user, boolean system, String subject, String body) throws MessagingException {
        sendMessage(user, system, subject, body, null);
    }

    @Override
    public void sendMessage(
            User user, boolean system, String subject, String body, MimeBodyPart attachment) throws MessagingException {
        
        // Capture current trace context for correlation
        SpanContext currentContext = Span.current().getSpanContext();
        String correlationId = currentContext.isValid() ? currentContext.getTraceId() : null;
        
        // Use timer to measure email sending time
        emailSendTimer.record(() -> {
            try {
                // Execute with circuit breaker and retry
                Supplier<Void> emailSendingSupplier = () -> {
                    try {
                        doSendMessage(user, system, subject, body, attachment, correlationId);
                        return null;
                    } catch (MessagingException e) {
                        throw new RuntimeException(e);
                    }
                };
                
                Supplier<Void> decoratedSupplier = Retry.decorateSupplier(retry, 
                        CircuitBreaker.decorateSupplier(circuitBreaker, emailSendingSupplier));
                
                decoratedSupplier.get();
                return null;
            } catch (Exception e) {
                // Publish to dead letter queue for failed deliveries
                if (correlationId != null) {
                    publishToDeadLetterQueue(user, subject, body, correlationId, e);
                }
                
                if (e.getCause() instanceof MessagingException) {
                    throw (MessagingException) e.getCause();
                } else {
                    throw new MessagingException("Failed to send email", e);
                }
            }
        });
    }
    
    private void doSendMessage(
            User user, boolean system, String subject, String body, MimeBodyPart attachment, String correlationId) 
            throws MessagingException {

        Properties properties = null;
        if (!config.getBoolean(Keys.MAIL_SMTP_IGNORE_USER_CONFIG)) {
            properties = getProperties(new PropertiesProvider(user));
        }
        if (properties == null && (system || !config.getBoolean(Keys.MAIL_SMTP_SYSTEM_ONLY))) {
            properties = getProperties(new PropertiesProvider(config));
        }
        if (properties == null) {
            throw new MessagingException("No SMTP configuration found");
        }

        Session session = Session.getInstance(properties);

        MimeMessage message = new MimeMessage(session);

        String from = properties.getProperty(Keys.MAIL_SMTP_FROM.getKey());
        if (from != null) {
            String fromName = properties.getProperty(Keys.MAIL_SMTP_FROM_NAME.getKey());
            if (fromName != null) {
                try {
                    message.setFrom(new InternetAddress(from, fromName));
                } catch (UnsupportedEncodingException e) {
                    throw new MessagingException("Email address issue");
                }
            } else {
                message.setFrom(new InternetAddress(from));
            }
        }

        message.addRecipient(Message.RecipientType.TO, new InternetAddress(user.getEmail()));
        message.setSubject(subject);
        message.setSentDate(new Date());
        
        // Add correlation ID header for distributed tracing
        if (correlationId != null) {
            message.addHeader(CORRELATION_ID_HEADER, correlationId);
        }

        if (attachment != null) {
            Multipart multipart = new MimeMultipart();

            BodyPart messageBodyPart = new MimeBodyPart();
            messageBodyPart.setContent(body, CONTENT_TYPE);
            multipart.addBodyPart(messageBodyPart);
            multipart.addBodyPart(attachment);

            message.setContent(multipart);
        } else {
            message.setContent(body, CONTENT_TYPE);
        }

        try (Transport transport = session.getTransport()) {
            transport.connect(
                    properties.getProperty(Keys.MAIL_SMTP_HOST.getKey()),
                    properties.getProperty(Keys.MAIL_SMTP_USERNAME.getKey()),
                    properties.getProperty(Keys.MAIL_SMTP_PASSWORD.getKey()));
            transport.sendMessage(message, message.getAllRecipients());
        }
    }
    
    private void publishToDeadLetterQueue(User user, String subject, String body, String correlationId, Exception exception) {
        try {
            EmailDeadLetterEvent event = new EmailDeadLetterEvent();
            event.setUserEmail(user.getEmail());
            event.setSubject(subject);
            event.setBody(body);
            event.setCorrelationId(correlationId);
            event.setErrorMessage(exception.getMessage());
            event.setTimestamp(new Date());
            
            messagePublisher.publish(DEAD_LETTER_TOPIC, event);
            LOGGER.info("Published failed email to dead letter queue with correlation ID: {}", correlationId);
        } catch (Exception e) {
            LOGGER.error("Failed to publish to dead letter queue: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Data class for dead letter queue events
     */
    private static class EmailDeadLetterEvent {
        private String userEmail;
        private String subject;
        private String body;
        private String correlationId;
        private String errorMessage;
        private Date timestamp;
        
        public String getUserEmail() {
            return userEmail;
        }
        
        public void setUserEmail(String userEmail) {
            this.userEmail = userEmail;
        }
        
        public String getSubject() {
            return subject;
        }
        
        public void setSubject(String subject) {
            this.subject = subject;
        }
        
        public String getBody() {
            return body;
        }
        
        public void setBody(String body) {
            this.body = body;
        }
        
        public String getCorrelationId() {
            return correlationId;
        }
        
        public void setCorrelationId(String correlationId) {
            this.correlationId = correlationId;
        }
        
        public String getErrorMessage() {
            return errorMessage;
        }
        
        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }
        
        public Date getTimestamp() {
            return timestamp;
        }
        
        public void setTimestamp(Date timestamp) {
            this.timestamp = timestamp;
        }
    }
}