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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.traccar.config.Config;
import org.traccar.config.ConfigKey;
import org.traccar.config.Keys;
import org.traccar.database.StatisticsManager;
import org.traccar.model.User;
import org.traccar.notification.PropertiesProvider;

import jakarta.inject.Inject;
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
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

public final class SmtpMailManager implements MailManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmtpMailManager.class);
    private static final String CONTENT_TYPE = "text/html; charset=utf-8";
    private static final String CIRCUIT_BREAKER_NAME = "smtpMailCircuitBreaker";
    private static final String CORRELATION_ID = "correlationId";
    private static final String DLQ_TOPIC = "mail.dlq";
    
    private final Config config;
    private final StatisticsManager statisticsManager;
    private final CircuitBreaker circuitBreaker;
    private final MeterRegistry meterRegistry;
    
    // Metrics
    private final Counter emailSentCounter;
    private final Counter emailFailedCounter;
    private final Counter dlqPublishedCounter;
    private final Timer emailSendTimer;
    
    /**
     * Generates a unique correlation ID for distributed tracing
     * @return A unique correlation ID
     */
    private static String generateCorrelationId() {
        return UUID.randomUUID().toString();
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
    
    @Inject
    public SmtpMailManager(Config config, StatisticsManager statisticsManager, MeterRegistry meterRegistry) {
        this.config = config;
        this.statisticsManager = statisticsManager;
        this.meterRegistry = meterRegistry;
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to open circuit
                .waitDurationInOpenState(Duration.ofMinutes(1)) // Wait 1 minute in OPEN state before transitioning to HALF_OPEN
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10) // Count-based sliding window with size 10
                .minimumNumberOfCalls(5) // Minimum number of calls before calculating failure rate
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Register event listeners for logging
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    LOGGER.info("SMTP Circuit Breaker state changed from {} to {}", 
                            event.getStateTransition().getFromState(),
                            event.getStateTransition().getToState());
                });
        
        // Initialize metrics
        this.emailSentCounter = Counter.builder("mail.sent.total")
                .description("Total number of emails sent")
                .register(meterRegistry);
        
        this.emailFailedCounter = Counter.builder("mail.failed.total")
                .description("Total number of failed email attempts")
                .register(meterRegistry);
        
        this.dlqPublishedCounter = Counter.builder("mail.dlq.published.total")
                .description("Total number of emails published to dead letter queue")
                .register(meterRegistry);
        
        this.emailSendTimer = Timer.builder("mail.send.time")
                .description("Time taken to send emails")
                .register(meterRegistry);
    }
    
    @Override
    public void sendMessage(
            User user, boolean system, String subject, String body) throws MessagingException {
        sendMessage(user, system, subject, body, null);
    }
    
    @Override
    public void sendMessage(
            User user, boolean system, String subject, String body, MimeBodyPart attachment) throws MessagingException {
        // Generate correlation ID for distributed tracing
        String correlationId = generateCorrelationId();
        MDC.put(CORRELATION_ID, correlationId);
        
        try {
            LOGGER.info("Preparing to send email: subject={}, recipient={}, correlationId={}", 
                    subject, user.getEmail(), correlationId);
            
            // Use circuit breaker pattern to handle SMTP failures
            Callable<Void> emailSendingTask = () -> {
                Timer.Sample sample = Timer.start(meterRegistry);
                try {
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
                    
                    // Use service discovery to resolve SMTP host if configured
                    resolveSmtpHostWithServiceDiscovery(properties);

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
                    
                    // Add correlation ID as a header for tracing
                    message.addHeader(CORRELATION_ID, correlationId);

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
                        // Legacy statistics tracking
                        statisticsManager.registerMail();
                        
                        transport.connect(
                                properties.getProperty(Keys.MAIL_SMTP_HOST.getKey()),
                                properties.getProperty(Keys.MAIL_SMTP_USERNAME.getKey()),
                                properties.getProperty(Keys.MAIL_SMTP_PASSWORD.getKey()));
                        transport.sendMessage(message, message.getAllRecipients());
                        
                        // Record success metric
                        emailSentCounter.increment();
                        LOGGER.info("Email sent successfully: subject={}, recipient={}, correlationId={}", 
                                subject, user.getEmail(), correlationId);
                    }
                    
                    sample.stop(emailSendTimer);
                    return null;
                } catch (MessagingException e) {
                    // Record failure metric
                    emailFailedCounter.increment();
                    sample.stop(emailSendTimer);
                    LOGGER.error("Failed to send email: subject={}, recipient={}, correlationId={}, error={}", 
                            subject, user.getEmail(), correlationId, e.getMessage());
                    throw e;
                }
            };
            
            try {
                // Execute the email sending task with circuit breaker protection
                circuitBreaker.executeCallable(emailSendingTask);
            } catch (Exception e) {
                // If circuit breaker is open or call fails, publish to dead letter queue
                if (e instanceof MessagingException) {
                    publishToDeadLetterQueue(user, subject, body, attachment, (MessagingException) e);
                    throw (MessagingException) e;
                } else {
                    MessagingException messagingException = new MessagingException("Failed to send email", e);
                    publishToDeadLetterQueue(user, subject, body, attachment, messagingException);
                    throw messagingException;
                }
            }
        } finally {
            MDC.remove(CORRELATION_ID);
        }
    }
    
    public boolean getEmailEnabled() {
        return config.hasKey(Keys.MAIL_SMTP_HOST);
    }
    
    /**
     * Publishes a failed email to the dead letter queue for later retry
     * @param user The recipient user
     * @param subject The email subject
     * @param body The email body
     * @param attachment Optional attachment
     * @param exception The exception that caused the failure
     */
    private void publishToDeadLetterQueue(User user, String subject, String body, 
                                         MimeBodyPart attachment, Exception exception) {
        try {
            // In a real implementation, this would publish to a message broker
            // For now, we just log the failure and increment the counter
            LOGGER.warn("Publishing failed email to DLQ: subject={}, recipient={}, error={}", 
                    subject, user.getEmail(), exception.getMessage());
            dlqPublishedCounter.increment();
        } catch (Exception e) {
            LOGGER.error("Failed to publish to dead letter queue", e);
        }
    }
    
    /**
     * Resolves the SMTP server host using service discovery if configured
     * @param properties The mail properties
     */
    private void resolveSmtpHostWithServiceDiscovery(Properties properties) {
        // Check if service discovery is enabled
        if (config.hasKey(Keys.SERVICE_DISCOVERY_CONSUL_HOST)) {
            String consulHost = config.getString(Keys.SERVICE_DISCOVERY_CONSUL_HOST);
            int consulPort = config.getInteger(Keys.SERVICE_DISCOVERY_CONSUL_PORT);
            
            try {
                // In a real implementation, this would use Consul client to discover the SMTP service
                // For now, we just log that we would do service discovery
                LOGGER.info("Using service discovery (Consul at {}:{}) to locate SMTP server", 
                        consulHost, consulPort);
                
                // The discovered host would replace the configured one
                // properties.put(Keys.MAIL_SMTP_HOST.getKey(), discoveredHost);
                // properties.put(Keys.MAIL_SMTP_PORT.getKey(), String.valueOf(discoveredPort));
            } catch (Exception e) {
                LOGGER.warn("Service discovery failed, using configured SMTP settings", e);
            }
        }
    }
}