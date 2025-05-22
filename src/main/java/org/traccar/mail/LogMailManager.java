/*
 * Copyright 2022 - 2023 Anton Tananaev (anton@traccar.org)
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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.traccar.model.User;

import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeBodyPart;
import java.util.HashMap;
import java.util.Map;

/**
 * A MailManager implementation that logs emails instead of sending them.
 * Supports distributed tracing with correlation IDs, structured logging,
 * and metrics collection.
 */
@Singleton
public class LogMailManager implements MailManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMailManager.class);
    private final MeterRegistry meterRegistry;
    private final Counter emailCounter;
    private final Counter emailErrorCounter;
    private boolean healthy = true;

    /**
     * Constructs a new LogMailManager with metrics collection.
     *
     * @param meterRegistry the meter registry for metrics collection
     */
    @Inject
    public LogMailManager(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.emailCounter = Counter.builder("mail.messages.logged.total")
                .description("Total number of email messages logged")
                .tag("service", "mail-manager")
                .tag("implementation", "log")
                .register(meterRegistry);
        this.emailErrorCounter = Counter.builder("mail.messages.error.total")
                .description("Total number of email message errors")
                .tag("service", "mail-manager")
                .tag("implementation", "log")
                .register(meterRegistry);
    }

    @Override
    public boolean getEmailEnabled() {
        return true;
    }

    @Override
    public void sendMessage(
            User user, boolean system, String subject, String body) throws MessagingException {
        sendMessage(user, system, subject, body, null, null);
    }

    /**
     * Send a message with correlation ID for distributed tracing.
     *
     * @param user recipient user
     * @param system whether this is a system message
     * @param subject email subject
     * @param body email body
     * @param correlationId unique identifier for distributed tracing
     * @throws MessagingException if there is an error sending the message
     */
    public void sendMessage(
            User user, boolean system, String subject, String body, String correlationId) throws MessagingException {
        sendMessage(user, system, subject, body, null, correlationId);
    }

    @Override
    public void sendMessage(
            User user, boolean system, String subject, String body, MimeBodyPart attachment) throws MessagingException {
        sendMessage(user, system, subject, body, attachment, null);
    }

    /**
     * Send a message with attachment and correlation ID for distributed tracing.
     *
     * @param user recipient user
     * @param system whether this is a system message
     * @param subject email subject
     * @param body email body
     * @param attachment email attachment
     * @param correlationId unique identifier for distributed tracing
     * @throws MessagingException if there is an error sending the message
     */
    public void sendMessage(
            User user, boolean system, String subject, String body, MimeBodyPart attachment, String correlationId)
            throws MessagingException {
        try {
            // Set correlation ID in MDC for distributed tracing if provided
            if (correlationId != null && !correlationId.isEmpty()) {
                MDC.put("correlationId", correlationId);
            }

            // Create structured log data
            Map<String, Object> logData = new HashMap<>();
            logData.put("event", "email_logged");
            logData.put("recipient", user.getEmail());
            logData.put("subject", subject);
            logData.put("system", system);
            logData.put("hasAttachment", attachment != null);
            if (attachment != null) {
                logData.put("attachmentName", attachment.getFileName());
            }
            logData.put("body", body);
            if (correlationId != null && !correlationId.isEmpty()) {
                logData.put("correlationId", correlationId);
            }

            // Log the structured data
            LOGGER.info("Email sent: {}", logData);

            // Increment metrics counter with tags
            Tags tags = Tags.of(
                    "system", String.valueOf(system),
                    "hasAttachment", String.valueOf(attachment != null),
                    "service", "mail-manager",
                    "implementation", "log");
            emailCounter.increment();
            meterRegistry.counter("mail.messages.logged.total", tags).increment();
        } catch (Exception e) {
            // Log error and increment error counter
            LOGGER.error("Error logging email: {}", e.getMessage(), e);
            emailErrorCounter.increment();
            healthy = false;
            throw new MessagingException("Error logging email", e);
        } finally {
            // Clean up MDC
            if (correlationId != null && !correlationId.isEmpty()) {
                MDC.remove("correlationId");
            }
        }
    }

    /**
     * Check if the mail manager is healthy.
     *
     * @return true if the mail manager is healthy, false otherwise
     */
    public boolean isHealthy() {
        return healthy;
    }

    /**
     * Reset the health status of the mail manager.
     * This can be called by health check services to reset the health status
     * after a failure has been resolved.
     */
    public void resetHealth() {
        healthy = true;
    }
}