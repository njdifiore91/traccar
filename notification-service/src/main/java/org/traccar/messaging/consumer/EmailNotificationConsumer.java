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
package org.traccar.messaging.consumer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeBodyPart;
import org.apache.velocity.VelocityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.traccar.mail.MailManager;
import org.traccar.model.Notification;
import org.traccar.model.Server;
import org.traccar.model.User;
import org.traccar.notification.NotificationMessage;
import org.traccar.template.TemplateEngine;
import org.traccar.template.TemplateEngine.TemplateSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Date;
import java.util.UUID;

/**
 * EmailNotificationConsumer is responsible for consuming email notification messages from the 'email-out' topic
 * and delivering them through the configured email service. It handles template rendering, attachment processing,
 * and delivery status tracking.
 * <p>
 * This consumer is a critical component in the notification pipeline, responsible for the final delivery of
 * email notifications to recipients. It supports multiple email providers and implements resilience patterns
 * to ensure reliable delivery.
 */
@Singleton
public class EmailNotificationConsumer extends BaseMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmailNotificationConsumer.class);
    private static final String EMAIL_OUT_TOPIC = "email-out";
    private static final String EMAIL_STATUS_TOPIC = "email-status";

    private final MailManager mailManager;
    private final TemplateEngine templateEngine;
    private final Server server;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    @Value("${notification.email.template.path:templates/email}")
    private String templatePath;

    @Value("${notification.email.template.source:CLASSPATH}")
    private String templateSource;

    /**
     * Creates a new EmailNotificationConsumer with the specified dependencies.
     *
     * @param mailManager The mail manager for sending emails
     * @param templateEngine The template engine for rendering email content
     * @param server The server configuration
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param meterRegistry The meter registry for metrics collection
     */
    @Inject
    public EmailNotificationConsumer(
            MailManager mailManager,
            TemplateEngine templateEngine,
            Server server,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.mailManager = mailManager;
        this.templateEngine = templateEngine;
        this.server = server;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Set template path for email templates
        templateEngine.setTemplatePath(TemplateSource.valueOf(templateSource), templatePath);
        
        LOGGER.info("EmailNotificationConsumer initialized with template source: {} and path: {}", 
                templateSource, templatePath);
    }
    
    /**
     * Kafka listener for the email-out topic. Processes email notification messages and sends emails.
     *
     * @param emailMessage The email message to process
     * @param correlationId The correlation ID for distributed tracing
     */
    @KafkaListener(topics = "${kafka.topic.email-out:email-out}", groupId = "${spring.kafka.consumer.group-id}")
    public void processEmailMessage(
            @Payload EmailMessage emailMessage,
            @Header(value = "correlationId", required = false) String correlationId) {
        
        // Generate correlation ID if not provided
        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
        }
        
        final String finalCorrelationId = correlationId;
        
        Span span = tracer.spanBuilder("processEmailMessage")
                .setAttribute("correlationId", finalCorrelationId)
                .setAttribute("messageId", emailMessage.getMessageId())
                .setAttribute("userId", emailMessage.getUser().getId())
                .setAttribute("notificationType", emailMessage.getNotification().getType())
                .setSpanKind(SpanKind.CONSUMER)
                .startSpan();
        
        Timer.Sample timer = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            LOGGER.debug("Processing email message with correlation ID: {}", finalCorrelationId);
            
            // Record metric for received messages
            meterRegistry.counter("email.message.received",
                    "type", emailMessage.getNotification().getType()).increment();
            
            // Process the email message
            processEmail(emailMessage, finalCorrelationId);
            
            // Publish status update
            publishEmailStatus(emailMessage, finalCorrelationId, true, null);
            
            // Record metric for successful processing
            meterRegistry.counter("email.message.processed",
                    "type", emailMessage.getNotification().getType(),
                    "status", "success").increment();
            
            LOGGER.debug("Successfully processed email message with correlation ID: {}", finalCorrelationId);
        } catch (Exception e) {
            span.recordException(e);
            span.setAttribute("error", true);
            
            // Record metric for failed processing
            meterRegistry.counter("email.message.processed",
                    "type", emailMessage.getNotification().getType(),
                    "status", "error",
                    "error", e.getClass().getSimpleName()).increment();
            
            LOGGER.error("Failed to process email message with correlation ID: {}", finalCorrelationId, e);
            
            // Publish to dead letter queue
            publishToDeadLetterQueue(emailMessage.getNotification(), emailMessage.getUser(), finalCorrelationId, e);
            
            // Publish status update with error
            publishEmailStatus(emailMessage, finalCorrelationId, false, e.getMessage());
        } finally {
            timer.stop(meterRegistry.timer("email.message.processing.time",
                    "type", emailMessage.getNotification().getType()));
            span.end();
        }
    }
    
    /**
     * Processes an email message by rendering the template and sending the email.
     *
     * @param emailMessage The email message to process
     * @param correlationId The correlation ID for distributed tracing
     * @throws MessagingException If there is an error sending the email
     */
    private void processEmail(EmailMessage emailMessage, String correlationId) throws MessagingException {
        Span span = tracer.spanBuilder("processEmail")
                .setAttribute("correlationId", correlationId)
                .setAttribute("messageId", emailMessage.getMessageId())
                .setAttribute("templateName", emailMessage.getTemplateName())
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Prepare template context
            VelocityContext context = templateEngine.prepareContext(server, emailMessage.getUser());
            
            // Add notification-specific variables to context
            context.put("notification", emailMessage.getNotification());
            context.put("subject", emailMessage.getSubject());
            if (emailMessage.getPosition() != null) {
                context.put("position", emailMessage.getPosition());
            }
            if (emailMessage.getEvent() != null) {
                context.put("event", emailMessage.getEvent());
            }
            if (emailMessage.getDevice() != null) {
                context.put("device", emailMessage.getDevice());
            }
            if (emailMessage.getGeofence() != null) {
                context.put("geofence", emailMessage.getGeofence());
            }
            if (emailMessage.getMaintenanceId() > 0) {
                context.put("maintenanceId", emailMessage.getMaintenanceId());
            }
            
            // Add custom properties if any
            if (emailMessage.getProperties() != null) {
                emailMessage.getProperties().forEach(context::put);
            }
            
            // Render template
            NotificationMessage message = templateEngine.formatMessage(
                    context, 
                    emailMessage.getTemplateName(), 
                    TemplateSource.valueOf(templateSource));
            
            // Process attachment if present
            MimeBodyPart attachment = null;
            if (emailMessage.getAttachment() != null) {
                attachment = createAttachment(emailMessage.getAttachment());
            }
            
            // Send email
            if (attachment != null) {
                mailManager.sendMessage(
                        correlationId,
                        emailMessage.getSourceService(),
                        emailMessage.getNotification().getType(),
                        emailMessage.getUser(),
                        false,
                        message.getSubject(),
                        message.getBody(),
                        attachment);
            } else {
                mailManager.sendMessage(
                        correlationId,
                        emailMessage.getSourceService(),
                        emailMessage.getNotification().getType(),
                        emailMessage.getUser(),
                        false,
                        message.getSubject(),
                        message.getBody());
            }
            
            LOGGER.debug("Email sent successfully with correlation ID: {}", correlationId);
        } catch (Exception e) {
            span.recordException(e);
            span.setAttribute("error", true);
            LOGGER.error("Failed to process email: {}", e.getMessage(), e);
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Creates a MIME attachment from the attachment data in the email message.
     *
     * @param attachment The attachment data
     * @return The MIME body part containing the attachment
     * @throws MessagingException If there is an error creating the attachment
     */
    private MimeBodyPart createAttachment(EmailAttachment attachment) throws MessagingException {
        try {
            MimeBodyPart mimeBodyPart = new MimeBodyPart();
            byte[] decodedData = Base64.getDecoder().decode(attachment.getData());
            mimeBodyPart.setDataHandler(new jakarta.activation.DataHandler(
                    new jakarta.activation.DataSource() {
                        @Override
                        public java.io.InputStream getInputStream() throws IOException {
                            return new ByteArrayInputStream(decodedData);
                        }

                        @Override
                        public java.io.OutputStream getOutputStream() throws IOException {
                            throw new UnsupportedOperationException("Read-only data");
                        }

                        @Override
                        public String getContentType() {
                            return attachment.getContentType();
                        }

                        @Override
                        public String getName() {
                            return attachment.getFileName();
                        }
                    }));
            mimeBodyPart.setFileName(attachment.getFileName());
            return mimeBodyPart;
        } catch (Exception e) {
            LOGGER.error("Failed to create attachment: {}", e.getMessage(), e);
            throw new MessagingException("Failed to create attachment", e);
        }
    }
    
    /**
     * Publishes an email status update to the notification status topic.
     *
     * @param emailMessage The email message
     * @param correlationId The correlation ID for distributed tracing
     * @param success Whether the email was sent successfully
     * @param errorMessage The error message if the email failed to send
     */
    private void publishEmailStatus(EmailMessage emailMessage, String correlationId, boolean success, String errorMessage) {
        try {
            EmailStatusMessage statusMessage = new EmailStatusMessage();
            statusMessage.setMessageId(emailMessage.getMessageId());
            statusMessage.setUserId(emailMessage.getUser().getId());
            statusMessage.setNotificationId(emailMessage.getNotification().getId());
            statusMessage.setCorrelationId(correlationId);
            statusMessage.setSuccess(success);
            statusMessage.setErrorMessage(errorMessage);
            statusMessage.setTimestamp(new Date());
            
            kafkaTemplate.send(notificationStatusTopic, correlationId, statusMessage);
            LOGGER.debug("Published email status update for message ID: {} with correlation ID: {}", 
                    emailMessage.getMessageId(), correlationId);
        } catch (Exception e) {
            LOGGER.error("Failed to publish email status update: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Email message data class for Kafka/RabbitMQ messages.
     */
    public static class EmailMessage {
        private String messageId;
        private Notification notification;
        private User user;
        private String subject;
        private String templateName;
        private String sourceService;
        private Object position;
        private Object event;
        private Object device;
        private Object geofence;
        private long maintenanceId;
        private java.util.Map<String, Object> properties;
        private EmailAttachment attachment;
        
        public String getMessageId() {
            return messageId;
        }
        
        public void setMessageId(String messageId) {
            this.messageId = messageId;
        }
        
        public Notification getNotification() {
            return notification;
        }
        
        public void setNotification(Notification notification) {
            this.notification = notification;
        }
        
        public User getUser() {
            return user;
        }
        
        public void setUser(User user) {
            this.user = user;
        }
        
        public String getSubject() {
            return subject;
        }
        
        public void setSubject(String subject) {
            this.subject = subject;
        }
        
        public String getTemplateName() {
            return templateName;
        }
        
        public void setTemplateName(String templateName) {
            this.templateName = templateName;
        }
        
        public String getSourceService() {
            return sourceService;
        }
        
        public void setSourceService(String sourceService) {
            this.sourceService = sourceService;
        }
        
        public Object getPosition() {
            return position;
        }
        
        public void setPosition(Object position) {
            this.position = position;
        }
        
        public Object getEvent() {
            return event;
        }
        
        public void setEvent(Object event) {
            this.event = event;
        }
        
        public Object getDevice() {
            return device;
        }
        
        public void setDevice(Object device) {
            this.device = device;
        }
        
        public Object getGeofence() {
            return geofence;
        }
        
        public void setGeofence(Object geofence) {
            this.geofence = geofence;
        }
        
        public long getMaintenanceId() {
            return maintenanceId;
        }
        
        public void setMaintenanceId(long maintenanceId) {
            this.maintenanceId = maintenanceId;
        }
        
        public java.util.Map<String, Object> getProperties() {
            return properties;
        }
        
        public void setProperties(java.util.Map<String, Object> properties) {
            this.properties = properties;
        }
        
        public EmailAttachment getAttachment() {
            return attachment;
        }
        
        public void setAttachment(EmailAttachment attachment) {
            this.attachment = attachment;
        }
    }
    
    /**
     * Email attachment data class for Kafka/RabbitMQ messages.
     */
    public static class EmailAttachment {
        private String fileName;
        private String contentType;
        private String data; // Base64 encoded data
        
        public String getFileName() {
            return fileName;
        }
        
        public void setFileName(String fileName) {
            this.fileName = fileName;
        }
        
        public String getContentType() {
            return contentType;
        }
        
        public void setContentType(String contentType) {
            this.contentType = contentType;
        }
        
        public String getData() {
            return data;
        }
        
        public void setData(String data) {
            this.data = data;
        }
    }
    
    /**
     * Email status message data class for Kafka/RabbitMQ messages.
     */
    public static class EmailStatusMessage {
        private String messageId;
        private long userId;
        private long notificationId;
        private String correlationId;
        private boolean success;
        private String errorMessage;
        private Date timestamp;
        
        public String getMessageId() {
            return messageId;
        }
        
        public void setMessageId(String messageId) {
            this.messageId = messageId;
        }
        
        public long getUserId() {
            return userId;
        }
        
        public void setUserId(long userId) {
            this.userId = userId;
        }
        
        public long getNotificationId() {
            return notificationId;
        }
        
        public void setNotificationId(long notificationId) {
            this.notificationId = notificationId;
        }
        
        public String getCorrelationId() {
            return correlationId;
        }
        
        public void setCorrelationId(String correlationId) {
            this.correlationId = correlationId;
        }
        
        public boolean isSuccess() {
            return success;
        }
        
        public void setSuccess(boolean success) {
            this.success = success;
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