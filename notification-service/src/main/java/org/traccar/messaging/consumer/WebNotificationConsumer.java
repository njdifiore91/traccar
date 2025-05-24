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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.traccar.model.Notification;
import org.traccar.model.User;
import org.traccar.notification.NotificationFormatter;
import org.traccar.service.WebSocketService;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumer for the 'web-out' topic that processes web notifications and delivers them
 * through WebSocket connections to the Traccar web interface.
 */
@Singleton
public class WebNotificationConsumer extends BaseMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebNotificationConsumer.class);

    private final WebSocketService webSocketService;
    private final NotificationFormatter notificationFormatter;
    private final ObjectMapper objectMapper;
    
    // Track delivery status for reporting
    private final Map<String, DeliveryStatus> deliveryStatusMap = new ConcurrentHashMap<>();

    @Inject
    public WebNotificationConsumer(
            WebSocketService webSocketService,
            NotificationFormatter notificationFormatter,
            ObjectMapper objectMapper) {
        this.webSocketService = webSocketService;
        this.notificationFormatter = notificationFormatter;
        this.objectMapper = objectMapper;
    }

    /**
     * Consumes messages from the 'web-out' topic and delivers them to WebSocket clients.
     * 
     * @param notification The notification to be delivered
     * @param user The target user for the notification
     * @param correlationId The correlation ID for tracking the notification through the system
     */
    @KafkaListener(topics = "${kafka.topic.web-out}", groupId = "${kafka.group.notification-service}")
    @CircuitBreaker(name = "webSocketDelivery", fallbackMethod = "fallbackProcessWebNotification")
    public void processWebNotification(Notification notification, User user, String correlationId) {
        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
            LOGGER.debug("Generated new correlation ID: {}", correlationId);
        }
        
        LOGGER.debug("Processing web notification with correlation ID: {}", correlationId);
        
        try {
            // Create delivery status entry
            DeliveryStatus status = new DeliveryStatus(correlationId, notification.getId(), user.getId());
            deliveryStatusMap.put(correlationId, status);
            
            // Validate user session
            if (!webSocketService.hasActiveSession(user.getId())) {
                LOGGER.debug("No active WebSocket session for user {}", user.getId());
                status.setStatus(DeliveryStatus.Status.NO_SESSION);
                publishDeliveryStatus(status);
                return;
            }
            
            // Format notification for web interface
            String formattedNotification = formatNotification(notification, user);
            
            // Deliver notification through WebSocket
            boolean delivered = webSocketService.sendNotification(user.getId(), formattedNotification);
            
            // Update and publish delivery status
            if (delivered) {
                status.setStatus(DeliveryStatus.Status.DELIVERED);
                LOGGER.debug("Web notification delivered to user {}", user.getId());
            } else {
                status.setStatus(DeliveryStatus.Status.FAILED);
                LOGGER.warn("Failed to deliver web notification to user {}", user.getId());
            }
            
            publishDeliveryStatus(status);
            
        } catch (Exception e) {
            LOGGER.error("Error processing web notification: {}", e.getMessage(), e);
            DeliveryStatus status = deliveryStatusMap.getOrDefault(
                    correlationId, new DeliveryStatus(correlationId, notification.getId(), user.getId()));
            status.setStatus(DeliveryStatus.Status.ERROR);
            status.setErrorMessage(e.getMessage());
            publishDeliveryStatus(status);
            throw e; // Let the circuit breaker handle it
        }
    }

    /**
     * Fallback method for the circuit breaker when the primary method fails.
     * 
     * @param notification The notification that failed to be delivered
     * @param user The target user for the notification
     * @param correlationId The correlation ID for tracking
     * @param e The exception that caused the failure
     */
    public void fallbackProcessWebNotification(
            Notification notification, User user, String correlationId, Exception e) {
        LOGGER.warn("Circuit breaker triggered for web notification delivery: {}", e.getMessage());
        
        DeliveryStatus status = deliveryStatusMap.getOrDefault(
                correlationId, new DeliveryStatus(correlationId, notification.getId(), user.getId()));
        status.setStatus(DeliveryStatus.Status.CIRCUIT_OPEN);
        status.setErrorMessage("Circuit breaker open: " + e.getMessage());
        
        publishDeliveryStatus(status);
        
        // Queue for retry through dead letter queue
        publishToDeadLetterQueue(notification, user, correlationId, e);
    }

    /**
     * Formats the notification for the web interface.
     * 
     * @param notification The notification to format
     * @param user The target user
     * @return Formatted notification as JSON string
     */
    private String formatNotification(Notification notification, User user) {
        try {
            Map<String, Object> formattedData = notificationFormatter.formatForWeb(notification, user);
            return objectMapper.writeValueAsString(formattedData);
        } catch (Exception e) {
            LOGGER.error("Error formatting notification: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to format notification", e);
        }
    }

    /**
     * Publishes delivery status to the status topic for tracking and reporting.
     * 
     * @param status The delivery status to publish
     */
    private void publishDeliveryStatus(DeliveryStatus status) {
        try {
            // Use the kafkaTemplate from BaseMessageConsumer
            kafkaTemplate.send(notificationStatusTopic, status.getCorrelationId(), status);
            LOGGER.debug("Published delivery status: {}", status);
            
            // Record metric for delivery status
            meterRegistry.counter("notification.web.status", 
                    "status", status.getStatus().name()).increment();
            
        } catch (Exception e) {
            LOGGER.error("Failed to publish delivery status: {}", e.getMessage(), e);
        }
    }

    /**
     * Publishes delivery status to the status topic for tracking and reporting.
     * 
     * @param status The delivery status to publish
     */
    private void publishDeliveryStatus(DeliveryStatus status) {
        try {
            // Use the kafkaTemplate from BaseMessageConsumer
            kafkaTemplate.send(notificationStatusTopic, status.getCorrelationId(), status);
            LOGGER.debug("Published delivery status: {}", status);
            
            // Record metric for delivery status
            meterRegistry.counter("notification.web.status", 
                    "status", status.getStatus().name()).increment();
            
        } catch (Exception e) {
            LOGGER.error("Failed to publish delivery status: {}", e.getMessage(), e);
        }
    }

    /**
     * Inner class to track delivery status of notifications.
     */
    public static class DeliveryStatus {
        
        public enum Status {
            PENDING,
            DELIVERED,
            FAILED,
            ERROR,
            NO_SESSION,
            CIRCUIT_OPEN
        }
        
        private final String correlationId;
        private final long notificationId;
        private final long userId;
        private Status status;
        private String errorMessage;
        private final long timestamp;
        
        public DeliveryStatus(String correlationId, long notificationId, long userId) {
            this.correlationId = correlationId;
            this.notificationId = notificationId;
            this.userId = userId;
            this.status = Status.PENDING;
            this.timestamp = System.currentTimeMillis();
        }

        public String getCorrelationId() {
            return correlationId;
        }

        public long getNotificationId() {
            return notificationId;
        }

        public long getUserId() {
            return userId;
        }

        public Status getStatus() {
            return status;
        }

        public void setStatus(Status status) {
            this.status = status;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public long getTimestamp() {
            return timestamp;
        }
        
        @Override
        public String toString() {
            return "DeliveryStatus{" +
                    "correlationId='" + correlationId + '\'' +
                    ", notificationId=" + notificationId +
                    ", userId=" + userId +
                    ", status=" + status +
                    ", errorMessage='" + errorMessage + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}