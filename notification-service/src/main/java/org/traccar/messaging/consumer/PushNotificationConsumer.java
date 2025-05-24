/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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
import com.google.firebase.messaging.MessagingErrorCode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.traccar.model.Notification;
import org.traccar.model.ObjectOperation;
import org.traccar.model.User;
import org.traccar.push.FirebaseClient;
import org.traccar.storage.Storage;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Consumer for the 'push-out' topic that processes push notifications and delivers them
 * through Firebase Cloud Messaging or other push notification services.
 */
@Service
public class PushNotificationConsumer extends BaseMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushNotificationConsumer.class);

    @Inject
    private FirebaseClient firebaseClient;

    @Inject
    private Storage storage;

    @Inject
    private ObjectMapper objectMapper;

    private Counter pushNotificationSuccessCounter;
    private Counter pushNotificationFailureCounter;
    private Counter tokenCounter;

    @Value("${kafka.topic.cache-invalidation:cache-invalidation}")
    private String cacheInvalidationTopic;

    @Inject
    public void setMeterRegistry(MeterRegistry meterRegistry) {
        pushNotificationSuccessCounter = meterRegistry.counter("notification.push.success");
        pushNotificationFailureCounter = meterRegistry.counter("notification.push.failure");
        tokenCounter = meterRegistry.counter("notification.push.tokens");
    }

    /**
     * Processes push notification messages from the 'push-out' topic.
     *
     * @param payload The notification message payload
     * @param correlationId The correlation ID for distributed tracing
     */
    @KafkaListener(topics = "${kafka.topic.push-out:push-out}", groupId = "${kafka.consumer.group.push:push-consumer-group}")
    public void processPushNotification(
            @Payload String payload,
            @Header(KafkaHeaders.CORRELATION_ID) String correlationId) {

        LOGGER.debug("Received push notification: {}", payload);

        try {
            // Parse the notification message
            PushNotificationPayload notificationPayload = objectMapper.readValue(payload, PushNotificationPayload.class);
            
            // Convert to standard notification model for potential DLQ handling
            Notification notification = new Notification();
            notification.setId(Long.parseLong(notificationPayload.getId()));
            notification.setUserId(notificationPayload.getUserId());
            notification.setType("push");
            notification.set("title", notificationPayload.getTitle());
            notification.set("body", notificationPayload.getBody());
            if (notificationPayload.getEventId() > 0) {
                notification.setAttributes(Map.of("eventId", String.valueOf(notificationPayload.getEventId())));
            }

            // Process the notification
            processNotification(notificationPayload, notification, correlationId);

        } catch (Exception e) {
            LOGGER.error("Error processing push notification: {}", e.getMessage(), e);
            // Cannot publish to DLQ here as we don't have a valid notification object
            publishNotificationStatus(payload, "FAILED", "Error parsing notification: " + e.getMessage(), correlationId);
        }
    }

    /**
     * Processes a push notification by retrieving user tokens and sending to Firebase.
     *
     * @param notificationPayload The notification payload to process
     * @param notification The standard notification object for DLQ
     * @param correlationId The correlation ID for distributed tracing
     */
    private void processNotification(PushNotificationPayload notificationPayload, Notification notification, String correlationId) {
        try {
            // Get the user
            User user = storage.getObject(User.class, notificationPayload.getUserId());
            if (user == null) {
                LOGGER.warn("User not found for push notification: {}", notificationPayload.getUserId());
                publishNotificationStatus(notificationPayload.getId(), "FAILED", "User not found", correlationId);
                return;
            }

            // Check if user has notification tokens
            if (!user.hasAttribute("notificationTokens") || user.getString("notificationTokens").isEmpty()) {
                LOGGER.debug("User has no notification tokens: {}", user.getId());
                publishNotificationStatus(notificationPayload.getId(), "SKIPPED", "No notification tokens", correlationId);
                return;
            }

            // Get the notification tokens
            List<String> registrationTokens = new ArrayList<>(
                    Arrays.asList(user.getString("notificationTokens").split("[, ]")));

            // Record token metrics
            tokenCounter.increment(registrationTokens.size());

            // Send the notification to all tokens
            sendPushNotifications(notificationPayload, registrationTokens, user, notification, correlationId);

        } catch (Exception e) {
            LOGGER.error("Error processing push notification", e);
            publishToDeadLetterQueue(notification, null, correlationId, e);
            publishNotificationStatus(notificationPayload.getId(), "FAILED", e.getMessage(), correlationId);
        }
    }

    /**
     * Sends push notifications to all user tokens and handles token validation.
     *
     * @param notificationPayload The notification payload to send
     * @param registrationTokens The list of device tokens
     * @param user The user to send notifications to
     * @param notification The standard notification object for DLQ
     * @param correlationId The correlation ID for distributed tracing
     */
    private void sendPushNotifications(
            PushNotificationPayload notificationPayload,
            List<String> registrationTokens,
            User user,
            Notification notification,
            String correlationId) {

        // Prepare notification data
        Map<String, String> data = new HashMap<>();
        if (notificationPayload.getEventId() != 0) {
            data.put("eventId", String.valueOf(notificationPayload.getEventId()));
        }

        // Add custom data if available
        if (notificationPayload.getData() != null) {
            data.putAll(notificationPayload.getData());
        }

        List<CompletableFuture<String>> futures = new ArrayList<>();
        List<String> failedTokens = new LinkedList<>();

        // Send to each token
        for (String token : registrationTokens) {
            CompletableFuture<String> future = firebaseClient.sendMessage(
                    token,
                    data,
                    notificationPayload.getTitle(),
                    notificationPayload.getBody());

            futures.add(future);
        }

        // Wait for all notifications to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((result, throwable) -> {
                    // Process results
                    for (int i = 0; i < futures.size(); i++) {
                        try {
                            futures.get(i).get(); // This will throw if the future completed exceptionally
                            pushNotificationSuccessCounter.increment();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            LOGGER.warn("Push notification interrupted", e);
                        } catch (ExecutionException e) {
                            String token = registrationTokens.get(i);
                            LOGGER.warn("Failed to send push notification to token: {}", token, e);

                            // Check if token is invalid or unregistered
                            if (e.getCause() != null && e.getCause().getMessage() != null) {
                                String errorMessage = e.getCause().getMessage();
                                if (errorMessage.contains(MessagingErrorCode.INVALID_ARGUMENT.name()) ||
                                        errorMessage.contains(MessagingErrorCode.UNREGISTERED.name())) {
                                    failedTokens.add(token);
                                }
                            }

                            pushNotificationFailureCounter.increment();
                        }
                    }

                    // Update user tokens if needed
                    if (!failedTokens.isEmpty()) {
                        updateUserTokens(user, registrationTokens, failedTokens);
                    }

                    // Publish delivery status
                    if (failedTokens.size() == registrationTokens.size()) {
                        publishNotificationStatus(notificationPayload.getId(), "FAILED", "All tokens failed", correlationId);
                    } else if (!failedTokens.isEmpty()) {
                        publishNotificationStatus(notificationPayload.getId(), "PARTIAL", 
                                String.format("%d of %d tokens failed", failedTokens.size(), registrationTokens.size()), 
                                correlationId);
                    } else {
                        publishNotificationStatus(notificationPayload.getId(), "DELIVERED", null, correlationId);
                    }
                });
    }

    /**
     * Updates user tokens by removing invalid ones.
     *
     * @param user The user to update
     * @param registrationTokens The original list of tokens
     * @param failedTokens The list of invalid tokens to remove
     */
    private void updateUserTokens(User user, List<String> registrationTokens, List<String> failedTokens) {
        try {
            // Remove failed tokens
            registrationTokens.removeAll(failedTokens);

            // Update user attribute
            if (registrationTokens.isEmpty()) {
                user.removeAttribute("notificationTokens");
                LOGGER.info("Removed all notification tokens for user: {}", user.getId());
            } else {
                user.set("notificationTokens", String.join(",", registrationTokens));
                LOGGER.info("Updated notification tokens for user: {}, removed {} invalid tokens", 
                        user.getId(), failedTokens.size());
            }

            // Save to database
            storage.updateObject(user, new Request(
                    new Columns.Include("attributes"),
                    new Condition.Equals("id", user.getId())));

            // Publish user update event for cache invalidation
            publishUserUpdate(user.getId());

        } catch (Exception e) {
            LOGGER.error("Failed to update user tokens", e);
        }
    }

    /**
     * Publishes a user update event for cache invalidation.
     *
     * @param userId The ID of the user that was updated
     */
    private void publishUserUpdate(long userId) {
        try {
            Map<String, Object> event = new HashMap<>();
            event.put("type", "USER_UPDATE");
            event.put("userId", userId);
            event.put("operation", ObjectOperation.UPDATE.name());

            kafkaTemplate.send(cacheInvalidationTopic, objectMapper.writeValueAsString(event));
            LOGGER.debug("Published user update event for cache invalidation: {}", userId);
        } catch (Exception e) {
            LOGGER.error("Failed to publish user update event", e);
        }
    }

    /**
     * Publishes the delivery status of a notification.
     *
     * @param notificationId The ID of the notification
     * @param status The delivery status (DELIVERED, FAILED, PARTIAL, SKIPPED)
     * @param message Additional status message
     * @param correlationId The correlation ID for distributed tracing
     */
    private void publishNotificationStatus(String notificationId, String status, String message, String correlationId) {
        try {
            Map<String, Object> statusUpdate = new HashMap<>();
            statusUpdate.put("id", notificationId);
            statusUpdate.put("channel", "PUSH");
            statusUpdate.put("status", status);
            if (message != null) {
                statusUpdate.put("message", message);
            }
            statusUpdate.put("timestamp", System.currentTimeMillis());

            kafkaTemplate.send(notificationStatusTopic, correlationId, objectMapper.writeValueAsString(statusUpdate));
            LOGGER.debug("Published notification status: {} - {}", notificationId, status);
        } catch (Exception e) {
            LOGGER.error("Failed to publish notification status", e);
        }
    }

    /**
     * Data class for push notification payloads.
     */
    public static class PushNotificationPayload {
        private String id;
        private long userId;
        private String title;
        private String body;
        private long eventId;
        private Map<String, String> data;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public long getUserId() {
            return userId;
        }

        public void setUserId(long userId) {
            this.userId = userId;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getBody() {
            return body;
        }

        public void setBody(String body) {
            this.body = body;
        }

        public long getEventId() {
            return eventId;
        }

        public void setEventId(long eventId) {
            this.eventId = eventId;
        }

        public Map<String, String> getData() {
            return data;
        }

        public void setData(Map<String, String> data) {
            this.data = data;
        }
    }
}