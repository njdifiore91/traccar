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
package org.traccar.service;

import org.traccar.model.Event;
import org.traccar.model.Notification;
import org.traccar.model.Position;
import org.traccar.model.User;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Core service interface for the Notification Service microservice.
 * Provides methods for processing events into notifications, selecting appropriate channels,
 * and managing delivery across multiple notification channels.
 * <p>
 * This interface serves as the primary entry point for notification processing logic,
 * orchestrating the flow from event reception to multi-channel delivery.
 */
public interface NotificationService {

    /**
     * Process an event into notifications and deliver them to appropriate recipients.
     * This method applies notification rules, determines recipients, formats content,
     * selects channels, and initiates delivery.
     *
     * @param event The event to process
     * @param position The position associated with the event (may be null)
     * @param correlationId Unique identifier for distributed tracing
     * @return A future that completes when initial processing is done (not when delivery completes)
     */
    CompletableFuture<Void> processEvent(Event event, Position position, String correlationId);

    /**
     * Process a batch of events into notifications and deliver them to appropriate recipients.
     * This method is optimized for bulk processing, allowing for more efficient resource utilization
     * and potential batching of notifications to the same recipients.
     *
     * @param events List of events to process
     * @param positions Map of positions associated with events (keyed by event ID)
     * @param correlationId Unique identifier for distributed tracing
     * @return A future that completes when initial processing is done (not when delivery completes)
     */
    CompletableFuture<Void> processEvents(List<Event> events, Map<Long, Position> positions, String correlationId);

    /**
     * Send a notification directly through specified channels, bypassing the rule evaluation process.
     * This method is used for system-generated notifications or API-triggered notifications.
     *
     * @param notification The notification to send
     * @param user The user who will receive the notification
     * @param channels List of channel types to use (e.g., "mail", "sms", "web")
     * @param correlationId Unique identifier for distributed tracing
     * @return A future that completes when initial processing is done (not when delivery completes)
     */
    CompletableFuture<Void> sendNotification(Notification notification, User user, List<String> channels, String correlationId);

    /**
     * Send a notification to multiple users through specified channels.
     * This method is used for system-generated notifications or API-triggered notifications
     * that need to be sent to multiple recipients.
     *
     * @param notification The notification to send
     * @param users List of users who will receive the notification
     * @param channels List of channel types to use (e.g., "mail", "sms", "web")
     * @param correlationId Unique identifier for distributed tracing
     * @return A future that completes when initial processing is done (not when delivery completes)
     */
    CompletableFuture<Void> sendNotificationToUsers(Notification notification, List<User> users, List<String> channels, String correlationId);

    /**
     * Get the delivery status of a notification.
     * This method provides information about whether a notification was successfully delivered
     * through each attempted channel.
     *
     * @param notificationId The ID of the notification to check
     * @param correlationId Unique identifier for distributed tracing
     * @return A map of channel types to delivery status (true for delivered, false for failed)
     */
    Map<String, Boolean> getDeliveryStatus(long notificationId, String correlationId);

    /**
     * Get detailed delivery information for a notification, including timestamps and error messages.
     * This method provides comprehensive information about delivery attempts across all channels.
     *
     * @param notificationId The ID of the notification to check
     * @param correlationId Unique identifier for distributed tracing
     * @return A map of channel types to delivery details
     */
    Map<String, DeliveryDetails> getDeliveryDetails(long notificationId, String correlationId);

    /**
     * Retry delivery of a failed notification through specified channels.
     * This method allows manual retry of notifications that failed to deliver.
     *
     * @param notificationId The ID of the notification to retry
     * @param channels List of channel types to retry (e.g., "mail", "sms", "web")
     * @param correlationId Unique identifier for distributed tracing
     * @return A future that completes when retry processing is done
     */
    CompletableFuture<Void> retryNotification(long notificationId, List<String> channels, String correlationId);

    /**
     * Get the list of available notification channels.
     * This method returns all notification channels that are currently configured and available.
     *
     * @return List of available notification channel types
     */
    List<String> getAvailableChannels();

    /**
     * Check if a notification channel is currently available.
     * This method verifies if a specific notification channel is configured and operational.
     *
     * @param channelType The channel type to check (e.g., "mail", "sms", "web")
     * @return true if the channel is available, false otherwise
     */
    boolean isChannelAvailable(String channelType);

    /**
     * Class representing detailed delivery information for a notification.
     */
    class DeliveryDetails {
        private final boolean delivered;
        private final long timestamp;
        private final String errorMessage;
        private final int attemptCount;

        public DeliveryDetails(boolean delivered, long timestamp, String errorMessage, int attemptCount) {
            this.delivered = delivered;
            this.timestamp = timestamp;
            this.errorMessage = errorMessage;
            this.attemptCount = attemptCount;
        }

        public boolean isDelivered() {
            return delivered;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public int getAttemptCount() {
            return attemptCount;
        }
    }
}