/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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
import java.util.concurrent.CompletableFuture;

/**
 * Core service interface for the Notification Service microservice.
 * Provides methods for processing events into notifications, selecting appropriate channels,
 * and managing delivery across multiple notification channels.
 */
public interface NotificationService {

    /**
     * Process an event and deliver notifications to appropriate recipients.
     *
     * @param event The event to process
     * @param position The position associated with the event (may be null)
     * @param correlationId Unique identifier for distributed tracing
     * @return A CompletableFuture that completes when notification processing is finished
     */
    CompletableFuture<Void> processEvent(Event event, Position position, String correlationId);

    /**
     * Process a notification for a specific user.
     *
     * @param notification The notification to process
     * @param user The user to receive the notification
     * @param event The event that triggered the notification
     * @param position The position associated with the event (may be null)
     * @param correlationId Unique identifier for distributed tracing
     * @return A CompletableFuture that completes when notification delivery is finished
     */
    CompletableFuture<Void> processNotification(
            Notification notification, User user, Event event, Position position, String correlationId);

    /**
     * Get a list of available notification channels.
     *
     * @return List of available notification channel types
     */
    List<String> getNotificationChannels();

    /**
     * Check if a notification channel is available.
     *
     * @param channel The notification channel to check
     * @return true if the channel is available, false otherwise
     */
    boolean isNotificationChannelAvailable(String channel);

    /**
     * Get notification delivery statistics.
     *
     * @return A summary of notification delivery statistics
     */
    NotificationStatistics getStatistics();

    /**
     * Statistics for notification delivery monitoring.
     */
    class NotificationStatistics {
        private final long totalProcessed;
        private final long totalDelivered;
        private final long totalFailed;
        private final long averageProcessingTimeMs;

        public NotificationStatistics(
                long totalProcessed, long totalDelivered, long totalFailed, long averageProcessingTimeMs) {
            this.totalProcessed = totalProcessed;
            this.totalDelivered = totalDelivered;
            this.totalFailed = totalFailed;
            this.averageProcessingTimeMs = averageProcessingTimeMs;
        }

        public long getTotalProcessed() {
            return totalProcessed;
        }

        public long getTotalDelivered() {
            return totalDelivered;
        }

        public long getTotalFailed() {
            return totalFailed;
        }

        public long getAverageProcessingTimeMs() {
            return averageProcessingTimeMs;
        }
    }
}