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
import org.traccar.model.User;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Service interface for determining notification recipients based on event context,
 * permissions, and user preferences. This service is responsible for building recipient
 * lists, filtering by permissions, and applying user notification preferences.
 */
public interface RecipientService {

    /**
     * Represents a notification recipient with user information and channel preferences.
     */
    class Recipient {
        private final User user;
        private final Set<String> channels;

        public Recipient(User user, Set<String> channels) {
            this.user = user;
            this.channels = channels;
        }

        public User getUser() {
            return user;
        }

        public Set<String> getChannels() {
            return channels;
        }
    }

    /**
     * Builds a list of recipients for a given event and notification type.
     * This method determines who should receive notifications based on the event context
     * and notification configuration.
     *
     * @param event The event that triggered the notification
     * @param notificationType The type of notification to be sent
     * @return A future that resolves to a list of recipients
     */
    CompletableFuture<List<Recipient>> buildRecipientList(Event event, String notificationType);

    /**
     * Filters a list of potential recipients based on their permissions.
     * Only users with appropriate permissions to view the event and related entities
     * will be included in the result.
     *
     * @param event The event that triggered the notification
     * @param potentialRecipients List of potential recipients to filter
     * @return A future that resolves to a filtered list of recipients with permissions
     */
    CompletableFuture<List<Recipient>> filterByPermissions(Event event, List<Recipient> potentialRecipients);

    /**
     * Applies user notification preferences to the recipient list.
     * This method filters recipients based on their notification preferences,
     * including enabled notification types and notification schedules.
     *
     * @param event The event that triggered the notification
     * @param notificationType The type of notification to be sent
     * @param recipients List of recipients to filter by preferences
     * @return A future that resolves to a list of recipients filtered by preferences
     */
    CompletableFuture<List<Recipient>> applyUserPreferences(Event event, String notificationType, List<Recipient> recipients);

    /**
     * Gets user notification settings for a specific notification type.
     * This method retrieves the notification settings for each user, including
     * which channels they have enabled for the given notification type.
     *
     * @param notificationType The type of notification
     * @param userIds Set of user IDs to retrieve settings for
     * @return A future that resolves to a map of user IDs to their notification settings
     */
    CompletableFuture<Map<Long, Notification>> getUserNotificationSettings(String notificationType, Set<Long> userIds);

    /**
     * Determines the notification channels for each recipient based on their preferences.
     * This method enhances the recipient list with channel information (email, SMS, push, etc.)
     * based on user preferences and notification type.
     *
     * @param recipients List of recipients to enhance with channel information
     * @param notificationType The type of notification to be sent
     * @return A future that resolves to a list of recipients with channel preferences
     */
    CompletableFuture<List<Recipient>> determineChannels(List<Recipient> recipients, String notificationType);

    /**
     * Deduplicates and validates the recipient list.
     * This method ensures that each user appears only once in the recipient list
     * and that all recipients are valid (e.g., have valid email addresses for email notifications).
     *
     * @param recipients List of recipients to deduplicate and validate
     * @return A future that resolves to a deduplicated and validated list of recipients
     */
    CompletableFuture<List<Recipient>> deduplicateAndValidate(List<Recipient> recipients);

    /**
     * Gets the list of users who should be notified about a specific event.
     * This is a convenience method that combines the other methods to provide a complete
     * workflow for determining notification recipients.
     *
     * @param event The event that triggered the notification
     * @param notificationType The type of notification to be sent
     * @return A future that resolves to a list of recipients for the notification
     */
    default CompletableFuture<List<Recipient>> getNotificationRecipients(Event event, String notificationType) {
        return buildRecipientList(event, notificationType)
                .thenCompose(recipients -> filterByPermissions(event, recipients))
                .thenCompose(recipients -> applyUserPreferences(event, notificationType, recipients))
                .thenCompose(recipients -> determineChannels(recipients, notificationType))
                .thenCompose(this::deduplicateAndValidate);
    }
}