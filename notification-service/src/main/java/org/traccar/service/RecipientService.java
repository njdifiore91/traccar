/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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

/**
 * Service interface for determining notification recipients based on event context,
 * permissions, and user preferences.
 */
public interface RecipientService {

    /**
     * Determines the list of recipients for a given event and notification configuration.
     * This method applies permission checks and user preferences to filter the recipients.
     *
     * @param event The event that triggered the notification
     * @param notification The notification configuration
     * @return A list of users who should receive the notification
     */
    List<User> getRecipients(Event event, Notification notification);

    /**
     * Determines the list of recipients with their channel-specific addressing information.
     * This method applies permission checks, user preferences, and includes addressing details
     * for each notification channel (email, phone, etc.).
     *
     * @param event The event that triggered the notification
     * @param notification The notification configuration
     * @return A map of users to their channel-specific addressing information
     */
    Map<User, Map<String, String>> getRecipientsWithAddressing(Event event, Notification notification);

    /**
     * Filters a list of users based on their permissions for the given event.
     * This method applies hierarchical permission checks (user, group, device).
     *
     * @param users The list of users to filter
     * @param event The event to check permissions against
     * @return A filtered list of users who have permission to receive notifications for the event
     */
    List<User> filterByPermission(List<User> users, Event event);

    /**
     * Applies user notification preferences to filter recipients.
     * This method checks if users have enabled notifications for the given event type and channel.
     *
     * @param users The list of users to filter
     * @param notification The notification configuration
     * @return A filtered list of users based on their notification preferences
     */
    List<User> applyUserPreferences(List<User> users, Notification notification);

    /**
     * Invalidates the permission check cache for a specific user.
     * This method should be called when user permissions change.
     *
     * @param userId The ID of the user whose cache should be invalidated
     */
    void invalidatePermissionCache(long userId);

    /**
     * Invalidates the entire permission check cache.
     * This method should be called when there are significant changes to the permission system.
     */
    void invalidatePermissionCache();
}