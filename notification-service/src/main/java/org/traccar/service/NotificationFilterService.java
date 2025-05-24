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
package org.traccar.service;

import org.traccar.model.Event;
import org.traccar.model.User;

import java.util.Date;

/**
 * Service interface for filtering notifications based on user-defined rules,
 * calendar schedules, and system policies. This interface provides methods for
 * evaluating whether an event should trigger a notification based on various criteria.
 */
public interface NotificationFilterService {

    /**
     * Checks if an event is recent enough to process for notifications.
     * Events that are too old may be skipped to prevent delayed notifications.
     *
     * @param event The event to check
     * @param currentTime Current system time for comparison
     * @return true if the event is recent enough to process, false otherwise
     */
    boolean validateEventAge(Event event, Date currentTime);

    /**
     * Checks if the event matches any user-defined notification rules.
     * Rules can be based on event type, device, geofence, or other attributes.
     *
     * @param event The event to check against rules
     * @param userId ID of the user whose rules should be checked
     * @return true if the event matches at least one notification rule, false otherwise
     */
    boolean matchesNotificationRules(Event event, long userId);

    /**
     * Verifies if the notification should be sent based on calendar scheduling constraints.
     * This allows notifications to be restricted to specific time periods.
     *
     * @param event The event to check
     * @param userId ID of the user whose calendar settings should be checked
     * @param currentTime Current system time for calendar comparison
     * @return true if the notification is within scheduled time periods, false otherwise
     */
    boolean isWithinCalendarSchedule(Event event, long userId, Date currentTime);

    /**
     * Applies user preferences to determine if a notification should be sent.
     * This includes user-specific settings like notification channels, quiet hours, etc.
     *
     * @param event The event to check
     * @param user The user object containing preference settings
     * @param notificationType The type of notification to be sent
     * @return true if the notification should be sent according to user preferences, false otherwise
     */
    boolean applyUserPreferences(Event event, User user, String notificationType);

    /**
     * Enforces system-wide notification policies such as rate limits and global quiet hours.
     * This prevents notification flooding and respects system-wide settings.
     *
     * @param event The event to check
     * @param userId ID of the user who would receive the notification
     * @param notificationType The type of notification to be sent
     * @return true if the notification complies with system policies, false otherwise
     */
    boolean enforceSystemPolicies(Event event, long userId, String notificationType);

    /**
     * Comprehensive check that combines all filtering criteria to determine if a notification
     * should be sent for an event to a specific user.
     *
     * @param event The event that might trigger a notification
     * @param user The user who would receive the notification
     * @param notificationType The type of notification to be sent
     * @param currentTime Current system time for temporal checks
     * @return true if the notification should be sent, false if it should be filtered out
     */
    boolean shouldSendNotification(Event event, User user, String notificationType, Date currentTime);
}