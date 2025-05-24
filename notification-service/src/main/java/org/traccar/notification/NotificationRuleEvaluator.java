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
package org.traccar.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.traccar.model.Calendar;
import org.traccar.model.Event;
import org.traccar.model.Notification;
import org.traccar.model.User;
import org.traccar.repository.NotificationRepository;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Evaluates notification rules against incoming events to determine if a notification should be triggered.
 * Applies user-defined rules, calendar-based scheduling constraints, and permission checks to filter events.
 * 
 * This component is part of the notification selection process in the Notification Service microservice.
 * It implements the following key features:
 * - Rule evaluation logic for different event types
 * - Calendar-based scheduling checks for time-based notification rules
 * - Permission-based filtering for notification recipients
 * - User preference application for notification customization
 * - Caching of frequently accessed rules for performance optimization
 */
@Service
public class NotificationRuleEvaluator {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationRuleEvaluator.class);

    private static final Duration EVENT_AGE_THRESHOLD = Duration.ofMinutes(5);
    
    // Maximum number of cached notification rules
    private static final int MAX_CACHE_SIZE = 1000;
    
    // Cache for frequently accessed rules
    private final Map<Long, Notification> notificationCache = new ConcurrentHashMap<>();
    
    private final CalendarService calendarService;
    private final PermissionService permissionService;
    private final UserPreferenceService userPreferenceService;
    private final NotificationRepository notificationRepository;
    
    /**
     * Creates a new instance of the NotificationRuleEvaluator.
     *
     * @param calendarService Service for retrieving and evaluating calendars
     * @param permissionService Service for checking user permissions
     * @param userPreferenceService Service for retrieving user preferences
     */
    @Autowired
    public NotificationRuleEvaluator(
            CalendarService calendarService,
            PermissionService permissionService,
            UserPreferenceService userPreferenceService,
            NotificationRepository notificationRepository) {
        this.calendarService = calendarService;
        this.permissionService = permissionService;
        this.userPreferenceService = userPreferenceService;
        this.notificationRepository = notificationRepository;
    }

    /**
     * Evaluates if a notification should be triggered for the given event and notification rule.
     *
     * @param event The event to evaluate
     * @param notification The notification rule to apply
     * @return true if the notification should be triggered, false otherwise
     */
    public boolean evaluate(Event event, Notification notification) {
        // Skip if event is too old
        if (isEventTooOld(event)) {
            LOGGER.debug("Event {} is too old, skipping notification", event.getId());
            return false;
        }
        
        // Check if notification is always triggered regardless of conditions
        if (notification.getAlways()) {
            LOGGER.debug("Notification {} is set to 'always', triggering for event {}", 
                    notification.getId(), event.getId());
            return true;
        }
        
        // Check if notification type matches event type
        if (!matchesEventType(event, notification)) {
            LOGGER.debug("Event type {} doesn't match notification type {}, skipping", 
                    event.getType(), notification.getType());
            return false;
        }
        
        // Check calendar-based scheduling constraints
        if (!isWithinCalendarSchedule(notification)) {
            LOGGER.debug("Event {} outside calendar schedule for notification {}", 
                    event.getId(), notification.getId());
            return false;
        }
        
        // Check notification conditions
        if (!evaluateConditions(event, notification)) {
            LOGGER.debug("Event {} doesn't meet conditions for notification {}", 
                    event.getId(), notification.getId());
            return false;
        }
        
        LOGGER.debug("Event {} triggers notification {}", event.getId(), notification.getId());
        return true;
    }
    
    /**
     * Determines if an event is too old to trigger notifications.
     *
     * @param event The event to check
     * @return true if the event is too old, false otherwise
     */
    private boolean isEventTooOld(Event event) {
        Date eventTime = event.getEventTime();
        if (eventTime == null) {
            return false; // Can't determine age, assume it's not too old
        }
        
        Instant eventInstant = eventTime.toInstant();
        Instant now = Instant.now();
        
        return Duration.between(eventInstant, now).compareTo(EVENT_AGE_THRESHOLD) > 0;
    }
    
    /**
     * Checks if the notification type matches the event type.
     *
     * @param event The event to check
     * @param notification The notification rule to apply
     * @return true if the types match, false otherwise
     */
    private boolean matchesEventType(Event event, Notification notification) {
        String notificationType = notification.getType();
        String eventType = event.getType();
        
        if (notificationType == null || eventType == null) {
            return false;
        }
        
        // Handle special case for command result events
        if (eventType.equals(Event.TYPE_COMMAND_RESULT)) {
            String commandType = event.getString("commandType");
            if (commandType != null) {
                return notificationType.equals(commandType);
            }
            return false;
        }
        
        return notificationType.equals(eventType);
    }
    
    /**
     * Checks if the current time is within the calendar schedule associated with the notification.
     *
     * @param notification The notification rule to check
     * @return true if within schedule or no calendar is associated, false otherwise
     */
    private boolean isWithinCalendarSchedule(Notification notification) {
        Long calendarId = notification.getCalendarId();
        if (calendarId == null) {
            return true; // No calendar restrictions
        }
        
        Calendar calendar = calendarService.getCalendarById(calendarId);
        if (calendar == null) {
            LOGGER.warn("Calendar {} not found for notification {}", calendarId, notification.getId());
            return true; // Calendar not found, assume no restrictions
        }
        
        return calendar.checkMoment(new Date());
    }
    
    /**
     * Evaluates the conditions defined in the notification rule against the event.
     *
     * @param event The event to evaluate
     * @param notification The notification rule containing conditions
     * @return true if all conditions are met, false otherwise
     */
    private boolean evaluateConditions(Event event, Notification notification) {
        // Get conditions from notification attributes
        String conditions = notification.getString("conditions");
        if (conditions == null || conditions.isEmpty()) {
            return true; // No conditions specified
        }
        
        try {
            // Parse and evaluate conditions
            return new ConditionEvaluator().evaluate(conditions, event);
        } catch (Exception e) {
            LOGGER.warn("Error evaluating conditions for notification {}: {}", 
                    notification.getId(), e.getMessage());
            return false;
        }
    }
    
    /**
     * Filters users who should receive the notification based on permissions and preferences.
     *
     * @param event The event that triggered the notification
     * @param notification The notification rule that was triggered
     * @param users The set of potential notification recipients
     * @return A filtered set of users who should receive the notification
     */
    public Set<User> filterRecipients(Event event, Notification notification, Set<User> users) {
        Set<User> recipients = ConcurrentHashMap.newKeySet();
        
        for (User user : users) {
            // Skip disabled users
            if (user.getDisabled()) {
                continue;
            }
            
            // Check if user has expired
            if (user.getExpirationTime() != null && user.getExpirationTime().before(new Date())) {
                LOGGER.debug("User {} account expired, skipping notification", user.getId());
                continue;
            }
            
            // Check if user has permission for the device
            if (event.getDeviceId() != 0 && !permissionService.hasDevicePermission(user, event.getDeviceId())) {
                LOGGER.debug("User {} lacks permission for device {}, skipping notification", 
                        user.getId(), event.getDeviceId());
                continue;
            }
            
            // Check if user has permission for the geofence (if applicable)
            if (event.getGeofenceId() != 0 && !permissionService.hasGeofencePermission(user, event.getGeofenceId())) {
                LOGGER.debug("User {} lacks permission for geofence {}, skipping notification", 
                        user.getId(), event.getGeofenceId());
                continue;
            }
            
            // Check user notification preferences
            if (!userPreferenceService.hasNotificationEnabled(user, notification.getType())) {
                LOGGER.debug("User {} has disabled notifications for type {}, skipping", 
                        user.getId(), notification.getType());
                continue;
            }
            
            // Check user notification channels
            if (!userPreferenceService.hasEnabledNotificationChannels(user)) {
                LOGGER.debug("User {} has no enabled notification channels, skipping", user.getId());
                continue;
            }
            
            recipients.add(user);
        }
        
        return recipients;
    }
    
    /**
     * Retrieves a notification rule from cache or loads it if not present.
     *
     * @param notificationId The ID of the notification rule to retrieve
     * @return The notification rule or null if not found
     */
    public Notification getNotification(long notificationId) {
        return notificationCache.computeIfAbsent(notificationId, this::loadNotification);
    }
    
    /**
     * Loads a notification rule from the database.
     *
     * @param notificationId The ID of the notification rule to load
     * @return The notification rule or null if not found
     */
    private Notification loadNotification(long notificationId) {
        try {
            LOGGER.debug("Loading notification {} from database", notificationId);
            Notification notification = notificationRepository.findById(notificationId).orElse(null);
            pruneCache(); // Check if cache needs pruning after adding a new entry
            return notification;
        } catch (Exception e) {
            LOGGER.warn("Error loading notification {}: {}", notificationId, e.getMessage());
            return null;
        }
    }
    
    /**
     * Invalidates the cache entry for a notification rule.
     *
     * @param notificationId The ID of the notification rule to invalidate
     */
    public void invalidateCache(long notificationId) {
        notificationCache.remove(notificationId);
        LOGGER.debug("Invalidated cache for notification {}", notificationId);
    }
    
    /**
     * Clears the entire notification rule cache.
     */
    public void clearCache() {
        int size = notificationCache.size();
        notificationCache.clear();
        LOGGER.debug("Cleared notification cache ({} entries)", size);
    }
    
    /**
     * Prunes the cache if it exceeds the maximum size.
     * This is a simple implementation that just clears the entire cache when it gets too large.
     * A more sophisticated implementation could use LRU or other eviction strategies.
     */
    private void pruneCache() {
        if (notificationCache.size() > MAX_CACHE_SIZE) {
            LOGGER.debug("Cache size {} exceeds maximum {}, clearing cache", 
                    notificationCache.size(), MAX_CACHE_SIZE);
            clearCache();
        }
    }
    
    /**
     * Inner class for evaluating notification conditions.
     */
    private static class ConditionEvaluator {
        
        /**
         * Evaluates a condition expression against an event.
         *
         * @param conditionExpression The condition expression to evaluate
         * @param event The event to evaluate against
         * @return true if the condition is met, false otherwise
         */
        public boolean evaluate(String conditionExpression, Event event) {
            // Simple implementation for now - would be expanded with a proper expression parser
            // This is a placeholder for the actual condition evaluation logic
            
            // Example implementation for basic conditions
            if (conditionExpression.contains("&&")) {
                String[] conditions = conditionExpression.split("&&");
                for (String condition : conditions) {
                    if (!evaluateSimpleCondition(condition.trim(), event)) {
                        return false;
                    }
                }
                return true;
            } else if (conditionExpression.contains("||")) {
                String[] conditions = conditionExpression.split("||");
                for (String condition : conditions) {
                    if (evaluateSimpleCondition(condition.trim(), event)) {
                        return true;
                    }
                }
                return false;
            } else {
                return evaluateSimpleCondition(conditionExpression, event);
            }
        }
        
        /**
         * Evaluates a simple condition against an event.
         *
         * @param condition The simple condition to evaluate
         * @param event The event to evaluate against
         * @return true if the condition is met, false otherwise
         */
        private boolean evaluateSimpleCondition(String condition, Event event) {
            // Parse the condition into attribute, operator, and value
            String[] parts = parseCondition(condition);
            if (parts == null || parts.length != 3) {
                return false;
            }
            
            String attribute = parts[0];
            String operator = parts[1];
            String value = parts[2];
            
            // Handle special attributes
            if (attribute.equals("geofenceId") && event.getGeofenceId() != 0) {
                return compareValues(String.valueOf(event.getGeofenceId()), operator, value);
            } else if (attribute.equals("maintenanceId") && event.getMaintenanceId() != 0) {
                return compareValues(String.valueOf(event.getMaintenanceId()), operator, value);
            } else if (attribute.startsWith("attribute:")) {
                String attributeName = attribute.substring("attribute:".length());
                Object attributeValue = event.getAttributes().get(attributeName);
                if (attributeValue != null) {
                    return compareValues(attributeValue.toString(), operator, value);
                }
                return false;
            }
            
            return false;
        }
        
        /**
         * Parses a condition string into attribute, operator, and value parts.
         *
         * @param condition The condition string to parse
         * @return Array containing [attribute, operator, value] or null if parsing fails
         */
        private String[] parseCondition(String condition) {
            // Look for common operators
            for (String op : new String[] {"==", "!=", ">=", "<=", ">", "<"}) {
                if (condition.contains(op)) {
                    String[] parts = condition.split(op, 2);
                    if (parts.length == 2) {
                        return new String[] {parts[0].trim(), op, parts[1].trim()};
                    }
                }
            }
            return null;
        }
        
        /**
         * Compares two values using the specified operator.
         *
         * @param actual The actual value from the event
         * @param operator The comparison operator
         * @param expected The expected value from the condition
         * @return true if the comparison succeeds, false otherwise
         */
        private boolean compareValues(String actual, String operator, String expected) {
            // Handle boolean values
            if (expected.equalsIgnoreCase("true") || expected.equalsIgnoreCase("false")) {
                boolean actualBool = Boolean.parseBoolean(actual);
                boolean expectedBool = Boolean.parseBoolean(expected);
                
                if (operator.equals("==")) {
                    return actualBool == expectedBool;
                } else if (operator.equals("!=")) {
                    return actualBool != expectedBool;
                }
                return false;
            }
            
            // Try numeric comparison
            try {
                double actualNum = Double.parseDouble(actual);
                double expectedNum = Double.parseDouble(expected);
                
                switch (operator) {
                    case "==": return actualNum == expectedNum;
                    case "!=": return actualNum != expectedNum;
                    case ">": return actualNum > expectedNum;
                    case ">=": return actualNum >= expectedNum;
                    case "<": return actualNum < expectedNum;
                    case "<=": return actualNum <= expectedNum;
                    default: return false;
                }
            } catch (NumberFormatException e) {
                // Not numeric, do string comparison
                switch (operator) {
                    case "==": return actual.equals(expected);
                    case "!=": return !actual.equals(expected);
                    default: return false;
                }
            }
        }
    }
}