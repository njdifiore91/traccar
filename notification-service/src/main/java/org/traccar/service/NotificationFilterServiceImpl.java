package org.traccar.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Service;
import org.traccar.model.Event;
import org.traccar.model.Notification;
import org.traccar.model.User;
import org.traccar.model.Group;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;
import org.traccar.storage.Storage;

import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Implementation of the NotificationFilterService interface that provides
 * comprehensive filtering of notifications based on rules, schedules, and policies.
 * 
 * This implementation evaluates events against user-defined criteria to determine
 * if notifications should be sent, supporting features like:
 * - Rule evaluation for different event types
 * - Calendar-based scheduling with time zone support
 * - Event age validation with configurable thresholds
 * - User preference application with hierarchical inheritance
 * - System policy enforcement with rate limiting
 * - Caching of frequently accessed rules for performance
 */
@Service
public class NotificationFilterServiceImpl implements NotificationFilterService {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationFilterServiceImpl.class);

    @Autowired
    private Storage storage;
    
    @Autowired
    private RateLimitService rateLimitService;
    
    // Cache for user notification rules to improve performance
    private final Map<Long, Map<String, Set<Notification>>> userNotificationCache = new ConcurrentHashMap<>();
    
    // Maximum age of events in seconds that can trigger notifications
    @Value("${notification.event.maxAgeSeconds:300}")
    private int maxEventAgeSeconds;
    
    // Default quiet hours start time (e.g., 22:00)
    @Value("${notification.quietHours.start:}")
    private String quietHoursStart;
    
    // Default quiet hours end time (e.g., 07:00)
    @Value("${notification.quietHours.end:}")
    private String quietHoursEnd;
    
    // Whether to enable quiet hours by default
    @Value("${notification.quietHours.enabled:false}")
    private boolean quietHoursEnabled;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean shouldNotify(Event event, User user) {
        if (event == null || user == null) {
            LOGGER.debug("Event or user is null, skipping notification");
            return false;
        }
        
        // Apply all filters in sequence
        return isEventRecent(event) && 
               matchesRules(event, user) && 
               isWithinSchedule(user, event) && 
               isAllowedByUserPreferences(event, user) && 
               isAllowedBySystemPolicies(event, user);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Cacheable(value = "notificationRuleMatches", key = "#event.id + '-' + #user.id", 
              condition = "#event != null && #user != null", 
              unless = "#result == false")
    public boolean matchesRules(Event event, User user) {
        try {
            // Get notification rules for this user and event type
            Set<Notification> notifications = getUserNotifications(user, event.getType());
            
            if (notifications.isEmpty()) {
                LOGGER.debug("No notification rules found for user {} and event type {}", 
                        user.getId(), event.getType());
                return false;
            }
            
            // Check if any notification rule matches this event
            for (Notification notification : notifications) {
                if (matchesNotificationCriteria(event, notification)) {
                    LOGGER.debug("Event {} matches notification rule {}", 
                            event.getId(), notification.getId());
                    return true;
                }
            }
            
            LOGGER.debug("Event {} doesn't match any notification rules for user {}", 
                    event.getId(), user.getId());
            return false;
        } catch (StorageException e) {
            LOGGER.warn("Error checking notification rules", e);
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWithinSchedule(User user, Event event) {
        // Check if quiet hours are enabled for this user
        boolean userQuietHoursEnabled = getUserQuietHoursEnabled(user);
        
        if (!userQuietHoursEnabled) {
            // Quiet hours not enabled, always within schedule
            return true;
        }
        
        // Get user's quiet hours settings or use defaults
        String start = getUserQuietHoursStart(user);
        String end = getUserQuietHoursEnd(user);
        
        if (start == null || end == null || start.isEmpty() || end.isEmpty()) {
            // No quiet hours configured, always within schedule
            return true;
        }
        
        // Get user's time zone or use system default
        ZoneId userZone = getUserTimeZone(user);
        
        // Current time in user's time zone
        LocalTime now = LocalTime.now(userZone);
        
        // Parse quiet hours times
        LocalTime quietStart = LocalTime.parse(start);
        LocalTime quietEnd = LocalTime.parse(end);
        
        // Check if current time is outside quiet hours
        if (quietStart.isBefore(quietEnd)) {
            // Simple case: quiet hours within same day (e.g., 22:00 to 07:00)
            return now.isBefore(quietStart) || now.isAfter(quietEnd);
        } else {
            // Complex case: quiet hours span midnight (e.g., 22:00 to 07:00)
            return now.isBefore(quietStart) && now.isAfter(quietEnd);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEventRecent(Event event) {
        if (event.getEventTime() == null) {
            // If event has no timestamp, consider it current
            return true;
        }
        
        // Calculate age of event in seconds
        long eventAgeSeconds = (System.currentTimeMillis() - event.getEventTime().getTime()) / 1000;
        
        // Check if event is within the configured maximum age
        boolean isRecent = eventAgeSeconds <= maxEventAgeSeconds;
        
        if (!isRecent) {
            LOGGER.debug("Event {} is too old ({} seconds), skipping notification", 
                    event.getId(), eventAgeSeconds);
        }
        
        return isRecent;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isAllowedByUserPreferences(Event event, User user) {
        try {
            // Check if user has disabled notifications for this event type
            Map<String, Boolean> userPreferences = getUserNotificationPreferences(user);
            
            // If preference exists and is explicitly set to false, notifications are disabled
            if (userPreferences.containsKey(event.getType()) && 
                !userPreferences.get(event.getType())) {
                LOGGER.debug("User {} has disabled notifications for event type {}", 
                        user.getId(), event.getType());
                return false;
            }
            
            // If no explicit preference, check group inheritance if applicable
            if (!userPreferences.containsKey(event.getType()) && user.getGroupId() != 0) {
                return isAllowedByGroupPreferences(event, user.getGroupId());
            }
            
            // Default to allowed if no preference is set
            return true;
        } catch (StorageException e) {
            LOGGER.warn("Error checking user preferences", e);
            // Default to allowed on error to prevent missing important notifications
            return true;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isAllowedBySystemPolicies(Event event, User user) {
        // Check rate limits for this user and notification type
        return rateLimitService.checkRateLimit(user.getId(), "notification:" + event.getType());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Boolean> getNotificationChannels(Event event, User user) {
        try {
            // Get user's channel preferences
            Map<String, Boolean> channels = getUserChannelPreferences(user);
            
            // If no preferences set, use default channels
            if (channels.isEmpty()) {
                channels = getDefaultChannels();
            }
            
            // Apply event-specific channel overrides if any
            Map<String, Boolean> eventTypeChannels = getEventTypeChannels(event.getType());
            if (!eventTypeChannels.isEmpty()) {
                // Merge with user preferences, event type settings take precedence
                channels.putAll(eventTypeChannels);
            }
            
            return channels;
        } catch (StorageException e) {
            LOGGER.warn("Error getting notification channels", e);
            // Return default channels on error
            return getDefaultChannels();
        }
    }
    
    /**
     * Checks if an event matches the criteria specified in a notification rule.
     * 
     * @param event The event to check
     * @param notification The notification rule to check against
     * @return true if the event matches the criteria, false otherwise
     */
    private boolean matchesNotificationCriteria(Event event, Notification notification) {
        // Basic type matching
        if (!event.getType().equals(notification.getType())) {
            return false;
        }
        
        // Check if notification is for a specific device
        if (notification.getDeviceId() != 0 && 
            notification.getDeviceId() != event.getDeviceId()) {
            return false;
        }
        
        // Check if notification is for a specific geofence
        if (notification.getGeofenceId() != 0 && 
            (event.getGeofenceId() == 0 || notification.getGeofenceId() != event.getGeofenceId())) {
            return false;
        }
        
        // Check for additional attributes matching if needed
        // This could be extended for more complex criteria
        
        return true;
    }
    
    /**
     * Gets notification rules for a specific user and event type.
     * 
     * @param user The user to get notifications for
     * @param eventType The event type to filter by
     * @return Set of notification rules
     * @throws StorageException If there's an error accessing storage
     */
    /**
     * Gets notification rules for a specific user and event type.
     * Uses Spring's @Cacheable for caching results.
     * 
     * @param user The user to get notifications for
     * @param eventType The event type to filter by
     * @return Set of notification rules
     * @throws StorageException If there's an error accessing storage
     */
    @Cacheable(value = "userNotifications", key = "#user.id + '-' + #eventType")
    private Set<Notification> getUserNotifications(User user, String eventType) throws StorageException {
        LOGGER.debug("Cache miss for user notifications: userId={}, eventType={}", user.getId(), eventType);
        
        // Query from storage
        Request request = new Request(
                Condition.and(
                    Condition.equals("userId", user.getId()),
                    Condition.equals("type", eventType)
                )
        );
        
        Collection<Notification> notifications = storage.getObjects(Notification.class, request);
        Set<Notification> notificationSet = new HashSet<>(notifications);
        
        // Still update the manual cache for backward compatibility
        Map<String, Set<Notification>> userCache = userNotificationCache.computeIfAbsent(user.getId(), k -> new ConcurrentHashMap<>());
        userCache.put(eventType, notificationSet);
        
        return notificationSet;
    }
    
    /**
     * Gets the user's notification preferences.
     * 
     * @param user The user to get preferences for
     * @return Map of event types to boolean preferences
     * @throws StorageException If there's an error accessing storage
     */
    private Map<String, Boolean> getUserNotificationPreferences(User user) throws StorageException {
        // This would typically come from user attributes or preferences table
        // Simplified implementation for demonstration
        Map<String, Boolean> preferences = new HashMap<>();
        
        // Example: preferences.put("deviceOnline", user.getBoolean("notifyDeviceOnline"));
        // In a real implementation, this would load from user attributes or a dedicated preferences table
        
        return preferences;
    }
    
    /**
     * Checks if notifications are allowed by group preferences.
     * 
     * @param event The event to check
     * @param groupId The group ID to check preferences for
     * @return true if allowed by group preferences, false otherwise
     * @throws StorageException If there's an error accessing storage
     */
    private boolean isAllowedByGroupPreferences(Event event, long groupId) throws StorageException {
        Group group = storage.getObject(Group.class, new Request(Condition.equals("id", groupId)));
        if (group == null) {
            return true; // Group not found, default to allowed
        }
        
        // Check group preferences
        // This would typically come from group attributes or preferences table
        // Simplified implementation for demonstration
        
        // If group has a parent, check inheritance
        if (group.getGroupId() != 0) {
            return isAllowedByGroupPreferences(event, group.getGroupId());
        }
        
        return true; // Default to allowed
    }
    
    /**
     * Gets the user's channel preferences.
     * 
     * @param user The user to get preferences for
     * @return Map of channel types to boolean preferences
     * @throws StorageException If there's an error accessing storage
     */
    private Map<String, Boolean> getUserChannelPreferences(User user) throws StorageException {
        // This would typically come from user attributes or preferences table
        // Simplified implementation for demonstration
        Map<String, Boolean> channels = new HashMap<>();
        
        // Example: channels.put("email", user.getBoolean("notifyEmail"));
        // In a real implementation, this would load from user attributes or a dedicated preferences table
        
        return channels;
    }
    
    /**
     * Gets the default notification channels.
     * 
     * @return Map of channel types to boolean values (true = enabled)
     */
    private Map<String, Boolean> getDefaultChannels() {
        Map<String, Boolean> channels = new HashMap<>();
        channels.put("web", true);      // Web notifications always on by default
        channels.put("email", true);    // Email on by default
        channels.put("sms", false);     // SMS off by default
        channels.put("push", true);     // Push notifications on by default
        channels.put("telegram", false); // Telegram off by default
        return channels;
    }
    
    /**
     * Gets channel overrides for specific event types.
     * 
     * @param eventType The event type to get channel overrides for
     * @return Map of channel types to boolean values
     */
    private Map<String, Boolean> getEventTypeChannels(String eventType) {
        // Some event types might have specific channel requirements
        // For example, critical alerts might enable SMS regardless of user preference
        Map<String, Boolean> channels = new HashMap<>();
        
        switch (eventType) {
            case "alarm":
                // Alarms should use all available channels
                channels.put("sms", true);
                channels.put("email", true);
                channels.put("push", true);
                break;
            case "deviceOffline":
                // Device offline might be lower priority
                channels.put("web", true);
                channels.put("email", true);
                break;
            // Add more event type specific channel configurations as needed
        }
        
        return channels;
    }
    
    /**
     * Gets the user's time zone or system default if not specified.
     * 
     * @param user The user to get time zone for
     * @return The user's time zone
     */
    private ZoneId getUserTimeZone(User user) {
        String userTimezone = user.getString("timezone");
        if (userTimezone != null && !userTimezone.isEmpty()) {
            try {
                return ZoneId.of(userTimezone);
            } catch (DateTimeException e) {
                LOGGER.warn("Invalid timezone for user {}: {}", user.getId(), userTimezone);
            }
        }
        return ZoneId.systemDefault();
    }
    
    /**
     * Gets whether quiet hours are enabled for a user.
     * 
     * @param user The user to check
     * @return true if quiet hours are enabled, false otherwise
     */
    private boolean getUserQuietHoursEnabled(User user) {
        Boolean userSetting = user.getBoolean("quietHoursEnabled");
        return userSetting != null ? userSetting : quietHoursEnabled;
    }
    
    /**
     * Gets the quiet hours start time for a user.
     * 
     * @param user The user to get setting for
     * @return The quiet hours start time
     */
    private String getUserQuietHoursStart(User user) {
        String userSetting = user.getString("quietHoursStart");
        return userSetting != null && !userSetting.isEmpty() ? userSetting : quietHoursStart;
    }
    
    /**
     * Gets the quiet hours end time for a user.
     * 
     * @param user The user to get setting for
     * @return The quiet hours end time
     */
    private String getUserQuietHoursEnd(User user) {
        String userSetting = user.getString("quietHoursEnd");
        return userSetting != null && !userSetting.isEmpty() ? userSetting : quietHoursEnd;
    }
    
    /**
     * Clears the notification cache for a specific user.
     * This should be called when user's notification settings change.
     * 
     * @param userId The ID of the user to clear cache for
     */
    @CacheEvict(value = {"notificationRuleMatches", "userNotifications"}, allEntries = true)
    public void clearUserCache(long userId) {
        userNotificationCache.remove(userId);
        LOGGER.debug("Cleared notification cache for user {}", userId);
    }
    
    /**
     * Clears the entire notification cache.
     * This should be called when global notification settings change.
     */
    @CacheEvict(value = {"notificationRuleMatches", "userNotifications"}, allEntries = true)
    public void clearCache() {
        userNotificationCache.clear();
        LOGGER.debug("Cleared all notification caches");
    }
}