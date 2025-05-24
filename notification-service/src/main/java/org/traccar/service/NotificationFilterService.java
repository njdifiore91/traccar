package org.traccar.service;

import org.traccar.model.Event;
import org.traccar.model.User;

import java.util.Map;

/**
 * Service interface for filtering notifications based on user-defined rules,
 * calendar schedules, and system policies.
 * 
 * This interface provides methods for evaluating whether an event should trigger
 * a notification based on various criteria including rule matching, calendar
 * scheduling, event age validation, user preferences, and system policies.
 */
public interface NotificationFilterService {

    /**
     * Evaluates whether a notification should be sent for the given event based on
     * user-defined rules and system policies.
     *
     * @param event The event to evaluate
     * @param user The user to evaluate rules for
     * @return true if notification should be sent, false otherwise
     */
    boolean shouldNotify(Event event, User user);
    
    /**
     * Checks if the event matches any notification rules for the given user.
     *
     * @param event The event to check against rules
     * @param user The user whose rules should be checked
     * @return true if the event matches any rules, false otherwise
     */
    boolean matchesRules(Event event, User user);
    
    /**
     * Checks if the current time is within the allowed notification schedule for the user.
     *
     * @param user The user to check schedule for
     * @param event The event to check schedule for (some schedules may be event-type specific)
     * @return true if current time is within allowed schedule, false otherwise
     */
    boolean isWithinSchedule(User user, Event event);
    
    /**
     * Validates if the event is recent enough to trigger a notification.
     * Events that are too old may be filtered out to prevent delayed notifications.
     *
     * @param event The event to validate
     * @return true if the event is recent enough, false if it's too old
     */
    boolean isEventRecent(Event event);
    
    /**
     * Applies user preferences to determine if notification should be sent.
     * This includes checking notification type preferences and inheritance from groups.
     *
     * @param event The event to check
     * @param user The user whose preferences should be applied
     * @return true if notification is allowed by user preferences, false otherwise
     */
    boolean isAllowedByUserPreferences(Event event, User user);
    
    /**
     * Checks if system policies allow sending the notification.
     * This includes rate limiting and other system-wide constraints.
     *
     * @param event The event to check
     * @param user The user to check policies for
     * @return true if notification is allowed by system policies, false otherwise
     */
    boolean isAllowedBySystemPolicies(Event event, User user);
    
    /**
     * Gets the notification channels that should be used for this event and user.
     * The result is a map of channel types to boolean values indicating whether
     * that channel should be used.
     *
     * @param event The event to determine channels for
     * @param user The user to determine channels for
     * @return Map of channel types to boolean values (true = use this channel)
     */
    Map<String, Boolean> getNotificationChannels(Event event, User user);
}