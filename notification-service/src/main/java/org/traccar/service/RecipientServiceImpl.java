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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.client.UserClient;
import org.traccar.client.DeviceClient;
import org.traccar.client.GroupClient;
import org.traccar.client.PermissionClient;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Group;
import org.traccar.model.Notification;
import org.traccar.model.User;
import org.traccar.model.UserPreference;
import org.traccar.notificators.Notificator;
import org.traccar.notificators.NotificatorManager;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implementation of the RecipientService interface that determines notification recipients
 * based on event context, permissions, and user preferences.
 */
@Singleton
public class RecipientServiceImpl implements RecipientService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecipientServiceImpl.class);

    private static final String NOTIFICATION_PREFIX = "notification.";
    private static final String NOTIFICATION_TYPE_PREFIX = "type.";
    private static final String NOTIFICATION_ALWAYS = "always";

    private final UserClient userClient;
    private final DeviceClient deviceClient;
    private final GroupClient groupClient;
    private final PermissionClient permissionClient;
    private final NotificatorManager notificatorManager;
    private final MeterRegistry meterRegistry;

    private final Cache<CacheKey, Boolean> permissionCache;
    private final Counter recipientCounter;
    private final Counter filteredRecipientCounter;

    /**
     * Cache key for permission checks, combining user ID, device ID, and permission type.
     */
    private static class CacheKey {
        private final long userId;
        private final long deviceId;
        private final String permission;

        public CacheKey(long userId, long deviceId, String permission) {
            this.userId = userId;
            this.deviceId = deviceId;
            this.permission = permission;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheKey cacheKey = (CacheKey) o;

            if (userId != cacheKey.userId) return false;
            if (deviceId != cacheKey.deviceId) return false;
            return permission.equals(cacheKey.permission);
        }

        @Override
        public int hashCode() {
            int result = (int) (userId ^ (userId >>> 32));
            result = 31 * result + (int) (deviceId ^ (deviceId >>> 32));
            result = 31 * result + permission.hashCode();
            return result;
        }
    }

    /**
     * Constructs a new RecipientServiceImpl with the required dependencies.
     *
     * @param userClient Client for user-related operations
     * @param deviceClient Client for device-related operations
     * @param groupClient Client for group-related operations
     * @param permissionClient Client for permission-related operations
     * @param notificatorManager Manager for notification channels
     * @param meterRegistry Registry for metrics collection
     */
    @Inject
    public RecipientServiceImpl(
            UserClient userClient,
            DeviceClient deviceClient,
            GroupClient groupClient,
            PermissionClient permissionClient,
            NotificatorManager notificatorManager,
            MeterRegistry meterRegistry) {
        this.userClient = userClient;
        this.deviceClient = deviceClient;
        this.groupClient = groupClient;
        this.permissionClient = permissionClient;
        this.notificatorManager = notificatorManager;
        this.meterRegistry = meterRegistry;

        // Initialize cache for permission checks with expiration
        this.permissionCache = Caffeine.newBuilder()
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .maximumSize(10000)
                .build();

        // Initialize metrics
        this.recipientCounter = Counter.builder("notification.recipients.total")
                .description("Total number of notification recipients")
                .register(meterRegistry);
        this.filteredRecipientCounter = Counter.builder("notification.recipients.filtered")
                .description("Number of filtered notification recipients")
                .register(meterRegistry);
    }

    @Override
    public List<User> getRecipients(Event event, Notification notification) {
        LOGGER.debug("Determining recipients for event {} and notification {}", event.getId(), notification.getId());
        
        // Get all users
        List<User> users = userClient.getAllUsers();
        int initialCount = users.size();
        
        // Apply permission filtering
        users = filterByPermission(users, event);
        
        // Apply user preferences
        users = applyUserPreferences(users, notification);
        
        // Update metrics
        recipientCounter.increment(users.size());
        filteredRecipientCounter.increment(initialCount - users.size());
        
        LOGGER.debug("Found {} recipients for event {}", users.size(), event.getId());
        return users;
    }

    @Override
    public Map<User, Map<String, String>> getRecipientsWithAddressing(Event event, Notification notification) {
        List<User> users = getRecipients(event, notification);
        Map<User, Map<String, String>> result = new HashMap<>();
        
        for (User user : users) {
            Map<String, String> addressing = new HashMap<>();
            
            // Add email address if available
            if (user.getEmail() != null && !user.getEmail().isEmpty()) {
                addressing.put("email", user.getEmail());
            }
            
            // Add phone number if available
            if (user.getPhone() != null && !user.getPhone().isEmpty()) {
                addressing.put("phone", user.getPhone());
            }
            
            // Add user ID for web notifications
            addressing.put("userId", String.valueOf(user.getId()));
            
            // Add any additional addressing information from user attributes
            Map<String, String> attributes = user.getAttributes();
            if (attributes != null) {
                // Add Telegram chat ID if available
                if (attributes.containsKey("telegramChatId")) {
                    addressing.put("telegramChatId", attributes.get("telegramChatId"));
                }
                
                // Add Firebase token if available
                if (attributes.containsKey("firebaseToken")) {
                    addressing.put("firebaseToken", attributes.get("firebaseToken"));
                }
                
                // Add Pushover user key if available
                if (attributes.containsKey("pushoverUserKey")) {
                    addressing.put("pushoverUserKey", attributes.get("pushoverUserKey"));
                }
            }
            
            // Only add users with at least one valid addressing method
            if (!addressing.isEmpty()) {
                result.put(user, addressing);
            }
        }
        
        return result;
    }

    @Override
    public List<User> filterByPermission(List<User> users, Event event) {
        if (event.getDeviceId() == 0) {
            return users; // No device to check permissions against
        }
        
        return users.stream()
                .filter(user -> hasPermission(user.getId(), event.getDeviceId()))
                .collect(Collectors.toList());
    }

    /**
     * Checks if a user has permission for a device, with caching for performance.
     * This method implements hierarchical permission checks (user, group, device).
     *
     * @param userId The ID of the user
     * @param deviceId The ID of the device
     * @return True if the user has permission, false otherwise
     */
    private boolean hasPermission(long userId, long deviceId) {
        CacheKey key = new CacheKey(userId, deviceId, "deviceReadPermission");
        
        return permissionCache.get(key, k -> {
            // Check direct device permission
            if (permissionClient.checkDevicePermission(userId, deviceId)) {
                return true;
            }
            
            // Check group-based permission (hierarchical)
            Device device = deviceClient.getDevice(deviceId);
            if (device != null && device.getGroupId() != 0) {
                // Get all groups in the hierarchy
                Set<Long> groupIds = new HashSet<>();
                collectGroupIds(groupIds, device.getGroupId());
                
                // Check permission for each group
                for (Long groupId : groupIds) {
                    if (permissionClient.checkGroupPermission(userId, groupId)) {
                        return true;
                    }
                }
            }
            
            // Check if user is admin (has access to all devices)
            User user = userClient.getUser(userId);
            return user != null && user.getAdministrator();
        });
    }

    /**
     * Recursively collects all group IDs in the hierarchy, including the given group ID
     * and all its parent groups.
     *
     * @param groupIds Set to collect group IDs into
     * @param groupId The group ID to start from
     */
    private void collectGroupIds(Set<Long> groupIds, long groupId) {
        if (groupId != 0 && groupIds.add(groupId)) {
            Group group = groupClient.getGroup(groupId);
            if (group != null && group.getGroupId() != 0) {
                collectGroupIds(groupIds, group.getGroupId());
            }
        }
    }

    @Override
    public List<User> applyUserPreferences(List<User> users, Notification notification) {
        if (notification == null) {
            return users;
        }
        
        String notificationType = notification.getType();
        Set<String> notificatorTypes = notificatorManager.getAllNotificatorTypes().stream()
                .map(t -> t.getType())
                .collect(Collectors.toSet());
        
        List<User> result = new ArrayList<>();
        
        for (User user : users) {
            // Get user preferences
            List<UserPreference> preferences = userClient.getUserPreferences(user.getId());
            Map<String, String> preferencesMap = preferences.stream()
                    .collect(Collectors.toMap(UserPreference::getKey, UserPreference::getValue));
            
            // Check if user has enabled this notification type
            String typeKey = NOTIFICATION_PREFIX + NOTIFICATION_TYPE_PREFIX + notificationType;
            if (NOTIFICATION_ALWAYS.equals(preferencesMap.get(typeKey))) {
                // User has enabled this notification type for all channels
                result.add(user);
                continue;
            }
            
            // Check if user has enabled any notification channel for this type
            boolean hasEnabledChannel = false;
            for (String notificatorType : notificatorTypes) {
                String preferenceKey = NOTIFICATION_PREFIX + notificatorType + "." + notificationType;
                if (Boolean.parseBoolean(preferencesMap.get(preferenceKey))) {
                    hasEnabledChannel = true;
                    break;
                }
            }
            
            if (hasEnabledChannel) {
                result.add(user);
            }
        }
        
        return result;
    }

    @Override
    public void invalidatePermissionCache(long userId) {
        // Remove all cache entries for this user
        permissionCache.asMap().keySet().removeIf(key -> key.userId == userId);
        LOGGER.debug("Invalidated permission cache for user {}", userId);
    }

    @Override
    public void invalidatePermissionCache() {
        // Clear the entire cache
        permissionCache.invalidateAll();
        LOGGER.debug("Invalidated entire permission cache");
    }
}