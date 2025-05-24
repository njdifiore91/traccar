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

import java.time.Duration;

/**
 * Service interface for implementing rate limiting across notification channels.
 * This interface provides methods for checking and enforcing rate limits,
 * tracking usage, and implementing channel-specific throttling policies.
 */
public interface RateLimitService {

    /**
     * Checks if a notification can be sent based on rate limits without consuming the limit.
     *
     * @param channelType The notification channel type (e.g., "email", "sms", "push")
     * @param key The resource identifier to check (e.g., user ID, device ID, recipient)
     * @return true if the notification is allowed, false if rate limited
     */
    boolean isAllowed(String channelType, String key);

    /**
     * Attempts to acquire a permit for sending a notification, consuming from the rate limit if successful.
     * This method is non-blocking and returns immediately.
     *
     * @param channelType The notification channel type (e.g., "email", "sms", "push")
     * @param key The resource identifier to check (e.g., user ID, device ID, recipient)
     * @return true if the notification is allowed and the permit was acquired, false if rate limited
     */
    boolean tryAcquire(String channelType, String key);

    /**
     * Attempts to acquire a permit for sending a notification, waiting up to the specified timeout
     * if necessary for the rate limit to allow the operation.
     *
     * @param channelType The notification channel type (e.g., "email", "sms", "push")
     * @param key The resource identifier to check (e.g., user ID, device ID, recipient)
     * @param timeout Maximum time to wait for a permit
     * @return true if the notification is allowed and the permit was acquired within the timeout, false otherwise
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    boolean tryAcquire(String channelType, String key, Duration timeout) throws InterruptedException;

    /**
     * Records a successful notification delivery, which may affect future rate limit calculations.
     * This can be used for adaptive rate limiting based on success/failure patterns.
     *
     * @param channelType The notification channel type (e.g., "email", "sms", "push")
     * @param key The resource identifier (e.g., user ID, device ID, recipient)
     */
    void recordSuccess(String channelType, String key);

    /**
     * Records a failed notification delivery, which may affect future rate limit calculations.
     * This can be used to implement circuit breaking or backoff strategies.
     *
     * @param channelType The notification channel type (e.g., "email", "sms", "push")
     * @param key The resource identifier (e.g., user ID, device ID, recipient)
     */
    void recordFailure(String channelType, String key);

    /**
     * Gets the current usage count for a specific channel and key.
     *
     * @param channelType The notification channel type (e.g., "email", "sms", "push")
     * @param key The resource identifier (e.g., user ID, device ID, recipient)
     * @return The current count of notifications sent within the current time window
     */
    long getCurrentUsage(String channelType, String key);

    /**
     * Gets the configured limit for a specific channel and key.
     *
     * @param channelType The notification channel type (e.g., "email", "sms", "push")
     * @param key The resource identifier (e.g., user ID, device ID, recipient)
     * @return The maximum number of notifications allowed within the time window
     */
    long getLimit(String channelType, String key);

    /**
     * Gets the remaining capacity for a specific channel and key.
     *
     * @param channelType The notification channel type (e.g., "email", "sms", "push")
     * @param key The resource identifier (e.g., user ID, device ID, recipient)
     * @return The number of additional notifications that can be sent within the current time window
     */
    long getRemainingCapacity(String channelType, String key);

    /**
     * Gets the estimated time until the rate limit resets for a specific channel and key.
     *
     * @param channelType The notification channel type (e.g., "email", "sms", "push")
     * @param key The resource identifier (e.g., user ID, device ID, recipient)
     * @return The estimated duration until the rate limit resets, or Duration.ZERO if not limited
     */
    Duration getTimeToReset(String channelType, String key);

    /**
     * Applies a user-level rate limit check. This is a higher-level method that considers
     * all notifications sent to a specific user across all devices and channels.
     *
     * @param userId The user ID to check
     * @return true if the notification is allowed, false if rate limited
     */
    boolean isUserAllowed(long userId);

    /**
     * Applies a device-level rate limit check. This is a higher-level method that considers
     * all notifications sent for a specific device across all channels.
     *
     * @param deviceId The device ID to check
     * @return true if the notification is allowed, false if rate limited
     */
    boolean isDeviceAllowed(long deviceId);

    /**
     * Applies a combined user and channel rate limit check.
     *
     * @param userId The user ID to check
     * @param channelType The notification channel type
     * @return true if the notification is allowed, false if rate limited
     */
    boolean isUserChannelAllowed(long userId, String channelType);

    /**
     * Applies a combined device and channel rate limit check.
     *
     * @param deviceId The device ID to check
     * @param channelType The notification channel type
     * @return true if the notification is allowed, false if rate limited
     */
    boolean isDeviceChannelAllowed(long deviceId, String channelType);

    /**
     * Resets rate limit counters for a specific channel and key.
     * This can be used for administrative purposes or when handling exceptional cases.
     *
     * @param channelType The notification channel type (e.g., "email", "sms", "push")
     * @param key The resource identifier (e.g., user ID, device ID, recipient)
     */
    void resetLimits(String channelType, String key);

    /**
     * Updates the rate limit configuration for a specific channel and key.
     * This allows dynamic adjustment of rate limits based on runtime conditions.
     *
     * @param channelType The notification channel type (e.g., "email", "sms", "push")
     * @param key The resource identifier (e.g., user ID, device ID, recipient)
     * @param limit The new limit to apply
     * @param window The time window for the limit
     */
    void updateLimitConfig(String channelType, String key, long limit, Duration window);

    /**
     * Registers a custom rate limit policy for a specific channel.
     * This allows for channel-specific rate limiting strategies.
     *
     * @param channelType The notification channel type (e.g., "email", "sms", "push")
     * @param policy The rate limit policy to apply
     */
    void registerChannelPolicy(String channelType, RateLimitPolicy policy);

    /**
     * Interface for implementing channel-specific rate limit policies.
     * Different notification channels may have different rate limiting requirements.
     */
    interface RateLimitPolicy {
        
        /**
         * Checks if a notification is allowed based on the policy rules.
         *
         * @param key The resource identifier
         * @return true if allowed, false if rate limited
         */
        boolean isAllowed(String key);
        
        /**
         * Gets the limit for a specific key according to this policy.
         *
         * @param key The resource identifier
         * @return The configured limit
         */
        long getLimit(String key);
        
        /**
         * Gets the time window for this policy.
         *
         * @return The time window duration
         */
        Duration getWindow();
    }
}