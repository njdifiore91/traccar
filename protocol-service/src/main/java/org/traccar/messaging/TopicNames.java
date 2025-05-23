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
package org.traccar.messaging;

import java.util.regex.Pattern;

/**
 * Provides constants for topic names used in the messaging subsystem.
 * This ensures consistency across the codebase and makes it easier to change topic names if needed.
 * 
 * Topic names follow the format: [prefix].[entity].[action]
 * - prefix: Optional environment-specific prefix (e.g., "dev", "staging", "prod")
 * - entity: The primary entity the message relates to (e.g., "position", "device")
 * - action: The action or state being communicated (e.g., "created", "updated", "connections")
 */
public final class TopicNames {

    private TopicNames() {
        // Utility class
    }

    // Topic name validation pattern
    private static final Pattern VALID_TOPIC_NAME = Pattern.compile("^[a-zA-Z0-9\\-_.]+$");
    
    // Default prefix for topic names (can be overridden via configuration)
    private static String topicPrefix = "";

    /**
     * Raw position data published by the Protocol Service.
     * Contains decoded but unprocessed position data from devices.
     * Consumed by the Position Processing Service for enrichment and storage.
     */
    public static final String RAW_POSITIONS = "raw.positions";

    /**
     * Device connection events published by the Protocol Service.
     * Includes connection, disconnection, and idle state changes.
     * Consumed by the State Service and API Gateway for device status tracking.
     */
    public static final String DEVICE_CONNECTIONS = "device.connections";

    /**
     * Device command responses published by the Protocol Service.
     * Contains the results of commands sent to devices.
     * Consumed by the API Gateway to inform clients of command execution status.
     */
    public static final String COMMAND_RESPONSES = "command.responses";

    /**
     * Device command requests published by the API Gateway.
     * Contains commands to be sent to devices.
     * Consumed by the Protocol Service for transmission to devices.
     */
    public static final String COMMAND_REQUESTS = "command.requests";

    /**
     * Enriched position data published by the Position Processing Service.
     * Contains positions with additional context (geocoding, geofencing, etc.).
     * Consumed by the Event Processing Service for event detection.
     */
    public static final String ENRICHED_POSITIONS = "enriched.positions";

    /**
     * Events detected by the Event Processing Service.
     * Contains events like geofence transitions, speed violations, etc.
     * Consumed by the Notification Service for alert delivery.
     */
    public static final String EVENTS = "events";

    /**
     * Notifications published by the Notification Service.
     * Contains formatted notifications ready for delivery.
     * Consumed by the API Gateway for WebSocket delivery to clients.
     */
    public static final String NOTIFICATIONS = "notifications";

    /**
     * Heartbeat events for WebSocket connections.
     * Used to maintain WebSocket connections and detect disconnections.
     * Published by a scheduled job and consumed by the API Gateway.
     */
    public static final String HEARTBEAT_EVENTS = "heartbeat.events";

    /**
     * Cache invalidation events.
     * Used to notify services when cached data should be refreshed.
     * Published by services that modify data and consumed by all services with caches.
     */
    public static final String CACHE_INVALIDATION = "cache.invalidation";

    /**
     * Sets the environment-specific prefix for topic names.
     * This allows for isolation between different environments (dev, staging, prod).
     * 
     * @param prefix The prefix to apply to all topic names (e.g., "dev", "staging", "prod")
     * @throws IllegalArgumentException If the prefix contains invalid characters
     */
    public static void setTopicPrefix(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            topicPrefix = "";
            return;
        }
        
        validateTopicName(prefix);
        topicPrefix = prefix + ".";
    }

    /**
     * Gets the full topic name with the current environment prefix applied.
     * 
     * @param baseTopic The base topic name constant
     * @return The full topic name with prefix
     */
    public static String getTopic(String baseTopic) {
        return topicPrefix + baseTopic;
    }

    /**
     * Gets the raw positions topic name with the current environment prefix applied.
     * 
     * @return The full raw positions topic name
     */
    public static String getRawPositionsTopic() {
        return getTopic(RAW_POSITIONS);
    }

    /**
     * Gets the device connections topic name with the current environment prefix applied.
     * 
     * @return The full device connections topic name
     */
    public static String getDeviceConnectionsTopic() {
        return getTopic(DEVICE_CONNECTIONS);
    }

    /**
     * Gets the command responses topic name with the current environment prefix applied.
     * 
     * @return The full command responses topic name
     */
    public static String getCommandResponsesTopic() {
        return getTopic(COMMAND_RESPONSES);
    }

    /**
     * Gets the command requests topic name with the current environment prefix applied.
     * 
     * @return The full command requests topic name
     */
    public static String getCommandRequestsTopic() {
        return getTopic(COMMAND_REQUESTS);
    }

    /**
     * Gets the enriched positions topic name with the current environment prefix applied.
     * 
     * @return The full enriched positions topic name
     */
    public static String getEnrichedPositionsTopic() {
        return getTopic(ENRICHED_POSITIONS);
    }

    /**
     * Gets the events topic name with the current environment prefix applied.
     * 
     * @return The full events topic name
     */
    public static String getEventsTopic() {
        return getTopic(EVENTS);
    }

    /**
     * Gets the notifications topic name with the current environment prefix applied.
     * 
     * @return The full notifications topic name
     */
    public static String getNotificationsTopic() {
        return getTopic(NOTIFICATIONS);
    }

    /**
     * Gets the heartbeat events topic name with the current environment prefix applied.
     * 
     * @return The full heartbeat events topic name
     */
    public static String getHeartbeatEventsTopic() {
        return getTopic(HEARTBEAT_EVENTS);
    }

    /**
     * Gets the cache invalidation topic name with the current environment prefix applied.
     * 
     * @return The full cache invalidation topic name
     */
    public static String getCacheInvalidationTopic() {
        return getTopic(CACHE_INVALIDATION);
    }

    /**
     * Validates that a topic name contains only allowed characters.
     * Valid characters are alphanumeric, hyphen, underscore, and period.
     * 
     * @param topicName The topic name to validate
     * @throws IllegalArgumentException If the topic name contains invalid characters
     */
    public static void validateTopicName(String topicName) {
        if (topicName == null || topicName.isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }
        
        if (!VALID_TOPIC_NAME.matcher(topicName).matches()) {
            throw new IllegalArgumentException(
                    "Topic name contains invalid characters. Only alphanumeric, hyphen, underscore, and period are allowed.");
        }
    }
}