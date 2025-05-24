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
package org.traccar.notification.mail;

import java.util.HashMap;
import java.util.Map;

/**
 * The NotificationContext class provides additional context for notifications
 * in the microservices architecture. It contains metadata needed for message broker
 * integration, distributed tracing, and other cross-cutting concerns.
 * <p>
 * This class supports the asynchronous messaging architecture of the notification service
 * by providing a container for message properties, routing information, and other
 * metadata required for proper message handling.
 */
public class NotificationContext {

    private String correlationId;
    private Map<String, Object> properties;
    private NotificationPriority priority;
    private long timestamp;
    private String sourceService;
    private int retryCount;

    /**
     * Creates a new notification context with default values.
     */
    public NotificationContext() {
        this.properties = new HashMap<>();
        this.priority = NotificationPriority.NORMAL;
        this.timestamp = System.currentTimeMillis();
        this.retryCount = 0;
    }

    /**
     * Creates a new notification context with the specified correlation ID.
     *
     * @param correlationId the unique identifier for distributed tracing
     */
    public NotificationContext(String correlationId) {
        this();
        this.correlationId = correlationId;
    }

    /**
     * Gets the correlation ID for distributed tracing.
     *
     * @return the correlation ID
     */
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * Sets the correlation ID for distributed tracing.
     *
     * @param correlationId the correlation ID to set
     * @return this context instance for method chaining
     */
    public NotificationContext setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
        return this;
    }

    /**
     * Gets the custom properties map.
     *
     * @return the properties map
     */
    public Map<String, Object> getProperties() {
        return properties;
    }

    /**
     * Sets a custom property value.
     *
     * @param key   the property key
     * @param value the property value
     * @return this context instance for method chaining
     */
    public NotificationContext setProperty(String key, Object value) {
        this.properties.put(key, value);
        return this;
    }

    /**
     * Gets a custom property value.
     *
     * @param key the property key
     * @return the property value, or null if not found
     */
    public Object getProperty(String key) {
        return this.properties.get(key);
    }

    /**
     * Gets the notification priority.
     *
     * @return the notification priority
     */
    public NotificationPriority getPriority() {
        return priority;
    }

    /**
     * Sets the notification priority.
     *
     * @param priority the priority to set
     * @return this context instance for method chaining
     */
    public NotificationContext setPriority(NotificationPriority priority) {
        this.priority = priority;
        return this;
    }

    /**
     * Gets the notification timestamp.
     *
     * @return the timestamp in milliseconds since epoch
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Sets the notification timestamp.
     *
     * @param timestamp the timestamp to set
     * @return this context instance for method chaining
     */
    public NotificationContext setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * Gets the source service that originated the notification.
     *
     * @return the source service name
     */
    public String getSourceService() {
        return sourceService;
    }

    /**
     * Sets the source service that originated the notification.
     *
     * @param sourceService the source service to set
     * @return this context instance for method chaining
     */
    public NotificationContext setSourceService(String sourceService) {
        this.sourceService = sourceService;
        return this;
    }

    /**
     * Gets the retry count for this notification.
     *
     * @return the current retry count
     */
    public int getRetryCount() {
        return retryCount;
    }

    /**
     * Sets the retry count for this notification.
     *
     * @param retryCount the retry count to set
     * @return this context instance for method chaining
     */
    public NotificationContext setRetryCount(int retryCount) {
        this.retryCount = retryCount;
        return this;
    }

    /**
     * Increments the retry count by one.
     *
     * @return this context instance for method chaining
     */
    public NotificationContext incrementRetryCount() {
        this.retryCount++;
        return this;
    }
}