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

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Service interface for tracking notification delivery status across all channels.
 * This service provides methods for recording delivery attempts, successes, and failures,
 * as well as querying delivery status for reporting and monitoring.
 */
public interface DeliveryStatusService {

    /**
     * Records a notification delivery attempt.
     *
     * @param notificationId The unique identifier of the notification
     * @param userId The user ID the notification is being delivered to
     * @param channel The notification channel (e.g., "email", "sms", "push")
     * @param correlationId The correlation ID for distributed tracing
     * @return The delivery attempt ID for tracking this specific attempt
     */
    String recordDeliveryAttempt(long notificationId, long userId, String channel, String correlationId);

    /**
     * Records a successful notification delivery.
     *
     * @param attemptId The delivery attempt ID returned by recordDeliveryAttempt
     * @param deliveryTime The timestamp when delivery was confirmed
     * @param metadata Additional metadata about the delivery (optional)
     */
    void recordDeliverySuccess(String attemptId, Instant deliveryTime, Map<String, Object> metadata);

    /**
     * Records a failed notification delivery.
     *
     * @param attemptId The delivery attempt ID returned by recordDeliveryAttempt
     * @param failureTime The timestamp when the failure occurred
     * @param errorCode The error code or type
     * @param errorMessage A human-readable error message
     * @param metadata Additional metadata about the failure (optional)
     */
    void recordDeliveryFailure(String attemptId, Instant failureTime, String errorCode, 
                              String errorMessage, Map<String, Object> metadata);

    /**
     * Retrieves the delivery status for a specific notification.
     *
     * @param notificationId The unique identifier of the notification
     * @return A map containing delivery status information for each channel and recipient
     */
    Map<String, Object> getDeliveryStatus(long notificationId);

    /**
     * Retrieves the delivery status for a specific notification and user.
     *
     * @param notificationId The unique identifier of the notification
     * @param userId The user ID to check delivery status for
     * @return A map containing delivery status information for the specified user
     */
    Map<String, Object> getDeliveryStatus(long notificationId, long userId);

    /**
     * Retrieves the delivery status for a specific notification, user, and channel.
     *
     * @param notificationId The unique identifier of the notification
     * @param userId The user ID to check delivery status for
     * @param channel The notification channel to check
     * @return A map containing delivery status information for the specified channel
     */
    Map<String, Object> getDeliveryStatus(long notificationId, long userId, String channel);

    /**
     * Generates a delivery report for a specific time period.
     *
     * @param startTime The start of the time period
     * @param endTime The end of the time period
     * @param channels Optional list of channels to include (null for all channels)
     * @param userIds Optional list of user IDs to include (null for all users)
     * @return A report containing delivery statistics and details
     */
    Map<String, Object> generateDeliveryReport(Instant startTime, Instant endTime, 
                                             List<String> channels, List<Long> userIds);

    /**
     * Publishes delivery status updates to the message broker for other services to consume.
     *
     * @param notificationId The unique identifier of the notification
     * @param status The current delivery status
     * @param correlationId The correlation ID for distributed tracing
     */
    void publishDeliveryStatus(long notificationId, Map<String, Object> status, String correlationId);

    /**
     * Retrieves delivery metrics for monitoring and alerting.
     *
     * @param timeWindow The time window to calculate metrics for (in seconds)
     * @return A map containing delivery metrics (success rate, failure rate, etc.)
     */
    Map<String, Object> getDeliveryMetrics(int timeWindow);

    /**
     * Retrieves delivery metrics for a specific channel.
     *
     * @param channel The notification channel to get metrics for
     * @param timeWindow The time window to calculate metrics for (in seconds)
     * @return A map containing delivery metrics for the specified channel
     */
    Map<String, Object> getDeliveryMetrics(String channel, int timeWindow);
}