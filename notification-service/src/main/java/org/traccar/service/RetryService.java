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

import org.traccar.notification.MessageException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Service interface for handling notification delivery retries with exponential backoff and jitter.
 * This interface provides methods for scheduling retries, tracking retry attempts, and implementing
 * backoff strategies for failed deliveries.
 */
public interface RetryService {

    /**
     * Schedule a retry for a failed notification delivery.
     *
     * @param channelId The ID of the notification channel (e.g., "email", "sms", "push")
     * @param notificationId The unique identifier of the notification
     * @param payload The notification payload to be delivered
     * @param exception The exception that caused the delivery failure
     * @param metadata Additional metadata about the notification and delivery attempt
     * @return A CompletableFuture that completes when the retry is scheduled
     */
    CompletableFuture<Void> scheduleRetry(String channelId, String notificationId, Object payload,
                                         MessageException exception, Map<String, Object> metadata);

    /**
     * Get the retry history for a specific notification.
     *
     * @param notificationId The unique identifier of the notification
     * @return A list of RetryAttempt objects representing the retry history
     */
    List<RetryAttempt> getRetryHistory(String notificationId);

    /**
     * Check if a notification has exceeded its retry limit for a specific channel.
     *
     * @param channelId The ID of the notification channel
     * @param notificationId The unique identifier of the notification
     * @return true if the notification has exceeded its retry limit, false otherwise
     */
    boolean hasExceededRetryLimit(String channelId, String notificationId);

    /**
     * Publish a failed notification to the dead letter queue after exceeding retry limits.
     *
     * @param channelId The ID of the notification channel
     * @param notificationId The unique identifier of the notification
     * @param payload The notification payload
     * @param exception The exception that caused the delivery failure
     * @param metadata Additional metadata about the notification and delivery attempts
     * @return A CompletableFuture that completes when the message is published to the dead letter queue
     */
    CompletableFuture<Void> publishToDeadLetterQueue(String channelId, String notificationId, Object payload,
                                                   MessageException exception, Map<String, Object> metadata);

    /**
     * Get metrics about retry operations across all channels.
     *
     * @return A map containing retry metrics
     */
    Map<String, Object> getRetryMetrics();

    /**
     * Class representing a single retry attempt for a notification.
     */
    class RetryAttempt {
        private final String notificationId;
        private final String channelId;
        private final int attemptNumber;
        private final long timestamp;
        private final String errorMessage;
        private final String errorType;
        private final long delayMs;

        /**
         * Constructor for RetryAttempt.
         *
         * @param notificationId The unique identifier of the notification
         * @param channelId The ID of the notification channel
         * @param attemptNumber The retry attempt number
         * @param timestamp The timestamp of the retry attempt
         * @param errorMessage The error message from the exception
         * @param errorType The type of the exception
         * @param delayMs The delay before the retry attempt in milliseconds
         */
        public RetryAttempt(String notificationId, String channelId, int attemptNumber,
                           long timestamp, String errorMessage, String errorType, long delayMs) {
            this.notificationId = notificationId;
            this.channelId = channelId;
            this.attemptNumber = attemptNumber;
            this.timestamp = timestamp;
            this.errorMessage = errorMessage;
            this.errorType = errorType;
            this.delayMs = delayMs;
        }

        /**
         * Get the notification ID.
         *
         * @return The notification ID
         */
        public String getNotificationId() {
            return notificationId;
        }

        /**
         * Get the channel ID.
         *
         * @return The channel ID
         */
        public String getChannelId() {
            return channelId;
        }

        /**
         * Get the attempt number.
         *
         * @return The attempt number
         */
        public int getAttemptNumber() {
            return attemptNumber;
        }

        /**
         * Get the timestamp of the retry attempt.
         *
         * @return The timestamp in milliseconds
         */
        public long getTimestamp() {
            return timestamp;
        }

        /**
         * Get the error message from the exception.
         *
         * @return The error message
         */
        public String getErrorMessage() {
            return errorMessage;
        }

        /**
         * Get the type of the exception.
         *
         * @return The exception type
         */
        public String getErrorType() {
            return errorType;
        }

        /**
         * Get the delay before the retry attempt.
         *
         * @return The delay in milliseconds
         */
        public long getDelayMs() {
            return delayMs;
        }
    }
}