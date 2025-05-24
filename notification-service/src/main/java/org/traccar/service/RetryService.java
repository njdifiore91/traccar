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
 * Provides methods for scheduling retries, tracking retry attempts, and implementing backoff strategies
 * for failed deliveries.
 */
public interface RetryService {

    /**
     * Schedule a retry for a failed notification delivery with exponential backoff and jitter.
     *
     * @param channelId The identifier of the notification channel (e.g., "email", "sms", "push")
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
     * @return A list of retry attempt records for the notification
     */
    List<RetryAttempt> getRetryHistory(String notificationId);

    /**
     * Check if a notification has exceeded its retry limit.
     *
     * @param channelId The identifier of the notification channel
     * @param notificationId The unique identifier of the notification
     * @return true if the notification has exceeded its retry limit, false otherwise
     */
    boolean hasExceededRetryLimit(String channelId, String notificationId);

    /**
     * Publish a notification to the dead letter queue after all retry attempts have been exhausted.
     *
     * @param channelId The identifier of the notification channel
     * @param notificationId The unique identifier of the notification
     * @param payload The notification payload
     * @param exception The last exception that caused the delivery failure
     * @param metadata Additional metadata about the notification and delivery attempts
     * @return A CompletableFuture that completes when the notification is published to the dead letter queue
     */
    CompletableFuture<Void> publishToDeadLetterQueue(String channelId, String notificationId, Object payload,
                                                   MessageException exception, Map<String, Object> metadata);

    /**
     * Get metrics for retry operations.
     *
     * @return A map of metric names to values
     */
    Map<String, Object> getRetryMetrics();

    /**
     * Class representing a retry attempt record.
     */
    class RetryAttempt {
        private final String notificationId;
        private final String channelId;
        private final int attemptNumber;
        private final long timestamp;
        private final String exceptionMessage;
        private final String exceptionType;
        private final long delayMs;

        public RetryAttempt(String notificationId, String channelId, int attemptNumber, 
                           long timestamp, String exceptionMessage, String exceptionType, long delayMs) {
            this.notificationId = notificationId;
            this.channelId = channelId;
            this.attemptNumber = attemptNumber;
            this.timestamp = timestamp;
            this.exceptionMessage = exceptionMessage;
            this.exceptionType = exceptionType;
            this.delayMs = delayMs;
        }

        public String getNotificationId() {
            return notificationId;
        }

        public String getChannelId() {
            return channelId;
        }

        public int getAttemptNumber() {
            return attemptNumber;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getExceptionMessage() {
            return exceptionMessage;
        }

        public String getExceptionType() {
            return exceptionType;
        }

        public long getDelayMs() {
            return delayMs;
        }
    }
}