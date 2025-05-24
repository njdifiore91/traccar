/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.messaging.consumer;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.traccar.model.Notification;
import org.traccar.model.User;

import java.util.UUID;

/**
 * Abstract base class for all message consumers in the Notification Service.
 * Provides common functionality for message consumption, error handling, and retry logic.
 */
public abstract class BaseMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseMessageConsumer.class);

    @Inject
    protected KafkaTemplate<String, Object> kafkaTemplate;

    @Inject
    protected MeterRegistry meterRegistry;

    @Value("${kafka.topic.dead-letter-queue}")
    protected String deadLetterQueueTopic;

    @Value("${kafka.topic.notification-status}")
    protected String notificationStatusTopic;

    @PostConstruct
    public void init() {
        LOGGER.info("Initialized {}", getClass().getSimpleName());
    }

    /**
     * Publishes a failed message to the dead letter queue for later retry.
     *
     * @param notification The notification that failed to be processed
     * @param user The target user for the notification
     * @param correlationId The correlation ID for tracking
     * @param exception The exception that caused the failure
     */
    protected void publishToDeadLetterQueue(
            Notification notification, User user, String correlationId, Exception exception) {
        try {
            if (correlationId == null) {
                correlationId = UUID.randomUUID().toString();
            }

            DeadLetterMessage message = new DeadLetterMessage(
                    notification, user, correlationId, exception.getMessage(),
                    System.currentTimeMillis(), getClass().getSimpleName());

            kafkaTemplate.send(deadLetterQueueTopic, correlationId, message);
            LOGGER.debug("Published to dead letter queue: {}, {}", notification.getId(), correlationId);

            // Record metric for dead letter queue publications
            meterRegistry.counter("notification.deadletter.count",
                    "consumer", getClass().getSimpleName()).increment();

        } catch (Exception e) {
            LOGGER.error("Failed to publish to dead letter queue: {}", e.getMessage(), e);
        }
    }

    /**
     * Inner class representing a message in the dead letter queue.
     */
    protected static class DeadLetterMessage {
        private final Notification notification;
        private final User user;
        private final String correlationId;
        private final String errorMessage;
        private final long timestamp;
        private final String source;
        private int retryCount;

        public DeadLetterMessage(
                Notification notification, User user, String correlationId,
                String errorMessage, long timestamp, String source) {
            this.notification = notification;
            this.user = user;
            this.correlationId = correlationId;
            this.errorMessage = errorMessage;
            this.timestamp = timestamp;
            this.source = source;
            this.retryCount = 0;
        }

        public Notification getNotification() {
            return notification;
        }

        public User getUser() {
            return user;
        }

        public String getCorrelationId() {
            return correlationId;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getSource() {
            return source;
        }

        public int getRetryCount() {
            return retryCount;
        }

        public void incrementRetryCount() {
            this.retryCount++;
        }
    }
}