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

import java.util.Map;

/**
 * Interface for publishing messages to a message broker.
 * Implementations can use different message brokers like Kafka or RabbitMQ.
 */
public interface MessagePublisher {

    /**
     * Publishes a message to the specified topic.
     *
     * @param topic The topic to publish the message to
     * @param message The message to publish as a map of key-value pairs
     * @throws MessagingException If there is an error publishing the message
     */
    void publish(String topic, Map<String, Object> message) throws MessagingException;

    /**
     * Publishes a message to the specified topic with headers.
     *
     * @param topic The topic to publish the message to
     * @param message The message to publish as a map of key-value pairs
     * @param headers Additional headers to include with the message
     * @throws MessagingException If there is an error publishing the message
     */
    default void publish(String topic, Map<String, Object> message, Map<String, String> headers) throws MessagingException {
        // Default implementation falls back to the basic publish method
        publish(topic, message);
    }

    /**
     * Checks if the message broker is available.
     *
     * @return true if the message broker is available, false otherwise
     */
    boolean isAvailable();

    /**
     * Gets the health status of the message broker connection.
     *
     * @return A map of health metrics
     */
    Map<String, Object> getHealthMetrics();
}