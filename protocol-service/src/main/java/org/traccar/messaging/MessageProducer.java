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
import java.util.concurrent.CompletableFuture;

/**
 * Interface for sending messages to a message broker.
 * Abstracts the functionality of publishing messages to topics or queues,
 * allowing the Protocol Service to communicate asynchronously without being coupled to
 * a specific message broker implementation.
 *
 * This interface is used by the Protocol Service to publish position data and device
 * connection events to the message broker for consumption by other microservices.
 */
public interface MessageProducer {

    /**
     * Checks if the producer is connected to the message broker.
     *
     * @return true if connected, false otherwise
     */
    boolean isConnected();

    /**
     * Publishes a message to the specified topic or queue with default headers.
     *
     * @param destination the topic or queue name
     * @param message the message payload
     * @return a CompletableFuture that completes when the message is acknowledged by the broker
     */
    CompletableFuture<Void> publish(String destination, Object message);

    /**
     * Publishes a message to the specified topic or queue with custom headers.
     *
     * @param destination the topic or queue name
     * @param message the message payload
     * @param headers additional message headers
     * @return a CompletableFuture that completes when the message is acknowledged by the broker
     */
    CompletableFuture<Void> publish(String destination, Object message, Map<String, Object> headers);

    /**
     * Publishes a message to the specified topic or queue with a specific key for partitioning.
     * The key is used by the broker to determine which partition the message should be sent to,
     * ensuring that messages with the same key are processed in order by the same consumer.
     *
     * @param destination the topic or queue name
     * @param key the partitioning key (typically device ID)
     * @param message the message payload
     * @return a CompletableFuture that completes when the message is acknowledged by the broker
     */
    CompletableFuture<Void> publish(String destination, String key, Object message);

    /**
     * Publishes a message to the specified topic or queue with a specific key for partitioning
     * and custom headers.
     *
     * @param destination the topic or queue name
     * @param key the partitioning key (typically device ID)
     * @param message the message payload
     * @param headers additional message headers
     * @return a CompletableFuture that completes when the message is acknowledged by the broker
     */
    CompletableFuture<Void> publish(String destination, String key, Object message, Map<String, Object> headers);

    /**
     * Publishes a batch of messages to the specified topic or queue.
     * This method is optimized for high throughput scenarios where multiple messages
     * need to be sent efficiently.
     *
     * @param destination the topic or queue name
     * @param messages the message payloads
     * @return a CompletableFuture that completes when all messages are acknowledged by the broker
     */
    CompletableFuture<Void> publishBatch(String destination, Iterable<?> messages);

    /**
     * Publishes a batch of messages to the specified topic or queue with keys for partitioning.
     * Each message is associated with a key that determines its partition.
     *
     * @param destination the topic or queue name
     * @param messagesWithKeys map of messages with their corresponding keys
     * @return a CompletableFuture that completes when all messages are acknowledged by the broker
     */
    CompletableFuture<Void> publishBatchWithKeys(String destination, Map<String, Object> messagesWithKeys);

    /**
     * Configures the retry policy for failed publish operations.
     *
     * @param maxRetries maximum number of retry attempts
     * @param initialBackoffMs initial backoff time in milliseconds
     * @param maxBackoffMs maximum backoff time in milliseconds
     */
    void setRetryPolicy(int maxRetries, long initialBackoffMs, long maxBackoffMs);

    /**
     * Performs a health check on the message broker connection.
     *
     * @return a CompletableFuture that completes with the health status
     */
    CompletableFuture<Boolean> checkHealth();

    /**
     * Closes the producer and releases any resources.
     */
    void close();
}