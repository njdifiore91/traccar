package org.traccar.messaging;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for sending messages to a message broker.
 * Abstracts the functionality of publishing messages to topics or queues,
 * allowing services to communicate asynchronously without being coupled to
 * a specific message broker implementation.
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
     * Publishes a message to the specified topic or queue with a routing key and custom headers.
     * The routing key is used by the broker to determine which consumers should receive the message.
     *
     * @param destination the topic or queue name
     * @param routingKey the routing key for message routing
     * @param message the message payload
     * @param headers additional message headers
     * @return a CompletableFuture that completes when the message is acknowledged by the broker
     */
    CompletableFuture<Void> publish(String destination, String routingKey, Object message, Map<String, Object> headers);

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
     * Closes the producer and releases any resources.
     */
    void close();
}