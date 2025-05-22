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

import java.util.concurrent.CompletableFuture;

/**
 * Interface for message broker integration to support asynchronous communication
 * between microservices. Provides methods for publishing messages to topics
 * and subscribing to receive messages.
 */
public interface MessageBrokerManager {

    /**
     * Publishes a message to the specified topic asynchronously.
     *
     * @param topic The topic to publish the message to
     * @param message The message to publish
     * @param <T> The type of the message
     * @return A CompletableFuture that completes when the message is published
     */
    <T> CompletableFuture<Void> publishAsync(String topic, T message);

    /**
     * Publishes a message to the specified topic synchronously.
     *
     * @param topic The topic to publish the message to
     * @param message The message to publish
     * @param <T> The type of the message
     * @throws MessageBrokerException If there is an error publishing the message
     */
    <T> void publish(String topic, T message) throws MessageBrokerException;

    /**
     * Subscribes to a topic to receive messages.
     *
     * @param topic The topic to subscribe to
     * @param messageType The class of the message type
     * @param handler The handler to process received messages
     * @param <T> The type of the message
     * @return A subscription ID that can be used to unsubscribe
     */
    <T> String subscribe(String topic, Class<T> messageType, MessageHandler<T> handler);

    /**
     * Unsubscribes from a topic using the subscription ID.
     *
     * @param subscriptionId The subscription ID returned from subscribe
     * @return true if successfully unsubscribed, false otherwise
     */
    boolean unsubscribe(String subscriptionId);

    /**
     * Checks if the message broker is connected and operational.
     *
     * @return true if connected, false otherwise
     */
    boolean isConnected();

    /**
     * Closes the connection to the message broker and releases resources.
     */
    void close();
}