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
 * Interface for message broker clients.
 * Implementations should provide methods to publish and subscribe to topics.
 */
public interface MessageBrokerClient {

    /**
     * Publish a message to a topic.
     *
     * @param topic   The topic to publish to
     * @param message The message to publish
     * @return A CompletableFuture that completes when the message is published
     */
    <T> CompletableFuture<Void> publish(String topic, T message);

    /**
     * Subscribe to a topic.
     *
     * @param topic    The topic to subscribe to
     * @param callback The callback to invoke when a message is received
     * @return A CompletableFuture that completes when the subscription is established
     */
    <T> CompletableFuture<Void> subscribe(String topic, MessageCallback<T> callback, Class<T> messageType);

    /**
     * Close the client and release resources.
     *
     * @return A CompletableFuture that completes when the client is closed
     */
    CompletableFuture<Void> close();

    /**
     * Check if the client is connected to the broker.
     *
     * @return true if connected, false otherwise
     */
    boolean isConnected();

    /**
     * Callback interface for message subscription.
     *
     * @param <T> The type of message
     */
    interface MessageCallback<T> {
        void onMessage(T message);
    }
}