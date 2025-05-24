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
 * Interface defining the contract for publishing enriched position messages to the message broker
 * from the Position Processing Service. Provides methods for sending messages to topics with proper
 * headers and metadata.
 */
public interface PositionProducer {

    /**
     * Publishes a position message to the message broker asynchronously.
     *
     * @param positionMessage The position message to publish
     * @return A CompletableFuture that completes when the message has been acknowledged by the broker
     */
    CompletableFuture<Void> sendAsync(PositionMessage positionMessage);

    /**
     * Publishes a position message to the message broker asynchronously with custom headers.
     *
     * @param positionMessage The position message to publish
     * @param headers Additional headers to include with the message
     * @return A CompletableFuture that completes when the message has been acknowledged by the broker
     */
    CompletableFuture<Void> sendAsync(PositionMessage positionMessage, Map<String, String> headers);

    /**
     * Publishes a position message to the message broker synchronously.
     *
     * @param positionMessage The position message to publish
     * @throws MessageException If there is an error publishing the message
     */
    void send(PositionMessage positionMessage) throws MessageException;

    /**
     * Publishes a position message to the message broker synchronously with custom headers.
     *
     * @param positionMessage The position message to publish
     * @param headers Additional headers to include with the message
     * @throws MessageException If there is an error publishing the message
     */
    void send(PositionMessage positionMessage, Map<String, String> headers) throws MessageException;

    /**
     * Flushes any buffered messages to the message broker.
     *
     * @throws MessageException If there is an error flushing the messages
     */
    void flush() throws MessageException;

    /**
     * Closes the producer and releases any resources.
     */
    void close();
}