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

import org.traccar.model.Position;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Interface defining the contract for publishing enriched position messages to the message broker
 * from the Position Processing Service. Provides methods for sending messages to topics with proper
 * headers and metadata.
 * <p>
 * This interface supports both Kafka and RabbitMQ implementations and implements the transaction
 * outbox pattern for reliable message publishing.
 */
public interface PositionProducer {

    /**
     * Publishes an enriched position message to the message broker.
     * <p>
     * This method uses the transaction outbox pattern to ensure reliable message delivery.
     * The position will be stored in a local outbox table before being published to the broker.
     *
     * @param position The enriched position to publish
     * @return A CompletableFuture that completes when the message has been successfully published
     * @throws MessageException If there is an error publishing the message
     */
    CompletableFuture<Void> publishPosition(Position position) throws MessageException;

    /**
     * Publishes an enriched position message to the message broker with custom headers.
     * <p>
     * This method uses the transaction outbox pattern to ensure reliable message delivery.
     * The position will be stored in a local outbox table before being published to the broker.
     *
     * @param position The enriched position to publish
     * @param headers  Additional message headers to include
     * @return A CompletableFuture that completes when the message has been successfully published
     * @throws MessageException If there is an error publishing the message
     */
    CompletableFuture<Void> publishPosition(Position position, Map<String, Object> headers) throws MessageException;

    /**
     * Publishes an enriched position message to the message broker with a specific correlation ID.
     * <p>
     * This method uses the transaction outbox pattern to ensure reliable message delivery.
     * The position will be stored in a local outbox table before being published to the broker.
     * The correlation ID is used for distributed tracing and message correlation across services.
     *
     * @param position      The enriched position to publish
     * @param correlationId The correlation ID for distributed tracing
     * @return A CompletableFuture that completes when the message has been successfully published
     * @throws MessageException If there is an error publishing the message
     */
    CompletableFuture<Void> publishPositionWithCorrelationId(Position position, String correlationId) throws MessageException;

    /**
     * Publishes an enriched position message to the message broker with a specific topic.
     * <p>
     * This method uses the transaction outbox pattern to ensure reliable message delivery.
     * The position will be stored in a local outbox table before being published to the broker.
     * The topic parameter allows publishing to different topics based on the message content or context.
     *
     * @param position The enriched position to publish
     * @param topic    The topic to publish the message to
     * @return A CompletableFuture that completes when the message has been successfully published
     * @throws MessageException If there is an error publishing the message
     */
    CompletableFuture<Void> publishPositionToTopic(Position position, String topic) throws MessageException;

    /**
     * Publishes an enriched position message to the message broker with a specific topic, correlation ID, and headers.
     * <p>
     * This method uses the transaction outbox pattern to ensure reliable message delivery.
     * The position will be stored in a local outbox table before being published to the broker.
     * This is the most flexible publishing method, allowing full customization of the message metadata.
     *
     * @param position      The enriched position to publish
     * @param topic         The topic to publish the message to
     * @param correlationId The correlation ID for distributed tracing
     * @param headers       Additional message headers to include
     * @return A CompletableFuture that completes when the message has been successfully published
     * @throws MessageException If there is an error publishing the message
     */
    CompletableFuture<Void> publishPosition(Position position, String topic, String correlationId, Map<String, Object> headers) throws MessageException;

    /**
     * Processes any pending messages in the outbox that haven't been published to the broker yet.
     * <p>
     * This method is typically called by a scheduled job to ensure that messages are eventually
     * delivered even if there was a temporary failure during the initial publish attempt.
     *
     * @return A CompletableFuture that completes when all pending messages have been processed
     * @throws MessageException If there is an error processing the outbox
     */
    CompletableFuture<Void> processOutbox() throws MessageException;

    /**
     * Checks if the producer is healthy and able to publish messages.
     * <p>
     * This method can be used by health checks to verify that the connection to the message broker is active.
     *
     * @return true if the producer is healthy, false otherwise
     */
    boolean isHealthy();

    /**
     * Closes the producer and releases any resources.
     * <p>
     * This method should be called when the producer is no longer needed to ensure proper cleanup.
     */
    void close();
}