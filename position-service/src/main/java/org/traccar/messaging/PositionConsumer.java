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
import java.util.function.Consumer;

/**
 * Interface defining the contract for consuming raw position messages from the message broker
 * in the Position Processing Service. Provides methods for subscribing to topics, processing
 * messages, and handling errors.
 * <p>
 * This interface supports both Kafka and RabbitMQ implementations and implements consumer groups
 * for load distribution. It ensures at-least-once delivery guarantee with idempotent processing
 * and supports distributed tracing for message correlation.
 */
public interface PositionConsumer extends AutoCloseable {

    /**
     * Subscribes to the specified position topic with default consumer group settings.
     * 
     * @param topic The topic to subscribe to
     * @return This consumer instance for method chaining
     */
    PositionConsumer subscribe(String topic);

    /**
     * Subscribes to the specified position topic with a specific consumer group.
     * This allows for load distribution across multiple consumer instances.
     * 
     * @param topic The topic to subscribe to
     * @param consumerGroup The consumer group identifier
     * @return This consumer instance for method chaining
     */
    PositionConsumer subscribe(String topic, String consumerGroup);

    /**
     * Subscribes to the specified position topic with a specific consumer group and additional options.
     * 
     * @param topic The topic to subscribe to
     * @param consumerGroup The consumer group identifier
     * @param options Additional consumer options as key-value pairs
     * @return This consumer instance for method chaining
     */
    PositionConsumer subscribe(String topic, String consumerGroup, Map<String, Object> options);

    /**
     * Registers a callback function to process position messages.
     * The callback will be invoked for each message received from the subscribed topics.
     * 
     * @param callback The callback function to process position messages
     * @return This consumer instance for method chaining
     */
    PositionConsumer onMessage(Consumer<Position> callback);

    /**
     * Registers a callback function to process position messages with their headers.
     * The callback will be invoked for each message received from the subscribed topics.
     * 
     * @param callback The callback function to process position messages with headers
     * @return This consumer instance for method chaining
     */
    PositionConsumer onMessage(java.util.function.BiConsumer<Position, Map<String, Object>> callback);

    /**
     * Registers a callback function to handle errors that occur during message consumption.
     * 
     * @param errorHandler The callback function to handle errors
     * @return This consumer instance for method chaining
     */
    PositionConsumer onError(Consumer<Throwable> errorHandler);

    /**
     * Starts consuming messages from the subscribed topics.
     * This method should be called after all subscriptions and callbacks are registered.
     * 
     * @return A CompletableFuture that completes when the consumer is started
     */
    CompletableFuture<Void> start();

    /**
     * Pauses message consumption from the subscribed topics.
     * This method can be used to temporarily stop processing messages without unsubscribing.
     * 
     * @return A CompletableFuture that completes when the consumer is paused
     */
    CompletableFuture<Void> pause();

    /**
     * Resumes message consumption after it has been paused.
     * 
     * @return A CompletableFuture that completes when the consumer is resumed
     */
    CompletableFuture<Void> resume();

    /**
     * Acknowledges the successful processing of a position message.
     * This method should be called after a message has been successfully processed
     * to prevent redelivery.
     * 
     * @param position The position message to acknowledge
     * @return A CompletableFuture that completes when the acknowledgment is processed
     */
    CompletableFuture<Void> acknowledge(Position position);

    /**
     * Acknowledges the successful processing of a position message with a specific message ID.
     * This method should be called after a message has been successfully processed
     * to prevent redelivery.
     * 
     * @param messageId The ID of the message to acknowledge
     * @return A CompletableFuture that completes when the acknowledgment is processed
     */
    CompletableFuture<Void> acknowledge(String messageId);

    /**
     * Rejects a position message, indicating that it could not be processed.
     * Depending on the implementation, this may cause the message to be redelivered,
     * sent to a dead-letter queue, or discarded.
     * 
     * @param position The position message to reject
     * @param requeue Whether the message should be requeued for redelivery
     * @return A CompletableFuture that completes when the rejection is processed
     */
    CompletableFuture<Void> reject(Position position, boolean requeue);

    /**
     * Rejects a position message with a specific message ID, indicating that it could not be processed.
     * Depending on the implementation, this may cause the message to be redelivered,
     * sent to a dead-letter queue, or discarded.
     * 
     * @param messageId The ID of the message to reject
     * @param requeue Whether the message should be requeued for redelivery
     * @return A CompletableFuture that completes when the rejection is processed
     */
    CompletableFuture<Void> reject(String messageId, boolean requeue);

    /**
     * Stops consuming messages and closes all resources associated with this consumer.
     * This method should be called when the consumer is no longer needed.
     * 
     * @throws Exception If an error occurs while closing the consumer
     */
    @Override
    void close() throws Exception;
}