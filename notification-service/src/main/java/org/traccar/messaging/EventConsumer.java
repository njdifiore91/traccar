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

import org.traccar.model.Event;

import java.util.Set;
import java.util.function.Consumer;

/**
 * Interface for consuming event messages from the message broker.
 * Provides methods for subscribing to event topics, processing events,
 * and handling errors. This interface is essential for the Notification Service
 * to receive events that trigger notifications across various channels.
 */
public interface EventConsumer {

    /**
     * Callback interface for successful event processing.
     */
    interface SuccessCallback {
        /**
         * Called when an event is successfully processed.
         *
         * @param event The processed event
         * @param correlationId The correlation ID for distributed tracing
         */
        void onSuccess(Event event, String correlationId);
    }

    /**
     * Callback interface for failed event processing.
     */
    interface ErrorCallback {
        /**
         * Called when an event processing fails.
         *
         * @param event The event that failed processing
         * @param exception The exception that occurred
         * @param correlationId The correlation ID for distributed tracing
         */
        void onError(Event event, Throwable exception, String correlationId);
    }

    /**
     * Starts consuming events from the configured topics.
     * This method should initialize the connection to the message broker
     * and begin listening for events.
     */
    void start();

    /**
     * Stops consuming events and closes connections to the message broker.
     * This method should perform any necessary cleanup.
     */
    void stop();

    /**
     * Subscribes to events of specific types.
     * 
     * @param eventTypes Set of event types to subscribe to
     * @return This consumer instance for method chaining
     */
    EventConsumer subscribe(Set<String> eventTypes);

    /**
     * Subscribes to all event types.
     * 
     * @return This consumer instance for method chaining
     */
    EventConsumer subscribeToAll();

    /**
     * Sets the consumer group ID for the message broker.
     * This is used to manage consumer groups and message partitioning.
     * 
     * @param groupId The consumer group ID
     * @return This consumer instance for method chaining
     */
    EventConsumer withConsumerGroup(String groupId);

    /**
     * Sets the success callback for event processing.
     * 
     * @param callback The callback to invoke when an event is successfully processed
     * @return This consumer instance for method chaining
     */
    EventConsumer onSuccess(SuccessCallback callback);

    /**
     * Sets the error callback for event processing.
     * 
     * @param callback The callback to invoke when event processing fails
     * @return This consumer instance for method chaining
     */
    EventConsumer onError(ErrorCallback callback);

    /**
     * Sets a filter for events based on a predicate.
     * 
     * @param filter A predicate that returns true for events that should be processed
     * @return This consumer instance for method chaining
     */
    EventConsumer withFilter(java.util.function.Predicate<Event> filter);

    /**
     * Sets the maximum number of concurrent messages to process.
     * 
     * @param concurrency The maximum number of concurrent messages
     * @return This consumer instance for method chaining
     */
    EventConsumer withConcurrency(int concurrency);

    /**
     * Enables or disables automatic acknowledgment of messages.
     * When disabled, messages must be manually acknowledged after processing.
     * 
     * @param autoAck Whether to automatically acknowledge messages
     * @return This consumer instance for method chaining
     */
    EventConsumer withAutoAck(boolean autoAck);

    /**
     * Manually acknowledges a message as successfully processed.
     * This should only be used when auto-acknowledgment is disabled.
     * 
     * @param event The event to acknowledge
     * @param correlationId The correlation ID for distributed tracing
     */
    void acknowledge(Event event, String correlationId);

    /**
     * Rejects a message, optionally requeuing it for later processing.
     * This should only be used when auto-acknowledgment is disabled.
     * 
     * @param event The event to reject
     * @param requeue Whether to requeue the message for later processing
     * @param correlationId The correlation ID for distributed tracing
     */
    void reject(Event event, boolean requeue, String correlationId);

    /**
     * Sets the dead letter queue for failed messages.
     * Messages that fail processing after the maximum number of retries
     * will be sent to this queue.
     * 
     * @param deadLetterQueue The name of the dead letter queue
     * @return This consumer instance for method chaining
     */
    EventConsumer withDeadLetterQueue(String deadLetterQueue);

    /**
     * Sets the maximum number of retry attempts for failed messages.
     * 
     * @param maxRetries The maximum number of retry attempts
     * @return This consumer instance for method chaining
     */
    EventConsumer withMaxRetries(int maxRetries);

    /**
     * Sets the retry backoff strategy for failed messages.
     * 
     * @param initialBackoff The initial backoff time in milliseconds
     * @param maxBackoff The maximum backoff time in milliseconds
     * @param backoffMultiplier The multiplier for exponential backoff
     * @return This consumer instance for method chaining
     */
    EventConsumer withRetryBackoff(long initialBackoff, long maxBackoff, double backoffMultiplier);

    /**
     * Sets the correlation ID header name for distributed tracing.
     * This header will be used to propagate the correlation ID across services.
     * 
     * @param headerName The name of the correlation ID header
     * @return This consumer instance for method chaining
     */
    EventConsumer withCorrelationIdHeader(String headerName);

    /**
     * Sets a consumer for handling raw message metadata.
     * This can be used for custom processing of message headers or properties.
     * 
     * @param metadataConsumer A consumer for message metadata
     * @return This consumer instance for method chaining
     */
    EventConsumer withMetadataHandler(Consumer<Object> metadataConsumer);
}