/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.model;

import java.util.Map;

/**
 * Interface defining the contract for message envelope integration.
 * Provides methods for accessing and manipulating message metadata,
 * routing information, and tracing context for cross-service communication.
 */
public interface MessageEnvelope {

    /**
     * Gets the unique identifier for this message.
     *
     * @return The message ID
     */
    String getMessageId();

    /**
     * Sets the unique identifier for this message.
     *
     * @param messageId The message ID
     */
    void setMessageId(String messageId);

    /**
     * Gets the timestamp when this message was created.
     *
     * @return The timestamp in milliseconds since epoch
     */
    long getTimestamp();

    /**
     * Sets the timestamp for this message.
     *
     * @param timestamp The timestamp in milliseconds since epoch
     */
    void setTimestamp(long timestamp);

    /**
     * Gets the time-to-live for this message in milliseconds.
     *
     * @return The TTL in milliseconds, or null if not set
     */
    Integer getTtl();

    /**
     * Sets the time-to-live for this message in milliseconds.
     *
     * @param ttl The TTL in milliseconds
     */
    void setTtl(Integer ttl);

    // Message routing metadata methods

    /**
     * Gets the topic name for Kafka-based messaging.
     *
     * @return The topic name
     */
    String getTopic();

    /**
     * Sets the topic name for Kafka-based messaging.
     *
     * @param topic The topic name
     */
    void setTopic(String topic);

    /**
     * Gets the partition for Kafka-based messaging.
     *
     * @return The partition number, or null if not set
     */
    Integer getPartition();

    /**
     * Sets the partition for Kafka-based messaging.
     *
     * @param partition The partition number
     */
    void setPartition(Integer partition);

    /**
     * Gets the routing key for RabbitMQ-based messaging.
     *
     * @return The routing key
     */
    String getRoutingKey();

    /**
     * Sets the routing key for RabbitMQ-based messaging.
     *
     * @param routingKey The routing key
     */
    void setRoutingKey(String routingKey);

    /**
     * Gets the exchange name for RabbitMQ-based messaging.
     *
     * @return The exchange name
     */
    String getExchange();

    /**
     * Sets the exchange name for RabbitMQ-based messaging.
     *
     * @param exchange The exchange name
     */
    void setExchange(String exchange);

    /**
     * Gets the headers for this message.
     *
     * @return The headers as a map
     */
    Map<String, String> getHeaders();

    /**
     * Sets the headers for this message.
     *
     * @param headers The headers as a map
     */
    void setHeaders(Map<String, String> headers);

    /**
     * Adds a header to this message.
     *
     * @param key The header key
     * @param value The header value
     */
    void addHeader(String key, String value);

    // Tracing context methods

    /**
     * Gets the trace ID for distributed tracing.
     *
     * @return The trace ID
     */
    String getTraceId();

    /**
     * Sets the trace ID for distributed tracing.
     *
     * @param traceId The trace ID
     */
    void setTraceId(String traceId);

    /**
     * Gets the span ID for distributed tracing.
     *
     * @return The span ID
     */
    String getSpanId();

    /**
     * Sets the span ID for distributed tracing.
     *
     * @param spanId The span ID
     */
    void setSpanId(String spanId);

    /**
     * Gets the parent span ID for distributed tracing.
     *
     * @return The parent span ID
     */
    String getParentSpanId();

    /**
     * Sets the parent span ID for distributed tracing.
     *
     * @param parentSpanId The parent span ID
     */
    void setParentSpanId(String parentSpanId);

    /**
     * Gets the baggage items for distributed tracing.
     * Baggage items are key-value pairs that are propagated through the distributed system.
     *
     * @return The baggage items as a map
     */
    Map<String, String> getBaggage();

    /**
     * Sets the baggage items for distributed tracing.
     *
     * @param baggage The baggage items as a map
     */
    void setBaggage(Map<String, String> baggage);

    /**
     * Adds a baggage item for distributed tracing.
     *
     * @param key The baggage item key
     * @param value The baggage item value
     */
    void addBaggageItem(String key, String value);
}