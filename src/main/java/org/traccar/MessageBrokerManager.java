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
package org.traccar;

import com.google.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.MessageConsumer;
import org.traccar.messaging.MessageConsumerFactory;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHandler;
import org.traccar.messaging.MessageHeaders;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageProducerFactory;
import org.traccar.messaging.MessageSerializer;
import org.traccar.messaging.ProtobufMessageSerializer;
import org.traccar.messaging.JsonMessageSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Provides a unified interface for asynchronous message publishing and consumption
 * across Kafka and RabbitMQ message brokers. This component enables event-driven
 * communication between the monolithic Traccar server and extracted microservices,
 * supporting the transition to a distributed architecture while maintaining backward
 * compatibility.
 */
public class MessageBrokerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBrokerManager.class);

    private final Config config;
    private final MessageProducerFactory producerFactory;
    private final MessageConsumerFactory consumerFactory;
    
    private final Map<String, MessageProducer> producers = new ConcurrentHashMap<>();
    private final Map<String, MessageConsumer> consumers = new ConcurrentHashMap<>();
    private final Map<String, MessageSerializer> serializers = new ConcurrentHashMap<>();

    private enum BrokerType {
        KAFKA,
        RABBITMQ
    }

    /**
     * Constructs a new MessageBrokerManager with the specified dependencies.
     *
     * @param config The configuration provider
     * @param producerFactory Factory for creating message producers
     * @param consumerFactory Factory for creating message consumers
     */
    @Inject
    public MessageBrokerManager(
            Config config,
            MessageProducerFactory producerFactory,
            MessageConsumerFactory consumerFactory) {
        this.config = config;
        this.producerFactory = producerFactory;
        this.consumerFactory = consumerFactory;
        
        // Initialize default serializers
        serializers.put("protobuf", new ProtobufMessageSerializer());
        serializers.put("json", new JsonMessageSerializer());
    }

    /**
     * Gets the configured broker type (Kafka or RabbitMQ) from configuration.
     *
     * @return The configured broker type
     */
    private BrokerType getBrokerType() {
        String brokerType = config.getString(Keys.BROKER_TYPE, "kafka").toUpperCase();
        try {
            return BrokerType.valueOf(brokerType);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Invalid broker type: {}, defaulting to KAFKA", brokerType);
            return BrokerType.KAFKA;
        }
    }

    /**
     * Gets the appropriate serializer for the specified content type.
     *
     * @param contentType The content type (e.g., "protobuf", "json")
     * @return The message serializer for the content type
     * @throws IllegalArgumentException if no serializer is found for the content type
     */
    private MessageSerializer getSerializer(String contentType) {
        MessageSerializer serializer = serializers.get(contentType.toLowerCase());
        if (serializer == null) {
            throw new IllegalArgumentException("No serializer found for content type: " + contentType);
        }
        return serializer;
    }

    /**
     * Publishes a message to the specified topic.
     *
     * @param topic The topic to publish to
     * @param message The message to publish
     * @param contentType The content type for serialization (e.g., "protobuf", "json")
     * @param <T> The type of the message
     * @return A CompletableFuture that completes when the message is acknowledged by the broker
     */
    public <T> CompletableFuture<Void> publish(String topic, T message, String contentType) {
        return publish(topic, message, contentType, new HashMap<>());
    }

    /**
     * Publishes a message to the specified topic with custom headers.
     *
     * @param topic The topic to publish to
     * @param message The message to publish
     * @param contentType The content type for serialization (e.g., "protobuf", "json")
     * @param headers Custom headers to include with the message
     * @param <T> The type of the message
     * @return A CompletableFuture that completes when the message is acknowledged by the broker
     */
    public <T> CompletableFuture<Void> publish(String topic, T message, String contentType, Map<String, String> headers) {
        try {
            MessageProducer producer = getOrCreateProducer(topic);
            MessageSerializer serializer = getSerializer(contentType);
            
            // Add standard headers
            Map<String, String> messageHeaders = new HashMap<>(headers);
            messageHeaders.put(MessageHeaders.CONTENT_TYPE, contentType);
            messageHeaders.put(MessageHeaders.MESSAGE_ID, UUID.randomUUID().toString());
            
            // Add correlation ID if not present
            if (!messageHeaders.containsKey(MessageHeaders.CORRELATION_ID)) {
                messageHeaders.put(MessageHeaders.CORRELATION_ID, UUID.randomUUID().toString());
            }
            
            // Add timestamp
            messageHeaders.put(MessageHeaders.TIMESTAMP, String.valueOf(System.currentTimeMillis()));
            
            // Serialize the message
            byte[] payload = serializer.serialize(message);
            
            // Create message envelope
            MessageEnvelope envelope = new MessageEnvelope(payload, messageHeaders);
            
            // Publish the message
            return producer.send(topic, envelope);
        } catch (Exception e) {
            LOGGER.error("Failed to publish message to topic: {}", topic, e);
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    /**
     * Subscribes to a topic with the specified message handler.
     *
     * @param topic The topic to subscribe to
     * @param groupId The consumer group ID
     * @param contentType The expected content type for deserialization
     * @param messageType The class of the message type
     * @param handler The handler to process received messages
     * @param <T> The type of the message
     * @return The created message consumer
     */
    public <T> MessageConsumer subscribe(String topic, String groupId, String contentType, 
                                        Class<T> messageType, Consumer<T> handler) {
        try {
            MessageConsumer consumer = getOrCreateConsumer(topic, groupId);
            MessageSerializer serializer = getSerializer(contentType);
            
            // Create a message handler that deserializes the message and passes it to the handler
            MessageHandler<MessageEnvelope> messageHandler = envelope -> {
                try {
                    // Check content type
                    String receivedContentType = envelope.getHeaders().getOrDefault(
                            MessageHeaders.CONTENT_TYPE, contentType);
                    if (!contentType.equalsIgnoreCase(receivedContentType)) {
                        LOGGER.warn("Received message with unexpected content type: {}, expected: {}",
                                receivedContentType, contentType);
                    }
                    
                    // Deserialize the message
                    T message = serializer.deserialize(envelope.getPayload(), messageType);
                    
                    // Process the message
                    handler.accept(message);
                    
                    // Acknowledge the message
                    return CompletableFuture.completedFuture(null);
                } catch (Exception e) {
                    LOGGER.error("Failed to process message from topic: {}", topic, e);
                    CompletableFuture<Void> future = new CompletableFuture<>();
                    future.completeExceptionally(e);
                    return future;
                }
            };
            
            // Subscribe to the topic
            consumer.subscribe(topic, messageHandler);
            
            return consumer;
        } catch (Exception e) {
            LOGGER.error("Failed to subscribe to topic: {}", topic, e);
            throw new RuntimeException("Failed to subscribe to topic: " + topic, e);
        }
    }

    /**
     * Gets or creates a message producer for the specified topic.
     *
     * @param topic The topic to produce messages to
     * @return The message producer
     */
    private MessageProducer getOrCreateProducer(String topic) {
        return producers.computeIfAbsent(topic, t -> {
            BrokerType brokerType = getBrokerType();
            LOGGER.info("Creating {} producer for topic: {}", brokerType, topic);
            return producerFactory.createProducer(brokerType.name().toLowerCase());
        });
    }

    /**
     * Gets or creates a message consumer for the specified topic and group ID.
     *
     * @param topic The topic to consume messages from
     * @param groupId The consumer group ID
     * @return The message consumer
     */
    private MessageConsumer getOrCreateConsumer(String topic, String groupId) {
        String key = topic + "-" + groupId;
        return consumers.computeIfAbsent(key, k -> {
            BrokerType brokerType = getBrokerType();
            LOGGER.info("Creating {} consumer for topic: {}, group: {}", brokerType, topic, groupId);
            return consumerFactory.createConsumer(brokerType.name().toLowerCase(), groupId);
        });
    }

    /**
     * Registers a custom message serializer for a specific content type.
     *
     * @param contentType The content type
     * @param serializer The message serializer
     */
    public void registerSerializer(String contentType, MessageSerializer serializer) {
        serializers.put(contentType.toLowerCase(), serializer);
    }

    /**
     * Closes all producers and consumers managed by this broker manager.
     * This method should be called during application shutdown to release resources.
     */
    public void close() {
        LOGGER.info("Closing message broker manager resources");
        
        // Close all producers
        for (Map.Entry<String, MessageProducer> entry : producers.entrySet()) {
            try {
                LOGGER.debug("Closing producer for topic: {}", entry.getKey());
                entry.getValue().close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close producer for topic: {}", entry.getKey(), e);
            }
        }
        producers.clear();
        
        // Close all consumers
        for (Map.Entry<String, MessageConsumer> entry : consumers.entrySet()) {
            try {
                LOGGER.debug("Closing consumer for topic: {}", entry.getKey());
                entry.getValue().close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close consumer for topic: {}", entry.getKey(), e);
            }
        }
        consumers.clear();
    }
}