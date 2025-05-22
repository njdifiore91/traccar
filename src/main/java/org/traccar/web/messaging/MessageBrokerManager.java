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
package org.traccar.web.messaging;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Manages message broker connections for WebSocket multiplexing.
 * Supports both Kafka and RabbitMQ message brokers.
 */
public class MessageBrokerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBrokerManager.class);

    private final Config config;
    private final MeterRegistry meterRegistry;
    private final ScheduledExecutorService executorService;
    private final Map<String, MessageBroker> brokers;
    private final Map<String, Map<String, Consumer<String>>> topicSubscribers;

    public MessageBrokerManager(Config config, MeterRegistry meterRegistry) {
        this.config = config;
        this.meterRegistry = meterRegistry;
        this.executorService = Executors.newScheduledThreadPool(2);
        this.brokers = new HashMap<>();
        this.topicSubscribers = new ConcurrentHashMap<>();
        
        // Initialize message brokers based on configuration
        initializeBrokers();
    }

    /**
     * Initialize message brokers based on configuration.
     */
    private void initializeBrokers() {
        String brokerType = config.getString("web.messageBroker.type", "kafka").toLowerCase();
        
        if ("kafka".equals(brokerType)) {
            brokers.put("default", new KafkaMessageBroker(config, meterRegistry));
            LOGGER.info("Initialized Kafka message broker");
        } else if ("rabbitmq".equals(brokerType)) {
            brokers.put("default", new RabbitMQMessageBroker(config, meterRegistry));
            LOGGER.info("Initialized RabbitMQ message broker");
        } else {
            LOGGER.warn("Unsupported message broker type: {}", brokerType);
        }
    }

    /**
     * Connect to all configured message brokers.
     */
    public void connect() {
        for (Map.Entry<String, MessageBroker> entry : brokers.entrySet()) {
            try {
                entry.getValue().connect();
                LOGGER.info("Connected to message broker: {}", entry.getKey());
            } catch (Exception e) {
                LOGGER.error("Failed to connect to message broker: {}", entry.getKey(), e);
            }
        }
        
        // Start health check for broker connections
        int healthCheckInterval = config.getInteger("web.messageBroker.healthCheckInterval", 30);
        executorService.scheduleAtFixedRate(this::checkBrokerConnections, 
                healthCheckInterval, healthCheckInterval, TimeUnit.SECONDS);
    }

    /**
     * Disconnect from all message brokers.
     */
    public void disconnect() {
        executorService.shutdown();
        
        for (Map.Entry<String, MessageBroker> entry : brokers.entrySet()) {
            try {
                entry.getValue().disconnect();
                LOGGER.info("Disconnected from message broker: {}", entry.getKey());
            } catch (Exception e) {
                LOGGER.error("Error disconnecting from message broker: {}", entry.getKey(), e);
            }
        }
    }

    /**
     * Subscribe to a topic on the default message broker.
     *
     * @param topic the topic to subscribe to
     * @param subscriberId a unique identifier for the subscriber
     * @param messageHandler the handler for received messages
     * @return a CompletableFuture that completes when the subscription is established
     */
    public CompletableFuture<Void> subscribe(String topic, String subscriberId, Consumer<String> messageHandler) {
        return subscribe("default", topic, subscriberId, messageHandler);
    }

    /**
     * Subscribe to a topic on a specific message broker.
     *
     * @param brokerName the name of the message broker
     * @param topic the topic to subscribe to
     * @param subscriberId a unique identifier for the subscriber
     * @param messageHandler the handler for received messages
     * @return a CompletableFuture that completes when the subscription is established
     */
    public CompletableFuture<Void> subscribe(String brokerName, String topic, String subscriberId, Consumer<String> messageHandler) {
        MessageBroker broker = brokers.get(brokerName);
        if (broker == null) {
            LOGGER.error("Message broker not found: {}", brokerName);
            return CompletableFuture.failedFuture(new IllegalArgumentException("Message broker not found: " + brokerName));
        }
        
        // Register the subscriber
        topicSubscribers.computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                .put(subscriberId, messageHandler);
        
        // Subscribe to the topic if this is the first subscriber
        if (topicSubscribers.get(topic).size() == 1) {
            return broker.subscribe(topic, message -> {
                // Distribute the message to all subscribers for this topic
                Timer timer = meterRegistry.timer("messageBroker.messageDistribution", "topic", topic);
                timer.record(() -> {
                    topicSubscribers.getOrDefault(topic, Map.of()).forEach((id, handler) -> {
                        try {
                            handler.accept(message);
                        } catch (Exception e) {
                            LOGGER.error("Error handling message for subscriber {}", id, e);
                        }
                    });
                });
            });
        }
        
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Unsubscribe from a topic on the default message broker.
     *
     * @param topic the topic to unsubscribe from
     * @param subscriberId the unique identifier of the subscriber
     */
    public void unsubscribe(String topic, String subscriberId) {
        unsubscribe("default", topic, subscriberId);
    }

    /**
     * Unsubscribe from a topic on a specific message broker.
     *
     * @param brokerName the name of the message broker
     * @param topic the topic to unsubscribe from
     * @param subscriberId the unique identifier of the subscriber
     */
    public void unsubscribe(String brokerName, String topic, String subscriberId) {
        Map<String, Consumer<String>> subscribers = topicSubscribers.get(topic);
        if (subscribers != null) {
            subscribers.remove(subscriberId);
            
            // If there are no more subscribers, unsubscribe from the topic
            if (subscribers.isEmpty()) {
                MessageBroker broker = brokers.get(brokerName);
                if (broker != null) {
                    broker.unsubscribe(topic);
                }
                topicSubscribers.remove(topic);
            }
        }
    }

    /**
     * Publish a message to a topic on the default message broker.
     *
     * @param topic the topic to publish to
     * @param message the message to publish
     * @return a CompletableFuture that completes when the message is published
     */
    public CompletableFuture<Void> publish(String topic, String message) {
        return publish("default", topic, message);
    }

    /**
     * Publish a message to a topic on a specific message broker.
     *
     * @param brokerName the name of the message broker
     * @param topic the topic to publish to
     * @param message the message to publish
     * @return a CompletableFuture that completes when the message is published
     */
    public CompletableFuture<Void> publish(String brokerName, String topic, String message) {
        MessageBroker broker = brokers.get(brokerName);
        if (broker == null) {
            LOGGER.error("Message broker not found: {}", brokerName);
            return CompletableFuture.failedFuture(new IllegalArgumentException("Message broker not found: " + brokerName));
        }
        
        Timer timer = meterRegistry.timer("messageBroker.publish", "topic", topic, "broker", brokerName);
        return timer.recordCallable(() -> broker.publish(topic, message));
    }

    /**
     * Check the health of all broker connections and attempt to reconnect if necessary.
     */
    private void checkBrokerConnections() {
        for (Map.Entry<String, MessageBroker> entry : brokers.entrySet()) {
            try {
                if (!entry.getValue().isConnected()) {
                    LOGGER.warn("Message broker connection lost: {}, attempting to reconnect", entry.getKey());
                    entry.getValue().connect();
                    
                    // Resubscribe to all topics
                    for (String topic : topicSubscribers.keySet()) {
                        if (!topicSubscribers.get(topic).isEmpty()) {
                            entry.getValue().subscribe(topic, message -> {
                                topicSubscribers.getOrDefault(topic, Map.of()).forEach((id, handler) -> {
                                    try {
                                        handler.accept(message);
                                    } catch (Exception e) {
                                        LOGGER.error("Error handling message for subscriber {}", id, e);
                                    }
                                });
                            });
                        }
                    }
                    
                    LOGGER.info("Reconnected to message broker: {}", entry.getKey());
                }
            } catch (Exception e) {
                LOGGER.error("Error checking message broker connection: {}", entry.getKey(), e);
            }
        }
    }

    /**
     * Interface for message broker implementations.
     */
    public interface MessageBroker {
        void connect() throws Exception;
        void disconnect() throws Exception;
        boolean isConnected();
        CompletableFuture<Void> subscribe(String topic, Consumer<String> messageHandler);
        void unsubscribe(String topic);
        CompletableFuture<Void> publish(String topic, String message);
    }

    /**
     * Kafka message broker implementation.
     */
    private static class KafkaMessageBroker implements MessageBroker {
        private final Config config;
        private final MeterRegistry meterRegistry;
        private boolean connected;

        public KafkaMessageBroker(Config config, MeterRegistry meterRegistry) {
            this.config = config;
            this.meterRegistry = meterRegistry;
            this.connected = false;
        }

        @Override
        public void connect() throws Exception {
            // TODO: Implement Kafka connection
            // This would use KafkaProducer and KafkaConsumer from the Kafka client library
            connected = true;
            LOGGER.info("Connected to Kafka broker");
        }

        @Override
        public void disconnect() throws Exception {
            // TODO: Implement Kafka disconnection
            connected = false;
            LOGGER.info("Disconnected from Kafka broker");
        }

        @Override
        public boolean isConnected() {
            return connected;
        }

        @Override
        public CompletableFuture<Void> subscribe(String topic, Consumer<String> messageHandler) {
            // TODO: Implement Kafka subscription
            LOGGER.info("Subscribed to Kafka topic: {}", topic);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void unsubscribe(String topic) {
            // TODO: Implement Kafka unsubscription
            LOGGER.info("Unsubscribed from Kafka topic: {}", topic);
        }

        @Override
        public CompletableFuture<Void> publish(String topic, String message) {
            // TODO: Implement Kafka message publishing
            LOGGER.debug("Published message to Kafka topic: {}", topic);
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * RabbitMQ message broker implementation.
     */
    private static class RabbitMQMessageBroker implements MessageBroker {
        private final Config config;
        private final MeterRegistry meterRegistry;
        private boolean connected;

        public RabbitMQMessageBroker(Config config, MeterRegistry meterRegistry) {
            this.config = config;
            this.meterRegistry = meterRegistry;
            this.connected = false;
        }

        @Override
        public void connect() throws Exception {
            // TODO: Implement RabbitMQ connection
            // This would use ConnectionFactory from the RabbitMQ client library
            connected = true;
            LOGGER.info("Connected to RabbitMQ broker");
        }

        @Override
        public void disconnect() throws Exception {
            // TODO: Implement RabbitMQ disconnection
            connected = false;
            LOGGER.info("Disconnected from RabbitMQ broker");
        }

        @Override
        public boolean isConnected() {
            return connected;
        }

        @Override
        public CompletableFuture<Void> subscribe(String topic, Consumer<String> messageHandler) {
            // TODO: Implement RabbitMQ subscription
            LOGGER.info("Subscribed to RabbitMQ topic: {}", topic);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void unsubscribe(String topic) {
            // TODO: Implement RabbitMQ unsubscription
            LOGGER.info("Unsubscribed from RabbitMQ topic: {}", topic);
        }

        @Override
        public CompletableFuture<Void> publish(String topic, String message) {
            // TODO: Implement RabbitMQ message publishing
            LOGGER.debug("Published message to RabbitMQ topic: {}", topic);
            return CompletableFuture.completedFuture(null);
        }
    }
}