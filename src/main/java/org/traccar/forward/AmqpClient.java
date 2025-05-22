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
package org.traccar.forward;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Client for interacting with RabbitMQ message broker.
 * Provides methods for publishing messages and consuming messages from a queue.
 */
public class AmqpClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmqpClient.class);

    private final ConnectionFactory factory;
    private final String exchange;
    private final String routingKey;
    private Connection connection;
    private Channel channel;
    private String consumerTag;
    private MessageHandler messageHandler;

    /**
     * Interface for handling received messages.
     */
    public interface MessageHandler {
        /**
         * Handles a received message.
         *
         * @param message    The message body as a string
         * @param properties The AMQP message properties
         */
        void handle(String message, AMQP.BasicProperties properties);
    }

    /**
     * Creates a new AMQP client with the specified connection parameters.
     *
     * @param connectionUrl The URL for connecting to the RabbitMQ server
     * @param exchange      The exchange to publish messages to
     * @param routingKey    The routing key for messages
     */
    public AmqpClient(String connectionUrl, String exchange, String routingKey) {
        this.factory = new ConnectionFactory();
        this.factory.setUri(connectionUrl);
        this.exchange = exchange;
        this.routingKey = routingKey;
        initialize();
    }

    /**
     * Initializes the connection and channel to the RabbitMQ server.
     */
    private void initialize() {
        try {
            this.connection = factory.newConnection();
            this.channel = connection.createChannel();
            
            // Declare the exchange if it doesn't exist
            channel.exchangeDeclare(exchange, "topic", true);
            
            // Declare a queue for the routing key if it doesn't exist
            channel.queueDeclare(routingKey, true, false, false, null);
            
            // Bind the queue to the exchange with the routing key
            channel.queueBind(routingKey, exchange, routingKey);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize AMQP connection", e);
            throw new RuntimeException("Failed to initialize AMQP connection", e);
        }
    }

    /**
     * Publishes a message to the exchange with the configured routing key.
     *
     * @param message The message to publish
     * @throws IOException If an error occurs while publishing the message
     */
    public void publishMessage(String message) throws IOException {
        publishMessage(message, new HashMap<>(), null);
    }

    /**
     * Publishes a message to the exchange with the configured routing key and headers.
     *
     * @param message The message to publish
     * @param headers Additional headers to include with the message
     * @param correlationId Optional correlation ID for the message
     * @throws IOException If an error occurs while publishing the message
     */
    public void publishMessage(String message, Map<String, String> headers, String correlationId) throws IOException {
        ensureConnection();
        
        // Convert string headers to AMQP header format
        Map<String, Object> amqpHeaders = new HashMap<>();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            amqpHeaders.put(entry.getKey(), entry.getValue());
        }
        
        // Build message properties
        AMQP.BasicProperties.Builder propertiesBuilder = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .deliveryMode(2) // persistent
                .headers(amqpHeaders);
        
        if (correlationId != null) {
            propertiesBuilder.correlationId(correlationId);
        }
        
        // Publish the message
        channel.basicPublish(
                exchange,
                routingKey,
                propertiesBuilder.build(),
                message.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Sets the message handler for consuming messages.
     *
     * @param messageHandler The handler for received messages
     */
    public void setMessageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    /**
     * Starts consuming messages from the queue with the specified consumer group.
     *
     * @param consumerGroup The consumer group ID
     */
    public void startConsuming(String consumerGroup) {
        if (messageHandler == null) {
            throw new IllegalStateException("Message handler must be set before starting consumption");
        }
        
        try {
            ensureConnection();
            
            // Declare a queue for the consumer group if it doesn't exist
            String queueName = routingKey + "." + consumerGroup;
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, exchange, routingKey);
            
            // Start consuming messages
            consumerTag = channel.basicConsume(queueName, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) {
                    String message = new String(body, StandardCharsets.UTF_8);
                    try {
                        messageHandler.handle(message, properties);
                    } catch (Exception e) {
                        LOGGER.error("Error handling AMQP message", e);
                    }
                }
            });
        } catch (IOException e) {
            LOGGER.error("Failed to start consuming messages", e);
            throw new RuntimeException("Failed to start consuming messages", e);
        }
    }

    /**
     * Stops consuming messages.
     */
    public void stopConsuming() {
        if (consumerTag != null && channel != null && channel.isOpen()) {
            try {
                channel.basicCancel(consumerTag);
                consumerTag = null;
            } catch (IOException e) {
                LOGGER.error("Failed to stop consuming messages", e);
            }
        }
    }

    /**
     * Ensures that the connection and channel are open, reconnecting if necessary.
     *
     * @throws IOException If an error occurs while reconnecting
     */
    private void ensureConnection() throws IOException {
        if (connection == null || !connection.isOpen() || channel == null || !channel.isOpen()) {
            try {
                if (channel != null) {
                    try {
                        channel.close();
                    } catch (Exception e) {
                        LOGGER.warn("Error closing channel", e);
                    }
                }
                
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Exception e) {
                        LOGGER.warn("Error closing connection", e);
                    }
                }
                
                initialize();
            } catch (Exception e) {
                LOGGER.error("Failed to reconnect to AMQP server", e);
                throw new IOException("Failed to reconnect to AMQP server", e);
            }
        }
    }

    /**
     * Closes the connection and channel to the RabbitMQ server.
     */
    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } catch (IOException | TimeoutException e) {
            LOGGER.warn("Error closing channel", e);
        } finally {
            channel = null;
        }
        
        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (IOException e) {
            LOGGER.warn("Error closing connection", e);
        } finally {
            connection = null;
        }
    }
}