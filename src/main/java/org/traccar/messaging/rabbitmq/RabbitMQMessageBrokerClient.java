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
package org.traccar.messaging.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.MessageBrokerClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RabbitMQ implementation of the MessageBrokerClient interface.
 */
public class RabbitMQMessageBrokerClient implements MessageBrokerClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQMessageBrokerClient.class);

    private final Connection connection;
    private final Channel channel;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean connected;
    private final String clientId;

    public RabbitMQMessageBrokerClient(Config config) {
        this.objectMapper = new ObjectMapper();
        this.connected = new AtomicBoolean(false);
        this.clientId = "traccar-" + UUID.randomUUID();

        try {
            String host = config.getString(Keys.PROCESSING_REMOTE_RABBITMQ_HOST.getKey(), "localhost");
            int port = config.getInteger(Keys.PROCESSING_REMOTE_RABBITMQ_PORT.getKey(), 5672);
            String username = config.getString(Keys.PROCESSING_REMOTE_RABBITMQ_USERNAME.getKey());
            String password = config.getString(Keys.PROCESSING_REMOTE_RABBITMQ_PASSWORD.getKey());
            String virtualHost = config.getString(Keys.PROCESSING_REMOTE_RABBITMQ_VIRTUAL_HOST.getKey(), "/");

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            if (username != null && !username.isEmpty()) {
                factory.setUsername(username);
            }
            if (password != null && !password.isEmpty()) {
                factory.setPassword(password);
            }
            factory.setVirtualHost(virtualHost);
            factory.setAutomaticRecoveryEnabled(true);

            this.connection = factory.newConnection(clientId);
            this.channel = connection.createChannel();
            this.connected.set(true);

            LOGGER.info("RabbitMQ message broker client initialized with host: {}:{}", host, port);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize RabbitMQ message broker client", e);
            throw new RuntimeException("Failed to initialize RabbitMQ client", e);
        }
    }

    @Override
    public <T> CompletableFuture<Void> publish(String topic, T message) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        try {
            // Ensure the exchange exists (topic in RabbitMQ is an exchange type)
            channel.exchangeDeclare(topic, "topic", true);

            String messageJson = objectMapper.writeValueAsString(message);
            String routingKey = message.getClass().getSimpleName().toLowerCase();

            channel.basicPublish(
                    topic,
                    routingKey,
                    new AMQP.BasicProperties.Builder()
                            .contentType("application/json")
                            .build(),
                    messageJson.getBytes(StandardCharsets.UTF_8));

            LOGGER.debug("Published message to exchange {} with routing key {}", topic, routingKey);
            future.complete(null);
        } catch (Exception e) {
            LOGGER.error("Failed to publish message to exchange {}", topic, e);
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public <T> CompletableFuture<Void> subscribe(String topic, MessageCallback<T> callback, Class<T> messageType) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        try {
            // Ensure the exchange exists
            channel.exchangeDeclare(topic, "topic", true);

            // Create a queue with a generated name
            String queueName = channel.queueDeclare().getQueue();

            // Bind the queue to the exchange with a routing key
            String routingKey = messageType.getSimpleName().toLowerCase();
            channel.queueBind(queueName, topic, routingKey);

            // Create a consumer that processes messages
            channel.basicConsume(queueName, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    try {
                        String messageJson = new String(body, StandardCharsets.UTF_8);
                        T message = objectMapper.readValue(messageJson, messageType);
                        callback.onMessage(message);
                    } catch (Exception e) {
                        LOGGER.error("Error processing message from queue {}", queueName, e);
                    }
                }
            });

            LOGGER.info("Subscribed to exchange {} with routing key {}", topic, routingKey);
            future.complete(null);
        } catch (Exception e) {
            LOGGER.error("Failed to subscribe to exchange {}", topic, e);
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public CompletableFuture<Void> close() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        try {
            connected.set(false);
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
            LOGGER.info("RabbitMQ message broker client closed");
            future.complete(null);
        } catch (IOException | TimeoutException e) {
            LOGGER.error("Error closing RabbitMQ message broker client", e);
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public boolean isConnected() {
        return connected.get() && connection != null && connection.isOpen();
    }
}