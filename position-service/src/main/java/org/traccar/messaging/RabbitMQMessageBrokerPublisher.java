/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.opentelemetry.context.propagation.TextMapPropagator;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.MessageBrokerPublisher;
import org.traccar.config.Config;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * RabbitMQ implementation of the MessageBrokerPublisher interface.
 * 
 * This class publishes messages to a RabbitMQ broker, with support for distributed tracing
 * and correlation IDs. It uses the RabbitMQ client to publish messages to exchanges, with
 * headers for tracing and correlation.
 */
@Singleton
public class RabbitMQMessageBrokerPublisher implements MessageBrokerPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQMessageBrokerPublisher.class);

    private final Connection connection;
    private final Channel channel;
    private final TextMapPropagator propagator;
    private final CircuitBreaker circuitBreaker;
    private final String exchangeName;

    /**
     * Constructs a new RabbitMQMessageBrokerPublisher with the necessary dependencies.
     *
     * @param config Configuration
     * @param propagator OpenTelemetry context propagator for distributed tracing
     * @param circuitBreakerRegistry Registry for circuit breakers
     * @throws Exception If there is an error connecting to RabbitMQ
     */
    @Inject
    public RabbitMQMessageBrokerPublisher(
            Config config,
            TextMapPropagator propagator,
            CircuitBreakerRegistry circuitBreakerRegistry) throws Exception {
        this.propagator = propagator;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("rabbitmq");
        this.exchangeName = config.getString("rabbitmq.exchange", "traccar");
        
        // Create a connection to RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.getString("rabbitmq.host", "localhost"));
        factory.setPort(config.getInteger("rabbitmq.port", 5672));
        factory.setUsername(config.getString("rabbitmq.username", "guest"));
        factory.setPassword(config.getString("rabbitmq.password", "guest"));
        factory.setVirtualHost(config.getString("rabbitmq.virtualHost", "/"));
        
        // Create a connection and channel
        this.connection = factory.newConnection();
        this.channel = connection.createChannel();
        
        // Declare the exchange
        channel.exchangeDeclare(exchangeName, "topic", true);
    }

    @Override
    public void publish(String topic, String payload, Map<String, String> headers) throws Exception {
        // Use circuit breaker to protect against RabbitMQ failures
        circuitBreaker.executeRunnable(() -> {
            try {
                // Create message properties with headers
                AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
                builder.contentType("application/json");
                builder.deliveryMode(2); // Persistent
                
                // Add headers
                Map<String, Object> messageHeaders = new HashMap<>();
                headers.forEach(messageHeaders::put);
                builder.headers(messageHeaders);
                
                // Publish the message to RabbitMQ
                channel.basicPublish(
                        exchangeName,
                        topic,
                        builder.build(),
                        payload.getBytes(StandardCharsets.UTF_8));
                
                LOGGER.debug("Published message to RabbitMQ exchange {} routing key {}", exchangeName, topic);
            } catch (Exception e) {
                LOGGER.error("Error publishing message to RabbitMQ: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public TextMapPropagator getPropagator() {
        return propagator;
    }

    /**
     * Close the RabbitMQ connection and channel when the application shuts down.
     *
     * @throws Exception If there is an error closing the connection or channel
     */
    public void close() throws Exception {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }
}