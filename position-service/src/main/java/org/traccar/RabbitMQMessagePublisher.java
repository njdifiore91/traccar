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
package org.traccar;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMQ implementation of the MessagePublisher interface.
 * Handles publishing messages to RabbitMQ exchanges with proper routing and headers.
 */
@Singleton
public class RabbitMQMessagePublisher implements MessagePublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQMessagePublisher.class);

    private final Connection connection;
    private final Channel channel;
    private final Tracer tracer;
    private final String exchangeName;

    @Inject
    public RabbitMQMessagePublisher(Config config, Tracer tracer) throws IOException, TimeoutException {
        this.tracer = tracer;
        this.exchangeName = config.getString("rabbitmq.exchange", "traccar");
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.getString("rabbitmq.host", "localhost"));
        factory.setPort(config.getInteger("rabbitmq.port", 5672));
        factory.setUsername(config.getString("rabbitmq.username", "guest"));
        factory.setPassword(config.getString("rabbitmq.password", "guest"));
        factory.setVirtualHost(config.getString("rabbitmq.virtualHost", "/"));
        
        connection = factory.newConnection();
        channel = connection.createChannel();
        
        // Declare a topic exchange
        channel.exchangeDeclare(exchangeName, "topic", true);
        
        LOGGER.info("Initialized RabbitMQ message publisher connected to {}:{}", 
                factory.getHost(), factory.getPort());
    }

    @Override
    public CompletableFuture<Void> publish(String topic, String key, String payload) {
        return publish(topic, key, payload, new HashMap<>());
    }

    @Override
    public CompletableFuture<Void> publish(String topic, String key, String payload, Map<String, String> headers) {
        Span span = tracer.spanBuilder("rabbitmq_publish").startSpan();
        
        try (var scope = span.makeCurrent()) {
            span.setAttribute("messaging.system", "rabbitmq");
            span.setAttribute("messaging.destination", topic);
            span.setAttribute("messaging.destination_kind", "topic");
            span.setAttribute("messaging.rabbitmq.routing_key", key);
            
            // Create message properties with headers
            AMQP.BasicProperties.Builder propertiesBuilder = new AMQP.BasicProperties.Builder();
            propertiesBuilder.contentType("application/json");
            propertiesBuilder.deliveryMode(2); // persistent
            
            // Add headers including tracing information
            Map<String, Object> messageHeaders = new HashMap<>();
            headers.forEach((headerKey, headerValue) -> {
                messageHeaders.put(headerKey, headerValue);
                span.setAttribute("messaging.header." + headerKey, headerValue);
            });
            
            // Add OpenTelemetry context propagation
            messageHeaders.put("traceparent", span.getSpanContext().getTraceId());
            propertiesBuilder.headers(messageHeaders);
            
            // Use topic as routing key prefix to support topic-based routing
            String routingKey = topic + "." + key;
            
            CompletableFuture<Void> future = new CompletableFuture<>();
            try {
                channel.basicPublish(
                        exchangeName,
                        routingKey,
                        propertiesBuilder.build(),
                        payload.getBytes(StandardCharsets.UTF_8));
                
                LOGGER.debug("Published message to RabbitMQ exchange {} with routing key {}", 
                        exchangeName, routingKey);
                future.complete(null);
            } catch (IOException e) {
                LOGGER.error("Failed to publish message to RabbitMQ", e);
                span.recordException(e);
                future.completeExceptionally(e);
            }
            
            return future;
        } finally {
            span.end();
        }
    }

    /**
     * Closes the RabbitMQ connection and channel.
     * Should be called when the application is shutting down.
     */
    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
            LOGGER.info("Closed RabbitMQ connection");
        } catch (IOException | TimeoutException e) {
            LOGGER.error("Error closing RabbitMQ connection", e);
        }
    }
}