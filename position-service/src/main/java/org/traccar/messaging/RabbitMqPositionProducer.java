/*
 * Copyright 2023-2025 Anton Tananaev (anton@traccar.org)
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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.api.GlobalOpenTelemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.messaging.rabbitmq.RabbitMQConfig;
import org.traccar.messaging.rabbitmq.RabbitMQRoutingStrategy;
import org.traccar.messaging.rabbitmq.RabbitMQRetryHandler;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMQ implementation of the PositionProducer interface.
 * Handles publishing of enriched position messages to RabbitMQ exchanges
 * with support for distributed tracing and reliable delivery.
 */
@Singleton
public class RabbitMqPositionProducer implements PositionProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqPositionProducer.class);
    
    private static final String EXCHANGE_NAME = "position-exchange";
    private static final String ROUTING_KEY_PREFIX = "position.";
    private static final String CONTENT_TYPE = "application/protobuf";
    
    private final Connection connection;
    private final Channel channel;
    private final String exchange;
    private final RabbitMQRoutingStrategy routingStrategy;
    private final RabbitMQRetryHandler retryHandler;
    private final Tracer tracer;
    
    private static final TextMapSetter<Map<String, String>> SETTER = 
        (carrier, key, value) -> carrier.put(key, value);

    /**
     * Constructs a new RabbitMqPositionProducer with the specified configuration.
     *
     * @param config The application configuration
     * @throws IOException If a connection or channel cannot be established
     * @throws TimeoutException If connection establishment times out
     */
    @Inject
    public RabbitMqPositionProducer(Config config) throws IOException, TimeoutException {
        // Initialize OpenTelemetry tracer
        this.tracer = GlobalOpenTelemetry.getTracer("position-service");
        
        // Initialize RabbitMQ connection
        ConnectionFactory factory = new ConnectionFactory();
        String connectionUrl = config.getString(RabbitMQConfig.CONNECTION_URL_KEY, RabbitMQConfig.DEFAULT_CONNECTION_URL);
        factory.setUri(connectionUrl);
        
        // Set connection properties for reliability
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000); // 5 seconds
        
        // Create connection and channel
        this.connection = factory.newConnection("position-service-producer");
        this.channel = connection.createChannel();
        
        // Configure channel for publisher confirms
        channel.confirmSelect();
        
        // Get exchange name from config or use default
        this.exchange = config.getString(RabbitMQConfig.POSITION_EXCHANGE_KEY, EXCHANGE_NAME);
        
        // Declare exchange
        channel.exchangeDeclare(exchange, "topic", true);
        
        // Initialize routing strategy
        this.routingStrategy = new RabbitMQRoutingStrategy();
        
        // Initialize retry handler
        this.retryHandler = new RabbitMQRetryHandler(config);
        
        LOGGER.info("RabbitMQ position producer initialized with exchange: {}", exchange);
    }

    /**
     * Publishes a position message to the RabbitMQ exchange.
     *
     * @param positionMessage The position message to publish
     * @param callback The callback to invoke after publishing
     */
    @Override
    public void publish(PositionMessage positionMessage, PublishCallback callback) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("publish_position")
                .setSpanKind(SpanKind.PRODUCER)
                .startSpan();
        
        try {
            // Add the position to the span for context
            span.setAttribute("position.deviceId", positionMessage.getDeviceId());
            span.setAttribute("position.time", positionMessage.getTime().toString());
            
            // Create headers for the message
            Map<String, String> headers = new HashMap<>();
            headers.put("content-type", CONTENT_TYPE);
            headers.put("message-type", "position");
            
            // If there's a correlation ID, use it
            if (positionMessage.getCorrelationId() != null) {
                headers.put("correlation-id", positionMessage.getCorrelationId());
                span.setAttribute("correlation.id", positionMessage.getCorrelationId());
            }
            
            // Inject the current span context into the headers
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .inject(Context.current(), headers, SETTER);
            
            // Determine routing key based on device ID
            String routingKey = routingStrategy.createRoutingKey(ROUTING_KEY_PREFIX, 
                    String.valueOf(positionMessage.getDeviceId()));
            
            // Convert headers to AMQP headers
            AMQP.BasicProperties properties = createProperties(headers);
            
            // Serialize the message to bytes
            byte[] messageBytes = positionMessage.toByteArray();
            
            // Publish the message with publisher confirms
            channel.basicPublish(exchange, routingKey, properties, messageBytes);
            
            // Wait for publisher confirm
            if (channel.waitForConfirms()) {
                LOGGER.debug("Position message published successfully: deviceId={}", 
                        positionMessage.getDeviceId());
                if (callback != null) {
                    callback.onPublished(positionMessage);
                }
            } else {
                LOGGER.error("Failed to get confirmation for position message: deviceId={}", 
                        positionMessage.getDeviceId());
                if (callback != null) {
                    callback.onError(positionMessage, 
                            new MessageException("Failed to get publisher confirmation"));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error publishing position message", e);
            span.recordException(e);
            
            // Handle retry logic
            if (retryHandler.shouldRetry(e)) {
                LOGGER.info("Scheduling retry for position message: deviceId={}", 
                        positionMessage.getDeviceId());
                retryHandler.scheduleRetry(() -> publish(positionMessage, callback));
            } else if (callback != null) {
                callback.onError(positionMessage, new MessageException("Failed to publish message", e));
            }
        } finally {
            span.end();
        }
    }

    /**
     * Publishes a position message to the RabbitMQ exchange without a callback.
     *
     * @param positionMessage The position message to publish
     */
    @Override
    public void publish(PositionMessage positionMessage) {
        publish(positionMessage, null);
    }

    /**
     * Creates AMQP properties with the specified headers.
     *
     * @param headers The headers to include in the properties
     * @return The AMQP properties
     */
    private AMQP.BasicProperties createProperties(Map<String, String> headers) {
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder()
                .contentType(CONTENT_TYPE)
                .deliveryMode(2) // persistent
                .timestamp(new java.util.Date());
        
        if (!headers.isEmpty()) {
            Map<String, Object> headerMap = new HashMap<>();
            headers.forEach(headerMap::put);
            builder.headers(headerMap);
        }
        
        return builder.build();
    }

    /**
     * Closes the RabbitMQ channel and connection.
     */
    @Override
    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
            LOGGER.info("RabbitMQ position producer closed");
        } catch (Exception e) {
            LOGGER.error("Error closing RabbitMQ resources", e);
        }
    }
}