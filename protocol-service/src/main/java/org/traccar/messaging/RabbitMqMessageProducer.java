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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.impl.OpenTelemetryMetricsCollector;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.model.Position;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMQ implementation of the MessageProducer interface.
 * Handles connection management, exchange declaration, and message routing for RabbitMQ.
 * Integrates with OpenTelemetry for distributed tracing and Micrometer for metrics collection.
 */
public class RabbitMqMessageProducer implements MessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqMessageProducer.class);

    private static final String TRACER_NAME = "org.traccar.messaging.rabbitmq";
    private static final String METRIC_PREFIX = "rabbitmq";
    
    private static final String POSITIONS_EXCHANGE = "positions";
    private static final String DEVICE_CONNECTIONS_EXCHANGE = "device.connections";
    
    private final Connection connection;
    private final Channel channel;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    
    private final String positionsExchange;
    private final String deviceConnectionsExchange;
    private final boolean durableExchanges;
    
    private final Timer positionPublishTimer;
    private final Timer deviceConnectionPublishTimer;

    /**
     * Creates a new RabbitMqMessageProducer with the specified configuration.
     *
     * @param config The configuration for RabbitMQ connection and settings
     * @param objectMapper The object mapper for serializing messages
     * @param openTelemetry The OpenTelemetry instance for distributed tracing
     * @param meterRegistry The meter registry for metrics collection
     * @throws RuntimeException If there is an error creating the RabbitMQ connection or channel
     */
    public RabbitMqMessageProducer(
            Config config,
            ObjectMapper objectMapper,
            OpenTelemetry openTelemetry,
            MeterRegistry meterRegistry) {
        
        this.objectMapper = objectMapper;
        this.tracer = openTelemetry.getTracer(TRACER_NAME);
        this.meterRegistry = meterRegistry;
        
        // Get configuration values
        String connectionUrl = config.getString("rabbitmq.url", "amqp://guest:guest@localhost:5672");
        this.positionsExchange = config.getString("rabbitmq.exchange.positions", POSITIONS_EXCHANGE);
        this.deviceConnectionsExchange = config.getString("rabbitmq.exchange.deviceConnections", DEVICE_CONNECTIONS_EXCHANGE);
        this.durableExchanges = config.getBoolean("rabbitmq.exchange.durable", true);
        
        // Create metrics
        Tags commonTags = Tags.of(
                Tag.of("component", "rabbitmq"),
                Tag.of("service", "protocol-service"));
        
        this.positionPublishTimer = Timer.builder(METRIC_PREFIX + ".position.publish")
                .tags(commonTags)
                .description("Time taken to publish position messages to RabbitMQ")
                .register(meterRegistry);
        
        this.deviceConnectionPublishTimer = Timer.builder(METRIC_PREFIX + ".device.connection.publish")
                .tags(commonTags)
                .description("Time taken to publish device connection messages to RabbitMQ")
                .register(meterRegistry);
        
        try {
            // Create RabbitMQ connection
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(connectionUrl);
            
            // Set up OpenTelemetry metrics collector
            Attributes attributes = Attributes.builder()
                    .put("service.name", "protocol-service")
                    .put("messaging.system", "rabbitmq")
                    .build();
            factory.setMetricsCollector(new OpenTelemetryMetricsCollector(openTelemetry, METRIC_PREFIX, attributes));
            
            // Create connection and channel
            this.connection = factory.newConnection("protocol-service");
            this.channel = connection.createChannel();
            
            // Declare exchanges
            channel.exchangeDeclare(positionsExchange, BuiltinExchangeType.TOPIC, durableExchanges);
            channel.exchangeDeclare(deviceConnectionsExchange, BuiltinExchangeType.TOPIC, durableExchanges);
            
            LOGGER.info("RabbitMQ connection established to {}", connectionUrl);
            LOGGER.info("Declared exchanges: {}, {}", positionsExchange, deviceConnectionsExchange);
            
        } catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException e) {
            LOGGER.error("Error configuring RabbitMQ connection", e);
            throw new RuntimeException("Error configuring RabbitMQ connection", e);
        } catch (IOException | TimeoutException e) {
            LOGGER.error("Error connecting to RabbitMQ", e);
            throw new RuntimeException("Error connecting to RabbitMQ", e);
        }
    }

    @Override
    public void sendPosition(Position position) throws Exception {
        String routingKey = "position." + position.getDeviceId();
        String messageContent = objectMapper.writeValueAsString(position);
        
        // Create span for tracing
        Span span = tracer.spanBuilder("rabbitmq.publish.position")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("messaging.system", "rabbitmq")
                .setAttribute("messaging.destination.name", positionsExchange + ":" + routingKey)
                .setAttribute("messaging.rabbitmq.routing_key", routingKey)
                .setAttribute("device.id", position.getDeviceId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Record metrics using timer
            positionPublishTimer.record(() -> {
                try {
                    // Create message headers with tracing context
                    Map<String, Object> headers = new HashMap<>();
                    headers.put("device_id", position.getDeviceId());
                    
                    // Create message properties with headers
                    AMQP.BasicProperties properties = MessageProperties.PERSISTENT_TEXT_PLAIN.builder()
                            .headers(headers)
                            .build();
                    
                    // Publish message
                    channel.basicPublish(positionsExchange, routingKey, properties, messageContent.getBytes());
                    
                    span.setAttribute("messaging.message_payload_size_bytes", messageContent.length());
                    span.setStatus(StatusCode.OK);
                    
                    LOGGER.debug("Published position for device {} to exchange {}", position.getDeviceId(), positionsExchange);
                } catch (IOException e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    LOGGER.error("Error publishing position message", e);
                    throw new RuntimeException("Error publishing position message", e);
                }
            });
        } finally {
            span.end();
        }
    }

    @Override
    public void sendDeviceConnectionStatus(long deviceId, boolean connected) throws Exception {
        String routingKey = "device." + deviceId + ".connection";
        Map<String, Object> message = new HashMap<>();
        message.put("deviceId", deviceId);
        message.put("connected", connected);
        String messageContent = objectMapper.writeValueAsString(message);
        
        // Create span for tracing
        Span span = tracer.spanBuilder("rabbitmq.publish.device.connection")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("messaging.system", "rabbitmq")
                .setAttribute("messaging.destination.name", deviceConnectionsExchange + ":" + routingKey)
                .setAttribute("messaging.rabbitmq.routing_key", routingKey)
                .setAttribute("device.id", deviceId)
                .setAttribute("device.connected", connected)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Record metrics using timer
            deviceConnectionPublishTimer.record(() -> {
                try {
                    // Create message headers with tracing context
                    Map<String, Object> headers = new HashMap<>();
                    headers.put("device_id", deviceId);
                    headers.put("connected", connected);
                    
                    // Create message properties with headers
                    AMQP.BasicProperties properties = MessageProperties.PERSISTENT_TEXT_PLAIN.builder()
                            .headers(headers)
                            .build();
                    
                    // Publish message
                    channel.basicPublish(deviceConnectionsExchange, routingKey, properties, messageContent.getBytes());
                    
                    span.setAttribute("messaging.message_payload_size_bytes", messageContent.length());
                    span.setStatus(StatusCode.OK);
                    
                    LOGGER.debug("Published connection status {} for device {} to exchange {}", 
                            connected ? "connected" : "disconnected", deviceId, deviceConnectionsExchange);
                } catch (IOException e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    LOGGER.error("Error publishing device connection message", e);
                    throw new RuntimeException("Error publishing device connection message", e);
                }
            });
        } finally {
            span.end();
        }
    }

    @Override
    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
                LOGGER.debug("RabbitMQ channel closed");
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
                LOGGER.info("RabbitMQ connection closed");
            }
        } catch (IOException | TimeoutException e) {
            LOGGER.warn("Error closing RabbitMQ connection", e);
        }
    }
}