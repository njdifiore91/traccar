/*
 * Copyright 2021 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.schedule;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.session.ConnectionManager;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Task responsible for sending WebSocket keepalive messages to connected clients.
 * In the microservices architecture, this task publishes heartbeat events to a message broker,
 * which are then consumed by API Gateway instances to send keepalive messages to WebSocket clients.
 */
@Singleton
public class TaskWebSocketKeepalive implements ScheduleTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskWebSocketKeepalive.class);
    private static final long PERIOD_SECONDS = 55;
    private static final String EXCHANGE_NAME = "heartbeat-events";
    private static final String ROUTING_KEY = "";

    private final ConnectionManager connectionManager;
    private final Config config;
    private final Tracer tracer;
    private final Meter meter;
    private final LongCounter heartbeatCounter;
    private final LongCounter failedHeartbeatCounter;

    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;

    /**
     * Constructs a new TaskWebSocketKeepalive instance.
     *
     * @param connectionManager The connection manager for backward compatibility
     * @param config The configuration for message broker settings
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param meter The OpenTelemetry meter for metrics collection
     */
    @Inject
    public TaskWebSocketKeepalive(
            ConnectionManager connectionManager,
            Config config,
            Tracer tracer,
            Meter meter) {
        this.connectionManager = connectionManager;
        this.config = config;
        this.tracer = tracer;
        this.meter = meter;

        // Initialize metrics
        this.heartbeatCounter = meter
                .counterBuilder("websocket.heartbeats")
                .setDescription("Number of WebSocket heartbeats sent")
                .setUnit("1")
                .build();

        this.failedHeartbeatCounter = meter
                .counterBuilder("websocket.heartbeats.failed")
                .setDescription("Number of failed WebSocket heartbeat attempts")
                .setUnit("1")
                .build();

        initializeMessageBroker();
    }

    /**
     * Initializes the message broker connection.
     */
    private void initializeMessageBroker() {
        try {
            factory = new ConnectionFactory();
            factory.setHost(config.getString(Keys.RABBITMQ_HOST, "localhost"));
            factory.setPort(config.getInteger(Keys.RABBITMQ_PORT, 5672));
            factory.setUsername(config.getString(Keys.RABBITMQ_USERNAME, "guest"));
            factory.setPassword(config.getString(Keys.RABBITMQ_PASSWORD, "guest"));
            factory.setVirtualHost(config.getString(Keys.RABBITMQ_VIRTUAL_HOST, "/"));

            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout", true);
            LOGGER.info("Message broker connection established for WebSocket keepalive");
        } catch (Exception e) {
            LOGGER.error("Failed to initialize message broker connection", e);
        }
    }

    @Override
    public void schedule(ScheduledExecutorService executor) {
        executor.scheduleAtFixedRate(this, PERIOD_SECONDS, PERIOD_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        Span span = tracer.spanBuilder("websocket.keepalive").startSpan();
        try (io.opentelemetry.context.Scope scope = span.makeCurrent()) {
            // For backward compatibility, still call the connection manager
            connectionManager.sendKeepalive();
            
            // Send heartbeat event to message broker for API Gateway instances
            publishHeartbeatEvent();
            
            span.setStatus(StatusCode.OK);
            heartbeatCounter.add(1);
        } catch (Exception e) {
            LOGGER.warn("Failed to send WebSocket keepalive", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Failed to send WebSocket keepalive");
            failedHeartbeatCounter.add(1);
            
            // Try to reconnect to the message broker if the connection is lost
            if (connection == null || !connection.isOpen()) {
                LOGGER.info("Attempting to reconnect to message broker");
                initializeMessageBroker();
            }
        } finally {
            span.end();
        }
    }

    /**
     * Publishes a heartbeat event to the message broker.
     * This event will be consumed by API Gateway instances to send keepalive messages to WebSocket clients.
     */
    private void publishHeartbeatEvent() {
        if (channel == null || !channel.isOpen()) {
            LOGGER.warn("Cannot publish heartbeat event: channel is not open");
            return;
        }

        try {
            Map<String, Object> message = new HashMap<>();
            message.put("type", "websocket.keepalive");
            message.put("timestamp", Instant.now().toString());
            message.put("correlationId", Span.current().getSpanContext().getTraceId());

            String messageJson = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(message);
            channel.basicPublish(
                    EXCHANGE_NAME,
                    ROUTING_KEY,
                    null,
                    messageJson.getBytes(StandardCharsets.UTF_8));

            LOGGER.debug("Published WebSocket keepalive event to message broker");
        } catch (Exception e) {
            LOGGER.error("Failed to publish heartbeat event", e);
            throw new RuntimeException("Failed to publish heartbeat event", e);
        }
    }

    /**
     * Closes the message broker connection when the application is shutting down.
     */
    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
            LOGGER.info("Closed message broker connection for WebSocket keepalive");
        } catch (Exception e) {
            LOGGER.error("Error closing message broker connection", e);
        }
    }
}