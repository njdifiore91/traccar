/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.broadcast;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.impl.MicrometerMetricsCollector;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapSetter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

public class RabbitMQBroadcastService extends BaseBroadcastService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQBroadcastService.class);

    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;
    private final Tracer tracer;
    
    private final String exchangeName;
    private final String queueName;
    private final String routingKey;
    
    private Connection connection;
    private Channel publishChannel;
    private Channel consumeChannel;
    
    private final CircuitBreaker circuitBreaker;
    
    private static final TextMapSetter<Map<String, String>> SETTER = new TextMapSetter<>() {
        @Override
        public void set(Map<String, String> carrier, String key, String value) {
            carrier.put(key, value);
        }
    };

    public RabbitMQBroadcastService(
            Config config, ExecutorService executorService, ObjectMapper objectMapper, 
            MeterRegistry meterRegistry, Tracer tracer) throws IOException, TimeoutException {
        
        this.executorService = executorService;
        this.objectMapper = objectMapper;
        this.tracer = tracer;
        
        // Configure RabbitMQ connection
        String brokerUrl = config.getString(Keys.BROADCAST_ADDRESS);
        int brokerPort = config.getInteger(Keys.BROADCAST_PORT, 5672);
        String username = config.getString(Keys.BROADCAST_USERNAME, "guest");
        String password = config.getString(Keys.BROADCAST_PASSWORD, "guest");
        String virtualHost = config.getString(Keys.BROADCAST_VIRTUALHOST, "/");
        
        // Configure exchange and queue names
        exchangeName = config.getString(Keys.BROADCAST_EXCHANGE, "traccar.broadcast");
        queueName = config.getString(Keys.BROADCAST_QUEUE, "traccar.broadcast.queue");
        routingKey = config.getString(Keys.BROADCAST_ROUTING_KEY, "traccar.broadcast");
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        circuitBreaker = circuitBreakerRegistry.circuitBreaker("rabbitmqBroadcast");
        
        // Setup RabbitMQ connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(brokerUrl);
        factory.setPort(brokerPort);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setVirtualHost(virtualHost);
        
        // Configure automatic recovery
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000); // 5 seconds
        factory.setTopologyRecoveryEnabled(true);
        
        // Configure connection timeout
        factory.setConnectionTimeout(5000); // 5 seconds
        
        // Configure metrics collection
        if (meterRegistry != null) {
            factory.setMetricsCollector(new MicrometerMetricsCollector(
                    meterRegistry, "rabbitmq", Tags.of("name", "broadcast")));
        }
        
        try {
            // Establish connection
            connection = factory.newConnection(executorService);
            
            // Create channels for publishing and consuming
            publishChannel = connection.createChannel();
            consumeChannel = connection.createChannel();
            
            // Declare exchange and queue
            publishChannel.exchangeDeclare(exchangeName, "topic", true);
            consumeChannel.queueDeclare(queueName, true, false, false, null);
            consumeChannel.queueBind(queueName, exchangeName, routingKey);
            
            LOGGER.info("RabbitMQ broadcast service initialized successfully");
        } catch (IOException | TimeoutException e) {
            LOGGER.error("Failed to initialize RabbitMQ broadcast service", e);
            close();
            throw e;
        }
    }

    @Override
    protected void sendMessage(BroadcastMessage message) {
        try {
            circuitBreaker.executeRunnable(() -> {
                try {
                    // Create message headers for distributed tracing
                    Map<String, String> headers = new HashMap<>();
                    Span span = tracer.spanBuilder("broadcast.send").startSpan();
                    
                    try {
                        SpanContext spanContext = span.getSpanContext();
                        if (spanContext.isValid()) {
                            tracer.getPropagators().getTextMapPropagator()
                                    .inject(Context.current().with(span), headers, SETTER);
                        }
                        
                        // Serialize message to JSON
                        byte[] messageBytes = objectMapper.writeValueAsBytes(message);
                        
                        // Create message properties with headers
                        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                                .contentType("application/json")
                                .headers(headers)
                                .build();
                        
                        // Publish message
                        publishChannel.basicPublish(exchangeName, routingKey, properties, messageBytes);
                    } finally {
                        span.end();
                    }
                } catch (IOException e) {
                    LOGGER.warn("Failed to send broadcast message", e);
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            LOGGER.warn("Circuit breaker prevented sending broadcast message", e);
        }
    }

    @Override
    public void start() throws IOException {
        try {
            // Setup consumer
            consumeChannel.basicConsume(queueName, true, new DefaultConsumer(consumeChannel) {
                @Override
                public void handleDelivery(
                        String consumerTag, Envelope envelope,
                        AMQP.BasicProperties properties, byte[] body) throws IOException {
                    
                    try {
                        // Extract tracing context from headers
                        Context context = Context.current();
                        if (properties.getHeaders() != null) {
                            context = tracer.getPropagators().getTextMapPropagator()
                                    .extract(context, properties.getHeaders(), (carrier, key) -> {
                                        Object value = carrier.get(key);
                                        return value != null ? value.toString() : null;
                                    });
                        }
                        
                        // Create span for message processing
                        Span span = tracer.spanBuilder("broadcast.receive")
                                .setParent(context)
                                .startSpan();
                        
                        try {
                            // Deserialize and handle message
                            String messageJson = new String(body, StandardCharsets.UTF_8);
                            BroadcastMessage message = objectMapper.readValue(messageJson, BroadcastMessage.class);
                            handleMessage(message);
                        } catch (Exception e) {
                            LOGGER.warn("Failed to process broadcast message", e);
                            span.recordException(e);
                        } finally {
                            span.end();
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Error handling broadcast message", e);
                    }
                }
            });
            
            LOGGER.info("RabbitMQ broadcast service started");
        } catch (IOException e) {
            LOGGER.error("Failed to start RabbitMQ broadcast service", e);
            throw e;
        }
    }

    @Override
    public void stop() {
        close();
    }
    
    private void close() {
        try {
            if (consumeChannel != null && consumeChannel.isOpen()) {
                consumeChannel.close();
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to close consume channel", e);
        }
        
        try {
            if (publishChannel != null && publishChannel.isOpen()) {
                publishChannel.close();
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to close publish channel", e);
        }
        
        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to close connection", e);
        }
        
        LOGGER.info("RabbitMQ broadcast service stopped");
    }
}