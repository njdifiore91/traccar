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
package org.traccar.messaging;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.session.discovery.ServiceDiscoveryManager;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Manages message broker connections and provides a unified interface for publishing and subscribing to topics.
 * Supports multiple message broker implementations (Kafka, RabbitMQ) based on configuration.
 */
@Singleton
public class MessageBrokerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBrokerManager.class);

    private final Config config;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final MeterRegistry meterRegistry;
    private final MessageBrokerClient brokerClient;
    
    // Metrics
    private final Timer publishTimer;
    private final Counter messagesPublishedCounter;
    private final Counter messagesReceivedCounter;
    
    // Subscription handlers
    private final Map<String, Consumer<String>> subscriptionHandlers = new ConcurrentHashMap<>();

    @Inject
    public MessageBrokerManager(Config config, ServiceDiscoveryManager serviceDiscoveryManager, MeterRegistry meterRegistry) {
        this.config = config;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.publishTimer = meterRegistry.timer("broker.publish.time");
        this.messagesPublishedCounter = meterRegistry.counter("broker.messages.published");
        this.messagesReceivedCounter = meterRegistry.counter("broker.messages.received");
        
        // Initialize broker client based on configuration
        String brokerType = config.getString(Keys.MESSAGE_BROKER_TYPE, "kafka");
        if ("rabbitmq".equalsIgnoreCase(brokerType)) {
            this.brokerClient = new RabbitMqClient(config, serviceDiscoveryManager, meterRegistry);
        } else {
            // Default to Kafka
            this.brokerClient = new KafkaClient(config, serviceDiscoveryManager, meterRegistry);
        }
        
        // Initialize broker connection
        brokerClient.initialize();
        
        // Register with service discovery
        serviceDiscoveryManager.register("message-broker-manager", "messaging");
        
        LOGGER.info("MessageBrokerManager initialized with {} client", brokerType);
    }

    /**
     * Publishes a message to the specified topic.
     *
     * @param topic   The topic to publish to
     * @param message The message to publish
     */
    public void publish(String topic, String message) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("publishMessage").start();
        span.setTag("topic", topic);
        span.setTag("messageSize", message.length());
        
        try {
            // Add correlation ID to message metadata
            Map<String, String> headers = new HashMap<>();
            headers.put("correlationId", correlationId);
            
            // Measure publish time
            publishTimer.record(() -> {
                brokerClient.publish(topic, message, headers);
                return null;
            });
            
            // Update metrics
            messagesPublishedCounter.increment();
            meterRegistry.counter("broker.topic." + topic + ".published").increment();
            
            LOGGER.debug("Published message to topic: {}, size: {} bytes", topic, message.length());
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error publishing message to topic {}: {}", topic, e.getMessage(), e);
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Subscribes to a topic with a message handler.
     *
     * @param topic   The topic to subscribe to
     * @param handler The handler to process received messages
     */
    public void subscribe(String topic, Consumer<String> handler) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("subscribe").start();
        span.setTag("topic", topic);
        
        try {
            // Store handler for this subscription
            subscriptionHandlers.put(topic, handler);
            
            // Create message handler that updates metrics and traces
            Consumer<String> wrappedHandler = message -> {
                String msgCorrelationId = UUID.randomUUID().toString();
                MDC.put("correlationId", msgCorrelationId);
                Span msgSpan = tracer.buildSpan("handleMessage").start();
                msgSpan.setTag("topic", topic);
                msgSpan.setTag("messageSize", message.length());
                
                try {
                    // Update metrics
                    messagesReceivedCounter.increment();
                    meterRegistry.counter("broker.topic." + topic + ".received").increment();
                    
                    // Call the actual handler
                    handler.accept(message);
                } catch (Exception e) {
                    Tags.ERROR.set(msgSpan, true);
                    msgSpan.log(Map.of("error.message", e.getMessage()));
                    LOGGER.error("Error processing message from topic {}: {}", topic, e.getMessage(), e);
                } finally {
                    msgSpan.finish();
                    MDC.remove("correlationId");
                }
            };
            
            // Subscribe to the topic
            brokerClient.subscribe(topic, wrappedHandler);
            
            LOGGER.info("Subscribed to topic: {}", topic);
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error subscribing to topic {}: {}", topic, e.getMessage(), e);
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Unsubscribes from a topic.
     *
     * @param topic The topic to unsubscribe from
     */
    public void unsubscribe(String topic) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("unsubscribe").start();
        span.setTag("topic", topic);
        
        try {
            // Remove handler for this subscription
            subscriptionHandlers.remove(topic);
            
            // Unsubscribe from the topic
            brokerClient.unsubscribe(topic);
            
            LOGGER.info("Unsubscribed from topic: {}", topic);
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error unsubscribing from topic {}: {}", topic, e.getMessage(), e);
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Shuts down the message broker manager and releases resources.
     */
    public void shutdown() {
        LOGGER.info("Shutting down MessageBrokerManager...");
        try {
            // Unsubscribe from all topics
            for (String topic : subscriptionHandlers.keySet()) {
                brokerClient.unsubscribe(topic);
            }
            subscriptionHandlers.clear();
            
            // Close broker client
            brokerClient.close();
            
            // Unregister from service discovery
            serviceDiscoveryManager.unregister("message-broker-manager");
            
            LOGGER.info("MessageBrokerManager shutdown complete");
        } catch (Exception e) {
            LOGGER.error("Error during MessageBrokerManager shutdown", e);
        }
    }

    /**
     * Interface for message broker client implementations.
     */
    private interface MessageBrokerClient {
        void initialize();
        void publish(String topic, String message, Map<String, String> headers);
        void subscribe(String topic, Consumer<String> handler);
        void unsubscribe(String topic);
        void close();
    }

    /**
     * Kafka implementation of the message broker client.
     */
    private static class KafkaClient implements MessageBrokerClient {
        private final Config config;
        private final ServiceDiscoveryManager serviceDiscoveryManager;
        private final MeterRegistry meterRegistry;
        
        public KafkaClient(Config config, ServiceDiscoveryManager serviceDiscoveryManager, MeterRegistry meterRegistry) {
            this.config = config;
            this.serviceDiscoveryManager = serviceDiscoveryManager;
            this.meterRegistry = meterRegistry;
        }
        
        @Override
        public void initialize() {
            // Implementation would initialize Kafka producer and consumer
            LOGGER.info("Initialized Kafka client");
        }
        
        @Override
        public void publish(String topic, String message, Map<String, String> headers) {
            // Implementation would publish message to Kafka topic
            LOGGER.debug("Published to Kafka topic: {}", topic);
        }
        
        @Override
        public void subscribe(String topic, Consumer<String> handler) {
            // Implementation would subscribe to Kafka topic
            LOGGER.debug("Subscribed to Kafka topic: {}", topic);
        }
        
        @Override
        public void unsubscribe(String topic) {
            // Implementation would unsubscribe from Kafka topic
            LOGGER.debug("Unsubscribed from Kafka topic: {}", topic);
        }
        
        @Override
        public void close() {
            // Implementation would close Kafka connections
            LOGGER.info("Closed Kafka client");
        }
    }

    /**
     * RabbitMQ implementation of the message broker client.
     */
    private static class RabbitMqClient implements MessageBrokerClient {
        private final Config config;
        private final ServiceDiscoveryManager serviceDiscoveryManager;
        private final MeterRegistry meterRegistry;
        
        public RabbitMqClient(Config config, ServiceDiscoveryManager serviceDiscoveryManager, MeterRegistry meterRegistry) {
            this.config = config;
            this.serviceDiscoveryManager = serviceDiscoveryManager;
            this.meterRegistry = meterRegistry;
        }
        
        @Override
        public void initialize() {
            // Implementation would initialize RabbitMQ connection
            LOGGER.info("Initialized RabbitMQ client");
        }
        
        @Override
        public void publish(String topic, String message, Map<String, String> headers) {
            // Implementation would publish message to RabbitMQ exchange
            LOGGER.debug("Published to RabbitMQ exchange: {}", topic);
        }
        
        @Override
        public void subscribe(String topic, Consumer<String> handler) {
            // Implementation would subscribe to RabbitMQ queue
            LOGGER.debug("Subscribed to RabbitMQ queue: {}", topic);
        }
        
        @Override
        public void unsubscribe(String topic) {
            // Implementation would unsubscribe from RabbitMQ queue
            LOGGER.debug("Unsubscribed from RabbitMQ queue: {}", topic);
        }
        
        @Override
        public void close() {
            // Implementation would close RabbitMQ connection
            LOGGER.info("Closed RabbitMQ client");
        }
    }
}