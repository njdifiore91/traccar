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
package org.traccar.notification;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.concurrent.TimeUnit;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/**
 * Manages the integration with message brokers (Kafka/RabbitMQ) for asynchronous notification processing.
 * Provides a unified interface for publishing notification messages to channel-specific topics and
 * consuming events from the event service.
 */
@Singleton
public class MessageBrokerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBrokerManager.class);

    private final Config config;
    private final ObjectMapper objectMapper;
    private final RetryRegistry retryRegistry;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final MessageBrokerClient brokerClient;
    private final MeterRegistry meterRegistry;
    
    // Metrics
    private final Counter publishCounter;
    private final Counter publishErrorCounter;
    private final Counter consumeCounter;
    private final Counter consumeErrorCounter;
    private final Counter dlqCounter;
    private final Timer publishTimer;
    private final Timer consumeTimer;
    
    // Correlation ID for distributed tracing
    private final String correlationId;

    /**
     * Channel-specific topics for different notification types
     */
    public static final String EMAIL_TOPIC = "email-out";
    public static final String SMS_TOPIC = "sms-out";
    public static final String PUSH_TOPIC = "push-out";
    public static final String WEB_TOPIC = "web-out";
    public static final String EVENTS_TOPIC = "events";
    public static final String DEAD_LETTER_TOPIC = "dead-letter-queue";

    /**
     * Constructs a new MessageBrokerManager with the specified configuration and object mapper.
     *
     * @param config The system configuration
     * @param objectMapper The object mapper for JSON serialization/deserialization
     */
    @Inject
    public MessageBrokerManager(Config config, ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.config = config;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.correlationId = java.util.UUID.randomUUID().toString();
        
        // Initialize metrics
        this.publishCounter = meterRegistry.counter("notification.broker.publish.count");
        this.publishErrorCounter = meterRegistry.counter("notification.broker.publish.error.count");
        this.consumeCounter = meterRegistry.counter("notification.broker.consume.count");
        this.consumeErrorCounter = meterRegistry.counter("notification.broker.consume.error.count");
        this.dlqCounter = meterRegistry.counter("notification.broker.dlq.count");
        this.publishTimer = meterRegistry.timer("notification.broker.publish.time");
        this.consumeTimer = meterRegistry.timer("notification.broker.consume.time");

        // Configure retry registry with exponential backoff
        // Configure retry registry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(config.getInteger(Keys.NOTIFICATION_RETRY_ATTEMPTS, 3))
                .waitDuration(Duration.ofMillis(config.getInteger(Keys.NOTIFICATION_RETRY_DELAY, 1000)))
                .exponentialBackoff(Duration.ofMillis(config.getInteger(Keys.NOTIFICATION_RETRY_MULTIPLIER, 2)))
                .retryExceptions(Exception.class)
                .ignoreExceptions(InterruptedException.class)
                .build();
        this.retryRegistry = RetryRegistry.of(retryConfig);
        
        // Configure circuit breaker registry
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(config.getFloat(Keys.NOTIFICATION_CIRCUIT_FAILURE_RATE, 50.0f))
                .waitDurationInOpenState(Duration.ofMillis(config.getInteger(Keys.NOTIFICATION_CIRCUIT_WAIT_DURATION, 10000)))
                .permittedNumberOfCallsInHalfOpenState(config.getInteger(Keys.NOTIFICATION_CIRCUIT_PERMITTED_CALLS, 10))
                .slidingWindowSize(config.getInteger(Keys.NOTIFICATION_CIRCUIT_WINDOW_SIZE, 100))
                .minimumNumberOfCalls(config.getInteger(Keys.NOTIFICATION_CIRCUIT_MIN_CALLS, 10))
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);

        // Initialize the appropriate broker client based on configuration
        String brokerType = config.getString(Keys.NOTIFICATION_BROKER_TYPE, "kafka").toLowerCase();
        if ("rabbitmq".equals(brokerType) || "amqp".equals(brokerType)) {
            this.brokerClient = new RabbitMqClient(config, objectMapper);
            LOGGER.info("Initialized RabbitMQ client for notification service");
        } else {
            this.brokerClient = new KafkaClient(config, objectMapper);
            LOGGER.info("Initialized Kafka client for notification service");
        }

        // Initialize the broker connection
        this.brokerClient.connect();
    }

    /**
     * Publishes a notification message to the specified topic with retry logic.
     *
     * @param topic The topic to publish to
     * @param message The message to publish
     * @param <T> The type of the message
     * @return A CompletableFuture that completes when the message is published
     */
    public <T> CompletableFuture<Void> publish(String topic, T message) {
        Retry retry = retryRegistry.retry("publish-" + topic);
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("publish-" + topic);
        
        return CircuitBreaker.decorateCompletionStage(
                circuitBreaker,
                Retry.decorateCompletionStage(
                    retry,
                    () -> CompletableFuture.supplyAsync(() -> {
                        return publishTimer.record(() -> {
                            try {
                                brokerClient.publish(topic, message);
                                publishCounter.increment();
                                LOGGER.debug("Published message to topic: {} with correlationId: {}", topic, correlationId);
                                return null;
                            } catch (Exception e) {
                                publishErrorCounter.increment();
                                LOGGER.error("Failed to publish message to topic: {} with correlationId: {}", topic, correlationId, e);
                                throw new RuntimeException("Failed to publish message", e);
                            }
                        });
                    })
                )
        ).toCompletableFuture();
    }

    /**
     * Publishes a notification message to the email topic.
     *
     * @param message The email notification message
     * @param <T> The type of the message
     * @return A CompletableFuture that completes when the message is published
     */
    public <T> CompletableFuture<Void> publishEmailNotification(T message) {
        return publish(EMAIL_TOPIC, message);
    }

    /**
     * Publishes a notification message to the SMS topic.
     *
     * @param message The SMS notification message
     * @param <T> The type of the message
     * @return A CompletableFuture that completes when the message is published
     */
    public <T> CompletableFuture<Void> publishSmsNotification(T message) {
        return publish(SMS_TOPIC, message);
    }

    /**
     * Publishes a notification message to the push notification topic.
     *
     * @param message The push notification message
     * @param <T> The type of the message
     * @return A CompletableFuture that completes when the message is published
     */
    public <T> CompletableFuture<Void> publishPushNotification(T message) {
        return publish(PUSH_TOPIC, message);
    }

    /**
     * Publishes a notification message to the web notification topic.
     *
     * @param message The web notification message
     * @param <T> The type of the message
     * @return A CompletableFuture that completes when the message is published
     */
    public <T> CompletableFuture<Void> publishWebNotification(T message) {
        return publish(WEB_TOPIC, message);
    }

    /**
     * Publishes a failed message to the dead letter queue.
     *
     * @param message The failed message
     * @param <T> The type of the message
     * @return A CompletableFuture that completes when the message is published
     */
    public <T> CompletableFuture<Void> publishToDeadLetterQueue(T message) {
        return publish(DEAD_LETTER_TOPIC, message);
    }

    /**
     * Subscribes to the events topic to receive event messages.
     *
     * @param messageHandler The handler for received messages
     * @param messageType The class of the message type
     * @param <T> The type of the message
     */
    public <T> void subscribeToEvents(Consumer<T> messageHandler, Class<T> messageType) {
        brokerClient.subscribe(EVENTS_TOPIC, messageHandler, messageType);
        LOGGER.info("Subscribed to events topic with correlationId: {}", correlationId);
    }
    
    /**
     * Subscribes to the dead letter queue to process failed messages.
     *
     * @param messageHandler The handler for dead letter messages
     */
    public void subscribeToDeadLetterQueue(Consumer<DeadLetterMessage> messageHandler) {
        brokerClient.subscribe(DEAD_LETTER_TOPIC, messageHandler, DeadLetterMessage.class);
        LOGGER.info("Subscribed to dead letter queue with correlationId: {}", correlationId);
    }
    
    /**
     * Attempts to reprocess a message from the dead letter queue.
     *
     * @param deadLetterMessage The dead letter message to reprocess
     * @return A CompletableFuture that completes when the message is reprocessed
     */
    public CompletableFuture<Void> reprocessDeadLetterMessage(DeadLetterMessage deadLetterMessage) {
        if (deadLetterMessage.getRetryCount() >= config.getInteger(Keys.NOTIFICATION_MAX_DLQ_RETRIES, 5)) {
            LOGGER.warn("Maximum retry count reached for message in topic: {} with correlationId: {}", 
                    deadLetterMessage.getOriginalTopic(), deadLetterMessage.getCorrelationId());
            return CompletableFuture.completedFuture(null);
        }
        
        // Create a new dead letter message with incremented retry count
        DeadLetterMessage retryMessage = new DeadLetterMessage(
                deadLetterMessage.getOriginalTopic(),
                deadLetterMessage.getOriginalMessage(),
                deadLetterMessage.getErrorMessage(),
                deadLetterMessage.getCorrelationId(),
                deadLetterMessage.getRetryCount() + 1);
        
        // Calculate exponential backoff delay
        long delay = (long) Math.pow(2, retryMessage.getRetryCount()) * 
                config.getInteger(Keys.NOTIFICATION_RETRY_DELAY, 1000);
        
        LOGGER.info("Reprocessing dead letter message for topic: {} with correlationId: {}, retry: {}, delay: {}ms", 
                retryMessage.getOriginalTopic(), retryMessage.getCorrelationId(), 
                retryMessage.getRetryCount(), delay);
        
        // Schedule reprocessing after delay
        return CompletableFuture.runAsync(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(delay);
                brokerClient.publish(retryMessage.getOriginalTopic(), retryMessage.getOriginalMessage());
                LOGGER.info("Successfully reprocessed dead letter message for topic: {} with correlationId: {}", 
                        retryMessage.getOriginalTopic(), retryMessage.getCorrelationId());
            } catch (Exception e) {
                LOGGER.error("Failed to reprocess dead letter message for topic: {} with correlationId: {}", 
                        retryMessage.getOriginalTopic(), retryMessage.getCorrelationId(), e);
                try {
                    publishToDeadLetterQueue(retryMessage);
                } catch (Exception dlqException) {
                    LOGGER.error("Failed to publish failed retry to dead letter queue", dlqException);
                }
            }
        });
    }

    /**
     * Closes the connection to the message broker.
     */
    public void close() {
        brokerClient.close();
        LOGGER.info("Closed connection to message broker");
    }

    /**
     * Interface for message broker clients (Kafka, RabbitMQ).
     */
    private interface MessageBrokerClient {
        /**
         * Connects to the message broker.
         */
        void connect();

        /**
         * Publishes a message to the specified topic.
         *
         * @param topic The topic to publish to
         * @param message The message to publish
         * @param <T> The type of the message
         * @throws Exception If an error occurs during publishing
         */
        <T> void publish(String topic, T message) throws Exception;

        /**
         * Subscribes to the specified topic to receive messages.
         *
         * @param topic The topic to subscribe to
         * @param messageHandler The handler for received messages
         * @param messageType The class of the message type
         * @param <T> The type of the message
         */
        <T> void subscribe(String topic, Consumer<T> messageHandler, Class<T> messageType);

        /**
         * Closes the connection to the message broker.
         */
        void close();
    }

    /**
     * Kafka implementation of the MessageBrokerClient interface.
     */
    private class KafkaClient implements MessageBrokerClient {
        private final Config config;
        private final ObjectMapper objectMapper;
        private org.apache.kafka.clients.producer.Producer<String, String> producer;

        public KafkaClient(Config config, ObjectMapper objectMapper) {
            this.config = config;
            this.objectMapper = objectMapper;
        }

        @Override
        public void connect() {
            try {
                java.util.Properties properties = new java.util.Properties();
                properties.put("bootstrap.servers", config.getString(Keys.NOTIFICATION_BROKER_URL));
                properties.put("acks", "all");
                properties.put("retries", 3);
                properties.put("batch.size", 16384);
                properties.put("linger.ms", 1);
                properties.put("buffer.memory", 33554432);
                properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
                LOGGER.info("Connected to Kafka broker at {}", config.getString(Keys.NOTIFICATION_BROKER_URL));
            } catch (Exception e) {
                LOGGER.error("Failed to connect to Kafka broker", e);
                throw new RuntimeException("Failed to connect to Kafka broker", e);
            }
        }

        @Override
        public <T> void publish(String topic, T message) throws Exception {
            String key = java.util.UUID.randomUUID().toString();
            String value = objectMapper.writeValueAsString(message);
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, key, value));
        }

        @Override
        public <T> void subscribe(String topic, Consumer<T> messageHandler, Class<T> messageType) {
            // Create a consumer group ID based on the service name
            String groupId = config.getString(Keys.NOTIFICATION_SERVICE_NAME, "notification-service");

            // Start a background thread to consume messages
            Thread consumerThread = new Thread(() -> {
                java.util.Properties properties = new java.util.Properties();
                properties.put("bootstrap.servers", config.getString(Keys.NOTIFICATION_BROKER_URL));
                properties.put("group.id", groupId);
                properties.put("enable.auto.commit", "true");
                properties.put("auto.commit.interval.ms", "1000");
                properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

                try (org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = 
                        new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties)) {
                    consumer.subscribe(java.util.Collections.singletonList(topic));
                    LOGGER.info("Subscribed to Kafka topic: {} with group ID: {}", topic, groupId);

                    while (!Thread.currentThread().isInterrupted()) {
                        org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records = 
                                consumer.poll(java.time.Duration.ofMillis(100));
                        for (org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record : records) {
                            try {
                                consumeTimer.record(() -> {
                                    try {
                                        T value = objectMapper.readValue(record.value(), messageType);
                                        consumeCounter.increment();
                                        messageHandler.accept(value);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                            } catch (Exception e) {
                                consumeErrorCounter.increment();
                                LOGGER.error("Error processing Kafka message from topic {}: {}", 
                                        topic, record.value(), e);
                                try {
                                    // Send to dead letter queue with correlation ID
                                    dlqCounter.increment();
                                    publishToDeadLetterQueue(new DeadLetterMessage(topic, record.value(), e.getMessage(), 
                                            correlationId, 0));
                                } catch (Exception dlqException) {
                                    LOGGER.error("Failed to publish to dead letter queue with correlationId: {}", 
                                            correlationId, dlqException);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Error in Kafka consumer for topic {}", topic, e);
                }
            });
            consumerThread.setDaemon(true);
            consumerThread.start();
        }

        @Override
        public void close() {
            if (producer != null) {
                producer.close();
                producer = null;
            }
        }
    }

    /**
     * RabbitMQ implementation of the MessageBrokerClient interface.
     */
    private class RabbitMqClient implements MessageBrokerClient {
        private final Config config;
        private final ObjectMapper objectMapper;
        private com.rabbitmq.client.Connection connection;
        private com.rabbitmq.client.Channel channel;

        public RabbitMqClient(Config config, ObjectMapper objectMapper) {
            this.config = config;
            this.objectMapper = objectMapper;
        }

        @Override
        public void connect() {
            try {
                com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
                factory.setUri(config.getString(Keys.NOTIFICATION_BROKER_URL));
                connection = factory.newConnection();
                channel = connection.createChannel();

                // Declare all the required topics (exchanges in RabbitMQ)
                channel.exchangeDeclare(EMAIL_TOPIC, "fanout", true);
                channel.exchangeDeclare(SMS_TOPIC, "fanout", true);
                channel.exchangeDeclare(PUSH_TOPIC, "fanout", true);
                channel.exchangeDeclare(WEB_TOPIC, "fanout", true);
                channel.exchangeDeclare(EVENTS_TOPIC, "fanout", true);
                channel.exchangeDeclare(DEAD_LETTER_TOPIC, "fanout", true);

                LOGGER.info("Connected to RabbitMQ broker at {}", config.getString(Keys.NOTIFICATION_BROKER_URL));
            } catch (Exception e) {
                LOGGER.error("Failed to connect to RabbitMQ broker", e);
                throw new RuntimeException("Failed to connect to RabbitMQ broker", e);
            }
        }

        @Override
        public <T> void publish(String topic, T message) throws Exception {
            String messageBody = objectMapper.writeValueAsString(message);
            channel.basicPublish(topic, "", null, messageBody.getBytes());
        }

        @Override
        public <T> void subscribe(String topic, Consumer<T> messageHandler, Class<T> messageType) {
            try {
                // Create a queue with a generated name
                String queueName = channel.queueDeclare().getQueue();
                
                // Bind the queue to the exchange
                channel.queueBind(queueName, topic, "");

                // Set up the consumer
                channel.basicConsume(queueName, true, (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody());
                    try {
                        consumeTimer.record(() -> {
                            try {
                                T value = objectMapper.readValue(message, messageType);
                                consumeCounter.increment();
                                messageHandler.accept(value);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
                    } catch (Exception e) {
                        consumeErrorCounter.increment();
                        LOGGER.error("Error processing RabbitMQ message from topic {}: {}", 
                                topic, message, e);
                        try {
                            // Send to dead letter queue with correlation ID
                            dlqCounter.increment();
                            publishToDeadLetterQueue(new DeadLetterMessage(topic, message, e.getMessage(), 
                                    correlationId, 0));
                        } catch (Exception dlqException) {
                            LOGGER.error("Failed to publish to dead letter queue with correlationId: {}", 
                                    correlationId, dlqException);
                        }
                    }
                }, consumerTag -> { });

                LOGGER.info("Subscribed to RabbitMQ topic: {} with queue: {}", topic, queueName);
            } catch (Exception e) {
                LOGGER.error("Error subscribing to RabbitMQ topic {}", topic, e);
                throw new RuntimeException("Error subscribing to RabbitMQ topic", e);
            }
        }

        @Override
        public void close() {
            try {
                if (channel != null && channel.isOpen()) {
                    channel.close();
                }
                if (connection != null && connection.isOpen()) {
                    connection.close();
                }
            } catch (Exception e) {
                LOGGER.error("Error closing RabbitMQ connection", e);
            }
        }
    }

    /**
     * Represents a message that failed to be processed and is sent to the dead letter queue.
     */
    public static class DeadLetterMessage {
        private final String originalTopic;
        private final String originalMessage;
        private final String errorMessage;
        private final long timestamp;
        private final String correlationId;
        private final int retryCount;

        public DeadLetterMessage(String originalTopic, String originalMessage, String errorMessage) {
            this(originalTopic, originalMessage, errorMessage, java.util.UUID.randomUUID().toString(), 0);
        }
        
        public DeadLetterMessage(String originalTopic, String originalMessage, String errorMessage, 
                                String correlationId, int retryCount) {
            this.originalTopic = originalTopic;
            this.originalMessage = originalMessage;
            this.errorMessage = errorMessage;
            this.timestamp = System.currentTimeMillis();
            this.correlationId = correlationId;
            this.retryCount = retryCount;
        }

        public String getOriginalTopic() {
            return originalTopic;
        }

        public String getOriginalMessage() {
            return originalMessage;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public long getTimestamp() {
            return timestamp;
        }
        
        public String getCorrelationId() {
            return correlationId;
        }
        
        public int getRetryCount() {
            return retryCount;
        }
    }
}