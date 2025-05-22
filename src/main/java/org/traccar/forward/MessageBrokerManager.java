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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Manages connections to message brokers (Kafka/RabbitMQ) and provides a unified interface
 * for publishing and consuming messages across the microservices architecture.
 * <p>
 * This component is critical for asynchronous communication between services, enabling reliable
 * message delivery with configurable retry policies, circuit breaking, and distributed tracing
 * context propagation.
 */
@Singleton
public class MessageBrokerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBrokerManager.class);

    private final Config config;
    private final ObjectMapper objectMapper;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final Executor asyncExecutor;

    private final Map<String, MessageProducer> producers = new HashMap<>();
    private final Map<String, MessageConsumer> consumers = new HashMap<>();

    private enum BrokerType {
        KAFKA, RABBITMQ
    }

    /**
     * Creates a new MessageBrokerManager with the specified dependencies.
     *
     * @param config              The application configuration
     * @param objectMapper        JSON serializer/deserializer
     * @param circuitBreakerRegistry Circuit breaker registry for resilience patterns
     * @param retryRegistry       Retry registry for resilience patterns
     * @param meterRegistry       Metrics registry for monitoring
     * @param tracer              OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public MessageBrokerManager(
            Config config,
            ObjectMapper objectMapper,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry,
            MeterRegistry meterRegistry,
            Tracer tracer) {
        this.config = config;
        this.objectMapper = objectMapper;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.retryRegistry = retryRegistry;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        this.asyncExecutor = Executors.newVirtualThreadPerTaskExecutor();

        // Initialize circuit breakers and retry policies if not already configured
        initializeCircuitBreakers();
        initializeRetryPolicies();
    }

    /**
     * Initializes circuit breakers for message broker operations if they don't already exist.
     */
    private void initializeCircuitBreakers() {
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();

        try {
            circuitBreakerRegistry.circuitBreaker("messageBroker", circuitBreakerConfig);
        } catch (IllegalArgumentException e) {
            // Circuit breaker already exists with custom configuration
            LOGGER.debug("Using existing circuit breaker configuration for message broker");
        }
    }

    /**
     * Initializes retry policies for message broker operations if they don't already exist.
     */
    private void initializeRetryPolicies() {
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(Exception.class)
                .ignoreExceptions(InterruptedException.class)
                .build();

        try {
            retryRegistry.retry("messageBroker", retryConfig);
        } catch (IllegalArgumentException e) {
            // Retry already exists with custom configuration
            LOGGER.debug("Using existing retry configuration for message broker");
        }
    }

    /**
     * Creates a message producer for the specified topic.
     *
     * @param topic The topic to publish messages to
     * @return A message producer for the specified topic
     */
    public MessageProducer createProducer(String topic) {
        if (producers.containsKey(topic)) {
            return producers.get(topic);
        }

        BrokerType brokerType = determineBrokerType();
        MessageProducer producer;

        switch (brokerType) {
            case KAFKA:
                producer = new KafkaMessageProducer(topic, config, objectMapper, 
                        circuitBreakerRegistry, retryRegistry, meterRegistry, tracer);
                break;
            case RABBITMQ:
                producer = new RabbitMqMessageProducer(topic, config, objectMapper, 
                        circuitBreakerRegistry, retryRegistry, meterRegistry, tracer);
                break;
            default:
                throw new IllegalStateException("Unsupported broker type: " + brokerType);
        }

        producers.put(topic, producer);
        return producer;
    }

    /**
     * Creates a message consumer for the specified topic.
     *
     * @param topic    The topic to consume messages from
     * @param groupId  The consumer group ID
     * @return A message consumer for the specified topic
     */
    public MessageConsumer createConsumer(String topic, String groupId) {
        String key = topic + "-" + groupId;
        if (consumers.containsKey(key)) {
            return consumers.get(key);
        }

        BrokerType brokerType = determineBrokerType();
        MessageConsumer consumer;

        switch (brokerType) {
            case KAFKA:
                consumer = new KafkaMessageConsumer(topic, groupId, config, objectMapper, 
                        meterRegistry, tracer);
                break;
            case RABBITMQ:
                consumer = new RabbitMqMessageConsumer(topic, groupId, config, objectMapper, 
                        meterRegistry, tracer);
                break;
            default:
                throw new IllegalStateException("Unsupported broker type: " + brokerType);
        }

        consumers.put(key, consumer);
        return consumer;
    }

    /**
     * Determines the broker type to use based on configuration.
     *
     * @return The broker type to use
     */
    private BrokerType determineBrokerType() {
        String brokerType = config.getString(Keys.BROKER_TYPE, "kafka").toLowerCase();
        switch (brokerType) {
            case "kafka":
                return BrokerType.KAFKA;
            case "rabbitmq":
            case "amqp":
                return BrokerType.RABBITMQ;
            default:
                LOGGER.warn("Unknown broker type: {}, defaulting to Kafka", brokerType);
                return BrokerType.KAFKA;
        }
    }

    /**
     * Closes all producers and consumers managed by this broker manager.
     */
    public void close() {
        producers.values().forEach(MessageProducer::close);
        consumers.values().forEach(MessageConsumer::close);
        producers.clear();
        consumers.clear();
    }

    /**
     * Interface for message producers that can publish messages to a topic.
     */
    public interface MessageProducer {
        /**
         * Publishes a message to the topic asynchronously.
         *
         * @param message The message to publish
         * @param <T>     The type of the message
         * @return A CompletableFuture that completes when the message is published
         */
        <T> CompletableFuture<Void> publish(T message);

        /**
         * Publishes a message to the topic asynchronously with a specific key.
         *
         * @param key     The message key for partitioning/routing
         * @param message The message to publish
         * @param <T>     The type of the message
         * @return A CompletableFuture that completes when the message is published
         */
        <T> CompletableFuture<Void> publish(String key, T message);

        /**
         * Publishes a message to the topic asynchronously with headers for context propagation.
         *
         * @param key     The message key for partitioning/routing
         * @param message The message to publish
         * @param headers Additional headers to include with the message
         * @param <T>     The type of the message
         * @return A CompletableFuture that completes when the message is published
         */
        <T> CompletableFuture<Void> publish(String key, T message, Map<String, String> headers);

        /**
         * Closes the producer and releases any resources.
         */
        void close();
    }

    /**
     * Interface for message consumers that can consume messages from a topic.
     */
    public interface MessageConsumer {
        /**
         * Subscribes to messages of the specified type with the given message handler.
         *
         * @param messageType    The class of the message type to consume
         * @param messageHandler The handler to process received messages
         * @param <T>           The type of the message
         */
        <T> void subscribe(Class<T> messageType, MessageHandler<T> messageHandler);

        /**
         * Starts consuming messages.
         */
        void start();

        /**
         * Stops consuming messages.
         */
        void stop();

        /**
         * Closes the consumer and releases any resources.
         */
        void close();
    }

    /**
     * Interface for message handlers that process consumed messages.
     *
     * @param <T> The type of the message
     */
    public interface MessageHandler<T> {
        /**
         * Handles a received message.
         *
         * @param key     The message key
         * @param message The message payload
         * @param headers The message headers
         */
        void handle(String key, T message, Map<String, String> headers);
    }

    /**
     * Abstract base class for message producers with common functionality.
     */
    private abstract class AbstractMessageProducer implements MessageProducer {
        protected final String topic;
        protected final ObjectMapper objectMapper;
        protected final CircuitBreaker circuitBreaker;
        protected final Retry retry;
        protected final Timer publishTimer;
        protected final Counter publishSuccessCounter;
        protected final Counter publishFailureCounter;
        protected final Tracer tracer;

        protected AbstractMessageProducer(
                String topic,
                ObjectMapper objectMapper,
                CircuitBreakerRegistry circuitBreakerRegistry,
                RetryRegistry retryRegistry,
                MeterRegistry meterRegistry,
                Tracer tracer) {
            this.topic = topic;
            this.objectMapper = objectMapper;
            this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("messageBroker");
            this.retry = retryRegistry.retry("messageBroker");
            this.tracer = tracer;

            // Initialize metrics
            this.publishTimer = Timer.builder("message.publish.time")
                    .tag("topic", topic)
                    .tag("broker", getBrokerType())
                    .register(meterRegistry);
            this.publishSuccessCounter = Counter.builder("message.publish.success")
                    .tag("topic", topic)
                    .tag("broker", getBrokerType())
                    .register(meterRegistry);
            this.publishFailureCounter = Counter.builder("message.publish.failure")
                    .tag("topic", topic)
                    .tag("broker", getBrokerType())
                    .register(meterRegistry);
        }

        @Override
        public <T> CompletableFuture<Void> publish(T message) {
            return publish(null, message, new HashMap<>());
        }

        @Override
        public <T> CompletableFuture<Void> publish(String key, T message) {
            return publish(key, message, new HashMap<>());
        }

        @Override
        public <T> CompletableFuture<Void> publish(String key, T message, Map<String, String> headers) {
            // Create a span for the publish operation
            Span span = tracer.spanBuilder("publish_" + topic)
                    .setSpanKind(SpanKind.PRODUCER)
                    .startSpan();

            try {
                // Inject the current context into the message headers for propagation
                injectTraceContext(span, headers);

                // Wrap the publish operation with circuit breaker and retry
                Supplier<CompletableFuture<Void>> publishSupplier = () -> {
                    Timer.Sample sample = Timer.start();
                    try {
                        return doPublish(key, message, headers)
                                .thenApply(result -> {
                                    publishSuccessCounter.increment();
                                    sample.stop(publishTimer);
                                    span.end();
                                    return result;
                                })
                                .exceptionally(ex -> {
                                    publishFailureCounter.increment();
                                    sample.stop(publishTimer);
                                    span.recordException(ex);
                                    span.end();
                                    throw new RuntimeException(ex);
                                });
                    } catch (Exception e) {
                        publishFailureCounter.increment();
                        sample.stop(publishTimer);
                        span.recordException(e);
                        span.end();
                        throw e;
                    }
                };

                // Apply circuit breaker and retry patterns
                return CompletableFuture.supplyAsync(
                        Retry.decorateSupplier(retry, 
                                CircuitBreaker.decorateSupplier(circuitBreaker, publishSupplier)),
                        asyncExecutor)
                        .thenCompose(future -> future);
            } catch (Exception e) {
                span.recordException(e);
                span.end();
                CompletableFuture<Void> future = new CompletableFuture<>();
                future.completeExceptionally(e);
                return future;
            }
        }

        /**
         * Injects the current trace context into the message headers.
         *
         * @param span    The current span
         * @param headers The headers to inject the context into
         */
        protected void injectTraceContext(Span span, Map<String, String> headers) {
            tracer.getOpenTelemetry().getPropagators().getTextMapPropagator()
                    .inject(Context.current().with(span), headers, new TextMapSetter<Map<String, String>>() {
                        @Override
                        public void set(Map<String, String> carrier, String key, String value) {
                            carrier.put(key, value);
                        }
                    });
        }

        /**
         * Performs the actual publish operation to the message broker.
         *
         * @param key     The message key
         * @param message The message payload
         * @param headers The message headers
         * @param <T>     The type of the message
         * @return A CompletableFuture that completes when the message is published
         */
        protected abstract <T> CompletableFuture<Void> doPublish(String key, T message, Map<String, String> headers);

        /**
         * Gets the broker type for metrics tagging.
         *
         * @return The broker type as a string
         */
        protected abstract String getBrokerType();
    }

    /**
     * Kafka implementation of the MessageProducer interface.
     */
    private class KafkaMessageProducer extends AbstractMessageProducer {
        private final org.apache.kafka.clients.producer.Producer<String, String> kafkaProducer;

        public KafkaMessageProducer(
                String topic,
                Config config,
                ObjectMapper objectMapper,
                CircuitBreakerRegistry circuitBreakerRegistry,
                RetryRegistry retryRegistry,
                MeterRegistry meterRegistry,
                Tracer tracer) {
            super(topic, objectMapper, circuitBreakerRegistry, retryRegistry, meterRegistry, tracer);

            // Initialize Kafka producer
            java.util.Properties properties = new java.util.Properties();
            properties.put("bootstrap.servers", config.getString(Keys.BROKER_URL));
            properties.put("acks", config.getString(Keys.BROKER_ACKS, "all"));
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("enable.idempotence", config.getBoolean(Keys.BROKER_IDEMPOTENCE, true));
            properties.put("retries", config.getInteger(Keys.BROKER_RETRIES, 3));
            properties.put("max.in.flight.requests.per.connection", 
                    config.getInteger(Keys.BROKER_MAX_IN_FLIGHT, 5));

            // Add any additional Kafka-specific properties from configuration
            for (Object key : config.getKeys()) {
                String keyStr = key.toString();
                if (keyStr.startsWith("kafka.")) {
                    String kafkaKey = keyStr.substring("kafka.".length());
                    properties.put(kafkaKey, config.getString(keyStr));
                }
            }

            this.kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
        }

        @Override
        protected <T> CompletableFuture<Void> doPublish(String key, T message, Map<String, String> headers) {
            CompletableFuture<Void> future = new CompletableFuture<>();

            try {
                String messageKey = key != null ? key : "";
                String messageValue = objectMapper.writeValueAsString(message);

                // Create Kafka record with headers
                org.apache.kafka.clients.producer.ProducerRecord<String, String> record = 
                        new org.apache.kafka.clients.producer.ProducerRecord<>(topic, messageKey, messageValue);

                // Add headers to the record
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    record.headers().add(entry.getKey(), entry.getValue().getBytes());
                }

                // Send the record asynchronously
                kafkaProducer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        future.completeExceptionally(exception);
                    } else {
                        future.complete(null);
                    }
                });
            } catch (Exception e) {
                future.completeExceptionally(e);
            }

            return future;
        }

        @Override
        protected String getBrokerType() {
            return "kafka";
        }

        @Override
        public void close() {
            kafkaProducer.close();
        }
    }

    /**
     * RabbitMQ implementation of the MessageProducer interface.
     */
    private class RabbitMqMessageProducer extends AbstractMessageProducer {
        private final AmqpClient amqpClient;

        public RabbitMqMessageProducer(
                String topic,
                Config config,
                ObjectMapper objectMapper,
                CircuitBreakerRegistry circuitBreakerRegistry,
                RetryRegistry retryRegistry,
                MeterRegistry meterRegistry,
                Tracer tracer) {
            super(topic, objectMapper, circuitBreakerRegistry, retryRegistry, meterRegistry, tracer);

            String connectionUrl = config.getString(Keys.BROKER_URL);
            String exchange = config.getString(Keys.BROKER_EXCHANGE, "traccar");
            this.amqpClient = new AmqpClient(connectionUrl, exchange, topic);
        }

        @Override
        protected <T> CompletableFuture<Void> doPublish(String key, T message, Map<String, String> headers) {
            CompletableFuture<Void> future = new CompletableFuture<>();

            try {
                String messageValue = objectMapper.writeValueAsString(message);
                amqpClient.publishMessage(messageValue, headers, key);
                future.complete(null);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }

            return future;
        }

        @Override
        protected String getBrokerType() {
            return "rabbitmq";
        }

        @Override
        public void close() {
            amqpClient.close();
        }
    }

    /**
     * Abstract base class for message consumers with common functionality.
     */
    private abstract class AbstractMessageConsumer implements MessageConsumer {
        protected final String topic;
        protected final String groupId;
        protected final ObjectMapper objectMapper;
        protected final Counter consumeCounter;
        protected final Counter errorCounter;
        protected final Timer processTimer;
        protected final Tracer tracer;

        protected AbstractMessageConsumer(
                String topic,
                String groupId,
                ObjectMapper objectMapper,
                MeterRegistry meterRegistry,
                Tracer tracer) {
            this.topic = topic;
            this.groupId = groupId;
            this.objectMapper = objectMapper;
            this.tracer = tracer;

            // Initialize metrics
            this.consumeCounter = Counter.builder("message.consume.count")
                    .tag("topic", topic)
                    .tag("group", groupId)
                    .tag("broker", getBrokerType())
                    .register(meterRegistry);
            this.errorCounter = Counter.builder("message.consume.errors")
                    .tag("topic", topic)
                    .tag("group", groupId)
                    .tag("broker", getBrokerType())
                    .register(meterRegistry);
            this.processTimer = Timer.builder("message.process.time")
                    .tag("topic", topic)
                    .tag("group", groupId)
                    .tag("broker", getBrokerType())
                    .register(meterRegistry);
        }

        /**
         * Gets the broker type for metrics tagging.
         *
         * @return The broker type as a string
         */
        protected abstract String getBrokerType();
    }

    /**
     * Kafka implementation of the MessageConsumer interface.
     */
    private class KafkaMessageConsumer extends AbstractMessageConsumer {
        private final org.apache.kafka.clients.consumer.Consumer<String, String> kafkaConsumer;
        private final Map<Class<?>, MessageHandler<?>> handlers = new HashMap<>();
        private volatile boolean running = false;
        private Thread consumerThread;

        public KafkaMessageConsumer(
                String topic,
                String groupId,
                Config config,
                ObjectMapper objectMapper,
                MeterRegistry meterRegistry,
                Tracer tracer) {
            super(topic, groupId, objectMapper, meterRegistry, tracer);

            // Initialize Kafka consumer
            java.util.Properties properties = new java.util.Properties();
            properties.put("bootstrap.servers", config.getString(Keys.BROKER_URL));
            properties.put("group.id", groupId);
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("enable.auto.commit", config.getBoolean(Keys.BROKER_AUTO_COMMIT, false));
            properties.put("auto.offset.reset", config.getString(Keys.BROKER_OFFSET_RESET, "earliest"));

            // Add any additional Kafka-specific properties from configuration
            for (Object key : config.getKeys()) {
                String keyStr = key.toString();
                if (keyStr.startsWith("kafka.")) {
                    String kafkaKey = keyStr.substring("kafka.".length());
                    properties.put(kafkaKey, config.getString(keyStr));
                }
            }

            this.kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
        }

        @Override
        public <T> void subscribe(Class<T> messageType, MessageHandler<T> messageHandler) {
            handlers.put(messageType, messageHandler);
        }

        @Override
        public void start() {
            if (running) {
                return;
            }

            running = true;
            kafkaConsumer.subscribe(java.util.Collections.singletonList(topic));

            consumerThread = new Thread(() -> {
                try {
                    while (running) {
                        try {
                            org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records = 
                                    kafkaConsumer.poll(java.time.Duration.ofMillis(100));

                            for (org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record : records) {
                                processRecord(record);
                            }

                            if (!config.getBoolean(Keys.BROKER_AUTO_COMMIT, false)) {
                                kafkaConsumer.commitSync();
                            }
                        } catch (Exception e) {
                            errorCounter.increment();
                            LOGGER.error("Error processing Kafka messages", e);
                        }
                    }
                } finally {
                    kafkaConsumer.close();
                }
            });

            consumerThread.setName("kafka-consumer-" + topic + "-" + groupId);
            consumerThread.start();
        }

        private void processRecord(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record) {
            consumeCounter.increment();
            Timer.Sample sample = Timer.start();

            // Extract trace context from headers
            Map<String, String> headers = new HashMap<>();
            record.headers().forEach(header -> 
                    headers.put(header.key(), new String(header.value())));

            // Extract trace context and create a span
            io.opentelemetry.context.Context context = extractTraceContext(headers);
            Span span = tracer.spanBuilder("consume_" + topic)
                    .setSpanKind(SpanKind.CONSUMER)
                    .setParent(context)
                    .startSpan();

            try {
                // Process the message with the appropriate handler
                for (Map.Entry<Class<?>, MessageHandler<?>> entry : handlers.entrySet()) {
                    Class<?> messageType = entry.getKey();
                    try {
                        Object message = objectMapper.readValue(record.value(), messageType);
                        processMessage(record.key(), message, headers, entry.getValue());
                    } catch (Exception e) {
                        errorCounter.increment();
                        span.recordException(e);
                        LOGGER.error("Error processing message of type {}", messageType.getName(), e);
                    }
                }
            } finally {
                sample.stop(processTimer);
                span.end();
            }
        }

        @SuppressWarnings("unchecked")
        private <T> void processMessage(String key, Object message, Map<String, String> headers, 
                                       MessageHandler<?> handler) {
            ((MessageHandler<T>) handler).handle(key, (T) message, headers);
        }

        private io.opentelemetry.context.Context extractTraceContext(Map<String, String> headers) {
            return tracer.getOpenTelemetry().getPropagators().getTextMapPropagator()
                    .extract(io.opentelemetry.context.Context.current(), headers, 
                            (carrier, key) -> carrier.get(key));
        }

        @Override
        public void stop() {
            running = false;
            if (consumerThread != null) {
                consumerThread.interrupt();
                try {
                    consumerThread.join(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void close() {
            stop();
            kafkaConsumer.close();
        }

        @Override
        protected String getBrokerType() {
            return "kafka";
        }
    }

    /**
     * RabbitMQ implementation of the MessageConsumer interface.
     */
    private class RabbitMqMessageConsumer extends AbstractMessageConsumer {
        private final AmqpClient amqpClient;
        private final Map<Class<?>, MessageHandler<?>> handlers = new HashMap<>();
        private volatile boolean running = false;

        public RabbitMqMessageConsumer(
                String topic,
                String groupId,
                Config config,
                ObjectMapper objectMapper,
                MeterRegistry meterRegistry,
                Tracer tracer) {
            super(topic, groupId, objectMapper, meterRegistry, tracer);

            String connectionUrl = config.getString(Keys.BROKER_URL);
            String exchange = config.getString(Keys.BROKER_EXCHANGE, "traccar");
            this.amqpClient = new AmqpClient(connectionUrl, exchange, topic);
        }

        @Override
        public <T> void subscribe(Class<T> messageType, MessageHandler<T> messageHandler) {
            handlers.put(messageType, messageHandler);
        }

        @Override
        public void start() {
            if (running) {
                return;
            }

            running = true;
            amqpClient.setMessageHandler((message, properties) -> {
                consumeCounter.increment();
                Timer.Sample sample = Timer.start();

                // Extract headers from AMQP properties
                Map<String, String> headers = new HashMap<>();
                if (properties.getHeaders() != null) {
                    for (Map.Entry<String, Object> entry : properties.getHeaders().entrySet()) {
                        if (entry.getValue() instanceof byte[]) {
                            headers.put(entry.getKey(), new String((byte[]) entry.getValue()));
                        } else if (entry.getValue() != null) {
                            headers.put(entry.getKey(), entry.getValue().toString());
                        }
                    }
                }

                // Extract trace context and create a span
                io.opentelemetry.context.Context context = extractTraceContext(headers);
                Span span = tracer.spanBuilder("consume_" + topic)
                        .setSpanKind(SpanKind.CONSUMER)
                        .setParent(context)
                        .startSpan();

                try {
                    // Process the message with the appropriate handler
                    for (Map.Entry<Class<?>, MessageHandler<?>> entry : handlers.entrySet()) {
                        Class<?> messageType = entry.getKey();
                        try {
                            Object messageObj = objectMapper.readValue(message, messageType);
                            String key = properties.getCorrelationId();
                            processMessage(key, messageObj, headers, entry.getValue());
                        } catch (Exception e) {
                            errorCounter.increment();
                            span.recordException(e);
                            LOGGER.error("Error processing message of type {}", messageType.getName(), e);
                        }
                    }
                } finally {
                    sample.stop(processTimer);
                    span.end();
                }
            });

            amqpClient.startConsuming(groupId);
        }

        @SuppressWarnings("unchecked")
        private <T> void processMessage(String key, Object message, Map<String, String> headers, 
                                       MessageHandler<?> handler) {
            ((MessageHandler<T>) handler).handle(key, (T) message, headers);
        }

        private io.opentelemetry.context.Context extractTraceContext(Map<String, String> headers) {
            return tracer.getOpenTelemetry().getPropagators().getTextMapPropagator()
                    .extract(io.opentelemetry.context.Context.current(), headers, 
                            (carrier, key) -> carrier.get(key));
        }

        @Override
        public void stop() {
            running = false;
            amqpClient.stopConsuming();
        }

        @Override
        public void close() {
            stop();
            amqpClient.close();
        }

        @Override
        protected String getBrokerType() {
            return "rabbitmq";
        }
    }
}