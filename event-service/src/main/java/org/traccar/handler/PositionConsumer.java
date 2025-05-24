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
package org.traccar.handler;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.handler.events.BaseEventHandler;
import org.traccar.model.Position;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Consumes enriched position data from the message broker (Kafka/RabbitMQ) and routes it to
 * the appropriate event handlers in the Event Processing Service.
 */
@Singleton
public class PositionConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionConsumer.class);
    private static final String CONSUMER_GROUP = "event-processors";
    private static final String TOPIC = "enriched.positions";
    private static final String DEAD_LETTER_TOPIC = "dead-letter.positions";
    private static final String CIRCUIT_BREAKER_NAME = "positionProcessing";
    private static final String CORRELATION_ID_HEADER = "X-Correlation-ID";

    private final Config config;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final TextMapPropagator propagator;
    private final CircuitBreaker circuitBreaker;
    private final ExecutorService executorService;
    private final BasePositionHandler.Callback positionCallback;
    private final Map<Long, BaseEventHandler.Callback> eventCallbacks;

    private MessageBrokerClient messageBrokerClient;
    private Counter positionsReceivedCounter;
    private Counter positionsProcessedCounter;
    private Counter positionsFailedCounter;
    private Counter deadLetterCounter;
    private Timer processingTimer;

    /**
     * Constructs a new PositionConsumer with the required dependencies.
     *
     * @param config Configuration for consumer settings
     * @param meterRegistry Registry for metrics collection
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param propagator OpenTelemetry context propagator
     * @param circuitBreakerRegistry Registry for circuit breaker configuration
     * @param eventHandlers Array of event handlers to process positions
     */
    @Inject
    public PositionConsumer(
            Config config,
            MeterRegistry meterRegistry,
            Tracer tracer,
            TextMapPropagator propagator,
            CircuitBreakerRegistry circuitBreakerRegistry,
            BaseEventHandler[] eventHandlers) {

        this.config = config;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        this.propagator = propagator;
        
        // Configure circuit breaker for position processing
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(10)
                .slidingWindowSize(100)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(
                CIRCUIT_BREAKER_NAME, circuitBreakerConfig);
        
        // Create thread pool for processing positions
        int threadPoolSize = config.getInteger(Keys.EVENT_THREAD_POOL_SIZE, 5);
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
        
        // Initialize callback maps
        this.eventCallbacks = new HashMap<>();
        for (BaseEventHandler handler : eventHandlers) {
            registerEventHandler(handler);
        }
        
        // Create position callback
        this.positionCallback = filtered -> {
            // Position processing complete
            positionsProcessedCounter.increment();
        };
    }

    /**
     * Initializes the consumer, metrics, and connects to the message broker.
     */
    @PostConstruct
    public void init() {
        // Initialize metrics
        positionsReceivedCounter = Counter.builder("positions.received")
                .description("Number of positions received from broker")
                .register(meterRegistry);
        
        positionsProcessedCounter = Counter.builder("positions.processed")
                .description("Number of positions successfully processed")
                .register(meterRegistry);
        
        positionsFailedCounter = Counter.builder("positions.failed")
                .description("Number of positions that failed processing")
                .register(meterRegistry);
        
        deadLetterCounter = Counter.builder("positions.deadletter")
                .description("Number of positions sent to dead letter queue")
                .register(meterRegistry);
        
        processingTimer = Timer.builder("positions.processing.time")
                .description("Time taken to process positions")
                .register(meterRegistry);
        
        // Initialize message broker client based on configuration
        String brokerType = config.getString(Keys.EVENT_BROKER_TYPE, "kafka");
        if ("kafka".equalsIgnoreCase(brokerType)) {
            messageBrokerClient = new KafkaClient();
        } else if ("rabbitmq".equalsIgnoreCase(brokerType)) {
            messageBrokerClient = new RabbitMQClient();
        } else {
            throw new IllegalArgumentException("Unsupported broker type: " + brokerType);
        }
        
        // Connect to message broker and start consuming
        try {
            messageBrokerClient.connect();
            messageBrokerClient.subscribe(TOPIC, CONSUMER_GROUP, this::processMessage);
            LOGGER.info("Position consumer started and subscribed to topic: {}", TOPIC);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize position consumer", e);
            throw new RuntimeException("Failed to initialize position consumer", e);
        }
    }

    /**
     * Cleans up resources when the consumer is shutting down.
     */
    @PreDestroy
    public void shutdown() {
        try {
            // Shutdown message broker client
            if (messageBrokerClient != null) {
                messageBrokerClient.disconnect();
            }
            
            // Shutdown executor service
            executorService.shutdown();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (Exception e) {
            LOGGER.warn("Error during position consumer shutdown", e);
        }
    }

    /**
     * Processes a message received from the message broker.
     *
     * @param message The message containing position data and headers
     */
    private void processMessage(BrokerMessage message) {
        positionsReceivedCounter.increment();
        
        // Extract correlation ID from message headers for distributed tracing
        Context context = propagator.extract(Context.current(), message.getHeaders(), new TextMapGetter<Map<String, String>>() {
            @Override
            public Iterable<String> keys(Map<String, String> headers) {
                return headers.keySet();
            }

            @Override
            public String get(Map<String, String> headers, String key) {
                return headers.get(key);
            }
        });
        
        // Create a span for position processing
        Span span = tracer.spanBuilder("process-position")
                .setParent(context)
                .setSpanKind(SpanKind.CONSUMER)
                .startSpan();
        
        try {
            // Deserialize position from message
            Position position = deserializePosition(message.getPayload());
            if (position == null) {
                LOGGER.warn("Received null position from broker, skipping");
                positionsFailedCounter.increment();
                return;
            }
            
            // Process position with circuit breaker
            Timer.Sample sample = Timer.start(meterRegistry);
            
            Supplier<CompletableFuture<Void>> positionProcessingSupplier = () -> 
                CompletableFuture.runAsync(() -> {
                    try {
                        // Route position to all registered event handlers
                        for (BaseEventHandler handler : getEventHandlers()) {
                            handler.analyzePosition(position, eventCallbacks.get(handler.hashCode()));
                        }
                    } catch (Exception e) {
                        LOGGER.error("Error processing position", e);
                        throw e;
                    }
                }, executorService);
            
            Function<Throwable, CompletableFuture<Void>> fallbackFunction = throwable -> {
                LOGGER.error("Circuit breaker open, sending to dead letter queue", throwable);
                positionsFailedCounter.increment();
                deadLetterCounter.increment();
                sendToDeadLetterQueue(message);
                return CompletableFuture.completedFuture(null);
            };
            
            // Execute with circuit breaker
            circuitBreaker.executeCompletionStage(positionProcessingSupplier, fallbackFunction).toCompletableFuture().join();
            
            // Record processing time
            sample.stop(processingTimer);
            
            // Acknowledge message
            messageBrokerClient.acknowledge(message);
            
        } catch (Exception e) {
            LOGGER.error("Failed to process position message", e);
            positionsFailedCounter.increment();
            
            // Send to dead letter queue after max retries
            if (message.getRetryCount() >= config.getInteger(Keys.EVENT_MAX_RETRY_COUNT, 3)) {
                deadLetterCounter.increment();
                sendToDeadLetterQueue(message);
            } else {
                // Retry with backoff
                messageBrokerClient.retry(message);
            }
        } finally {
            span.end();
        }
    }

    /**
     * Sends a failed message to the dead letter queue.
     *
     * @param message The message that failed processing
     */
    private void sendToDeadLetterQueue(BrokerMessage message) {
        try {
            messageBrokerClient.sendToDeadLetterQueue(message, DEAD_LETTER_TOPIC);
        } catch (Exception e) {
            LOGGER.error("Failed to send message to dead letter queue", e);
        }
    }

    /**
     * Deserializes a position from the message payload.
     *
     * @param payload The message payload
     * @return The deserialized Position object
     */
    private Position deserializePosition(byte[] payload) {
        try {
            // Implementation depends on serialization format (JSON, Protocol Buffers, etc.)
            // For this example, we'll assume a simple deserialization
            return Position.fromJson(new String(payload));
        } catch (Exception e) {
            LOGGER.error("Failed to deserialize position", e);
            return null;
        }
    }

    /**
     * Registers an event handler to receive positions.
     *
     * @param handler The event handler to register
     */
    private void registerEventHandler(BaseEventHandler handler) {
        eventCallbacks.put(handler.hashCode(), event -> {
            // Event detected by handler
            LOGGER.debug("Event detected: {}", event.getType());
            // In a real implementation, this would publish the event to a broker
        });
    }

    /**
     * Gets all registered event handlers.
     *
     * @return Array of registered event handlers
     */
    private BaseEventHandler[] getEventHandlers() {
        return eventCallbacks.keySet().stream()
                .map(key -> (BaseEventHandler) key)
                .toArray(BaseEventHandler[]::new);
    }

    /**
     * Interface for message broker client implementations.
     */
    private interface MessageBrokerClient {
        void connect() throws Exception;
        void disconnect() throws Exception;
        void subscribe(String topic, String consumerGroup, MessageHandler handler) throws Exception;
        void acknowledge(BrokerMessage message) throws Exception;
        void retry(BrokerMessage message) throws Exception;
        void sendToDeadLetterQueue(BrokerMessage message, String deadLetterTopic) throws Exception;
    }

    /**
     * Interface for message handling.
     */
    private interface MessageHandler {
        void handle(BrokerMessage message);
    }

    /**
     * Represents a message from the broker.
     */
    private static class BrokerMessage {
        private final byte[] payload;
        private final Map<String, String> headers;
        private final int retryCount;

        public BrokerMessage(byte[] payload, Map<String, String> headers, int retryCount) {
            this.payload = payload;
            this.headers = headers;
            this.retryCount = retryCount;
        }

        public byte[] getPayload() {
            return payload;
        }

        public Map<String, String> getHeaders() {
            return headers;
        }

        public int getRetryCount() {
            return retryCount;
        }
    }

    /**
     * Kafka implementation of the MessageBrokerClient.
     */
    private class KafkaClient implements MessageBrokerClient {
        // Implementation would use KafkaConsumer and KafkaProducer
        @Override
        public void connect() throws Exception {
            // Initialize Kafka client
            LOGGER.info("Connecting to Kafka broker");
        }

        @Override
        public void disconnect() throws Exception {
            // Close Kafka client
            LOGGER.info("Disconnecting from Kafka broker");
        }

        @Override
        public void subscribe(String topic, String consumerGroup, MessageHandler handler) throws Exception {
            // Subscribe to Kafka topic with consumer group
            LOGGER.info("Subscribing to Kafka topic: {} with consumer group: {}", topic, consumerGroup);
        }

        @Override
        public void acknowledge(BrokerMessage message) throws Exception {
            // Commit offset in Kafka
            LOGGER.debug("Acknowledging Kafka message");
        }

        @Override
        public void retry(BrokerMessage message) throws Exception {
            // Retry logic for Kafka
            LOGGER.debug("Retrying Kafka message");
        }

        @Override
        public void sendToDeadLetterQueue(BrokerMessage message, String deadLetterTopic) throws Exception {
            // Send to Kafka dead letter topic
            LOGGER.info("Sending message to Kafka dead letter topic: {}", deadLetterTopic);
        }
    }

    /**
     * RabbitMQ implementation of the MessageBrokerClient.
     */
    private class RabbitMQClient implements MessageBrokerClient {
        // Implementation would use RabbitMQ client
        @Override
        public void connect() throws Exception {
            // Initialize RabbitMQ client
            LOGGER.info("Connecting to RabbitMQ broker");
        }

        @Override
        public void disconnect() throws Exception {
            // Close RabbitMQ client
            LOGGER.info("Disconnecting from RabbitMQ broker");
        }

        @Override
        public void subscribe(String topic, String consumerGroup, MessageHandler handler) throws Exception {
            // Subscribe to RabbitMQ queue
            LOGGER.info("Subscribing to RabbitMQ queue: {} with consumer tag: {}", topic, consumerGroup);
        }

        @Override
        public void acknowledge(BrokerMessage message) throws Exception {
            // Acknowledge RabbitMQ message
            LOGGER.debug("Acknowledging RabbitMQ message");
        }

        @Override
        public void retry(BrokerMessage message) throws Exception {
            // Retry logic for RabbitMQ
            LOGGER.debug("Retrying RabbitMQ message");
        }

        @Override
        public void sendToDeadLetterQueue(BrokerMessage message, String deadLetterTopic) throws Exception {
            // Send to RabbitMQ dead letter exchange
            LOGGER.info("Sending message to RabbitMQ dead letter exchange: {}", deadLetterTopic);
        }
    }
}