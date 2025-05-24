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
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.handler.events.BaseEventHandler;
import org.traccar.messaging.MessageConverter;
import org.traccar.model.Event;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * EventProducer is responsible for publishing detected events to the message broker
 * (Kafka/RabbitMQ) for consumption by the Notification Service. It provides a standardized
 * interface for all event handlers to publish events with proper serialization, topic routing,
 * and delivery guarantees.
 * 
 * This class implements the {@link BaseEventHandler.Callback} interface to receive events
 * from event handlers and publish them to the message broker. It uses the transaction outbox
 * pattern to ensure reliable delivery, even in the face of service failures.
 * 
 * Key features:
 * - Correlation ID propagation for distributed tracing
 * - Metrics collection for monitoring event production rates and types
 * - Circuit breaker pattern for resilient message publishing
 * - Transaction outbox pattern for reliable message delivery
 * - Support for different event types with appropriate topic routing
 * - Retry mechanism for failed message publishing
 */
@Singleton
public class EventProducer implements BaseEventHandler.Callback {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProducer.class);

    private static final String CIRCUIT_BREAKER_NAME = "eventProducer";
    private static final String RETRY_NAME = "eventProducer";
    private static final String EVENTS_TOPIC = "events";
    private static final String OUTBOX_TABLE = "event_outbox";
    
    // Configuration keys (should be defined in Keys class in a real implementation)
    private static final String KEY_EVENT_BROKER_TYPE = "event.broker.type";
    private static final String KEY_EVENT_PRODUCER_THREAD_POOL_SIZE = "event.producer.threadPoolSize";
    private static final String KEY_EVENT_OUTBOX_INITIAL_DELAY = "event.outbox.initialDelay";
    private static final String KEY_EVENT_OUTBOX_INTERVAL = "event.outbox.interval";

    // OpenTelemetry attribute keys
    private static final AttributeKey<String> EVENT_TYPE_KEY = AttributeKey.stringKey("event.type");
    private static final AttributeKey<Long> DEVICE_ID_KEY = AttributeKey.longKey("device.id");
    private static final AttributeKey<String> CORRELATION_ID_KEY = AttributeKey.stringKey("correlation.id");
    private static final AttributeKey<String> BROKER_TOPIC_KEY = AttributeKey.stringKey("broker.topic");
    private static final AttributeKey<String> BROKER_TYPE_KEY = AttributeKey.stringKey("broker.type");

    private final Config config;
    private final Storage storage;
    private final MessageConverter messageConverter;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Tracer tracer;
    private final Meter meter;
    private final LongCounter eventCounter;
    private final Executor asyncExecutor;
    private final String brokerType;

    /**
     * Constructs a new EventProducer with the necessary dependencies.
     *
     * @param config Configuration for the event producer
     * @param storage Storage for the transaction outbox pattern
     * @param messageConverter Converter for serializing events to broker messages
     * @param circuitBreakerRegistry Registry for creating circuit breakers
     * @param retryRegistry Registry for creating retry policies
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meter OpenTelemetry meter for metrics collection
     */
    @Inject
    public EventProducer(
            Config config,
            Storage storage,
            MessageConverter messageConverter,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry,
            Tracer tracer,
            Meter meter) {

        this.config = config;
        this.storage = storage;
        this.messageConverter = messageConverter;
        this.tracer = tracer;
        this.meter = meter;
        this.brokerType = config.getString(KEY_EVENT_BROKER_TYPE, "kafka");

        // Configure and create circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME, circuitBreakerConfig);

        // Configure and create retry policy
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(CompletionException.class, StorageException.class)
                .build();
        this.retry = retryRegistry.retry(RETRY_NAME, retryConfig);

        // Create metrics
        this.eventCounter = meter.counterBuilder("events.published")
                .setDescription("Number of events published to the message broker")
                .build();

        // Create executor for async operations
        this.asyncExecutor = Executors.newFixedThreadPool(
                config.getInteger(KEY_EVENT_PRODUCER_THREAD_POOL_SIZE, 5));
    }

    /**
     * Publishes an event to the message broker asynchronously with the transaction outbox pattern
     * for reliable delivery. The event is first stored in the outbox table and then published to
     * the broker. If the broker is unavailable, the event will be retried later.
     *
     * @param event The event to publish
     * @param correlationId The correlation ID for distributed tracing (can be null)
     * @return A CompletableFuture that completes when the event is published or fails with an exception
     */
    public CompletableFuture<Void> publishEvent(Event event, String correlationId) {
        // Create a new span for the publish operation
        Span span = tracer.spanBuilder("publish_event")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(EVENT_TYPE_KEY, event.getType())
                .setAttribute(DEVICE_ID_KEY, event.getDeviceId())
                .setAttribute(BROKER_TOPIC_KEY, EVENTS_TOPIC)
                .setAttribute(BROKER_TYPE_KEY, brokerType)
                .startSpan();

        // If no correlation ID is provided, generate one
        final String finalCorrelationId = correlationId != null ? correlationId : UUID.randomUUID().toString();
        span.setAttribute(CORRELATION_ID_KEY, finalCorrelationId);

        try (Scope scope = span.makeCurrent()) {
            // Store the event in the outbox table first (transactional outbox pattern)
            return storeEventInOutbox(event, finalCorrelationId)
                    .thenComposeAsync(outboxId -> {
                        // Then publish to the message broker
                        return publishToMessageBroker(event, finalCorrelationId, outboxId);
                    }, asyncExecutor)
                    .whenComplete((result, throwable) -> {
                        if (throwable != null) {
                            LOGGER.error("Failed to publish event: {}", event, throwable);
                            span.setStatus(StatusCode.ERROR, throwable.getMessage());
                            span.recordException(throwable);
                        } else {
                            span.setStatus(StatusCode.OK);
                            // Record metric for successful event publishing
                            eventCounter.add(1, Attributes.of(
                                    EVENT_TYPE_KEY, event.getType(),
                                    BROKER_TYPE_KEY, brokerType));
                        }
                        span.end();
                    });
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            span.end();
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Stores the event in the outbox table for reliable delivery.
     *
     * @param event The event to store
     * @param correlationId The correlation ID for distributed tracing
     * @return A CompletableFuture that completes with the outbox ID when the event is stored
     */
    private CompletableFuture<Long> storeEventInOutbox(Event event, String correlationId) {
        Span span = tracer.spanBuilder("store_event_in_outbox")
                .setParent(Context.current())
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            // Create outbox entry
            Map<String, Object> outboxEntry = new HashMap<>();
            outboxEntry.put("eventId", event.getId());
            outboxEntry.put("eventType", event.getType());
            outboxEntry.put("deviceId", event.getDeviceId());
            outboxEntry.put("correlationId", correlationId);
            outboxEntry.put("payload", messageConverter.eventToJson(event));
            outboxEntry.put("status", "PENDING");
            outboxEntry.put("createdAt", System.currentTimeMillis());

            // Use circuit breaker and retry for database operations
            Supplier<CompletableFuture<Long>> storeOutboxSupplier = () -> {
                try {
                    storage.insertObject(OUTBOX_TABLE, outboxEntry, true);
                    return CompletableFuture.completedFuture((Long) outboxEntry.get("id"));
                } catch (StorageException e) {
                    return CompletableFuture.failedFuture(e);
                }
            };

            return Retry.decorateCompletionStage(
                    retry,
                    CircuitBreaker.decorateSupplier(circuitBreaker, storeOutboxSupplier)
            ).get();
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            span.end();
            return CompletableFuture.failedFuture(e);
        } finally {
            span.end();
        }
    }

    /**
     * Publishes the event to the message broker and updates the outbox entry status.
     *
     * @param event The event to publish
     * @param correlationId The correlation ID for distributed tracing
     * @param outboxId The ID of the outbox entry
     * @return A CompletableFuture that completes when the event is published
     */
    private CompletableFuture<Void> publishToMessageBroker(Event event, String correlationId, Long outboxId) {
        Span span = tracer.spanBuilder("publish_to_message_broker")
                .setParent(Context.current())
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(BROKER_TOPIC_KEY, EVENTS_TOPIC)
                .setAttribute(EVENT_TYPE_KEY, event.getType())
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            // Determine which broker implementation to use based on configuration
            if ("kafka".equalsIgnoreCase(brokerType)) {
                return publishToKafka(event, correlationId, outboxId, span);
            } else if ("rabbitmq".equalsIgnoreCase(brokerType)) {
                return publishToRabbitMQ(event, correlationId, outboxId, span);
            } else {
                throw new IllegalArgumentException("Unsupported broker type: " + brokerType);
            }
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            span.end();
            return CompletableFuture.failedFuture(e);
        } finally {
            span.end();
        }
    }
    
    /**
     * Publishes the event to Kafka and updates the outbox entry status.
     *
     * @param event The event to publish
     * @param correlationId The correlation ID for distributed tracing
     * @param outboxId The ID of the outbox entry
     * @param parentSpan The parent span for tracing
     * @return A CompletableFuture that completes when the event is published
     */
    private CompletableFuture<Void> publishToKafka(Event event, String correlationId, Long outboxId, Span parentSpan) {
        // In a real implementation, this would use KafkaProducer to publish the event
        // For now, we'll simulate the publish and update the outbox status
        return CompletableFuture.runAsync(() -> {
            try {
                LOGGER.debug("Publishing event {} to Kafka topic {}", event.getId(), EVENTS_TOPIC);
                
                // Simulate Kafka producer send operation
                // In a real implementation, this would be:                
                // ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                //     EVENTS_TOPIC,                      // topic
                //     event.getType(),                   // key (for partitioning)
                //     messageConverter.eventToBytes(event) // value
                // );
                // 
                // // Add headers for tracing and correlation
                // record.headers().add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8));
                // 
                // // Send with callback
                // kafkaProducer.send(record, (metadata, exception) -> {
                //     if (exception != null) {
                //         LOGGER.error("Failed to publish event to Kafka: {}", event.getId(), exception);
                //     } else {
                //         updateOutboxStatus(outboxId, "PUBLISHED");
                //     }
                // });
                
                // Simulate successful publish
                Thread.sleep(50);
                updateOutboxStatus(outboxId, "PUBLISHED");
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
            } catch (Exception e) {
                LOGGER.error("Failed to publish event to Kafka: {}", event.getId(), e);
                throw new CompletionException(e);
            }
        }, asyncExecutor);
    }
    
    /**
     * Publishes the event to RabbitMQ and updates the outbox entry status.
     *
     * @param event The event to publish
     * @param correlationId The correlation ID for distributed tracing
     * @param outboxId The ID of the outbox entry
     * @param parentSpan The parent span for tracing
     * @return A CompletableFuture that completes when the event is published
     */
    private CompletableFuture<Void> publishToRabbitMQ(Event event, String correlationId, Long outboxId, Span parentSpan) {
        // In a real implementation, this would use RabbitMQ client to publish the event
        // For now, we'll simulate the publish and update the outbox status
        return CompletableFuture.runAsync(() -> {
            try {
                LOGGER.debug("Publishing event {} to RabbitMQ exchange {}", event.getId(), EVENTS_TOPIC);
                
                // Simulate RabbitMQ channel publish operation
                // In a real implementation, this would be:
                // 
                // AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                //     .messageId(event.getId().toString())
                //     .correlationId(correlationId)
                //     .contentType("application/json")
                //     .deliveryMode(2) // persistent
                //     .build();
                // 
                // channel.basicPublish(
                //     EVENTS_TOPIC,     // exchange
                //     event.getType(),  // routing key
                //     properties,       // properties
                //     messageConverter.eventToBytes(event) // body
                // );
                
                // Simulate successful publish
                Thread.sleep(50);
                updateOutboxStatus(outboxId, "PUBLISHED");
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
            } catch (Exception e) {
                LOGGER.error("Failed to publish event to RabbitMQ: {}", event.getId(), e);
                throw new CompletionException(e);
            }
        }, asyncExecutor);
    }
    
    /**
     * Updates the status of an outbox entry.
     *
     * @param outboxId The ID of the outbox entry
     * @param status The new status (PUBLISHED, FAILED, etc.)
     * @throws StorageException If the update fails
     */
    private void updateOutboxStatus(Long outboxId, String status) throws StorageException {
        Map<String, Object> updates = new HashMap<>();
        updates.put("status", status);
        updates.put("publishedAt", System.currentTimeMillis());
        
        storage.updateObject(OUTBOX_TABLE, updates, new Request(
                new Columns.All(), new Condition.Equals("id", outboxId)));
    }

    /**
     * Processes any pending outbox entries that failed to publish previously.
     * This method should be called periodically by a scheduled job.
     *
     * @return A CompletableFuture that completes when all pending outbox entries are processed
     */
    public CompletableFuture<Void> processOutbox() {
        Span span = tracer.spanBuilder("process_outbox")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            // Find all pending outbox entries
            return CompletableFuture.runAsync(() -> {
                try {
                    // Find all PENDING entries older than 5 minutes
                    long cutoffTime = System.currentTimeMillis() - Duration.ofMinutes(5).toMillis();
                    
                    // Query for pending outbox entries
                    Request request = new Request(
                            new Columns.All(),
                            new Condition.And(
                                    new Condition.Equals("status", "PENDING"),
                                    new Condition.Less("createdAt", cutoffTime)));
                    
                    // In a real implementation, this would process each entry
                    try {
                        storage.getObjects(Map.class, request).forEach(entry -> {
                            try {
                                Long outboxId = (Long) entry.get("id");
                                String eventJson = (String) entry.get("payload");
                                String correlationId = (String) entry.get("correlationId");
                                
                                // Deserialize the event
                                Event event = messageConverter.jsonToEvent(eventJson);
                                
                                // Create a new span for this retry
                                Span retrySpan = tracer.spanBuilder("retry_publish_event")
                                        .setParent(Context.current())
                                        .setSpanKind(SpanKind.PRODUCER)
                                        .setAttribute(EVENT_TYPE_KEY, event.getType())
                                        .setAttribute(DEVICE_ID_KEY, event.getDeviceId())
                                        .setAttribute(CORRELATION_ID_KEY, correlationId)
                                        .setAttribute(BROKER_TOPIC_KEY, EVENTS_TOPIC)
                                        .setAttribute(BROKER_TYPE_KEY, brokerType)
                                        .startSpan();
                                
                                try (Scope retryScope = retrySpan.makeCurrent()) {
                                    // Determine which broker implementation to use
                                    if ("kafka".equalsIgnoreCase(brokerType)) {
                                        publishToKafka(event, correlationId, outboxId, retrySpan).join();
                                    } else if ("rabbitmq".equalsIgnoreCase(brokerType)) {
                                        publishToRabbitMQ(event, correlationId, outboxId, retrySpan).join();
                                    }
                                    
                                    // Record metric for successful retry
                                    eventCounter.add(1, Attributes.of(
                                            EVENT_TYPE_KEY, event.getType(),
                                            BROKER_TYPE_KEY, brokerType,
                                            AttributeKey.stringKey("retry"), "true"));
                                    
                                    retrySpan.setStatus(StatusCode.OK);
                                } catch (Exception e) {
                                    retrySpan.setStatus(StatusCode.ERROR, e.getMessage());
                                    retrySpan.recordException(e);
                                    
                                    // Update status to FAILED if max retries exceeded
                                    Integer retryCount = (Integer) entry.getOrDefault("retryCount", 0);
                                    if (retryCount >= 3) {
                                        try {
                                            updateOutboxStatus(outboxId, "FAILED");
                                        } catch (StorageException se) {
                                            LOGGER.error("Failed to update outbox entry status: {}", outboxId, se);
                                        }
                                    } else {
                                        // Increment retry count
                                        try {
                                            Map<String, Object> updates = new HashMap<>();
                                            updates.put("retryCount", retryCount + 1);
                                            updates.put("lastRetryAt", System.currentTimeMillis());
                                            
                                            storage.updateObject(OUTBOX_TABLE, updates, new Request(
                                                    new Columns.All(), new Condition.Equals("id", outboxId)));
                                        } catch (StorageException se) {
                                            LOGGER.error("Failed to update outbox entry retry count: {}", outboxId, se);
                                        }
                                    }
                                    
                                    LOGGER.warn("Failed to retry publishing event: {}", outboxId, e);
                                } finally {
                                    retrySpan.end();
                                }
                            } catch (Exception e) {
                                LOGGER.error("Error processing outbox entry: {}", entry.get("id"), e);
                            }
                        });
                    } catch (StorageException e) {
                        LOGGER.error("Failed to query outbox entries", e);
                        throw new CompletionException(e);
                    }
                    
                    LOGGER.debug("Processed pending outbox entries");
                } catch (Exception e) {
                    LOGGER.error("Failed to process outbox entries", e);
                    throw new CompletionException(e);
                }
            }, asyncExecutor);
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            span.end();
            return CompletableFuture.failedFuture(e);
        } finally {
            span.end();
        }
    }
    
    /**
     * Schedules the outbox processor to run periodically.
     * This method should be called during service initialization.
     */
    public void scheduleOutboxProcessing() {
        // Schedule the outbox processor to run periodically
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(
            () -> processOutbox().exceptionally(e -> {
                LOGGER.error("Scheduled outbox processing failed", e);
                return null;
            }),
            config.getInteger(KEY_EVENT_OUTBOX_INITIAL_DELAY, 60),
            config.getInteger(KEY_EVENT_OUTBOX_INTERVAL, 60),
            TimeUnit.SECONDS
        );
        
        LOGGER.info("Outbox processing scheduled with initial delay {} seconds and interval {} seconds", 
                config.getInteger(KEY_EVENT_OUTBOX_INITIAL_DELAY, 60),
                config.getInteger(KEY_EVENT_OUTBOX_INTERVAL, 60));
    }
    
    /**
     * Implementation of the BaseEventHandler.Callback interface.
     * This method is called by event handlers when an event is detected.
     * 
     * @param event The detected event
     */
    @Override
    public void eventDetected(Event event) {
        // Extract correlation ID from the current tracing context if available
        String correlationId = null;
        Span currentSpan = Span.current();
        if (currentSpan != null) {
            correlationId = currentSpan.getAttribute(CORRELATION_ID_KEY);
        }
        
        // Publish the event asynchronously
        publishEvent(event, correlationId).exceptionally(throwable -> {
            LOGGER.error("Failed to publish event: {}", event, throwable);
            return null;
        });
    }
}