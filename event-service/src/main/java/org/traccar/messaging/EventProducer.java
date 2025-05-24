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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.extension.annotations.WithSpan;
import io.opentelemetry.semconv.SemanticAttributes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.model.Event;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Publishes detected events to the message broker (Kafka/RabbitMQ).
 * Implements the transaction outbox pattern to ensure reliable delivery
 * with at-least-once semantics even during service failures.
 */
@Singleton
public class EventProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProducer.class);
    private static final String TOPIC_NAME = "events";
    private static final String OUTBOX_TABLE = "event_outbox";
    private static final int DEFAULT_RETRY_ATTEMPTS = 3;
    private static final int DEFAULT_RETRY_DELAY_MS = 1000;
    private static final int DEFAULT_OUTBOX_POLL_INTERVAL_MS = 5000;

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;
    private final Storage storage;
    private final Tracer tracer;
    private final ScheduledExecutorService scheduler;
    private final int maxRetryAttempts;
    private final int retryDelayMs;

    /**
     * TextMapSetter implementation for injecting trace context into Kafka headers
     */
    private static final TextMapSetter<Headers> HEADER_SETTER = (headers, key, value) -> 
            headers.add(key, value.getBytes());

    /**
     * Creates a new EventProducer with the specified dependencies.
     *
     * @param config Configuration for Kafka/RabbitMQ settings
     * @param objectMapper JSON serializer/deserializer
     * @param storage Database storage for outbox pattern
     * @param tracer OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public EventProducer(Config config, ObjectMapper objectMapper, Storage storage, Tracer tracer) {
        this.objectMapper = objectMapper;
        this.storage = storage;
        this.tracer = tracer;
        this.maxRetryAttempts = config.getInteger("event.producer.maxRetryAttempts", DEFAULT_RETRY_ATTEMPTS);
        this.retryDelayMs = config.getInteger("event.producer.retryDelayMs", DEFAULT_RETRY_DELAY_MS);
        int outboxPollInterval = config.getInteger("event.producer.outboxPollIntervalMs", DEFAULT_OUTBOX_POLL_INTERVAL_MS);

        // Configure Kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Configure acknowledgment settings
        String acks = config.getString("kafka.producer.acks", "all"); // Options: none, leader, all
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        
        // Configure retries
        props.put(ProducerConfig.RETRIES_CONFIG, maxRetryAttempts);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryDelayMs);
        
        // Enable idempotence for exactly-once semantics when acks=all
        if ("all".equals(acks)) {
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        }
        
        // Create Kafka producer
        this.producer = new KafkaProducer<>(props);
        
        // Initialize scheduler for outbox processing
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.scheduler.scheduleAtFixedRate(
                this::processOutbox,
                outboxPollInterval,
                outboxPollInterval,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Publishes an event to the message broker using the transaction outbox pattern.
     * The event is first stored in the outbox table and then published to Kafka.
     *
     * @param event The event to publish
     * @return CompletableFuture that completes when the event is published
     */
    @WithSpan(value = "EventProducer.publishEvent", kind = SpanKind.PRODUCER)
    public CompletableFuture<Void> publishEvent(Event event) {
        Span span = Span.current();
        span.setAttribute("event.type", event.getType());
        span.setAttribute("event.deviceId", event.getDeviceId());
        
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        try {
            // Store event in outbox table
            storeEventInOutbox(event);
            
            // Try to publish immediately for better latency
            publishToKafka(event)
                .thenRun(() -> {
                    try {
                        // Remove from outbox on successful publish
                        removeFromOutbox(event.getId());
                        future.complete(null);
                    } catch (StorageException e) {
                        LOGGER.warn("Failed to remove event from outbox, will be retried later: {}", e.getMessage());
                        future.complete(null); // Still consider it a success as it will be cleaned up later
                    }
                })
                .exceptionally(e -> {
                    LOGGER.warn("Failed to publish event, will be retried from outbox: {}", e.getMessage());
                    future.complete(null); // Will be retried from outbox
                    return null;
                });
        } catch (Exception e) {
            LOGGER.error("Error storing event in outbox: {}", e.getMessage(), e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            future.completeExceptionally(e);
        }
        
        return future;
    }

    /**
     * Stores an event in the outbox table for reliable delivery.
     *
     * @param event The event to store
     * @throws StorageException If there's an error storing the event
     * @throws JsonProcessingException If there's an error serializing the event
     */
    private void storeEventInOutbox(Event event) throws StorageException, JsonProcessingException {
        Map<String, Object> outboxEntry = new HashMap<>();
        outboxEntry.put("id", event.getId());
        outboxEntry.put("eventType", event.getType());
        outboxEntry.put("payload", objectMapper.writeValueAsString(event));
        outboxEntry.put("createdAt", event.getEventTime());
        outboxEntry.put("attempts", 0);
        outboxEntry.put("processed", false);
        
        storage.insertObject(OUTBOX_TABLE, outboxEntry, true);
        LOGGER.debug("Stored event in outbox: {}", event.getId());
    }

    /**
     * Removes an event from the outbox table after successful processing.
     *
     * @param eventId The ID of the event to remove
     * @throws StorageException If there's an error removing the event
     */
    private void removeFromOutbox(long eventId) throws StorageException {
        storage.removeObject(OUTBOX_TABLE, new Request(
                new Condition.Equals("id", eventId)));
        LOGGER.debug("Removed event from outbox: {}", eventId);
    }

    /**
     * Marks an event as processed in the outbox table.
     *
     * @param eventId The ID of the event to mark as processed
     * @throws StorageException If there's an error updating the event
     */
    private void markAsProcessed(long eventId) throws StorageException {
        Map<String, Object> update = new HashMap<>();
        update.put("processed", true);
        
        storage.updateObject(OUTBOX_TABLE, update, new Request(
                new Condition.Equals("id", eventId)));
        LOGGER.debug("Marked event as processed in outbox: {}", eventId);
    }

    /**
     * Increments the retry attempts for an event in the outbox table.
     *
     * @param eventId The ID of the event to update
     * @throws StorageException If there's an error updating the event
     */
    private void incrementAttempts(long eventId) throws StorageException {
        // First get current attempts
        Request request = new Request(
                new Columns.All(),
                new Condition.Equals("id", eventId));
        
        Map<String, Object> outboxEntry = storage.getObject(OUTBOX_TABLE, request);
        if (outboxEntry != null) {
            int attempts = ((Number) outboxEntry.getOrDefault("attempts", 0)).intValue() + 1;
            
            Map<String, Object> update = new HashMap<>();
            update.put("attempts", attempts);
            
            storage.updateObject(OUTBOX_TABLE, update, new Request(
                    new Condition.Equals("id", eventId)));
            LOGGER.debug("Incremented attempts for event in outbox: {} (attempts: {})", eventId, attempts);
        }
    }

    /**
     * Processes the outbox table, publishing any unpublished events.
     * This method is called periodically by the scheduler.
     */
    private void processOutbox() {
        Span span = tracer.spanBuilder("EventProducer.processOutbox")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            LOGGER.debug("Processing event outbox");
            
            // Get unprocessed events from outbox
            Request request = new Request(
                    new Columns.All(),
                    new Condition.Equals("processed", false));
            
            List<Map<String, Object>> outboxEntries = storage.getObjects(OUTBOX_TABLE, request);
            span.setAttribute("outbox.entries.count", outboxEntries.size());
            
            for (Map<String, Object> entry : outboxEntries) {
                long eventId = ((Number) entry.get("id")).longValue();
                int attempts = ((Number) entry.getOrDefault("attempts", 0)).intValue();
                
                if (attempts >= maxRetryAttempts) {
                    LOGGER.error("Max retry attempts reached for event: {}", eventId);
                    try {
                        markAsProcessed(eventId); // Mark as processed to avoid further retries
                    } catch (StorageException e) {
                        LOGGER.error("Failed to mark event as processed: {}", e.getMessage(), e);
                    }
                    continue;
                }
                
                try {
                    String eventType = (String) entry.get("eventType");
                    String payload = (String) entry.get("payload");
                    Event event = objectMapper.readValue(payload, Event.class);
                    
                    // Publish to Kafka
                    publishToKafka(event)
                        .thenRun(() -> {
                            try {
                                markAsProcessed(eventId);
                            } catch (StorageException e) {
                                LOGGER.error("Failed to mark event as processed: {}", e.getMessage(), e);
                            }
                        })
                        .exceptionally(e -> {
                            LOGGER.warn("Failed to publish event from outbox: {}", e.getMessage());
                            try {
                                incrementAttempts(eventId);
                            } catch (StorageException se) {
                                LOGGER.error("Failed to increment attempts: {}", se.getMessage(), se);
                            }
                            return null;
                        });
                } catch (Exception e) {
                    LOGGER.error("Error processing outbox entry: {}", e.getMessage(), e);
                    try {
                        incrementAttempts(eventId);
                    } catch (StorageException se) {
                        LOGGER.error("Failed to increment attempts: {}", se.getMessage(), se);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error processing outbox: {}", e.getMessage(), e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }

    /**
     * Publishes an event to Kafka with OpenTelemetry context propagation.
     *
     * @param event The event to publish
     * @return CompletableFuture that completes when the event is published
     */
    private CompletableFuture<Void> publishToKafka(Event event) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        try {
            String eventJson = objectMapper.writeValueAsString(event);
            String eventType = event.getType();
            
            // Create producer record with event type as key for partitioning
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, eventType, eventJson);
            
            // Get current span context and inject it into Kafka headers for distributed tracing
            Span span = Span.current();
            Context context = Context.current();
            
            // Add OpenTelemetry semantic conventions for messaging
            span.setAttribute(SemanticAttributes.MESSAGING_SYSTEM, "kafka");
            span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION_NAME, TOPIC_NAME);
            span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION_KIND, "topic");
            span.setAttribute(SemanticAttributes.MESSAGING_MESSAGE_ID, String.valueOf(event.getId()));
            span.setAttribute(SemanticAttributes.MESSAGING_KAFKA_MESSAGE_KEY, eventType);
            
            // Inject the current context into the Kafka record headers
            io.opentelemetry.api.GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                    .inject(context, record.headers(), HEADER_SETTER);
            
            // Send the record to Kafka
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOGGER.error("Failed to send event to Kafka: {}", exception.getMessage(), exception);
                    span.setStatus(StatusCode.ERROR, exception.getMessage());
                    future.completeExceptionally(exception);
                } else {
                    LOGGER.debug("Event sent to Kafka: topic={}, partition={}, offset={}", 
                            metadata.topic(), metadata.partition(), metadata.offset());
                    span.setAttribute("kafka.partition", metadata.partition());
                    span.setAttribute("kafka.offset", metadata.offset());
                    future.complete(null);
                }
            });
        } catch (Exception e) {
            LOGGER.error("Error serializing event: {}", e.getMessage(), e);
            Span.current().setStatus(StatusCode.ERROR, e.getMessage());
            future.completeExceptionally(e);
        }
        
        return future;
    }

    /**
     * Closes the producer and scheduler when the service is shutting down.
     */
    public void close() {
        try {
            scheduler.shutdown();
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
        
        // Process any remaining outbox entries before shutting down
        processOutbox();
        
        // Close the producer with a grace period
        producer.close(Duration.ofSeconds(5));
    }
}