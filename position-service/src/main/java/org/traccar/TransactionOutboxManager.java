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
package org.traccar;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages the transaction outbox pattern for reliable message publishing.
 * Stores messages in an outbox table and processes them asynchronously.
 */
@Singleton
public class TransactionOutboxManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionOutboxManager.class);

    private final Storage storage;
    private final MessagePublisher messagePublisher;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;
    private final ScheduledExecutorService scheduler;

    /**
     * Outbox message entity for storing messages before publishing.
     */
    public static class OutboxMessage {
        private long id;
        private String topic;
        private String key;
        private String payload;
        private String correlationId;
        private boolean processed;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getPayload() {
            return payload;
        }

        public void setPayload(String payload) {
            this.payload = payload;
        }

        public String getCorrelationId() {
            return correlationId;
        }

        public void setCorrelationId(String correlationId) {
            this.correlationId = correlationId;
        }

        public boolean isProcessed() {
            return processed;
        }

        public void setProcessed(boolean processed) {
            this.processed = processed;
        }
    }

    @Inject
    public TransactionOutboxManager(
            Config config,
            Storage storage,
            MessagePublisher messagePublisher,
            ObjectMapper objectMapper,
            Tracer tracer) {
        this.storage = storage;
        this.messagePublisher = messagePublisher;
        this.objectMapper = objectMapper;
        this.tracer = tracer;
        
        // Schedule periodic processing of outbox messages
        this.scheduler = Executors.newScheduledThreadPool(1);
        int processingIntervalSeconds = config.getInteger("outbox.processing.interval.seconds", 5);
        scheduler.scheduleAtFixedRate(this::processOutboxMessages, processingIntervalSeconds, 
                processingIntervalSeconds, TimeUnit.SECONDS);
        
        LOGGER.info("Transaction outbox manager initialized with processing interval of {} seconds", 
                processingIntervalSeconds);
    }

    /**
     * Adds a message to the outbox table within a transaction.
     *
     * @param transactionStorage Storage with active transaction
     * @param topic Topic to publish to
     * @param payload Object to serialize and publish
     * @param correlationId Correlation ID for distributed tracing
     * @throws StorageException If storage operation fails
     */
    public void addToOutbox(Storage transactionStorage, String topic, Object payload, String correlationId) 
            throws StorageException {
        String key = null;
        
        // If payload has a deviceId field, use it as the message key for partitioning
        if (payload instanceof org.traccar.model.Message message) {
            key = String.valueOf(message.getDeviceId());
        }
        
        // Create outbox message
        OutboxMessage outboxMessage = new OutboxMessage();
        outboxMessage.setTopic(topic);
        outboxMessage.setKey(key);
        outboxMessage.setPayload(serializePayload(payload));
        outboxMessage.setCorrelationId(correlationId);
        outboxMessage.setProcessed(false);
        
        // Store in outbox table within the transaction
        transactionStorage.addObject(outboxMessage);
        
        LOGGER.debug("Added message to outbox for topic {} with correlation ID {}", topic, correlationId);
    }

    /**
     * Processes pending outbox messages by publishing them to the message broker.
     * This method is called periodically by the scheduler.
     */
    public void processOutboxMessages() {
        Span span = tracer.spanBuilder("process-outbox-messages").startSpan();
        
        try (var scope = span.makeCurrent()) {
            try {
                // Retrieve unprocessed outbox messages
                var outboxMessages = storage.getObjects(OutboxMessage.class, 
                        new Request(new Condition.Equals("processed", false)));
                
                span.setAttribute("outbox.messages.count", outboxMessages.size());
                LOGGER.debug("Processing {} outbox messages", outboxMessages.size());
                
                for (OutboxMessage message : outboxMessages) {
                    Span messageSpan = tracer.spanBuilder("publish-outbox-message")
                            .setAttribute("outbox.message.id", message.getId())
                            .setAttribute("message.topic", message.getTopic())
                            .setAttribute("message.correlation_id", message.getCorrelationId())
                            .startSpan();
                    
                    try (var messageScope = messageSpan.makeCurrent()) {
                        // Create message headers with tracing information
                        Map<String, String> headers = new HashMap<>();
                        headers.put("x-correlation-id", message.getCorrelationId());
                        
                        // Publish message to broker
                        messagePublisher.publish(message.getTopic(), message.getKey(), message.getPayload(), headers)
                                .thenAccept(result -> {
                                    try {
                                        // Mark as processed
                                        OutboxMessage processed = new OutboxMessage();
                                        processed.setId(message.getId());
                                        processed.setProcessed(true);
                                        storage.updateObject(processed, 
                                                new Request(new Columns.Include("processed"),
                                                        new Condition.Equals("id", message.getId())));
                                        LOGGER.debug("Marked outbox message {} as processed", message.getId());
                                    } catch (StorageException e) {
                                        LOGGER.error("Failed to mark outbox message as processed", e);
                                        messageSpan.recordException(e);
                                    }
                                })
                                .exceptionally(e -> {
                                    LOGGER.error("Failed to publish message from outbox", e);
                                    messageSpan.recordException(e);
                                    return null;
                                });
                    } finally {
                        messageSpan.end();
                    }
                }
            } catch (StorageException e) {
                LOGGER.error("Failed to retrieve outbox messages", e);
                span.recordException(e);
            }
        } finally {
            span.end();
        }
    }

    /**
     * Serializes an object to JSON.
     *
     * @param payload The object to serialize
     * @return JSON string representation of the object
     * @throws StorageException if serialization fails
     */
    private String serializePayload(Object payload) throws StorageException {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to serialize payload", e);
            throw new StorageException(e);
        }
    }

    /**
     * Shuts down the scheduler.
     * Should be called when the application is shutting down.
     */
    public void shutdown() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            LOGGER.info("Transaction outbox manager scheduler shut down");
        }
    }
}