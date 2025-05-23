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
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.model.Position;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Publishes enriched position messages to the message broker after processing.
 * Implements the transaction outbox pattern for reliable publishing and
 * configures message partitioning by device ID.
 */
@Singleton
public class EnrichedPositionProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnrichedPositionProducer.class);

    private final Config config;
    private final Storage storage;
    private final MessagePublisher messagePublisher;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;

    /**
     * Outbox message entity for storing messages before publishing.
     */
    public static class OutboxMessage {
        private long id;
        private String topic;
        private String key;
        private String payload;
        private String traceId;
        private String spanId;
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

        public String getTraceId() {
            return traceId;
        }

        public void setTraceId(String traceId) {
            this.traceId = traceId;
        }

        public String getSpanId() {
            return spanId;
        }

        public void setSpanId(String spanId) {
            this.spanId = spanId;
        }

        public boolean isProcessed() {
            return processed;
        }

        public void setProcessed(boolean processed) {
            this.processed = processed;
        }
    }

    @Inject
    public EnrichedPositionProducer(
            Config config,
            Storage storage,
            MessagePublisher messagePublisher,
            ObjectMapper objectMapper,
            Tracer tracer) {
        this.config = config;
        this.storage = storage;
        this.messagePublisher = messagePublisher;
        this.objectMapper = objectMapper;
        this.tracer = tracer;
    }

    /**
     * Publishes an enriched position to the message broker.
     * Uses the transaction outbox pattern to ensure reliable delivery.
     *
     * @param position The enriched position to publish
     * @return CompletableFuture that completes when the message is stored in the outbox
     */
    public CompletableFuture<Void> publishEnrichedPosition(Position position) {
        String topic = config.getString("message.topic.enrichedPositions", "enriched-positions");
        String key = String.valueOf(position.getDeviceId());

        Span span = tracer.spanBuilder("publish_enriched_position").startSpan();
        SpanContext spanContext = span.getSpanContext();

        try (var scope = span.makeCurrent()) {
            span.setAttribute("device.id", position.getDeviceId());
            span.setAttribute("position.id", position.getId());
            span.setAttribute("message.topic", topic);
            span.setAttribute("message.key", key);

            // Create outbox message
            OutboxMessage outboxMessage = new OutboxMessage();
            outboxMessage.setTopic(topic);
            outboxMessage.setKey(key);
            outboxMessage.setPayload(serializePosition(position));
            outboxMessage.setTraceId(spanContext.getTraceId());
            outboxMessage.setSpanId(spanContext.getSpanId());
            outboxMessage.setProcessed(false);

            // Store in outbox table
            return CompletableFuture.runAsync(() -> {
                try {
                    storage.addObject(outboxMessage);
                    LOGGER.debug("Stored position {} in outbox for device {}", 
                            position.getId(), position.getDeviceId());
                    processOutboxMessages();
                } catch (StorageException e) {
                    LOGGER.error("Failed to store position in outbox", e);
                    span.recordException(e);
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Processes pending outbox messages by publishing them to the message broker.
     */
    public void processOutboxMessages() {
        try {
            var outboxMessages = storage.getObjects(OutboxMessage.class, 
                    new Request(new Condition.Equals("processed", false)));

            for (OutboxMessage message : outboxMessages) {
                Span span = tracer.spanBuilder("process_outbox_message")
                        .setParent(Context.current().with(Span.current()))
                        .startSpan();

                try (var scope = span.makeCurrent()) {
                    span.setAttribute("outbox.message.id", message.getId());
                    span.setAttribute("message.topic", message.getTopic());
                    span.setAttribute("message.key", message.getKey());

                    // Create message headers with tracing information
                    Map<String, String> headers = new HashMap<>();
                    headers.put("trace-id", message.getTraceId());
                    headers.put("span-id", message.getSpanId());
                    headers.put("correlation-id", UUID.randomUUID().toString());

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
                                    span.recordException(e);
                                }
                            })
                            .exceptionally(e -> {
                                LOGGER.error("Failed to publish message from outbox", e);
                                span.recordException(e);
                                return null;
                            });
                } finally {
                    span.end();
                }
            }
        } catch (StorageException e) {
            LOGGER.error("Failed to retrieve outbox messages", e);
        }
    }

    /**
     * Serializes a position object to JSON.
     *
     * @param position The position to serialize
     * @return JSON string representation of the position
     * @throws RuntimeException if serialization fails
     */
    private String serializePosition(Position position) {
        try {
            return objectMapper.writeValueAsString(position);
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to serialize position", e);
            throw new RuntimeException("Failed to serialize position", e);
        }
    }
}