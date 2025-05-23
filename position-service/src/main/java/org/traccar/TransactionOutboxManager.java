/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.traccar.config.Config;
import org.traccar.model.OutboxMessage;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Order;
import org.traccar.storage.query.Request;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements the Transaction Outbox pattern for reliable message publishing.
 * 
 * This class provides methods to add messages to the outbox table as part of a database transaction,
 * and a scheduled task to publish those messages to the message broker. This ensures that messages
 * are reliably delivered to the broker even if the service fails after committing the transaction
 * but before publishing the message.
 */
@Singleton
public class TransactionOutboxManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionOutboxManager.class);

    private final Storage storage;
    private final ObjectMapper objectMapper;
    private final MessageBrokerPublisher messageBrokerPublisher;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final int batchSize;
    private final int maxRetries;

    /**
     * Constructs a new TransactionOutboxManager with the necessary dependencies.
     *
     * @param config Configuration
     * @param storage Storage for persisting outbox messages
     * @param objectMapper JSON object mapper for serializing messages
     * @param messageBrokerPublisher Publisher for sending messages to the broker
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry
     */
    @Inject
    public TransactionOutboxManager(
            Config config,
            Storage storage,
            ObjectMapper objectMapper,
            MessageBrokerPublisher messageBrokerPublisher,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.storage = storage;
        this.objectMapper = objectMapper;
        this.messageBrokerPublisher = messageBrokerPublisher;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.batchSize = config.getInteger("outbox.batchSize", 100);
        this.maxRetries = config.getInteger("outbox.maxRetries", 5);
    }

    /**
     * Add a message to the outbox table as part of a database transaction.
     *
     * @param transactionStorage Storage with an active transaction
     * @param topic Topic to publish the message to
     * @param payload Message payload
     * @param correlationId Correlation ID for distributed tracing
     * @throws StorageException If there is an error storing the message
     */
    public void addToOutbox(Storage transactionStorage, String topic, Object payload, String correlationId) 
            throws StorageException {
        try {
            // Serialize the payload to JSON
            String payloadJson = objectMapper.writeValueAsString(payload);
            
            // Create a new outbox message
            OutboxMessage outboxMessage = new OutboxMessage();
            outboxMessage.setTopic(topic);
            outboxMessage.setPayload(payloadJson);
            outboxMessage.setCorrelationId(correlationId);
            outboxMessage.setCreatedAt(new Date());
            outboxMessage.setStatus(OutboxMessage.Status.PENDING);
            outboxMessage.setRetryCount(0);
            
            // Store the outbox message in the database as part of the transaction
            transactionStorage.addObject(outboxMessage, new Request(new Columns.Exclude("id")));
            
            // Record metric for outbox message creation
            meterRegistry.counter("outbox.message.created", "topic", topic).increment();
            
            LOGGER.debug("Added message to outbox with correlation ID: {}", correlationId);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error serializing payload to JSON: {}", e.getMessage());
            throw new StorageException(e);
        }
    }

    /**
     * Scheduled task to process pending outbox messages and publish them to the message broker.
     * 
     * This method is scheduled to run at a fixed rate to ensure that messages are published
     * to the broker even if the service fails after committing the transaction but before
     * publishing the message.
     */
    @Scheduled(fixedDelayString = "${outbox.processInterval:1000}")
    public void processOutbox() {
        Span span = tracer.spanBuilder("process-outbox")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Retrieve pending outbox messages from the database
            List<OutboxMessage> pendingMessages = getPendingMessages();
            
            if (!pendingMessages.isEmpty()) {
                LOGGER.debug("Processing {} pending outbox messages", pendingMessages.size());
                span.setAttribute("outbox.messages.count", pendingMessages.size());
                
                // Process each pending message
                for (OutboxMessage message : pendingMessages) {
                    processMessage(message);
                }
            }
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error processing outbox: {}", e.getMessage());
            meterRegistry.counter("outbox.process.error").increment();
        } finally {
            span.end();
        }
    }

    /**
     * Retrieve pending outbox messages from the database.
     *
     * @return List of pending outbox messages
     * @throws StorageException If there is an error retrieving the messages
     */
    private List<OutboxMessage> getPendingMessages() throws StorageException {
        return storage.getObjects(OutboxMessage.class, new Request(
                new Condition.Equals("status", OutboxMessage.Status.PENDING),
                new Order("createdAt", true, 0),
                new Order("id", true, 0))
                .setLimit(batchSize));
    }

    /**
     * Process a single outbox message by publishing it to the message broker.
     *
     * @param message Outbox message to process
     */
    private void processMessage(OutboxMessage message) {
        Span span = tracer.spanBuilder("process-outbox-message")
                .setParent(Context.current())
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("outbox.message.id", message.getId())
                .setAttribute("outbox.message.topic", message.getTopic())
                .setAttribute("x-correlation-id", message.getCorrelationId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Create message headers with correlation ID for distributed tracing
            Map<String, String> headers = new HashMap<>();
            headers.put("x-correlation-id", message.getCorrelationId());
            
            // Add OpenTelemetry context to headers for distributed tracing
            Map<String, String> tracingHeaders = new HashMap<>();
            messageBrokerPublisher.getPropagator().inject(Context.current(), tracingHeaders, 
                    (carrier, key, value) -> carrier.put(key, value));
            headers.putAll(tracingHeaders);
            
            // Publish the message to the broker
            messageBrokerPublisher.publish(message.getTopic(), message.getPayload(), headers);
            
            // Mark the message as processed
            markAsProcessed(message);
            
            // Record metric for successful message publishing
            meterRegistry.counter("outbox.message.published", "topic", message.getTopic()).increment();
            
            LOGGER.debug("Published message from outbox with correlation ID: {}", message.getCorrelationId());
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error publishing message from outbox: {}", e.getMessage());
            
            // Increment retry count and mark as failed if max retries reached
            handlePublishingFailure(message);
            
            // Record metric for failed message publishing
            meterRegistry.counter("outbox.message.failed", "topic", message.getTopic()).increment();
        } finally {
            span.end();
        }
    }

    /**
     * Mark an outbox message as processed in the database.
     *
     * @param message Outbox message to mark as processed
     */
    private void markAsProcessed(OutboxMessage message) {
        try {
            message.setStatus(OutboxMessage.Status.PROCESSED);
            message.setProcessedAt(new Date());
            
            storage.updateObject(message, new Request(
                    new Condition.Equals("id", message.getId())));
        } catch (StorageException e) {
            LOGGER.error("Error marking outbox message as processed: {}", e.getMessage());
        }
    }

    /**
     * Handle a failure to publish an outbox message by incrementing the retry count
     * and marking it as failed if the maximum number of retries has been reached.
     *
     * @param message Outbox message that failed to publish
     */
    private void handlePublishingFailure(OutboxMessage message) {
        try {
            message.setRetryCount(message.getRetryCount() + 1);
            
            if (message.getRetryCount() >= maxRetries) {
                message.setStatus(OutboxMessage.Status.FAILED);
                message.setFailedAt(new Date());
                LOGGER.warn("Outbox message with correlation ID {} failed after {} retries", 
                        message.getCorrelationId(), message.getRetryCount());
            }
            
            storage.updateObject(message, new Request(
                    new Condition.Equals("id", message.getId())));
        } catch (StorageException e) {
            LOGGER.error("Error updating outbox message retry count: {}", e.getMessage());
        }
    }
}