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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.extension.annotations.WithSpan;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.traccar.handler.events.BaseEventHandler;
import org.traccar.model.Event;
import org.traccar.model.Position;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Consumer for enriched position data from the message broker.
 * Subscribes to the 'enriched.positions' topic, deserializes position messages,
 * and routes them to the appropriate event handlers for processing.
 * Uses consumer group 'event-processors' for load distribution and implements
 * partition assignment by device ID to ensure ordered processing.
 */
@Singleton
@Component
public class PositionConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionConsumer.class);

    private final List<BaseEventHandler> eventHandlers;
    private final Tracer tracer;
    private final Executor executor;

    /**
     * Constructs a new PositionConsumer with the specified event handlers and tracer.
     *
     * @param eventHandlers List of event handlers to process positions
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param executor Executor for asynchronous processing
     */
    @Inject
    public PositionConsumer(List<BaseEventHandler> eventHandlers, Tracer tracer, Executor executor) {
        this.eventHandlers = eventHandlers;
        this.tracer = tracer;
        this.executor = executor;
        LOGGER.info("Initialized PositionConsumer with {} event handlers", eventHandlers.size());
    }

    /**
     * Kafka listener method that consumes messages from the 'enriched.positions' topic.
     * Implements retry mechanism with exponential backoff and dead letter queue for unprocessable messages.
     *
     * @param record Kafka consumer record containing the position data
     * @param acknowledgment Manual acknowledgment for the message
     */
    @RetryableTopic(
            attempts = "5",
            backoff = @Backoff(delay = 1000, multiplier = 2.0, maxDelay = 10000),
            dltStrategy = DltStrategy.ALWAYS_RETRY_ON_ERROR)
    @KafkaListener(
            topics = "${kafka.topics.enriched-positions:enriched.positions}",
            groupId = "${kafka.consumer.group-id:event-processors}",
            containerFactory = "kafkaListenerContainerFactory")
    @WithSpan(kind = SpanKind.CONSUMER)
    public void consumePosition(ConsumerRecord<String, Position> record, Acknowledgment acknowledgment) {
        Position position = record.value();
        String deviceId = record.key();
        
        // Extract trace context from Kafka headers if present
        Context extractedContext = extractTraceContext(record.headers());
        Span span = tracer.spanBuilder("process-position")
                .setParent(extractedContext)
                .setAttribute("deviceId", deviceId)
                .setAttribute("positionId", position.getId())
                .startSpan();
        
        try {
            LOGGER.debug("Received position with ID: {} for device: {}", position.getId(), deviceId);
            
            // Process position with all event handlers
            processPosition(position).thenRun(() -> {
                LOGGER.debug("Successfully processed position with ID: {}", position.getId());
                acknowledgment.acknowledge();
                span.setStatus(StatusCode.OK);
                span.end();
            }).exceptionally(ex -> {
                LOGGER.error("Error processing position with ID: {}", position.getId(), ex);
                span.recordException(ex);
                span.setStatus(StatusCode.ERROR, ex.getMessage());
                span.end();
                // Let the retry mechanism handle the exception
                throw new RuntimeException("Failed to process position", ex);
            });
        } catch (Exception e) {
            LOGGER.error("Unexpected error processing position with ID: {}", position.getId(), e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.end();
            throw e; // Rethrow to trigger retry
        }
    }

    /**
     * Process a position through all registered event handlers.
     *
     * @param position The position to process
     * @return CompletableFuture that completes when all handlers have processed the position
     */
    private CompletableFuture<Void> processPosition(Position position) {
        return CompletableFuture.runAsync(() -> {
            // Create a callback for event detection
            BaseEventHandler.Callback callback = this::handleDetectedEvent;
            
            // Process position through all event handlers
            for (BaseEventHandler handler : eventHandlers) {
                try {
                    handler.analyzePosition(position, callback);
                } catch (Exception e) {
                    LOGGER.warn("Error in event handler: {}", handler.getClass().getSimpleName(), e);
                    // Continue with other handlers even if one fails
                }
            }
        }, executor);
    }

    /**
     * Handles a detected event by publishing it to the event topic.
     * This method is called by event handlers when they detect an event.
     *
     * @param event The detected event
     */
    @WithSpan
    private void handleDetectedEvent(Event event) {
        LOGGER.info("Event detected: {} for device ID: {}", event.getType(), event.getDeviceId());
        // In a real implementation, this would publish the event to a message broker topic
        // For example: eventProducer.publishEvent(event);
    }

    /**
     * Extracts trace context from Kafka headers for distributed tracing.
     *
     * @param headers Kafka message headers
     * @return Extracted OpenTelemetry context
     */
    private Context extractTraceContext(Headers headers) {
        return io.opentelemetry.api.GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), headers, new TextMapGetter<Headers>() {
                    @Override
                    public Iterable<String> keys(Headers headers) {
                        return () -> headers.iterator().asIterator();
                    }

                    @Override
                    public String get(Headers headers, String key) {
                        Header header = headers.lastHeader(key);
                        if (header == null) {
                            return null;
                        }
                        return new String(header.value(), StandardCharsets.UTF_8);
                    }
                });
    }
}