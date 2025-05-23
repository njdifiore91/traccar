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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.traccar.model.Position;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Consumer for position messages from the message broker.
 * 
 * This class consumes position messages from the message broker, extracts the correlation ID
 * and tracing information from the message headers, and passes the position to the
 * ProcessingHandler for processing.
 */
@Component
public class PositionConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionConsumer.class);

    private final ProcessingHandler processingHandler;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;
    private final TextMapPropagator propagator;
    private final MetricsCollector metricsCollector;

    /**
     * Constructs a new PositionConsumer with the necessary dependencies.
     *
     * @param processingHandler Handler for processing positions
     * @param objectMapper JSON object mapper for deserializing positions
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param propagator OpenTelemetry context propagator
     * @param metricsCollector Metrics collector
     */
    @Inject
    public PositionConsumer(
            ProcessingHandler processingHandler,
            ObjectMapper objectMapper,
            Tracer tracer,
            TextMapPropagator propagator,
            MetricsCollector metricsCollector) {
        this.processingHandler = processingHandler;
        this.objectMapper = objectMapper;
        this.tracer = tracer;
        this.propagator = propagator;
        this.metricsCollector = metricsCollector;
    }

    /**
     * Consume a position message from the message broker.
     * 
     * This method is called by the Kafka listener when a new position message is received.
     * It extracts the correlation ID and tracing information from the message headers,
     * deserializes the position, and passes it to the ProcessingHandler for processing.
     *
     * @param record Kafka consumer record containing the position message
     * @param acknowledgment Kafka acknowledgment for manual acknowledgment
     */
    @KafkaListener(topics = "${position.topic:raw-positions}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        // Extract headers from the Kafka record
        Map<String, String> headers = extractHeaders(record);
        
        // Extract correlation ID from headers or generate a new one
        String correlationId = headers.getOrDefault("x-correlation-id", UUID.randomUUID().toString());
        
        // Extract tracing context from headers
        Context context = propagator.extract(Context.current(), headers, new HeadersGetter());
        
        // Create a new span for position consumption
        Span span = tracer.spanBuilder("consume-position")
                .setParent(context)
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("kafka.topic", record.topic())
                .setAttribute("kafka.partition", record.partition())
                .setAttribute("kafka.offset", record.offset())
                .setAttribute("x-correlation-id", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Record that a position was received
            metricsCollector.recordPositionReceived();
            
            // Deserialize the position from JSON
            Position position = objectMapper.readValue(record.value(), Position.class);
            
            // Add correlation ID to position attributes for tracing
            position.set("correlationId", correlationId);
            
            // Process the position asynchronously
            CompletableFuture<Void> future = processingHandler.processPosition(position, headers);
            
            // Acknowledge the message when processing is complete
            future.whenComplete((result, error) -> {
                if (error != null) {
                    // Record error metrics
                    metricsCollector.recordPositionError();
                    LOGGER.error("Error processing position with correlation ID {}: {}",
                            correlationId, error.getMessage());
                    span.recordException(error);
                    span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, error.getMessage());
                } else {
                    // Record success metrics
                    metricsCollector.recordPositionProcessed();
                    LOGGER.debug("Successfully processed position with correlation ID {}", correlationId);
                }
                
                // Acknowledge the message regardless of success or failure
                // The transaction outbox pattern ensures that the message will be processed eventually
                acknowledgment.acknowledge();
            });
        } catch (Exception e) {
            // Record error metrics
            metricsCollector.recordPositionError();
            LOGGER.error("Error consuming position message: {}", e.getMessage());
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            
            // Acknowledge the message to prevent redelivery
            // The error will be logged and monitored, but we don't want to block the consumer
            acknowledgment.acknowledge();
        } finally {
            span.end();
        }
    }

    /**
     * Extract headers from a Kafka consumer record.
     *
     * @param record Kafka consumer record
     * @return Map of header key-value pairs
     */
    private Map<String, String> extractHeaders(ConsumerRecord<String, String> record) {
        Map<String, String> headers = new HashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }
        return headers;
    }

    /**
     * TextMapGetter implementation for extracting tracing context from message headers.
     */
    private static class HeadersGetter implements TextMapGetter<Map<String, String>> {
        @Override
        public Iterable<String> keys(Map<String, String> headers) {
            return headers.keySet();
        }

        @Override
        public String get(Map<String, String> headers, String key) {
            return headers.get(key);
        }
    }
}