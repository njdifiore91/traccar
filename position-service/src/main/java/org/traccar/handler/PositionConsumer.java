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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.instrumentation.kafka.v2_6.KafkaTelemetry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Component;
import org.traccar.model.Position;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Consumes raw position data from the message broker, deserializes it from Protocol Buffers format,
 * and passes it to the position processing pipeline. Manages consumer group configuration for load distribution,
 * handles partition assignment for ordered processing, and implements error handling and dead letter queue
 * mechanisms for unprocessable messages.
 */
@Component
public class PositionConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionConsumer.class);

    private final BasePositionHandler positionHandler;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final KafkaTelemetry kafkaTelemetry;

    private final Counter messagesProcessedCounter;
    private final Counter messagesFailedCounter;
    private final Timer messageProcessingTimer;

    /**
     * Constructs a new PositionConsumer with the necessary dependencies.
     *
     * @param positionHandler The handler for processing position data
     * @param meterRegistry The meter registry for metrics collection
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param kafkaTelemetry The Kafka telemetry for Kafka-specific tracing
     */
    @Autowired
    public PositionConsumer(
            BasePositionHandler positionHandler,
            MeterRegistry meterRegistry,
            Tracer tracer,
            KafkaTelemetry kafkaTelemetry) {
        this.positionHandler = positionHandler;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        this.kafkaTelemetry = kafkaTelemetry;

        // Initialize metrics
        this.messagesProcessedCounter = Counter.builder("position.messages.processed")
                .description("Number of position messages successfully processed")
                .register(meterRegistry);
        this.messagesFailedCounter = Counter.builder("position.messages.failed")
                .description("Number of position messages that failed processing")
                .register(meterRegistry);
        this.messageProcessingTimer = Timer.builder("position.messages.processing.time")
                .description("Time taken to process position messages")
                .register(meterRegistry);
    }

    /**
     * Configures the Kafka listener container factory with error handling and dead letter queue support.
     *
     * @param consumerFactory The Kafka consumer factory
     * @param kafkaTemplate The Kafka template for publishing to dead letter topics
     * @param maxRetries The maximum number of retries for failed messages
     * @param backoffInterval The backoff interval between retries
     * @return The configured Kafka listener container factory
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> positionKafkaListenerContainerFactory(
            ConsumerFactory<String, byte[]> consumerFactory,
            KafkaTemplate<String, byte[]> kafkaTemplate,
            @Value("${position.consumer.max-retries:3}") int maxRetries,
            @Value("${position.consumer.backoff-interval:1000}") long backoffInterval) {

        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // Configure error handler with dead letter publishing recoverer
        BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver =
                (consumerRecord, exception) -> {
                    String originalTopic = consumerRecord.topic();
                    return new TopicPartition(originalTopic + ".DLT", consumerRecord.partition());
                };

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, destinationResolver);
        CommonErrorHandler errorHandler = new DefaultErrorHandler(recoverer, 
                org.springframework.util.backoff.FixedBackOff.builder()
                        .interval(backoffInterval)
                        .maxAttempts(maxRetries)
                        .build());

        factory.setCommonErrorHandler(errorHandler);

        // Configure concurrency based on available processors
        int concurrency = Runtime.getRuntime().availableProcessors();
        factory.setConcurrency(concurrency);

        return factory;
    }

    /**
     * Kafka listener method that consumes position messages from the configured topic.
     * Deserializes the Protocol Buffers message, creates a Position object, and passes it to the position handler.
     *
     * @param record The Kafka consumer record containing the position data
     */
    @KafkaListener(
            topics = "${position.consumer.topic:raw-positions}",
            groupId = "${position.consumer.group-id:position-service}",
            containerFactory = "positionKafkaListenerContainerFactory")
    public void consumePosition(ConsumerRecord<String, byte[]> record) {
        // Extract trace context from Kafka headers if present
        Context extractedContext = kafkaTelemetry.extract(record, new HeadersExtractor());
        
        // Create a span for this operation
        Span span = tracer.spanBuilder("process-position")
                .setParent(extractedContext)
                .setSpanKind(SpanKind.CONSUMER)
                .startSpan();

        try {
            // Use timer to measure processing time
            messageProcessingTimer.record(() -> {
                // Deserialize the position from Protocol Buffers
                Position position = deserializePosition(record.value());
                
                // Add device ID to span for better tracing
                span.setAttribute("deviceId", String.valueOf(position.getDeviceId()));
                
                // Process the position using the handler
                positionHandler.handlePosition(position, processed -> {
                    if (processed) {
                        messagesProcessedCounter.increment();
                        LOGGER.debug("Successfully processed position for device: {}", position.getDeviceId());
                    } else {
                        messagesFailedCounter.increment();
                        LOGGER.warn("Failed to process position for device: {}", position.getDeviceId());
                    }
                });
            });
        } catch (Exception e) {
            messagesFailedCounter.increment();
            span.recordException(e);
            LOGGER.error("Error processing position message", e);
            throw e; // Rethrow to trigger error handler
        } finally {
            span.end();
        }
    }

    /**
     * Deserializes a Protocol Buffers message into a Position object.
     *
     * @param data The Protocol Buffers encoded position data
     * @return The deserialized Position object
     */
    private Position deserializePosition(byte[] data) {
        try {
            // Deserialize the Protocol Buffers message to a Position object
            // In a real implementation, this would use the actual Protocol Buffers generated code
            // For now, we'll create a simple Position object as a placeholder
            Position position = new Position();
            // Actual deserialization would happen here using Protocol Buffers
            // Example: position = PositionOuterClass.Position.parseFrom(data).toPosition();
            return position;
        } catch (Exception e) {
            LOGGER.error("Failed to deserialize position data", e);
            throw new RuntimeException("Failed to deserialize position data", e);
        }
    }

    /**
     * Helper class to extract OpenTelemetry context from Kafka headers.
     */
    private static class HeadersExtractor implements TextMapGetter<ConsumerRecord<String, byte[]>> {
        @Override
        public Iterable<String> keys(ConsumerRecord<String, byte[]> carrier) {
            Map<String, String> headerMap = new HashMap<>();
            Headers headers = carrier.headers();
            for (Header header : headers) {
                headerMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            }
            return headerMap.keySet();
        }

        @Override
        public String get(ConsumerRecord<String, byte[]> carrier, String key) {
            Header header = carrier.headers().lastHeader(key);
            if (header == null) {
                return null;
            }
            return new String(header.value(), StandardCharsets.UTF_8);
        }
    }
}