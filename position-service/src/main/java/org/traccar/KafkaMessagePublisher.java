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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka implementation of the MessagePublisher interface.
 * Handles publishing messages to Kafka topics with proper partitioning and headers.
 */
@Singleton
public class KafkaMessagePublisher implements MessagePublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessagePublisher.class);

    private final KafkaProducer<String, String> producer;
    private final Tracer tracer;

    @Inject
    public KafkaMessagePublisher(Config config, Tracer tracer) {
        this.tracer = tracer;
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                config.getString("kafka.bootstrap.servers", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, config.getString("kafka.producer.acks", "all"));
        props.put(ProducerConfig.RETRIES_CONFIG, config.getInteger("kafka.producer.retries", 3));
        props.put(ProducerConfig.LINGER_MS_CONFIG, config.getInteger("kafka.producer.linger.ms", 1));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 
                config.getInteger("kafka.producer.buffer.memory", 33554432));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 
                config.getInteger("kafka.producer.batch.size", 16384));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, 
                config.getString("kafka.producer.compression.type", "snappy"));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, 
                config.getBoolean("kafka.producer.enable.idempotence", true));
        
        producer = new KafkaProducer<>(props);
        
        LOGGER.info("Initialized Kafka message publisher with bootstrap servers: {}", 
                props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Override
    public CompletableFuture<Void> publish(String topic, String key, String payload) {
        return publish(topic, key, payload, new HashMap<>());
    }

    @Override
    public CompletableFuture<Void> publish(String topic, String key, String payload, Map<String, String> headers) {
        Span span = tracer.spanBuilder("kafka_publish").startSpan();
        
        try (var scope = span.makeCurrent()) {
            span.setAttribute("messaging.system", "kafka");
            span.setAttribute("messaging.destination", topic);
            span.setAttribute("messaging.destination_kind", "topic");
            span.setAttribute("messaging.kafka.key", key);
            
            // Convert headers to Kafka headers
            List<Header> kafkaHeaders = new ArrayList<>();
            headers.forEach((headerKey, headerValue) -> {
                kafkaHeaders.add(new RecordHeader(headerKey, 
                        headerValue.getBytes(StandardCharsets.UTF_8)));
                span.setAttribute("messaging.header." + headerKey, headerValue);
            });
            
            // Add OpenTelemetry context propagation
            // This allows distributed tracing across services
            kafkaHeaders.add(new RecordHeader("traceparent", 
                    span.getSpanContext().getTraceId().getBytes(StandardCharsets.UTF_8)));
            
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, key, payload, kafkaHeaders);
            
            CompletableFuture<Void> future = new CompletableFuture<>();
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOGGER.error("Failed to publish message to Kafka topic {}", topic, exception);
                    span.recordException(exception);
                    future.completeExceptionally(exception);
                } else {
                    LOGGER.debug("Published message to Kafka topic {} partition {} offset {}", 
                            metadata.topic(), metadata.partition(), metadata.offset());
                    span.setAttribute("messaging.kafka.partition", metadata.partition());
                    span.setAttribute("messaging.kafka.offset", metadata.offset());
                    future.complete(null);
                }
            });
            
            return future;
        } finally {
            span.end();
        }
    }

    /**
     * Closes the Kafka producer.
     * Should be called when the application is shutting down.
     */
    public void close() {
        if (producer != null) {
            producer.close();
            LOGGER.info("Closed Kafka producer");
        }
    }
}