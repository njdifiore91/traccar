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
package org.traccar.messaging;

import io.opentelemetry.context.propagation.TextMapPropagator;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.MessageBrokerPublisher;
import org.traccar.config.Config;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Kafka implementation of the MessageBrokerPublisher interface.
 * 
 * This class publishes messages to a Kafka broker, with support for distributed tracing
 * and correlation IDs. It uses the Kafka producer to publish messages to topics, with
 * headers for tracing and correlation.
 */
@Singleton
public class KafkaMessageBrokerPublisher implements MessageBrokerPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageBrokerPublisher.class);

    private final KafkaProducer<String, String> producer;
    private final TextMapPropagator propagator;

    /**
     * Constructs a new KafkaMessageBrokerPublisher with the necessary dependencies.
     *
     * @param config Configuration
     * @param producer Kafka producer
     * @param propagator OpenTelemetry context propagator for distributed tracing
     */
    @Inject
    public KafkaMessageBrokerPublisher(
            Config config,
            KafkaProducer<String, String> producer,
            TextMapPropagator propagator) {
        this.producer = producer;
        this.propagator = propagator;
    }

    @Override
    public void publish(String topic, String payload, Map<String, String> headers) throws Exception {
        // Convert headers to Kafka headers
        List<Header> kafkaHeaders = new ArrayList<>();
        headers.forEach((key, value) -> {
            kafkaHeaders.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
        });
        
        // Create a producer record with the payload and headers
        ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,
                null,  // No partition specified, Kafka will determine based on key
                headers.getOrDefault("x-correlation-id", null),  // Use correlation ID as key for partitioning
                payload,
                kafkaHeaders
        );
        
        // Send the record to Kafka
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                LOGGER.error("Error publishing message to Kafka topic {}: {}", topic, exception.getMessage());
            } else {
                LOGGER.debug("Published message to Kafka topic {} partition {} offset {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    @Override
    public TextMapPropagator getPropagator() {
        return propagator;
    }
}