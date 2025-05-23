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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Position;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.kafka.v2_6.KafkaTelemetry;
import io.opentelemetry.api.OpenTelemetry;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka implementation of the MessageProducer interface.
 * Publishes positions and device status updates to Kafka topics.
 */
public class KafkaMessageProducer implements MessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageProducer.class);

    private final Producer<Long, String> producer;
    private final ObjectMapper objectMapper;
    private final String positionsTopic;
    private final String deviceStatusTopic;
    private final KafkaTelemetry kafkaTelemetry;

    /**
     * Construct Kafka message producer with provided configuration.
     *
     * @param config Configuration parameters
     * @param openTelemetry OpenTelemetry instance for tracing
     */
    public KafkaMessageProducer(Config config, OpenTelemetry openTelemetry) {
        this.objectMapper = new ObjectMapper();
        this.positionsTopic = config.getString(Keys.KAFKA_POSITIONS_TOPIC.getKey(), "raw.positions");
        this.deviceStatusTopic = config.getString(Keys.KAFKA_DEVICE_STATUS_TOPIC.getKey(), "device.connections");
        
        // Configure Kafka producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                config.getString(Keys.KAFKA_BOOTSTRAP_SERVERS.getKey(), "localhost:9092"));
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, 
                config.getString(Keys.KAFKA_CLIENT_ID.getKey(), "traccar-protocol-service"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        
        // Create Kafka producer with OpenTelemetry instrumentation
        this.kafkaTelemetry = KafkaTelemetry.create(openTelemetry);
        this.producer = new KafkaProducer<>(properties);
        
        LOGGER.info("Kafka message producer initialized with topics: positions={}, deviceStatus={}", 
                positionsTopic, deviceStatusTopic);
    }

    @Override
    public void sendPosition(Position position) throws Exception {
        String json = objectMapper.writeValueAsString(position);
        
        // Create producer record with device ID as key for partitioning
        ProducerRecord<Long, String> record = new ProducerRecord<>(
                positionsTopic, position.getDeviceId(), json);
        
        // Inject OpenTelemetry context for distributed tracing
        record = kafkaTelemetry.wrap(record, Context.current());
        
        // Add span attributes for better tracing
        Span span = Span.current();
        span.setAttribute("messaging.system", "kafka");
        span.setAttribute("messaging.destination", positionsTopic);
        span.setAttribute("messaging.destination_kind", "topic");
        
        // Send record asynchronously with callback
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                LOGGER.warn("Failed to send position to Kafka", exception);
                span.recordException(exception);
            } else {
                LOGGER.debug("Position sent to Kafka: topic={}, partition={}, offset={}", 
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    @Override
    public void sendDeviceConnectionStatus(long deviceId, boolean connected) throws Exception {
        Map<String, Object> status = new HashMap<>();
        status.put("deviceId", deviceId);
        status.put("connected", connected);
        status.put("timestamp", System.currentTimeMillis());
        
        String json = objectMapper.writeValueAsString(status);
        
        // Create producer record with device ID as key for partitioning
        ProducerRecord<Long, String> record = new ProducerRecord<>(
                deviceStatusTopic, deviceId, json);
        
        // Inject OpenTelemetry context for distributed tracing
        record = kafkaTelemetry.wrap(record, Context.current());
        
        // Add span attributes for better tracing
        Span span = Span.current();
        span.setAttribute("messaging.system", "kafka");
        span.setAttribute("messaging.destination", deviceStatusTopic);
        span.setAttribute("messaging.destination_kind", "topic");
        
        // Send record asynchronously with callback
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                LOGGER.warn("Failed to send device status to Kafka", exception);
                span.recordException(exception);
            } else {
                LOGGER.debug("Device status sent to Kafka: topic={}, partition={}, offset={}", 
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
            LOGGER.info("Kafka message producer closed");
        }
    }
}