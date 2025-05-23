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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Position;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the MessageProducer interface for Apache Kafka.
 * Handles the details of Kafka producer configuration, topic partitioning,
 * and message delivery guarantees. Integrates with OpenTelemetry for distributed
 * tracing and Micrometer for metrics collection.
 */
@Singleton
public class KafkaMessageProducer implements MessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageProducer.class);

    private final Producer<String, String> producer;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final MessagingConfig config;

    private final Timer positionSendTimer;
    private final Timer deviceStatusSendTimer;
    private final Counter positionSendErrorCounter;
    private final Counter deviceStatusSendErrorCounter;
    private final Counter totalMessagesSentCounter;

    private static final TextMapSetter<ProducerRecord<String, String>> KAFKA_HEADER_SETTER =
            (record, key, value) -> record.headers().add(key, value.getBytes());

    /**
     * Creates a new KafkaMessageProducer with the specified dependencies.
     *
     * @param objectMapper JSON serializer
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     * @param config Messaging configuration
     */
    @Inject
    public KafkaMessageProducer(
            ObjectMapper objectMapper,
            Tracer tracer,
            MeterRegistry meterRegistry,
            MessagingConfig config) {
        this.objectMapper = objectMapper;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.config = config;

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Configure acknowledgment level
        properties.put(ProducerConfig.ACKS_CONFIG, config.getKafkaAcksConfig());
        
        // Configure retries
        properties.put(ProducerConfig.RETRIES_CONFIG, config.getKafkaRetries());
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, config.getKafkaRetryBackoffMs());
        
        // Configure batching
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getKafkaBatchSize());
        properties.put(ProducerConfig.LINGER_MS_CONFIG, config.getKafkaLingerMs());
        
        // Configure buffer memory
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getKafkaBufferMemory());
        
        // Configure compression
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getKafkaCompressionType());
        
        // Enable idempotence for exactly-once semantics if configured
        if (config.isKafkaEnableIdempotence()) {
            properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            // When idempotence is enabled, max.in.flight.requests.per.connection must be <= 5
            properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        }
        
        // Configure client ID for metrics and logging
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, config.getKafkaClientId());

        producer = new KafkaProducer<>(properties);

        // Initialize metrics
        positionSendTimer = meterRegistry.timer("kafka.producer.position.send");
        deviceStatusSendTimer = meterRegistry.timer("kafka.producer.device.status.send");
        positionSendErrorCounter = meterRegistry.counter("kafka.producer.position.errors");
        deviceStatusSendErrorCounter = meterRegistry.counter("kafka.producer.device.status.errors");
        totalMessagesSentCounter = meterRegistry.counter("kafka.producer.messages.sent");

        LOGGER.info("Initialized Kafka producer with bootstrap servers: {}", config.getKafkaBootstrapServers());
    }

    @Override
    public void sendPosition(Position position) throws Exception {
        String key = Long.toString(position.getDeviceId());
        String topic = config.getPositionsTopic();
        
        Span span = tracer.spanBuilder("kafka.send.position")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("messaging.system", "kafka")
                .setAttribute("messaging.destination", topic)
                .setAttribute("messaging.destination_kind", "topic")
                .setAttribute("messaging.kafka.client_id", config.getKafkaClientId())
                .setAttribute("messaging.kafka.partition", position.getDeviceId() % config.getKafkaPartitionCount())
                .setAttribute("device.id", position.getDeviceId())
                .startSpan();
        
        try {
            String value = objectMapper.writeValueAsString(position);
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic, 
                    (int) (position.getDeviceId() % config.getKafkaPartitionCount()), 
                    key, 
                    value);
            
            // Inject tracing context into Kafka headers
            tracer.getOpenTelemetry().getPropagators().getTextMapPropagator()
                    .inject(Context.current().with(span), record, KAFKA_HEADER_SETTER);
            
            // Send the message and measure the time it takes
            positionSendTimer.record(() -> {
                try {
                    Future<RecordMetadata> future = producer.send(record);
                    if (config.isKafkaSyncSend()) {
                        // Wait for the send to complete if synchronous sending is enabled
                        future.get(config.getKafkaSendTimeoutMs(), TimeUnit.MILLISECONDS);
                    }
                    totalMessagesSentCounter.increment();
                } catch (Exception e) {
                    positionSendErrorCounter.increment();
                    span.recordException(e);
                    throw new RuntimeException("Failed to send position to Kafka", e);
                }
            });
        } catch (JsonProcessingException e) {
            positionSendErrorCounter.increment();
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    @Override
    public void sendDeviceConnectionStatus(long deviceId, boolean connected) throws Exception {
        String key = Long.toString(deviceId);
        String topic = config.getDeviceStatusTopic();
        
        Span span = tracer.spanBuilder("kafka.send.device.status")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("messaging.system", "kafka")
                .setAttribute("messaging.destination", topic)
                .setAttribute("messaging.destination_kind", "topic")
                .setAttribute("messaging.kafka.client_id", config.getKafkaClientId())
                .setAttribute("messaging.kafka.partition", deviceId % config.getKafkaPartitionCount())
                .setAttribute("device.id", deviceId)
                .setAttribute("device.connected", connected)
                .startSpan();
        
        try {
            // Create a simple status object to serialize
            DeviceStatus status = new DeviceStatus(deviceId, connected);
            String value = objectMapper.writeValueAsString(status);
            
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic, 
                    (int) (deviceId % config.getKafkaPartitionCount()), 
                    key, 
                    value);
            
            // Inject tracing context into Kafka headers
            tracer.getOpenTelemetry().getPropagators().getTextMapPropagator()
                    .inject(Context.current().with(span), record, KAFKA_HEADER_SETTER);
            
            // Send the message and measure the time it takes
            deviceStatusSendTimer.record(() -> {
                try {
                    Future<RecordMetadata> future = producer.send(record);
                    if (config.isKafkaSyncSend()) {
                        // Wait for the send to complete if synchronous sending is enabled
                        future.get(config.getKafkaSendTimeoutMs(), TimeUnit.MILLISECONDS);
                    }
                    totalMessagesSentCounter.increment();
                } catch (Exception e) {
                    deviceStatusSendErrorCounter.increment();
                    span.recordException(e);
                    throw new RuntimeException("Failed to send device status to Kafka", e);
                }
            });
        } catch (JsonProcessingException e) {
            deviceStatusSendErrorCounter.increment();
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    @Override
    public boolean isConnected() {
        try {
            // Check if the producer is connected by sending a metadata request
            producer.partitionsFor(config.getPositionsTopic());
            return true;
        } catch (Exception e) {
            LOGGER.warn("Failed to check Kafka connection", e);
            return false;
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            try {
                // Flush any pending messages before closing
                producer.flush();
                producer.close(config.getKafkaCloseTimeoutMs(), TimeUnit.MILLISECONDS);
                LOGGER.info("Kafka producer closed successfully");
            } catch (Exception e) {
                LOGGER.error("Error closing Kafka producer", e);
            }
        }
    }

    /**
     * Simple class to represent device connection status for serialization.
     */
    private static class DeviceStatus {
        private final long deviceId;
        private final boolean connected;

        public DeviceStatus(long deviceId, boolean connected) {
            this.deviceId = deviceId;
            this.connected = connected;
        }

        public long getDeviceId() {
            return deviceId;
        }

        public boolean isConnected() {
            return connected;
        }
    }
}