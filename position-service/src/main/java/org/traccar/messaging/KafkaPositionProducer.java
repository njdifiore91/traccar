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

import com.google.inject.Inject;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.KafkaTelemetry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Kafka implementation of the PositionProducer interface for the Position Processing Service.
 * Configures and manages Kafka producer instances, handles message serialization, and publishes
 * enriched position messages.
 */
public class KafkaPositionProducer implements PositionProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPositionProducer.class);

    private final Producer<String, byte[]> producer;
    private final ProtobufMessageSerializer serializer;
    private final String topic;
    private final Tracer tracer;
    private final KafkaTelemetry kafkaTelemetry;

    private static final TextMapSetter<Map<String, String>> SETTER =
            (carrier, key, value) -> {
                if (carrier != null) {
                    carrier.put(key, value);
                }
            };

    /**
     * Constructs a new KafkaPositionProducer with the specified configuration.
     *
     * @param config The message broker configuration
     */
    @Inject
    public KafkaPositionProducer(MessageBrokerConfig config) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // Add additional configuration from the config object
        Map<String, Object> additionalConfig = config.getKafkaProducerProperties();
        if (additionalConfig != null) {
            properties.putAll(additionalConfig);
        }

        // Initialize OpenTelemetry
        tracer = GlobalOpenTelemetry.getTracer("org.traccar.messaging.KafkaPositionProducer");
        kafkaTelemetry = KafkaTelemetry.create(GlobalOpenTelemetry.get());
        
        // Add OpenTelemetry configuration
        properties.putAll(kafkaTelemetry.metricConfigProperties());

        producer = new KafkaProducer<>(properties);
        serializer = new ProtobufMessageSerializer();
        topic = config.getPositionTopic();
        
        LOGGER.info("Initialized Kafka position producer for topic: {}", topic);
    }

    @Override
    public CompletableFuture<Void> sendAsync(PositionMessage positionMessage) {
        return sendAsync(positionMessage, new HashMap<>());
    }

    @Override
    public CompletableFuture<Void> sendAsync(PositionMessage positionMessage, Map<String, String> headers) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        // Create a span for the send operation
        Span span = tracer.spanBuilder("send_position")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("messaging.system", "kafka")
                .setAttribute("messaging.destination", topic)
                .setAttribute("messaging.destination_kind", "topic")
                .setAttribute("messaging.protocol", "kafka")
                .setAttribute("messaging.kafka.client_id", producer.clientId())
                .setAttribute("device.id", String.valueOf(positionMessage.getDeviceId()))
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Serialize the position message
            byte[] serializedMessage = serializer.serialize(positionMessage);
            
            // Create the producer record with the device ID as the key for partitioning
            String key = String.valueOf(positionMessage.getDeviceId());
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, serializedMessage);
            
            // Add headers to the record
            List<Header> recordHeaders = new ArrayList<>();
            
            // Add OpenTelemetry context propagation headers
            Map<String, String> otelHeaders = new HashMap<>();
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                    .inject(Context.current(), otelHeaders, SETTER);
            
            otelHeaders.forEach((headerKey, headerValue) -> 
                    recordHeaders.add(new RecordHeader(headerKey, headerValue.getBytes(StandardCharsets.UTF_8))));
            
            // Add custom headers
            headers.forEach((headerKey, headerValue) -> 
                    recordHeaders.add(new RecordHeader(headerKey, headerValue.getBytes(StandardCharsets.UTF_8))));
            
            // Add correlation ID if available
            String correlationId = positionMessage.getCorrelationId();
            if (correlationId != null && !correlationId.isEmpty()) {
                recordHeaders.add(new RecordHeader("correlation-id", correlationId.getBytes(StandardCharsets.UTF_8)));
                span.setAttribute("messaging.correlation_id", correlationId);
            }
            
            // Add all headers to the record
            recordHeaders.forEach(record.headers()::add);
            
            // Send the record asynchronously
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    span.setStatus(StatusCode.ERROR, exception.getMessage());
                    span.recordException(exception);
                    future.completeExceptionally(new MessageException("Failed to send position message", exception));
                } else {
                    span.setAttribute("messaging.kafka.topic_partition", metadata.partition());
                    span.setAttribute("messaging.kafka.offset", metadata.offset());
                    future.complete(null);
                }
                span.end();
            });
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            span.end();
            future.completeExceptionally(new MessageException("Error serializing or sending position message", e));
        }
        
        return future;
    }

    @Override
    public void send(PositionMessage positionMessage) throws MessageException {
        send(positionMessage, new HashMap<>());
    }

    @Override
    public void send(PositionMessage positionMessage, Map<String, String> headers) throws MessageException {
        try {
            sendAsync(positionMessage, headers).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessageException("Interrupted while sending position message", e);
        } catch (ExecutionException e) {
            throw new MessageException("Failed to send position message", e.getCause());
        }
    }

    @Override
    public void flush() throws MessageException {
        try {
            producer.flush();
        } catch (Exception e) {
            throw new MessageException("Failed to flush position producer", e);
        }
    }

    @Override
    public void close() {
        try {
            producer.close();
            LOGGER.info("Closed Kafka position producer");
        } catch (Exception e) {
            LOGGER.warn("Error closing Kafka position producer", e);
        }
    }
}