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
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.KafkaTelemetry;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Kafka implementation of the PositionConsumer interface for the Position Processing Service.
 * Configures and manages Kafka consumer instances, handles message deserialization, and processes
 * raw position messages.
 */
public class KafkaPositionConsumer implements PositionConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPositionConsumer.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final Duration COMMIT_INTERVAL = Duration.ofSeconds(5);

    private final KafkaConsumer<String, byte[]> consumer;
    private final ProtobufMessageDeserializer deserializer;
    private final String topic;
    private final ExecutorService executorService;
    private final AtomicBoolean running;
    private final Tracer tracer;
    private final KafkaTelemetry kafkaTelemetry;
    private Consumer<PositionMessage> messageHandler;
    private Consumer<Exception> errorHandler;
    private long lastCommitTime;

    private static final TextMapGetter<ConsumerRecord<String, byte[]>> GETTER =
            new TextMapGetter<ConsumerRecord<String, byte[]>>() {
                @Override
                public Iterable<String> keys(ConsumerRecord<String, byte[]> carrier) {
                    return () -> Collections.list(carrier.headers().toArray()).stream()
                            .map(Header::key)
                            .iterator();
                }

                @Override
                public String get(ConsumerRecord<String, byte[]> carrier, String key) {
                    Header header = carrier.headers().lastHeader(key);
                    if (header == null) {
                        return null;
                    }
                    return new String(header.value(), StandardCharsets.UTF_8);
                }
            };

    /**
     * Constructs a new KafkaPositionConsumer with the specified configuration.
     *
     * @param config The message broker configuration
     */
    @Inject
    public KafkaPositionConsumer(MessageBrokerConfig config) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.getConsumerGroupId());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        
        // Configure partition assignment strategy to ensure ordering by device ID
        // Use the sticky assignor to minimize partition reassignments during rebalancing
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, 
                "org.apache.kafka.clients.consumer.StickyAssignor");
        
        // Configure consumer for better performance and reliability
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024); // Wait until at least 1KB of data is available
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500); // But don't wait more than 500ms
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 minutes max processing time
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // 30 seconds session timeout
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000); // 10 seconds heartbeat interval
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000); // 1 minute default API timeout
        
        // Add additional configuration from the config object
        Map<String, Object> additionalConfig = config.getKafkaConsumerProperties();
        if (additionalConfig != null) {
            properties.putAll(additionalConfig);
        }

        // Initialize OpenTelemetry
        tracer = GlobalOpenTelemetry.getTracer("org.traccar.messaging.KafkaPositionConsumer");
        kafkaTelemetry = KafkaTelemetry.create(GlobalOpenTelemetry.get());
        
        // Add OpenTelemetry configuration
        properties.putAll(kafkaTelemetry.metricConfigProperties());

        consumer = new KafkaConsumer<>(properties);
        deserializer = new ProtobufMessageDeserializer();
        topic = config.getRawPositionTopic();
        
        // Create a dedicated executor service for the consumer
        executorService = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "kafka-position-consumer");
            thread.setDaemon(true);
            return thread;
        });
        
        running = new AtomicBoolean(false);
        lastCommitTime = System.currentTimeMillis();
        
        LOGGER.info("Initialized Kafka position consumer for topic: {} with group ID: {}", 
                topic, config.getConsumerGroupId());
        LOGGER.info("Bootstrap servers: {}", config.getKafkaBootstrapServers());
    }

    @Override
    public void subscribe(Consumer<PositionMessage> messageHandler, Consumer<Exception> errorHandler) {
        this.messageHandler = messageHandler;
        this.errorHandler = errorHandler;
        
        if (running.compareAndSet(false, true)) {
            // Subscribe to the topic
            consumer.subscribe(Collections.singletonList(topic));
            
            // Start the consumer thread
            executorService.submit(this::consumeMessages);
            
            // Log the subscription
            LOGGER.info("Subscribed to topic: {} with consumer group: {}", 
                    topic, consumer.groupMetadata().groupId());
            
            // Log the partition assignment strategy
            LOGGER.info("Using partition assignment strategy: {}", 
                    consumer.groupMetadata().partitionAssignor());
        } else {
            LOGGER.warn("Consumer is already running");
        }
    }

    @Override
    public void unsubscribe() {
        if (running.compareAndSet(true, false)) {
            consumer.wakeup();
            LOGGER.info("Unsubscribed from topic: {}", topic);
        }
    }

    @Override
    public void pause() {
        consumer.pause(consumer.assignment());
        LOGGER.info("Paused consumption from topic: {}", topic);
    }

    @Override
    public void resume() {
        consumer.resume(consumer.assignment());
        LOGGER.info("Resumed consumption from topic: {}", topic);
    }

    @Override
    public void seek(String partition, long offset) {
        for (TopicPartition topicPartition : consumer.assignment()) {
            if (String.valueOf(topicPartition.partition()).equals(partition)) {
                consumer.seek(topicPartition, offset);
                LOGGER.info("Seeking to offset {} in partition {} of topic: {}", offset, partition, topic);
                break;
            }
        }
    }

    @Override
    public void seekToBeginning() {
        consumer.seekToBeginning(consumer.assignment());
        LOGGER.info("Seeking to beginning of all partitions in topic: {}", topic);
    }

    @Override
    public void seekToEnd() {
        consumer.seekToEnd(consumer.assignment());
        LOGGER.info("Seeking to end of all partitions in topic: {}", topic);
    }

    @Override
    public void commitSync() {
        try {
            consumer.commitSync();
            lastCommitTime = System.currentTimeMillis();
            LOGGER.debug("Committed offsets synchronously");
        } catch (CommitFailedException e) {
            LOGGER.warn("Failed to commit offsets synchronously", e);
            if (errorHandler != null) {
                errorHandler.accept(new MessageException("Failed to commit offsets", e, MessageException.ErrorType.ACKNOWLEDGMENT));
            }
        }
    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        consumer.commitAsync((offsets, exception) -> {
            if (exception != null) {
                LOGGER.warn("Failed to commit offsets asynchronously", exception);
                if (errorHandler != null) {
                    errorHandler.accept(new MessageException("Failed to commit offsets", exception, MessageException.ErrorType.ACKNOWLEDGMENT));
                }
                future.completeExceptionally(exception);
            } else {
                lastCommitTime = System.currentTimeMillis();
                LOGGER.debug("Committed offsets asynchronously");
                future.complete(null);
            }
        });
        return future;
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            consumer.wakeup();
        }
        
        try {
            executorService.shutdown();
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
        }
        
        try {
            consumer.close();
            LOGGER.info("Closed Kafka position consumer");
        } catch (Exception e) {
            LOGGER.warn("Error closing Kafka position consumer", e);
        }
    }

    /**
     * Main consumer loop that polls for messages and processes them.
     */
    private void consumeMessages() {
        try {
            while (running.get()) {
                try {
                    // Poll for new records
                    ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
                    int recordCount = records.count();
                    
                    if (recordCount > 0) {
                        LOGGER.debug("Received {} records from topic {}", recordCount, topic);
                        
                        // Process each record
                        for (ConsumerRecord<String, byte[]> record : records) {
                            // Log the record details at trace level
                            LOGGER.trace("Processing record from topic {} partition {} offset {}", 
                                    record.topic(), record.partition(), record.offset());
                            
                            // Process the record
                            processRecord(record);
                        }
                    }
                    
                    // Commit offsets periodically or after processing a batch
                    if (recordCount > 0 || System.currentTimeMillis() - lastCommitTime > COMMIT_INTERVAL.toMillis()) {
                        consumer.commitAsync((offsets, exception) -> {
                            if (exception != null) {
                                LOGGER.warn("Async commit failed", exception);
                            } else {
                                LOGGER.debug("Async commit succeeded for {} offsets", offsets.size());
                            }
                        });
                        lastCommitTime = System.currentTimeMillis();
                    }
                } catch (WakeupException e) {
                    // Ignore wakeup exception, it's used to interrupt the consumer
                    LOGGER.info("Consumer wakeup requested");
                    break;
                } catch (Exception e) {
                    LOGGER.error("Error processing messages", e);
                    if (errorHandler != null) {
                        errorHandler.accept(e);
                    }
                    
                    // Add a small delay to prevent tight error loops
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            // Ensure we commit offsets before shutting down
            try {
                LOGGER.info("Committing final offsets before shutdown");
                consumer.commitSync();
            } catch (Exception e) {
                LOGGER.warn("Failed to commit offsets on shutdown", e);
            }
        }
    }

    /**
     * Processes a single Kafka record.
     *
     * @param record The Kafka record to process
     */
    private void processRecord(ConsumerRecord<String, byte[]> record) {
        // Extract OpenTelemetry context from record headers
        Context extractedContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), record, GETTER);
        
        // Create a span for the receive operation
        Span span = tracer.spanBuilder("receive_position")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("messaging.system", "kafka")
                .setAttribute("messaging.destination", record.topic())
                .setAttribute("messaging.destination_kind", "topic")
                .setAttribute("messaging.protocol", "kafka")
                .setAttribute("messaging.kafka.partition", record.partition())
                .setAttribute("messaging.kafka.offset", record.offset())
                .setAttribute("messaging.kafka.consumer_group", consumer.groupMetadata().groupId())
                .setParent(extractedContext)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Extract correlation ID from headers if available
            String correlationId = null;
            Header correlationHeader = record.headers().lastHeader("correlation-id");
            if (correlationHeader != null) {
                correlationId = new String(correlationHeader.value(), StandardCharsets.UTF_8);
                span.setAttribute("messaging.correlation_id", correlationId);
            }
            
            // Deserialize the message
            PositionMessage positionMessage = deserializer.deserialize(record.value());
            
            // Set correlation ID on the message if available
            if (correlationId != null) {
                positionMessage.setCorrelationId(correlationId);
            }
            
            // Set device ID attribute on the span
            span.setAttribute("device.id", String.valueOf(positionMessage.getDeviceId()));
            
            // Process the message
            if (messageHandler != null) {
                try {
                    messageHandler.accept(positionMessage);
                } catch (Exception e) {
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    throw e;
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error processing record from topic {} partition {} offset {}", 
                    record.topic(), record.partition(), record.offset(), e);
            if (errorHandler != null) {
                errorHandler.accept(new MessageException("Error processing position message", e));
            }
        } finally {
            span.end();
        }
    }

    /**
     * Deserializer for Protocol Buffer messages.
     */
    private static class ProtobufMessageDeserializer {
        
        /**
         * Deserializes a byte array into a PositionMessage.
         *
         * @param data The serialized message data
         * @return The deserialized PositionMessage
         * @throws MessageException If deserialization fails
         */
        public PositionMessage deserialize(byte[] data) throws MessageException {
            try {
                // In a real implementation, this would use Protocol Buffers to deserialize
                // For example:
                // org.traccar.proto.Position protoPosition = org.traccar.proto.Position.parseFrom(data);
                // PositionMessage message = new PositionMessage();
                // message.setId(protoPosition.getId());
                // message.setDeviceId(protoPosition.getDeviceId());
                // ... and so on for all fields
                
                // For demonstration purposes, we'll create a simple implementation
                // that assumes the data is a serialized PositionMessage
                // In production, this would be replaced with actual Protocol Buffers deserialization
                
                // This is a placeholder implementation
                PositionMessage message = new PositionMessage();
                
                // In a real implementation, these values would come from the deserialized protobuf message
                // This is just a placeholder to demonstrate the concept
                message.setId(System.currentTimeMillis()); // Use timestamp as a unique ID for demonstration
                message.setDeviceId(Long.parseLong(new String(data, 0, 8, StandardCharsets.UTF_8).trim()));
                message.setProtocol("protobuf");
                message.setDeviceTime(java.time.Instant.now().toString());
                message.setServerTime(java.time.Instant.now().toString());
                message.setFixTime(java.time.Instant.now().toString());
                message.setValid(true);
                
                // Extract latitude and longitude from the data (simplified example)
                if (data.length >= 24) {
                    message.setLatitude(Double.longBitsToDouble(bytesToLong(data, 8)));
                    message.setLongitude(Double.longBitsToDouble(bytesToLong(data, 16)));
                } else {
                    message.setLatitude(0.0);
                    message.setLongitude(0.0);
                }
                
                // Add any additional attributes if available in the data
                Map<String, Object> attributes = new HashMap<>();
                attributes.put("raw", true);
                attributes.put("source", "kafka");
                message.setAttributes(attributes);
                
                return message;
            } catch (Exception e) {
                throw new MessageException("Failed to deserialize position message", e, MessageException.ErrorType.SERIALIZATION);
            }
        }
        
        /**
         * Converts 8 bytes from the specified offset in the byte array to a long value.
         *
         * @param bytes The byte array
         * @param offset The offset in the byte array
         * @return The long value
         */
        private long bytesToLong(byte[] bytes, int offset) {
            long result = 0;
            for (int i = 0; i < 8; i++) {
                result <<= 8;
                result |= (bytes[offset + i] & 0xFF);
            }
            return result;
        }
    }
}