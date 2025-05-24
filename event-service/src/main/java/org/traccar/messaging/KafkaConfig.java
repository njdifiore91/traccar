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

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

import org.traccar.model.Event;
import org.traccar.model.Position;

/**
 * Kafka configuration for the Event Processing Service.
 * Configures Kafka consumer for position data, producer for events,
 * and Kafka Streams for stateful event processing.
 * 
 * This configuration supports:
 * 1. Consuming position data from the Position Processing Service
 * 2. Publishing events to the Notification Service
 * 3. Serialization/deserialization for Position and Event objects
 * 4. Error handling with dead letter topics
 * 5. Stateful event processing with Kafka Streams
 * 6. Metrics collection for monitoring Kafka operations
 */
@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:event-service}")
    private String consumerGroupId;

    @Value("${spring.kafka.consumer.auto-offset-reset:earliest}")
    private String autoOffsetReset;

    @Value("${spring.kafka.consumer.max-poll-records:500}")
    private int maxPollRecords;

    @Value("${spring.kafka.producer.acks:all}")
    private String producerAcks;

    @Value("${spring.kafka.producer.retries:3}")
    private int producerRetries;

    @Value("${spring.kafka.producer.batch-size:16384}")
    private int producerBatchSize;

    @Value("${spring.kafka.producer.buffer-memory:33554432}")
    private int producerBufferMemory;

    @Value("${spring.kafka.topics.position:enriched-positions}")
    private String positionTopic;

    @Value("${spring.kafka.topics.event:events}")
    private String eventTopic;

    @Value("${spring.kafka.topics.dead-letter:dead-letter}")
    private String deadLetterTopic;
    
    @Value("${spring.kafka.streams.state-store-directory:/tmp/kafka-streams}")
    private String stateStoreDirectory;
    
    @Value("${spring.kafka.streams.commit-interval-ms:1000}")
    private int streamsCommitIntervalMs;

    /**
     * Configures the Kafka consumer factory for position data.
     * Uses error handling deserializer and JSON deserializer for position objects.
     *
     * @return ConsumerFactory for position data
     */
    @Bean
    public ConsumerFactory<String, Object> positionConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        
        // Configure key deserializer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        // Configure value deserializer with error handling
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "org.traccar.model,org.traccar.proto");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Position.class.getName());
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Configures the Kafka listener container factory for position data.
     * Includes error handling with dead letter topic and retry mechanism.
     *
     * @param positionConsumerFactory Consumer factory for position data
     * @param kafkaTemplate Kafka template for publishing to dead letter topic
     * @return ConcurrentKafkaListenerContainerFactory for position data
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> positionConsumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate) {
        
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(positionConsumerFactory);
        factory.setConcurrency(3); // Number of consumer threads
        factory.setBatchListener(true); // Enable batch processing
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setSyncCommits(true);
        factory.getContainerProperties().setPollTimeout(5000); // 5 seconds poll timeout
        
        // Configure error handling with dead letter topic
        DeadLetterPublishingRecoverer recoverer = 
                new DeadLetterPublishingRecoverer(kafkaTemplate, 
                        (record, ex) -> deadLetterTopic);
        
        // Configure retry with exponential backoff
        ExponentialBackOff backOff = new ExponentialBackOff(
                1000L, // Initial interval
                2.0);  // Multiplier
        backOff.setMaxInterval(10000L); // Max interval
        backOff.setMaxElapsedTime(30000L); // Max elapsed time
        
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        factory.setCommonErrorHandler(errorHandler);
        
        return factory;
    }

    /**
     * Configures the Kafka producer factory for event data.
     * Uses JSON serializer for event objects.
     *
     * @return ProducerFactory for event data
     */
    @Bean
    public ProducerFactory<String, Object> eventProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, producerAcks);
        props.put(ProducerConfig.RETRIES_CONFIG, producerRetries);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, producerBatchSize);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, producerBufferMemory);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(JsonSerializer.TYPE_MAPPINGS, "event:org.traccar.model.Event");
        
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * Configures the Kafka template for event data.
     *
     * @param eventProducerFactory Producer factory for event data
     * @return KafkaTemplate for event data
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> eventProducerFactory) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(eventProducerFactory);
        template.setDefaultTopic(eventTopic);
        return template;
    }

    /**
     * Configures Kafka Streams for stateful event processing.
     *
     * @return KafkaStreamsConfiguration for event processing
     */
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonSerde");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDirectory);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, streamsCommitIntervalMs);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2); // Number of stream threads
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024); // 10MB cache
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        
        return new KafkaStreamsConfiguration(props);
    }

    /**
     * Configures Kafka metrics collection.
     *
     * @param meterRegistry Micrometer registry for metrics collection
     * @return KafkaMetricsCollector for Kafka metrics
     */
    @Bean
    public KafkaMetricsCollector kafkaMetricsCollector(MeterRegistry meterRegistry) {
        return new KafkaMetricsCollector(meterRegistry);
    }

    /**
     * Inner class for collecting Kafka metrics.
     */
    public static class KafkaMetricsCollector {
        private final MeterRegistry meterRegistry;

        public KafkaMetricsCollector(MeterRegistry meterRegistry) {
            this.meterRegistry = meterRegistry;
            registerCommonMetrics();
        }
        
        private void registerCommonMetrics() {
            // Register consumer metrics
            meterRegistry.gauge("kafka.consumer.records.lag.max", 0.0);
            meterRegistry.gauge("kafka.consumer.fetch.rate", 0.0);
            meterRegistry.counter("kafka.consumer.records.consumed.total");
            
            // Register producer metrics
            meterRegistry.gauge("kafka.producer.request.rate", 0.0);
            meterRegistry.gauge("kafka.producer.response.rate", 0.0);
            meterRegistry.counter("kafka.producer.record.send.total");
            meterRegistry.counter("kafka.producer.record.error.total");
            
            // Register streams metrics
            meterRegistry.gauge("kafka.streams.process.rate", 0.0);
            meterRegistry.gauge("kafka.streams.commit.rate", 0.0);
            meterRegistry.gauge("kafka.streams.task.count", 0.0);
        }
    }
}