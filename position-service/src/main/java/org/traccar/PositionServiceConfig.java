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
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.propagation.TextMapPropagator;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.traccar.config.Config;
import org.traccar.messaging.KafkaMessageBrokerPublisher;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for the Position Service.
 * 
 * This class configures the message broker integration, service discovery, and other components
 * for the Position Service. It provides beans for Kafka producer and consumer, OpenTelemetry
 * tracer, and other dependencies.
 */
@Configuration
public class PositionServiceConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionServiceConfig.class);

    /**
     * Create a Kafka producer for publishing messages to the broker.
     *
     * @param config Configuration
     * @param bootstrapServers Kafka bootstrap servers
     * @return KafkaProducer instance
     */
    @Bean
    @Singleton
    public KafkaProducer<String, String> kafkaProducer(
            Config config,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        return new KafkaProducer<>(props);
    }

    /**
     * Create a Kafka consumer factory for consuming messages from the broker.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param groupId Consumer group ID
     * @return ConsumerFactory instance
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id}") String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Create a Kafka listener container factory for consuming messages from the broker.
     *
     * @param consumerFactory Consumer factory
     * @return ConcurrentKafkaListenerContainerFactory instance
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(10); // Number of consumer threads
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        return factory;
    }

    /**
     * Create a message broker publisher for publishing messages to the broker.
     *
     * @param config Configuration
     * @param kafkaProducer Kafka producer
     * @param propagator OpenTelemetry context propagator
     * @return MessageBrokerPublisher instance
     */
    @Bean
    @Singleton
    public MessageBrokerPublisher messageBrokerPublisher(
            Config config,
            KafkaProducer<String, String> kafkaProducer,
            TextMapPropagator propagator) {
        return new KafkaMessageBrokerPublisher(config, kafkaProducer, propagator);
    }

    /**
     * Create an OpenTelemetry tracer for distributed tracing.
     *
     * @param openTelemetry OpenTelemetry instance
     * @return Tracer instance
     */
    @Bean
    @Singleton
    public Tracer tracer(OpenTelemetry openTelemetry) {
        return openTelemetry.getTracer("position-service");
    }

    /**
     * Create an OpenTelemetry context propagator for distributed tracing.
     *
     * @param openTelemetry OpenTelemetry instance
     * @return TextMapPropagator instance
     */
    @Bean
    @Singleton
    public TextMapPropagator propagator(OpenTelemetry openTelemetry) {
        return openTelemetry.getPropagators().getTextMapPropagator();
    }

    /**
     * Create a JSON object mapper for serializing and deserializing objects.
     *
     * @return ObjectMapper instance
     */
    @Bean
    @Primary
    @Singleton
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}