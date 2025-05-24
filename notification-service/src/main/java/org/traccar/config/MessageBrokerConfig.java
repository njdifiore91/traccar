/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for message broker integration in the Notification Service.
 * This class configures Kafka producers and consumers for asynchronous communication
 * with other microservices in the Traccar system.
 */
@Configuration
@EnableKafka
@ConditionalOnProperty(name = "messaging.broker.type", havingValue = "kafka", matchIfMissing = true)
public class MessageBrokerConfig {

    @Value("${kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group-id:notification-service}")
    private String groupId;

    @Value("${kafka.consumer.auto-offset-reset:earliest}")
    private String autoOffsetReset;

    @Value("${kafka.consumer.max-poll-records:500}")
    private int maxPollRecords;

    @Value("${kafka.consumer.concurrency:3}")
    private int concurrency;

    /**
     * Configure Kafka consumer factory for consuming events from the message broker.
     */
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "org.traccar.*");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Configure Kafka listener container factory for processing messages.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            KafkaTemplate<String, Object> kafkaTemplate) {
        
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        // Configure error handling with dead letter topic and retry
        ExponentialBackOff backOff = new ExponentialBackOff(
                1000L, // initial interval
                2.0);  // multiplier
        backOff.setMaxInterval(60000L); // 1 minute max
        backOff.setMaxElapsedTime(900000L); // 15 minutes total
        
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(kafkaTemplate), backOff);
        factory.setCommonErrorHandler(errorHandler);
        
        return factory;
    }

    /**
     * Configure Kafka producer factory for sending messages to the broker.
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * Configure Kafka template for sending messages.
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}