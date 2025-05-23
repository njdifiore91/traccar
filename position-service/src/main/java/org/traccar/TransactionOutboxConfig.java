package org.traccar;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for the transaction outbox pattern implementation.
 * <p>
 * This class provides the necessary beans for the transaction outbox pattern,
 * including Kafka configuration for message publishing.
 */
@Configuration
@EnableScheduling
public class TransactionOutboxConfig {

    /**
     * Creates a Kafka producer factory with default settings.
     * <p>
     * In a real-world scenario, these settings would be loaded from configuration properties.
     *
     * @return a configured Kafka producer factory
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka.bootstrap-servers:localhost:9092}");
        configProps.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        configProps.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        // Enable idempotence to prevent duplicate messages
        configProps.put(org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // Set acks to 'all' for maximum durability
        configProps.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all");
        // Configure retries
        configProps.put(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG, 3);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Creates a Kafka template for sending messages to Kafka topics.
     *
     * @param producerFactory the producer factory to use
     * @return a configured Kafka template
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}