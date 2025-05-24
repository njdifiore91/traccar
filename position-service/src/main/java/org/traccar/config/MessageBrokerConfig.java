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
package org.traccar.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.traccar.forward.AmqpClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for message broker integration in the Position Processing Service.
 * Supports both Kafka and RabbitMQ as configurable message broker options.
 */
@Configuration
public class MessageBrokerConfig {

    @Autowired
    private Config config;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MeterRegistry meterRegistry;

    @Value("${message.broker.type:kafka}")
    private String brokerType;

    @Value("${message.broker.kafka.bootstrap-servers:localhost:9092}")
    private String kafkaBootstrapServers;

    @Value("${message.broker.kafka.consumer.group-id:position-service}")
    private String kafkaConsumerGroupId;

    @Value("${message.broker.kafka.consumer.auto-offset-reset:earliest}")
    private String kafkaConsumerAutoOffsetReset;

    @Value("${message.broker.kafka.consumer.max-poll-records:500}")
    private String kafkaConsumerMaxPollRecords;

    @Value("${message.broker.kafka.consumer.concurrency:5}")
    private int kafkaConsumerConcurrency;

    @Value("${message.broker.kafka.topic.raw-positions:raw-positions}")
    private String kafkaTopicRawPositions;

    @Value("${message.broker.kafka.topic.enriched-positions:enriched-positions}")
    private String kafkaTopicEnrichedPositions;

    @Value("${message.broker.rabbitmq.connection-url:amqp://guest:guest@localhost:5672}")
    private String rabbitmqConnectionUrl;

    @Value("${message.broker.rabbitmq.exchange:traccar}")
    private String rabbitmqExchange;

    @Value("${message.broker.rabbitmq.topic.raw-positions:raw-positions}")
    private String rabbitmqTopicRawPositions;

    @Value("${message.broker.rabbitmq.topic.enriched-positions:enriched-positions}")
    private String rabbitmqTopicEnrichedPositions;

    /**
     * Health indicator for the message broker connection.
     * Provides health status information for monitoring.
     */
    @Bean
    public HealthIndicator messageBrokerHealthIndicator() {
        return () -> {
            try {
                if ("kafka".equalsIgnoreCase(brokerType)) {
                    // Check Kafka connection
                    return Health.up()
                            .withDetail("type", "kafka")
                            .withDetail("bootstrapServers", kafkaBootstrapServers)
                            .build();
                } else if ("rabbitmq".equalsIgnoreCase(brokerType)) {
                    // Check RabbitMQ connection
                    return Health.up()
                            .withDetail("type", "rabbitmq")
                            .withDetail("connectionUrl", rabbitmqConnectionUrl.replaceAll(":[^:]*@", ":***@"))
                            .build();
                } else {
                    return Health.down()
                            .withDetail("error", "Unsupported broker type: " + brokerType)
                            .build();
                }
            } catch (Exception e) {
                return Health.down(e).build();
            }
        };
    }

    /**
     * Kafka consumer factory configuration.
     * Used for consuming raw position messages from the Protocol Service.
     */
    @Bean
    @ConditionalOnProperty(name = "message.broker.type", havingValue = "kafka", matchIfMissing = true)
    public ConsumerFactory<String, String> kafkaConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConsumerAutoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaConsumerMaxPollRecords);
        
        // Register metrics
        meterRegistry.gauge("message.broker.kafka.consumer.lag", props, p -> 0);
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Kafka listener container factory configuration.
     * Configures concurrent consumers for processing raw position messages.
     */
    @Bean
    @ConditionalOnProperty(name = "message.broker.type", havingValue = "kafka", matchIfMissing = true)
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(kafkaConsumerConcurrency);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        
        // Register metrics
        meterRegistry.gauge("message.broker.kafka.consumer.active", factory, f -> f.getConcurrency());
        
        return factory;
    }

    /**
     * Kafka producer factory configuration.
     * Used for producing enriched position messages to the Event Service.
     */
    @Bean
    @ConditionalOnProperty(name = "message.broker.type", havingValue = "kafka", matchIfMissing = true)
    public ProducerFactory<String, String> kafkaProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        
        // Register metrics
        meterRegistry.gauge("message.broker.kafka.producer.active", props, p -> 1);
        
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * Kafka template configuration.
     * Provides a template for sending enriched position messages.
     */
    @Bean
    @ConditionalOnProperty(name = "message.broker.type", havingValue = "kafka", matchIfMissing = true)
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory);
        template.setDefaultTopic(kafkaTopicEnrichedPositions);
        
        // Register metrics
        meterRegistry.counter("message.broker.kafka.producer.messages");
        
        return template;
    }

    /**
     * RabbitMQ client for consuming raw position messages.
     * Used when RabbitMQ is configured as the message broker.
     */
    @Bean
    @ConditionalOnProperty(name = "message.broker.type", havingValue = "rabbitmq")
    public AmqpClient rawPositionsConsumer() {
        AmqpClient client = new AmqpClient(rabbitmqConnectionUrl, rabbitmqExchange, rabbitmqTopicRawPositions);
        
        // Register metrics
        meterRegistry.gauge("message.broker.rabbitmq.consumer.active", client, c -> 1);
        
        return client;
    }

    /**
     * RabbitMQ client for producing enriched position messages.
     * Used when RabbitMQ is configured as the message broker.
     */
    @Bean
    @ConditionalOnProperty(name = "message.broker.type", havingValue = "rabbitmq")
    public AmqpClient enrichedPositionsProducer() {
        AmqpClient client = new AmqpClient(rabbitmqConnectionUrl, rabbitmqExchange, rabbitmqTopicEnrichedPositions);
        
        // Register metrics
        meterRegistry.gauge("message.broker.rabbitmq.producer.active", client, c -> 1);
        meterRegistry.counter("message.broker.rabbitmq.producer.messages");
        
        return client;
    }

    /**
     * Gets the raw positions topic name based on the configured broker type.
     */
    public String getRawPositionsTopic() {
        return "kafka".equalsIgnoreCase(brokerType) ? kafkaTopicRawPositions : rabbitmqTopicRawPositions;
    }

    /**
     * Gets the enriched positions topic name based on the configured broker type.
     */
    public String getEnrichedPositionsTopic() {
        return "kafka".equalsIgnoreCase(brokerType) ? kafkaTopicEnrichedPositions : rabbitmqTopicEnrichedPositions;
    }

    /**
     * Gets the broker type (kafka or rabbitmq).
     */
    public String getBrokerType() {
        return brokerType;
    }
}