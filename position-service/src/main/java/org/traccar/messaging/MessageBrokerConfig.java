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

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
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
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for message broker integration in the Position Processing Service.
 * Supports both Kafka and RabbitMQ as message broker options through conditional configuration.
 * Configures consumer properties for raw position messages and producer properties for enriched position messages.
 * Sets up error handling and retry mechanisms for message processing.
 */
@Configuration
public class MessageBrokerConfig {

    // Common configuration properties
    @Value("${messaging.broker.type:kafka}")
    private String brokerType;

    // Kafka configuration properties
    @Value("${messaging.kafka.bootstrap-servers:localhost:9092}")
    private String kafkaBootstrapServers;

    @Value("${messaging.kafka.consumer.group-id:position-service}")
    private String kafkaConsumerGroupId;

    @Value("${messaging.kafka.consumer.auto-offset-reset:earliest}")
    private String kafkaAutoOffsetReset;

    @Value("${messaging.kafka.consumer.max-poll-records:500}")
    private int kafkaMaxPollRecords;

    @Value("${messaging.kafka.consumer.concurrency:3}")
    private int kafkaConcurrency;

    @Value("${messaging.kafka.topic.raw-positions:raw-positions}")
    private String kafkaRawPositionsTopic;

    @Value("${messaging.kafka.topic.enriched-positions:enriched-positions}")
    private String kafkaEnrichedPositionsTopic;

    // RabbitMQ configuration properties
    @Value("${messaging.rabbitmq.host:localhost}")
    private String rabbitmqHost;

    @Value("${messaging.rabbitmq.port:5672}")
    private int rabbitmqPort;

    @Value("${messaging.rabbitmq.username:guest}")
    private String rabbitmqUsername;

    @Value("${messaging.rabbitmq.password:guest}")
    private String rabbitmqPassword;

    @Value("${messaging.rabbitmq.exchange:traccar-exchange}")
    private String rabbitmqExchange;

    @Value("${messaging.rabbitmq.queue.raw-positions:raw-positions}")
    private String rabbitmqRawPositionsQueue;

    @Value("${messaging.rabbitmq.queue.enriched-positions:enriched-positions}")
    private String rabbitmqEnrichedPositionsQueue;

    @Value("${messaging.rabbitmq.routing-key.raw-positions:raw-positions}")
    private String rabbitmqRawPositionsRoutingKey;

    @Value("${messaging.rabbitmq.routing-key.enriched-positions:enriched-positions}")
    private String rabbitmqEnrichedPositionsRoutingKey;

    @Value("${messaging.rabbitmq.listener.concurrency:3}")
    private int rabbitmqConcurrency;

    @Value("${messaging.rabbitmq.listener.max-concurrency:10}")
    private int rabbitmqMaxConcurrency;

    // Error handling and retry configuration
    @Value("${messaging.retry.max-attempts:3}")
    private int retryMaxAttempts;

    @Value("${messaging.retry.initial-interval:1000}")
    private long retryInitialInterval;

    @Value("${messaging.retry.multiplier:2.0}")
    private double retryMultiplier;

    @Value("${messaging.retry.max-interval:10000}")
    private long retryMaxInterval;

    /**
     * Common message converter for JSON serialization/deserialization.
     * Used by both Kafka and RabbitMQ configurations.
     */
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    /**
     * Kafka configuration section.
     * Only activated when messaging.broker.type=kafka
     */
    @Configuration
    @EnableKafka
    @ConditionalOnProperty(name = "messaging.broker.type", havingValue = "kafka", matchIfMissing = true)
    public class KafkaConfig {

        /**
         * Configures Kafka consumer properties.
         */
        @Bean
        public Map<String, Object> consumerConfigs() {
            Map<String, Object> props = new HashMap<>();
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerGroupId);
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaAutoOffsetReset);
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaMaxPollRecords);
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                    org.apache.kafka.common.serialization.StringDeserializer.class);
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                    org.apache.kafka.common.serialization.StringDeserializer.class);
            return props;
        }

        /**
         * Creates a Kafka consumer factory with the configured properties.
         */
        @Bean
        public ConsumerFactory<String, String> consumerFactory() {
            return new DefaultKafkaConsumerFactory<>(consumerConfigs());
        }

        /**
         * Configures the Kafka listener container factory with error handling and concurrency settings.
         */
        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory());
            factory.setConcurrency(kafkaConcurrency);
            factory.setMessageConverter(new JsonMessageConverter());
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
            
            // Configure error handling with retry
            DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                    new FixedBackOff(retryInitialInterval, retryMaxAttempts));
            factory.setCommonErrorHandler(errorHandler);
            
            return factory;
        }

        /**
         * Configures Kafka producer properties.
         */
        @Bean
        public Map<String, Object> producerConfigs() {
            Map<String, Object> props = new HashMap<>();
            props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
            props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                    org.apache.kafka.common.serialization.StringSerializer.class);
            props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                    org.apache.kafka.common.serialization.StringSerializer.class);
            // Enable idempotent producer for exactly-once semantics
            props.put(org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            props.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all");
            props.put(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG, Integer.toString(retryMaxAttempts));
            return props;
        }

        /**
         * Creates a Kafka producer factory with the configured properties.
         */
        @Bean
        public ProducerFactory<String, String> producerFactory() {
            return new DefaultKafkaProducerFactory<>(producerConfigs());
        }

        /**
         * Creates a KafkaTemplate for sending messages to Kafka topics.
         */
        @Bean
        public KafkaTemplate<String, String> kafkaTemplate() {
            KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory());
            template.setMessageConverter(new JsonMessageConverter());
            return template;
        }
    }

    /**
     * RabbitMQ configuration section.
     * Only activated when messaging.broker.type=rabbitmq
     */
    @Configuration
    @ConditionalOnProperty(name = "messaging.broker.type", havingValue = "rabbitmq")
    public class RabbitMQConfig {

        /**
         * Configures the RabbitMQ exchange.
         */
        @Bean
        public TopicExchange exchange() {
            return new TopicExchange(rabbitmqExchange);
        }

        /**
         * Configures the RabbitMQ queue for raw positions.
         */
        @Bean
        public Queue rawPositionsQueue() {
            return new Queue(rabbitmqRawPositionsQueue, true);
        }

        /**
         * Configures the RabbitMQ queue for enriched positions.
         */
        @Bean
        public Queue enrichedPositionsQueue() {
            return new Queue(rabbitmqEnrichedPositionsQueue, true);
        }

        /**
         * Binds the raw positions queue to the exchange with the appropriate routing key.
         */
        @Bean
        public Binding rawPositionsBinding(Queue rawPositionsQueue, TopicExchange exchange) {
            return BindingBuilder.bind(rawPositionsQueue).to(exchange).with(rabbitmqRawPositionsRoutingKey);
        }

        /**
         * Binds the enriched positions queue to the exchange with the appropriate routing key.
         */
        @Bean
        public Binding enrichedPositionsBinding(Queue enrichedPositionsQueue, TopicExchange exchange) {
            return BindingBuilder.bind(enrichedPositionsQueue).to(exchange).with(rabbitmqEnrichedPositionsRoutingKey);
        }

        /**
         * Configures the RabbitMQ listener container factory with error handling and concurrency settings.
         */
        @Bean
        public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
            SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
            factory.setConnectionFactory(connectionFactory);
            factory.setConcurrentConsumers(rabbitmqConcurrency);
            factory.setMaxConcurrentConsumers(rabbitmqMaxConcurrency);
            factory.setMessageConverter(jsonMessageConverter());
            factory.setDefaultRequeueRejected(false); // Don't requeue failed messages automatically
            factory.setAcknowledgeMode(org.springframework.amqp.core.AcknowledgeMode.MANUAL);
            
            // Configure retry and error handling
            factory.setAdviceChain(org.springframework.amqp.rabbit.config.RetryInterceptorBuilder
                    .stateless()
                    .maxAttempts(retryMaxAttempts)
                    .backOffOptions(retryInitialInterval, retryMultiplier, retryMaxInterval)
                    .build());
            
            return factory;
        }

        /**
         * Configures the RabbitTemplate for sending messages to RabbitMQ queues.
         */
        @Bean
        public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
            RabbitTemplate template = new RabbitTemplate(connectionFactory);
            template.setMessageConverter(jsonMessageConverter());
            template.setConfirmCallback((correlationData, ack, cause) -> {
                if (!ack) {
                    // Log failed message publishing
                    System.err.println("Failed to publish message: " + cause);
                }
            });
            return template;
        }
    }
}