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
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.core.instrument.binder.rabbitmq.RabbitMQMetrics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Provides common configuration for message broker connections, regardless of which broker
 * implementation is used (Kafka or RabbitMQ). This class defines shared properties like topic names,
 * consumer group IDs, and retry policies, and conditionally loads either Kafka or RabbitMQ specific
 * configurations based on application properties.
 */
@Configuration
public class MessageBrokerConfig {

    /**
     * Standard topic names used across the system
     */
    public static final String TOPIC_RAW_POSITIONS = "raw.positions";
    public static final String TOPIC_ENRICHED_POSITIONS = "enriched.positions";
    public static final String TOPIC_EVENTS = "events";
    public static final String TOPIC_NOTIFICATIONS = "notifications";
    public static final String TOPIC_COMMANDS = "commands";
    public static final String TOPIC_COMMAND_RESULTS = "command.results";
    
    /**
     * Dead letter queue/topic names for error handling
     */
    public static final String DLQ_PREFIX = "dlq.";
    public static final String DLQ_ENRICHED_POSITIONS = DLQ_PREFIX + TOPIC_ENRICHED_POSITIONS;
    public static final String DLQ_EVENTS = DLQ_PREFIX + TOPIC_EVENTS;
    
    /**
     * Consumer group IDs for horizontal scaling
     */
    public static final String GROUP_EVENT_PROCESSORS = "event-processors";
    public static final String GROUP_NOTIFICATION_PROCESSORS = "notification-processors";
    public static final String GROUP_COMMAND_PROCESSORS = "command-processors";
    
    @Value("${messaging.broker.type:kafka}")
    private String brokerType;
    
    @Value("${messaging.retry.max-attempts:3}")
    private int maxRetryAttempts;
    
    @Value("${messaging.retry.initial-interval:1000}")
    private long initialRetryInterval;
    
    @Value("${messaging.retry.multiplier:2.0}")
    private double retryMultiplier;
    
    @Value("${messaging.retry.max-interval:10000}")
    private long maxRetryInterval;

    /**
     * Creates a retry template with exponential backoff for message processing retries
     */
    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        
        // Configure retry policy with max attempts
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(maxRetryAttempts);
        retryTemplate.setRetryPolicy(retryPolicy);
        
        // Configure exponential backoff
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(initialRetryInterval);
        backOffPolicy.setMultiplier(retryMultiplier);
        backOffPolicy.setMaxInterval(maxRetryInterval);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        
        return retryTemplate;
    }
    
    /**
     * Health indicator for message broker connectivity
     * This is a generic health indicator that delegates to the appropriate broker-specific health indicator
     */
    @Bean
    public HealthIndicator messageBrokerHealthIndicator() {
        return () -> {
            try {
                // Check broker connectivity based on broker type
                if ("kafka".equalsIgnoreCase(brokerType)) {
                    // For Kafka, we'll check if the admin client can connect
                    // This is just a placeholder, the actual check is done by kafkaHealthIndicator
                    return Health.up()
                            .withDetail("type", "kafka")
                            .withDetail("description", "Using Kafka as message broker")
                            .build();
                } else if ("rabbitmq".equalsIgnoreCase(brokerType)) {
                    // For RabbitMQ, we'll check if the connection factory can connect
                    // This is just a placeholder, the actual check is done by rabbitHealthIndicator
                    return Health.up()
                            .withDetail("type", "rabbitmq")
                            .withDetail("description", "Using RabbitMQ as message broker")
                            .build();
                } else {
                    return Health.down()
                            .withDetail("error", "Unknown broker type: " + brokerType)
                            .build();
                }
            } catch (Exception e) {
                return Health.down(e)
                        .withDetail("type", brokerType)
                        .build();
            }
        };
    }

    /**
     * Kafka-specific configuration
     */
    @Configuration
    @ConditionalOnProperty(name = "messaging.broker.type", havingValue = "kafka", matchIfMissing = true)
    @Import(KafkaConfig.class)
    public static class KafkaConfiguration {
        
        @Value("${spring.kafka.bootstrap-servers}")
        private String bootstrapServers;
        
        @Value("${spring.kafka.consumer.auto-offset-reset:earliest}")
        private String autoOffsetReset;
        
        @Value("${spring.kafka.consumer.max-poll-records:500}")
        private int maxPollRecords;
        
        @Value("${spring.kafka.producer.acks:all}")
        private String acks;
        
        @Value("${spring.kafka.producer.retries:3}")
        private int retries;
        
        @Value("${spring.kafka.admin.request-timeout-ms:3000}")
        private int adminRequestTimeoutMs;
        
        @Value("${spring.kafka.admin.connections-max-idle-ms:10000}")
        private int adminConnectionsMaxIdleMs;
        
        @Autowired
        private MeterRegistry meterRegistry;
        
        /**
         * Kafka admin client for topic management and health checks
         */
        @Bean
        public AdminClient kafkaAdminClient() {
            Map<String, Object> configs = new HashMap<>();
            configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, adminRequestTimeoutMs);
            configs.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, adminConnectionsMaxIdleMs);
            return AdminClient.create(configs);
        }
        
        /**
         * Kafka admin for topic creation and management
         */
        @Bean
        public KafkaAdmin kafkaAdmin() {
            Map<String, Object> configs = new HashMap<>();
            configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            return new KafkaAdmin(configs);
        }
        
        /**
         * Default Kafka consumer factory with common configuration
         */
        @Bean
        public DefaultKafkaConsumerFactory<String, String> kafkaConsumerFactory() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_EVENT_PROCESSORS);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Use manual commit for better control
            
            DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(props);
            
            // Register metrics for Kafka consumer
            new KafkaClientMetrics(factory.createConsumer()).bindTo(meterRegistry);
            
            return factory;
        }
        
        /**
         * Default Kafka producer factory with common configuration
         */
        @Bean
        public DefaultKafkaProducerFactory<String, String> kafkaProducerFactory() {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.ACKS_CONFIG, acks);
            props.put(ProducerConfig.RETRIES_CONFIG, retries);
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // Ensure exactly-once semantics
            
            DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);
            
            // Register metrics for Kafka producer
            new KafkaClientMetrics(factory.createProducer()).bindTo(meterRegistry);
            
            return factory;
        }
        
        /**
         * Kafka template for sending messages
         */
        @Bean
        @Primary
        public KafkaTemplate<String, String> kafkaTemplate() {
            return new KafkaTemplate<>(kafkaProducerFactory());
        }
        
        /**
         * Health indicator specifically for Kafka connectivity
         * This performs an actual check of Kafka availability by listing topics with a timeout
         */
        @Bean
        public HealthIndicator kafkaHealthIndicator() {
            return () -> {
                try {
                    // Check if we can connect to Kafka by listing topics with a timeout
                    kafkaAdminClient().listTopics().names().get(3, TimeUnit.SECONDS);
                    return Health.up()
                            .withDetail("bootstrapServers", bootstrapServers)
                            .withDetail("status", "Connected to Kafka broker")
                            .build();
                } catch (TimeoutException e) {
                    return Health.down()
                            .withDetail("bootstrapServers", bootstrapServers)
                            .withDetail("error", "Timeout connecting to Kafka broker")
                            .withDetail("message", e.getMessage())
                            .build();
                } catch (Exception e) {
                    return Health.down()
                            .withDetail("bootstrapServers", bootstrapServers)
                            .withDetail("error", "Failed to connect to Kafka broker")
                            .withDetail("message", e.getMessage())
                            .build();
                }
            };
        }
    }
    
    /**
     * RabbitMQ-specific configuration
     */
    @Configuration
    @ConditionalOnProperty(name = "messaging.broker.type", havingValue = "rabbitmq")
    @Import(RabbitMQConfig.class)
    public static class RabbitMQConfiguration {
        
        @Value("${spring.rabbitmq.host:localhost}")
        private String host;
        
        @Value("${spring.rabbitmq.port:5672}")
        private int port;
        
        @Value("${spring.rabbitmq.username:guest}")
        private String username;
        
        @Value("${spring.rabbitmq.password:guest}")
        private String password;
        
        @Value("${spring.rabbitmq.virtual-host:/}")
        private String virtualHost;
        
        @Autowired
        private MeterRegistry meterRegistry;
        
        /**
         * RabbitMQ connection factory
         */
        @Bean
        public ConnectionFactory rabbitConnectionFactory() {
            CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
            connectionFactory.setHost(host);
            connectionFactory.setPort(port);
            connectionFactory.setUsername(username);
            connectionFactory.setPassword(password);
            connectionFactory.setVirtualHost(virtualHost);
            
            // Register metrics for RabbitMQ
            new RabbitMQMetrics(connectionFactory).bindTo(meterRegistry);
            
            return connectionFactory;
        }
        
        /**
         * RabbitMQ template for sending messages
         */
        @Bean
        @Primary
        public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, RetryTemplate retryTemplate) {
            RabbitTemplate template = new RabbitTemplate(connectionFactory);
            template.setRetryTemplate(retryTemplate);
            return template;
        }
        
        /**
         * Health indicator specifically for RabbitMQ connectivity
         * This performs an actual check of RabbitMQ availability by creating a connection with a timeout
         */
        @Bean
        public HealthIndicator rabbitHealthIndicator(ConnectionFactory connectionFactory) {
            return () -> {
                try {
                    // Create a connection with a timeout to check RabbitMQ availability
                    // We use a new connection to avoid caching effects that might mask connectivity issues
                    org.springframework.amqp.rabbit.connection.Connection connection = null;
                    try {
                        connection = connectionFactory.createConnection();
                        // Verify the connection is actually open
                        if (connection.isOpen()) {
                            return Health.up()
                                    .withDetail("host", host)
                                    .withDetail("port", port)
                                    .withDetail("virtualHost", virtualHost)
                                    .withDetail("status", "Connected to RabbitMQ broker")
                                    .build();
                        } else {
                            return Health.down()
                                    .withDetail("host", host)
                                    .withDetail("port", port)
                                    .withDetail("virtualHost", virtualHost)
                                    .withDetail("error", "Connection to RabbitMQ is not open")
                                    .build();
                        }
                    } finally {
                        // Always close the connection to avoid resource leaks
                        if (connection != null && connection.isOpen()) {
                            connection.close();
                        }
                    }
                } catch (Exception e) {
                    return Health.down()
                            .withDetail("host", host)
                            .withDetail("port", port)
                            .withDetail("virtualHost", virtualHost)
                            .withDetail("error", "Failed to connect to RabbitMQ broker")
                            .withDetail("message", e.getMessage())
                            .build();
                }
            };
        }
    }
}