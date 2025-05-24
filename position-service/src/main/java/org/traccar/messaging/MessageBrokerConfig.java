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
import org.traccar.config.Config;

import java.util.HashMap;
import java.util.Map;

/**
 * Configures the message broker (Kafka/RabbitMQ) for the Position Processing Service,
 * providing connection settings, serialization/deserialization configuration, and
 * topic/consumer group definitions.
 */
public class MessageBrokerConfig {

    private final Config config;
    private final String brokerType;
    private final String rawPositionTopic;
    private final String enrichedPositionTopic;
    private final String consumerGroupId;
    private final Map<String, Object> kafkaProducerProperties;
    private final Map<String, Object> kafkaConsumerProperties;

    /**
     * Constructs a new MessageBrokerConfig with the specified configuration.
     *
     * @param config The application configuration
     */
    @Inject
    public MessageBrokerConfig(Config config) {
        this.config = config;
        this.brokerType = config.getString("message.broker.type", "kafka");
        this.rawPositionTopic = config.getString("message.topic.raw.position", "raw-positions");
        this.enrichedPositionTopic = config.getString("message.topic.enriched.position", "enriched-positions");
        this.consumerGroupId = config.getString("message.consumer.group.id", "position-service");
        
        // Initialize Kafka producer properties
        this.kafkaProducerProperties = new HashMap<>();
        config.getKeys().forEach(key -> {
            if (key.startsWith("kafka.producer.")) {
                String propertyKey = key.substring("kafka.producer.".length());
                kafkaProducerProperties.put(propertyKey, config.getString(key));
            }
        });
        
        // Initialize Kafka consumer properties
        this.kafkaConsumerProperties = new HashMap<>();
        config.getKeys().forEach(key -> {
            if (key.startsWith("kafka.consumer.")) {
                String propertyKey = key.substring("kafka.consumer.".length());
                kafkaConsumerProperties.put(propertyKey, config.getString(key));
            }
        });
    }

    /**
     * Gets the type of message broker to use (kafka or rabbitmq).
     *
     * @return The broker type
     */
    public String getBrokerType() {
        return brokerType;
    }

    /**
     * Gets the Kafka bootstrap servers configuration.
     *
     * @return The Kafka bootstrap servers
     */
    public String getKafkaBootstrapServers() {
        return config.getString("kafka.bootstrap.servers", "localhost:9092");
    }

    /**
     * Gets the RabbitMQ host configuration.
     *
     * @return The RabbitMQ host
     */
    public String getRabbitMqHost() {
        return config.getString("rabbitmq.host", "localhost");
    }

    /**
     * Gets the RabbitMQ port configuration.
     *
     * @return The RabbitMQ port
     */
    public int getRabbitMqPort() {
        return config.getInteger("rabbitmq.port", 5672);
    }

    /**
     * Gets the RabbitMQ username configuration.
     *
     * @return The RabbitMQ username
     */
    public String getRabbitMqUsername() {
        return config.getString("rabbitmq.username", "guest");
    }

    /**
     * Gets the RabbitMQ password configuration.
     *
     * @return The RabbitMQ password
     */
    public String getRabbitMqPassword() {
        return config.getString("rabbitmq.password", "guest");
    }

    /**
     * Gets the RabbitMQ virtual host configuration.
     *
     * @return The RabbitMQ virtual host
     */
    public String getRabbitMqVirtualHost() {
        return config.getString("rabbitmq.virtualHost", "/");
    }

    /**
     * Gets the RabbitMQ exchange configuration.
     *
     * @return The RabbitMQ exchange
     */
    public String getRabbitMqExchange() {
        return config.getString("rabbitmq.exchange", "traccar");
    }

    /**
     * Gets the topic for raw position messages.
     *
     * @return The raw position topic
     */
    public String getRawPositionTopic() {
        return rawPositionTopic;
    }

    /**
     * Gets the topic for enriched position messages.
     *
     * @return The enriched position topic
     */
    public String getEnrichedPositionTopic() {
        return enrichedPositionTopic;
    }

    /**
     * Gets the topic for position messages (alias for getEnrichedPositionTopic).
     *
     * @return The position topic
     */
    public String getPositionTopic() {
        return getEnrichedPositionTopic();
    }

    /**
     * Gets the consumer group ID for the position service.
     *
     * @return The consumer group ID
     */
    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    /**
     * Gets the additional Kafka producer properties.
     *
     * @return The Kafka producer properties
     */
    public Map<String, Object> getKafkaProducerProperties() {
        return kafkaProducerProperties;
    }

    /**
     * Gets the additional Kafka consumer properties.
     *
     * @return The Kafka consumer properties
     */
    public Map<String, Object> getKafkaConsumerProperties() {
        return kafkaConsumerProperties;
    }

    /**
     * Gets the dead letter topic for raw position messages.
     *
     * @return The raw position dead letter topic
     */
    public String getRawPositionDeadLetterTopic() {
        return rawPositionTopic + ".dlq";
    }

    /**
     * Gets the dead letter topic for enriched position messages.
     *
     * @return The enriched position dead letter topic
     */
    public String getEnrichedPositionDeadLetterTopic() {
        return enrichedPositionTopic + ".dlq";
    }

    /**
     * Gets the maximum number of retry attempts for message processing.
     *
     * @return The maximum retry attempts
     */
    public int getMaxRetryAttempts() {
        return config.getInteger("message.retry.max.attempts", 3);
    }

    /**
     * Gets the initial retry delay in milliseconds.
     *
     * @return The initial retry delay
     */
    public long getInitialRetryDelayMs() {
        return config.getLong("message.retry.initial.delay.ms", 1000);
    }

    /**
     * Gets the maximum retry delay in milliseconds.
     *
     * @return The maximum retry delay
     */
    public long getMaxRetryDelayMs() {
        return config.getLong("message.retry.max.delay.ms", 10000);
    }

    /**
     * Gets the retry backoff multiplier.
     *
     * @return The retry backoff multiplier
     */
    public double getRetryBackoffMultiplier() {
        return config.getDouble("message.retry.backoff.multiplier", 2.0);
    }
}