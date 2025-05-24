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
package org.traccar.health;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.KafkaPositionConsumer;
import org.traccar.messaging.KafkaPositionProducer;
import org.traccar.messaging.MessageBrokerConfig;
import org.traccar.messaging.MessageConstants;
import org.traccar.messaging.RabbitMqPositionConsumer;
import org.traccar.messaging.RabbitMqPositionProducer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Health indicator for message broker connectivity and performance.
 * Monitors the health of the message broker (Kafka/RabbitMQ) used by the Position Processing Service.
 */
@Component
public class MessageBrokerHealthIndicator implements HealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBrokerHealthIndicator.class);

    private static final String BROKER_TYPE_KEY = "brokerType";
    private static final String BROKER_TYPE_KAFKA = "kafka";
    private static final String BROKER_TYPE_RABBITMQ = "rabbitmq";
    private static final String CONNECTION_STATUS_KEY = "connectionStatus";
    private static final String PRODUCER_STATUS_KEY = "producerStatus";
    private static final String CONSUMER_STATUS_KEY = "consumerStatus";
    private static final String TOPICS_KEY = "topics";
    private static final String CONSUMER_GROUPS_KEY = "consumerGroups";
    private static final String MESSAGE_LAG_KEY = "messageLag";
    
    private static final int TIMEOUT_SECONDS = 5;

    @Autowired
    private Config config;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Autowired(required = false)
    private KafkaPositionProducer kafkaPositionProducer;
    
    @Autowired(required = false)
    private KafkaPositionConsumer kafkaPositionConsumer;
    
    @Autowired(required = false)
    private RabbitMqPositionProducer rabbitMqPositionProducer;
    
    @Autowired(required = false)
    private RabbitMqPositionConsumer rabbitMqPositionConsumer;
    
    @Autowired(required = false)
    private MessageBrokerConfig messageBrokerConfig;

    @Override
    public Health health() {
        String brokerType = config.getString(Keys.POSITION_FORWARD_TYPE);
        if (brokerType == null) {
            brokerType = "kafka"; // Default to Kafka if not specified
        }
        
        Health.Builder builder = new Health.Builder();
        builder.withDetail(BROKER_TYPE_KEY, brokerType);
        
        try {
            if (BROKER_TYPE_KAFKA.equalsIgnoreCase(brokerType)) {
                checkKafkaHealth(builder);
            } else if (BROKER_TYPE_RABBITMQ.equalsIgnoreCase(brokerType)) {
                checkRabbitMqHealth(builder);
            } else {
                builder.down().withDetail("error", "Unsupported broker type: " + brokerType);
            }
        } catch (Exception e) {
            LOGGER.error("Error checking message broker health", e);
            builder.down().withDetail("error", e.getMessage());
        }
        
        return builder.build();
    }
    
    private void checkKafkaHealth(Health.Builder builder) {
        if (kafkaPositionProducer == null || kafkaPositionConsumer == null) {
            builder.down().withDetail("error", "Kafka producer or consumer not configured");
            return;
        }
        
        try {
            // Check Kafka broker connectivity using AdminClient
            Properties properties = new Properties();
            properties.put("bootstrap.servers", config.getString(Keys.FORWARD_URL));
            properties.put("request.timeout.ms", "5000");
            
            try (AdminClient adminClient = AdminClient.create(properties)) {
                // Check if broker is available by listing topics
                ListTopicsResult topicsResult = adminClient.listTopics();
                KafkaFuture<Collection<TopicListing>> topicsFuture = topicsResult.listings();
                Collection<TopicListing> topics = topicsFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                
                // Check consumer groups
                ListConsumerGroupsResult groupsResult = adminClient.listConsumerGroups();
                KafkaFuture<Collection<ConsumerGroupListing>> groupsFuture = groupsResult.valid();
                Collection<ConsumerGroupListing> groups = groupsFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                
                // Add details to health check
                Map<String, String> topicsMap = new HashMap<>();
                for (TopicListing topic : topics) {
                    topicsMap.put(topic.name(), "available");
                }
                
                Map<String, String> groupsMap = new HashMap<>();
                for (ConsumerGroupListing group : groups) {
                    groupsMap.put(group.groupId(), "active");
                }
                
                builder.up()
                    .withDetail(CONNECTION_STATUS_KEY, "connected")
                    .withDetail(PRODUCER_STATUS_KEY, "available")
                    .withDetail(CONSUMER_STATUS_KEY, "available")
                    .withDetail(TOPICS_KEY, topicsMap)
                    .withDetail(CONSUMER_GROUPS_KEY, groupsMap);
                
                // Check for specific position topics
                if (!topicsMap.containsKey(MessageConstants.RAW_POSITION_TOPIC) || 
                    !topicsMap.containsKey(MessageConstants.PROCESSED_POSITION_TOPIC)) {
                    builder.down().withDetail("error", "Required position topics not found");
                }
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOGGER.error("Error connecting to Kafka", e);
            builder.down().withDetail("error", "Failed to connect to Kafka: " + e.getMessage());
        }
    }
    
    private void checkRabbitMqHealth(Health.Builder builder) {
        if (rabbitMqPositionProducer == null || rabbitMqPositionConsumer == null) {
            builder.down().withDetail("error", "RabbitMQ producer or consumer not configured");
            return;
        }
        
        try {
            // For RabbitMQ, we'll check if the connection is open
            boolean producerConnected = rabbitMqPositionProducer != null && rabbitMqPositionProducer.isConnected();
            boolean consumerConnected = rabbitMqPositionConsumer != null && rabbitMqPositionConsumer.isConnected();
            
            if (producerConnected && consumerConnected) {
                builder.up()
                    .withDetail(CONNECTION_STATUS_KEY, "connected")
                    .withDetail(PRODUCER_STATUS_KEY, producerConnected ? "available" : "unavailable")
                    .withDetail(CONSUMER_STATUS_KEY, consumerConnected ? "available" : "unavailable");
            } else {
                builder.down()
                    .withDetail(CONNECTION_STATUS_KEY, "disconnected")
                    .withDetail(PRODUCER_STATUS_KEY, producerConnected ? "available" : "unavailable")
                    .withDetail(CONSUMER_STATUS_KEY, consumerConnected ? "available" : "unavailable");
            }
        } catch (Exception e) {
            LOGGER.error("Error checking RabbitMQ health", e);
            builder.down().withDetail("error", "Failed to check RabbitMQ health: " + e.getMessage());
        }
    }
}