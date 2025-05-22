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

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for message broker settings used for asynchronous communication
 * between microservices. Supports both Kafka and RabbitMQ message brokers.
 */
@Singleton
public class MessageBrokerConfig {

    private final Config config;

    /**
     * Construct message broker configuration.
     *
     * @param config Base configuration
     */
    @Inject
    public MessageBrokerConfig(Config config) {
        this.config = config;
    }

    /**
     * Get the type of message broker to use.
     * Supported values: "kafka", "rabbitmq"
     *
     * @return Message broker type
     */
    public String getBrokerType() {
        return config.getString(Keys.MESSAGE_BROKER_TYPE);
    }

    /**
     * Check if Kafka is enabled as the message broker.
     *
     * @return True if Kafka is enabled
     */
    public boolean isKafkaEnabled() {
        return "kafka".equalsIgnoreCase(getBrokerType());
    }

    /**
     * Check if RabbitMQ is enabled as the message broker.
     *
     * @return True if RabbitMQ is enabled
     */
    public boolean isRabbitMQEnabled() {
        return "rabbitmq".equalsIgnoreCase(getBrokerType());
    }

    /**
     * Get the connection string for the message broker.
     * For Kafka, this is the bootstrap servers list (e.g., "localhost:9092,localhost:9093").
     * For RabbitMQ, this is the connection URI (e.g., "amqp://guest:guest@localhost:5672").
     *
     * @return Connection string
     */
    public String getConnectionString() {
        return config.getString(Keys.MESSAGE_BROKER_CONNECTION);
    }

    /**
     * Get the client ID or application name used to identify this application
     * to the message broker.
     *
     * @return Client ID or application name
     */
    public String getClientId() {
        return config.getString(Keys.MESSAGE_BROKER_CLIENT_ID);
    }

    /**
     * Get the serialization format for messages.
     * Supported values: "json", "protobuf", "avro"
     *
     * @return Serialization format
     */
    public String getSerializationFormat() {
        return config.getString(Keys.MESSAGE_BROKER_SERIALIZATION_FORMAT, "json");
    }

    /**
     * Get the schema registry URL for Avro or Protobuf serialization.
     *
     * @return Schema registry URL or null if not configured
     */
    public String getSchemaRegistryUrl() {
        return config.getString(Keys.MESSAGE_BROKER_SCHEMA_REGISTRY_URL);
    }

    /**
     * Get the topic or exchange name for position data.
     *
     * @return Position topic or exchange name
     */
    public String getPositionTopic() {
        return config.getString(Keys.MESSAGE_BROKER_POSITION_TOPIC, "positions");
    }

    /**
     * Get the topic or exchange name for event data.
     *
     * @return Event topic or exchange name
     */
    public String getEventTopic() {
        return config.getString(Keys.MESSAGE_BROKER_EVENT_TOPIC, "events");
    }

    /**
     * Get the topic or exchange name for command data.
     *
     * @return Command topic or exchange name
     */
    public String getCommandTopic() {
        return config.getString(Keys.MESSAGE_BROKER_COMMAND_TOPIC, "commands");
    }

    /**
     * Get the topic or exchange name for notification data.
     *
     * @return Notification topic or exchange name
     */
    public String getNotificationTopic() {
        return config.getString(Keys.MESSAGE_BROKER_NOTIFICATION_TOPIC, "notifications");
    }

    /**
     * Get the consumer group ID for Kafka or RabbitMQ consumer.
     *
     * @return Consumer group ID
     */
    public String getConsumerGroupId() {
        return config.getString(Keys.MESSAGE_BROKER_CONSUMER_GROUP_ID);
    }

    /**
     * Get the number of partitions for Kafka topics.
     * Not applicable for RabbitMQ.
     *
     * @return Number of partitions
     */
    public int getPartitionCount() {
        return config.getInteger(Keys.MESSAGE_BROKER_PARTITION_COUNT, 3);
    }

    /**
     * Get the replication factor for Kafka topics.
     * Not applicable for RabbitMQ.
     *
     * @return Replication factor
     */
    public int getReplicationFactor() {
        return config.getInteger(Keys.MESSAGE_BROKER_REPLICATION_FACTOR, 1);
    }

    /**
     * Get the acknowledgment mode for producers.
     * For Kafka: "0" (no acks), "1" (leader only), "all" (all replicas)
     * For RabbitMQ: "none", "single", "multiple"
     *
     * @return Acknowledgment mode
     */
    public String getProducerAcknowledgments() {
        return config.getString(Keys.MESSAGE_BROKER_PRODUCER_ACKS, "all");
    }

    /**
     * Get the maximum number of in-flight requests per connection for Kafka producer.
     * Not applicable for RabbitMQ.
     *
     * @return Maximum in-flight requests
     */
    public int getProducerMaxInFlightRequests() {
        return config.getInteger(Keys.MESSAGE_BROKER_PRODUCER_MAX_IN_FLIGHT, 5);
    }

    /**
     * Get the batch size for Kafka producer in bytes.
     * Not applicable for RabbitMQ.
     *
     * @return Batch size in bytes
     */
    public int getProducerBatchSize() {
        return config.getInteger(Keys.MESSAGE_BROKER_PRODUCER_BATCH_SIZE, 16384);
    }

    /**
     * Get the linger time for Kafka producer in milliseconds.
     * Not applicable for RabbitMQ.
     *
     * @return Linger time in milliseconds
     */
    public int getProducerLingerMs() {
        return config.getInteger(Keys.MESSAGE_BROKER_PRODUCER_LINGER_MS, 0);
    }

    /**
     * Get the buffer memory for Kafka producer in bytes.
     * Not applicable for RabbitMQ.
     *
     * @return Buffer memory in bytes
     */
    public long getProducerBufferMemory() {
        return config.getLong(Keys.MESSAGE_BROKER_PRODUCER_BUFFER_MEMORY, 33554432L);
    }

    /**
     * Get the compression type for Kafka producer.
     * Supported values: "none", "gzip", "snappy", "lz4", "zstd"
     * Not applicable for RabbitMQ.
     *
     * @return Compression type
     */
    public String getProducerCompressionType() {
        return config.getString(Keys.MESSAGE_BROKER_PRODUCER_COMPRESSION_TYPE, "none");
    }

    /**
     * Get the maximum poll records for Kafka consumer.
     * Not applicable for RabbitMQ.
     *
     * @return Maximum poll records
     */
    public int getConsumerMaxPollRecords() {
        return config.getInteger(Keys.MESSAGE_BROKER_CONSUMER_MAX_POLL_RECORDS, 500);
    }

    /**
     * Get the auto offset reset policy for Kafka consumer.
     * Supported values: "earliest", "latest", "none"
     * Not applicable for RabbitMQ.
     *
     * @return Auto offset reset policy
     */
    public String getConsumerAutoOffsetReset() {
        return config.getString(Keys.MESSAGE_BROKER_CONSUMER_AUTO_OFFSET_RESET, "latest");
    }

    /**
     * Get the prefetch count for RabbitMQ consumer.
     * Not applicable for Kafka.
     *
     * @return Prefetch count
     */
    public int getConsumerPrefetchCount() {
        return config.getInteger(Keys.MESSAGE_BROKER_CONSUMER_PREFETCH_COUNT, 10);
    }

    /**
     * Get the queue durability setting for RabbitMQ.
     * Not applicable for Kafka.
     *
     * @return True if queues should be durable
     */
    public boolean isQueueDurable() {
        return config.getBoolean(Keys.MESSAGE_BROKER_QUEUE_DURABLE, true);
    }

    /**
     * Get the exchange durability setting for RabbitMQ.
     * Not applicable for Kafka.
     *
     * @return True if exchanges should be durable
     */
    public boolean isExchangeDurable() {
        return config.getBoolean(Keys.MESSAGE_BROKER_EXCHANGE_DURABLE, true);
    }

    /**
     * Get the exchange type for RabbitMQ.
     * Supported values: "direct", "fanout", "topic", "headers"
     * Not applicable for Kafka.
     *
     * @return Exchange type
     */
    public String getExchangeType() {
        return config.getString(Keys.MESSAGE_BROKER_EXCHANGE_TYPE, "topic");
    }

    /**
     * Get the retry policy for message delivery.
     * Supported values: "none", "simple", "exponential"
     *
     * @return Retry policy
     */
    public String getRetryPolicy() {
        return config.getString(Keys.MESSAGE_BROKER_RETRY_POLICY, "exponential");
    }

    /**
     * Get the maximum number of retry attempts for message delivery.
     *
     * @return Maximum retry attempts
     */
    public int getMaxRetryAttempts() {
        return config.getInteger(Keys.MESSAGE_BROKER_MAX_RETRY_ATTEMPTS, 3);
    }

    /**
     * Get the initial retry interval in milliseconds.
     *
     * @return Initial retry interval in milliseconds
     */
    public long getInitialRetryIntervalMs() {
        return config.getLong(Keys.MESSAGE_BROKER_INITIAL_RETRY_INTERVAL_MS, 1000L);
    }

    /**
     * Get the maximum retry interval in milliseconds.
     * Used for exponential backoff.
     *
     * @return Maximum retry interval in milliseconds
     */
    public long getMaxRetryIntervalMs() {
        return config.getLong(Keys.MESSAGE_BROKER_MAX_RETRY_INTERVAL_MS, 60000L);
    }

    /**
     * Get the retry backoff multiplier.
     * Used for exponential backoff.
     *
     * @return Retry backoff multiplier
     */
    public double getRetryBackoffMultiplier() {
        return config.getDouble(Keys.MESSAGE_BROKER_RETRY_BACKOFF_MULTIPLIER, 2.0);
    }

    /**
     * Get the dead letter queue or topic name.
     *
     * @return Dead letter queue or topic name
     */
    public String getDeadLetterDestination() {
        return config.getString(Keys.MESSAGE_BROKER_DEAD_LETTER_DESTINATION, "dead-letter");
    }

    /**
     * Check if SSL is enabled for the message broker connection.
     *
     * @return True if SSL is enabled
     */
    public boolean isSslEnabled() {
        return config.getBoolean(Keys.MESSAGE_BROKER_SSL_ENABLED, false);
    }

    /**
     * Get the SSL keystore location.
     *
     * @return SSL keystore location
     */
    public String getSslKeystoreLocation() {
        return config.getString(Keys.MESSAGE_BROKER_SSL_KEYSTORE_LOCATION);
    }

    /**
     * Get the SSL keystore password.
     *
     * @return SSL keystore password
     */
    public String getSslKeystorePassword() {
        return config.getString(Keys.MESSAGE_BROKER_SSL_KEYSTORE_PASSWORD);
    }

    /**
     * Get the SSL truststore location.
     *
     * @return SSL truststore location
     */
    public String getSslTruststoreLocation() {
        return config.getString(Keys.MESSAGE_BROKER_SSL_TRUSTSTORE_LOCATION);
    }

    /**
     * Get the SSL truststore password.
     *
     * @return SSL truststore password
     */
    public String getSslTruststorePassword() {
        return config.getString(Keys.MESSAGE_BROKER_SSL_TRUSTSTORE_PASSWORD);
    }

    /**
     * Get the message retention period in milliseconds.
     * Applicable for Kafka topics.
     *
     * @return Message retention period in milliseconds
     */
    public long getMessageRetentionMs() {
        return config.getLong(Keys.MESSAGE_BROKER_MESSAGE_RETENTION_MS, 604800000L); // 7 days by default
    }

    /**
     * Get the message retention size in bytes.
     * Applicable for Kafka topics.
     *
     * @return Message retention size in bytes
     */
    public long getMessageRetentionBytes() {
        return config.getLong(Keys.MESSAGE_BROKER_MESSAGE_RETENTION_BYTES, -1L); // Unlimited by default
    }

    /**
     * Check if message schema validation is enabled.
     *
     * @return True if schema validation is enabled
     */
    public boolean isSchemaValidationEnabled() {
        return config.getBoolean(Keys.MESSAGE_BROKER_SCHEMA_VALIDATION_ENABLED, false);
    }

    /**
     * Get the schema evolution strategy.
     * Supported values: "backward", "forward", "full", "none"
     *
     * @return Schema evolution strategy
     */
    public String getSchemaEvolutionStrategy() {
        return config.getString(Keys.MESSAGE_BROKER_SCHEMA_EVOLUTION_STRATEGY, "backward");
    }
    
    /**
     * Get Kafka producer configuration as a map.
     * 
     * @return Map of Kafka producer configuration properties
     */
    public Map<String, Object> getKafkaProducerConfig() {
        Map<String, Object> properties = new HashMap<>();
        
        properties.put("bootstrap.servers", getConnectionString());
        properties.put("client.id", getClientId());
        properties.put("acks", getProducerAcknowledgments());
        properties.put("max.in.flight.requests.per.connection", getProducerMaxInFlightRequests());
        properties.put("batch.size", getProducerBatchSize());
        properties.put("linger.ms", getProducerLingerMs());
        properties.put("buffer.memory", getProducerBufferMemory());
        properties.put("compression.type", getProducerCompressionType());
        
        // Configure serializers based on format
        String format = getSerializationFormat();
        if ("json".equalsIgnoreCase(format)) {
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        } else if ("protobuf".equalsIgnoreCase(format)) {
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer");
            if (getSchemaRegistryUrl() != null) {
                properties.put("schema.registry.url", getSchemaRegistryUrl());
            }
        } else if ("avro".equalsIgnoreCase(format)) {
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
            if (getSchemaRegistryUrl() != null) {
                properties.put("schema.registry.url", getSchemaRegistryUrl());
            }
        }
        
        // Configure SSL if enabled
        if (isSslEnabled()) {
            properties.put("security.protocol", "SSL");
            if (getSslKeystoreLocation() != null) {
                properties.put("ssl.keystore.location", getSslKeystoreLocation());
            }
            if (getSslKeystorePassword() != null) {
                properties.put("ssl.keystore.password", getSslKeystorePassword());
            }
            if (getSslTruststoreLocation() != null) {
                properties.put("ssl.truststore.location", getSslTruststoreLocation());
            }
            if (getSslTruststorePassword() != null) {
                properties.put("ssl.truststore.password", getSslTruststorePassword());
            }
        }
        
        return properties;
    }
    
    /**
     * Get Kafka consumer configuration as a map.
     * 
     * @return Map of Kafka consumer configuration properties
     */
    public Map<String, Object> getKafkaConsumerConfig() {
        Map<String, Object> properties = new HashMap<>();
        
        properties.put("bootstrap.servers", getConnectionString());
        properties.put("group.id", getConsumerGroupId());
        properties.put("client.id", getClientId());
        properties.put("max.poll.records", getConsumerMaxPollRecords());
        properties.put("auto.offset.reset", getConsumerAutoOffsetReset());
        properties.put("enable.auto.commit", "false"); // Manual commit for better control
        
        // Configure deserializers based on format
        String format = getSerializationFormat();
        if ("json".equalsIgnoreCase(format)) {
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        } else if ("protobuf".equalsIgnoreCase(format)) {
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer");
            if (getSchemaRegistryUrl() != null) {
                properties.put("schema.registry.url", getSchemaRegistryUrl());
            }
        } else if ("avro".equalsIgnoreCase(format)) {
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
            if (getSchemaRegistryUrl() != null) {
                properties.put("schema.registry.url", getSchemaRegistryUrl());
            }
        }
        
        // Configure SSL if enabled
        if (isSslEnabled()) {
            properties.put("security.protocol", "SSL");
            if (getSslKeystoreLocation() != null) {
                properties.put("ssl.keystore.location", getSslKeystoreLocation());
            }
            if (getSslKeystorePassword() != null) {
                properties.put("ssl.keystore.password", getSslKeystorePassword());
            }
            if (getSslTruststoreLocation() != null) {
                properties.put("ssl.truststore.location", getSslTruststoreLocation());
            }
            if (getSslTruststorePassword() != null) {
                properties.put("ssl.truststore.password", getSslTruststorePassword());
            }
        }
        
        return properties;
    }
    
    /**
     * Get RabbitMQ connection configuration as a map.
     * 
     * @return Map of RabbitMQ connection configuration properties
     */
    public Map<String, Object> getRabbitMQConnectionConfig() {
        Map<String, Object> properties = new HashMap<>();
        
        // Parse connection URI to extract components if needed
        String connectionString = getConnectionString();
        properties.put("uri", connectionString);
        
        // Configure SSL if enabled
        if (isSslEnabled()) {
            properties.put("ssl", true);
            if (getSslKeystoreLocation() != null) {
                properties.put("keyStore", getSslKeystoreLocation());
            }
            if (getSslKeystorePassword() != null) {
                properties.put("keyStorePassphrase", getSslKeystorePassword());
            }
            if (getSslTruststoreLocation() != null) {
                properties.put("trustStore", getSslTruststoreLocation());
            }
            if (getSslTruststorePassword() != null) {
                properties.put("trustStorePassphrase", getSslTruststorePassword());
            }
        }
        
        return properties;
    }
    
    /**
     * Get RabbitMQ exchange configuration as a map.
     * 
     * @return Map of RabbitMQ exchange configuration properties
     */
    public Map<String, Object> getRabbitMQExchangeConfig() {
        Map<String, Object> properties = new HashMap<>();
        
        properties.put("type", getExchangeType());
        properties.put("durable", isExchangeDurable());
        properties.put("autoDelete", false);
        
        return properties;
    }
    
    /**
     * Get RabbitMQ queue configuration as a map.
     * 
     * @return Map of RabbitMQ queue configuration properties
     */
    public Map<String, Object> getRabbitMQQueueConfig() {
        Map<String, Object> properties = new HashMap<>();
        
        properties.put("durable", isQueueDurable());
        properties.put("exclusive", false);
        properties.put("autoDelete", false);
        
        // Configure dead letter exchange if retry policy is enabled
        if (!"none".equalsIgnoreCase(getRetryPolicy())) {
            properties.put("x-dead-letter-exchange", getDeadLetterDestination());
        }
        
        return properties;
    }
    
    /**
     * Get RabbitMQ consumer configuration as a map.
     * 
     * @return Map of RabbitMQ consumer configuration properties
     */
    public Map<String, Object> getRabbitMQConsumerConfig() {
        Map<String, Object> properties = new HashMap<>();
        
        properties.put("prefetchCount", getConsumerPrefetchCount());
        properties.put("exclusive", false);
        properties.put("autoAck", false); // Manual acknowledgment for better control
        
        return properties;
    }
}