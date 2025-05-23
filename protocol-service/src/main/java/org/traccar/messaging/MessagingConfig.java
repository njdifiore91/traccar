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
import com.google.inject.Singleton;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for the messaging subsystem, supporting both Kafka and RabbitMQ.
 * This class provides configuration for broker connection settings, serialization options,
 * and retry policies based on environment variables and configuration files.
 */
@Singleton
public class MessagingConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessagingConfig.class);

    /**
     * Supported message broker types.
     */
    public enum BrokerType {
        KAFKA,
        RABBITMQ
    }

    /**
     * Supported serialization formats.
     */
    public enum SerializationFormat {
        PROTOBUF,
        JSON
    }

    private final Config config;
    private final BrokerType brokerType;
    private final SerializationFormat serializationFormat;
    private final Map<String, Object> brokerConfig;
    private final RetryPolicy retryPolicy;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;

    /**
     * Creates a new MessagingConfig instance with the specified configuration.
     *
     * @param config The Traccar configuration
     * @param meterRegistry The meter registry for metrics collection
     * @param tracer The OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public MessagingConfig(Config config, MeterRegistry meterRegistry, Tracer tracer) {
        this.config = config;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        this.brokerType = detectBrokerType();
        this.serializationFormat = detectSerializationFormat();
        this.brokerConfig = loadBrokerConfig();
        this.retryPolicy = createRetryPolicy();
        
        LOGGER.info("Messaging configuration initialized with broker type: {}, serialization format: {}",
                brokerType, serializationFormat);
    }

    /**
     * Detects the broker type based on configuration.
     * Defaults to Kafka if not specified.
     *
     * @return The detected broker type
     */
    private BrokerType detectBrokerType() {
        String brokerTypeStr = config.getString("messaging.broker.type", "kafka").toUpperCase();
        try {
            return BrokerType.valueOf(brokerTypeStr);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Invalid broker type: {}. Defaulting to Kafka.", brokerTypeStr);
            return BrokerType.KAFKA;
        }
    }

    /**
     * Detects the serialization format based on configuration.
     * Defaults to Protocol Buffers if not specified.
     *
     * @return The detected serialization format
     */
    private SerializationFormat detectSerializationFormat() {
        String formatStr = config.getString("messaging.serialization.format", "protobuf").toUpperCase();
        try {
            return SerializationFormat.valueOf(formatStr);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Invalid serialization format: {}. Defaulting to Protocol Buffers.", formatStr);
            return SerializationFormat.PROTOBUF;
        }
    }

    /**
     * Loads broker-specific configuration based on the detected broker type.
     *
     * @return A map of broker configuration properties
     */
    private Map<String, Object> loadBrokerConfig() {
        Map<String, Object> properties = new HashMap<>();
        
        // Common properties
        properties.put("messaging.client.id", config.getString("messaging.client.id", "protocol-service"));
        properties.put("messaging.topic.prefix", config.getString("messaging.topic.prefix", ""));
        
        // Broker-specific properties
        switch (brokerType) {
            case KAFKA:
                loadKafkaConfig(properties);
                break;
            case RABBITMQ:
                loadRabbitMqConfig(properties);
                break;
        }
        
        return properties;
    }

    /**
     * Loads Kafka-specific configuration properties.
     *
     * @param properties The properties map to populate
     */
    private void loadKafkaConfig(Map<String, Object> properties) {
        // Bootstrap servers (comma-separated list of broker addresses)
        properties.put("bootstrap.servers", 
                config.getString("messaging.kafka.bootstrap.servers", "localhost:9092"));
        
        // Producer configuration
        properties.put("acks", config.getString("messaging.kafka.acks", "all"));
        properties.put("retries", config.getInteger("messaging.kafka.retries", 3));
        properties.put("batch.size", config.getInteger("messaging.kafka.batch.size", 16384));
        properties.put("linger.ms", config.getInteger("messaging.kafka.linger.ms", 1));
        properties.put("buffer.memory", config.getInteger("messaging.kafka.buffer.memory", 33554432));
        properties.put("max.in.flight.requests.per.connection", 
                config.getInteger("messaging.kafka.max.in.flight.requests", 5));
        properties.put("enable.idempotence", 
                config.getBoolean("messaging.kafka.enable.idempotence", true));
        properties.put("compression.type", 
                config.getString("messaging.kafka.compression.type", "snappy"));
        
        // Security configuration (if enabled)
        if (config.getBoolean("messaging.kafka.security.enabled", false)) {
            properties.put("security.protocol", 
                    config.getString("messaging.kafka.security.protocol", "SASL_SSL"));
            properties.put("sasl.mechanism", 
                    config.getString("messaging.kafka.sasl.mechanism", "PLAIN"));
            properties.put("sasl.jaas.config", 
                    config.getString("messaging.kafka.sasl.jaas.config", ""));
        }
    }

    /**
     * Loads RabbitMQ-specific configuration properties.
     *
     * @param properties The properties map to populate
     */
    private void loadRabbitMqConfig(Map<String, Object> properties) {
        // Connection properties
        properties.put("host", config.getString("messaging.rabbitmq.host", "localhost"));
        properties.put("port", config.getInteger("messaging.rabbitmq.port", 5672));
        properties.put("username", config.getString("messaging.rabbitmq.username", "guest"));
        properties.put("password", config.getString("messaging.rabbitmq.password", "guest"));
        properties.put("virtual-host", config.getString("messaging.rabbitmq.virtual-host", "/"));
        
        // Exchange configuration
        properties.put("exchange", config.getString("messaging.rabbitmq.exchange", "traccar"));
        properties.put("exchange.type", config.getString("messaging.rabbitmq.exchange.type", "topic"));
        properties.put("exchange.durable", config.getBoolean("messaging.rabbitmq.exchange.durable", true));
        
        // Connection pool configuration
        properties.put("connection.timeout", 
                config.getInteger("messaging.rabbitmq.connection.timeout", 60000));
        properties.put("connection.heartbeat", 
                config.getInteger("messaging.rabbitmq.connection.heartbeat", 60));
        properties.put("channel.pool.size", 
                config.getInteger("messaging.rabbitmq.channel.pool.size", 10));
        
        // Publisher confirms
        properties.put("publisher.confirms", 
                config.getBoolean("messaging.rabbitmq.publisher.confirms", true));
    }

    /**
     * Creates a retry policy based on configuration.
     *
     * @return The configured retry policy
     */
    private RetryPolicy createRetryPolicy() {
        int maxAttempts = config.getInteger("messaging.retry.max.attempts", 3);
        long initialBackoffMs = config.getLong("messaging.retry.initial.backoff.ms", 1000);
        long maxBackoffMs = config.getLong("messaging.retry.max.backoff.ms", 10000);
        double backoffMultiplier = Double.parseDouble(
                config.getString("messaging.retry.backoff.multiplier", "2.0"));
        boolean randomizationFactor = config.getBoolean("messaging.retry.randomization.factor", true);
        
        return new RetryPolicy(maxAttempts, initialBackoffMs, maxBackoffMs, backoffMultiplier, randomizationFactor);
    }

    /**
     * Gets the configured broker type.
     *
     * @return The broker type (KAFKA or RABBITMQ)
     */
    public BrokerType getBrokerType() {
        return brokerType;
    }

    /**
     * Gets the configured serialization format.
     *
     * @return The serialization format (PROTOBUF or JSON)
     */
    public SerializationFormat getSerializationFormat() {
        return serializationFormat;
    }

    /**
     * Gets the broker configuration properties.
     *
     * @return A map of broker configuration properties
     */
    public Map<String, Object> getBrokerConfig() {
        return brokerConfig;
    }

    /**
     * Gets the configured retry policy.
     *
     * @return The retry policy
     */
    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    /**
     * Gets the meter registry for metrics collection.
     *
     * @return The meter registry
     */
    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    /**
     * Gets the OpenTelemetry tracer for distributed tracing.
     *
     * @return The tracer
     */
    public Tracer getTracer() {
        return tracer;
    }

    /**
     * Gets a specific broker configuration property.
     *
     * @param key The property key
     * @param defaultValue The default value if the property is not found
     * @param <T> The property type
     * @return The property value or the default value if not found
     */
    @SuppressWarnings("unchecked")
    public <T> T getBrokerProperty(String key, T defaultValue) {
        return (T) brokerConfig.getOrDefault(key, defaultValue);
    }

    /**
     * Gets the topic prefix for the current environment.
     *
     * @return The topic prefix or an empty string if not configured
     */
    public String getTopicPrefix() {
        return (String) brokerConfig.getOrDefault("messaging.topic.prefix", "");
    }

    /**
     * Constructs a full topic name with the configured prefix.
     *
     * @param baseTopic The base topic name
     * @return The full topic name with prefix
     */
    public String getFullTopicName(String baseTopic) {
        String prefix = getTopicPrefix();
        return prefix.isEmpty() ? baseTopic : prefix + "." + baseTopic;
    }

    /**
     * Retry policy for messaging operations.
     */
    public static class RetryPolicy {
        private final int maxAttempts;
        private final long initialBackoffMs;
        private final long maxBackoffMs;
        private final double backoffMultiplier;
        private final boolean randomizationFactor;

        /**
         * Creates a new retry policy with the specified parameters.
         *
         * @param maxAttempts The maximum number of retry attempts
         * @param initialBackoffMs The initial backoff time in milliseconds
         * @param maxBackoffMs The maximum backoff time in milliseconds
         * @param backoffMultiplier The multiplier for exponential backoff
         * @param randomizationFactor Whether to add randomization to backoff times
         */
        public RetryPolicy(int maxAttempts, long initialBackoffMs, long maxBackoffMs, 
                          double backoffMultiplier, boolean randomizationFactor) {
            this.maxAttempts = maxAttempts;
            this.initialBackoffMs = initialBackoffMs;
            this.maxBackoffMs = maxBackoffMs;
            this.backoffMultiplier = backoffMultiplier;
            this.randomizationFactor = randomizationFactor;
        }

        /**
         * Gets the maximum number of retry attempts.
         *
         * @return The maximum number of retry attempts
         */
        public int getMaxAttempts() {
            return maxAttempts;
        }

        /**
         * Gets the initial backoff time in milliseconds.
         *
         * @return The initial backoff time
         */
        public long getInitialBackoffMs() {
            return initialBackoffMs;
        }

        /**
         * Gets the maximum backoff time in milliseconds.
         *
         * @return The maximum backoff time
         */
        public long getMaxBackoffMs() {
            return maxBackoffMs;
        }

        /**
         * Gets the backoff multiplier for exponential backoff.
         *
         * @return The backoff multiplier
         */
        public double getBackoffMultiplier() {
            return backoffMultiplier;
        }

        /**
         * Checks if randomization factor is enabled.
         *
         * @return True if randomization is enabled, false otherwise
         */
        public boolean isRandomizationFactor() {
            return randomizationFactor;
        }

        /**
         * Calculates the backoff duration for a specific attempt.
         *
         * @param attempt The current attempt number (0-based)
         * @return The backoff duration
         */
        public Duration calculateBackoffDuration(int attempt) {
            double backoff = initialBackoffMs * Math.pow(backoffMultiplier, attempt);
            
            // Add randomization if enabled (±15%)
            if (randomizationFactor) {
                double randomFactor = 0.85 + Math.random() * 0.3; // 0.85 to 1.15
                backoff = backoff * randomFactor;
            }
            
            // Cap at max backoff
            backoff = Math.min(backoff, maxBackoffMs);
            
            return Duration.ofMillis((long) backoff);
        }
    }
}