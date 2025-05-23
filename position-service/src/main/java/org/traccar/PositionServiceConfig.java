/*
 * Copyright 2018 - 2025 Anton Tananaev (anton@traccar.org)
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

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.web.client.RestTemplate;

import com.ecwid.consul.v1.ConsulClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsonp.JSONPModule;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Configuration class for the Position Processing Service.
 * 
 * This class configures the following components:
 * - Message broker connection (Kafka/RabbitMQ)
 * - Service discovery (Consul/Kubernetes)
 * - Circuit breakers (Resilience4j)
 * - Health checks
 * - Metrics collection (Micrometer/Prometheus)
 */
@Configuration
public class PositionServiceConfig {

    /**
     * Provides an executor service for asynchronous operations.
     * 
     * @return ExecutorService instance with a cached thread pool
     */
    @Bean
    public ExecutorService executorService() {
        return Executors.newCachedThreadPool();
    }

    /**
     * Provides an ObjectMapper for JSON serialization/deserialization.
     * 
     * @return Configured ObjectMapper instance
     */
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JSONPModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return objectMapper;
    }

    /**
     * Provides a RestTemplate for HTTP client operations.
     * 
     * @param builder RestTemplateBuilder injected by Spring
     * @return Configured RestTemplate instance
     */
    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        return builder
                .setConnectTimeout(Duration.ofSeconds(5))
                .setReadTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Configures Kafka producer factory for publishing position messages.
     * 
     * @param bootstrapServers Kafka bootstrap servers address
     * @return Configured ProducerFactory instance
     */
    @Bean
    @ConditionalOnProperty(name = "position.broker.type", havingValue = "kafka")
    public ProducerFactory<String, Object> kafkaProducerFactory(
            @Value("${position.kafka.bootstrap-servers}") String bootstrapServers) {
        
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                org.apache.kafka.common.serialization.StringSerializer.class);
        configProps.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                org.springframework.kafka.support.serializer.JsonSerializer.class);
        configProps.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Configures Kafka template for publishing position messages.
     * 
     * @param producerFactory Kafka producer factory
     * @return Configured KafkaTemplate instance
     */
    @Bean
    @ConditionalOnProperty(name = "position.broker.type", havingValue = "kafka")
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * Configures Kafka consumer factory for consuming position messages.
     * 
     * @param bootstrapServers Kafka bootstrap servers address
     * @param groupId Consumer group ID
     * @return Configured ConsumerFactory instance
     */
    @Bean
    @ConditionalOnProperty(name = "position.broker.type", havingValue = "kafka")
    public ConsumerFactory<String, Object> kafkaConsumerFactory(
            @Value("${position.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${position.kafka.consumer.group-id}") String groupId) {
        
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                org.apache.kafka.common.serialization.StringDeserializer.class);
        configProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                org.springframework.kafka.support.serializer.JsonDeserializer.class);
        configProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        configProps.put(org.springframework.kafka.support.serializer.JsonDeserializer.TRUSTED_PACKAGES, "org.traccar.*");
        
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    /**
     * Configures RabbitMQ connection for message broker integration.
     * This is an alternative to Kafka and will be used if position.broker.type=rabbitmq.
     * 
     * @param host RabbitMQ host
     * @param port RabbitMQ port
     * @param username RabbitMQ username
     * @param password RabbitMQ password
     * @return Configured RabbitMQ connection factory
     */
    @Bean
    @ConditionalOnProperty(name = "position.broker.type", havingValue = "rabbitmq")
    public org.springframework.amqp.rabbit.connection.CachingConnectionFactory rabbitConnectionFactory(
            @Value("${position.rabbitmq.host}") String host,
            @Value("${position.rabbitmq.port}") int port,
            @Value("${position.rabbitmq.username}") String username,
            @Value("${position.rabbitmq.password}") String password) {
        
        org.springframework.amqp.rabbit.connection.CachingConnectionFactory connectionFactory = 
                new org.springframework.amqp.rabbit.connection.CachingConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setPort(port);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        return connectionFactory;
    }

    /**
     * Configures RabbitMQ template for publishing position messages.
     * 
     * @param connectionFactory RabbitMQ connection factory
     * @param objectMapper JSON object mapper
     * @return Configured RabbitTemplate instance
     */
    @Bean
    @ConditionalOnProperty(name = "position.broker.type", havingValue = "rabbitmq")
    public org.springframework.amqp.rabbit.core.RabbitTemplate rabbitTemplate(
            org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory,
            ObjectMapper objectMapper) {
        
        org.springframework.amqp.rabbit.core.RabbitTemplate rabbitTemplate = 
                new org.springframework.amqp.rabbit.core.RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(
                new org.springframework.amqp.support.converter.Jackson2JsonMessageConverter(objectMapper));
        return rabbitTemplate;
    }

    /**
     * Configures Consul client for service discovery.
     * This will be used if position.discovery.type=consul.
     * 
     * @param host Consul host
     * @param port Consul port
     * @return Configured ConsulClient instance
     */
    @Bean
    @ConditionalOnProperty(name = "position.discovery.type", havingValue = "consul")
    public ConsulClient consulClient(
            @Value("${position.consul.host:localhost}") String host,
            @Value("${position.consul.port:8500}") int port) {
        
        return new ConsulClient(host, port);
    }

    /**
     * Configures Kubernetes client for service discovery.
     * This will be used if position.discovery.type=kubernetes.
     * 
     * @return Configured KubernetesClient instance
     */
    @Bean
    @ConditionalOnProperty(name = "position.discovery.type", havingValue = "kubernetes")
    public io.fabric8.kubernetes.client.KubernetesClient kubernetesClient() {
        return new io.fabric8.kubernetes.client.KubernetesClientBuilder().build();
    }

    /**
     * Configures CircuitBreakerRegistry for resilience patterns.
     * 
     * @return Configured CircuitBreakerRegistry instance
     */
    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry() {
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofMillis(1000))
                .permittedNumberOfCallsInHalfOpenState(2)
                .slidingWindowSize(10)
                .build();
        
        return CircuitBreakerRegistry.of(circuitBreakerConfig);
    }

    /**
     * Configures RetryRegistry for resilience patterns.
     * 
     * @return Configured RetryRegistry instance
     */
    @Bean
    public RetryRegistry retryRegistry() {
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(1000))
                .build();
        
        return RetryRegistry.of(retryConfig);
    }

    /**
     * Configures Prometheus meter registry for metrics collection.
     * 
     * @return Configured PrometheusMeterRegistry instance
     */
    @Bean
    public MeterRegistry meterRegistry() {
        return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    }

    /**
     * Configures health indicator for the Position Processing Service.
     * 
     * @return Configured HealthIndicator instance
     */
    @Bean
    public HealthIndicator positionServiceHealthIndicator() {
        return () -> {
            // In a real implementation, this would check dependencies like database, message broker, etc.
            return Health.up().withDetail("service", "position-service").build();
        };
    }

    /**
     * Configures health indicator for the message broker connection.
     * 
     * @return Configured HealthIndicator instance
     */
    @Bean
    public HealthIndicator messageBrokerHealthIndicator() {
        return () -> {
            // In a real implementation, this would check the message broker connection
            return Health.up().withDetail("component", "message-broker").build();
        };
    }

    /**
     * Configures health indicator for the service discovery connection.
     * 
     * @return Configured HealthIndicator instance
     */
    @Bean
    public HealthIndicator serviceDiscoveryHealthIndicator() {
        return () -> {
            // In a real implementation, this would check the service discovery connection
            return Health.up().withDetail("component", "service-discovery").build();
        };
    }
}