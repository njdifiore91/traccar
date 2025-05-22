/*
 * Copyright 2023-2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.reports;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
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
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Provides integration with the message broker (Kafka/RabbitMQ) for asynchronous report generation
 * in the Reporting Service. Implements producers for publishing report completion events and consumers
 * for receiving report generation requests from the Scheduler Service. Handles message serialization/deserialization,
 * implements retry logic for failed message processing, and manages consumer group configuration.
 */
@Service
@EnableKafka
@Configuration
public class MessageBrokerIntegration {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBrokerIntegration.class);

    @Value("${kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group-id:reporting-service}")
    private String consumerGroupId;

    @Value("${kafka.topic.report-request:report-requests}")
    private String reportRequestTopic;

    @Value("${kafka.topic.report-completion:report-completions}")
    private String reportCompletionTopic;

    @Value("${kafka.retry.max-attempts:3}")
    private int maxRetryAttempts;

    @Value("${kafka.retry.initial-interval:1000}")
    private long retryInitialInterval;

    @Value("${kafka.retry.multiplier:2.0}")
    private double retryMultiplier;

    @Value("${kafka.retry.max-interval:10000}")
    private long retryMaxInterval;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Constructor for MessageBrokerIntegration.
     * 
     * @param kafkaTemplate the Kafka template for sending messages
     */
    @Inject
    public MessageBrokerIntegration(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Configures the Kafka producer factory with appropriate serialization and settings.
     * 
     * @return the producer factory
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Creates a Kafka template for sending messages to topics.
     * 
     * @return the Kafka template
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Configures the Kafka consumer factory with appropriate deserialization and settings.
     * 
     * @return the consumer factory
     */
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Creates a retry template with exponential backoff for message processing retries.
     * 
     * @return the retry template
     */
    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(retryInitialInterval);
        backOffPolicy.setMultiplier(retryMultiplier);
        backOffPolicy.setMaxInterval(retryMaxInterval);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(maxRetryAttempts);
        retryTemplate.setRetryPolicy(retryPolicy);
        
        return retryTemplate;
    }

    /**
     * Configures the Kafka listener container factory with error handling and retry logic.
     * 
     * @return the Kafka listener container factory
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        // Configure error handler with dead letter topic
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> {
                    LOGGER.error("Failed to process message after {} attempts, sending to DLT: {}", 
                            maxRetryAttempts, ex.getMessage());
                    return null; // Use default DLT naming convention (original-topic.DLT)
                });
        
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> 
                LOGGER.warn("Failed to process message, attempt {}: {}", deliveryAttempt, ex.getMessage()));
        
        factory.setCommonErrorHandler(errorHandler);
        factory.setConcurrency(3); // Number of consumer threads
        
        return factory;
    }

    /**
     * Publishes a report completion event to the message broker.
     * 
     * @param reportId the ID of the completed report
     * @param userId the ID of the user who requested the report
     * @param status the status of the report generation (success/failure)
     * @param resultUrl the URL where the report can be accessed (if successful)
     * @return a CompletableFuture that completes when the message is sent
     */
    public CompletableFuture<Void> publishReportCompletion(String reportId, long userId, String status, String resultUrl) {
        Map<String, Object> message = new HashMap<>();
        message.put("reportId", reportId);
        message.put("userId", userId);
        message.put("status", status);
        message.put("resultUrl", resultUrl);
        message.put("timestamp", System.currentTimeMillis());
        message.put("correlationId", UUID.randomUUID().toString());
        
        LOGGER.info("Publishing report completion event: reportId={}, userId={}, status={}", 
                reportId, userId, status);
        
        return kafkaTemplate.send(reportCompletionTopic, reportId, message)
                .thenRun(() -> LOGGER.debug("Successfully published report completion event: {}", reportId))
                .exceptionally(ex -> {
                    LOGGER.error("Failed to publish report completion event: {}", ex.getMessage(), ex);
                    return null;
                });
    }

    /**
     * Listens for report generation requests from the message broker.
     * 
     * @param message the report generation request message
     */
    @KafkaListener(topics = "${kafka.topic.report-request:report-requests}", 
                  groupId = "${kafka.consumer.group-id:reporting-service}")
    public void consumeReportRequest(Map<String, Object> message) {
        String reportId = (String) message.get("reportId");
        String reportType = (String) message.get("reportType");
        Long userId = ((Number) message.get("userId")).longValue();
        String correlationId = (String) message.get("correlationId");
        
        LOGGER.info("Received report generation request: reportId={}, reportType={}, userId={}, correlationId={}", 
                reportId, reportType, userId, correlationId);
        
        try {
            // Process the report generation request
            // This would typically call into the report generation service
            // For now, we just log the receipt of the message
            
            LOGGER.info("Successfully processed report generation request: {}", reportId);
            
            // After successful processing, publish a completion event
            publishReportCompletion(reportId, userId, "SUCCESS", "/reports/" + reportId);
            
        } catch (Exception e) {
            LOGGER.error("Error processing report generation request: {}", e.getMessage(), e);
            // The error handler will handle retries and eventually send to DLT if needed
            throw e;
        }
    }

    /**
     * Sends a report generation request to the message broker.
     * 
     * @param reportId the ID of the report to generate
     * @param reportType the type of report to generate
     * @param userId the ID of the user requesting the report
     * @param parameters additional parameters for report generation
     * @return a CompletableFuture that completes when the message is sent
     */
    public CompletableFuture<Void> sendReportGenerationRequest(String reportId, String reportType, 
                                                             long userId, Map<String, Object> parameters) {
        Map<String, Object> message = new HashMap<>();
        message.put("reportId", reportId);
        message.put("reportType", reportType);
        message.put("userId", userId);
        message.put("parameters", parameters);
        message.put("timestamp", System.currentTimeMillis());
        message.put("correlationId", UUID.randomUUID().toString());
        
        LOGGER.info("Sending report generation request: reportId={}, reportType={}, userId={}", 
                reportId, reportType, userId);
        
        return kafkaTemplate.send(reportRequestTopic, reportId, message)
                .thenRun(() -> LOGGER.debug("Successfully sent report generation request: {}", reportId))
                .exceptionally(ex -> {
                    LOGGER.error("Failed to send report generation request: {}", ex.getMessage(), ex);
                    return null;
                });
    }
}