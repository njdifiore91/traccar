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
package org.traccar.messaging;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * RabbitMQ-specific configuration for the Event Processing Service.
 * Configures exchanges, queues, bindings, and message handling settings.
 */
@Configuration
public class RabbitMQConfig {

    @Value("${rabbitmq.exchange.positions:positions}")
    private String positionsExchangeName;

    @Value("${rabbitmq.exchange.events:events}")
    private String eventsExchangeName;

    @Value("${rabbitmq.queue.positions:event-service.positions}")
    private String positionsQueueName;

    @Value("${rabbitmq.queue.events:events}")
    private String eventsQueueName;

    @Value("${rabbitmq.routing-key.positions:enriched.positions}")
    private String positionsRoutingKey;

    @Value("${rabbitmq.routing-key.events:events}")
    private String eventsRoutingKey;

    @Value("${rabbitmq.queue.dlq.positions:event-service.positions.dlq}")
    private String positionsDlqName;

    @Value("${rabbitmq.exchange.dlx:dlx}")
    private String deadLetterExchangeName;

    @Value("${rabbitmq.queue.ttl:30000}")
    private int queueTtl;
    
    @Value("${rabbitmq.listener.concurrency:2}")
    private int concurrency;
    
    @Value("${rabbitmq.listener.max-concurrency:5}")
    private int maxConcurrency;
    
    @Value("${rabbitmq.retry.initial-interval:1000}")
    private long retryInitialInterval;
    
    @Value("${rabbitmq.retry.max-interval:10000}")
    private long retryMaxInterval;
    
    @Value("${rabbitmq.retry.multiplier:2.0}")
    private double retryMultiplier;
    
    @Value("${rabbitmq.retry.max-attempts:3}")
    private int maxAttempts;

    /**
     * Configure the exchange for position messages.
     * Uses a topic exchange to support routing based on position attributes.
     */
    @Bean
    public TopicExchange positionsExchange() {
        return new TopicExchange(positionsExchangeName, true, false);
    }

    /**
     * Configure the exchange for event messages.
     * Uses a direct exchange for simple routing to notification service.
     */
    @Bean
    public DirectExchange eventsExchange() {
        return new DirectExchange(eventsExchangeName, true, false);
    }

    /**
     * Configure the dead letter exchange for failed message processing.
     */
    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(deadLetterExchangeName, true, false);
    }

    /**
     * Configure the queue for position messages with dead letter handling.
     * Messages that cannot be processed will be sent to the dead letter queue.
     */
    @Bean
    public Queue positionsQueue() {
        return QueueBuilder.durable(positionsQueueName)
                .withArgument("x-dead-letter-exchange", deadLetterExchangeName)
                .withArgument("x-dead-letter-routing-key", positionsDlqName)
                .withArgument("x-message-ttl", queueTtl)
                .build();
    }

    /**
     * Configure the queue for event messages.
     */
    @Bean
    public Queue eventsQueue() {
        return QueueBuilder.durable(eventsQueueName)
                .build();
    }

    /**
     * Configure the dead letter queue for position messages that failed processing.
     */
    @Bean
    public Queue positionsDlq() {
        return QueueBuilder.durable(positionsDlqName)
                .build();
    }

    /**
     * Bind the positions queue to the positions exchange with the appropriate routing key.
     */
    @Bean
    public Binding positionsBinding() {
        return BindingBuilder
                .bind(positionsQueue())
                .to(positionsExchange())
                .with(positionsRoutingKey);
    }

    /**
     * Bind the events queue to the events exchange with the appropriate routing key.
     */
    @Bean
    public Binding eventsBinding() {
        return BindingBuilder
                .bind(eventsQueue())
                .to(eventsExchange())
                .with(eventsRoutingKey);
    }

    /**
     * Bind the positions dead letter queue to the dead letter exchange.
     */
    @Bean
    public Binding positionsDlqBinding() {
        return BindingBuilder
                .bind(positionsDlq())
                .to(deadLetterExchange())
                .with(positionsDlqName);
    }

    /**
     * Configure the message converter for serializing/deserializing messages.
     * Uses Jackson for JSON conversion.
     */
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    /**
     * Configure the RabbitTemplate with the message converter.
     * Adds publisher confirms and returns for reliable message delivery.
     */
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(jsonMessageConverter());
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            if (!ack) {
                // Handle nack - message was not confirmed
                // This would typically be logged and/or trigger a retry mechanism
            }
        });
        rabbitTemplate.setReturnsCallback(returned -> {
            // Handle returned message - message could not be routed to queue
            // This would typically be logged and/or trigger a retry mechanism
        });
        return rabbitTemplate;
    }
    
    /**
     * Configure RabbitMQ connection factory with appropriate settings for the Event Service.
     * This bean customizes the auto-configured connection factory.
     */
    @Bean
    public org.springframework.amqp.rabbit.connection.ConnectionFactoryCustomizer connectionFactoryCustomizer() {
        return factory -> {
            factory.setPublisherConfirmType(org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType.CORRELATED);
            factory.setPublisherReturns(true);
        };
    }
    
    /**
     * Configure the retry template for message processing retries.
     * Uses exponential backoff to avoid overwhelming the system during retries.
     */
    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(retryInitialInterval);
        backOffPolicy.setMaxInterval(retryMaxInterval);
        backOffPolicy.setMultiplier(retryMultiplier);
        
        retryTemplate.setBackOffPolicy(backOffPolicy);
        
        return retryTemplate;
    }
    
    /**
     * Configure the RabbitMQ listener container factory with appropriate settings.
     * This factory is used to create containers for @RabbitListener annotations.
     */
    @Bean
    public RabbitListenerContainerFactory<?> rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            SimpleRabbitListenerContainerFactoryConfigurer configurer,
            MeterRegistry meterRegistry) {
        
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        
        // Set concurrency limits
        factory.setConcurrentConsumers(concurrency);
        factory.setMaxConcurrentConsumers(maxConcurrency);
        
        // Set message converter
        factory.setMessageConverter(jsonMessageConverter());
        
        // Configure acknowledgment mode - manual ack to ensure proper error handling
        factory.setAcknowledgeMode(org.springframework.amqp.core.AcknowledgeMode.MANUAL);
        
        // Add metrics collection
        factory.setMicrometerEnabled(true);
        factory.setObservationRegistry(meterRegistry.getObservationRegistry());
        
        return factory;
    }
}