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
package org.traccar.forward;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * RabbitMQ implementation of the MessageProducer interface.
 */
public class RabbitMqMessageProducer implements MessageBrokerManager.MessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqMessageProducer.class);

    private final AmqpClient amqpClient;
    private final ObjectMapper objectMapper;
    private final MessageBrokerManager.AbstractMessageProducer delegate;

    public RabbitMqMessageProducer(
            String topic,
            Config config,
            ObjectMapper objectMapper,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry,
            MeterRegistry meterRegistry,
            Tracer tracer) {
        this.objectMapper = objectMapper;

        String connectionUrl = config.getString(Keys.BROKER_URL);
        String exchange = config.getString(Keys.BROKER_EXCHANGE, "traccar");
        this.amqpClient = new AmqpClient(connectionUrl, exchange, topic);
        
        // Create delegate for common functionality
        this.delegate = new MessageBrokerManager.AbstractMessageProducer(
                topic, objectMapper, circuitBreakerRegistry, retryRegistry, meterRegistry, tracer) {
            @Override
            protected <T> CompletableFuture<Void> doPublish(String key, T message, Map<String, String> headers) {
                return RabbitMqMessageProducer.this.doPublish(key, message, headers);
            }

            @Override
            protected String getBrokerType() {
                return "rabbitmq";
            }
        };
    }

    @Override
    public <T> CompletableFuture<Void> publish(T message) {
        return delegate.publish(message);
    }

    @Override
    public <T> CompletableFuture<Void> publish(String key, T message) {
        return delegate.publish(key, message);
    }

    @Override
    public <T> CompletableFuture<Void> publish(String key, T message, Map<String, String> headers) {
        return delegate.publish(key, message, headers);
    }

    protected <T> CompletableFuture<Void> doPublish(String key, T message, Map<String, String> headers) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        try {
            String messageValue = objectMapper.writeValueAsString(message);
            amqpClient.publishMessage(messageValue, headers, key);
            future.complete(null);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public void close() {
        amqpClient.close();
    }
}