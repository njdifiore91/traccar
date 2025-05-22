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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka implementation of the MessageProducer interface.
 */
public class KafkaMessageProducer implements MessageBrokerManager.MessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageProducer.class);

    private final String topic;
    private final ObjectMapper objectMapper;
    private final KafkaProducer<String, String> kafkaProducer;
    private final MessageBrokerManager.AbstractMessageProducer delegate;

    public KafkaMessageProducer(
            String topic,
            Config config,
            ObjectMapper objectMapper,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry,
            MeterRegistry meterRegistry,
            Tracer tracer) {
        this.topic = topic;
        this.objectMapper = objectMapper;

        // Initialize Kafka producer
        Properties properties = new Properties();
        properties.put("bootstrap.servers", config.getString(Keys.BROKER_URL));
        properties.put("acks", config.getString(Keys.BROKER_ACKS, "all"));
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("enable.idempotence", config.getBoolean(Keys.BROKER_IDEMPOTENCE, true));
        properties.put("retries", config.getInteger(Keys.BROKER_RETRIES, 3));
        properties.put("max.in.flight.requests.per.connection", 
                config.getInteger(Keys.BROKER_MAX_IN_FLIGHT, 5));

        // Add any additional Kafka-specific properties from configuration
        for (Object key : config.getKeys()) {
            String keyStr = key.toString();
            if (keyStr.startsWith("kafka.")) {
                String kafkaKey = keyStr.substring("kafka.".length());
                properties.put(kafkaKey, config.getString(keyStr));
            }
        }

        this.kafkaProducer = new KafkaProducer<>(properties);
        
        // Create delegate for common functionality
        this.delegate = new MessageBrokerManager.AbstractMessageProducer(
                topic, objectMapper, circuitBreakerRegistry, retryRegistry, meterRegistry, tracer) {
            @Override
            protected <T> CompletableFuture<Void> doPublish(String key, T message, Map<String, String> headers) {
                return KafkaMessageProducer.this.doPublish(key, message, headers);
            }

            @Override
            protected String getBrokerType() {
                return "kafka";
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
            String messageKey = key != null ? key : "";
            String messageValue = objectMapper.writeValueAsString(message);

            // Create Kafka record with headers
            ProducerRecord<String, String> record = 
                    new ProducerRecord<>(topic, messageKey, messageValue);

            // Add headers to the record
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                record.headers().add(entry.getKey(), entry.getValue().getBytes());
            }

            // Send the record asynchronously
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    future.complete(null);
                }
            });
        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }
}