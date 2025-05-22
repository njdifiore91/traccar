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
package org.traccar.messaging.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.MessageBrokerClient;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka implementation of the MessageBrokerClient interface.
 */
public class KafkaMessageBrokerClient implements MessageBrokerClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageBrokerClient.class);

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;
    private final AtomicBoolean connected;
    private final String clientId;

    public KafkaMessageBrokerClient(Config config) {
        this.objectMapper = new ObjectMapper();
        this.executorService = Executors.newCachedThreadPool();
        this.connected = new AtomicBoolean(false);
        this.clientId = "traccar-" + UUID.randomUUID();

        String bootstrapServers = config.getString(Keys.PROCESSING_REMOTE_KAFKA_BOOTSTRAP_SERVERS.getKey());
        
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
        
        this.producer = new KafkaProducer<>(producerProps);
        this.connected.set(true);
        
        LOGGER.info("Kafka message broker client initialized with bootstrap servers: {}", bootstrapServers);
    }

    @Override
    public <T> CompletableFuture<Void> publish(String topic, T message) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        try {
            String messageJson = objectMapper.writeValueAsString(message);
            String key = message.getClass().getSimpleName() + "-" + System.currentTimeMillis();
            
            producer.send(new ProducerRecord<>(topic, key, messageJson), (metadata, exception) -> {
                if (exception != null) {
                    LOGGER.error("Failed to publish message to topic {}", topic, exception);
                    future.completeExceptionally(exception);
                } else {
                    LOGGER.debug("Published message to topic {} at offset {}", topic, metadata.offset());
                    future.complete(null);
                }
            });
        } catch (Exception e) {
            LOGGER.error("Error serializing message for topic {}", topic, e);
            future.completeExceptionally(e);
        }
        
        return future;
    }

    @Override
    public <T> CompletableFuture<Void> subscribe(String topic, MessageCallback<T> callback, Class<T> messageType) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        executorService.submit(() -> {
            try {
                Properties consumerProps = new Properties();
                consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                        producer.config().getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
                consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, clientId + "-" + topic);
                consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
                consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
                
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
                consumer.subscribe(Collections.singletonList(topic));
                
                future.complete(null);
                
                LOGGER.info("Subscribed to Kafka topic: {}", topic);
                
                while (connected.get()) {
                    try {
                        consumer.poll(Duration.ofMillis(100)).forEach(record -> {
                            try {
                                T message = objectMapper.readValue(record.value(), messageType);
                                callback.onMessage(message);
                            } catch (Exception e) {
                                LOGGER.error("Error deserializing message from topic {}", topic, e);
                            }
                        });
                    } catch (Exception e) {
                        LOGGER.error("Error polling messages from topic {}", topic, e);
                    }
                }
                
                consumer.close();
            } catch (Exception e) {
                LOGGER.error("Error subscribing to topic {}", topic, e);
                future.completeExceptionally(e);
            }
        });
        
        return future;
    }

    @Override
    public CompletableFuture<Void> close() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        try {
            connected.set(false);
            producer.close();
            executorService.shutdown();
            future.complete(null);
            LOGGER.info("Kafka message broker client closed");
        } catch (Exception e) {
            LOGGER.error("Error closing Kafka message broker client", e);
            future.completeExceptionally(e);
        }
        
        return future;
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }
}