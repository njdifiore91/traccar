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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.api.GlobalOpenTelemetry;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Handles message broker integration for publishing events to Kafka.
 */
@Singleton
public class MessageBroker {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBroker.class);
    private static final String USER_EXPIRATION_TOPIC = "traccar.expirations.user";
    private static final String DEVICE_EXPIRATION_TOPIC = "traccar.expirations.device";

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;
    private final boolean enabled;

    private static final TextMapSetter<ProducerRecord<String, String>> SETTER =
            (carrier, key, value) -> carrier.headers().add(new RecordHeader(key, value.getBytes()));

    @Inject
    public MessageBroker(Config config, Tracer tracer) {
        this.tracer = tracer;
        this.objectMapper = new ObjectMapper();
        this.enabled = config.getBoolean(Keys.MESSAGE_BROKER_ENABLED, false);

        if (enabled) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                    config.getString(Keys.MESSAGE_BROKER_KAFKA_BOOTSTRAP_SERVERS, "localhost:9092"));
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 3);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            
            this.producer = new KafkaProducer<>(props);
            LOGGER.info("Message broker initialized with Kafka producer");
        } else {
            this.producer = null;
            LOGGER.info("Message broker disabled");
        }
    }

    /**
     * Publishes a user expiration event to the message broker.
     *
     * @param userId the ID of the user that expired
     * @param isExpiration true if this is an expiration event, false if it's a reminder
     */
    public void publishUserExpirationEvent(long userId, boolean isExpiration) {
        if (!enabled || producer == null) {
            return;
        }

        try {
            Span span = tracer.spanBuilder("messageBroker.publish.userExpiration").startSpan();
            try {
                Map<String, Object> event = new HashMap<>();
                event.put("userId", userId);
                event.put("type", isExpiration ? "expiration" : "reminder");
                event.put("timestamp", System.currentTimeMillis());

                String key = String.valueOf(userId);
                String value = objectMapper.writeValueAsString(event);

                ProducerRecord<String, String> record = new ProducerRecord<>(USER_EXPIRATION_TOPIC, key, value);
                
                // Inject the current context for distributed tracing
                GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                        .inject(Context.current(), record, SETTER);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.error("Failed to send user expiration event", exception);
                    } else {
                        LOGGER.debug("User expiration event sent to topic: {}, partition: {}, offset: {}",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });
            } finally {
                span.end();
            }
        } catch (Exception e) {
            LOGGER.error("Error publishing user expiration event", e);
        }
    }

    /**
     * Publishes a device expiration event to the message broker.
     *
     * @param deviceId the ID of the device that expired
     * @param isExpiration true if this is an expiration event, false if it's a reminder
     */
    public void publishDeviceExpirationEvent(long deviceId, boolean isExpiration) {
        if (!enabled || producer == null) {
            return;
        }

        try {
            Span span = tracer.spanBuilder("messageBroker.publish.deviceExpiration").startSpan();
            try {
                Map<String, Object> event = new HashMap<>();
                event.put("deviceId", deviceId);
                event.put("type", isExpiration ? "expiration" : "reminder");
                event.put("timestamp", System.currentTimeMillis());

                String key = String.valueOf(deviceId);
                String value = objectMapper.writeValueAsString(event);

                ProducerRecord<String, String> record = new ProducerRecord<>(DEVICE_EXPIRATION_TOPIC, key, value);
                
                // Inject the current context for distributed tracing
                GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                        .inject(Context.current(), record, SETTER);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.error("Failed to send device expiration event", exception);
                    } else {
                        LOGGER.debug("Device expiration event sent to topic: {}, partition: {}, offset: {}",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });
            } finally {
                span.end();
            }
        } catch (Exception e) {
            LOGGER.error("Error publishing device expiration event", e);
        }
    }

    /**
     * Closes the message broker producer.
     */
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
            LOGGER.info("Message broker producer closed");
        }
    }
}