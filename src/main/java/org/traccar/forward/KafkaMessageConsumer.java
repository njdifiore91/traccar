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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka implementation of the MessageConsumer interface.
 */
public class KafkaMessageConsumer implements MessageBrokerManager.MessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageConsumer.class);

    private final String topic;
    private final String groupId;
    private final ObjectMapper objectMapper;
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final Map<Class<?>, MessageBrokerManager.MessageHandler<?>> handlers = new HashMap<>();
    private final MessageBrokerManager.AbstractMessageConsumer delegate;
    private final Config config;
    private volatile boolean running = false;
    private Thread consumerThread;

    public KafkaMessageConsumer(
            String topic,
            String groupId,
            Config config,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            Tracer tracer) {
        this.topic = topic;
        this.groupId = groupId;
        this.objectMapper = objectMapper;
        this.config = config;

        // Initialize Kafka consumer
        Properties properties = new Properties();
        properties.put("bootstrap.servers", config.getString(Keys.BROKER_URL));
        properties.put("group.id", groupId);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("enable.auto.commit", config.getBoolean(Keys.BROKER_AUTO_COMMIT, false));
        properties.put("auto.offset.reset", config.getString(Keys.BROKER_OFFSET_RESET, "earliest"));

        // Add any additional Kafka-specific properties from configuration
        for (Object key : config.getKeys()) {
            String keyStr = key.toString();
            if (keyStr.startsWith("kafka.")) {
                String kafkaKey = keyStr.substring("kafka.".length());
                properties.put(kafkaKey, config.getString(keyStr));
            }
        }

        this.kafkaConsumer = new KafkaConsumer<>(properties);
        
        // Create delegate for common functionality
        this.delegate = new MessageBrokerManager.AbstractMessageConsumer(
                topic, groupId, objectMapper, meterRegistry, tracer) {
            @Override
            protected String getBrokerType() {
                return "kafka";
            }
        };
    }

    @Override
    public <T> void subscribe(Class<T> messageType, MessageBrokerManager.MessageHandler<T> messageHandler) {
        handlers.put(messageType, messageHandler);
    }

    @Override
    public void start() {
        if (running) {
            return;
        }

        running = true;
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        consumerThread = new Thread(() -> {
            try {
                while (running) {
                    try {
                        ConsumerRecords<String, String> records = 
                                kafkaConsumer.poll(Duration.ofMillis(100));

                        for (ConsumerRecord<String, String> record : records) {
                            processRecord(record);
                        }

                        if (!config.getBoolean(Keys.BROKER_AUTO_COMMIT, false)) {
                            kafkaConsumer.commitSync();
                        }
                    } catch (Exception e) {
                        delegate.errorCounter.increment();
                        LOGGER.error("Error processing Kafka messages", e);
                    }
                }
            } finally {
                kafkaConsumer.close();
            }
        });

        consumerThread.setName("kafka-consumer-" + topic + "-" + groupId);
        consumerThread.start();
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        delegate.consumeCounter.increment();
        Timer.Sample sample = Timer.start();

        // Extract trace context from headers
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(header -> 
                headers.put(header.key(), new String(header.value())));

        // Extract trace context and create a span
        Context context = extractTraceContext(headers);
        Span span = delegate.tracer.spanBuilder("consume_" + topic)
                .setSpanKind(SpanKind.CONSUMER)
                .setParent(context)
                .startSpan();

        try {
            // Process the message with the appropriate handler
            for (Map.Entry<Class<?>, MessageBrokerManager.MessageHandler<?>> entry : handlers.entrySet()) {
                Class<?> messageType = entry.getKey();
                try {
                    Object message = objectMapper.readValue(record.value(), messageType);
                    processMessage(record.key(), message, headers, entry.getValue());
                } catch (Exception e) {
                    delegate.errorCounter.increment();
                    span.recordException(e);
                    LOGGER.error("Error processing message of type {}", messageType.getName(), e);
                }
            }
        } finally {
            sample.stop(delegate.processTimer);
            span.end();
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void processMessage(String key, Object message, Map<String, String> headers, 
                                   MessageBrokerManager.MessageHandler<?> handler) {
        ((MessageBrokerManager.MessageHandler<T>) handler).handle(key, (T) message, headers);
    }

    private Context extractTraceContext(Map<String, String> headers) {
        return delegate.tracer.getOpenTelemetry().getPropagators().getTextMapPropagator()
                .extract(Context.current(), headers, 
                        (carrier, key) -> carrier.get(key));
    }

    @Override
    public void stop() {
        running = false;
        if (consumerThread != null) {
            consumerThread.interrupt();
            try {
                consumerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() {
        stop();
        kafkaConsumer.close();
    }
}