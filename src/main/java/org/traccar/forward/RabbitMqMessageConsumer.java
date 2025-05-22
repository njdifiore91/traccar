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
import com.rabbitmq.client.AMQP;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.util.HashMap;
import java.util.Map;

/**
 * RabbitMQ implementation of the MessageConsumer interface.
 */
public class RabbitMqMessageConsumer implements MessageBrokerManager.MessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqMessageConsumer.class);

    private final AmqpClient amqpClient;
    private final ObjectMapper objectMapper;
    private final Map<Class<?>, MessageBrokerManager.MessageHandler<?>> handlers = new HashMap<>();
    private final MessageBrokerManager.AbstractMessageConsumer delegate;
    private volatile boolean running = false;

    public RabbitMqMessageConsumer(
            String topic,
            String groupId,
            Config config,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            Tracer tracer) {
        this.objectMapper = objectMapper;

        String connectionUrl = config.getString(Keys.BROKER_URL);
        String exchange = config.getString(Keys.BROKER_EXCHANGE, "traccar");
        this.amqpClient = new AmqpClient(connectionUrl, exchange, topic);
        
        // Create delegate for common functionality
        this.delegate = new MessageBrokerManager.AbstractMessageConsumer(
                topic, groupId, objectMapper, meterRegistry, tracer) {
            @Override
            protected String getBrokerType() {
                return "rabbitmq";
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
        amqpClient.setMessageHandler((message, properties) -> {
            delegate.consumeCounter.increment();
            Timer.Sample sample = Timer.start();

            // Extract headers from AMQP properties
            Map<String, String> headers = new HashMap<>();
            if (properties.getHeaders() != null) {
                for (Map.Entry<String, Object> entry : properties.getHeaders().entrySet()) {
                    if (entry.getValue() instanceof byte[]) {
                        headers.put(entry.getKey(), new String((byte[]) entry.getValue()));
                    } else if (entry.getValue() != null) {
                        headers.put(entry.getKey(), entry.getValue().toString());
                    }
                }
            }

            // Extract trace context and create a span
            Context context = extractTraceContext(headers);
            Span span = delegate.tracer.spanBuilder("consume_" + delegate.topic)
                    .setSpanKind(SpanKind.CONSUMER)
                    .setParent(context)
                    .startSpan();

            try {
                // Process the message with the appropriate handler
                for (Map.Entry<Class<?>, MessageBrokerManager.MessageHandler<?>> entry : handlers.entrySet()) {
                    Class<?> messageType = entry.getKey();
                    try {
                        Object messageObj = objectMapper.readValue(message, messageType);
                        String key = properties.getCorrelationId();
                        processMessage(key, messageObj, headers, entry.getValue());
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
        });

        amqpClient.startConsuming(delegate.groupId);
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
        amqpClient.stopConsuming();
    }

    @Override
    public void close() {
        stop();
        amqpClient.close();
    }
}