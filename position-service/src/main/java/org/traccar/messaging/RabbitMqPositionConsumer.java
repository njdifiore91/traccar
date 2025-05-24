/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.impl.OpenTelemetryMetricsCollector;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.semconv.SemanticAttributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * RabbitMQ implementation of the PositionConsumer interface for the Position Processing Service.
 * Configures and manages RabbitMQ consumer instances, handles message deserialization,
 * and processes raw position messages.
 */
public class RabbitMqPositionConsumer implements PositionConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqPositionConsumer.class);

    private final ConnectionFactory connectionFactory;
    private final String exchangeName;
    private final String queueName;
    private final String routingKey;
    private final String deadLetterExchange;
    private final int prefetchCount;
    private final boolean autoAck;
    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;

    private Connection connection;
    private Channel channel;
    private Consumer<PositionMessage> messageHandler;

    /**
     * Constructs a new RabbitMqPositionConsumer with the specified configuration.
     *
     * @param config Configuration for RabbitMQ connection and consumer settings
     * @param openTelemetry OpenTelemetry instance for distributed tracing
     */
    public RabbitMqPositionConsumer(Config config, OpenTelemetry openTelemetry) {
        this.openTelemetry = openTelemetry;
        this.tracer = openTelemetry.getTracer("org.traccar.messaging.RabbitMqPositionConsumer");

        connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(config.getString(Keys.RABBITMQ_CONNECTION_URL));
        connectionFactory.setAutomaticRecoveryEnabled(true);
        connectionFactory.setTopologyRecoveryEnabled(true);
        
        // Configure metrics collection with OpenTelemetry
        connectionFactory.setMetricsCollector(
                new OpenTelemetryMetricsCollector(openTelemetry, "rabbitmq", 
                        Attributes.builder()
                                .put("service.name", "position-service")
                                .put("messaging.system", "rabbitmq")
                                .build()));

        exchangeName = config.getString(Keys.RABBITMQ_POSITION_EXCHANGE, MessageConstants.POSITION_EXCHANGE);
        queueName = config.getString(Keys.RABBITMQ_POSITION_QUEUE, MessageConstants.POSITION_QUEUE);
        routingKey = config.getString(Keys.RABBITMQ_POSITION_ROUTING_KEY, MessageConstants.POSITION_ROUTING_KEY);
        deadLetterExchange = config.getString(Keys.RABBITMQ_DEAD_LETTER_EXCHANGE, MessageConstants.DEAD_LETTER_EXCHANGE);
        prefetchCount = config.getInteger(Keys.RABBITMQ_PREFETCH_COUNT, 10);
        autoAck = config.getBoolean(Keys.RABBITMQ_AUTO_ACK, false);
    }

    /**
     * Initializes the RabbitMQ connection, channel, and sets up the exchange, queue, and consumer.
     *
     * @throws MessageException if there is an error initializing the consumer
     */
    @Override
    public void initialize() throws MessageException {
        try {
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();

            // Set up dead letter exchange for unprocessable messages
            Map<String, Object> args = new HashMap<>();
            args.put("x-dead-letter-exchange", deadLetterExchange);
            args.put("x-dead-letter-routing-key", routingKey + ".dead");

            // Declare exchanges and queues
            channel.exchangeDeclare(exchangeName, "direct", true);
            channel.exchangeDeclare(deadLetterExchange, "direct", true);
            channel.queueDeclare(queueName, true, false, false, args);
            channel.queueDeclare(queueName + ".dead", true, false, false, null);

            // Bind queues to exchanges
            channel.queueBind(queueName, exchangeName, routingKey);
            channel.queueBind(queueName + ".dead", deadLetterExchange, routingKey + ".dead");

            // Set prefetch count for load balancing
            channel.basicQos(prefetchCount);

            LOGGER.info("RabbitMQ consumer initialized for exchange: {}, queue: {}, routing key: {}",
                    exchangeName, queueName, routingKey);
        } catch (Exception e) {
            throw new MessageException("Failed to initialize RabbitMQ consumer", e);
        }
    }

    /**
     * Starts consuming messages from the RabbitMQ queue and processes them using the provided message handler.
     *
     * @param handler The consumer function that will process the received position messages
     * @throws MessageException if there is an error starting the consumer
     */
    @Override
    public void subscribe(Consumer<PositionMessage> handler) throws MessageException {
        if (channel == null) {
            throw new MessageException("RabbitMQ consumer not initialized");
        }

        this.messageHandler = handler;

        try {
            channel.basicConsume(queueName, autoAck, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    processMessage(envelope, properties, body);
                }
            });
            LOGGER.info("Started consuming messages from queue: {}", queueName);
        } catch (IOException e) {
            throw new MessageException("Failed to subscribe to RabbitMQ queue", e);
        }
    }

    /**
     * Processes a received message, extracts tracing context, deserializes the message,
     * and passes it to the message handler.
     *
     * @param envelope The message envelope containing delivery information
     * @param properties The message properties
     * @param body The message body as a byte array
     * @throws IOException if there is an error processing the message
     */
    private void processMessage(Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        // Extract tracing context from message headers
        Context extractedContext = openTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), properties.getHeaders(), new TextMapGetter<Map<String, Object>>() {
                    @Override
                    public Iterable<String> keys(Map<String, Object> carrier) {
                        return carrier.keySet();
                    }

                    @Override
                    public String get(Map<String, Object> carrier, String key) {
                        Object value = carrier.get(key);
                        return value != null ? value.toString() : null;
                    }
                });

        // Create a span for message processing
        SpanBuilder spanBuilder = tracer.spanBuilder("process_position_message")
                .setSpanKind(SpanKind.CONSUMER)
                .setParent(extractedContext);

        // Add messaging attributes to the span
        AttributesBuilder attributesBuilder = Attributes.builder()
                .put(SemanticAttributes.MESSAGING_SYSTEM, "rabbitmq")
                .put(SemanticAttributes.MESSAGING_DESTINATION_NAME, exchangeName + ":" + routingKey + ":" + queueName)
                .put(SemanticAttributes.MESSAGING_OPERATION, "receive")
                .put(SemanticAttributes.MESSAGING_MESSAGE_ID, envelope.getDeliveryTag() + "")
                .put(SemanticAttributes.MESSAGING_DESTINATION_KIND, "queue");

        if (properties.getMessageId() != null) {
            attributesBuilder.put(SemanticAttributes.MESSAGING_MESSAGE_ID, properties.getMessageId());
        }

        if (properties.getCorrelationId() != null) {
            attributesBuilder.put("messaging.correlation_id", properties.getCorrelationId());
        }

        Span span = spanBuilder.setAllAttributes(attributesBuilder.build()).startSpan();

        try (Scope scope = span.makeCurrent()) {
            // Deserialize the message
            PositionMessage positionMessage;
            try {
                positionMessage = PositionMessage.parseFrom(body);
                span.setAttribute("position.device_id", positionMessage.getDeviceId());
            } catch (Exception e) {
                LOGGER.error("Failed to deserialize position message", e);
                span.setStatus(StatusCode.ERROR, "Failed to deserialize position message");
                span.recordException(e);
                
                // Reject the message and don't requeue if it can't be deserialized
                if (!autoAck) {
                    channel.basicReject(envelope.getDeliveryTag(), false);
                }
                return;
            }

            // Process the message with the handler
            try {
                messageHandler.accept(positionMessage);
                
                // Acknowledge the message if auto-ack is disabled
                if (!autoAck) {
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            } catch (Exception e) {
                LOGGER.error("Failed to process position message", e);
                span.setStatus(StatusCode.ERROR, "Failed to process position message");
                span.recordException(e);
                
                // Reject and requeue the message for retry if processing fails
                if (!autoAck) {
                    // Requeue the message for retry
                    channel.basicNack(envelope.getDeliveryTag(), false, true);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error in message processing", e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            
            if (!autoAck) {
                // Reject and don't requeue if there's an unexpected error
                channel.basicReject(envelope.getDeliveryTag(), false);
            }
        } finally {
            span.end();
        }
    }

    /**
     * Pauses message consumption from the queue.
     *
     * @throws MessageException if there is an error pausing the consumer
     */
    @Override
    public void pause() throws MessageException {
        try {
            if (channel != null && channel.isOpen()) {
                // Set prefetch count to 0 to stop receiving new messages
                channel.basicQos(0);
                LOGGER.info("Paused consuming messages from queue: {}", queueName);
            }
        } catch (IOException e) {
            throw new MessageException("Failed to pause RabbitMQ consumer", e);
        }
    }

    /**
     * Resumes message consumption from the queue.
     *
     * @throws MessageException if there is an error resuming the consumer
     */
    @Override
    public void resume() throws MessageException {
        try {
            if (channel != null && channel.isOpen()) {
                // Restore the original prefetch count
                channel.basicQos(prefetchCount);
                LOGGER.info("Resumed consuming messages from queue: {}", queueName);
            }
        } catch (IOException e) {
            throw new MessageException("Failed to resume RabbitMQ consumer", e);
        }
    }

    /**
     * Closes the RabbitMQ channel and connection.
     */
    @Override
    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } catch (IOException | TimeoutException e) {
            LOGGER.warn("Error closing RabbitMQ channel", e);
        }

        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (IOException e) {
            LOGGER.warn("Error closing RabbitMQ connection", e);
        }

        LOGGER.info("RabbitMQ consumer closed");
    }
}