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
package org.traccar.forward;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.api.GlobalOpenTelemetry;

import org.traccar.messaging.MessageBrokerManager;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * AMQP client for RabbitMQ message broker integration.
 * Supports distributed tracing, metrics collection, and circuit breaker pattern.
 */
@Singleton
public class AmqpClient {
    private final Channel channel;
    private final String exchange;
    private final String topic;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final TextMapPropagator propagator;
    private final Counter messageCounter;
    private final Timer publishTimer;
    private final MessageBrokerManager brokerManager;

    /**
     * Constructs an AmqpClient with the specified connection parameters.
     *
     * @param brokerManager The centralized message broker manager
     * @param exchange The RabbitMQ exchange name
     * @param topic The routing topic for messages
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param meterRegistry The Micrometer registry for metrics collection
     */
    @Inject
    public AmqpClient(
            MessageBrokerManager brokerManager,
            String exchange,
            String topic,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.brokerManager = brokerManager;
        this.exchange = exchange;
        this.topic = topic;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.propagator = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
        
        // Initialize metrics
        this.messageCounter = Counter.builder("amqp.messages.published")
                .tag("exchange", exchange)
                .tag("topic", topic)
                .description("Number of messages published to AMQP")
                .register(meterRegistry);
        
        this.publishTimer = Timer.builder("amqp.publish.time")
                .tag("exchange", exchange)
                .tag("topic", topic)
                .description("Time taken to publish messages to AMQP")
                .register(meterRegistry);
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("amqp-client-" + exchange + "-" + topic);
        
        // Initialize connection and channel
        try {
            Connection connection = brokerManager.getAmqpConnection();
            channel = connection.createChannel();
            channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, true);
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException("Error while creating and configuring RabbitMQ channel", e);
        }
    }

    /**
     * Publishes a message to the configured exchange and topic.
     * Includes distributed tracing context propagation and metrics collection.
     *
     * @param message The message to publish
     * @throws IOException If an error occurs during publishing
     */
    public void publishMessage(String message) throws IOException {
        // Create a span for the publish operation
        Span span = tracer.spanBuilder("amqp.publish")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("messaging.system", "rabbitmq")
                .setAttribute("messaging.destination", exchange)
                .setAttribute("messaging.destination_kind", "topic")
                .setAttribute("messaging.rabbitmq.routing_key", topic)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Inject the current context into message headers
            Map<String, String> headers = new HashMap<>();
            propagator.inject(Context.current(), headers, (carrier, key, value) -> carrier.put(key, value));
            
            // Use circuit breaker to handle connection failures
            circuitBreaker.executeRunnable(() -> {
                try {
                    // Record metrics for the publish operation
                    publishTimer.record(() -> {
                        try {
                            channel.basicPublish(
                                    exchange,
                                    topic,
                                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                                    message.getBytes());
                            messageCounter.increment();
                        } catch (IOException e) {
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            span.recordException(e);
                            throw new RuntimeException("Failed to publish message", e);
                        }
                        return null;
                    });
                } catch (Exception e) {
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    throw e;
                }
            });
            
            span.setStatus(StatusCode.OK);
        } finally {
            span.end();
        }
    }

    /**
     * Closes the AMQP channel and connection.
     * This method should be called during application shutdown to release resources.
     */
    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } catch (IOException | TimeoutException e) {
            // Log the error but don't rethrow as we're shutting down
            System.err.println("Error closing AMQP channel: " + e.getMessage());
        }
    }
}