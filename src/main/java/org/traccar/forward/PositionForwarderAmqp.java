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

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHeaders;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageProducerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PositionForwarderAmqp implements PositionForwarder, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionForwarderAmqp.class);

    private final MessageProducer messageProducer;
    private final ObjectMapper objectMapper;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Timer forwardTimer;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final String exchange;
    private final String topic;

    @Inject
    public PositionForwarderAmqp(
            Config config,
            ObjectMapper objectMapper,
            MessageProducerFactory messageProducerFactory,
            CircuitBreakerRegistry circuitBreakerRegistry,
            Tracer tracer,
            MeterRegistry meterRegistry) {

        this.objectMapper = objectMapper;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;

        // Get configuration values
        String connectionUrl = config.getString(Keys.FORWARD_URL);
        exchange = config.getString(Keys.FORWARD_EXCHANGE);
        topic = config.getString(Keys.FORWARD_TOPIC);

        // Create metrics
        forwardTimer = Timer.builder("position.forward.time")
                .description("Time taken to forward position data")
                .tag("forwarder", "amqp")
                .tag("exchange", exchange)
                .tag("topic", topic)
                .register(meterRegistry);

        successCounter = Counter.builder("position.forward.success")
                .description("Number of successfully forwarded positions")
                .tag("forwarder", "amqp")
                .tag("exchange", exchange)
                .tag("topic", topic)
                .register(meterRegistry);

        failureCounter = Counter.builder("position.forward.failure")
                .description("Number of failed position forwarding attempts")
                .tag("forwarder", "amqp")
                .tag("exchange", exchange)
                .tag("topic", topic)
                .register(meterRegistry);

        // Create circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .recordExceptions(IOException.class, RuntimeException.class)
                .build();

        circuitBreaker = circuitBreakerRegistry.circuitBreaker(
                "positionForwarderAmqp", circuitBreakerConfig);

        // Create message producer
        messageProducer = messageProducerFactory.createRabbitMQProducer(
                connectionUrl, exchange, true);

        LOGGER.info("Initialized AMQP position forwarder for exchange: {}, topic: {}", exchange, topic);
    }

    @Override
    public void forward(PositionData positionData, ResultHandler resultHandler) {
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("forward.position.amqp")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("exchange", exchange)
                .setAttribute("topic", topic)
                .setAttribute("deviceId", String.valueOf(positionData.getDeviceId()))
                .startSpan();

        // Use timer to measure forwarding time
        Timer.Sample sample = Timer.start(meterRegistry);

        try (Scope scope = span.makeCurrent()) {
            // Execute with circuit breaker
            circuitBreaker.executeCheckedSupplier(() -> {
                try {
                    // Serialize position data
                    String value = objectMapper.writeValueAsString(positionData);

                    // Create message headers with tracing context
                    Map<String, Object> headers = new HashMap<>();
                    headers.put(MessageHeaders.CONTENT_TYPE, "application/json");
                    headers.put(MessageHeaders.CORRELATION_ID, span.getSpanContext().getTraceId());

                    // Create message envelope
                    MessageEnvelope envelope = new MessageEnvelope(value.getBytes(), headers, topic);

                    // Send message
                    messageProducer.send(envelope);

                    // Record success
                    successCounter.increment();
                    resultHandler.onResult(true, null);
                    return true;
                } catch (Exception e) {
                    // Record failure
                    failureCounter.increment();
                    span.recordException(e);
                    resultHandler.onResult(false, e);
                    throw e;
                } finally {
                    // Record timing
                    sample.stop(forwardTimer);
                }
            });
        } catch (Throwable e) {
            // This will be reached if the circuit breaker is open or if an exception
            // is thrown during execution
            LOGGER.warn("Failed to forward position data via AMQP", e);
            span.recordException(e);
            resultHandler.onResult(false, e instanceof Exception ? (Exception) e : new RuntimeException(e));
        } finally {
            span.end();
        }
    }

    @Override
    public void close() throws Exception {
        if (messageProducer instanceof AutoCloseable) {
            ((AutoCloseable) messageProducer).close();
            LOGGER.info("Closed AMQP position forwarder resources");
        }
    }
}