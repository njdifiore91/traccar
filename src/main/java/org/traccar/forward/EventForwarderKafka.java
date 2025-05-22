/*
 * Copyright 2022 - 2025 Anton Tananaev (anton@traccar.org)
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.MessageBrokerManager;
import org.traccar.messaging.MessageHeaders;
import org.traccar.messaging.MessageProducer;

import javax.inject.Inject;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class EventForwarderKafka implements EventForwarder {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventForwarderKafka.class);

    private final MessageProducer producer;
    private final ObjectMapper objectMapper;
    private final String topic;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Timer sendTimer;
    private final Counter successCounter;
    private final Counter failureCounter;

    @Inject
    public EventForwarderKafka(
            Config config,
            ObjectMapper objectMapper,
            MessageBrokerManager messageBrokerManager,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.topic = config.getString(Keys.EVENT_FORWARD_TOPIC);

        // Initialize metrics
        this.sendTimer = Timer.builder("event.forward.kafka.send.time")
                .description("Time taken to send events to Kafka")
                .register(meterRegistry);
        this.successCounter = Counter.builder("event.forward.kafka.success")
                .description("Number of successfully forwarded events")
                .register(meterRegistry);
        this.failureCounter = Counter.builder("event.forward.kafka.failure")
                .description("Number of failed event forwarding attempts")
                .register(meterRegistry);

        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to open circuit
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .recordExceptions(Exception.class)
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("eventForwarderKafka");

        // Get producer from MessageBrokerManager
        this.producer = messageBrokerManager.getProducer("event-forwarder");

        LOGGER.info("Initialized Kafka event forwarder with topic: {}", topic);
    }

    @Override
    public void forward(EventData eventData, ResultHandler resultHandler) {
        // Create a span for tracing
        Span span = tracer.spanBuilder("event.forward.kafka")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("messaging.system", "kafka")
                .setAttribute("messaging.destination", topic)
                .setAttribute("messaging.destination_kind", "topic")
                .setAttribute("event.type", eventData.getEvent().getType())
                .setAttribute("device.id", String.valueOf(eventData.getDevice().getId()))
                .startSpan();

        // Use the span as the current context
        try (Scope scope = span.makeCurrent()) {
            Timer.Sample sample = Timer.start(meterRegistry);
            
            try {
                // Execute with circuit breaker
                circuitBreaker.executeSupplier(() -> {
                    try {
                        String key = Long.toString(eventData.getDevice().getId());
                        String value = objectMapper.writeValueAsString(eventData);
                        
                        // Create message headers with trace context
                        Map<String, String> headers = new HashMap<>();
                        headers.put(MessageHeaders.DEVICE_ID, key);
                        headers.put(MessageHeaders.EVENT_TYPE, eventData.getEvent().getType());
                        
                        // Send message asynchronously
                        CompletableFuture<Void> future = producer.sendAsync(topic, key, value, headers);
                        
                        // Handle completion
                        future.whenComplete((result, exception) -> {
                            if (exception != null) {
                                span.setStatus(StatusCode.ERROR, exception.getMessage());
                                span.recordException(exception);
                                failureCounter.increment();
                                resultHandler.onResult(false, exception);
                            } else {
                                span.setStatus(StatusCode.OK);
                                successCounter.increment();
                                resultHandler.onResult(true, null);
                            }
                            sample.stop(sendTimer);
                            span.end();
                        });
                        
                        return true;
                    } catch (JsonProcessingException e) {
                        span.setStatus(StatusCode.ERROR, e.getMessage());
                        span.recordException(e);
                        failureCounter.increment();
                        sample.stop(sendTimer);
                        span.end();
                        resultHandler.onResult(false, e);
                        return false;
                    }
                });
            } catch (Exception e) {
                LOGGER.error("Circuit breaker prevented message sending: {}", e.getMessage());
                span.setStatus(StatusCode.ERROR, "Circuit breaker open: " + e.getMessage());
                span.recordException(e);
                failureCounter.increment();
                sample.stop(sendTimer);
                span.end();
                resultHandler.onResult(false, e);
            }
        }
    }
}