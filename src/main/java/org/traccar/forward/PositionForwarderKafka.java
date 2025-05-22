/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.MessageBrokerManager;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PositionForwarderKafka implements PositionForwarder {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionForwarderKafka.class);

    private final MessageBrokerManager messageBrokerManager;
    private final ObjectMapper objectMapper;
    private final String topic;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final TextMapPropagator propagator;
    private final MeterRegistry meterRegistry;
    private final Timer sendTimer;
    private final Counter successCounter;
    private final Counter errorCounter;

    @Inject
    public PositionForwarderKafka(Config config, ObjectMapper objectMapper, MessageBrokerManager messageBrokerManager, 
                                  CircuitBreakerRegistry circuitBreakerRegistry, MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.messageBrokerManager = messageBrokerManager;
        this.topic = config.getString(Keys.FORWARD_TOPIC);
        this.meterRegistry = meterRegistry;
        
        // Initialize OpenTelemetry components
        this.tracer = GlobalOpenTelemetry.getTracer("org.traccar.forward.PositionForwarderKafka");
        this.propagator = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("positionForwarderKafka", circuitBreakerConfig);
        
        // Register metrics
        this.sendTimer = Timer.builder("traccar.position.forward.kafka.send")
                .description("Time taken to forward position data to Kafka")
                .tags(List.of(Tag.of("topic", topic)))
                .register(meterRegistry);
        
        this.successCounter = Counter.builder("traccar.position.forward.kafka.success")
                .description("Number of successfully forwarded positions to Kafka")
                .tags(List.of(Tag.of("topic", topic)))
                .register(meterRegistry);
        
        this.errorCounter = Counter.builder("traccar.position.forward.kafka.error")
                .description("Number of failed position forwards to Kafka")
                .tags(List.of(Tag.of("topic", topic)))
                .register(meterRegistry);
        
        // Register circuit breaker metrics
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    LOGGER.info("Circuit breaker state changed from {} to {}", 
                            event.getStateTransition().getFromState(),
                            event.getStateTransition().getToState());
                });
    }

    @Override
    public void forward(PositionData positionData, ResultHandler resultHandler) {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            // Execute with circuit breaker
            circuitBreaker.executeSupplier(() -> {
                try {
                    // Create span for tracing
                    Span span = tracer.spanBuilder("kafka.position.forward").startSpan();
                    
                    try (io.opentelemetry.context.Scope scope = span.makeCurrent()) {
                        String key = Long.toString(positionData.getDevice().getId());
                        String value = objectMapper.writeValueAsString(positionData);
                        
                        // Create record with headers for tracing context
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                        
                        // Inject tracing context into headers
                        injectTraceContext(record);
                        
                        // Send record to Kafka
                        messageBrokerManager.sendKafkaMessage(record).get(10, TimeUnit.SECONDS);
                        
                        // Record success metrics
                        successCounter.increment();
                        span.setAttribute("kafka.success", true);
                        
                        resultHandler.onResult(true, null);
                        return true;
                    } catch (Exception e) {
                        span.recordException(e);
                        span.setAttribute("kafka.success", false);
                        throw e;
                    } finally {
                        span.end();
                    }
                } catch (JsonProcessingException e) {
                    LOGGER.error("Error serializing position data", e);
                    errorCounter.increment();
                    resultHandler.onResult(false, e);
                    return false;
                } catch (Exception e) {
                    LOGGER.error("Error forwarding position to Kafka", e);
                    errorCounter.increment();
                    resultHandler.onResult(false, e);
                    return false;
                }
            });
        } catch (Exception e) {
            // This will be triggered if the circuit breaker is open
            LOGGER.error("Circuit breaker prevented forwarding position to Kafka", e);
            errorCounter.increment();
            resultHandler.onResult(false, e);
        } finally {
            sample.stop(sendTimer);
        }
    }
    
    private void injectTraceContext(ProducerRecord<String, String> record) {
        Context context = Context.current();
        propagator.inject(context, record, (carrier, key, value) -> {
            List<Header> headers = new ArrayList<>(carrier.headers().toArray());
            headers.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
            for (Header header : headers) {
                carrier.headers().add(header);
            }
        });
        
        // Add current span context as additional metadata
        SpanContext spanContext = Span.current().getSpanContext();
        if (spanContext.isValid()) {
            record.headers().add(new RecordHeader("traccar.trace.id", 
                    spanContext.getTraceId().getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("traccar.span.id", 
                    spanContext.getSpanId().getBytes(StandardCharsets.UTF_8)));
        }
    }
}