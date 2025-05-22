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
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.MessageBrokerManager;
import org.traccar.messaging.MessageHeaders;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class EventForwarderAmqp implements EventForwarder {

    private final MessageBrokerManager messageBrokerManager;
    private final ObjectMapper objectMapper;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final TextMapPropagator propagator;
    private final Timer forwardTimer;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final String exchange;
    private final String topic;

    public EventForwarderAmqp(
            Config config,
            ObjectMapper objectMapper,
            CircuitBreakerRegistry circuitBreakerRegistry,
            MessageBrokerManager messageBrokerManager,
            Tracer tracer,
            TextMapPropagator propagator,
            MeterRegistry meterRegistry) {
        
        this.objectMapper = objectMapper;
        this.messageBrokerManager = messageBrokerManager;
        this.tracer = tracer;
        this.propagator = propagator;
        
        // Get configuration values
        String connectionUrl = config.getString(Keys.EVENT_FORWARD_URL);
        this.exchange = config.getString(Keys.EVENT_FORWARD_EXCHANGE);
        this.topic = config.getString(Keys.EVENT_FORWARD_TOPIC);
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(
                "eventForwarderAmqp", circuitBreakerConfig);
        
        // Initialize metrics
        this.forwardTimer = Timer.builder("event.forward.amqp.duration")
                .description("Time taken to forward events via AMQP")
                .register(meterRegistry);
        
        this.successCounter = Counter.builder("event.forward.amqp.success")
                .description("Number of successfully forwarded events via AMQP")
                .register(meterRegistry);
        
        this.failureCounter = Counter.builder("event.forward.amqp.failure")
                .description("Number of failed event forwarding attempts via AMQP")
                .register(meterRegistry);
    }

    @Override
    public void forward(EventData eventData, ResultHandler resultHandler) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("EventForwarderAmqp.forward")
                .setSpanKind(SpanKind.PRODUCER)
                .startSpan();
        
        try {
            // Use the circuit breaker to protect against broker failures
            circuitBreaker.executeSupplier(() -> {
                Timer.Sample sample = Timer.start();
                try {
                    // Serialize the event data to JSON
                    String value = objectMapper.writeValueAsString(eventData);
                    
                    // Create message headers with trace context
                    Map<String, String> headers = new HashMap<>();
                    propagator.inject(Context.current(), headers, (carrier, key, val) -> carrier.put(key, val));
                    
                    // Publish the message via the MessageBrokerManager
                    messageBrokerManager.publishMessage(exchange, topic, value, headers);
                    
                    // Record metrics for success
                    successCounter.increment();
                    sample.stop(forwardTimer);
                    
                    // Notify success
                    resultHandler.onResult(true, null);
                    return true;
                } catch (IOException e) {
                    // Record metrics for failure
                    failureCounter.increment();
                    sample.stop(forwardTimer);
                    
                    // Notify failure
                    resultHandler.onResult(false, e);
                    throw new RuntimeException("Failed to forward event", e);
                }
            });
        } catch (Exception e) {
            // This will be called if the circuit breaker is open or if the execution fails
            failureCounter.increment();
            resultHandler.onResult(false, e);
        } finally {
            // End the span
            span.end();
        }
    }
}