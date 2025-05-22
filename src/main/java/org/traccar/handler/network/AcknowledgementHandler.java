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
package org.traccar.handler.network;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.inject.Inject;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Handler for managing acknowledgements of network messages.
 * Supports both direct processing and asynchronous processing via message broker.
 * Implements distributed tracing, metrics collection, and circuit breaker patterns.
 */
public class AcknowledgementHandler extends ChannelOutboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AcknowledgementHandler.class);

    /**
     * Base interface for acknowledgement events.
     */
    public interface Event {
        /**
         * Get the correlation ID for this event.
         * @return correlation ID string
         */
        String getCorrelationId();
    }

    /**
     * Event indicating a message has been received.
     */
    public static class EventReceived implements Event {
        private final String correlationId;

        public EventReceived() {
            this.correlationId = UUID.randomUUID().toString();
        }

        public EventReceived(String correlationId) {
            this.correlationId = correlationId;
        }

        @Override
        public String getCorrelationId() {
            return correlationId;
        }
    }

    /**
     * Event indicating a message has been decoded into objects.
     */
    public static class EventDecoded implements Event {
        private final Collection<Object> objects;
        private final String correlationId;

        public EventDecoded(Collection<Object> objects, String correlationId) {
            this.objects = objects;
            this.correlationId = correlationId;
        }

        public Collection<Object> getObjects() {
            return objects;
        }

        @Override
        public String getCorrelationId() {
            return correlationId;
        }
    }

    /**
     * Event indicating an object has been handled.
     */
    public static class EventHandled implements Event {
        private final Object object;
        private final String correlationId;

        public EventHandled(Object object, String correlationId) {
            this.object = object;
            this.correlationId = correlationId;
        }

        public Object getObject() {
            return object;
        }

        @Override
        public String getCorrelationId() {
            return correlationId;
        }
    }

    /**
     * Entry for storing messages and promises in the queue.
     */
    private static final class Entry {
        private final Object message;
        private final ChannelPromise promise;
        private final String correlationId;

        private Entry(Object message, ChannelPromise promise, String correlationId) {
            this.message = message;
            this.promise = promise;
            this.correlationId = correlationId;
        }

        public Object getMessage() {
            return message;
        }

        public ChannelPromise getPromise() {
            return promise;
        }

        public String getCorrelationId() {
            return correlationId;
        }
    }

    private List<Entry> queue;
    private final Set<Object> waiting = new HashSet<>();
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Timer processingTimer;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final MessageBrokerService messageBrokerService;

    /**
     * Constructs a new AcknowledgementHandler with the specified dependencies.
     *
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     * @param messageBrokerService Service for asynchronous message processing
     */
    @Inject
    public AcknowledgementHandler(Tracer tracer, MeterRegistry meterRegistry, MessageBrokerService messageBrokerService) {
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.messageBrokerService = messageBrokerService;

        // Initialize metrics
        this.processingTimer = Timer.builder("acknowledgement.processing.time")
                .description("Time taken to process acknowledgements")
                .register(meterRegistry);
        this.successCounter = Counter.builder("acknowledgement.success")
                .description("Number of successful acknowledgements")
                .register(meterRegistry);
        this.failureCounter = Counter.builder("acknowledgement.failure")
                .description("Number of failed acknowledgements")
                .register(meterRegistry);

        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofMillis(1000))
                .permittedNumberOfCallsInHalfOpenState(2)
                .slidingWindowSize(10)
                .build();
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("acknowledgementHandler");
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // Create a span for this write operation
        Span span = tracer.spanBuilder("acknowledgement.write")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();

        String correlationId = null;
        if (msg instanceof Event) {
            correlationId = ((Event) msg).getCorrelationId();
            span.setAttribute("correlationId", correlationId);
        }

        // Add correlation ID to MDC for logging
        if (correlationId != null) {
            MDC.put("correlationId", correlationId);
        }

        try (Scope scope = span.makeCurrent()) {
            // Execute the write operation with circuit breaker protection
            executeWithCircuitBreaker(() -> {
                Timer.Sample sample = Timer.start(meterRegistry);
                try {
                    processWrite(ctx, msg, promise, correlationId);
                    sample.stop(processingTimer);
                    successCounter.increment();
                    return CompletableFuture.completedFuture(null);
                } catch (Exception e) {
                    sample.stop(processingTimer);
                    failureCounter.increment();
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    LOGGER.error("Error processing write operation", e);
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
            if (correlationId != null) {
                MDC.remove("correlationId");
            }
        }
    }

    /**
     * Process the write operation with the given message and promise.
     *
     * @param ctx the channel handler context
     * @param msg the message to write
     * @param promise the channel promise
     * @param correlationId the correlation ID for tracing
     * @throws Exception if an error occurs during processing
     */
    private void processWrite(ChannelHandlerContext ctx, Object msg, ChannelPromise promise, String correlationId) throws Exception {
        List<Entry> output = new LinkedList<>();
        synchronized (this) {
            if (msg instanceof Event) {
                if (msg instanceof EventReceived) {
                    LOGGER.debug("Event received [correlationId={}]", correlationId);
                    if (queue == null) {
                        queue = new LinkedList<>();
                    }
                    
                    // Publish event to message broker for asynchronous processing
                    messageBrokerService.publishEvent("events.received", msg);
                    
                } else if (msg instanceof EventDecoded event) {
                    LOGGER.debug("Event decoded {} [correlationId={}]", event.getObjects().size(), correlationId);
                    waiting.addAll(event.getObjects());
                    
                    // Publish event to message broker for asynchronous processing
                    messageBrokerService.publishEvent("events.decoded", msg);
                    
                } else if (msg instanceof EventHandled event) {
                    LOGGER.debug("Event handled [correlationId={}]", correlationId);
                    waiting.remove(event.getObject());
                    
                    // Publish event to message broker for asynchronous processing
                    messageBrokerService.publishEvent("events.handled", msg);
                }
                
                if (!(msg instanceof EventReceived) && waiting.isEmpty()) {
                    output.addAll(queue);
                    queue = null;
                }
            } else if (queue != null) {
                LOGGER.debug("Message queued [correlationId={}]", correlationId);
                queue.add(new Entry(msg, promise, correlationId));
            } else {
                LOGGER.debug("Message sent [correlationId={}]", correlationId);
                output.add(new Entry(msg, promise, correlationId));
            }
        }
        
        // Create a span for sending messages
        Span sendSpan = tracer.spanBuilder("acknowledgement.send_messages")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("messageCount", output.size())
                .startSpan();
        
        try (Scope scope = sendSpan.makeCurrent()) {
            for (Entry entry : output) {
                // Add correlation ID to context for each message
                if (entry.getCorrelationId() != null) {
                    MDC.put("correlationId", entry.getCorrelationId());
                }
                
                try {
                    ctx.write(entry.getMessage(), entry.getPromise());
                } finally {
                    if (entry.getCorrelationId() != null) {
                        MDC.remove("correlationId");
                    }
                }
            }
        } finally {
            sendSpan.end();
        }
    }

    /**
     * Execute an operation with circuit breaker protection.
     *
     * @param supplier the operation to execute
     * @return the result of the operation
     */
    private <T> T executeWithCircuitBreaker(Supplier<T> supplier) {
        return circuitBreaker.executeSupplier(supplier);
    }

    /**
     * Service interface for message broker integration.
     */
    public interface MessageBrokerService {
        /**
         * Publish an event to the specified topic.
         *
         * @param topic the topic to publish to
         * @param event the event to publish
         */
        void publishEvent(String topic, Object event);
    }
}