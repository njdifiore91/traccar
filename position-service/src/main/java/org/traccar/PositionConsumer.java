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
package org.traccar;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.messaging.MessageConsumer;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHandler;
import org.traccar.messaging.MessageHeaders;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageProducerFactory;
import org.traccar.messaging.MessageConsumerFactory;
import org.traccar.model.Position;
import org.traccar.proto.PositionOuterClass;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Consumes raw position messages from the message broker, implements consumer group
 * for load distribution, and forwards positions to the processing pipeline with
 * error handling and retry logic.
 */
@Singleton
public class PositionConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionConsumer.class);
    private static final String CONSUMER_GROUP = "position-processors";
    private static final String RAW_POSITIONS_TOPIC = "raw-positions";
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_BACKOFF_MS = 1000; // 1 second initial backoff

    private final Config config;
    private final ProcessingHandler processingHandler;
    private final MessageConsumerFactory consumerFactory;
    private final MessageProducerFactory producerFactory;
    private final Tracer tracer;
    private final TransactionOutboxManager outboxManager;
    private final CircuitBreakerConfig circuitBreakerConfig;

    private MessageConsumer<PositionOuterClass.Position> consumer;
    private MessageProducer<Position> deadLetterProducer;

    @Inject
    public PositionConsumer(
            Config config,
            ProcessingHandler processingHandler,
            MessageConsumerFactory consumerFactory,
            MessageProducerFactory producerFactory,
            Tracer tracer,
            TransactionOutboxManager outboxManager,
            CircuitBreakerConfig circuitBreakerConfig) {
        this.config = config;
        this.processingHandler = processingHandler;
        this.consumerFactory = consumerFactory;
        this.producerFactory = producerFactory;
        this.tracer = tracer;
        this.outboxManager = outboxManager;
        this.circuitBreakerConfig = circuitBreakerConfig;
    }

    /**
     * Initializes the position consumer, setting up the message broker connection
     * and subscribing to the raw positions topic.
     */
    @PostConstruct
    public void init() {
        LOGGER.info("Initializing Position Consumer with consumer group: {}", CONSUMER_GROUP);
        
        // Create the consumer with the consumer group ID for load distribution
        consumer = consumerFactory.createConsumer(
                PositionOuterClass.Position.class,
                RAW_POSITIONS_TOPIC,
                CONSUMER_GROUP,
                new PositionMessageHandler());
        
        // Create a producer for the dead letter queue
        deadLetterProducer = producerFactory.createProducer(
                Position.class,
                RAW_POSITIONS_TOPIC + ".dlq");
        
        LOGGER.info("Position Consumer initialized and subscribed to topic: {}", RAW_POSITIONS_TOPIC);
    }

    /**
     * Cleans up resources when the application is shutting down.
     */
    @PreDestroy
    public void destroy() {
        LOGGER.info("Shutting down Position Consumer");
        if (consumer != null) {
            consumer.close();
        }
        if (deadLetterProducer != null) {
            deadLetterProducer.close();
        }
    }

    /**
     * Handler for processing position messages from the message broker.
     */
    private class PositionMessageHandler implements MessageHandler<PositionOuterClass.Position> {

        @Override
        public CompletableFuture<Void> handle(MessageEnvelope<PositionOuterClass.Position> messageEnvelope) {
            PositionOuterClass.Position protoPosition = messageEnvelope.getPayload();
            Map<String, String> headers = messageEnvelope.getHeaders();
            
            // Extract tracing context from message headers
            Context extractedContext = extractTraceContext(headers);
            Span span = tracer.spanBuilder("process-position")
                    .setParent(extractedContext)
                    .setSpanKind(SpanKind.CONSUMER)
                    .startSpan();
            
            try (Scope scope = span.makeCurrent()) {
                // Add relevant attributes to the span
                span.setAttribute("deviceId", protoPosition.getDeviceId());
                span.setAttribute("protocol", protoPosition.getProtocol());
                if (protoPosition.hasFixTime()) {
                    span.setAttribute("fixTime", protoPosition.getFixTime().getSeconds());
                }
                
                LOGGER.debug("Received position message for device: {}", protoPosition.getDeviceId());
                
                // Convert protobuf position to domain model position
                Position position = convertToPosition(protoPosition);
                
                // Process the position through the processing pipeline
                return processPositionWithRetry(position, span, 0)
                        .whenComplete((result, throwable) -> {
                            if (throwable != null) {
                                span.recordException(throwable);
                                span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, throwable.getMessage());
                                LOGGER.error("Failed to process position after retries", throwable);
                                sendToDeadLetterQueue(position, throwable, headers);
                            } else {
                                span.setStatus(io.opentelemetry.api.trace.StatusCode.OK);
                            }
                            span.end();
                        });
            } catch (Exception e) {
                span.recordException(e);
                span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
                span.end();
                LOGGER.error("Error processing position message", e);
                sendToDeadLetterQueue(convertToPosition(protoPosition), e, headers);
                return CompletableFuture.completedFuture(null);
            }
        }

        /**
         * Process a position with retry logic using exponential backoff.
         */
        private CompletableFuture<Void> processPositionWithRetry(Position position, Span parentSpan, int attempt) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            
            // Check if we've exceeded the maximum retry attempts
            if (attempt >= MAX_RETRY_ATTEMPTS) {
                String errorMsg = "Maximum retry attempts reached for position: " + position.getDeviceId();
                LOGGER.error(errorMsg);
                future.completeExceptionally(new RuntimeException(errorMsg));
                return future;
            }
            
            // Create a child span for this attempt
            Span attemptSpan = tracer.spanBuilder("process-position-attempt")
                    .setParent(Context.current().with(parentSpan))
                    .setAttribute("attempt", attempt + 1)
                    .setAttribute("deviceId", position.getDeviceId())
                    .startSpan();
            
            try (Scope scope = attemptSpan.makeCurrent()) {
                // Use circuit breaker to protect against cascading failures
                circuitBreakerConfig.getPositionProcessingCircuitBreaker().executeRunnable(() -> {
                    try {
                        // Use the transaction outbox pattern to ensure reliable processing
                        outboxManager.executeInTransaction(() -> {
                            // Process the position through the processing handler
                            processingHandler.onReleased(null, position);
                            future.complete(null);
                        });
                    } catch (Exception e) {
                        attemptSpan.recordException(e);
                        attemptSpan.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
                        
                        // Calculate backoff time with exponential increase
                        long backoffMs = RETRY_BACKOFF_MS * (long) Math.pow(2, attempt);
                        LOGGER.warn("Error processing position (attempt {}), retrying in {} ms", 
                                attempt + 1, backoffMs, e);
                        
                        // Schedule retry after backoff
                        CompletableFuture.delayedExecutor(backoffMs, TimeUnit.MILLISECONDS)
                                .execute(() -> {
                                    processPositionWithRetry(position, parentSpan, attempt + 1)
                                            .whenComplete((result, throwable) -> {
                                                if (throwable != null) {
                                                    future.completeExceptionally(throwable);
                                                } else {
                                                    future.complete(null);
                                                }
                                            });
                                });
                    }
                }, e -> {
                    // Circuit breaker is open, send to dead letter queue immediately
                    attemptSpan.recordException(e);
                    attemptSpan.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, 
                            "Circuit breaker open: " + e.getMessage());
                    LOGGER.error("Circuit breaker open, not processing position", e);
                    future.completeExceptionally(e);
                });
            } catch (Exception e) {
                attemptSpan.recordException(e);
                attemptSpan.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
                future.completeExceptionally(e);
            } finally {
                attemptSpan.end();
            }
            
            return future;
        }

        /**
         * Send a failed position to the dead letter queue for later analysis or reprocessing.
         */
        private void sendToDeadLetterQueue(Position position, Throwable throwable, Map<String, String> originalHeaders) {
            try {
                MessageEnvelope<Position> deadLetterEnvelope = new MessageEnvelope<>(position);
                
                // Copy original headers
                if (originalHeaders != null) {
                    originalHeaders.forEach(deadLetterEnvelope::addHeader);
                }
                
                // Add error information
                deadLetterEnvelope.addHeader("error-message", throwable.getMessage());
                deadLetterEnvelope.addHeader("error-type", throwable.getClass().getName());
                deadLetterEnvelope.addHeader("failed-at", String.valueOf(System.currentTimeMillis()));
                deadLetterEnvelope.addHeader("retry-count", String.valueOf(MAX_RETRY_ATTEMPTS));
                
                deadLetterProducer.send(deadLetterEnvelope);
                LOGGER.info("Position sent to dead letter queue for device: {}", position.getDeviceId());
            } catch (Exception e) {
                LOGGER.error("Failed to send position to dead letter queue", e);
            }
        }

        /**
         * Extract the OpenTelemetry trace context from message headers.
         */
        private Context extractTraceContext(Map<String, String> headers) {
            return io.opentelemetry.api.GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                    .extract(Context.current(), headers, new TextMapGetter<Map<String, String>>() {
                        @Override
                        public Iterable<String> keys(Map<String, String> carrier) {
                            return carrier.keySet();
                        }

                        @Override
                        public String get(Map<String, String> carrier, String key) {
                            return carrier.get(key);
                        }
                    });
        }

        /**
         * Convert a Protocol Buffers Position message to a domain model Position.
         */
        private Position convertToPosition(PositionOuterClass.Position protoPosition) {
            Position position = new Position();
            position.setDeviceId(protoPosition.getDeviceId());
            position.setProtocol(protoPosition.getProtocol());
            
            if (protoPosition.hasValid()) {
                position.setValid(protoPosition.getValid());
            }
            
            if (protoPosition.hasLatitude()) {
                position.setLatitude(protoPosition.getLatitude());
            }
            
            if (protoPosition.hasLongitude()) {
                position.setLongitude(protoPosition.getLongitude());
            }
            
            if (protoPosition.hasAltitude()) {
                position.setAltitude(protoPosition.getAltitude());
            }
            
            if (protoPosition.hasSpeed()) {
                position.setSpeed(protoPosition.getSpeed());
            }
            
            if (protoPosition.hasCourse()) {
                position.setCourse(protoPosition.getCourse());
            }
            
            if (protoPosition.hasAddress()) {
                position.setAddress(protoPosition.getAddress());
            }
            
            if (protoPosition.hasFixTime()) {
                position.setFixTime(new java.util.Date(protoPosition.getFixTime().getSeconds() * 1000));
            }
            
            if (protoPosition.hasDeviceTime()) {
                position.setDeviceTime(new java.util.Date(protoPosition.getDeviceTime().getSeconds() * 1000));
            }
            
            if (protoPosition.hasServerTime()) {
                position.setServerTime(new java.util.Date(protoPosition.getServerTime().getSeconds() * 1000));
            } else {
                position.setServerTime(new java.util.Date());
            }
            
            // Convert attributes map
            protoPosition.getAttributesMap().forEach((key, value) -> {
                if (value.hasNumberValue()) {
                    position.set(key, value.getNumberValue());
                } else if (value.hasBoolValue()) {
                    position.set(key, value.getBoolValue());
                } else if (value.hasStringValue()) {
                    position.set(key, value.getStringValue());
                }
            });
            
            return position;
        }
    }
}