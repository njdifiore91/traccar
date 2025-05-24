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
package org.traccar.handler;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.config.ConfigKey;
import org.traccar.messaging.MessageConstants;
import org.traccar.messaging.MessageException;
import org.traccar.messaging.PositionMessage;
import org.traccar.messaging.PositionProducer;
import org.traccar.model.Position;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Component responsible for publishing enriched position data to the message broker
 * for consumption by downstream services. It handles serialization of Position objects
 * to Protocol Buffers format, manages topic partitioning based on device ID for ordered
 * processing, and implements retry mechanisms for reliable message delivery.
 */
@Singleton
public class EnrichedPositionProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnrichedPositionProducer.class);
    
    private static final String SPAN_NAME = "publish_enriched_position";
    private static final String METRIC_NAMESPACE = "org.traccar.position";
    private static final String PUBLISH_COUNTER_NAME = METRIC_NAMESPACE + ".enriched_positions_published";
    private static final String RETRY_COUNTER_NAME = METRIC_NAMESPACE + ".enriched_positions_retried";
    private static final String ERROR_COUNTER_NAME = METRIC_NAMESPACE + ".enriched_positions_failed";
    
    private static final AttributeKey<String> DEVICE_ID_KEY = AttributeKey.stringKey("deviceId");
    private static final AttributeKey<String> TOPIC_KEY = AttributeKey.stringKey("topic");
    private static final AttributeKey<String> CORRELATION_ID_KEY = AttributeKey.stringKey("correlationId");
    private static final AttributeKey<String> PROTOCOL_KEY = AttributeKey.stringKey("protocol");
    
    private final PositionProducer positionProducer;
    private final Config config;
    private final Tracer tracer;
    private final Meter meter;
    private final Executor retryExecutor;
    
    private final LongCounter publishCounter;
    private final LongCounter retryCounter;
    private final LongCounter errorCounter;
    
    // Configuration keys for position publishing
    private static final ConfigKey<Integer> POSITION_PUBLISH_MAX_RETRIES = new ConfigKey<>(
            "position.publish.maxRetries", Integer.class, 3);
    private static final ConfigKey<Long> POSITION_PUBLISH_RETRY_DELAY = new ConfigKey<>(
            "position.publish.retryDelay", Long.class, 100L);
    private static final ConfigKey<Double> POSITION_PUBLISH_RETRY_MULTIPLIER = new ConfigKey<>(
            "position.publish.retryMultiplier", Double.class, 2.0);
    private static final ConfigKey<Long> POSITION_PUBLISH_MAX_RETRY_DELAY = new ConfigKey<>(
            "position.publish.maxRetryDelay", Long.class, 10000L);
    private static final ConfigKey<Long> POSITION_PUBLISH_TIMEOUT = new ConfigKey<>(
            "position.publish.timeout", Long.class, 5000L);
            
    private final int maxRetries;
    private final long initialRetryDelayMs;
    private final double retryBackoffMultiplier;
    private final long maxRetryDelayMs;
    
    /**
     * Constructs a new EnrichedPositionProducer with the specified dependencies.
     *
     * @param positionProducer The underlying position producer implementation (Kafka or RabbitMQ)
     * @param config The application configuration
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param meter The OpenTelemetry meter for metrics collection
     */
    @Inject
    public EnrichedPositionProducer(
            PositionProducer positionProducer,
            Config config,
            Tracer tracer,
            Meter meter) {
        this.positionProducer = positionProducer;
        this.config = config;
        this.tracer = tracer;
        this.meter = meter;
        
        // Initialize retry configuration from application config
        this.maxRetries = config.getInteger(POSITION_PUBLISH_MAX_RETRIES);
        this.initialRetryDelayMs = config.getLong(POSITION_PUBLISH_RETRY_DELAY);
        this.retryBackoffMultiplier = config.getDouble(POSITION_PUBLISH_RETRY_MULTIPLIER);
        this.maxRetryDelayMs = config.getLong(POSITION_PUBLISH_MAX_RETRY_DELAY);
        
        // Create a dedicated thread pool for retry operations
        this.retryExecutor = Executors.newVirtualThreadPerTaskExecutor();
        
        // Initialize metrics
        this.publishCounter = meter.counterBuilder(PUBLISH_COUNTER_NAME)
                .setDescription("Number of enriched positions published to the message broker")
                .build();
        
        this.retryCounter = meter.counterBuilder(RETRY_COUNTER_NAME)
                .setDescription("Number of retried publish operations for enriched positions")
                .build();
        
        this.errorCounter = meter.counterBuilder(ERROR_COUNTER_NAME)
                .setDescription("Number of failed publish operations for enriched positions")
                .build();
    }
    
    /**
     * Publishes an enriched position to the message broker for consumption by downstream services.
     * This method handles serialization, partitioning, and implements retry logic for reliable delivery.
     *
     * @param position The enriched position to publish
     * @return A CompletableFuture that completes when the position is successfully published or fails with an exception
     */
    public CompletableFuture<Void> publishPosition(Position position) {
        String correlationId = UUID.randomUUID().toString();
        
        // Create a span for this operation
        Span span = tracer.spanBuilder(SPAN_NAME)
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(DEVICE_ID_KEY, String.valueOf(position.getDeviceId()))
                .setAttribute(TOPIC_KEY, MessageConstants.TOPIC_ENRICHED_POSITIONS)
                .setAttribute(CORRELATION_ID_KEY, correlationId)
                .setAttribute(PROTOCOL_KEY, position.getProtocol())
                .startSpan();
        
        // Create a new context with the current span
        Context context = Context.current().with(span);
        
        // Create a CompletableFuture to track the publish operation
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        try {
            // Convert Position to PositionMessage for serialization
            PositionMessage message = convertToPositionMessage(position, correlationId);
            
            // Publish the message with retry logic
            publishWithRetry(message, 0, span, future);
            
        } catch (Exception e) {
            // Handle any unexpected exceptions during conversion or publishing
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            errorCounter.add(1, Attributes.of(DEVICE_ID_KEY, String.valueOf(position.getDeviceId())));
            future.completeExceptionally(e);
        } finally {
            // End the span when the operation is complete
            span.end();
        }
        
        return future;
    }
    
    /**
     * Converts a Position object to a PositionMessage for serialization and publishing.
     *
     * @param position The position to convert
     * @param correlationId The correlation ID for distributed tracing
     * @return A PositionMessage containing the position data and metadata
     */
    private PositionMessage convertToPositionMessage(Position position, String correlationId) {
        PositionMessage message = new PositionMessage();
        
        // Set position data
        message.setDeviceId(position.getDeviceId());
        message.setProtocol(position.getProtocol());
        message.setServerTime(position.getServerTime().getTime());
        message.setDeviceTime(position.getDeviceTime().getTime());
        message.setFixTime(position.getFixTime().getTime());
        message.setValid(position.getValid());
        message.setLatitude(position.getLatitude());
        message.setLongitude(position.getLongitude());
        message.setAltitude(position.getAltitude());
        message.setSpeed(position.getSpeed());
        message.setCourse(position.getCourse());
        message.setAccuracy(position.getAccuracy());
        message.setNetwork(position.getNetwork());
        
        // Set position attributes
        Map<String, Object> attributes = position.getAttributes();
        if (attributes != null) {
            message.setAttributes(attributes);
        }
        
        // Set metadata for tracing and processing
        message.setPositionId(position.getId());
        message.setCorrelationId(correlationId);
        message.setTimestamp(System.currentTimeMillis());
        
        return message;
    }
    
    /**
     * Publishes a position message with retry logic for transient failures.
     * Uses exponential backoff with jitter for retry delays.
     *
     * @param message The position message to publish
     * @param retryCount The current retry count
     * @param span The OpenTelemetry span for this operation
     * @param future The CompletableFuture to complete when the operation succeeds or fails
     */
    private void publishWithRetry(PositionMessage message, int retryCount, Span span, CompletableFuture<Void> future) {
        // Add retry count to span for observability
        if (retryCount > 0) {
            span.setAttribute("retry.count", retryCount);
        }
        try {
            // Publish the message to the broker
            positionProducer.publish(MessageConstants.TOPIC_ENRICHED_POSITIONS, message, String.valueOf(message.getDeviceId()))
                    .thenAccept(result -> {
                        // Record successful publish
                        publishCounter.add(1, Attributes.of(
                                DEVICE_ID_KEY, String.valueOf(message.getDeviceId()),
                                PROTOCOL_KEY, message.getProtocol()));
                        span.addEvent("Position published successfully");
                        future.complete(null);
                    })
                    .exceptionally(e -> {
                        handlePublishError(message, retryCount, e, span, future);
                        return null;
                    });
        } catch (Exception e) {
            handlePublishError(message, retryCount, e, span, future);
        }
    }
    
    /**
     * Handles errors that occur during publishing and implements retry logic.
     *
     * @param message The position message that failed to publish
     * @param retryCount The current retry count
     * @param exception The exception that occurred
     * @param span The OpenTelemetry span for this operation
     * @param future The CompletableFuture to complete when the operation succeeds or fails
     */
    private void handlePublishError(PositionMessage message, int retryCount, Throwable exception, Span span, CompletableFuture<Void> future) {
        // Check if we should retry based on the exception type and retry count
        if (shouldRetry(exception, retryCount)) {
            // Calculate retry delay with exponential backoff and jitter
            long delayMs = calculateRetryDelay(retryCount);
            
            // Record retry metrics
            retryCounter.add(1, Attributes.of(DEVICE_ID_KEY, String.valueOf(message.getDeviceId())));
            span.addEvent("Retrying position publish", Attributes.of(
                    AttributeKey.longKey("retryCount"), retryCount + 1,
                    AttributeKey.longKey("delayMs"), delayMs));
            
            // Schedule retry after delay
            CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS, retryExecutor)
                    .execute(() -> publishWithRetry(message, retryCount + 1, span, future));
        } else {
            // Max retries exceeded or non-retryable error
            String errorMessage = "Failed to publish position after " + retryCount + " retries: " + exception.getMessage();
            LOGGER.error(errorMessage, exception);
            
            // Record error metrics
            errorCounter.add(1, Attributes.of(DEVICE_ID_KEY, String.valueOf(message.getDeviceId())));
            span.recordException(exception);
            span.setStatus(StatusCode.ERROR, errorMessage);
            
            // Complete the future exceptionally
            future.completeExceptionally(exception);
        }
    }
    
    /**
     * Determines whether a publish operation should be retried based on the exception type and retry count.
     *
     * @param exception The exception that occurred
     * @param retryCount The current retry count
     * @return true if the operation should be retried, false otherwise
     */
    private boolean shouldRetry(Throwable exception, int retryCount) {
        // Don't retry if we've exceeded the maximum retry count
        if (retryCount >= maxRetries) {
            return false;
        }
        
        // Determine if the exception is retryable
        if (exception instanceof MessageException messageException) {
            // Check if the specific message exception is retryable
            return messageException.isRetryable();
        }
        
        // By default, retry for transient errors like connection issues
        return exception instanceof java.io.IOException ||
               exception instanceof java.net.SocketTimeoutException ||
               exception instanceof java.util.concurrent.TimeoutException;
    }
    
    /**
     * Calculates the retry delay using exponential backoff with jitter.
     *
     * @param retryCount The current retry count
     * @return The delay in milliseconds before the next retry
     */
    private long calculateRetryDelay(int retryCount) {
        // Calculate base delay with exponential backoff
        double exponentialDelay = initialRetryDelayMs * Math.pow(retryBackoffMultiplier, retryCount);
        
        // Apply maximum delay cap
        long baseDelay = Math.min((long) exponentialDelay, maxRetryDelayMs);
        
        // Add jitter (±20%) to avoid thundering herd problem
        double jitterFactor = 0.8 + (Math.random() * 0.4); // Random value between 0.8 and 1.2
        long delayWithJitter = (long) (baseDelay * jitterFactor);
        
        return delayWithJitter;
    }
    
    /**
     * Publishes an enriched position to the message broker and waits for the operation to complete.
     * This is a blocking version of the publishPosition method.
     *
     * @param position The enriched position to publish
     * @param timeout The maximum time to wait for the publish operation to complete
     * @param unit The time unit of the timeout parameter
     * @throws Exception If the publish operation fails or times out
     */
    public void publishPositionSync(Position position, long timeout, TimeUnit unit) throws Exception {
        try {
            publishPosition(position).get(timeout, unit);
        } catch (Exception e) {
            LOGGER.error("Failed to publish position synchronously: {}", e.getMessage());
            throw e;
        }
    }
    
    /**
     * Publishes an enriched position to the message broker with a default timeout.
     * This is a blocking version of the publishPosition method with a default timeout.
     *
     * @param position The enriched position to publish
     * @throws Exception If the publish operation fails or times out
     */
    public void publishPositionSync(Position position) throws Exception {
        long defaultTimeoutMs = config.getLong(POSITION_PUBLISH_TIMEOUT);
        publishPositionSync(position, defaultTimeoutMs, TimeUnit.MILLISECONDS);
    }
}