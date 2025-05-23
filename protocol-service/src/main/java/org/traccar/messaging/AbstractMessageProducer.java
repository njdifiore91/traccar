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
package org.traccar.messaging;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Abstract base implementation of the MessageProducer interface with common functionality
 * for all message broker implementations. It handles message serialization, retry logic,
 * metrics collection, and distributed tracing.
 * <p>
 * This class provides:
 * <ul>
 *   <li>Resilient message publishing with retry mechanism and exponential backoff</li>
 *   <li>Distributed tracing for message publishing operations using OpenTelemetry</li>
 *   <li>Metrics collection for monitoring using Micrometer</li>
 *   <li>Consistent error handling and logging across implementations</li>
 *   <li>Support for multiple serialization formats</li>
 * </ul>
 */
public abstract class AbstractMessageProducer implements MessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMessageProducer.class);
    
    private static final String DEFAULT_RETRY_NAME = "message-producer-retry";
    private static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final Duration DEFAULT_INITIAL_INTERVAL = Duration.ofMillis(100);
    private static final double DEFAULT_MULTIPLIER = 2.0;
    
    protected final MessageSerializer serializer;
    protected final MessagingTracer tracer;
    protected final MessagingMetrics metrics;
    protected final RetryRegistry retryRegistry;
    protected final Executor asyncExecutor;
    
    /**
     * Creates a new AbstractMessageProducer with the specified dependencies.
     *
     * @param serializer the message serializer to use
     * @param tracer the messaging tracer for distributed tracing
     * @param metrics the messaging metrics collector
     * @param meterRegistry the meter registry for metrics collection
     */
    @Inject
    public AbstractMessageProducer(
            MessageSerializer serializer,
            MessagingTracer tracer,
            MessagingMetrics metrics,
            MeterRegistry meterRegistry) {
        this.serializer = serializer;
        this.tracer = tracer;
        this.metrics = metrics;
        this.retryRegistry = createRetryRegistry();
        this.asyncExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }
    
    /**
     * Creates a retry registry with default configuration.
     *
     * @return the retry registry
     */
    protected RetryRegistry createRetryRegistry() {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(DEFAULT_MAX_ATTEMPTS)
                .waitDuration(DEFAULT_INITIAL_INTERVAL)
                .exponentialBackoff(DEFAULT_INITIAL_INTERVAL, DEFAULT_MULTIPLIER)
                .retryExceptions(Exception.class)
                .ignoreExceptions(InterruptedException.class)
                .build();
        
        return RetryRegistry.of(Map.of(DEFAULT_RETRY_NAME, config));
    }
    
    @Override
    public CompletableFuture<Void> publish(String destination, Object message) {
        return publish(destination, null, message, new HashMap<>());
    }
    
    @Override
    public CompletableFuture<Void> publish(String destination, Object message, Map<String, Object> headers) {
        return publish(destination, null, message, headers);
    }
    
    @Override
    public CompletableFuture<Void> publish(String destination, String routingKey, Object message, Map<String, Object> headers) {
        String messageId = UUID.randomUUID().toString();
        Map<String, Object> enrichedHeaders = enrichHeaders(headers, messageId);
        
        // Start tracing span
        Span span = startPublishSpan(destination, messageId, routingKey);
        
        try {
            // Serialize the message
            byte[] serializedMessage = serializeMessage(message, enrichedHeaders, span);
            
            // Create retry with exponential backoff
            Retry retry = retryRegistry.retry(DEFAULT_RETRY_NAME);
            
            // Execute the publish operation with retry
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return Retry.decorateSupplier(retry, () -> {
                        try {
                            // Record metrics for the publish operation
                            return metrics.recordPublishTime(destination, () -> {
                                doPublish(destination, routingKey, serializedMessage, enrichedHeaders);
                                return null;
                            });
                        } catch (Exception e) {
                            LOGGER.warn("Failed to publish message to {}: {}", destination, e.getMessage());
                            metrics.recordMessagePublishError(destination, e.getClass().getSimpleName());
                            tracer.recordError(span, e);
                            throw e;
                        }
                    }).get();
                } catch (Exception e) {
                    LOGGER.error("Failed to publish message to {} after {} attempts: {}", 
                            destination, DEFAULT_MAX_ATTEMPTS, e.getMessage());
                    metrics.recordMessagePublishError(destination, "MaxRetriesExceeded");
                    tracer.recordError(span, e);
                    throw new RuntimeException("Failed to publish message after retries", e);
                } finally {
                    span.end();
                }
            }, asyncExecutor).thenApply(result -> null);
        } catch (Exception e) {
            LOGGER.error("Error preparing message for publishing to {}: {}", destination, e.getMessage());
            metrics.recordMessagePublishError(destination, "SerializationError");
            tracer.recordError(span, e);
            span.end();
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }
    
    @Override
    public CompletableFuture<Void> publishBatch(String destination, Iterable<?> messages) {
        CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        
        for (Object message : messages) {
            result = result.thenCompose(v -> publish(destination, message));
        }
        
        return result;
    }
    
    /**
     * Enriches the headers with additional metadata.
     *
     * @param headers the original headers
     * @param messageId the unique message identifier
     * @return the enriched headers
     */
    protected Map<String, Object> enrichHeaders(Map<String, Object> headers, String messageId) {
        Map<String, Object> enrichedHeaders = new HashMap<>(headers);
        enrichedHeaders.put("messageId", messageId);
        enrichedHeaders.put("timestamp", System.currentTimeMillis());
        enrichedHeaders.put("contentType", serializer.getContentType());
        enrichedHeaders.put("schemaVersion", serializer.getSchemaVersion());
        
        // Convert headers to string map for tracing context injection
        Map<String, String> stringHeaders = new HashMap<>();
        enrichedHeaders.forEach((key, value) -> stringHeaders.put(key, String.valueOf(value)));
        
        // Inject tracing context into headers
        Map<String, String> headersWithContext = tracer.injectTraceContext(stringHeaders);
        
        // Convert back to Object map and merge with enriched headers
        headersWithContext.forEach(enrichedHeaders::put);
        
        return enrichedHeaders;
    }
    
    /**
     * Serializes a message for publishing.
     *
     * @param message the message to serialize
     * @param headers the message headers
     * @param span the current tracing span
     * @return the serialized message as a byte array
     * @throws MessageSerializer.SerializationException if serialization fails
     */
    protected byte[] serializeMessage(Object message, Map<String, Object> headers, Span span) 
            throws MessageSerializer.SerializationException {
        byte[] serializedMessage = serializer.serialize(message, headers);
        tracer.setPayloadSize(span, serializedMessage.length);
        metrics.recordMessageSize(message.getClass().getSimpleName(), serializedMessage.length);
        return serializedMessage;
    }
    
    /**
     * Starts a publish span for tracing.
     *
     * @param destination the destination topic or queue
     * @param messageId the unique message identifier
     * @param routingKey the optional routing key
     * @return the created span
     */
    protected Span startPublishSpan(String destination, String messageId, String routingKey) {
        String messagingSystem = getMessagingSystemName();
        Span span = tracer.startPublishSpan(destination, messageId, messagingSystem);
        
        if (routingKey != null) {
            span.setAttribute("messaging.routing_key", routingKey);
        }
        
        return span;
    }
    
    /**
     * Gets the name of the messaging system for tracing and metrics.
     *
     * @return the messaging system name (e.g., "kafka", "rabbitmq")
     */
    protected abstract String getMessagingSystemName();
    
    /**
     * Performs the actual publish operation to the message broker.
     * This method must be implemented by concrete subclasses.
     *
     * @param destination the destination topic or queue
     * @param routingKey the optional routing key
     * @param serializedMessage the serialized message as a byte array
     * @param headers the message headers
     * @throws Exception if the publish operation fails
     */
    protected abstract void doPublish(String destination, String routingKey, byte[] serializedMessage, 
                                     Map<String, Object> headers) throws Exception;
    
    @Override
    public void close() {
        // Default implementation does nothing
        // Subclasses should override this method to release resources
    }
}