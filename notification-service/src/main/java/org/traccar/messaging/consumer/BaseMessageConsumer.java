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
package org.traccar.messaging.consumer;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.messaging.EventConsumer;
import org.traccar.model.Event;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Abstract base class for all message consumers in the Notification Service.
 * Provides common functionality for message consumption, error handling, and retry logic.
 * Implements the EventConsumer interface and provides template methods for message processing
 * that subclasses can override.
 */
public abstract class BaseMessageConsumer implements EventConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseMessageConsumer.class);
    
    private static final String DEFAULT_CORRELATION_ID_HEADER = "X-Correlation-ID";
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_INITIAL_BACKOFF = 1000; // 1 second
    private static final long DEFAULT_MAX_BACKOFF = 60000; // 60 seconds
    private static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0;
    private static final int DEFAULT_CONCURRENCY = 1;
    
    private final Executor executor;
    private final RetryRegistry retryRegistry;
    private final MeterRegistry meterRegistry;
    
    private String consumerGroup;
    private Set<String> eventTypes = new HashSet<>();
    private boolean subscribeAll = false;
    private Predicate<Event> filter;
    private SuccessCallback successCallback;
    private ErrorCallback errorCallback;
    private int concurrency = DEFAULT_CONCURRENCY;
    private boolean autoAck = true;
    private String deadLetterQueue;
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private long initialBackoff = DEFAULT_INITIAL_BACKOFF;
    private long maxBackoff = DEFAULT_MAX_BACKOFF;
    private double backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;
    private String correlationIdHeader = DEFAULT_CORRELATION_ID_HEADER;
    private Consumer<Object> metadataConsumer;
    private boolean running = false;
    
    // Metrics
    private Counter messagesReceivedCounter;
    private Counter messagesProcessedCounter;
    private Counter messagesFailedCounter;
    private Counter messagesRetriedCounter;
    private Counter messagesSentToDlqCounter;
    private Timer messageProcessingTimer;
    
    /**
     * Constructor with required dependencies.
     *
     * @param meterRegistry The meter registry for metrics collection
     */
    protected BaseMessageConsumer(MeterRegistry meterRegistry) {
        this(meterRegistry, RetryRegistry.ofDefaults(), Executors.newFixedThreadPool(DEFAULT_CONCURRENCY));
    }
    
    /**
     * Constructor with all dependencies.
     *
     * @param meterRegistry The meter registry for metrics collection
     * @param retryRegistry The retry registry for retry configuration
     * @param executor The executor for async processing
     */
    protected BaseMessageConsumer(MeterRegistry meterRegistry, RetryRegistry retryRegistry, Executor executor) {
        this.meterRegistry = meterRegistry;
        this.retryRegistry = retryRegistry;
        this.executor = executor;
        initializeMetrics();
    }
    
    /**
     * Initialize metrics for monitoring message processing.
     */
    private void initializeMetrics() {
        String className = this.getClass().getSimpleName();
        
        messagesReceivedCounter = Counter.builder("message.received")
                .description("Number of messages received")
                .tag("consumer", className)
                .register(meterRegistry);
        
        messagesProcessedCounter = Counter.builder("message.processed")
                .description("Number of messages successfully processed")
                .tag("consumer", className)
                .register(meterRegistry);
        
        messagesFailedCounter = Counter.builder("message.failed")
                .description("Number of messages that failed processing")
                .tag("consumer", className)
                .register(meterRegistry);
        
        messagesRetriedCounter = Counter.builder("message.retried")
                .description("Number of messages that were retried")
                .tag("consumer", className)
                .register(meterRegistry);
        
        messagesSentToDlqCounter = Counter.builder("message.sent.to.dlq")
                .description("Number of messages sent to dead letter queue")
                .tag("consumer", className)
                .register(meterRegistry);
        
        messageProcessingTimer = Timer.builder("message.processing.time")
                .description("Time taken to process messages")
                .tag("consumer", className)
                .register(meterRegistry);
    }
    
    @Override
    public void start() {
        if (running) {
            LOGGER.warn("Consumer already running");
            return;
        }
        
        configureRetry();
        doStart();
        running = true;
        LOGGER.info("Started message consumer: {}", this.getClass().getSimpleName());
    }
    
    /**
     * Configure the retry mechanism with exponential backoff.
     */
    private void configureRetry() {
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(maxRetries)
                .waitDuration(Duration.ofMillis(initialBackoff))
                .intervalFunction(attempt -> {
                    long interval = (long) (initialBackoff * Math.pow(backoffMultiplier, attempt - 1));
                    return Math.min(interval, maxBackoff);
                })
                .retryOnException(e -> shouldRetry(e))
                .build();
        
        retryRegistry.addConfiguration("default", retryConfig);
    }
    
    /**
     * Determine if an exception should trigger a retry.
     * Subclasses can override this to provide custom retry logic.
     *
     * @param exception The exception to check
     * @return true if the operation should be retried, false otherwise
     */
    protected boolean shouldRetry(Throwable exception) {
        // By default, retry on all exceptions except for those that indicate
        // a permanent failure or invalid message
        return !(exception instanceof IllegalArgumentException);
    }
    
    /**
     * Template method for starting the consumer.
     * Subclasses must implement this to connect to the message broker
     * and start consuming messages.
     */
    protected abstract void doStart();
    
    @Override
    public void stop() {
        if (!running) {
            LOGGER.warn("Consumer not running");
            return;
        }
        
        doStop();
        running = false;
        LOGGER.info("Stopped message consumer: {}", this.getClass().getSimpleName());
    }
    
    /**
     * Template method for stopping the consumer.
     * Subclasses must implement this to disconnect from the message broker
     * and stop consuming messages.
     */
    protected abstract void doStop();
    
    @Override
    public EventConsumer subscribe(Set<String> eventTypes) {
        this.eventTypes.addAll(eventTypes);
        return this;
    }
    
    @Override
    public EventConsumer subscribeToAll() {
        this.subscribeAll = true;
        return this;
    }
    
    @Override
    public EventConsumer withConsumerGroup(String groupId) {
        this.consumerGroup = groupId;
        return this;
    }
    
    @Override
    public EventConsumer onSuccess(SuccessCallback callback) {
        this.successCallback = callback;
        return this;
    }
    
    @Override
    public EventConsumer onError(ErrorCallback callback) {
        this.errorCallback = callback;
        return this;
    }
    
    @Override
    public EventConsumer withFilter(Predicate<Event> filter) {
        this.filter = filter;
        return this;
    }
    
    @Override
    public EventConsumer withConcurrency(int concurrency) {
        this.concurrency = concurrency;
        return this;
    }
    
    @Override
    public EventConsumer withAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
        return this;
    }
    
    @Override
    public void acknowledge(Event event, String correlationId) {
        doAcknowledge(event, correlationId);
    }
    
    /**
     * Template method for acknowledging a message.
     * Subclasses must implement this to acknowledge the message with the broker.
     *
     * @param event The event to acknowledge
     * @param correlationId The correlation ID for distributed tracing
     */
    protected abstract void doAcknowledge(Event event, String correlationId);
    
    @Override
    public void reject(Event event, boolean requeue, String correlationId) {
        doReject(event, requeue, correlationId);
    }
    
    /**
     * Template method for rejecting a message.
     * Subclasses must implement this to reject the message with the broker.
     *
     * @param event The event to reject
     * @param requeue Whether to requeue the message for later processing
     * @param correlationId The correlation ID for distributed tracing
     */
    protected abstract void doReject(Event event, boolean requeue, String correlationId);
    
    @Override
    public EventConsumer withDeadLetterQueue(String deadLetterQueue) {
        this.deadLetterQueue = deadLetterQueue;
        return this;
    }
    
    @Override
    public EventConsumer withMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }
    
    @Override
    public EventConsumer withRetryBackoff(long initialBackoff, long maxBackoff, double backoffMultiplier) {
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
        this.backoffMultiplier = backoffMultiplier;
        return this;
    }
    
    @Override
    public EventConsumer withCorrelationIdHeader(String headerName) {
        this.correlationIdHeader = headerName;
        return this;
    }
    
    @Override
    public EventConsumer withMetadataHandler(Consumer<Object> metadataConsumer) {
        this.metadataConsumer = metadataConsumer;
        return this;
    }
    
    /**
     * Process an event message with retry logic and error handling.
     * This method is called by subclasses when a message is received from the broker.
     *
     * @param event The event to process
     * @param metadata The message metadata (headers, properties, etc.)
     */
    protected void processMessage(Event event, Object metadata) {
        messagesReceivedCounter.increment();
        
        // Extract or generate correlation ID
        String correlationId = extractCorrelationId(metadata);
        if (correlationId == null) {
            correlationId = generateCorrelationId();
            LOGGER.debug("Generated correlation ID: {}", correlationId);
        }
        
        // Process metadata if handler is provided
        if (metadataConsumer != null) {
            metadataConsumer.accept(metadata);
        }
        
        // Apply filter if provided
        if (filter != null && !filter.test(event)) {
            LOGGER.debug("Event filtered out: {}, correlationId: {}", event, correlationId);
            if (autoAck) {
                acknowledge(event, correlationId);
            }
            return;
        }
        
        // Create retry instance
        Retry retry = retryRegistry.retry("default");
        String finalCorrelationId = correlationId;
        
        // Process message with retry and timing
        Timer.Sample sample = Timer.start(meterRegistry);
        CompletableFuture.supplyAsync(() -> {
            try {
                return Retry.decorateSupplier(retry, () -> {
                    try {
                        LOGGER.debug("Processing event: {}, correlationId: {}", event, finalCorrelationId);
                        doProcessMessage(event, finalCorrelationId);
                        return true;
                    } catch (Exception e) {
                        messagesRetriedCounter.increment();
                        LOGGER.warn("Retrying event processing: {}, correlationId: {}", event, finalCorrelationId, e);
                        throw e;
                    }
                }).get();
            } catch (Exception e) {
                LOGGER.error("Failed to process event after retries: {}, correlationId: {}", event, finalCorrelationId, e);
                messagesFailedCounter.increment();
                
                // Send to dead letter queue if configured
                if (deadLetterQueue != null) {
                    try {
                        sendToDeadLetterQueue(event, finalCorrelationId, e);
                        messagesSentToDlqCounter.increment();
                    } catch (Exception dlqException) {
                        LOGGER.error("Failed to send to dead letter queue: {}, correlationId: {}", 
                                event, finalCorrelationId, dlqException);
                    }
                }
                
                // Call error callback if provided
                if (errorCallback != null) {
                    errorCallback.onError(event, e, finalCorrelationId);
                }
                
                // Reject message if not auto-ack
                if (!autoAck) {
                    reject(event, false, finalCorrelationId);
                }
                
                return false;
            }
        }, executor).thenAccept(success -> {
            // Record processing time
            sample.stop(messageProcessingTimer);
            
            if (success) {
                messagesProcessedCounter.increment();
                LOGGER.debug("Successfully processed event: {}, correlationId: {}", event, finalCorrelationId);
                
                // Call success callback if provided
                if (successCallback != null) {
                    successCallback.onSuccess(event, finalCorrelationId);
                }
                
                // Acknowledge message if not auto-ack
                if (!autoAck) {
                    acknowledge(event, finalCorrelationId);
                }
            }
        });
    }
    
    /**
     * Extract correlation ID from message metadata.
     * Subclasses should override this to extract the correlation ID from
     * the specific message broker's metadata format.
     *
     * @param metadata The message metadata
     * @return The correlation ID, or null if not found
     */
    protected String extractCorrelationId(Object metadata) {
        // Default implementation returns null, subclasses should override
        return null;
    }
    
    /**
     * Generate a new correlation ID.
     *
     * @return A new correlation ID
     */
    protected String generateCorrelationId() {
        return UUID.randomUUID().toString();
    }
    
    /**
     * Template method for processing a message.
     * Subclasses must implement this to process the message.
     *
     * @param event The event to process
     * @param correlationId The correlation ID for distributed tracing
     * @throws Exception If an error occurs during processing
     */
    protected abstract void doProcessMessage(Event event, String correlationId) throws Exception;
    
    /**
     * Send a failed message to the dead letter queue.
     * Subclasses must implement this to send the message to the dead letter queue.
     *
     * @param event The event to send to the dead letter queue
     * @param correlationId The correlation ID for distributed tracing
     * @param exception The exception that caused the failure
     * @throws Exception If an error occurs while sending to the dead letter queue
     */
    protected abstract void sendToDeadLetterQueue(Event event, String correlationId, Exception exception) throws Exception;
    
    /**
     * Get the consumer group ID.
     *
     * @return The consumer group ID
     */
    protected String getConsumerGroup() {
        return consumerGroup;
    }
    
    /**
     * Get the set of event types to subscribe to.
     *
     * @return The set of event types
     */
    protected Set<String> getEventTypes() {
        return eventTypes;
    }
    
    /**
     * Check if the consumer should subscribe to all event types.
     *
     * @return true if the consumer should subscribe to all event types, false otherwise
     */
    protected boolean isSubscribeAll() {
        return subscribeAll;
    }
    
    /**
     * Get the concurrency level for message processing.
     *
     * @return The concurrency level
     */
    protected int getConcurrency() {
        return concurrency;
    }
    
    /**
     * Check if auto-acknowledgment is enabled.
     *
     * @return true if auto-acknowledgment is enabled, false otherwise
     */
    protected boolean isAutoAck() {
        return autoAck;
    }
    
    /**
     * Get the dead letter queue name.
     *
     * @return The dead letter queue name
     */
    protected String getDeadLetterQueue() {
        return deadLetterQueue;
    }
    
    /**
     * Get the correlation ID header name.
     *
     * @return The correlation ID header name
     */
    protected String getCorrelationIdHeader() {
        return correlationIdHeader;
    }
}