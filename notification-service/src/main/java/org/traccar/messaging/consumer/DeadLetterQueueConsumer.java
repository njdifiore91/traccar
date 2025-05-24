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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
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
import org.traccar.messaging.MessageConsumer;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHandler;
import org.traccar.messaging.MessageHeaders;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessagingConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Consumes messages from the dead-letter-queue topic, analyzes failure patterns,
 * and applies recovery strategies based on error type. Implements retry logic with
 * exponential backoff and maintains correlation IDs for tracing.
 */
@Singleton
public class DeadLetterQueueConsumer extends BaseMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeadLetterQueueConsumer.class);

    private static final String TOPIC_DEAD_LETTER_QUEUE = "dead-letter-queue";
    private static final String HEADER_ERROR_TYPE = "error-type";
    private static final String HEADER_ORIGINAL_TOPIC = "original-topic";
    private static final String HEADER_RETRY_COUNT = "retry-count";
    private static final String HEADER_FAILURE_REASON = "failure-reason";
    private static final String HEADER_FAILED_TIMESTAMP = "failed-timestamp";

    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final ScheduledExecutorService scheduler;
    private final Map<ErrorType, RecoveryStrategy> recoveryStrategies;
    private final int maxRetryAttempts;
    private final long initialRetryDelayMs;
    private final double retryBackoffMultiplier;
    private final long maxRetryDelayMs;

    /**
     * Error types for categorizing notification failures.
     */
    public enum ErrorType {
        NETWORK,           // Network connectivity issues
        AUTHENTICATION,    // Authentication failures with external services
        RATE_LIMITING,     // Rate limit exceeded with external services
        SERVICE_UNAVAILABLE, // External service temporarily unavailable
        INVALID_FORMAT,    // Message format or content issues
        BROKER_FAILURE,    // Message broker connectivity issues
        DISCOVERY_FAILURE, // Service discovery failures
        UNKNOWN            // Uncategorized errors
    }

    /**
     * Interface for recovery strategies based on error type.
     */
    private interface RecoveryStrategy {
        boolean apply(MessageEnvelope message, ErrorType errorType, int retryCount);
    }

    /**
     * Creates a new DeadLetterQueueConsumer.
     *
     * @param config Configuration for the service
     * @param messagingConfig Messaging-specific configuration
     * @param messageProducer Producer for sending messages back to original topics
     * @param tracer OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public DeadLetterQueueConsumer(
            Config config,
            MessagingConfig messagingConfig,
            MessageProducer messageProducer,
            Tracer tracer) {
        super(config, messagingConfig);
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.maxRetryAttempts = config.getInteger("notification.retry.maxAttempts", 5);
        this.initialRetryDelayMs = config.getLong("notification.retry.initialDelayMs", 1000);
        this.retryBackoffMultiplier = config.getDouble("notification.retry.backoffMultiplier", 2.0);
        this.maxRetryDelayMs = config.getLong("notification.retry.maxDelayMs", 60000);
        
        // Initialize recovery strategies for different error types
        this.recoveryStrategies = initializeRecoveryStrategies();
        
        // Start the consumer for the dead-letter-queue topic
        startConsumer();
    }

    /**
     * Initializes recovery strategies for different error types.
     *
     * @return Map of error types to recovery strategies
     */
    private Map<ErrorType, RecoveryStrategy> initializeRecoveryStrategies() {
        Map<ErrorType, RecoveryStrategy> strategies = new HashMap<>();
        
        // Network errors - retry with exponential backoff
        strategies.put(ErrorType.NETWORK, (message, errorType, retryCount) -> {
            if (retryCount < maxRetryAttempts) {
                scheduleRetry(message, errorType, retryCount);
                return true;
            }
            return false;
        });
        
        // Authentication errors - alert admin and don't retry automatically
        strategies.put(ErrorType.AUTHENTICATION, (message, errorType, retryCount) -> {
            LOGGER.error("Authentication error detected for message with correlation ID: {}", 
                    message.getHeaders().get(MessageHeaders.CORRELATION_ID));
            // These errors typically require manual intervention
            return false;
        });
        
        // Rate limiting errors - retry with longer backoff
        strategies.put(ErrorType.RATE_LIMITING, (message, errorType, retryCount) -> {
            if (retryCount < maxRetryAttempts) {
                // Use a longer delay for rate limiting issues
                long delay = calculateRetryDelay(retryCount) * 2;
                scheduleRetryWithDelay(message, errorType, retryCount, delay);
                return true;
            }
            return false;
        });
        
        // Service unavailable errors - check service status before retry
        strategies.put(ErrorType.SERVICE_UNAVAILABLE, (message, errorType, retryCount) -> {
            if (retryCount < maxRetryAttempts) {
                scheduleRetry(message, errorType, retryCount);
                return true;
            }
            return false;
        });
        
        // Invalid format errors - typically can't be recovered automatically
        strategies.put(ErrorType.INVALID_FORMAT, (message, errorType, retryCount) -> {
            LOGGER.error("Invalid format error for message with correlation ID: {}", 
                    message.getHeaders().get(MessageHeaders.CORRELATION_ID));
            // These errors typically require manual intervention
            return false;
        });
        
        // Broker failure errors - wait for broker to be available
        strategies.put(ErrorType.BROKER_FAILURE, (message, errorType, retryCount) -> {
            if (retryCount < maxRetryAttempts) {
                // Use a longer delay for broker issues
                long delay = calculateRetryDelay(retryCount) * 2;
                scheduleRetryWithDelay(message, errorType, retryCount, delay);
                return true;
            }
            return false;
        });
        
        // Discovery failure errors - refresh service registry
        strategies.put(ErrorType.DISCOVERY_FAILURE, (message, errorType, retryCount) -> {
            if (retryCount < maxRetryAttempts) {
                // Use cached endpoints and retry
                scheduleRetry(message, errorType, retryCount);
                return true;
            }
            return false;
        });
        
        // Unknown errors - retry a few times but with caution
        strategies.put(ErrorType.UNKNOWN, (message, errorType, retryCount) -> {
            if (retryCount < 3) { // Fewer retries for unknown errors
                scheduleRetry(message, errorType, retryCount);
                return true;
            }
            return false;
        });
        
        return strategies;
    }

    /**
     * Starts the consumer for the dead-letter-queue topic.
     */
    private void startConsumer() {
        try {
            // Create a message handler for the dead-letter-queue
            MessageHandler<MessageEnvelope> handler = this::processDeadLetterMessage;
            
            // Create a consumer for the dead-letter-queue topic
            MessageConsumer consumer = getMessageConsumerFactory().createConsumer(
                    TOPIC_DEAD_LETTER_QUEUE,
                    "notification-service-dlq-consumer",
                    handler);
            
            LOGGER.info("Started dead letter queue consumer for topic: {}", TOPIC_DEAD_LETTER_QUEUE);
        } catch (Exception e) {
            LOGGER.error("Failed to start dead letter queue consumer", e);
        }
    }

    /**
     * Processes a message from the dead-letter-queue.
     *
     * @param message The failed message with error context
     * @return true if the message was processed successfully
     */
    private boolean processDeadLetterMessage(MessageEnvelope message) {
        String correlationId = extractCorrelationId(message);
        ErrorType errorType = extractErrorType(message);
        int retryCount = extractRetryCount(message);
        String originalTopic = extractOriginalTopic(message);
        String failureReason = extractFailureReason(message);
        
        // Create a span for tracing this processing
        Span span = tracer.spanBuilder("process-dead-letter")
                .setParent(Context.current().with(Span.current()))
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("correlation.id", correlationId)
                .setAttribute("error.type", errorType.name())
                .setAttribute("retry.count", retryCount)
                .setAttribute("original.topic", originalTopic)
                .setAttribute("failure.reason", failureReason)
                .startSpan();
        
        try {
            LOGGER.info("Processing dead letter message: correlationId={}, errorType={}, retryCount={}, originalTopic={}", 
                    correlationId, errorType, retryCount, originalTopic);
            
            // Apply the appropriate recovery strategy based on error type
            RecoveryStrategy strategy = recoveryStrategies.getOrDefault(errorType, recoveryStrategies.get(ErrorType.UNKNOWN));
            boolean recovered = strategy.apply(message, errorType, retryCount);
            
            if (recovered) {
                LOGGER.info("Recovery strategy applied for message: {}", correlationId);
                span.setStatus(StatusCode.OK);
            } else {
                LOGGER.warn("Failed to recover message after {} attempts: {}", retryCount, correlationId);
                span.setStatus(StatusCode.ERROR, "Failed to recover after multiple attempts");
            }
            
            return true; // Always acknowledge the dead letter message
        } catch (Exception e) {
            LOGGER.error("Error processing dead letter message: {}", correlationId, e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            return true; // Still acknowledge to prevent redelivery loop
        } finally {
            span.end();
        }
    }

    /**
     * Schedules a retry for a failed message with exponential backoff.
     *
     * @param message The failed message
     * @param errorType The type of error that occurred
     * @param retryCount The current retry count
     */
    private void scheduleRetry(MessageEnvelope message, ErrorType errorType, int retryCount) {
        long delay = calculateRetryDelay(retryCount);
        scheduleRetryWithDelay(message, errorType, retryCount, delay);
    }

    /**
     * Schedules a retry for a failed message with a specific delay.
     *
     * @param message The failed message
     * @param errorType The type of error that occurred
     * @param retryCount The current retry count
     * @param delayMs The delay in milliseconds before retry
     */
    private void scheduleRetryWithDelay(MessageEnvelope message, ErrorType errorType, int retryCount, long delayMs) {
        String correlationId = extractCorrelationId(message);
        String originalTopic = extractOriginalTopic(message);
        
        LOGGER.info("Scheduling retry {} for message {} to topic {} after {}ms", 
                retryCount + 1, correlationId, originalTopic, delayMs);
        
        scheduler.schedule(() -> {
            try {
                // Create a new span for the retry attempt
                Span span = tracer.spanBuilder("retry-dead-letter")
                        .setParent(Context.current())
                        .setSpanKind(SpanKind.PRODUCER)
                        .setAttribute("correlation.id", correlationId)
                        .setAttribute("retry.count", retryCount + 1)
                        .setAttribute("original.topic", originalTopic)
                        .startSpan();
                
                try {
                    // Update headers for the retry
                    Map<String, String> headers = new HashMap<>(message.getHeaders());
                    headers.put(HEADER_RETRY_COUNT, String.valueOf(retryCount + 1));
                    
                    // Create a new message envelope with updated headers
                    MessageEnvelope retryMessage = new MessageEnvelope(
                            message.getPayload(),
                            headers);
                    
                    // Use circuit breaker pattern for the retry
                    CircuitBreaker circuitBreaker = createCircuitBreaker("retry-publisher");
                    Retry retry = createRetry("retry-publisher");
                    
                    // Combine circuit breaker and retry for resilience
                    Function<MessageEnvelope, Boolean> publishWithResilience = 
                            Retry.decorateFunction(retry, 
                                    CircuitBreaker.decorateFunction(circuitBreaker, 
                                            msg -> messageProducer.send(originalTopic, msg)));
                    
                    // Send the message back to the original topic
                    boolean result = publishWithResilience.apply(retryMessage);
                    
                    if (result) {
                        LOGGER.info("Successfully retried message {} to topic {}", correlationId, originalTopic);
                        span.setStatus(StatusCode.OK);
                    } else {
                        LOGGER.warn("Failed to retry message {} to topic {}", correlationId, originalTopic);
                        span.setStatus(StatusCode.ERROR, "Failed to publish retry");
                    }
                } catch (Exception e) {
                    LOGGER.error("Error retrying message {} to topic {}", correlationId, originalTopic, e);
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    
                    // If retry fails, increment retry count and send back to dead letter queue
                    if (retryCount + 1 < maxRetryAttempts) {
                        Map<String, String> headers = new HashMap<>(message.getHeaders());
                        headers.put(HEADER_RETRY_COUNT, String.valueOf(retryCount + 1));
                        headers.put(HEADER_FAILURE_REASON, e.getMessage());
                        
                        MessageEnvelope dlqMessage = new MessageEnvelope(
                                message.getPayload(),
                                headers);
                        
                        messageProducer.send(TOPIC_DEAD_LETTER_QUEUE, dlqMessage);
                    }
                } finally {
                    span.end();
                }
            } catch (Exception e) {
                LOGGER.error("Unexpected error in retry scheduler for message {}", correlationId, e);
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Calculates the retry delay using exponential backoff.
     *
     * @param retryCount The current retry count
     * @return The delay in milliseconds before the next retry
     */
    private long calculateRetryDelay(int retryCount) {
        // Calculate exponential backoff: initialDelay * (backoffMultiplier ^ retryCount)
        double delay = initialRetryDelayMs * Math.pow(retryBackoffMultiplier, retryCount);
        // Add some jitter to prevent thundering herd problem
        delay = delay * (0.75 + Math.random() * 0.5);
        // Cap the maximum delay
        return Math.min((long) delay, maxRetryDelayMs);
    }

    /**
     * Creates a circuit breaker for resilient operations.
     *
     * @param name The name of the circuit breaker
     * @return A configured CircuitBreaker instance
     */
    private CircuitBreaker createCircuitBreaker(String name) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slowCallRateThreshold(50)
                .slowCallDurationThreshold(Duration.ofSeconds(2))
                .permittedNumberOfCallsInHalfOpenState(3)
                .minimumNumberOfCalls(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
        
        return CircuitBreaker.of(name, config);
    }

    /**
     * Creates a retry configuration for resilient operations.
     *
     * @param name The name of the retry configuration
     * @return A configured Retry instance
     */
    private Retry createRetry(String name) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(1000))
                .retryExceptions(Exception.class)
                .ignoreExceptions(InterruptedException.class)
                .build();
        
        return Retry.of(name, config);
    }

    /**
     * Extracts the correlation ID from a message.
     *
     * @param message The message envelope
     * @return The correlation ID or a default value if not present
     */
    private String extractCorrelationId(MessageEnvelope message) {
        return message.getHeaders().getOrDefault(MessageHeaders.CORRELATION_ID, "unknown");
    }

    /**
     * Extracts the error type from a message.
     *
     * @param message The message envelope
     * @return The error type or UNKNOWN if not present
     */
    private ErrorType extractErrorType(MessageEnvelope message) {
        String errorTypeStr = message.getHeaders().getOrDefault(HEADER_ERROR_TYPE, ErrorType.UNKNOWN.name());
        try {
            return ErrorType.valueOf(errorTypeStr);
        } catch (IllegalArgumentException e) {
            return ErrorType.UNKNOWN;
        }
    }

    /**
     * Extracts the retry count from a message.
     *
     * @param message The message envelope
     * @return The retry count or 0 if not present
     */
    private int extractRetryCount(MessageEnvelope message) {
        String retryCountStr = message.getHeaders().getOrDefault(HEADER_RETRY_COUNT, "0");
        try {
            return Integer.parseInt(retryCountStr);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * Extracts the original topic from a message.
     *
     * @param message The message envelope
     * @return The original topic or null if not present
     */
    private String extractOriginalTopic(MessageEnvelope message) {
        return message.getHeaders().getOrDefault(HEADER_ORIGINAL_TOPIC, null);
    }

    /**
     * Extracts the failure reason from a message.
     *
     * @param message The message envelope
     * @return The failure reason or "unknown" if not present
     */
    private String extractFailureReason(MessageEnvelope message) {
        return message.getHeaders().getOrDefault(HEADER_FAILURE_REASON, "unknown");
    }

    /**
     * Cleans up resources when the service is shutting down.
     */
    @Override
    public void close() {
        try {
            scheduler.shutdown();
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while shutting down scheduler", e);
        } finally {
            super.close();
        }
    }
}