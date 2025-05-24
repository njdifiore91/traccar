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
package org.traccar.notification;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.traccar.messaging.MessageConsumer;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHeaders;
import org.traccar.messaging.MessageProducer;
import org.traccar.service.RetryService;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Processes failed notification messages from the dead letter queue,
 * implementing retry logic with exponential backoff and preserving the original message context.
 * It analyzes failure patterns, categorizes errors, and applies appropriate recovery strategies.
 */
@Component
public class DeadLetterQueueHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeadLetterQueueHandler.class);

    private static final String RETRY_NAME = "deadLetterQueueRetry";
    private static final String ORIGINAL_TOPIC_HEADER = "x-original-topic";
    private static final String ERROR_TYPE_HEADER = "x-error-type";
    private static final String RETRY_COUNT_HEADER = "x-retry-count";
    private static final String CORRELATION_ID_HEADER = "x-correlation-id";
    private static final String ERROR_MESSAGE_HEADER = "x-error-message";

    /**
     * Error categories for failed notifications
     */
    public enum ErrorCategory {
        NETWORK(true),           // Network connectivity issues, typically transient
        AUTHENTICATION(false),   // Authentication failures, typically configuration issues
        RATE_LIMITING(true),     // Rate limiting by external services, retry with longer backoff
        SERVICE_UNAVAILABLE(true), // External service unavailable, retry after delay
        INVALID_FORMAT(false),   // Message format issues, typically code or template problems
        INVALID_RECIPIENT(false), // Invalid recipient address, typically data issues
        BROKER_FAILURE(true),    // Message broker connectivity issues
        UNKNOWN(true);           // Uncategorized errors, attempt retry

        private final boolean retryable;

        ErrorCategory(boolean retryable) {
            this.retryable = retryable;
        }

        public boolean isRetryable() {
            return retryable;
        }
    }

    private final MessageConsumer deadLetterConsumer;
    private final MessageProducer messageProducer;
    private final RetryService retryService;
    private final RetryRegistry retryRegistry;
    private final MeterRegistry meterRegistry;

    private final String deadLetterTopic;
    private final int maxRetryAttempts;
    private final long initialBackoffMillis;
    private final double backoffMultiplier;
    private final long maxBackoffMillis;

    private Counter processedCounter;
    private Counter retriedCounter;
    private Counter discardedCounter;
    private Map<ErrorCategory, Counter> errorCategoryCounters;
    private Timer processingTimer;

    @Autowired
    public DeadLetterQueueHandler(
            MessageConsumer deadLetterConsumer,
            MessageProducer messageProducer,
            RetryService retryService,
            RetryRegistry retryRegistry,
            MeterRegistry meterRegistry,
            @Value("${notification.deadletter.topic:notification.deadletter}") String deadLetterTopic,
            @Value("${notification.retry.maxAttempts:5}") int maxRetryAttempts,
            @Value("${notification.retry.initialBackoffMillis:1000}") long initialBackoffMillis,
            @Value("${notification.retry.backoffMultiplier:2.0}") double backoffMultiplier,
            @Value("${notification.retry.maxBackoffMillis:300000}") long maxBackoffMillis) {
        this.deadLetterConsumer = deadLetterConsumer;
        this.messageProducer = messageProducer;
        this.retryService = retryService;
        this.retryRegistry = retryRegistry;
        this.meterRegistry = meterRegistry;
        this.deadLetterTopic = deadLetterTopic;
        this.maxRetryAttempts = maxRetryAttempts;
        this.initialBackoffMillis = initialBackoffMillis;
        this.backoffMultiplier = backoffMultiplier;
        this.maxBackoffMillis = maxBackoffMillis;
    }

    @PostConstruct
    public void init() {
        // Initialize metrics
        processedCounter = meterRegistry.counter("notification.deadletter.processed");
        retriedCounter = meterRegistry.counter("notification.deadletter.retried");
        discardedCounter = meterRegistry.counter("notification.deadletter.discarded");
        errorCategoryCounters = new HashMap<>();
        for (ErrorCategory category : ErrorCategory.values()) {
            errorCategoryCounters.put(category, 
                meterRegistry.counter("notification.deadletter.category", "type", category.name()));
        }
        processingTimer = meterRegistry.timer("notification.deadletter.processing.time");

        // Configure retry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(maxRetryAttempts)
                .waitDuration(Duration.ofMillis(initialBackoffMillis))
                .retryExceptions(Exception.class)
                .ignoreExceptions(InterruptedException.class)
                .intervalFunction(attempt -> {
                    long backoff = (long) (initialBackoffMillis * Math.pow(backoffMultiplier, attempt - 1));
                    return Math.min(backoff, maxBackoffMillis);
                })
                .build();
        retryRegistry.addConfiguration(RETRY_NAME, retryConfig);

        // Subscribe to dead letter queue
        deadLetterConsumer.subscribe(deadLetterTopic, this::processDeadLetterMessage);
        LOGGER.info("Dead letter queue handler initialized for topic: {}", deadLetterTopic);
    }

    /**
     * Process a message from the dead letter queue
     *
     * @param messageEnvelope The message envelope containing the failed message and metadata
     */
    public void processDeadLetterMessage(MessageEnvelope messageEnvelope) {
        processingTimer.record(() -> {
            try {
                processedCounter.increment();
                LOGGER.debug("Processing dead letter message: {}", messageEnvelope);

                // Extract metadata from headers
                String correlationId = extractCorrelationId(messageEnvelope);
                String originalTopic = extractOriginalTopic(messageEnvelope);
                ErrorCategory errorCategory = categorizeError(messageEnvelope);
                int retryCount = extractRetryCount(messageEnvelope);

                // Log with correlation ID for tracing
                LOGGER.info("Processing dead letter message [correlationId={}, errorCategory={}, retryCount={}]", 
                    correlationId, errorCategory, retryCount);

                // Increment category counter
                errorCategoryCounters.get(errorCategory).increment();

                // Determine if message should be retried
                if (shouldRetryMessage(errorCategory, retryCount)) {
                    retryMessage(messageEnvelope, originalTopic, errorCategory, retryCount, correlationId);
                } else {
                    handleNonRetryableMessage(messageEnvelope, errorCategory, correlationId);
                }
            } catch (Exception e) {
                LOGGER.error("Error processing dead letter message", e);
            }
        });
    }

    /**
     * Extract the correlation ID from the message headers, or generate a new one if not present
     */
    private String extractCorrelationId(MessageEnvelope messageEnvelope) {
        return messageEnvelope.getHeaders().getOrDefault(
                CORRELATION_ID_HEADER, 
                "dlq-" + System.currentTimeMillis() + "-" + Math.random()).toString();
    }

    /**
     * Extract the original topic from the message headers
     */
    private String extractOriginalTopic(MessageEnvelope messageEnvelope) {
        return messageEnvelope.getHeaders().getOrDefault(ORIGINAL_TOPIC_HEADER, "notification.unknown").toString();
    }

    /**
     * Extract the retry count from the message headers, defaulting to 0 if not present
     */
    private int extractRetryCount(MessageEnvelope messageEnvelope) {
        Object retryCountObj = messageEnvelope.getHeaders().get(RETRY_COUNT_HEADER);
        if (retryCountObj instanceof Number) {
            return ((Number) retryCountObj).intValue();
        } else if (retryCountObj instanceof String) {
            try {
                return Integer.parseInt((String) retryCountObj);
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }

    /**
     * Categorize the error based on message headers and content
     */
    private ErrorCategory categorizeError(MessageEnvelope messageEnvelope) {
        // Check if error type is explicitly set in headers
        Object errorTypeObj = messageEnvelope.getHeaders().get(ERROR_TYPE_HEADER);
        if (errorTypeObj instanceof String) {
            try {
                return ErrorCategory.valueOf((String) errorTypeObj);
            } catch (IllegalArgumentException e) {
                // Invalid error type, continue with analysis
            }
        }

        // Analyze error message for categorization
        String errorMessage = extractErrorMessage(messageEnvelope);
        if (errorMessage == null) {
            return ErrorCategory.UNKNOWN;
        }

        // Categorize based on error message patterns
        errorMessage = errorMessage.toLowerCase();
        if (errorMessage.contains("network") || errorMessage.contains("connection") || 
            errorMessage.contains("timeout") || errorMessage.contains("unreachable")) {
            return ErrorCategory.NETWORK;
        } else if (errorMessage.contains("auth") || errorMessage.contains("credential") || 
                  errorMessage.contains("permission") || errorMessage.contains("unauthorized")) {
            return ErrorCategory.AUTHENTICATION;
        } else if (errorMessage.contains("rate") || errorMessage.contains("limit") || 
                  errorMessage.contains("throttle") || errorMessage.contains("quota")) {
            return ErrorCategory.RATE_LIMITING;
        } else if (errorMessage.contains("unavailable") || errorMessage.contains("down") || 
                  errorMessage.contains("maintenance")) {
            return ErrorCategory.SERVICE_UNAVAILABLE;
        } else if (errorMessage.contains("format") || errorMessage.contains("invalid") || 
                  errorMessage.contains("malformed")) {
            return ErrorCategory.INVALID_FORMAT;
        } else if (errorMessage.contains("recipient") || errorMessage.contains("address") || 
                  errorMessage.contains("destination")) {
            return ErrorCategory.INVALID_RECIPIENT;
        } else if (errorMessage.contains("broker") || errorMessage.contains("kafka") || 
                  errorMessage.contains("rabbitmq") || errorMessage.contains("queue")) {
            return ErrorCategory.BROKER_FAILURE;
        }

        return ErrorCategory.UNKNOWN;
    }

    /**
     * Extract the error message from the message headers
     */
    private String extractErrorMessage(MessageEnvelope messageEnvelope) {
        Object errorMessageObj = messageEnvelope.getHeaders().get(ERROR_MESSAGE_HEADER);
        return errorMessageObj != null ? errorMessageObj.toString() : null;
    }

    /**
     * Determine if a message should be retried based on error category and retry count
     */
    private boolean shouldRetryMessage(ErrorCategory errorCategory, int retryCount) {
        // Check if the error category is retryable
        if (!errorCategory.isRetryable()) {
            LOGGER.info("Message with non-retryable error category {} will not be retried", errorCategory);
            return false;
        }

        // Check if we've exceeded the maximum retry attempts
        if (retryCount >= maxRetryAttempts) {
            LOGGER.info("Message exceeded maximum retry attempts ({}), will not be retried", maxRetryAttempts);
            return false;
        }

        return true;
    }

    /**
     * Retry a message with exponential backoff
     */
    private void retryMessage(MessageEnvelope messageEnvelope, String originalTopic, 
                             ErrorCategory errorCategory, int retryCount, String correlationId) {
        try {
            // Increment retry count
            int newRetryCount = retryCount + 1;
            
            // Calculate backoff delay
            long backoffMillis = calculateBackoffMillis(newRetryCount, errorCategory);
            
            // Update headers for retry
            Map<String, Object> headers = new HashMap<>(messageEnvelope.getHeaders());
            headers.put(RETRY_COUNT_HEADER, newRetryCount);
            headers.put(CORRELATION_ID_HEADER, correlationId);
            headers.put(MessageHeaders.TIMESTAMP, System.currentTimeMillis());
            
            // Create new message envelope with updated headers
            MessageEnvelope retryEnvelope = new MessageEnvelope(
                messageEnvelope.getPayload(),
                headers,
                messageEnvelope.getKey());
            
            // Schedule retry with backoff
            LOGGER.info("Scheduling retry #{} for message [correlationId={}] with {}ms backoff to topic {}", 
                newRetryCount, correlationId, backoffMillis, originalTopic);
            
            // Use retry service to schedule the retry with appropriate backoff
            retryService.scheduleRetry(() -> {
                messageProducer.send(originalTopic, retryEnvelope);
                return null;
            }, backoffMillis, TimeUnit.MILLISECONDS);
            
            retriedCounter.increment();
            
        } catch (Exception e) {
            LOGGER.error("Failed to schedule retry for message [correlationId={}]", correlationId, e);
            discardedCounter.increment();
        }
    }

    /**
     * Calculate backoff delay based on retry count and error category
     */
    private long calculateBackoffMillis(int retryCount, ErrorCategory errorCategory) {
        // Base calculation with exponential backoff
        long baseBackoff = (long) (initialBackoffMillis * Math.pow(backoffMultiplier, retryCount - 1));
        
        // Apply category-specific multipliers
        double categoryMultiplier = 1.0;
        switch (errorCategory) {
            case RATE_LIMITING:
                // Rate limiting errors need longer backoff
                categoryMultiplier = 2.0;
                break;
            case SERVICE_UNAVAILABLE:
                // Service unavailable errors need longer backoff
                categoryMultiplier = 1.5;
                break;
            case NETWORK:
                // Network errors might resolve quickly
                categoryMultiplier = 1.0;
                break;
            case BROKER_FAILURE:
                // Broker failures need moderate backoff
                categoryMultiplier = 1.2;
                break;
            default:
                categoryMultiplier = 1.0;
        }
        
        // Apply jitter to prevent thundering herd (±10%)
        double jitter = 0.9 + (Math.random() * 0.2);
        
        // Calculate final backoff with category multiplier and jitter
        long finalBackoff = (long) (baseBackoff * categoryMultiplier * jitter);
        
        // Ensure we don't exceed maximum backoff
        return Math.min(finalBackoff, maxBackoffMillis);
    }

    /**
     * Handle a message that should not be retried
     */
    private void handleNonRetryableMessage(MessageEnvelope messageEnvelope, 
                                          ErrorCategory errorCategory, 
                                          String correlationId) {
        LOGGER.warn("Message [correlationId={}] with error category {} will not be retried", 
            correlationId, errorCategory);
        
        // Log detailed information for manual intervention
        LOGGER.info("Non-retryable message details [correlationId={}]: headers={}, payload={}", 
            correlationId, messageEnvelope.getHeaders(), messageEnvelope.getPayload());
        
        // For certain categories, we might want to alert operations
        if (errorCategory == ErrorCategory.AUTHENTICATION || 
            errorCategory == ErrorCategory.INVALID_FORMAT) {
            // These likely require configuration or code changes
            LOGGER.error("Critical non-retryable error [correlationId={}]: {} - requires manual intervention", 
                correlationId, errorCategory);
            // In a real implementation, this might trigger an alert to operations
        }
        
        discardedCounter.increment();
    }
}