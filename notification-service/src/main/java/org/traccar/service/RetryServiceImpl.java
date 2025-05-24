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
package org.traccar.service;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.retry.event.RetryEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.traccar.notification.MessageException;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Implementation of the RetryService interface that provides a robust retry mechanism
 * with exponential backoff and jitter for failed notification deliveries.
 * This implementation schedules retries with increasing delays, tracks attempt history,
 * and routes exhausted retries to dead letter queues.
 */
@Service
public class RetryServiceImpl implements RetryService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryServiceImpl.class);

    private final Map<String, Map<String, List<RetryAttempt>>> retryHistory = new ConcurrentHashMap<>();
    private final Map<String, RetryConfig> channelRetryConfigs = new ConcurrentHashMap<>();
    private final Map<String, Integer> retryAttemptCounts = new ConcurrentHashMap<>();
    
    private final RetryRegistry retryRegistry;
    private final ScheduledExecutorService executorService;
    private final MeterRegistry meterRegistry;
    private final MessageBrokerService messageBrokerService;

    @Value("${notification.retry.maxAttempts:3}")
    private int defaultMaxAttempts;

    @Value("${notification.retry.initialInterval:500}")
    private long defaultInitialIntervalMs;

    @Value("${notification.retry.multiplier:2.0}")
    private double defaultMultiplier;

    @Value("${notification.retry.randomizationFactor:0.5}")
    private double defaultRandomizationFactor;

    @Value("${notification.retry.channel.email.maxAttempts:5}")
    private int emailMaxAttempts;

    @Value("${notification.retry.channel.email.initialInterval:1000}")
    private long emailInitialIntervalMs;

    @Value("${notification.retry.channel.sms.maxAttempts:3}")
    private int smsMaxAttempts;

    @Value("${notification.retry.channel.sms.initialInterval:2000}")
    private long smsInitialIntervalMs;

    @Value("${notification.retry.channel.push.maxAttempts:2}")
    private int pushMaxAttempts;

    @Value("${notification.retry.channel.push.initialInterval:1000}")
    private long pushInitialIntervalMs;

    /**
     * Constructor for RetryServiceImpl.
     *
     * @param retryRegistry The Resilience4j RetryRegistry for creating and managing Retry objects
     * @param executorService The ScheduledExecutorService for scheduling retry tasks
     * @param meterRegistry The MeterRegistry for collecting metrics
     * @param messageBrokerService The MessageBrokerService for publishing to dead letter queues
     */
    @Autowired
    public RetryServiceImpl(RetryRegistry retryRegistry, 
                           ScheduledExecutorService executorService,
                           MeterRegistry meterRegistry,
                           MessageBrokerService messageBrokerService) {
        this.retryRegistry = retryRegistry;
        this.executorService = executorService;
        this.meterRegistry = meterRegistry;
        this.messageBrokerService = messageBrokerService;
    }

    /**
     * Initialize the retry service with channel-specific retry configurations.
     */
    @PostConstruct
    public void init() {
        // Configure default retry config
        RetryConfig defaultConfig = RetryConfig.custom()
                .maxAttempts(defaultMaxAttempts)
                .waitDuration(Duration.ofMillis(defaultInitialIntervalMs))
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(
                        defaultInitialIntervalMs, defaultMultiplier, defaultRandomizationFactor))
                .retryExceptions(MessageException.class)
                .build();

        // Configure channel-specific retry configs
        channelRetryConfigs.put("email", RetryConfig.custom()
                .maxAttempts(emailMaxAttempts)
                .waitDuration(Duration.ofMillis(emailInitialIntervalMs))
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(
                        emailInitialIntervalMs, defaultMultiplier, defaultRandomizationFactor))
                .retryExceptions(MessageException.class)
                .build());

        channelRetryConfigs.put("sms", RetryConfig.custom()
                .maxAttempts(smsMaxAttempts)
                .waitDuration(Duration.ofMillis(smsInitialIntervalMs))
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(
                        smsInitialIntervalMs, defaultMultiplier, defaultRandomizationFactor))
                .retryExceptions(MessageException.class)
                .build());

        channelRetryConfigs.put("push", RetryConfig.custom()
                .maxAttempts(pushMaxAttempts)
                .waitDuration(Duration.ofMillis(pushInitialIntervalMs))
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(
                        pushInitialIntervalMs, defaultMultiplier, defaultRandomizationFactor))
                .retryExceptions(MessageException.class)
                .build());

        // Register event consumers for metrics and logging
        retryRegistry.getEventPublisher().onEvent(event -> {
            RetryEvent retryEvent = (RetryEvent) event;
            String channelId = retryEvent.getName();
            String notificationId = extractNotificationId(retryEvent);
            
            if (retryEvent.getEventType() == RetryEvent.Type.RETRY) {
                incrementRetryAttemptCount(channelId, notificationId);
                meterRegistry.counter("resilience4j.retry.attempts", 
                        List.of(Tag.of("channel", channelId), Tag.of("notification", notificationId)))
                        .increment();
                LOGGER.debug("Retry attempt for notification {}: {}", notificationId, retryEvent);
            } else if (retryEvent.getEventType() == RetryEvent.Type.SUCCESS) {
                meterRegistry.counter("resilience4j.retry.success", 
                        List.of(Tag.of("channel", channelId), Tag.of("notification", notificationId)))
                        .increment();
                LOGGER.info("Retry succeeded for notification {}: {}", notificationId, retryEvent);
            } else if (retryEvent.getEventType() == RetryEvent.Type.ERROR) {
                meterRegistry.counter("resilience4j.retry.error", 
                        List.of(Tag.of("channel", channelId), Tag.of("notification", notificationId)))
                        .increment();
                LOGGER.error("Retry error for notification {}: {}", notificationId, retryEvent);
            }
        });

        LOGGER.info("RetryService initialized with configurations for email, sms, and push channels");
    }

    /**
     * Extract the notification ID from a RetryEvent.
     *
     * @param retryEvent The RetryEvent
     * @return The notification ID
     */
    private String extractNotificationId(RetryEvent retryEvent) {
        // In a real implementation, this would extract the notification ID from the context
        // For simplicity, we'll return a placeholder
        return "unknown";
    }

    /**
     * Increment the retry attempt count for a notification.
     *
     * @param channelId The channel ID
     * @param notificationId The notification ID
     */
    private void incrementRetryAttemptCount(String channelId, String notificationId) {
        String key = channelId + ":" + notificationId;
        retryAttemptCounts.compute(key, (k, v) -> (v == null) ? 1 : v + 1);
    }

    /**
     * Get the retry attempt count for a notification.
     *
     * @param channelId The channel ID
     * @param notificationId The notification ID
     * @return The retry attempt count
     */
    private int getRetryAttemptCount(String channelId, String notificationId) {
        String key = channelId + ":" + notificationId;
        return retryAttemptCounts.getOrDefault(key, 0);
    }

    /**
     * Get the RetryConfig for a specific channel.
     *
     * @param channelId The channel ID
     * @return The RetryConfig for the channel, or the default RetryConfig if not found
     */
    private RetryConfig getRetryConfigForChannel(String channelId) {
        return channelRetryConfigs.getOrDefault(channelId, channelRetryConfigs.get("default"));
    }

    /**
     * Get the maximum number of retry attempts for a specific channel.
     *
     * @param channelId The channel ID
     * @return The maximum number of retry attempts
     */
    private int getMaxAttemptsForChannel(String channelId) {
        RetryConfig config = getRetryConfigForChannel(channelId);
        return config.getMaxAttempts();
    }

    @Override
    @Async
    public CompletableFuture<Void> scheduleRetry(String channelId, String notificationId, Object payload,
                                               MessageException exception, Map<String, Object> metadata) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        try {
            // Get the retry config for the channel
            RetryConfig retryConfig = getRetryConfigForChannel(channelId);
            
            // Create a retry with the channel-specific config
            Retry retry = retryRegistry.retry(channelId, retryConfig);
            
            // Get the current attempt number
            int attemptNumber = getRetryAttemptCount(channelId, notificationId) + 1;
            
            // Check if we've exceeded the retry limit
            if (attemptNumber > retryConfig.getMaxAttempts()) {
                LOGGER.warn("Retry limit exceeded for notification {} on channel {}", notificationId, channelId);
                publishToDeadLetterQueue(channelId, notificationId, payload, exception, metadata)
                        .thenRun(() -> future.complete(null))
                        .exceptionally(ex -> {
                            future.completeExceptionally(ex);
                            return null;
                        });
                return future;
            }
            
            // Calculate the delay for this retry attempt using exponential backoff with jitter
            long delayMs = calculateDelayWithJitter(channelId, attemptNumber);
            
            // Record the retry attempt
            recordRetryAttempt(channelId, notificationId, attemptNumber, exception, delayMs);
            
            // Schedule the retry with the calculated delay
            executorService.schedule(() -> {
                try {
                    // Execute the retry logic
                    Supplier<Void> retryableSupplier = Retry.decorateSupplier(retry, () -> {
                        // This would typically call the notification delivery logic
                        // For now, we'll just log the retry attempt
                        LOGGER.info("Executing retry attempt {} for notification {} on channel {}",
                                attemptNumber, notificationId, channelId);
                        return null;
                    });
                    
                    retryableSupplier.get();
                    future.complete(null);
                } catch (Exception e) {
                    // If the retry fails, schedule another retry or publish to DLQ if limit reached
                    if (attemptNumber >= retryConfig.getMaxAttempts()) {
                        publishToDeadLetterQueue(channelId, notificationId, payload, exception, metadata)
                                .thenRun(() -> future.complete(null))
                                .exceptionally(ex -> {
                                    future.completeExceptionally(ex);
                                    return null;
                                });
                    } else {
                        scheduleRetry(channelId, notificationId, payload, exception, metadata)
                                .thenRun(() -> future.complete(null))
                                .exceptionally(ex -> {
                                    future.completeExceptionally(ex);
                                    return null;
                                });
                    }
                }
            }, delayMs, TimeUnit.MILLISECONDS);
            
            // Increment metrics for scheduled retries
            meterRegistry.counter("resilience4j.retry.scheduled", 
                    List.of(Tag.of("channel", channelId), Tag.of("notification", notificationId)))
                    .increment();
            
            LOGGER.info("Scheduled retry attempt {} for notification {} on channel {} with delay {}ms",
                    attemptNumber, notificationId, channelId, delayMs);
            
        } catch (Exception e) {
            LOGGER.error("Error scheduling retry for notification {} on channel {}: {}",
                    notificationId, channelId, e.getMessage(), e);
            future.completeExceptionally(e);
        }
        
        return future;
    }

    /**
     * Calculate the delay for a retry attempt using exponential backoff with jitter.
     *
     * @param channelId The channel ID
     * @param attemptNumber The retry attempt number
     * @return The delay in milliseconds
     */
    private long calculateDelayWithJitter(String channelId, int attemptNumber) {
        RetryConfig config = getRetryConfigForChannel(channelId);
        long initialInterval = config.getWaitDuration().toMillis();
        double multiplier = 2.0; // Default multiplier
        double randomizationFactor = 0.5; // Default randomization factor
        
        // Calculate exponential backoff: initialInterval * multiplier^(attemptNumber-1)
        double exponentialBackoff = initialInterval * Math.pow(multiplier, attemptNumber - 1);
        
        // Apply jitter: exponentialBackoff * (1 ± randomizationFactor)
        double jitter = exponentialBackoff * randomizationFactor;
        double lowerBound = exponentialBackoff - jitter;
        double upperBound = exponentialBackoff + jitter;
        
        // Generate a random delay within the jitter bounds
        return (long) (lowerBound + (Math.random() * (upperBound - lowerBound)));
    }

    /**
     * Record a retry attempt in the retry history.
     *
     * @param channelId The channel ID
     * @param notificationId The notification ID
     * @param attemptNumber The retry attempt number
     * @param exception The exception that caused the delivery failure
     * @param delayMs The delay before the retry attempt
     */
    private void recordRetryAttempt(String channelId, String notificationId, int attemptNumber,
                                   MessageException exception, long delayMs) {
        RetryAttempt attempt = new RetryAttempt(
                notificationId,
                channelId,
                attemptNumber,
                System.currentTimeMillis(),
                exception.getMessage(),
                exception.getClass().getName(),
                delayMs
        );
        
        retryHistory.computeIfAbsent(channelId, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(notificationId, k -> Collections.synchronizedList(new ArrayList<>()))
                .add(attempt);
        
        // Update metrics
        meterRegistry.counter("resilience4j.retry.attempts.recorded", 
                List.of(Tag.of("channel", channelId), Tag.of("notification", notificationId)))
                .increment();
    }

    @Override
    public List<RetryAttempt> getRetryHistory(String notificationId) {
        List<RetryAttempt> attempts = new ArrayList<>();
        
        // Collect retry attempts across all channels for the given notification ID
        for (Map<String, List<RetryAttempt>> channelHistory : retryHistory.values()) {
            List<RetryAttempt> notificationAttempts = channelHistory.get(notificationId);
            if (notificationAttempts != null) {
                attempts.addAll(notificationAttempts);
            }
        }
        
        // Sort attempts by timestamp
        attempts.sort((a1, a2) -> Long.compare(a1.getTimestamp(), a2.getTimestamp()));
        
        return attempts;
    }

    @Override
    public boolean hasExceededRetryLimit(String channelId, String notificationId) {
        int attemptCount = getRetryAttemptCount(channelId, notificationId);
        int maxAttempts = getMaxAttemptsForChannel(channelId);
        return attemptCount >= maxAttempts;
    }

    @Override
    @Async
    public CompletableFuture<Void> publishToDeadLetterQueue(String channelId, String notificationId, Object payload,
                                                         MessageException exception, Map<String, Object> metadata) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        try {
            // Create a dead letter message with all relevant information
            Map<String, Object> deadLetterMessage = new HashMap<>(metadata);
            deadLetterMessage.put("notificationId", notificationId);
            deadLetterMessage.put("channelId", channelId);
            deadLetterMessage.put("payload", payload);
            deadLetterMessage.put("exceptionMessage", exception.getMessage());
            deadLetterMessage.put("exceptionType", exception.getClass().getName());
            deadLetterMessage.put("timestamp", System.currentTimeMillis());
            deadLetterMessage.put("retryHistory", getRetryHistory(notificationId));
            
            // Publish to the dead letter queue for the specific channel
            String deadLetterQueueTopic = "notification.deadletter." + channelId;
            messageBrokerService.publish(deadLetterQueueTopic, deadLetterMessage)
                    .thenRun(() -> {
                        // Update metrics for dead letter queue publications
                        meterRegistry.counter("resilience4j.retry.deadletter", 
                                List.of(Tag.of("channel", channelId), Tag.of("notification", notificationId)))
                                .increment();
                        
                        LOGGER.info("Published notification {} to dead letter queue for channel {}",
                                notificationId, channelId);
                        
                        future.complete(null);
                    })
                    .exceptionally(ex -> {
                        LOGGER.error("Error publishing to dead letter queue for notification {} on channel {}: {}",
                                notificationId, channelId, ex.getMessage(), ex);
                        future.completeExceptionally(ex);
                        return null;
                    });
        } catch (Exception e) {
            LOGGER.error("Error preparing dead letter message for notification {} on channel {}: {}",
                    notificationId, channelId, e.getMessage(), e);
            future.completeExceptionally(e);
        }
        
        return future;
    }

    @Override
    public Map<String, Object> getRetryMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        // Collect metrics for each channel
        for (String channelId : channelRetryConfigs.keySet()) {
            Map<String, Object> channelMetrics = new HashMap<>();
            
            // Count total retry attempts for this channel
            long totalAttempts = retryAttemptCounts.entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(channelId + ":"))
                    .mapToLong(Map.Entry::getValue)
                    .sum();
            
            channelMetrics.put("totalAttempts", totalAttempts);
            
            // Count notifications that exceeded retry limits
            long exceededLimitCount = retryAttemptCounts.entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(channelId + ":"))
                    .filter(entry -> entry.getValue() >= getMaxAttemptsForChannel(channelId))
                    .count();
            
            channelMetrics.put("exceededLimitCount", exceededLimitCount);
            
            // Add channel metrics to overall metrics
            metrics.put(channelId, channelMetrics);
        }
        
        // Add global metrics
        metrics.put("totalNotificationsRetried", retryAttemptCounts.size());
        
        return metrics;
    }

    /**
     * Interface for the message broker service used to publish to dead letter queues.
     * This would typically be implemented by a separate service that integrates with
     * a message broker like RabbitMQ or Kafka.
     */
    public interface MessageBrokerService {
        CompletableFuture<Void> publish(String topic, Object message);
    }

    /**
     * Interface for the interval function used to calculate retry delays.
     */
    public interface IntervalFunction {
        long apply(int attemptCount);

        /**
         * Create an exponential backoff interval function with randomization (jitter).
         *
         * @param initialIntervalMillis The initial interval in milliseconds
         * @param multiplier The multiplier for exponential backoff
         * @param randomizationFactor The randomization factor for jitter (0.0 to 1.0)
         * @return An IntervalFunction that calculates delays with exponential backoff and jitter
         */
        static IntervalFunction ofExponentialRandomBackoff(
                long initialIntervalMillis, double multiplier, double randomizationFactor) {
            return attemptCount -> {
                double exponentialBackoff = initialIntervalMillis * Math.pow(multiplier, attemptCount - 1);
                double jitter = exponentialBackoff * randomizationFactor;
                double lowerBound = exponentialBackoff - jitter;
                double upperBound = exponentialBackoff + jitter;
                return (long) (lowerBound + (Math.random() * (upperBound - lowerBound)));
            };
        }
    }
}