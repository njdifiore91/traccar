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
package org.traccar.notificators;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.traccar.model.Event;
import org.traccar.model.Notification;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.MessageException;
import org.traccar.notification.NotificationMessage;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Processes failed notification messages from the dead letter queue, implementing specialized
 * recovery strategies for different notification channels. It analyzes failure patterns, categorizes errors,
 * and applies appropriate retry or fallback mechanisms.
 */
@Component
public class NotificatorDeadLetterHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificatorDeadLetterHandler.class);

    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final int INITIAL_BACKOFF_SECONDS = 5;
    private static final int MAX_BACKOFF_SECONDS = 300; // 5 minutes
    
    private final Map<String, CircuitBreaker> channelCircuitBreakers = new HashMap<>();
    private final Map<String, Integer> failureCounters = new HashMap<>();
    private final ScheduledExecutorService scheduler;
    
    private final NotificatorCircuitBreaker circuitBreakerFactory;
    private final Map<String, Notificator> notificators;

    @Autowired
    public NotificatorDeadLetterHandler(
            NotificatorCircuitBreaker circuitBreakerFactory,
            Map<String, Notificator> notificators) {
        this.circuitBreakerFactory = circuitBreakerFactory;
        this.notificators = notificators;
        this.scheduler = Executors.newScheduledThreadPool(2);
        initializeCircuitBreakers();
    }

    /**
     * Initialize circuit breakers for each notification channel
     */
    private void initializeCircuitBreakers() {
        for (String channel : notificators.keySet()) {
            channelCircuitBreakers.put(channel, circuitBreakerFactory.createCircuitBreaker(channel));
            failureCounters.put(channel, 0);
        }
    }

    /**
     * Process a failed notification message from the dead letter queue
     *
     * @param channel The notification channel that failed (email, sms, push, etc.)
     * @param user The user who should receive the notification
     * @param message The notification message content
     * @param event The event that triggered the notification
     * @param position The position associated with the event
     * @param correlationId The correlation ID for distributed tracing
     * @param errorType The type of error that occurred
     * @param retryCount The number of times this notification has been retried
     * @return CompletableFuture<Boolean> indicating success or failure
     */
    public CompletableFuture<Boolean> processFailedNotification(
            String channel,
            User user,
            NotificationMessage message,
            Event event,
            Position position,
            String correlationId,
            String errorType,
            int retryCount) {
        
        LOGGER.debug("Processing failed notification for channel: {}, correlationId: {}, retryCount: {}", 
                channel, correlationId, retryCount);
        
        // Analyze error type and determine recovery strategy
        RecoveryStrategy strategy = analyzeErrorAndDetermineStrategy(channel, errorType, retryCount);
        
        // Apply the recovery strategy
        return applyRecoveryStrategy(strategy, channel, user, message, event, position, correlationId, retryCount);
    }

    /**
     * Analyze the error type and determine the appropriate recovery strategy
     *
     * @param channel The notification channel
     * @param errorType The type of error that occurred
     * @param retryCount The number of times this notification has been retried
     * @return The recovery strategy to apply
     */
    private RecoveryStrategy analyzeErrorAndDetermineStrategy(String channel, String errorType, int retryCount) {
        // Check if we've exceeded max retry attempts
        if (retryCount >= MAX_RETRY_ATTEMPTS) {
            return RecoveryStrategy.FALLBACK_CHANNEL;
        }
        
        // Check circuit breaker state
        CircuitBreaker circuitBreaker = channelCircuitBreakers.get(channel);
        if (circuitBreaker != null && circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            return RecoveryStrategy.FALLBACK_CHANNEL;
        }
        
        // Categorize error and determine strategy
        switch (errorType) {
            case "NETWORK_ERROR":
            case "TIMEOUT_ERROR":
            case "SERVICE_UNAVAILABLE":
                return RecoveryStrategy.RETRY_WITH_BACKOFF;
                
            case "AUTHENTICATION_ERROR":
            case "AUTHORIZATION_ERROR":
                // These errors won't be resolved by retrying
                return RecoveryStrategy.ALERT_ADMIN;
                
            case "RATE_LIMIT_ERROR":
                return RecoveryStrategy.RETRY_WITH_LONGER_BACKOFF;
                
            case "INVALID_RECIPIENT":
            case "INVALID_CONTENT":
                return RecoveryStrategy.LOG_AND_DISCARD;
                
            default:
                return RecoveryStrategy.RETRY_WITH_BACKOFF;
        }
    }

    /**
     * Apply the determined recovery strategy
     *
     * @param strategy The recovery strategy to apply
     * @param channel The notification channel
     * @param user The user who should receive the notification
     * @param message The notification message content
     * @param event The event that triggered the notification
     * @param position The position associated with the event
     * @param correlationId The correlation ID for distributed tracing
     * @param retryCount The number of times this notification has been retried
     * @return CompletableFuture<Boolean> indicating success or failure
     */
    private CompletableFuture<Boolean> applyRecoveryStrategy(
            RecoveryStrategy strategy,
            String channel,
            User user,
            NotificationMessage message,
            Event event,
            Position position,
            String correlationId,
            int retryCount) {
        
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        
        switch (strategy) {
            case RETRY_WITH_BACKOFF:
                scheduleRetryWithBackoff(channel, user, message, event, position, correlationId, retryCount, result);
                break;
                
            case RETRY_WITH_LONGER_BACKOFF:
                scheduleRetryWithLongerBackoff(channel, user, message, event, position, correlationId, retryCount, result);
                break;
                
            case FALLBACK_CHANNEL:
                attemptFallbackChannel(channel, user, message, event, position, correlationId, result);
                break;
                
            case ALERT_ADMIN:
                alertAdministrator(channel, user, message, event, correlationId);
                result.complete(false); // Mark as failed since we're not retrying
                break;
                
            case LOG_AND_DISCARD:
                LOGGER.warn("Discarding invalid notification for channel: {}, correlationId: {}, error: invalid content or recipient",
                        channel, correlationId);
                result.complete(false); // Mark as failed since we're not retrying
                break;
        }
        
        return result;
    }

    /**
     * Schedule a retry with exponential backoff
     */
    private void scheduleRetryWithBackoff(
            String channel,
            User user,
            NotificationMessage message,
            Event event,
            Position position,
            String correlationId,
            int retryCount,
            CompletableFuture<Boolean> result) {
        
        // Calculate backoff time with exponential increase
        int backoffSeconds = INITIAL_BACKOFF_SECONDS * (1 << retryCount);
        backoffSeconds = Math.min(backoffSeconds, MAX_BACKOFF_SECONDS);
        
        LOGGER.debug("Scheduling retry for channel: {}, correlationId: {}, retryCount: {}, backoff: {} seconds",
                channel, correlationId, retryCount, backoffSeconds);
        
        // Schedule the retry
        scheduler.schedule(() -> {
            try {
                executeNotificatorWithCircuitBreaker(channel, user, message, event, position)
                        .thenAccept(success -> {
                            if (success) {
                                LOGGER.info("Retry successful for channel: {}, correlationId: {}, retryCount: {}",
                                        channel, correlationId, retryCount);
                                resetFailureCounter(channel);
                                result.complete(true);
                            } else {
                                LOGGER.warn("Retry failed for channel: {}, correlationId: {}, retryCount: {}",
                                        channel, correlationId, retryCount);
                                result.complete(false);
                            }
                        });
            } catch (Exception e) {
                LOGGER.error("Error during retry for channel: {}, correlationId: {}", channel, correlationId, e);
                result.complete(false);
            }
        }, backoffSeconds, TimeUnit.SECONDS);
    }

    /**
     * Schedule a retry with a longer backoff period (for rate limiting issues)
     */
    private void scheduleRetryWithLongerBackoff(
            String channel,
            User user,
            NotificationMessage message,
            Event event,
            Position position,
            String correlationId,
            int retryCount,
            CompletableFuture<Boolean> result) {
        
        // For rate limiting, use a longer backoff
        int backoffSeconds = INITIAL_BACKOFF_SECONDS * (1 << (retryCount + 2)); // More aggressive backoff
        backoffSeconds = Math.min(backoffSeconds, MAX_BACKOFF_SECONDS);
        
        LOGGER.debug("Scheduling retry with longer backoff for channel: {}, correlationId: {}, backoff: {} seconds",
                channel, correlationId, backoffSeconds);
        
        // Schedule the retry with longer backoff
        scheduler.schedule(() -> {
            try {
                executeNotificatorWithCircuitBreaker(channel, user, message, event, position)
                        .thenAccept(success -> {
                            if (success) {
                                LOGGER.info("Retry successful after longer backoff for channel: {}, correlationId: {}",
                                        channel, correlationId);
                                resetFailureCounter(channel);
                                result.complete(true);
                            } else {
                                LOGGER.warn("Retry failed after longer backoff for channel: {}, correlationId: {}",
                                        channel, correlationId);
                                result.complete(false);
                            }
                        });
            } catch (Exception e) {
                LOGGER.error("Error during retry with longer backoff for channel: {}, correlationId: {}",
                        channel, correlationId, e);
                result.complete(false);
            }
        }, backoffSeconds, TimeUnit.SECONDS);
    }

    /**
     * Attempt to send the notification through a fallback channel
     */
    private void attemptFallbackChannel(
            String channel,
            User user,
            NotificationMessage message,
            Event event,
            Position position,
            String correlationId,
            CompletableFuture<Boolean> result) {
        
        String fallbackChannel = determineFallbackChannel(channel);
        
        if (fallbackChannel != null && !fallbackChannel.equals(channel)) {
            LOGGER.info("Attempting fallback from channel: {} to channel: {}, correlationId: {}",
                    channel, fallbackChannel, correlationId);
            
            try {
                executeNotificatorWithCircuitBreaker(fallbackChannel, user, message, event, position)
                        .thenAccept(success -> {
                            if (success) {
                                LOGGER.info("Fallback successful using channel: {}, correlationId: {}",
                                        fallbackChannel, correlationId);
                                result.complete(true);
                            } else {
                                LOGGER.warn("Fallback failed using channel: {}, correlationId: {}",
                                        fallbackChannel, correlationId);
                                result.complete(false);
                            }
                        });
            } catch (Exception e) {
                LOGGER.error("Error during fallback to channel: {}, correlationId: {}",
                        fallbackChannel, correlationId, e);
                result.complete(false);
            }
        } else {
            LOGGER.warn("No fallback channel available for: {}, correlationId: {}",
                    channel, correlationId);
            result.complete(false);
        }
    }

    /**
     * Determine the appropriate fallback channel based on the original channel
     *
     * @param originalChannel The original notification channel that failed
     * @return The fallback channel to use, or null if no fallback is available
     */
    private String determineFallbackChannel(String originalChannel) {
        // Implement channel fallback logic
        switch (originalChannel) {
            case "email":
                return "sms";
            case "sms":
                return "push";
            case "push":
                return "web";
            case "web":
                return "email";
            default:
                // For other channels, try web as fallback if available
                return notificators.containsKey("web") ? "web" : null;
        }
    }

    /**
     * Alert administrator about persistent notification failures
     */
    private void alertAdministrator(
            String channel,
            User user,
            NotificationMessage message,
            Event event,
            String correlationId) {
        
        LOGGER.error("Critical notification failure for channel: {}, user: {}, correlationId: {}",
                channel, user.getId(), correlationId);
        
        // Here you would implement logic to alert administrators
        // This could be through a dedicated admin notification channel,
        // logging to a monitoring system, etc.
    }

    /**
     * Execute a notificator with circuit breaker protection
     */
    private CompletableFuture<Boolean> executeNotificatorWithCircuitBreaker(
            String channel,
            User user,
            NotificationMessage message,
            Event event,
            Position position) {
        
        Notificator notificator = notificators.get(channel);
        if (notificator == null) {
            LOGGER.warn("Notificator not found for channel: {}", channel);
            return CompletableFuture.completedFuture(false);
        }
        
        CircuitBreaker circuitBreaker = channelCircuitBreakers.get(channel);
        if (circuitBreaker == null) {
            // If no circuit breaker exists for this channel, create one
            circuitBreaker = circuitBreakerFactory.createCircuitBreaker(channel);
            channelCircuitBreakers.put(channel, circuitBreaker);
        }
        
        // Create retry with exponential backoff
        Retry retry = Retry.of(channel + "-retry", config -> config
                .maxAttempts(2) // Small number of immediate retries
                .waitDuration(Duration.ofSeconds(1))
                .retryOnException(e -> e instanceof MessageException));
        
        // Create a supplier that will execute the notificator
        Supplier<Boolean> notificatorSupplier = () -> {
            try {
                notificator.send(user, message, event, position);
                return true;
            } catch (MessageException e) {
                incrementFailureCounter(channel);
                LOGGER.warn("Notification failed for channel: {}", channel, e);
                return false;
            }
        };
        
        // Decorate the supplier with circuit breaker and retry
        Supplier<Boolean> decoratedSupplier = CircuitBreaker
                .decorateSupplier(circuitBreaker, notificatorSupplier);
        
        // Further decorate with retry
        decoratedSupplier = Retry
                .decorateSupplier(retry, decoratedSupplier);
        
        // Execute asynchronously
        return CompletableFuture.supplyAsync(decoratedSupplier)
                .exceptionally(ex -> {
                    LOGGER.error("Exception during notification execution for channel: {}", channel, ex);
                    return false;
                });
    }

    /**
     * Increment the failure counter for a channel
     */
    private void incrementFailureCounter(String channel) {
        failureCounters.compute(channel, (k, v) -> v == null ? 1 : v + 1);
    }

    /**
     * Reset the failure counter for a channel after a successful delivery
     */
    private void resetFailureCounter(String channel) {
        failureCounters.put(channel, 0);
    }

    /**
     * Recovery strategies for failed notifications
     */
    private enum RecoveryStrategy {
        RETRY_WITH_BACKOFF,         // Retry with exponential backoff
        RETRY_WITH_LONGER_BACKOFF,  // Retry with longer backoff (for rate limiting)
        FALLBACK_CHANNEL,           // Try an alternative notification channel
        ALERT_ADMIN,                // Alert administrator (for auth errors)
        LOG_AND_DISCARD             // Log and discard (for invalid content/recipient)
    }
}