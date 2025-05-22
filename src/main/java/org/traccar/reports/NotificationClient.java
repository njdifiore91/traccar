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
package org.traccar.reports;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.User;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Client for sending report completion notifications to the Notification Service.
 * Provides integration with the Notification Service for sending notifications to users
 * when reports are completed. Supports multiple notification channels and implements
 * retry logic for failed notification delivery.
 */
@Singleton
public class NotificationClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationClient.class);

    private final Config config;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final ScheduledExecutorService executorService;
    private final Map<String, NotificationStatus> notificationStatusMap;

    /**
     * Status of a notification delivery attempt.
     */
    public enum NotificationStatus {
        PENDING,
        DELIVERED,
        FAILED
    }

    /**
     * Constructs a new NotificationClient with the specified configuration.
     *
     * @param config The system configuration
     */
    @Inject
    public NotificationClient(Config config) {
        this.config = config;
        this.notificationStatusMap = new HashMap<>();
        this.executorService = Executors.newScheduledThreadPool(2);

        // Configure Circuit Breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("notificationService");

        // Configure Retry
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofSeconds(2))
                .retryExceptions(Exception.class)
                .build();

        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry("notificationService");

        // Register event listeners for monitoring
        registerEventListeners();
    }

    /**
     * Registers event listeners for circuit breaker and retry events.
     */
    private void registerEventListeners() {
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker state changed from {} to {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));

        retry.getEventPublisher()
                .onRetry(event -> LOGGER.info("Retry attempt {} after {} ms",
                        event.getNumberOfRetryAttempts(),
                        event.getWaitInterval().toMillis()));
    }

    /**
     * Sends a report completion notification to a user.
     *
     * @param user The user to notify
     * @param reportType The type of report that was completed
     * @param reportId The ID of the completed report
     * @param metadata Additional metadata about the report
     * @return The notification ID that can be used to track delivery status
     */
    public String sendReportCompletionNotification(User user, String reportType, String reportId, Map<String, Object> metadata) {
        String notificationId = UUID.randomUUID().toString();
        notificationStatusMap.put(notificationId, NotificationStatus.PENDING);

        try {
            // Create notification request with report metadata
            Map<String, Object> notificationRequest = createNotificationRequest(user, reportType, reportId, metadata);
            
            // Send notification with circuit breaker and retry
            CompletableFuture.supplyAsync(() -> {
                return Retry.decorateSupplier(retry, () -> 
                    CircuitBreaker.decorateSupplier(circuitBreaker, () -> 
                        sendNotificationToService(notificationId, notificationRequest))
                    .get())
                .get();
            }, executorService)
            .thenAccept(result -> {
                if (result) {
                    notificationStatusMap.put(notificationId, NotificationStatus.DELIVERED);
                    LOGGER.info("Notification {} delivered successfully", notificationId);
                } else {
                    notificationStatusMap.put(notificationId, NotificationStatus.FAILED);
                    LOGGER.warn("Notification {} delivery failed", notificationId);
                }
            })
            .exceptionally(e -> {
                notificationStatusMap.put(notificationId, NotificationStatus.FAILED);
                LOGGER.error("Error sending notification {}: {}", notificationId, e.getMessage(), e);
                return null;
            });

        } catch (Exception e) {
            notificationStatusMap.put(notificationId, NotificationStatus.FAILED);
            LOGGER.error("Failed to create notification request: {}", e.getMessage(), e);
        }

        return notificationId;
    }

    /**
     * Creates a notification request with report metadata.
     *
     * @param user The user to notify
     * @param reportType The type of report that was completed
     * @param reportId The ID of the completed report
     * @param metadata Additional metadata about the report
     * @return A map containing the notification request data
     */
    private Map<String, Object> createNotificationRequest(User user, String reportType, String reportId, Map<String, Object> metadata) {
        Map<String, Object> request = new HashMap<>();
        
        request.put("userId", user.getId());
        request.put("userEmail", user.getEmail());
        request.put("type", "REPORT_COMPLETION");
        request.put("reportType", reportType);
        request.put("reportId", reportId);
        request.put("timestamp", System.currentTimeMillis());
        
        // Add notification channels based on user preferences
        Map<String, Boolean> channels = new HashMap<>();
        channels.put("email", true); // Default to email notifications
        
        // Check if SMS notifications are enabled for the user
        if (user.hasAttribute("notificationSms") && Boolean.parseBoolean(user.getString("notificationSms"))) {
            channels.put("sms", true);
        }
        
        // Check if push notifications are enabled for the user
        if (user.hasAttribute("notificationPush") && Boolean.parseBoolean(user.getString("notificationPush"))) {
            channels.put("push", true);
        }
        
        request.put("channels", channels);
        
        // Add report metadata
        if (metadata != null && !metadata.isEmpty()) {
            request.put("metadata", metadata);
        }
        
        return request;
    }

    /**
     * Sends a notification request to the Notification Service.
     * In a microservices architecture, this would typically publish a message to a message broker
     * (Kafka/RabbitMQ) that the Notification Service subscribes to.
     *
     * @param notificationId The ID of the notification
     * @param notificationRequest The notification request data
     * @return true if the notification was sent successfully, false otherwise
     */
    private boolean sendNotificationToService(String notificationId, Map<String, Object> notificationRequest) {
        try {
            // In a real implementation, this would publish to a message broker
            // For example, using a Kafka or RabbitMQ client
            
            // Simulate message broker publishing
            String notificationServiceTopic = config.getString(Keys.NOTIFICATION_SERVICE_TOPIC, "notifications");
            LOGGER.debug("Publishing notification {} to topic {}: {}", notificationId, notificationServiceTopic, notificationRequest);
            
            // Simulate network latency and potential failures
            simulateNetworkConditions();
            
            return true;
        } catch (Exception e) {
            LOGGER.error("Failed to send notification to service: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Simulates network conditions for testing purposes.
     * This method would be removed in a production implementation.
     */
    private void simulateNetworkConditions() throws Exception {
        // Simulate network latency
        Thread.sleep(50 + (long) (Math.random() * 100));
        
        // Simulate occasional failures (10% chance)
        if (Math.random() < 0.1) {
            throw new Exception("Simulated network failure");
        }
    }

    /**
     * Gets the current status of a notification.
     *
     * @param notificationId The ID of the notification to check
     * @return The current status of the notification, or null if the notification ID is not found
     */
    public NotificationStatus getNotificationStatus(String notificationId) {
        return notificationStatusMap.get(notificationId);
    }

    /**
     * Schedules a report completion notification to be sent after a delay.
     *
     * @param user The user to notify
     * @param reportType The type of report that was completed
     * @param reportId The ID of the completed report
     * @param metadata Additional metadata about the report
     * @param delaySeconds The delay in seconds before sending the notification
     * @return The notification ID that can be used to track delivery status
     */
    public String scheduleReportCompletionNotification(User user, String reportType, String reportId, 
                                                     Map<String, Object> metadata, long delaySeconds) {
        String notificationId = UUID.randomUUID().toString();
        notificationStatusMap.put(notificationId, NotificationStatus.PENDING);
        
        executorService.schedule(() -> {
            sendReportCompletionNotification(user, reportType, reportId, metadata);
        }, delaySeconds, TimeUnit.SECONDS);
        
        return notificationId;
    }

    /**
     * Cleans up resources when the client is no longer needed.
     * Should be called when the application is shutting down.
     */
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}