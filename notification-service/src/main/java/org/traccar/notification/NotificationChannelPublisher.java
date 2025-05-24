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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.traccar.messaging.MessageHeaders;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageProducerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Publishes formatted notifications to channel-specific message broker topics for asynchronous delivery.
 * Handles routing to the appropriate channel (email, SMS, push, web) based on notification type and user preferences.
 * Implements message partitioning by recipient for ordered delivery, rate limiting for external services,
 * circuit breakers for resilience, and delivery status tracking.
 */
@Service
public class NotificationChannelPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationChannelPublisher.class);

    private static final String EMAIL_TOPIC = "email-out";
    private static final String SMS_TOPIC = "sms-out";
    private static final String PUSH_TOPIC = "push-out";
    private static final String WEB_TOPIC = "web-out";
    private static final String DEAD_LETTER_TOPIC = "dead-letter-queue";

    private final MessageProducer emailProducer;
    private final MessageProducer smsProducer;
    private final MessageProducer pushProducer;
    private final MessageProducer webProducer;
    private final MessageProducer deadLetterProducer;

    private final Map<String, RateLimiter> channelRateLimiters = new ConcurrentHashMap<>();
    private final Map<String, CircuitBreaker> channelCircuitBreakers = new ConcurrentHashMap<>();
    
    // Track delivery status by notification ID
    private final Map<String, DeliveryStatus> deliveryStatusMap = new ConcurrentHashMap<>();

    /**
     * Delivery status tracking for notifications
     */
    public static class DeliveryStatus {
        private final String notificationId;
        private final long creationTime;
        private final Map<String, ChannelStatus> channelStatuses = new HashMap<>();

        public DeliveryStatus(String notificationId) {
            this.notificationId = notificationId;
            this.creationTime = System.currentTimeMillis();
        }

        public String getNotificationId() {
            return notificationId;
        }

        public long getCreationTime() {
            return creationTime;
        }

        public Map<String, ChannelStatus> getChannelStatuses() {
            return channelStatuses;
        }

        public void updateChannelStatus(String channel, ChannelStatus status) {
            channelStatuses.put(channel, status);
        }

        public boolean isComplete() {
            return channelStatuses.values().stream()
                    .allMatch(status -> status == ChannelStatus.DELIVERED || status == ChannelStatus.FAILED);
        }
    }

    /**
     * Status of notification delivery for a specific channel
     */
    public enum ChannelStatus {
        PENDING,
        SENT,
        DELIVERED,
        FAILED
    }

    @Autowired
    public NotificationChannelPublisher(MessageProducerFactory messageProducerFactory) {
        // Initialize message producers for each channel
        this.emailProducer = messageProducerFactory.createProducer(EMAIL_TOPIC);
        this.smsProducer = messageProducerFactory.createProducer(SMS_TOPIC);
        this.pushProducer = messageProducerFactory.createProducer(PUSH_TOPIC);
        this.webProducer = messageProducerFactory.createProducer(WEB_TOPIC);
        this.deadLetterProducer = messageProducerFactory.createProducer(DEAD_LETTER_TOPIC);

        // Initialize rate limiters for each channel
        initializeRateLimiters();
        
        // Initialize circuit breakers for each channel
        initializeCircuitBreakers();
    }

    /**
     * Initialize rate limiters for each notification channel
     */
    private void initializeRateLimiters() {
        // Configure rate limiters with different limits for each channel
        RateLimiterConfig emailConfig = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofMinutes(1))
                .limitForPeriod(100) // 100 emails per minute
                .timeoutDuration(Duration.ofSeconds(5))
                .build();
        channelRateLimiters.put(EMAIL_TOPIC, RateLimiter.of("email-rate-limiter", emailConfig));

        RateLimiterConfig smsConfig = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofMinutes(1))
                .limitForPeriod(50) // 50 SMS per minute
                .timeoutDuration(Duration.ofSeconds(5))
                .build();
        channelRateLimiters.put(SMS_TOPIC, RateLimiter.of("sms-rate-limiter", smsConfig));

        RateLimiterConfig pushConfig = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofMinutes(1))
                .limitForPeriod(200) // 200 push notifications per minute
                .timeoutDuration(Duration.ofSeconds(5))
                .build();
        channelRateLimiters.put(PUSH_TOPIC, RateLimiter.of("push-rate-limiter", pushConfig));

        RateLimiterConfig webConfig = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofMinutes(1))
                .limitForPeriod(500) // 500 web notifications per minute
                .timeoutDuration(Duration.ofSeconds(5))
                .build();
        channelRateLimiters.put(WEB_TOPIC, RateLimiter.of("web-rate-limiter", webConfig));
    }

    /**
     * Initialize circuit breakers for each notification channel
     */
    private void initializeCircuitBreakers() {
        // Configure circuit breakers with different thresholds for each channel
        CircuitBreakerConfig emailConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate
                .waitDurationInOpenState(Duration.ofMinutes(1))
                .permittedNumberOfCallsInHalfOpenState(10)
                .slidingWindowSize(100)
                .build();
        channelCircuitBreakers.put(EMAIL_TOPIC, CircuitBreaker.of("email-circuit-breaker", emailConfig));

        CircuitBreakerConfig smsConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate
                .waitDurationInOpenState(Duration.ofMinutes(1))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(50)
                .build();
        channelCircuitBreakers.put(SMS_TOPIC, CircuitBreaker.of("sms-circuit-breaker", smsConfig));

        CircuitBreakerConfig pushConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate
                .waitDurationInOpenState(Duration.ofMinutes(1))
                .permittedNumberOfCallsInHalfOpenState(20)
                .slidingWindowSize(100)
                .build();
        channelCircuitBreakers.put(PUSH_TOPIC, CircuitBreaker.of("push-circuit-breaker", pushConfig));

        CircuitBreakerConfig webConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate
                .waitDurationInOpenState(Duration.ofMinutes(1))
                .permittedNumberOfCallsInHalfOpenState(20)
                .slidingWindowSize(100)
                .build();
        channelCircuitBreakers.put(WEB_TOPIC, CircuitBreaker.of("web-circuit-breaker", webConfig));
    }

    /**
     * Publish a notification to the email channel
     * 
     * @param notification The notification message to publish
     * @param recipientId The recipient ID for partitioning
     * @return The notification ID for tracking
     */
    public String publishEmailNotification(NotificationMessage notification, String recipientId) {
        return publishToChannel(notification, recipientId, EMAIL_TOPIC, emailProducer);
    }

    /**
     * Publish a notification to the SMS channel
     * 
     * @param notification The notification message to publish
     * @param recipientId The recipient ID for partitioning
     * @return The notification ID for tracking
     */
    public String publishSmsNotification(NotificationMessage notification, String recipientId) {
        return publishToChannel(notification, recipientId, SMS_TOPIC, smsProducer);
    }

    /**
     * Publish a notification to the push notification channel
     * 
     * @param notification The notification message to publish
     * @param recipientId The recipient ID for partitioning
     * @return The notification ID for tracking
     */
    public String publishPushNotification(NotificationMessage notification, String recipientId) {
        return publishToChannel(notification, recipientId, PUSH_TOPIC, pushProducer);
    }

    /**
     * Publish a notification to the web notification channel
     * 
     * @param notification The notification message to publish
     * @param recipientId The recipient ID for partitioning
     * @return The notification ID for tracking
     */
    public String publishWebNotification(NotificationMessage notification, String recipientId) {
        return publishToChannel(notification, recipientId, WEB_TOPIC, webProducer);
    }

    /**
     * Publish a notification to a specific channel with rate limiting, circuit breaking, and partitioning
     * 
     * @param notification The notification message to publish
     * @param recipientId The recipient ID for partitioning
     * @param channelTopic The channel topic to publish to
     * @param producer The message producer for the channel
     * @return The notification ID for tracking
     */
    private String publishToChannel(NotificationMessage notification, String recipientId, 
                                   String channelTopic, MessageProducer producer) {
        String notificationId = UUID.randomUUID().toString();
        
        // Create delivery status tracking
        DeliveryStatus status = new DeliveryStatus(notificationId);
        status.updateChannelStatus(channelTopic, ChannelStatus.PENDING);
        deliveryStatusMap.put(notificationId, status);
        
        try {
            // Apply rate limiting
            RateLimiter rateLimiter = channelRateLimiters.get(channelTopic);
            boolean permitted = rateLimiter.acquirePermission(100); // Wait up to 100ms for permission
            
            if (!permitted) {
                LOGGER.warn("Rate limit exceeded for channel {}, sending to dead letter queue", channelTopic);
                publishToDeadLetterQueue(notification, recipientId, channelTopic, "RATE_LIMIT_EXCEEDED");
                status.updateChannelStatus(channelTopic, ChannelStatus.FAILED);
                return notificationId;
            }
            
            // Apply circuit breaker
            CircuitBreaker circuitBreaker = channelCircuitBreakers.get(channelTopic);
            if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
                LOGGER.warn("Circuit breaker open for channel {}, sending to dead letter queue", channelTopic);
                publishToDeadLetterQueue(notification, recipientId, channelTopic, "CIRCUIT_BREAKER_OPEN");
                status.updateChannelStatus(channelTopic, ChannelStatus.FAILED);
                return notificationId;
            }
            
            // Create message headers with correlation ID and notification ID
            Map<String, Object> headers = new HashMap<>();
            headers.put(MessageHeaders.CORRELATION_ID, notification.getCorrelationId() != null ? 
                    notification.getCorrelationId() : UUID.randomUUID().toString());
            headers.put(MessageHeaders.MESSAGE_ID, notificationId);
            headers.put("notificationType", notification.getType());
            headers.put("recipientId", recipientId);
            
            // Publish message with partition key (recipientId) for ordered delivery
            circuitBreaker.executeSupplier(() -> {
                producer.send(notification, headers, recipientId, (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.error("Failed to publish notification to channel {}: {}", 
                                channelTopic, exception.getMessage(), exception);
                        status.updateChannelStatus(channelTopic, ChannelStatus.FAILED);
                        publishToDeadLetterQueue(notification, recipientId, channelTopic, 
                                "PUBLISH_FAILED: " + exception.getMessage());
                    } else {
                        LOGGER.debug("Successfully published notification {} to channel {}", 
                                notificationId, channelTopic);
                        status.updateChannelStatus(channelTopic, ChannelStatus.SENT);
                    }
                });
                return true;
            });
            
            return notificationId;
        } catch (Exception e) {
            LOGGER.error("Error publishing notification to channel {}: {}", 
                    channelTopic, e.getMessage(), e);
            status.updateChannelStatus(channelTopic, ChannelStatus.FAILED);
            publishToDeadLetterQueue(notification, recipientId, channelTopic, 
                    "EXCEPTION: " + e.getMessage());
            return notificationId;
        }
    }

    /**
     * Publish a failed notification to the dead letter queue for later processing
     * 
     * @param notification The original notification message
     * @param recipientId The recipient ID
     * @param originalTopic The original topic that failed
     * @param errorReason The reason for the failure
     */
    private void publishToDeadLetterQueue(NotificationMessage notification, String recipientId, 
                                         String originalTopic, String errorReason) {
        try {
            Map<String, Object> headers = new HashMap<>();
            headers.put(MessageHeaders.CORRELATION_ID, notification.getCorrelationId() != null ? 
                    notification.getCorrelationId() : UUID.randomUUID().toString());
            headers.put("originalTopic", originalTopic);
            headers.put("errorReason", errorReason);
            headers.put("failureTime", System.currentTimeMillis());
            headers.put("recipientId", recipientId);
            
            deadLetterProducer.send(notification, headers, recipientId, (metadata, exception) -> {
                if (exception != null) {
                    LOGGER.error("Failed to publish to dead letter queue: {}", 
                            exception.getMessage(), exception);
                } else {
                    LOGGER.debug("Successfully published to dead letter queue");
                }
            });
        } catch (Exception e) {
            LOGGER.error("Error publishing to dead letter queue: {}", e.getMessage(), e);
        }
    }

    /**
     * Update the delivery status of a notification
     * 
     * @param notificationId The notification ID
     * @param channel The channel (topic)
     * @param status The new status
     */
    public void updateDeliveryStatus(String notificationId, String channel, ChannelStatus status) {
        DeliveryStatus deliveryStatus = deliveryStatusMap.get(notificationId);
        if (deliveryStatus != null) {
            deliveryStatus.updateChannelStatus(channel, status);
            
            // If all channels are complete, schedule for removal after some time
            if (deliveryStatus.isComplete()) {
                scheduleStatusCleanup(notificationId);
            }
        }
    }

    /**
     * Get the current delivery status of a notification
     * 
     * @param notificationId The notification ID
     * @return The delivery status, or null if not found
     */
    public DeliveryStatus getDeliveryStatus(String notificationId) {
        return deliveryStatusMap.get(notificationId);
    }

    /**
     * Schedule cleanup of delivery status after it's complete
     * 
     * @param notificationId The notification ID to clean up
     */
    private void scheduleStatusCleanup(String notificationId) {
        // In a real implementation, this would use a scheduled executor service
        // For simplicity, we'll just remove it after a delay in a separate thread
        new Thread(() -> {
            try {
                // Keep status for 1 hour after completion
                TimeUnit.HOURS.sleep(1);
                deliveryStatusMap.remove(notificationId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }
}