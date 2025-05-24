/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.traccar.model.Notification;
import org.traccar.model.User;
import org.traccar.notification.MessageException;
import org.traccar.sms.SmsManager;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Implements a consumer for the 'sms-out' topic that processes SMS notifications and delivers them
 * through SMS gateways or providers like AWS SNS. It handles text message formatting, rate limiting,
 * and delivery status tracking. This component is responsible for the final delivery of SMS notifications
 * to recipients, with support for multiple SMS providers and fallback mechanisms.
 */
@Singleton
@Component
public class SmsNotificationConsumer extends BaseMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmsNotificationConsumer.class);

    @Inject
    private SmsManager smsManager;

    @Value("${aws.sns.enabled:false}")
    private boolean awsSnsEnabled;

    @Value("${aws.sns.region:us-east-1}")
    private String awsSnsRegion;

    @Value("${sms.rate.limit.period:60}")
    private int rateLimitPeriod; // in seconds

    @Value("${sms.rate.limit.limit:100}")
    private int rateLimitLimit; // max requests per period

    private AmazonSNS snsClient;
    private CircuitBreaker primaryCircuitBreaker;
    private CircuitBreaker fallbackCircuitBreaker;
    private RateLimiter smsRateLimiter;

    // Metrics
    private Counter smsDeliveredCounter;
    private Counter smsFailedCounter;
    private Timer smsDeliveryTimer;

    @PostConstruct
    public void init() {
        super.init();
        initializeCircuitBreakers();
        initializeRateLimiter();
        initializeMetrics();
        initializeAwsSns();
        LOGGER.info("SMS Notification Consumer initialized with rate limit of {} requests per {} seconds",
                rateLimitLimit, rateLimitPeriod);
    }

    private void initializeCircuitBreakers() {
        // Configure circuit breaker for primary SMS provider
        CircuitBreakerConfig primaryConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to open circuit
                .waitDurationInOpenState(Duration.ofMinutes(1))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .recordExceptions(MessageException.class, Exception.class)
                .build();

        // Configure circuit breaker for fallback SMS provider
        CircuitBreakerConfig fallbackConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to open circuit
                .waitDurationInOpenState(Duration.ofMinutes(2)) // Longer wait time for fallback
                .permittedNumberOfCallsInHalfOpenState(3)
                .slidingWindowSize(10)
                .recordExceptions(MessageException.class, Exception.class)
                .build();

        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(primaryConfig);
        primaryCircuitBreaker = registry.circuitBreaker("smsPrimaryProvider");
        fallbackCircuitBreaker = CircuitBreakerRegistry.of(fallbackConfig).circuitBreaker("smsFallbackProvider");

        // Add state transition logging
        primaryCircuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.warn("Primary SMS circuit breaker state changed from {} to {}",
                        event.getStateTransition().getFromState(), event.getStateTransition().getToState()));

        fallbackCircuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.warn("Fallback SMS circuit breaker state changed from {} to {}",
                        event.getStateTransition().getFromState(), event.getStateTransition().getToState()));
    }

    private void initializeRateLimiter() {
        RateLimiterConfig config = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofSeconds(rateLimitPeriod))
                .limitForPeriod(rateLimitLimit)
                .timeoutDuration(Duration.ofSeconds(5)) // How long to wait for permission
                .build();

        RateLimiterRegistry registry = RateLimiterRegistry.of(config);
        smsRateLimiter = registry.rateLimiter("smsRateLimiter");

        // Add events logging
        smsRateLimiter.getEventPublisher()
                .onFailure(event -> LOGGER.warn("SMS rate limit exceeded: {}", event.toString()));
    }

    private void initializeMetrics() {
        smsDeliveredCounter = meterRegistry.counter("notification.sms.delivered");
        smsFailedCounter = meterRegistry.counter("notification.sms.failed");
        smsDeliveryTimer = meterRegistry.timer("notification.sms.delivery.time");
    }

    private void initializeAwsSns() {
        if (awsSnsEnabled) {
            try {
                snsClient = AmazonSNSClientBuilder.standard()
                        .withRegion(awsSnsRegion)
                        .build();
                LOGGER.info("AWS SNS client initialized for region {}", awsSnsRegion);
            } catch (Exception e) {
                LOGGER.error("Failed to initialize AWS SNS client: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * Consumes SMS notification messages from the 'sms-out' topic.
     *
     * @param smsNotification The SMS notification message to process
     * @param correlationId   The correlation ID for tracking the message across services
     */
    @KafkaListener(topics = "${kafka.topic.sms-out}", groupId = "${kafka.consumer.group-id}")
    public void consumeSmsNotification(SmsNotificationMessage smsNotification, String correlationId) {
        LOGGER.debug("Received SMS notification message: {}, correlationId: {}", smsNotification, correlationId);

        if (smsNotification == null || smsNotification.getUser() == null || 
                smsNotification.getNotification() == null) {
            LOGGER.error("Invalid SMS notification message received: {}", smsNotification);
            return;
        }

        User user = smsNotification.getUser();
        Notification notification = smsNotification.getNotification();

        if (user.getPhone() == null || user.getPhone().isEmpty()) {
            LOGGER.warn("User {} has no phone number configured, skipping SMS notification", user.getId());
            return;
        }

        try {
            // Apply rate limiting
            boolean permissionAcquired = smsRateLimiter.acquirePermission();
            if (!permissionAcquired) {
                LOGGER.warn("SMS rate limit exceeded, publishing to dead letter queue: {}", correlationId);
                publishToDeadLetterQueue(notification, user, correlationId, 
                        new MessageException("SMS rate limit exceeded"));
                smsFailedCounter.increment();
                return;
            }

            // Attempt delivery with primary provider first, then fallback if needed
            Timer.Sample sample = Timer.start(meterRegistry);
            boolean delivered = deliverSmsWithFallback(user.getPhone(), notification.getBody(), 
                    notification.isCommand(), correlationId);
            sample.stop(smsDeliveryTimer);

            if (delivered) {
                smsDeliveredCounter.increment();
                LOGGER.debug("SMS notification delivered successfully: {}", correlationId);
                // Publish delivery status to notification-status topic
                publishDeliveryStatus(notification, user, correlationId, true, null);
            } else {
                smsFailedCounter.increment();
                LOGGER.error("Failed to deliver SMS notification after all attempts: {}", correlationId);
                // Publish to dead letter queue for retry
                publishToDeadLetterQueue(notification, user, correlationId, 
                        new MessageException("All SMS delivery attempts failed"));
            }
        } catch (Exception e) {
            smsFailedCounter.increment();
            LOGGER.error("Error processing SMS notification: {}, {}", correlationId, e.getMessage(), e);
            publishToDeadLetterQueue(notification, user, correlationId, e);
        }
    }

    /**
     * Attempts to deliver an SMS using the primary provider, falling back to secondary if needed.
     *
     * @param phoneNumber   The recipient's phone number
     * @param message       The message content
     * @param isCommand     Whether this is a command message
     * @param correlationId The correlation ID for tracking
     * @return true if delivery was successful with any provider, false otherwise
     */
    private boolean deliverSmsWithFallback(String phoneNumber, String message, boolean isCommand, String correlationId) {
        // Try primary provider (SmsManager) first
        try {
            boolean primaryResult = deliverWithPrimaryProvider(phoneNumber, message, isCommand, correlationId);
            if (primaryResult) {
                return true;
            }
        } catch (Exception e) {
            LOGGER.warn("Primary SMS provider failed: {}, {}", correlationId, e.getMessage());
        }

        // If primary fails and AWS SNS is enabled, try it as fallback
        if (awsSnsEnabled && snsClient != null) {
            try {
                boolean fallbackResult = deliverWithFallbackProvider(phoneNumber, message, correlationId);
                if (fallbackResult) {
                    return true;
                }
            } catch (Exception e) {
                LOGGER.error("Fallback SMS provider (AWS SNS) failed: {}, {}", correlationId, e.getMessage());
            }
        }

        return false;
    }

    /**
     * Delivers SMS using the primary provider (SmsManager) with circuit breaker protection.
     *
     * @param phoneNumber   The recipient's phone number
     * @param message       The message content
     * @param isCommand     Whether this is a command message
     * @param correlationId The correlation ID for tracking
     * @return true if delivery was successful, false otherwise
     */
    private boolean deliverWithPrimaryProvider(String phoneNumber, String message, boolean isCommand, String correlationId) {
        Supplier<Boolean> decoratedSupplier = CircuitBreaker.decorateSupplier(
                primaryCircuitBreaker,
                () -> {
                    try {
                        CompletableFuture<Void> future = smsManager.sendMessageAsync(phoneNumber, message, isCommand, correlationId);
                        future.get(30, TimeUnit.SECONDS); // Wait for completion with timeout
                        return true;
                    } catch (Exception e) {
                        LOGGER.warn("Error sending SMS via primary provider: {}", e.getMessage());
                        throw new RuntimeException("Primary SMS delivery failed", e);
                    }
                });

        try {
            return decoratedSupplier.get();
        } catch (Exception e) {
            LOGGER.warn("Circuit breaker prevented SMS delivery via primary provider: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Delivers SMS using the fallback provider (AWS SNS) with circuit breaker protection.
     *
     * @param phoneNumber   The recipient's phone number
     * @param message       The message content
     * @param correlationId The correlation ID for tracking
     * @return true if delivery was successful, false otherwise
     */
    private boolean deliverWithFallbackProvider(String phoneNumber, String message, String correlationId) {
        Supplier<Boolean> decoratedSupplier = CircuitBreaker.decorateSupplier(
                fallbackCircuitBreaker,
                () -> {
                    try {
                        PublishRequest publishRequest = new PublishRequest()
                                .withMessage(message)
                                .withPhoneNumber(phoneNumber)
                                .withMessageAttributes(java.util.Map.of(
                                        "correlationId", new com.amazonaws.services.sns.model.MessageAttributeValue()
                                                .withDataType("String")
                                                .withStringValue(correlationId)
                                ));

                        PublishResult result = snsClient.publish(publishRequest);
                        LOGGER.debug("AWS SNS message sent with ID: {}", result.getMessageId());
                        return true;
                    } catch (Exception e) {
                        LOGGER.warn("Error sending SMS via AWS SNS: {}", e.getMessage());
                        throw new RuntimeException("AWS SNS delivery failed", e);
                    }
                });

        try {
            return decoratedSupplier.get();
        } catch (Exception e) {
            LOGGER.warn("Circuit breaker prevented SMS delivery via AWS SNS: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Publishes delivery status to the notification-status topic.
     *
     * @param notification  The notification that was processed
     * @param user          The target user for the notification
     * @param correlationId The correlation ID for tracking
     * @param success       Whether delivery was successful
     * @param errorMessage  Error message if delivery failed, null otherwise
     */
    private void publishDeliveryStatus(Notification notification, User user, String correlationId, 
                                      boolean success, String errorMessage) {
        try {
            NotificationStatusMessage statusMessage = new NotificationStatusMessage(
                    notification.getId(),
                    user.getId(),
                    "sms",
                    success,
                    errorMessage,
                    System.currentTimeMillis()
            );

            kafkaTemplate.send(notificationStatusTopic, correlationId, statusMessage);
        } catch (Exception e) {
            LOGGER.error("Failed to publish delivery status: {}", e.getMessage(), e);
        }
    }

    /**
     * Message class for SMS notifications.
     */
    public static class SmsNotificationMessage {
        private Notification notification;
        private User user;
        private String message;

        public SmsNotificationMessage() {
        }

        public SmsNotificationMessage(Notification notification, User user, String message) {
            this.notification = notification;
            this.user = user;
            this.message = message;
        }

        public Notification getNotification() {
            return notification;
        }

        public void setNotification(Notification notification) {
            this.notification = notification;
        }

        public User getUser() {
            return user;
        }

        public void setUser(User user) {
            this.user = user;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        @Override
        public String toString() {
            return "SmsNotificationMessage{" +
                    "notificationId=" + (notification != null ? notification.getId() : "null") +
                    ", userId=" + (user != null ? user.getId() : "null") +
                    "}";
        }
    }

    /**
     * Message class for notification delivery status.
     */
    public static class NotificationStatusMessage {
        private long notificationId;
        private long userId;
        private String channel;
        private boolean success;
        private String errorMessage;
        private long timestamp;

        public NotificationStatusMessage() {
        }

        public NotificationStatusMessage(long notificationId, long userId, String channel, 
                                        boolean success, String errorMessage, long timestamp) {
            this.notificationId = notificationId;
            this.userId = userId;
            this.channel = channel;
            this.success = success;
            this.errorMessage = errorMessage;
            this.timestamp = timestamp;
        }

        public long getNotificationId() {
            return notificationId;
        }

        public void setNotificationId(long notificationId) {
            this.notificationId = notificationId;
        }

        public long getUserId() {
            return userId;
        }

        public void setUserId(long userId) {
            this.userId = userId;
        }

        public String getChannel() {
            return channel;
        }

        public void setChannel(String channel) {
            this.channel = channel;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}