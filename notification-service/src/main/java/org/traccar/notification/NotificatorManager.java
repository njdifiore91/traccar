/*
 * Copyright 2016 - 2023 Anton Tananaev (anton@traccar.org)
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
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Service;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHeaders;
import org.traccar.messaging.MessageProducer;
import org.traccar.notificators.Notificator;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Manager for notification delivery via various channels.
 * Uses message broker for asynchronous notification dispatch and implements
 * resilience patterns for external service calls.
 */
@Service
@ConfigurationProperties(prefix = "notification")
public class NotificatorManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificatorManager.class);

    private final Map<String, NotificatorConfig> notificatorConfigs = new HashMap<>();
    private final Map<String, CircuitBreaker> circuitBreakers = new ConcurrentHashMap<>();
    private final Map<String, RateLimiter> rateLimiters = new ConcurrentHashMap<>();

    private final MessageProducer messageProducer;
    private final ServiceDiscovery serviceDiscovery;
    private final MeterRegistry meterRegistry;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RateLimiterRegistry rateLimiterRegistry;

    private boolean initialized = false;

    /**
     * Configuration properties for each notificator type.
     */
    public static class NotificatorConfig {
        private boolean enabled = true;
        private String topicName;
        private int rateLimitForPeriod = 100;
        private Duration rateLimitPeriod = Duration.ofMinutes(1);
        private Duration circuitBreakerWaitDuration = Duration.ofSeconds(60);
        private float circuitBreakerFailureRateThreshold = 50.0f;
        private int circuitBreakerMinimumNumberOfCalls = 10;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getTopicName() {
            return topicName;
        }

        public void setTopicName(String topicName) {
            this.topicName = topicName;
        }

        public int getRateLimitForPeriod() {
            return rateLimitForPeriod;
        }

        public void setRateLimitForPeriod(int rateLimitForPeriod) {
            this.rateLimitForPeriod = rateLimitForPeriod;
        }

        public Duration getRateLimitPeriod() {
            return rateLimitPeriod;
        }

        public void setRateLimitPeriod(Duration rateLimitPeriod) {
            this.rateLimitPeriod = rateLimitPeriod;
        }

        public Duration getCircuitBreakerWaitDuration() {
            return circuitBreakerWaitDuration;
        }

        public void setCircuitBreakerWaitDuration(Duration circuitBreakerWaitDuration) {
            this.circuitBreakerWaitDuration = circuitBreakerWaitDuration;
        }

        public float getCircuitBreakerFailureRateThreshold() {
            return circuitBreakerFailureRateThreshold;
        }

        public void setCircuitBreakerFailureRateThreshold(float circuitBreakerFailureRateThreshold) {
            this.circuitBreakerFailureRateThreshold = circuitBreakerFailureRateThreshold;
        }

        public int getCircuitBreakerMinimumNumberOfCalls() {
            return circuitBreakerMinimumNumberOfCalls;
        }

        public void setCircuitBreakerMinimumNumberOfCalls(int circuitBreakerMinimumNumberOfCalls) {
            this.circuitBreakerMinimumNumberOfCalls = circuitBreakerMinimumNumberOfCalls;
        }
    }

    /**
     * Constructor for NotificatorManager.
     *
     * @param messageProducer Message producer for sending notifications to broker
     * @param serviceDiscovery Service discovery for finding notificator endpoints
     * @param meterRegistry Meter registry for metrics collection
     * @param circuitBreakerRegistry Circuit breaker registry for resilience patterns
     * @param rateLimiterRegistry Rate limiter registry for throttling
     */
    @Autowired
    public NotificatorManager(
            MessageProducer messageProducer,
            ServiceDiscovery serviceDiscovery,
            MeterRegistry meterRegistry,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RateLimiterRegistry rateLimiterRegistry) {
        this.messageProducer = messageProducer;
        this.serviceDiscovery = serviceDiscovery;
        this.meterRegistry = meterRegistry;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.rateLimiterRegistry = rateLimiterRegistry;
    }

    /**
     * Initialize the NotificatorManager.
     * Sets up default configurations for notificators and initializes resilience components.
     */
    @PostConstruct
    public void init() {
        // Set up default configurations if not provided externally
        setupDefaultConfigurations();
        
        // Initialize circuit breakers and rate limiters for each notificator
        for (Map.Entry<String, NotificatorConfig> entry : notificatorConfigs.entrySet()) {
            String type = entry.getKey();
            NotificatorConfig config = entry.getValue();
            
            if (config.isEnabled()) {
                setupCircuitBreaker(type, config);
                setupRateLimiter(type, config);
                LOGGER.info("Initialized notificator: {}", type);
            } else {
                LOGGER.info("Notificator disabled: {}", type);
            }
        }
        
        initialized = true;
    }

    /**
     * Set up default configurations for standard notificator types.
     */
    private void setupDefaultConfigurations() {
        if (notificatorConfigs.isEmpty()) {
            // Email notificator configuration
            NotificatorConfig emailConfig = new NotificatorConfig();
            emailConfig.setTopicName("email-out");
            notificatorConfigs.put("mail", emailConfig);

            // SMS notificator configuration
            NotificatorConfig smsConfig = new NotificatorConfig();
            smsConfig.setTopicName("sms-out");
            smsConfig.setRateLimitForPeriod(50); // Lower rate limit for SMS
            notificatorConfigs.put("sms", smsConfig);

            // Push notificator configuration
            NotificatorConfig pushConfig = new NotificatorConfig();
            pushConfig.setTopicName("push-out");
            notificatorConfigs.put("firebase", pushConfig);

            // Web notificator configuration
            NotificatorConfig webConfig = new NotificatorConfig();
            webConfig.setTopicName("web-out");
            notificatorConfigs.put("web", webConfig);

            // Telegram notificator configuration
            NotificatorConfig telegramConfig = new NotificatorConfig();
            telegramConfig.setTopicName("telegram-out");
            telegramConfig.setRateLimitForPeriod(30); // Lower rate limit for Telegram API
            notificatorConfigs.put("telegram", telegramConfig);

            // Pushover notificator configuration
            NotificatorConfig pushoverConfig = new NotificatorConfig();
            pushoverConfig.setTopicName("pushover-out");
            pushoverConfig.setRateLimitForPeriod(30); // Lower rate limit for Pushover API
            notificatorConfigs.put("pushover", pushoverConfig);
        }
    }

    /**
     * Set up a circuit breaker for a notificator type.
     *
     * @param type Notificator type
     * @param config Notificator configuration
     */
    private void setupCircuitBreaker(String type, NotificatorConfig config) {
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(config.getCircuitBreakerFailureRateThreshold())
                .waitDurationInOpenState(config.getCircuitBreakerWaitDuration())
                .minimumNumberOfCalls(config.getCircuitBreakerMinimumNumberOfCalls())
                .build();

        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(
                "notificator-" + type, circuitBreakerConfig);

        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    LOGGER.info("Circuit breaker '{}' state changed from {} to {}",
                            event.getCircuitBreakerName(),
                            event.getStateTransition().getFromState(),
                            event.getStateTransition().getToState());
                    
                    // Record metric for circuit breaker state change
                    meterRegistry.counter(
                            "notification.circuit_breaker.state_change",
                            "type", type,
                            "from", event.getStateTransition().getFromState().name(),
                            "to", event.getStateTransition().getToState().name())
                            .increment();
                });

        circuitBreakers.put(type, circuitBreaker);
    }

    /**
     * Set up a rate limiter for a notificator type.
     *
     * @param type Notificator type
     * @param config Notificator configuration
     */
    private void setupRateLimiter(String type, NotificatorConfig config) {
        RateLimiterConfig rateLimiterConfig = RateLimiterConfig.custom()
                .limitRefreshPeriod(config.getRateLimitPeriod())
                .limitForPeriod(config.getRateLimitForPeriod())
                .timeoutDuration(Duration.ofSeconds(5))
                .build();

        RateLimiter rateLimiter = rateLimiterRegistry.rateLimiter(
                "notificator-" + type, rateLimiterConfig);

        rateLimiter.getEventPublisher()
                .onFailure(event -> {
                    LOGGER.warn("Rate limit exceeded for notificator: {}", type);
                    
                    // Record metric for rate limit exceeded
                    meterRegistry.counter(
                            "notification.rate_limit.exceeded",
                            "type", type)
                            .increment();
                });

        rateLimiters.put(type, rateLimiter);
    }

    /**
     * Clean up resources when the manager is being destroyed.
     */
    @PreDestroy
    public void destroy() {
        LOGGER.info("Shutting down NotificatorManager");
        initialized = false;
    }

    /**
     * Get notificator configurations.
     *
     * @return Map of notificator configurations by type
     */
    public Map<String, NotificatorConfig> getNotificatorConfigs() {
        return notificatorConfigs;
    }

    /**
     * Send a notification message to the appropriate channel.
     *
     * @param type Notificator type (mail, sms, firebase, etc.)
     * @param message Notification message to send
     * @throws MessageException If there's an error sending the message
     */
    public void sendNotification(String type, NotificationMessage message) throws MessageException {
        if (!initialized) {
            throw new MessageException("NotificatorManager not initialized");
        }

        NotificatorConfig config = notificatorConfigs.get(type);
        if (config == null || !config.isEnabled()) {
            throw new MessageException("Notificator not configured or disabled: " + type);
        }

        // Create timer for measuring notification processing time
        Timer.Sample sample = Timer.start(meterRegistry);

        try {
            // Apply rate limiting
            RateLimiter rateLimiter = rateLimiters.get(type);
            if (rateLimiter != null) {
                rateLimiter.acquirePermission();
            }

            // Discover service endpoints if needed for direct calls
            discoverServiceEndpoints(type);

            // Create message envelope with headers
            MessageEnvelope envelope = createMessageEnvelope(type, message);

            // Apply circuit breaker pattern for external service calls
            CircuitBreaker circuitBreaker = circuitBreakers.get(type);
            if (circuitBreaker != null) {
                Supplier<Boolean> decoratedSupplier = CircuitBreaker.decorateSupplier(
                        circuitBreaker, () -> publishMessage(config.getTopicName(), envelope));
                decoratedSupplier.get();
            } else {
                publishMessage(config.getTopicName(), envelope);
            }

            // Record successful notification
            meterRegistry.counter(
                    "notification.sent",
                    "type", type,
                    "status", "success")
                    .increment();

            // Record notification timing
            sample.stop(meterRegistry.timer(
                    "notification.processing.time",
                    "type", type));

        } catch (Exception e) {
            // Record failed notification
            meterRegistry.counter(
                    "notification.sent",
                    "type", type,
                    "status", "failure",
                    "reason", e.getClass().getSimpleName())
                    .increment();

            // Record notification timing even for failures
            sample.stop(meterRegistry.timer(
                    "notification.processing.time",
                    "type", type));

            LOGGER.warn("Failed to send notification: {}", type, e);
            throw new MessageException(e);
        }
    }

    /**
     * Create a message envelope with appropriate headers for the notification.
     *
     * @param type Notificator type
     * @param message Notification message
     * @return Message envelope with headers
     */
    private MessageEnvelope createMessageEnvelope(String type, NotificationMessage message) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(MessageHeaders.CONTENT_TYPE, "application/json");
        headers.put(MessageHeaders.MESSAGE_ID, java.util.UUID.randomUUID().toString());
        headers.put(MessageHeaders.TIMESTAMP, System.currentTimeMillis());
        headers.put("notification-type", type);
        
        // Add correlation ID for distributed tracing if available
        if (message.getCorrelationId() != null) {
            headers.put(MessageHeaders.CORRELATION_ID, message.getCorrelationId());
        } else {
            headers.put(MessageHeaders.CORRELATION_ID, java.util.UUID.randomUUID().toString());
        }

        return new MessageEnvelope(message, headers);
    }

    /**
     * Publish a message to the specified topic.
     *
     * @param topic Topic name
     * @param envelope Message envelope
     * @return True if the message was published successfully
     */
    private boolean publishMessage(String topic, MessageEnvelope envelope) {
        try {
            messageProducer.send(topic, envelope);
            return true;
        } catch (Exception e) {
            LOGGER.error("Failed to publish message to topic: {}", topic, e);
            throw e;
        }
    }

    /**
     * Discover service endpoints for a notificator type using service discovery.
     * This is used for direct calls to external services when needed.
     *
     * @param type Notificator type
     * @return List of service instances for the notificator type
     */
    private List<ServiceInstance> discoverServiceEndpoints(String type) {
        try {
            String serviceName = "notificator-" + type;
            List<ServiceInstance> instances = serviceDiscovery.findServiceInstances(serviceName);
            
            if (instances.isEmpty()) {
                LOGGER.debug("No service instances found for: {}", serviceName);
            } else {
                LOGGER.debug("Found {} service instances for: {}", instances.size(), serviceName);
            }
            
            return instances;
        } catch (Exception e) {
            LOGGER.warn("Failed to discover service endpoints for: {}", type, e);
            return List.of();
        }
    }

    /**
     * Check if a notificator type is enabled.
     *
     * @param type Notificator type
     * @return True if the notificator is enabled
     */
    public boolean isNotificatorEnabled(String type) {
        NotificatorConfig config = notificatorConfigs.get(type);
        return config != null && config.isEnabled();
    }

    /**
     * Get the circuit breaker for a notificator type.
     *
     * @param type Notificator type
     * @return Circuit breaker for the notificator type
     */
    public CircuitBreaker getCircuitBreaker(String type) {
        return circuitBreakers.get(type);
    }

    /**
     * Get the rate limiter for a notificator type.
     *
     * @param type Notificator type
     * @return Rate limiter for the notificator type
     */
    public RateLimiter getRateLimiter(String type) {
        return rateLimiters.get(type);
    }
}