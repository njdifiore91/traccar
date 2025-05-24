/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.MessageException;
import org.traccar.notification.NotificationMessage;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Provides circuit breaker configurations and utilities for all notificators to protect against failures
 * when calling external services. Implements the circuit breaker pattern using Resilience4j, preventing
 * cascading failures when external notification services are degraded or unavailable.
 */
@Singleton
public class NotificatorCircuitBreaker {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificatorCircuitBreaker.class);

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final Map<String, CircuitBreaker> circuitBreakers = new ConcurrentHashMap<>();
    private final Config config;

    /**
     * Constructs a new NotificatorCircuitBreaker with the specified configuration and meter registry.
     * Initializes the circuit breaker registry with default configurations and registers metrics.
     *
     * @param config The system configuration
     * @param meterRegistry The meter registry for exposing circuit breaker metrics
     */
    @Inject
    public NotificatorCircuitBreaker(Config config, MeterRegistry meterRegistry) {
        this.config = config;
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(getDefaultCircuitBreakerConfig());
        
        // Register circuit breaker metrics with the meter registry
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(circuitBreakerRegistry)
                .bindTo(meterRegistry);
        
        LOGGER.info("Initialized NotificatorCircuitBreaker with default configuration");
    }

    /**
     * Creates the default circuit breaker configuration with sensible defaults.
     * These can be overridden by specific channel configurations.
     *
     * @return The default CircuitBreakerConfig
     */
    private CircuitBreakerConfig getDefaultCircuitBreakerConfig() {
        return CircuitBreakerConfig.custom()
                .failureRateThreshold(config.getFloat(Keys.NOTIFICATION_CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD, 50.0f))
                .slowCallRateThreshold(config.getFloat(Keys.NOTIFICATION_CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD, 50.0f))
                .slowCallDurationThreshold(Duration.ofMillis(
                        config.getLong(Keys.NOTIFICATION_CIRCUIT_BREAKER_SLOW_CALL_DURATION_THRESHOLD, 2000L)))
                .permittedNumberOfCallsInHalfOpenState(
                        config.getInteger(Keys.NOTIFICATION_CIRCUIT_BREAKER_PERMITTED_CALLS_IN_HALF_OPEN, 10))
                .minimumNumberOfCalls(
                        config.getInteger(Keys.NOTIFICATION_CIRCUIT_BREAKER_MINIMUM_CALLS, 10))
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(config.getInteger(Keys.NOTIFICATION_CIRCUIT_BREAKER_SLIDING_WINDOW_SIZE, 100))
                .waitDurationInOpenState(Duration.ofSeconds(
                        config.getLong(Keys.NOTIFICATION_CIRCUIT_BREAKER_WAIT_DURATION_IN_OPEN_STATE, 60L)))
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .recordExceptions(Exception.class)
                .build();
    }

    /**
     * Gets a circuit breaker for the specified notification channel. If one doesn't exist, it creates
     * a new one with channel-specific configurations if available, or falls back to defaults.
     *
     * @param channelName The name of the notification channel (e.g., "firebase", "mail", "pushover")
     * @return A CircuitBreaker instance for the specified channel
     */
    public CircuitBreaker getCircuitBreaker(String channelName) {
        return circuitBreakers.computeIfAbsent(channelName, name -> {
            CircuitBreakerConfig channelConfig = getChannelSpecificConfig(name);
            CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(name, channelConfig);
            
            // Register event listeners for logging
            circuitBreaker.getEventPublisher()
                    .onStateTransition(event -> LOGGER.info(
                            "Circuit breaker '{}' state changed from {} to {}",
                            event.getCircuitBreakerName(),
                            event.getStateTransition().getFromState(),
                            event.getStateTransition().getToState()))
                    .onError(event -> LOGGER.debug(
                            "Circuit breaker '{}' recorded error: {}",
                            event.getCircuitBreakerName(),
                            event.getThrowable().getMessage()));
            
            LOGGER.info("Created circuit breaker for notification channel: {}", name);
            return circuitBreaker;
        });
    }

    /**
     * Gets channel-specific circuit breaker configuration if available, or falls back to defaults.
     * Different notification channels may have different requirements for circuit breaker behavior.
     *
     * @param channelName The name of the notification channel
     * @return A CircuitBreakerConfig for the specified channel
     */
    private CircuitBreakerConfig getChannelSpecificConfig(String channelName) {
        CircuitBreakerConfig.Builder builder = CircuitBreakerConfig.custom();
        
        // Apply channel-specific configurations based on channel name
        switch (channelName.toLowerCase()) {
            case "firebase":
                // Firebase may have different timeout characteristics
                builder.slowCallDurationThreshold(Duration.ofMillis(
                        config.getLong(Keys.NOTIFICATION_CIRCUIT_BREAKER_FIREBASE_SLOW_CALL_THRESHOLD, 3000L)));
                break;
            case "mail":
                // Email servers might be slower but more reliable
                builder.slowCallDurationThreshold(Duration.ofMillis(
                        config.getLong(Keys.NOTIFICATION_CIRCUIT_BREAKER_MAIL_SLOW_CALL_THRESHOLD, 5000L)));
                builder.failureRateThreshold(
                        config.getFloat(Keys.NOTIFICATION_CIRCUIT_BREAKER_MAIL_FAILURE_RATE_THRESHOLD, 70.0f));
                break;
            case "sms":
                // SMS gateways might need different thresholds
                builder.slowCallDurationThreshold(Duration.ofMillis(
                        config.getLong(Keys.NOTIFICATION_CIRCUIT_BREAKER_SMS_SLOW_CALL_THRESHOLD, 3000L)));
                break;
            case "pushover":
            case "telegram":
            case "traccar":
                // Web API calls might have standard thresholds
                builder.slowCallDurationThreshold(Duration.ofMillis(
                        config.getLong(Keys.NOTIFICATION_CIRCUIT_BREAKER_API_SLOW_CALL_THRESHOLD, 2000L)));
                break;
            default:
                // Use default configuration for unknown channels
                return getDefaultCircuitBreakerConfig();
        }
        
        // Apply common configurations from defaults for unspecified parameters
        CircuitBreakerConfig defaultConfig = getDefaultCircuitBreakerConfig();
        return builder
                .failureRateThreshold(config.getFloat(
                        "notification.circuitBreaker." + channelName + ".failureRateThreshold",
                        defaultConfig.getFailureRateThreshold()))
                .slowCallRateThreshold(config.getFloat(
                        "notification.circuitBreaker." + channelName + ".slowCallRateThreshold",
                        defaultConfig.getSlowCallRateThreshold()))
                .permittedNumberOfCallsInHalfOpenState(config.getInteger(
                        "notification.circuitBreaker." + channelName + ".permittedCallsInHalfOpen",
                        defaultConfig.getPermittedNumberOfCallsInHalfOpenState()))
                .minimumNumberOfCalls(config.getInteger(
                        "notification.circuitBreaker." + channelName + ".minimumCalls",
                        defaultConfig.getMinimumNumberOfCalls()))
                .slidingWindowType(defaultConfig.getSlidingWindowType())
                .slidingWindowSize(config.getInteger(
                        "notification.circuitBreaker." + channelName + ".slidingWindowSize",
                        defaultConfig.getSlidingWindowSize()))
                .waitDurationInOpenState(Duration.ofSeconds(config.getLong(
                        "notification.circuitBreaker." + channelName + ".waitDurationInOpenState",
                        defaultConfig.getWaitDurationInOpenState().getSeconds())))
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .recordExceptions(Exception.class)
                .build();
    }

    /**
     * Executes a notification operation with circuit breaker protection.
     * If the circuit breaker is open, the fallback will be executed instead.
     *
     * @param notificator The notificator instance
     * @param user The user to send the notification to
     * @param message The notification message
     * @param event The event that triggered the notification
     * @param position The position associated with the event
     * @throws MessageException If the notification fails and cannot be handled by the circuit breaker
     */
    public void executeNotification(Notificator notificator, User user, NotificationMessage message,
                                   Event event, Position position) throws MessageException {
        String channelName = getChannelName(notificator);
        CircuitBreaker circuitBreaker = getCircuitBreaker(channelName);
        
        try {
            // Execute the notification with circuit breaker protection
            Supplier<Void> notificationSupplier = () -> {
                try {
                    notificator.send(user, message, event, position);
                    return null;
                } catch (MessageException e) {
                    throw new RuntimeException(e);
                }
            };
            
            CircuitBreaker.decorateSupplier(circuitBreaker, notificationSupplier).get();
        } catch (Exception e) {
            // Handle circuit breaker open state or other exceptions
            if (e instanceof io.github.resilience4j.circuitbreaker.CallNotPermittedException) {
                LOGGER.warn("Circuit breaker for {} is open, notification not sent", channelName);
                handleFallback(notificator, user, message, event, position);
            } else if (e.getCause() instanceof MessageException) {
                throw (MessageException) e.getCause();
            } else {
                throw new MessageException(e);
            }
        }
    }

    /**
     * Handles fallback behavior when a circuit breaker is open.
     * This might include logging the notification for later retry, sending through an alternative channel,
     * or other recovery mechanisms.
     *
     * @param notificator The notificator that failed
     * @param user The user to send the notification to
     * @param message The notification message
     * @param event The event that triggered the notification
     * @param position The position associated with the event
     */
    private void handleFallback(Notificator notificator, User user, NotificationMessage message,
                               Event event, Position position) {
        String channelName = getChannelName(notificator);
        LOGGER.info("Executing fallback for {} notification to user {}", channelName, user.getId());
        
        // Implement fallback strategy based on configuration
        boolean storeForRetry = config.getBoolean(Keys.NOTIFICATION_CIRCUIT_BREAKER_STORE_FOR_RETRY, true);
        boolean useAlternativeChannel = config.getBoolean(Keys.NOTIFICATION_CIRCUIT_BREAKER_USE_ALTERNATIVE_CHANNEL, false);
        
        if (storeForRetry) {
            // Store the notification for later retry
            // This could be implemented with a persistent queue or database
            LOGGER.info("Storing {} notification for user {} for later retry", channelName, user.getId());
            // Implementation would depend on the specific retry mechanism
        }
        
        if (useAlternativeChannel) {
            // Try to send through an alternative channel if configured
            String alternativeChannel = config.getString(Keys.NOTIFICATION_CIRCUIT_BREAKER_ALTERNATIVE_CHANNEL);
            if (alternativeChannel != null && !alternativeChannel.isEmpty()) {
                LOGGER.info("Attempting to send notification via alternative channel: {}", alternativeChannel);
                // Implementation would depend on the notification service architecture
            }
        }
    }

    /**
     * Gets the channel name from a notificator instance.
     * This is used to identify which circuit breaker to use.
     *
     * @param notificator The notificator instance
     * @return The channel name (e.g., "firebase", "mail", "pushover")
     */
    private String getChannelName(Notificator notificator) {
        String className = notificator.getClass().getSimpleName();
        if (className.startsWith("Notificator")) {
            return className.substring("Notificator".length()).toLowerCase();
        }
        return className.toLowerCase();
    }

    /**
     * Checks the health of all circuit breakers.
     * This can be used by health check endpoints to report the overall health of the notification system.
     *
     * @return true if all circuit breakers are in a healthy state, false otherwise
     */
    public boolean isHealthy() {
        // A circuit breaker is considered healthy if it's not in the OPEN state
        return circuitBreakers.values().stream()
                .noneMatch(cb -> cb.getState() == CircuitBreaker.State.OPEN);
    }

    /**
     * Gets the current state of a specific circuit breaker.
     *
     * @param channelName The name of the notification channel
     * @return The current state of the circuit breaker, or null if it doesn't exist
     */
    public CircuitBreaker.State getCircuitBreakerState(String channelName) {
        CircuitBreaker circuitBreaker = circuitBreakers.get(channelName);
        return circuitBreaker != null ? circuitBreaker.getState() : null;
    }

    /**
     * Resets a specific circuit breaker to its closed state.
     * This can be useful for manual recovery after fixing an issue with an external service.
     *
     * @param channelName The name of the notification channel
     */
    public void resetCircuitBreaker(String channelName) {
        CircuitBreaker circuitBreaker = circuitBreakers.get(channelName);
        if (circuitBreaker != null) {
            circuitBreaker.reset();
            LOGGER.info("Reset circuit breaker for channel: {}", channelName);
        }
    }

    /**
     * Gets metrics for a specific circuit breaker.
     * This can be used for monitoring and diagnostics.
     *
     * @param channelName The name of the notification channel
     * @return A map of metric names to values, or null if the circuit breaker doesn't exist
     */
    public Map<String, Object> getCircuitBreakerMetrics(String channelName) {
        CircuitBreaker circuitBreaker = circuitBreakers.get(channelName);
        if (circuitBreaker == null) {
            return null;
        }
        
        Map<String, Object> metrics = new ConcurrentHashMap<>();
        metrics.put("state", circuitBreaker.getState().toString());
        metrics.put("failureRate", circuitBreaker.getMetrics().getFailureRate());
        metrics.put("slowCallRate", circuitBreaker.getMetrics().getSlowCallRate());
        metrics.put("numberOfSuccessfulCalls", circuitBreaker.getMetrics().getNumberOfSuccessfulCalls());
        metrics.put("numberOfFailedCalls", circuitBreaker.getMetrics().getNumberOfFailedCalls());
        metrics.put("numberOfSlowCalls", circuitBreaker.getMetrics().getNumberOfSlowCalls());
        metrics.put("numberOfNotPermittedCalls", circuitBreaker.getMetrics().getNumberOfNotPermittedCalls());
        
        return metrics;
    }
}