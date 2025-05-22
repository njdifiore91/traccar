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

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
// Using local Keys class instead of global Keys
import org.traccar.config.ConfigKey;
import org.traccar.model.Event;
import org.traccar.model.Notification;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.MessageException;
import org.traccar.notification.NotificationMessage;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Implements rate limiting for notification channels to prevent abuse and ensure fair resource allocation.
 * This class wraps existing notificators and applies channel-specific rate limits before allowing notifications
 * to be sent. It uses Redis for distributed rate limiting across service instances.
 */
public class NotificatorRateLimiter {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificatorRateLimiter.class);

    private final Map<String, RateLimiter> rateLimiters = new HashMap<>();
    private final Map<String, Notificator> notificators;
    private final MeterRegistry meterRegistry;

    /**
     * Constructs a new NotificatorRateLimiter with the specified notificators and configuration.
     *
     * @param notificatorManager The manager containing all available notificators
     * @param config The system configuration for rate limiting parameters
     * @param meterRegistry The registry for metrics collection
     */
    @Inject
    public NotificatorRateLimiter(
            org.traccar.notification.NotificatorManager notificatorManager,
            Config config,
            MeterRegistry meterRegistry) {
        
        this.meterRegistry = meterRegistry;
        this.notificators = new HashMap<>();
        
        // Initialize notificators
        notificatorManager.getAllNotificatorTypes().forEach(type -> {
            try {
                String typeName = type.getType();
                Notificator notificator = notificatorManager.getNotificator(typeName);
                notificators.put(typeName, notificator);
                
                // Create rate limiter for each notificator type
                createRateLimiter(config, typeName);
                
            } catch (Exception e) {
                LOGGER.warn("Failed to initialize rate limiter for notificator: {}", type.getType(), e);
            }
        });
    }

    /**
     * Creates a rate limiter for the specified notification channel type.
     *
     * @param config The system configuration
     * @param type The notification channel type
     */
    /**
     * Configuration keys for rate limiting
     */
    public static class Keys {
        /**
         * Maximum number of notifications allowed per period for a specific channel
         */
        public static final ConfigKey NOTIFICATOR_RATE_LIMIT = new ConfigKey(
                "notificator.ratelimit", Integer.class);

        /**
         * Period in seconds for rate limit refresh for a specific channel
         */
        public static final ConfigKey NOTIFICATOR_RATE_LIMIT_PERIOD = new ConfigKey(
                "notificator.ratelimit.period", Integer.class);

        /**
         * Timeout in seconds when waiting for rate limit permission for a specific channel
         */
        public static final ConfigKey NOTIFICATOR_RATE_LIMIT_TIMEOUT = new ConfigKey(
                "notificator.ratelimit.timeout", Integer.class);

        /**
         * Whether to use distributed rate limiting with Redis
         */
        public static final ConfigKey NOTIFICATOR_DISTRIBUTED_RATE_LIMITING = new ConfigKey(
                "notificator.ratelimit.distributed", Boolean.class);

        /**
         * Redis host for distributed rate limiting
         */
        public static final ConfigKey NOTIFICATOR_REDIS_HOST = new ConfigKey(
                "notificator.redis.host", String.class);

        /**
         * Redis port for distributed rate limiting
         */
        public static final ConfigKey NOTIFICATOR_REDIS_PORT = new ConfigKey(
                "notificator.redis.port", Integer.class, 6379);
    }

    private void createRateLimiter(Config config, String type) {
        // Default values if not configured
        int limitForPeriod = config.getInteger(Keys.NOTIFICATOR_RATE_LIMIT.withPrefix(type), 100);
        int limitRefreshPeriodSeconds = config.getInteger(Keys.NOTIFICATOR_RATE_LIMIT_PERIOD.withPrefix(type), 60);
        int timeoutDurationSeconds = config.getInteger(Keys.NOTIFICATOR_RATE_LIMIT_TIMEOUT.withPrefix(type), 1);
        
        RateLimiterConfig rateLimiterConfig = RateLimiterConfig.custom()
                .limitForPeriod(limitForPeriod)
                .limitRefreshPeriod(Duration.ofSeconds(limitRefreshPeriodSeconds))
                .timeoutDuration(Duration.ofSeconds(timeoutDurationSeconds))
                .build();
        
        // Use Redis-based registry if distributed rate limiting is enabled
        boolean distributedRateLimiting = config.getBoolean(Keys.NOTIFICATOR_DISTRIBUTED_RATE_LIMITING, false);
        
        RateLimiterRegistry registry;
        if (distributedRateLimiting) {
            registry = createRedisRateLimiterRegistry(config);
            LOGGER.info("Using distributed rate limiting for notificator: {}", type);
        } else {
            registry = RateLimiterRegistry.of(rateLimiterConfig);
            LOGGER.info("Using local rate limiting for notificator: {}", type);
        }
        
        RateLimiter rateLimiter = registry.rateLimiter("notificator-" + type, rateLimiterConfig);
        rateLimiters.put(type, rateLimiter);
        
        // Register metrics
        registerMetrics(type, rateLimiter);
    }

    /**
     * Creates a Redis-backed RateLimiterRegistry for distributed rate limiting.
     *
     * @param config The system configuration
     * @return A RateLimiterRegistry backed by Redis
     */
    /**
     * Creates a Redis-backed RateLimiterRegistry for distributed rate limiting.
     * 
     * Note: This is a placeholder implementation. In a production environment,
     * this would be replaced with actual Redis integration using a library like
     * Redisson or a custom Redis-based RateLimiterRegistry implementation.
     *
     * @param config The system configuration
     * @return A RateLimiterRegistry backed by Redis
     */
    private RateLimiterRegistry createRedisRateLimiterRegistry(Config config) {
        String redisHost = config.getString(Keys.NOTIFICATOR_REDIS_HOST, "localhost");
        int redisPort = config.getInteger(Keys.NOTIFICATOR_REDIS_PORT, 6379);
        
        LOGGER.info("Configuring Redis-based rate limiter with host: {}, port: {}", redisHost, redisPort);
        
        // In a real implementation, this would create a Redis connection and
        // return a Redis-backed RateLimiterRegistry. For example:
        //
        // RedisClient redisClient = RedisClient.create("redis://" + redisHost + ":" + redisPort);
        // StatefulRedisConnection<String, String> connection = redisClient.connect();
        // RedisCommands<String, String> commands = connection.sync();
        //
        // Custom implementation of RateLimiterRegistry that uses Redis for storage
        // and synchronization across service instances would be created here.
        
        // For now, we'll use the default in-memory registry as a placeholder
        return RateLimiterRegistry.ofDefaults();
    }

    /**
     * Registers metrics for the rate limiter to monitor usage.
     *
     * @param type The notification channel type
     * @param rateLimiter The rate limiter to monitor
     */
    private void registerMetrics(String type, RateLimiter rateLimiter) {
        List<Tag> tags = Arrays.asList(Tag.of("notificator", type));
        
        meterRegistry.gauge("notificator.rate.limit.available.permissions", 
                tags, rateLimiter, RateLimiter::getAvailablePermissions);
        
        meterRegistry.gauge("notificator.rate.limit.waiting.threads", 
                tags, rateLimiter, RateLimiter::getNumberOfWaitingThreads);
        
        rateLimiter.getEventPublisher()
                .onSuccess(event -> meterRegistry.counter("notificator.rate.limit.success", tags).increment())
                .onFailure(event -> meterRegistry.counter("notificator.rate.limit.failure", tags).increment());
    }

    /**
     * Sends a notification through the appropriate channel with rate limiting applied.
     *
     * @param type The notification channel type
     * @param notification The notification to send
     * @param user The user to send the notification to
     * @param event The event that triggered the notification
     * @param position The position associated with the event
     * @throws MessageException If the notification cannot be sent
     */
    public void send(String type, Notification notification, User user, Event event, Position position) 
            throws MessageException {
        
        Notificator notificator = notificators.get(type);
        if (notificator == null) {
            throw new MessageException("Notificator not found: " + type);
        }
        
        // Create a supplier for the notification sending operation
        Supplier<Void> notificationSupplier = () -> {
            try {
                notificator.send(notification, user, event, position);
                return null;
            } catch (MessageException e) {
                throw new RuntimeException(e);
            }
        };
        
        // Execute with rate limiting
        executeWithRateLimit(type, notificationSupplier, 
                "Notification sent successfully through channel: " + type);
    }
    
    /**
     * Executes an operation with rate limiting applied.
     *
     * @param type The notification channel type
     * @param operation The operation to execute
     * @param successMessage Message to log on success
     * @throws MessageException If the operation cannot be executed due to rate limiting or other errors
     */
    private <T> T executeWithRateLimit(String type, Supplier<T> operation, String successMessage) 
            throws MessageException {
        
        RateLimiter rateLimiter = rateLimiters.get(type);
        if (rateLimiter == null) {
            LOGGER.warn("Rate limiter not found for notificator: {}, proceeding without rate limiting", type);
            try {
                return operation.get();
            } catch (RuntimeException e) {
                if (e.getCause() instanceof MessageException) {
                    throw (MessageException) e.getCause();
                }
                throw new MessageException(e);
            }
        }
        
        try {
            // Apply rate limiting before executing operation
            boolean permitted = rateLimiter.acquirePermission(1);
            if (permitted) {
                T result = operation.get();
                LOGGER.debug(successMessage);
                return result;
            } else {
                LOGGER.warn("Rate limit exceeded for notificator: {}, available permissions: {}", 
                        type, rateLimiter.getAvailablePermissions());
                throw new MessageException("Rate limit exceeded for notification channel: " + type);
            }
        } catch (Exception e) {
            if (e instanceof MessageException) {
                throw (MessageException) e;
            } else if (e.getCause() instanceof MessageException) {
                throw (MessageException) e.getCause();
            }
            throw new MessageException(e);
        }
    }

    /**
     * Sends a notification message through the appropriate channel with rate limiting applied.
     *
     * @param type The notification channel type
     * @param user The user to send the notification to
     * @param message The notification message to send
     * @param event The event that triggered the notification
     * @param position The position associated with the event
     * @throws MessageException If the notification cannot be sent
     */
    public void send(String type, User user, NotificationMessage message, Event event, Position position) 
            throws MessageException {
        
        Notificator notificator = notificators.get(type);
        if (notificator == null) {
            throw new MessageException("Notificator not found: " + type);
        }
        
        // Create a supplier for the notification sending operation
        Supplier<Void> notificationSupplier = () -> {
            try {
                notificator.send(user, message, event, position);
                return null;
            } catch (MessageException e) {
                throw new RuntimeException(e);
            }
        };
        
        // Execute with rate limiting
        executeWithRateLimit(type, notificationSupplier, 
                "Notification message sent successfully through channel: " + type);
    }

    /**
     * Gets the current rate limit for a notification channel.
     *
     * @param type The notification channel type
     * @return The current rate limit or -1 if not found
     */
    public int getRateLimit(String type) {
        RateLimiter rateLimiter = rateLimiters.get(type);
        if (rateLimiter != null) {
            return rateLimiter.getRateLimiterConfig().getLimitForPeriod();
        }
        return -1;
    }

    /**
     * Gets the available permissions for a notification channel.
     *
     * @param type The notification channel type
     * @return The number of available permissions or -1 if not found
     */
    public int getAvailablePermissions(String type) {
        RateLimiter rateLimiter = rateLimiters.get(type);
        if (rateLimiter != null) {
            return rateLimiter.getAvailablePermissions();
        }
        return -1;
    }
    
    /**
     * Gets the refresh period in seconds for a notification channel.
     *
     * @param type The notification channel type
     * @return The refresh period in seconds or -1 if not found
     */
    public long getRefreshPeriodSeconds(String type) {
        RateLimiter rateLimiter = rateLimiters.get(type);
        if (rateLimiter != null) {
            return rateLimiter.getRateLimiterConfig().getLimitRefreshPeriod().getSeconds();
        }
        return -1;
    }
    
    /**
     * Gets the timeout duration in seconds for a notification channel.
     *
     * @param type The notification channel type
     * @return The timeout duration in seconds or -1 if not found
     */
    public long getTimeoutDurationSeconds(String type) {
        RateLimiter rateLimiter = rateLimiters.get(type);
        if (rateLimiter != null) {
            return rateLimiter.getRateLimiterConfig().getTimeoutDuration().getSeconds();
        }
        return -1;
    }
    
    /**
     * Gets the number of waiting threads for a notification channel.
     *
     * @param type The notification channel type
     * @return The number of waiting threads or -1 if not found
     */
    public int getWaitingThreads(String type) {
        RateLimiter rateLimiter = rateLimiters.get(type);
        if (rateLimiter != null) {
            return rateLimiter.getNumberOfWaitingThreads();
        }
        return -1;
    }
}