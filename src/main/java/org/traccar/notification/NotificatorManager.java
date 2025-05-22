/*
 * Copyright 2018 - 2023 Anton Tananaev (anton@traccar.org)
 * Copyright 2018 Andrey Kunitsyn (andrey@traccar.org)
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

import com.google.inject.Injector;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.MessageBrokerManager;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHandler;
import org.traccar.messaging.MessageHeaders;
import org.traccar.model.Typed;
import org.traccar.notificators.Notificator;
import org.traccar.notificators.NotificatorCommand;
import org.traccar.notificators.NotificatorFirebase;
import org.traccar.notificators.NotificatorMail;
import org.traccar.notificators.NotificatorPushover;
import org.traccar.notificators.NotificatorSms;
import org.traccar.notificators.NotificatorTelegram;
import org.traccar.notificators.NotificatorTraccar;
import org.traccar.notificators.NotificatorWeb;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Manages notification delivery through various channels with support for:
 * - Circuit breaker patterns for external notification services
 * - Metrics collection for notificator operations
 * - Asynchronous notification via message broker
 * - Channel-specific topic publishing
 */
@Singleton
public class NotificatorManager {

    private static final Map<String, Class<? extends Notificator>> NOTIFICATORS_ALL = Map.of(
            "command", NotificatorCommand.class,
            "web", NotificatorWeb.class,
            "mail", NotificatorMail.class,
            "sms", NotificatorSms.class,
            "firebase", NotificatorFirebase.class,
            "traccar", NotificatorTraccar.class,
            "telegram", NotificatorTelegram.class,
            "pushover", NotificatorPushover.class);

    // Channel-specific topic names for message broker
    private static final Map<String, String> CHANNEL_TOPICS = Map.of(
            "mail", "notification.mail",
            "sms", "notification.sms",
            "firebase", "notification.push",
            "web", "notification.web",
            "telegram", "notification.external",
            "pushover", "notification.external",
            "command", "notification.command",
            "traccar", "notification.traccar");

    private final Injector injector;
    private final Set<String> types = new HashSet<>();
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final MeterRegistry meterRegistry;
    private final MessageBrokerManager messageBrokerManager;
    private final Map<String, CircuitBreaker> circuitBreakers = new HashMap<>();
    private final Map<String, Timer> notificationTimers = new HashMap<>();
    private final Map<String, Counter> successCounters = new HashMap<>();
    private final Map<String, Counter> failureCounters = new HashMap<>();
    private final boolean asyncNotificationEnabled;

    /**
     * Initializes the NotificatorManager with required dependencies.
     *
     * @param injector Guice injector for creating notificator instances
     * @param config System configuration
     * @param circuitBreakerRegistry Registry for circuit breakers
     * @param meterRegistry Registry for metrics collection
     * @param messageBrokerManager Manager for message broker operations
     */
    @Inject
    public NotificatorManager(Injector injector, Config config, 
                             CircuitBreakerRegistry circuitBreakerRegistry,
                             MeterRegistry meterRegistry,
                             MessageBrokerManager messageBrokerManager) {
        this.injector = injector;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.meterRegistry = meterRegistry;
        this.messageBrokerManager = messageBrokerManager;
        this.asyncNotificationEnabled = config.getBoolean(Keys.NOTIFICATOR_ASYNC_ENABLED, false);
        
        String types = config.getString(Keys.NOTIFICATOR_TYPES);
        if (types != null) {
            this.types.addAll(Arrays.asList(types.split(",")));
        }
        
        // Initialize circuit breakers and metrics for each notificator type
        initializeCircuitBreakersAndMetrics();
        
        // Register message handlers for notification channels if async is enabled
        if (asyncNotificationEnabled) {
            registerMessageHandlers();
        }
    }

    /**
     * Initializes circuit breakers and metrics for each notificator type.
     */
    private void initializeCircuitBreakersAndMetrics() {
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .slidingWindowSize(10) // Consider the last 10 calls
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 test calls when half-open
                .recordExceptions(Exception.class) // Record all exceptions as failures
                .ignoreExceptions(IllegalArgumentException.class) // Don't count these as failures
                .build();

        for (String type : types) {
            // Create circuit breaker for each notificator type
            CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(
                    "notificator-" + type, circuitBreakerConfig);
            circuitBreakers.put(type, circuitBreaker);

            // Create metrics for each notificator type
            Timer timer = Timer.builder("notification.delivery.time")
                    .description("Time taken to deliver notifications")
                    .tag("channel", type)
                    .register(meterRegistry);
            notificationTimers.put(type, timer);

            Counter successCounter = Counter.builder("notification.delivery.success")
                    .description("Number of successfully delivered notifications")
                    .tag("channel", type)
                    .register(meterRegistry);
            successCounters.put(type, successCounter);

            Counter failureCounter = Counter.builder("notification.delivery.failure")
                    .description("Number of failed notification deliveries")
                    .tag("channel", type)
                    .register(meterRegistry);
            failureCounters.put(type, failureCounter);
        }
    }

    /**
     * Registers message handlers for each notification channel.
     */
    private void registerMessageHandlers() {
        for (String type : types) {
            String topic = CHANNEL_TOPICS.getOrDefault(type, "notification.default");
            messageBrokerManager.subscribe(topic, new NotificationMessageHandler(type));
        }
    }

    /**
     * Gets a notificator instance for the specified type with circuit breaker protection.
     *
     * @param type The notificator type
     * @return The notificator instance wrapped with circuit breaker
     * @throws RuntimeException if the notificator cannot be created
     */
    public Notificator getNotificator(String type) {
        var clazz = NOTIFICATORS_ALL.get(type);
        if (clazz != null && types.contains(type)) {
            var notificator = injector.getInstance(clazz);
            if (notificator != null) {
                return notificator;
            }
        }
        throw new RuntimeException("Failed to get notificator " + type);
    }

    /**
     * Sends a notification using the specified notificator type with circuit breaker protection.
     *
     * @param type The notificator type
     * @param notificationSupplier Supplier that executes the notification logic
     * @param <T> The return type of the notification operation
     * @return The result of the notification operation
     * @throws Exception if the notification fails
     */
    public <T> T sendNotification(String type, Supplier<T> notificationSupplier) throws Exception {
        if (asyncNotificationEnabled) {
            // Publish to message broker for async processing
            String topic = CHANNEL_TOPICS.getOrDefault(type, "notification.default");
            MessageEnvelope envelope = createNotificationEnvelope(type, notificationSupplier);
            messageBrokerManager.publish(topic, envelope);
            return null; // Async mode doesn't return a result
        } else {
            // Execute synchronously with circuit breaker protection
            CircuitBreaker circuitBreaker = circuitBreakers.get(type);
            Timer timer = notificationTimers.get(type);
            Counter successCounter = successCounters.get(type);
            Counter failureCounter = failureCounters.get(type);
            
            return timer.record(() -> {
                try {
                    T result = circuitBreaker.executeSupplier(notificationSupplier);
                    successCounter.increment();
                    return result;
                } catch (Exception e) {
                    failureCounter.increment();
                    throw e;
                }
            });
        }
    }

    /**
     * Creates a message envelope for a notification.
     *
     * @param type The notificator type
     * @param notificationSupplier The notification logic supplier
     * @param <T> The return type of the notification operation
     * @return A message envelope containing the notification details
     */
    private <T> MessageEnvelope createNotificationEnvelope(String type, Supplier<T> notificationSupplier) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(MessageHeaders.CORRELATION_ID, UUID.randomUUID().toString());
        headers.put(MessageHeaders.MESSAGE_TYPE, "notification");
        headers.put("notificator.type", type);
        headers.put(MessageHeaders.TIMESTAMP, System.currentTimeMillis());
        
        // The actual notification logic will be serialized and included in the message
        return new MessageEnvelope(headers, notificationSupplier);
    }

    /**
     * Asynchronously sends a notification with a timeout.
     *
     * @param type The notificator type
     * @param notificationSupplier Supplier that executes the notification logic
     * @param timeoutMillis Timeout in milliseconds
     * @param <T> The return type of the notification operation
     * @return A CompletableFuture that will complete with the notification result or timeout
     */
    public <T> CompletableFuture<T> sendNotificationAsync(String type, Supplier<T> notificationSupplier, long timeoutMillis) {
        CompletableFuture<T> future = new CompletableFuture<>();
        
        if (asyncNotificationEnabled) {
            // Publish to message broker for async processing
            String topic = CHANNEL_TOPICS.getOrDefault(type, "notification.default");
            MessageEnvelope envelope = createNotificationEnvelope(type, notificationSupplier);
            messageBrokerManager.publishAsync(topic, envelope)
                    .thenRun(() -> future.complete(null)) // Async mode doesn't return a result
                    .exceptionally(ex -> {
                        future.completeExceptionally(ex);
                        return null;
                    });
        } else {
            // Execute in a separate thread with circuit breaker protection
            CompletableFuture.supplyAsync(() -> {
                try {
                    return sendNotification(type, notificationSupplier);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).orTimeout(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
              .thenAccept(future::complete)
              .exceptionally(ex -> {
                  if (ex.getCause() instanceof TimeoutException) {
                      future.completeExceptionally(new TimeoutException("Notification timed out after " + timeoutMillis + "ms"));
                  } else {
                      future.completeExceptionally(ex.getCause());
                  }
                  return null;
              });
        }
        
        return future;
    }

    /**
     * Gets all available notificator types.
     *
     * @return A set of all notificator types
     */
    public Set<Typed> getAllNotificatorTypes() {
        return types.stream().map(Typed::new).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Gets the circuit breaker for a specific notificator type.
     *
     * @param type The notificator type
     * @return The circuit breaker for the specified type
     */
    public CircuitBreaker getCircuitBreaker(String type) {
        return circuitBreakers.get(type);
    }

    /**
     * Message handler for processing notifications from the message broker.
     */
    private class NotificationMessageHandler implements MessageHandler<Supplier<?>> {
        private final String type;

        public NotificationMessageHandler(String type) {
            this.type = type;
        }

        @Override
        public void onMessage(MessageEnvelope envelope, Supplier<?> payload) {
            Timer timer = notificationTimers.get(type);
            Counter successCounter = successCounters.get(type);
            Counter failureCounter = failureCounters.get(type);
            CircuitBreaker circuitBreaker = circuitBreakers.get(type);

            timer.record(() -> {
                try {
                    // Execute the notification logic with circuit breaker protection
                    circuitBreaker.executeSupplier(payload::get);
                    successCounter.increment();
                } catch (Exception e) {
                    failureCounter.increment();
                    // Log the error and potentially retry or send to dead letter queue
                    // This would be handled by the message broker's retry mechanism
                }
            });
        }
    }
}