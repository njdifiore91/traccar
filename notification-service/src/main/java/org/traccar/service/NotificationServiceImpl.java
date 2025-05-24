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
package org.traccar.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.traccar.model.Event;
import org.traccar.model.Notification;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.MessageException;
import org.traccar.notification.NotificationFormatter;
import org.traccar.notification.NotificationMessage;
import org.traccar.notificators.Notificator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of the NotificationService interface.
 * Provides the core business logic for processing events into notifications
 * and orchestrating their delivery across multiple channels.
 */
@Singleton
public class NotificationServiceImpl implements NotificationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationServiceImpl.class);
    private static final String CORRELATION_ID_KEY = "correlationId";

    private final NotificationFormatter notificationFormatter;
    private final NotificationFilterService notificationFilterService;
    private final RecipientService recipientService;
    private final RetryService retryService;
    private final DeliveryStatusService deliveryStatusService;
    private final RateLimitService rateLimitService;
    private final org.traccar.notification.NotificatorManager notificatorManager;
    private final Executor notificationExecutor;
    private final MeterRegistry meterRegistry;

    // Metrics
    private final Counter processedCounter;
    private final Counter deliveredCounter;
    private final Counter failedCounter;
    private final Timer processingTimer;
    private final AtomicLong totalProcessingTimeMs = new AtomicLong(0);
    private final AtomicLong totalProcessedCount = new AtomicLong(0);

    // Channel status tracking
    private final ConcurrentHashMap<String, Boolean> channelAvailability = new ConcurrentHashMap<>();

    @Inject
    public NotificationServiceImpl(
            NotificationFormatter notificationFormatter,
            NotificationFilterService notificationFilterService,
            RecipientService recipientService,
            RetryService retryService,
            DeliveryStatusService deliveryStatusService,
            RateLimitService rateLimitService,
            org.traccar.notification.NotificatorManager notificatorManager,
            Executor notificationExecutor,
            MeterRegistry meterRegistry) {
        this.notificationFormatter = notificationFormatter;
        this.notificationFilterService = notificationFilterService;
        this.recipientService = recipientService;
        this.retryService = retryService;
        this.deliveryStatusService = deliveryStatusService;
        this.rateLimitService = rateLimitService;
        this.notificatorManager = notificatorManager;
        this.notificationExecutor = notificationExecutor;
        this.meterRegistry = meterRegistry;

        // Initialize metrics
        this.processedCounter = Counter.builder("notification.processed")
                .description("Total number of notifications processed")
                .register(meterRegistry);
        this.deliveredCounter = Counter.builder("notification.delivered")
                .description("Total number of notifications successfully delivered")
                .register(meterRegistry);
        this.failedCounter = Counter.builder("notification.failed")
                .description("Total number of notifications that failed to deliver")
                .register(meterRegistry);
        this.processingTimer = Timer.builder("notification.processing.time")
                .description("Time taken to process notifications")
                .register(meterRegistry);

        // Initialize channel availability
        notificatorManager.getAllNotificatorTypes().forEach(type -> 
                channelAvailability.put(type.getType(), true));
    }

    @Override
    public CompletableFuture<Void> processEvent(Event event, Position position, String correlationId) {
        if (correlationId == null || correlationId.isEmpty()) {
            correlationId = UUID.randomUUID().toString();
        }

        final String finalCorrelationId = correlationId;
        MDC.put(CORRELATION_ID_KEY, finalCorrelationId);

        LOGGER.debug("Processing event: {} with correlationId: {}", event.getType(), finalCorrelationId);
        processedCounter.increment();

        return CompletableFuture.runAsync(() -> {
            try {
                MDC.put(CORRELATION_ID_KEY, finalCorrelationId);
                long startTime = System.currentTimeMillis();

                // Check if event should be processed based on age and other criteria
                if (!notificationFilterService.shouldProcessEvent(event)) {
                    LOGGER.debug("Event filtered out: {}", event.getType());
                    return;
                }

                // Get users who should receive notifications for this event
                List<User> users = recipientService.getRecipients(event);
                if (users.isEmpty()) {
                    LOGGER.debug("No recipients found for event: {}", event.getType());
                    return;
                }

                // Create a notification object for this event
                Notification notification = new Notification();
                notification.setType(event.getType());
                notification.setAlways(true);

                // Process notification for each user
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                for (User user : users) {
                    futures.add(processNotification(notification, user, event, position, finalCorrelationId));
                }

                // Wait for all notifications to be processed
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

                // Update metrics
                long processingTime = System.currentTimeMillis() - startTime;
                totalProcessingTimeMs.addAndGet(processingTime);
                totalProcessedCount.incrementAndGet();
                processingTimer.record(() -> {});

                LOGGER.debug("Event processing completed in {}ms: {}", processingTime, event.getType());
            } catch (Exception e) {
                LOGGER.error("Error processing event: {}", event.getType(), e);
                failedCounter.increment();
            } finally {
                MDC.remove(CORRELATION_ID_KEY);
            }
        }, notificationExecutor);
    }

    @Override
    public CompletableFuture<Void> processNotification(
            Notification notification, User user, Event event, Position position, String correlationId) {
        if (correlationId == null || correlationId.isEmpty()) {
            correlationId = UUID.randomUUID().toString();
        }

        final String finalCorrelationId = correlationId;
        MDC.put(CORRELATION_ID_KEY, finalCorrelationId);

        LOGGER.debug("Processing notification for user: {} with correlationId: {}", user.getId(), finalCorrelationId);

        return CompletableFuture.runAsync(() -> {
            try {
                MDC.put(CORRELATION_ID_KEY, finalCorrelationId);

                // Check if notification should be sent based on user preferences and calendar
                if (!notificationFilterService.shouldSendNotification(notification, user, event)) {
                    LOGGER.debug("Notification filtered out for user: {}", user.getId());
                    return;
                }

                // Get notification channels for this user and notification type
                Set<String> notificatorTypes = notification.getNotificatorsTypes();
                if (notificatorTypes.isEmpty()) {
                    // If no specific notificators are set, use all available
                    notificatorTypes = recipientService.getUserNotificatorTypes(user, event.getType());
                }

                if (notificatorTypes.isEmpty()) {
                    LOGGER.debug("No notification channels available for user: {}", user.getId());
                    return;
                }

                // Format the notification message
                NotificationMessage message = notificationFormatter.formatMessage(
                        notification, user, event, position, "templates/notification");

                // Send notification through each channel
                for (String notificatorType : notificatorTypes) {
                    // Check rate limits for this channel
                    if (!rateLimitService.checkRateLimit(user.getId(), notificatorType)) {
                        LOGGER.debug("Rate limit exceeded for user: {} on channel: {}", 
                                user.getId(), notificatorType);
                        continue;
                    }

                    // Check channel availability
                    if (!isNotificationChannelAvailable(notificatorType)) {
                        LOGGER.debug("Channel unavailable: {}", notificatorType);
                        continue;
                    }

                    try {
                        // Get the notificator for this channel
                        Notificator notificator = notificatorManager.getNotificator(notificatorType);
                        
                        // Send the notification
                        notificator.send(user, message, event, position);
                        
                        // Record successful delivery
                        deliveryStatusService.recordSuccess(notification, user, event, notificatorType, finalCorrelationId);
                        deliveredCounter.increment();
                        
                        LOGGER.debug("Notification sent to user: {} via channel: {}", 
                                user.getId(), notificatorType);
                    } catch (MessageException e) {
                        LOGGER.warn("Failed to send notification to user: {} via channel: {}", 
                                user.getId(), notificatorType, e);
                        
                        // Record failure and schedule retry
                        deliveryStatusService.recordFailure(notification, user, event, notificatorType, 
                                e.getMessage(), finalCorrelationId);
                        retryService.scheduleRetry(notification, user, event, position, 
                                notificatorType, finalCorrelationId);
                        failedCounter.increment();
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Error processing notification for user: {}", user.getId(), e);
                failedCounter.increment();
            } finally {
                MDC.remove(CORRELATION_ID_KEY);
            }
        }, notificationExecutor);
    }

    @Override
    public List<String> getNotificationChannels() {
        return notificatorManager.getAllNotificatorTypes().stream()
                .map(type -> type.getType())
                .toList();
    }

    @Override
    public boolean isNotificationChannelAvailable(String channel) {
        return channelAvailability.getOrDefault(channel, false);
    }

    /**
     * Update the availability status of a notification channel.
     * This is called by circuit breakers when external services change state.
     *
     * @param channel The notification channel
     * @param available Whether the channel is available
     */
    public void updateChannelAvailability(String channel, boolean available) {
        boolean previous = channelAvailability.put(channel, available);
        if (previous != available) {
            LOGGER.info("Notification channel {} is now {}", channel, available ? "available" : "unavailable");
        }
    }

    @Override
    public NotificationStatistics getStatistics() {
        long processed = totalProcessedCount.get();
        long avgTime = processed > 0 ? totalProcessingTimeMs.get() / processed : 0;
        
        return new NotificationStatistics(
                processed,
                deliveredCounter.count(),
                failedCounter.count(),
                avgTime);
    }
}