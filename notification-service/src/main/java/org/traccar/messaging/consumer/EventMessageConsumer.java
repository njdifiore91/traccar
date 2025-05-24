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

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.traccar.messaging.EventConsumer;
import org.traccar.model.Event;
import org.traccar.model.User;
import org.traccar.notification.NotificationRuleEvaluator;
import org.traccar.notification.NotificationService;
import org.traccar.service.UserService;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Implements a consumer for the 'events' topic that processes incoming event messages
 * from the Event Processing Service. It applies filtering rules, determines notification
 * recipients, and routes events to the appropriate notification channels.
 */
@Service
public class EventMessageConsumer extends BaseMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventMessageConsumer.class);

    @Value("${notification.event.max-age-minutes:5}")
    private int maxEventAgeMinutes;

    @Value("${kafka.topic.email-out}")
    private String emailOutTopic;

    @Value("${kafka.topic.sms-out}")
    private String smsOutTopic;

    @Value("${kafka.topic.push-out}")
    private String pushOutTopic;

    @Value("${kafka.topic.web-out}")
    private String webOutTopic;

    private final NotificationRuleEvaluator ruleEvaluator;
    private final UserService userService;
    private final NotificationService notificationService;

    @Inject
    public EventMessageConsumer(
            NotificationRuleEvaluator ruleEvaluator,
            UserService userService,
            NotificationService notificationService,
            MeterRegistry meterRegistry) {
        this.ruleEvaluator = ruleEvaluator;
        this.userService = userService;
        this.notificationService = notificationService;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void init() {
        LOGGER.info("Initialized EventMessageConsumer with maxEventAgeMinutes={}", maxEventAgeMinutes);
    }

    /**
     * Consumes event messages from the 'events' topic.
     * This method is triggered whenever a new event message is received from the Event Processing Service.
     *
     * @param event The event payload
     * @param correlationId The correlation ID for distributed tracing
     */
    @KafkaListener(topics = "${kafka.topic.events}", groupId = "${kafka.consumer.group-id.notification}")
    public void consumeEvent(
            @Payload Event event,
            @Header(value = KafkaHeaders.CORRELATION_ID, required = false) String correlationId) {
        
        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
            LOGGER.debug("Generated new correlation ID: {}", correlationId);
        }

        LOGGER.debug("Received event: {}, correlationId: {}", event.getId(), correlationId);
        
        try {
            // Record metric for received events
            meterRegistry.counter("notification.events.received",
                    "type", event.getType()).increment();

            // Validate event age
            if (!isEventRecent(event)) {
                LOGGER.info("Skipping outdated event: {}, type: {}, time: {}", 
                        event.getId(), event.getType(), event.getEventTime());
                meterRegistry.counter("notification.events.outdated").increment();
                return;
            }

            // Process the event and route to appropriate notification channels
            processEvent(event, correlationId);
            
        } catch (Exception e) {
            LOGGER.error("Error processing event: {}", e.getMessage(), e);
            meterRegistry.counter("notification.events.error",
                    "type", event.getType(),
                    "error", e.getClass().getSimpleName()).increment();
        }
    }

    /**
     * Checks if the event is recent enough to be processed.
     * Events older than the configured maximum age are considered outdated and will be skipped.
     *
     * @param event The event to check
     * @return true if the event is recent, false otherwise
     */
    private boolean isEventRecent(Event event) {
        if (event.getEventTime() == null) {
            LOGGER.warn("Event has no timestamp: {}", event.getId());
            return true; // Process events without timestamps
        }

        Instant eventTime = event.getEventTime().toInstant();
        Instant cutoffTime = Instant.now().minus(Duration.ofMinutes(maxEventAgeMinutes));
        
        return eventTime.isAfter(cutoffTime);
    }

    /**
     * Processes an event by evaluating notification rules, determining recipients,
     * and routing to appropriate notification channels.
     *
     * @param event The event to process
     * @param correlationId The correlation ID for distributed tracing
     */
    private void processEvent(Event event, String correlationId) {
        // Evaluate notification rules to determine if this event should trigger notifications
        if (!ruleEvaluator.shouldNotify(event)) {
            LOGGER.debug("Event does not match any notification rules: {}", event.getId());
            meterRegistry.counter("notification.events.filtered",
                    "type", event.getType()).increment();
            return;
        }

        // Determine recipients based on user preferences and permissions
        List<User> recipients = userService.getNotificationRecipients(event);
        
        if (recipients.isEmpty()) {
            LOGGER.debug("No recipients found for event: {}", event.getId());
            meterRegistry.counter("notification.events.no_recipients",
                    "type", event.getType()).increment();
            return;
        }

        LOGGER.debug("Found {} recipients for event: {}", recipients.size(), event.getId());
        
        // Create and route notifications to appropriate channels
        for (User user : recipients) {
            try {
                routeNotifications(event, user, correlationId);
            } catch (Exception e) {
                LOGGER.error("Error routing notification for user {}: {}", 
                        user.getId(), e.getMessage(), e);
                meterRegistry.counter("notification.routing.error",
                        "userId", user.getId().toString()).increment();
            }
        }
    }

    /**
     * Routes notifications to appropriate channels based on user preferences.
     *
     * @param event The event that triggered the notification
     * @param user The recipient user
     * @param correlationId The correlation ID for distributed tracing
     */
    private void routeNotifications(Event event, User user, String correlationId) {
        // Create notification object from the event
        var notification = notificationService.createNotification(event, user);
        
        // Route to email channel if enabled for this user
        if (user.getEmail() != null && notificationService.isEmailEnabled(user, event.getType())) {
            LOGGER.debug("Routing notification to email channel for user: {}", user.getId());
            kafkaTemplate.send(emailOutTopic, correlationId, notification);
            meterRegistry.counter("notification.channel.email",
                    "type", event.getType()).increment();
        }
        
        // Route to SMS channel if enabled for this user
        if (user.getPhone() != null && notificationService.isSmsEnabled(user, event.getType())) {
            LOGGER.debug("Routing notification to SMS channel for user: {}", user.getId());
            kafkaTemplate.send(smsOutTopic, correlationId, notification);
            meterRegistry.counter("notification.channel.sms",
                    "type", event.getType()).increment();
        }
        
        // Route to push notification channel if enabled for this user
        if (notificationService.isPushEnabled(user, event.getType())) {
            LOGGER.debug("Routing notification to push channel for user: {}", user.getId());
            kafkaTemplate.send(pushOutTopic, correlationId, notification);
            meterRegistry.counter("notification.channel.push",
                    "type", event.getType()).increment();
        }
        
        // Route to web notification channel if enabled for this user
        if (notificationService.isWebEnabled(user, event.getType())) {
            LOGGER.debug("Routing notification to web channel for user: {}", user.getId());
            kafkaTemplate.send(webOutTopic, correlationId, notification);
            meterRegistry.counter("notification.channel.web",
                    "type", event.getType()).increment();
        }
    }
}