/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.traccar.messaging.MessageConsumer;
import org.traccar.messaging.MessageConsumerFactory;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHandler;
import org.traccar.messaging.MessageHeaders;
import org.traccar.model.Event;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;

/**
 * Consumes event messages from the message broker and processes them for notification generation.
 * This component is responsible for filtering events based on age and relevance,
 * determining notification recipients, and routing events to the appropriate notification handlers.
 */
@Service
public class EventConsumer implements MessageHandler<Event> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventConsumer.class);

    private final MessageConsumer<Event> messageConsumer;
    private final NotificatorManager notificatorManager;
    private final NotificationRuleEvaluator ruleEvaluator;
    private final NotificationChannelPublisher channelPublisher;
    private final DeadLetterQueueHandler deadLetterQueueHandler;

    @Value("${notification.event.max-age-seconds:300}")
    private int maxEventAgeSeconds;

    @Value("${notification.consumer.group-id:notification-service}")
    private String consumerGroupId;

    @Value("${notification.topic.events:events}")
    private String eventsTopic;

    @Autowired
    public EventConsumer(
            MessageConsumerFactory messageConsumerFactory,
            NotificatorManager notificatorManager,
            NotificationRuleEvaluator ruleEvaluator,
            NotificationChannelPublisher channelPublisher,
            DeadLetterQueueHandler deadLetterQueueHandler) {
        this.messageConsumer = messageConsumerFactory.createConsumer(Event.class, consumerGroupId);
        this.notificatorManager = notificatorManager;
        this.ruleEvaluator = ruleEvaluator;
        this.channelPublisher = channelPublisher;
        this.deadLetterQueueHandler = deadLetterQueueHandler;
    }

    @PostConstruct
    public void init() {
        LOGGER.info("Initializing EventConsumer with consumer group: {}", consumerGroupId);
        messageConsumer.subscribe(eventsTopic, this);
    }

    @PreDestroy
    public void destroy() {
        LOGGER.info("Shutting down EventConsumer");
        messageConsumer.unsubscribe(eventsTopic);
    }

    @Override
    public void handle(MessageEnvelope<Event> messageEnvelope) {
        Event event = messageEnvelope.getPayload();
        String correlationId = messageEnvelope.getHeaders().getOrDefault(
                MessageHeaders.CORRELATION_ID, UUID.randomUUID().toString());
        
        LOGGER.debug("Received event: type={}, deviceId={}, correlationId={}", 
                event.getType(), event.getDeviceId(), correlationId);

        try {
            if (isEventValid(event)) {
                processEvent(event, correlationId);
            } else {
                LOGGER.warn("Skipping outdated or invalid event: type={}, deviceId={}, eventTime={}, correlationId={}", 
                        event.getType(), event.getDeviceId(), event.getEventTime(), correlationId);
            }
        } catch (Exception e) {
            LOGGER.error("Error processing event: type={}, deviceId={}, correlationId={}", 
                    event.getType(), event.getDeviceId(), correlationId, e);
            deadLetterQueueHandler.handleFailedMessage(messageEnvelope, e);
        }
    }

    /**
     * Validates if the event is recent enough to be processed.
     * Events older than the configured threshold are considered outdated and will be skipped.
     *
     * @param event The event to validate
     * @return true if the event is valid and should be processed, false otherwise
     */
    private boolean isEventValid(Event event) {
        if (event == null || event.getEventTime() == null) {
            return false;
        }

        Instant eventTime = event.getEventTime().toInstant();
        Instant now = Instant.now();
        Duration age = Duration.between(eventTime, now);

        return age.getSeconds() <= maxEventAgeSeconds;
    }

    /**
     * Processes a valid event by evaluating notification rules and routing to appropriate channels.
     *
     * @param event The event to process
     * @param correlationId The correlation ID for distributed tracing
     */
    private void processEvent(Event event, String correlationId) {
        if (ruleEvaluator.shouldNotify(event)) {
            LOGGER.debug("Event matched notification rules: type={}, deviceId={}, correlationId={}", 
                    event.getType(), event.getDeviceId(), correlationId);
            
            // Determine recipients and notification channels
            var notificationRecipients = ruleEvaluator.getNotificationRecipients(event);
            
            // Format and publish notifications to appropriate channels
            notificationRecipients.forEach(recipient -> {
                try {
                    channelPublisher.publishNotification(event, recipient, correlationId);
                } catch (Exception e) {
                    LOGGER.error("Failed to publish notification for event: type={}, deviceId={}, recipient={}, correlationId={}", 
                            event.getType(), event.getDeviceId(), recipient, correlationId, e);
                }
            });
        } else {
            LOGGER.debug("Event did not match any notification rules: type={}, deviceId={}, correlationId={}", 
                    event.getType(), event.getDeviceId(), correlationId);
        }
    }
}