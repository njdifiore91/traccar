/*
 * Copyright 2018 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.notificators;

import org.traccar.model.Event;
import org.traccar.model.Notification;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.NotificationFormatter;
import org.traccar.messaging.MessageBrokerManager;
import org.traccar.metrics.NotificationMetrics;
import org.traccar.tracing.DistributedTracingContext;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Web notificator that publishes notifications to the message broker for WebSocket delivery.
 * This class is part of the Notification Service in the microservices architecture.
 */
@Singleton
public final class NotificatorWeb extends Notificator {

    private final MessageBrokerManager messageBrokerManager;
    private final NotificationFormatter notificationFormatter;
    private final NotificationMetrics notificationMetrics;
    private final DistributedTracingContext tracingContext;

    /**
     * Constructs a new NotificatorWeb with required dependencies.
     * 
     * @param messageBrokerManager The message broker manager for publishing notifications
     * @param notificationFormatter The formatter for notification messages
     * @param notificationMetrics The metrics collector for notifications
     * @param tracingContext The distributed tracing context
     */
    @Inject
    public NotificatorWeb(
            MessageBrokerManager messageBrokerManager,
            NotificationFormatter notificationFormatter,
            NotificationMetrics notificationMetrics,
            DistributedTracingContext tracingContext) {
        super(null, null);
        this.messageBrokerManager = messageBrokerManager;
        this.notificationFormatter = notificationFormatter;
        this.notificationMetrics = notificationMetrics;
        this.tracingContext = tracingContext;
    }

    /**
     * Sends a notification to a user via WebSocket by publishing to the message broker.
     * Includes distributed tracing context and collects metrics.
     *
     * @param notification The notification configuration
     * @param user The target user
     * @param event The event that triggered the notification
     * @param position The position associated with the event (may be null)
     */
    @Override
    public void send(Notification notification, User user, Event event, Position position) {
        // Create a span for this notification operation
        try (var span = tracingContext.startSpan("web.notification.send")) {
            // Add relevant attributes to the span
            span.setAttribute("notification.type", event.getType());
            span.setAttribute("notification.userId", user.getId());
            
            // Create a copy of the event to avoid modifying the original
            Event copy = new Event();
            copy.setId(event.getId());
            copy.setDeviceId(event.getDeviceId());
            copy.setType(event.getType());
            copy.setEventTime(event.getEventTime());
            copy.setPositionId(event.getPositionId());
            copy.setGeofenceId(event.getGeofenceId());
            copy.setMaintenanceId(event.getMaintenanceId());
            copy.getAttributes().putAll(event.getAttributes());

            // Format the notification message
            var message = notificationFormatter.formatMessage(notification, user, event, position, "short");
            copy.set("message", message.getBody());
            
            // Add correlation ID for distributed tracing
            copy.set("correlationId", tracingContext.getCurrentTraceId());
            
            // Publish the notification to the web-out topic in the message broker
            messageBrokerManager.publishWebNotification(user.getId(), copy);
            
            // Record metrics for this notification
            notificationMetrics.recordWebNotificationSent(event.getType());
        } catch (Exception e) {
            notificationMetrics.recordWebNotificationError(event.getType());
            throw e;
        }
    }

    /**
     * Performs a health check for the WebSocket notification capability.
     * 
     * @return true if the WebSocket notification system is healthy, false otherwise
     */
    public boolean checkHealth() {
        return messageBrokerManager.checkWebNotificationHealth();
    }
}