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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import org.traccar.messaging.MessageBrokerManager;
import org.traccar.metrics.NotificationMetrics;
import org.traccar.model.Event;
import org.traccar.model.Notification;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.MessageException;
import org.traccar.notification.NotificationFormatter;
import org.traccar.notification.NotificationMessage;

/**
 * Base class for all notification delivery methods.
 * Provides common functionality for formatting messages and handling delivery.
 */
public abstract class Notificator {

    private final NotificationFormatter notificationFormatter;
    private final String templatePath;
    private final MessageBrokerManager messageBrokerManager;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final NotificationMetrics metrics;

    /**
     * Constructor for Notificator with all required dependencies.
     *
     * @param notificationFormatter Formatter for notification messages
     * @param templatePath Path to notification templates
     * @param messageBrokerManager Manager for asynchronous message delivery
     * @param circuitBreaker Circuit breaker for resilient notification delivery
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param metrics Metrics collector for notification statistics
     */
    public Notificator(NotificationFormatter notificationFormatter, 
                      String templatePath,
                      MessageBrokerManager messageBrokerManager,
                      CircuitBreaker circuitBreaker,
                      Tracer tracer,
                      NotificationMetrics metrics) {
        this.notificationFormatter = notificationFormatter;
        this.templatePath = templatePath;
        this.messageBrokerManager = messageBrokerManager;
        this.circuitBreaker = circuitBreaker;
        this.tracer = tracer;
        this.metrics = metrics;
    }

    /**
     * Sends a notification to a user based on an event and position.
     * This method formats the message and then calls the send method with the formatted message.
     *
     * @param notification Notification configuration
     * @param user User to send the notification to
     * @param event Event that triggered the notification
     * @param position Position associated with the event
     * @throws MessageException If there is an error sending the message
     */
    public void send(Notification notification, User user, Event event, Position position) throws MessageException {
        Span span = tracer.spanBuilder("notificator.format").setParent(Context.current()).startSpan();
        try {
            span.setAttribute("notification.type", notification.getType());
            span.setAttribute("notification.user", user.getId());
            span.setAttribute("notification.event", event.getType());
            
            var message = notificationFormatter.formatMessage(notification, user, event, position, templatePath);
            send(user, message, event, position);
        } finally {
            span.end();
        }
    }

    /**
     * Sends a pre-formatted notification message to a user.
     * This method should be implemented by subclasses to handle the actual delivery.
     * The base implementation throws UnsupportedOperationException.
     *
     * @param user User to send the notification to
     * @param message Pre-formatted notification message
     * @param event Event that triggered the notification
     * @param position Position associated with the event
     * @throws MessageException If there is an error sending the message
     */
    public void send(User user, NotificationMessage message, Event event, Position position) throws MessageException {
        throw new UnsupportedOperationException();
    }

    /**
     * Sends a notification asynchronously using the message broker.
     * This method can be used by subclasses to delegate the actual delivery to the message broker.
     *
     * @param user User to send the notification to
     * @param message Pre-formatted notification message
     * @param event Event that triggered the notification
     * @param position Position associated with the event
     */
    protected void sendAsync(User user, NotificationMessage message, Event event, Position position) {
        Span span = tracer.spanBuilder("notificator.sendAsync").setParent(Context.current()).startSpan();
        try {
            span.setAttribute("notification.channel", getClass().getSimpleName());
            span.setAttribute("notification.user", user.getId());
            span.setAttribute("notification.event", event.getType());
            
            metrics.recordNotificationAttempt(getClass().getSimpleName());
            
            messageBrokerManager.publishNotification(user, message, event, position, getClass().getSimpleName());
        } finally {
            span.end();
        }
    }

    /**
     * Executes a notification delivery operation with circuit breaker protection.
     * This method can be used by subclasses to wrap external API calls with circuit breaker protection.
     *
     * @param <T> Return type of the operation
     * @param operation Operation to execute with circuit breaker protection
     * @return Result of the operation
     * @throws MessageException If there is an error executing the operation
     */
    protected <T> T executeWithCircuitBreaker(CircuitBreakerOperation<T> operation) throws MessageException {
        Span span = tracer.spanBuilder("notificator.circuitBreaker").setParent(Context.current()).startSpan();
        try {
            span.setAttribute("circuitBreaker.state", circuitBreaker.getState().name());
            
            return circuitBreaker.executeCheckedSupplier(() -> {
                try {
                    T result = operation.execute();
                    metrics.recordNotificationSuccess(getClass().getSimpleName());
                    return result;
                } catch (Exception e) {
                    metrics.recordNotificationFailure(getClass().getSimpleName());
                    throw e;
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            if (e instanceof MessageException) {
                throw (MessageException) e;
            } else {
                throw new MessageException(e);
            }
        } finally {
            span.end();
        }
    }

    /**
     * Functional interface for operations that can be executed with circuit breaker protection.
     *
     * @param <T> Return type of the operation
     */
    @FunctionalInterface
    protected interface CircuitBreakerOperation<T> {
        T execute() throws Exception;
    }

    /**
     * Returns the health status of this notificator.
     * This method can be used by health check endpoints to determine if the notificator is healthy.
     *
     * @return true if the notificator is healthy, false otherwise
     */
    public boolean isHealthy() {
        return !circuitBreaker.getState().equals(CircuitBreaker.State.OPEN);
    }
}