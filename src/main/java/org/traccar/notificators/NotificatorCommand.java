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
package org.traccar.notificators;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.traccar.database.CommandsManager;
import org.traccar.messaging.MessageBroker;
import org.traccar.metrics.NotificationMetrics;
import org.traccar.model.Command;
import org.traccar.model.Event;
import org.traccar.model.Notification;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.MessageException;
import org.traccar.storage.Storage;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;
import org.traccar.tracing.DistributedTracingContext;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * Notificator that sends commands to devices.
 * Implements circuit breaker pattern, rate limiting, distributed tracing, and metrics collection.
 */
@Singleton
public class NotificatorCommand extends Notificator {

    private final Storage storage;
    private final CommandsManager commandsManager;
    private final CircuitBreaker circuitBreaker;
    private final RateLimiter rateLimiter;
    private final NotificationMetrics metrics;
    private final DistributedTracingContext tracingContext;
    private final MessageBroker messageBroker;

    /**
     * Constructor for NotificatorCommand.
     * Initializes circuit breaker, rate limiter, metrics, and tracing context.
     *
     * @param storage Storage instance for retrieving commands
     * @param commandsManager CommandsManager for sending commands
     * @param metrics NotificationMetrics for collecting metrics
     * @param tracingContext DistributedTracingContext for distributed tracing
     * @param messageBroker MessageBroker for sending failed commands to dead letter queue
     */
    @Inject
    public NotificatorCommand(
            Storage storage, 
            CommandsManager commandsManager,
            NotificationMetrics metrics,
            DistributedTracingContext tracingContext,
            MessageBroker messageBroker) {
        super(null, null);
        this.storage = storage;
        this.commandsManager = commandsManager;
        this.metrics = metrics;
        this.tracingContext = tracingContext;
        this.messageBroker = messageBroker;
        
        // Initialize circuit breaker with configuration
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to open circuit
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds in OPEN state
                .slidingWindowSize(10) // Consider last 10 calls for failure rate
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in HALF_OPEN state
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("commandNotificator");
        
        // Initialize rate limiter with configuration
        RateLimiterConfig rateLimiterConfig = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofSeconds(1)) // Refresh limit every second
                .limitForPeriod(50) // Allow 50 calls per refresh period
                .timeoutDuration(Duration.ofSeconds(5)) // Wait 5 seconds for permission
                .build();
        
        RateLimiterRegistry rateLimiterRegistry = RateLimiterRegistry.of(rateLimiterConfig);
        this.rateLimiter = rateLimiterRegistry.rateLimiter("commandNotificator");
    }

    @Override
    public void send(Notification notification, User user, Event event, Position position) throws MessageException {
        // Start tracing span
        Tracer tracer = tracingContext.getTracer();
        Span span = tracer.spanBuilder("NotificatorCommand.send")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("notification.id", notification != null ? notification.getId() : 0)
                .setAttribute("event.id", event != null ? event.getId() : 0)
                .setAttribute("device.id", event != null ? event.getDeviceId() : 0)
                .startSpan();
        
        try (io.opentelemetry.context.Scope scope = span.makeCurrent()) {
            // Validate notification
            if (notification == null || notification.getCommandId() <= 0) {
                span.setStatus(StatusCode.ERROR, "Saved command not provided");
                metrics.recordFailedNotification("command_not_provided");
                throw new MessageException("Saved command not provided");
            }
            
            // Use rate limiter to prevent overwhelming the command service
            rateLimiter.acquirePermission();
            
            // Use circuit breaker to prevent cascading failures
            Supplier<Void> commandSupplier = () -> {
                try {
                    // Record metrics for command retrieval start
                    metrics.recordCommandRetrievalStart();
                    
                    // Retrieve command from storage
                    Command command = storage.getObject(Command.class, new Request(
                            new Columns.All(), new Condition.Equals("id", notification.getCommandId())));
                    
                    // Record metrics for command retrieval success
                    metrics.recordCommandRetrievalSuccess();
                    
                    // Set device ID and send command
                    command.setDeviceId(event.getDeviceId());
                    
                    // Record metrics for command sending start
                    metrics.recordCommandSendingStart();
                    
                    // Send command
                    commandsManager.sendCommand(command);
                    
                    // Record metrics for command sending success
                    metrics.recordCommandSendingSuccess();
                    
                    // Set span status to success
                    span.setStatus(StatusCode.OK);
                    
                } catch (Exception e) {
                    // Record metrics for failure
                    metrics.recordFailedNotification(e.getClass().getSimpleName());
                    
                    // Set span status to error with exception details
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    
                    // Send failed command to dead letter queue for later processing
                    sendToDeadLetterQueue(notification, user, event, position, e);
                    
                    // Rethrow exception to be handled by circuit breaker
                    throw new RuntimeException(e);
                }
                return null;
            };
            
            try {
                // Execute command with circuit breaker protection
                circuitBreaker.executeSupplier(commandSupplier);
                
                // Record successful notification
                metrics.recordSuccessfulNotification();
                
            } catch (Exception e) {
                // Record metrics for circuit breaker failure
                metrics.recordCircuitBreakerFailure();
                
                // Set span status to error with exception details
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.recordException(e);
                
                // Throw MessageException to be handled by caller
                throw new MessageException(e);
            }
        } finally {
            // End the span regardless of outcome
            span.end();
        }
    }
    
    /**
     * Sends failed command to dead letter queue for later processing.
     *
     * @param notification The notification that failed
     * @param user The user associated with the notification
     * @param event The event that triggered the notification
     * @param position The position associated with the event
     * @param exception The exception that caused the failure
     */
    private void sendToDeadLetterQueue(Notification notification, User user, Event event, Position position, Exception exception) {
        try {
            // Create dead letter queue message with all relevant information
            DeadLetterQueueMessage message = new DeadLetterQueueMessage(
                    notification, user, event, position, exception.getMessage());
            
            // Send to dead letter queue topic
            messageBroker.publish("notification.command.deadletter", message);
            
            // Record metrics for dead letter queue
            metrics.recordDeadLetterQueueMessage();
            
        } catch (Exception e) {
            // If sending to dead letter queue fails, just log the error
            // We don't want to throw another exception here
            System.err.println("Failed to send to dead letter queue: " + e.getMessage());
        }
    }
    
    /**
     * Message structure for dead letter queue.
     */
    private static class DeadLetterQueueMessage {
        private final long notificationId;
        private final long userId;
        private final long eventId;
        private final long positionId;
        private final String errorMessage;
        
        public DeadLetterQueueMessage(Notification notification, User user, Event event, Position position, String errorMessage) {
            this.notificationId = notification != null ? notification.getId() : 0;
            this.userId = user != null ? user.getId() : 0;
            this.eventId = event != null ? event.getId() : 0;
            this.positionId = position != null ? position.getId() : 0;
            this.errorMessage = errorMessage;
        }
        
        public long getNotificationId() {
            return notificationId;
        }
        
        public long getUserId() {
            return userId;
        }
        
        public long getEventId() {
            return eventId;
        }
        
        public long getPositionId() {
            return positionId;
        }
        
        public String getErrorMessage() {
            return errorMessage;
        }
    }
}