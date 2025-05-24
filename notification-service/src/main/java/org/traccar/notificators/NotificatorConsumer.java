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

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Event;
import org.traccar.model.Notification;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.MessageException;
import org.traccar.notification.NotificationFormatter;
import org.traccar.notification.NotificationMessage;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Base class for all notificator consumers that subscribe to channel-specific message broker topics.
 * Provides common functionality for consuming notification messages, handling retries, and reporting delivery status.
 * This component is essential for the asynchronous notification delivery pipeline, enabling each notificator
 * to process messages from its dedicated topic.
 * 
 * <p>Implementation classes should:</p>
 * <ul>
 *   <li>Implement the message broker specific consumer logic</li>
 *   <li>Handle dead letter queue publishing for failed deliveries</li>
 *   <li>Report delivery status back to the notification service</li>
 *   <li>Propagate correlation IDs for distributed tracing</li>
 * </ul>
 * 
 * <p>Example usage:</p>
 * <pre>
 * @Component
 * public class EmailNotificatorConsumer extends NotificatorConsumer {
 *     
 *     @Autowired
 *     public EmailNotificatorConsumer(NotificationFormatter formatter, RetryRegistry retryRegistry,
 *                                    Tracer tracer, MeterRegistry meterRegistry) {
 *         super(formatter, "email/templates", "email", retryRegistry, tracer, meterRegistry);
 *     }
 *     
 *     // Implementation details
 * }
 * </pre>
 */
public abstract class NotificatorConsumer extends Notificator {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificatorConsumer.class);

    private final String channelName;
    private final RetryRegistry retryRegistry;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Executor executor;
    
    // Configuration properties
    private String topicName;
    private String consumerGroupId;
    private String deadLetterTopic;
    private String statusTopic;

    // Metrics
    private final Counter deliveryAttempts;
    private final Counter deliverySuccesses;
    private final Counter deliveryFailures;
    private final Counter deadLetterPublishes;
    private final Timer deliveryTimer;

    /**
     * Constructor for NotificatorConsumer.
     *
     * @param notificationFormatter The formatter for notification messages
     * @param templatePath The path to notification templates
     * @param channelName The name of the notification channel (e.g., "email", "sms")
     * @param retryRegistry The registry for retry configurations
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param meterRegistry The Micrometer registry for metrics
     */
    protected NotificatorConsumer(
            NotificationFormatter notificationFormatter,
            String templatePath,
            String channelName,
            RetryRegistry retryRegistry,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        super(notificationFormatter, templatePath);
        this.channelName = channelName;
        this.retryRegistry = retryRegistry;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();

        // Initialize metrics
        this.deliveryAttempts = Counter.builder("notification.delivery.attempts")
                .tag("channel", channelName)
                .description("Number of notification delivery attempts")
                .register(meterRegistry);
        this.deliverySuccesses = Counter.builder("notification.delivery.successes")
                .tag("channel", channelName)
                .description("Number of successful notification deliveries")
                .register(meterRegistry);
        this.deliveryFailures = Counter.builder("notification.delivery.failures")
                .tag("channel", channelName)
                .description("Number of failed notification deliveries")
                .register(meterRegistry);
        this.deadLetterPublishes = Counter.builder("notification.delivery.deadletter")
                .tag("channel", channelName)
                .description("Number of notifications sent to dead letter queue")
                .register(meterRegistry);
        this.deliveryTimer = Timer.builder("notification.delivery.time")
                .tag("channel", channelName)
                .description("Time taken to deliver notifications")
                .register(meterRegistry);
    }

    /**
     * Initializes the consumer and starts listening to the channel-specific topic.
     * This method should be called during service startup.
     * 
     * @return This consumer instance for method chaining
     */
    @PostConstruct
    public abstract NotificatorConsumer initialize();

    /**
     * Stops the consumer and closes connections to the message broker.
     * This method should be called during service shutdown.
     */
    @PreDestroy
    public abstract void shutdown();
    
    /**
     * Configures the consumer with the specified topic name.
     * 
     * @param topicName The name of the topic to consume from
     * @return This consumer instance for method chaining
     */
    public abstract NotificatorConsumer withTopic(String topicName);
    
    /**
     * Configures the consumer with the specified consumer group ID.
     * 
     * @param groupId The consumer group ID
     * @return This consumer instance for method chaining
     */
    public abstract NotificatorConsumer withConsumerGroup(String groupId);
    
    /**
     * Configures the consumer with the specified dead letter queue topic.
     * 
     * @param deadLetterTopic The name of the dead letter queue topic
     * @return This consumer instance for method chaining
     */
    public abstract NotificatorConsumer withDeadLetterTopic(String deadLetterTopic);
    
    /**
     * Configures the consumer with the specified status reporting topic.
     * 
     * @param statusTopic The name of the status reporting topic
     * @return This consumer instance for method chaining
     */
    public abstract NotificatorConsumer withStatusTopic(String statusTopic);

    /**
     * Processes a notification message received from the message broker.
     * This method handles the common processing logic, including tracing, metrics, and retries.
     *
     * @param user The recipient user
     * @param message The notification message
     * @param event The event that triggered the notification
     * @param position The position associated with the event
     * @param correlationId The correlation ID for distributed tracing, or null to generate a new one
     * @return A CompletableFuture that completes when the notification is processed
     */
    protected CompletableFuture<Void> processNotification(
            User user,
            NotificationMessage message,
            Event event,
            Position position,
            String correlationId) {
        
        // Generate a correlation ID if none was provided
        final String traceId = correlationId != null ? correlationId : UUID.randomUUID().toString();

        return CompletableFuture.runAsync(() -> {
            Span span = tracer.spanBuilder("process_" + channelName + "_notification")
                    .setParent(Context.current().with(Span.current()))
                    .setSpanKind(SpanKind.CONSUMER)
                    .setAttribute("notification.channel", channelName)
                    .setAttribute("notification.correlation_id", traceId)
                    .setAttribute("notification.user_id", String.valueOf(user.getId()))
                    .setAttribute("notification.event_id", String.valueOf(event.getId()))
                    .startSpan();

            try (Scope scope = span.makeCurrent()) {
                deliveryAttempts.increment();
                LOGGER.debug("Processing {} notification for user {}, correlation ID: {}",
                        channelName, user.getId(), traceId);

                // Get retry configuration for this channel
                Retry retry = retryRegistry.retry(channelName + "Notificator",
                        () -> createRetryConfig(channelName));

                // Execute with retry
                Supplier<Void> deliverySupplier = Retry.decorateSupplier(retry, () -> {
                    try {
                        deliveryTimer.record(() -> {
                            try {
                                send(user, message, event, position);
                            } catch (MessageException e) {
                                throw new RuntimeException(e);
                            }
                        });
                        return null;
                    } catch (Exception e) {
                        LOGGER.warn("Failed to deliver {} notification to user {}, attempt will be retried. Correlation ID: {}",
                                channelName, user.getId(), traceId, e);
                        span.recordException(e);
                        throw e;
                    }
                });

                try {
                    deliverySupplier.get();
                    deliverySuccesses.increment();
                    span.setAttribute("notification.status", "delivered");
                    LOGGER.info("Successfully delivered {} notification to user {}, correlation ID: {}",
                            channelName, user.getId(), traceId);
                    reportDeliveryStatus(user, event, true, null, traceId);
                } catch (Exception e) {
                    deliveryFailures.increment();
                    span.setAttribute("notification.status", "failed");
                    LOGGER.error("Failed to deliver {} notification to user {} after all retry attempts. Correlation ID: {}",
                            channelName, user.getId(), traceId, e);
                    publishToDeadLetterQueue(user, message, event, position, e, traceId);
                    reportDeliveryStatus(user, event, false, e.getMessage(), traceId);
                }
            } finally {
                span.end();
            }
        }, executor);
    }

    /**
     * Creates a retry configuration for the specified channel.
     *
     * @param channel The notification channel
     * @return The retry configuration
     */
    protected RetryConfig createRetryConfig(String channel) {
        return RetryConfig.custom()
                .maxAttempts(getMaxRetryAttempts())
                .waitDuration(Duration.ofMillis(getInitialBackoffMillis()))
                .retryExceptions(RuntimeException.class)
                .retryOnResult(result -> false) // Don't retry on result
                .exponentialBackoff(getBackoffMultiplier(), Duration.ofMillis(getMaxBackoffMillis()))
                .failAfterMaxAttempts(true)
                .build();
    }

    /**
     * Gets the maximum number of retry attempts for failed deliveries.
     *
     * @return The maximum number of retry attempts
     */
    @Value("${notification.retry.max-attempts:3}")
    private int maxRetryAttempts;

    @Value("${notification.retry.initial-backoff:1000}")
    private long initialBackoffMillis;

    @Value("${notification.retry.max-backoff:30000}")
    private long maxBackoffMillis;

    @Value("${notification.retry.backoff-multiplier:2.0}")
    private double backoffMultiplier;
    
    /**
     * Gets the maximum number of retry attempts for failed deliveries.
     *
     * @return The maximum number of retry attempts
     */
    protected int getMaxRetryAttempts() {
        return maxRetryAttempts;
    }

    /**
     * Gets the initial backoff time in milliseconds for retry attempts.
     *
     * @return The initial backoff time in milliseconds
     */
    protected long getInitialBackoffMillis() {
        return initialBackoffMillis;
    }

    /**
     * Gets the maximum backoff time in milliseconds for retry attempts.
     *
     * @return The maximum backoff time in milliseconds
     */
    protected long getMaxBackoffMillis() {
        return maxBackoffMillis;
    }

    /**
     * Gets the multiplier for exponential backoff calculation.
     *
     * @return The backoff multiplier
     */
    protected double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    /**
     * Publishes a failed notification to the dead letter queue.
     * This method should be implemented by subclasses to handle the specific message broker.
     *
     * @param user The recipient user
     * @param message The notification message
     * @param event The event that triggered the notification
     * @param position The position associated with the event
     * @param exception The exception that caused the failure
     * @param correlationId The correlation ID for distributed tracing
     */
    protected abstract void publishToDeadLetterQueue(
            User user,
            NotificationMessage message,
            Event event,
            Position position,
            Exception exception,
            String correlationId);

    /**
     * Reports the delivery status back to the notification service.
     * This method should be implemented by subclasses to handle the specific message broker.
     *
     * @param user The recipient user
     * @param event The event that triggered the notification
     * @param success Whether the delivery was successful
     * @param errorMessage The error message if delivery failed, null otherwise
     * @param correlationId The correlation ID for distributed tracing
     */
    protected abstract void reportDeliveryStatus(
            User user,
            Event event,
            boolean success,
            String errorMessage,
            String correlationId);

    /**
     * Gets the name of the notification channel.
     *
     * @return The channel name
     */
    public String getChannelName() {
        return channelName;
    }
    
    /**
     * Gets the topic name for this consumer.
     *
     * @return The topic name
     */
    public String getTopicName() {
        return topicName;
    }
    
    /**
     * Gets the consumer group ID for this consumer.
     *
     * @return The consumer group ID
     */
    public String getConsumerGroupId() {
        return consumerGroupId;
    }
    
    /**
     * Gets the dead letter topic name for this consumer.
     *
     * @return The dead letter topic name
     */
    public String getDeadLetterTopic() {
        return deadLetterTopic;
    }
    
    /**
     * Gets the status topic name for this consumer.
     *
     * @return The status topic name
     */
    public String getStatusTopic() {
        return statusTopic;
    }

    /**
     * Creates a notification message with the specified headers.
     *
     * @param notification The notification to format
     * @param user The recipient user
     * @param event The event that triggered the notification
     * @param position The position associated with the event
     * @param headers Additional headers to include
     * @return The formatted notification message
     * @throws MessageException If there is an error formatting the message
     */
    protected NotificationMessage createMessage(
            Notification notification,
            User user,
            Event event,
            Position position,
            Map<String, String> headers) throws MessageException {
        NotificationMessage message = super.notificationFormatter.formatMessage(
                notification, user, event, position, templatePath);
        // In a future implementation, NotificationMessage could be extended to support headers
        // For now, we'll just return the formatted message
        return message;
    }
}