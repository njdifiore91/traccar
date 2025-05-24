/*
 * Copyright 2016 - 2023 Anton Tananaev (anton@traccar.org)
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

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.traccar.messaging.consumer.EventMessageConsumer;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.template.TemplateContextBuilder;
import org.traccar.template.TemplateEngine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Formats notifications by combining templates with context data retrieved from various sources.
 * This class is responsible for preparing notification content for delivery through multiple channels.
 * It uses asynchronous processing, distributed caching, and circuit breakers for resilience.
 */
@Service
public class NotificationFormatter {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationFormatter.class);

    private final TemplateEngine templateEngine;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, Object> redisTemplate;
    private final DiscoveryClient discoveryClient;
    private final Tracer tracer;
    private final PropertiesProvider propertiesProvider;

    @Value("${notification.context.request.topic:context-request}")
    private String contextRequestTopic;

    @Value("${notification.context.response.topic:context-response}")
    private String contextResponseTopic;

    @Value("${notification.context.request.timeout:5000}")
    private long contextRequestTimeout;

    /**
     * Constructs a new NotificationFormatter with required dependencies.
     *
     * @param templateEngine     The template engine for rendering notification content
     * @param kafkaTemplate     Kafka template for message broker communication
     * @param redisTemplate     Redis template for distributed caching
     * @param discoveryClient   Service discovery client for locating dependent services
     * @param tracer            OpenTelemetry tracer for distributed tracing
     * @param propertiesProvider Provider for accessing configuration properties
     */
    @Autowired
    public NotificationFormatter(
            TemplateEngine templateEngine,
            KafkaTemplate<String, Object> kafkaTemplate,
            RedisTemplate<String, Object> redisTemplate,
            DiscoveryClient discoveryClient,
            Tracer tracer,
            PropertiesProvider propertiesProvider) {
        this.templateEngine = templateEngine;
        this.kafkaTemplate = kafkaTemplate;
        this.redisTemplate = redisTemplate;
        this.discoveryClient = discoveryClient;
        this.tracer = tracer;
        this.propertiesProvider = propertiesProvider;
    }

    /**
     * Asynchronously formats a notification for a specific user based on an event and position.
     * Uses distributed tracing to track the formatting process across service boundaries.
     *
     * @param user      The user receiving the notification
     * @param event     The event triggering the notification
     * @param position  The position associated with the event
     * @param template  The notification template to use
     * @param correlationId The correlation ID for distributed tracing
     * @return A CompletableFuture containing the formatted notification text
     */
    public CompletableFuture<String> formatAsync(
            User user, Event event, Position position, String template, String correlationId) {
        Span span = tracer.spanBuilder("notification.format")
                .setParent(Context.current().with(Span.current()))
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("correlationId", correlationId)
                .setAttribute("userId", user.getId())
                .setAttribute("eventId", event.getId())
                .setAttribute("templateName", template)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            LOGGER.debug("Formatting notification for user {} with template {}", user.getId(), template);
            return buildContextAsync(user, event, position, correlationId)
                    .thenCompose(context -> renderTemplateAsync(template, context, correlationId));
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            return CompletableFuture.failedFuture(e);
        } finally {
            span.end();
        }
    }

    /**
     * Asynchronously builds the context data needed for template rendering.
     * Uses circuit breakers to protect against failures when retrieving data from external sources.
     *
     * @param user      The user receiving the notification
     * @param event     The event triggering the notification
     * @param position  The position associated with the event
     * @param correlationId The correlation ID for distributed tracing
     * @return A CompletableFuture containing the context data map
     */
    @CircuitBreaker(name = "contextBuilder", fallbackMethod = "buildContextFallback")
    @Retry(name = "contextBuilder")
    private CompletableFuture<Map<String, Object>> buildContextAsync(
            User user, Event event, Position position, String correlationId) {
        Span span = tracer.spanBuilder("notification.buildContext")
                .setParent(Context.current().with(Span.current()))
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("correlationId", correlationId)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            // First check if the context is already cached in Redis
            String cacheKey = "notification:context:" + event.getId();
            Map<String, Object> cachedContext = (Map<String, Object>) redisTemplate.opsForValue().get(cacheKey);
            if (cachedContext != null) {
                LOGGER.debug("Using cached context for event {}", event.getId());
                span.setAttribute("cache.hit", true);
                return CompletableFuture.completedFuture(cachedContext);
            }
            span.setAttribute("cache.hit", false);

            // If not cached, build the context asynchronously
            return retrieveDeviceAsync(event.getDeviceId(), correlationId)
                    .thenCompose(device -> {
                        // Use TemplateContextBuilder to construct the context
                        return new TemplateContextBuilder()
                                .withUser(user)
                                .withDevice(device)
                                .withEvent(event)
                                .withPosition(position)
                                .withCorrelationId(correlationId)
                                .withServerProperties(propertiesProvider)
                                .buildAsync()
                                .thenApply(context -> {
                                    // Cache the context in Redis for future use
                                    redisTemplate.opsForValue().set(cacheKey, context);
                                    return context;
                                });
                    });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            return CompletableFuture.failedFuture(e);
        } finally {
            span.end();
        }
    }

    /**
     * Fallback method for context building when the circuit breaker is open.
     * Provides a minimal context with available data to ensure notifications can still be sent.
     *
     * @param user      The user receiving the notification
     * @param event     The event triggering the notification
     * @param position  The position associated with the event
     * @param correlationId The correlation ID for distributed tracing
     * @param e         The exception that triggered the fallback
     * @return A CompletableFuture containing a minimal context map
     */
    private CompletableFuture<Map<String, Object>> buildContextFallback(
            User user, Event event, Position position, String correlationId, Exception e) {
        LOGGER.warn("Using fallback context builder due to error: {}", e.getMessage());
        Map<String, Object> fallbackContext = new HashMap<>();
        fallbackContext.put("user", user);
        fallbackContext.put("event", event);
        if (position != null) {
            fallbackContext.put("position", position);
        }
        fallbackContext.put("correlationId", correlationId);
        return CompletableFuture.completedFuture(fallbackContext);
    }

    /**
     * Asynchronously retrieves device information using the message broker.
     * Uses service discovery to locate the device service and circuit breakers for resilience.
     *
     * @param deviceId      The ID of the device to retrieve
     * @param correlationId The correlation ID for distributed tracing
     * @return A CompletableFuture containing the device information
     */
    @CircuitBreaker(name = "deviceRetrieval", fallbackMethod = "retrieveDeviceFallback")
    @Retry(name = "deviceRetrieval")
    private CompletableFuture<Device> retrieveDeviceAsync(long deviceId, String correlationId) {
        Span span = tracer.spanBuilder("notification.retrieveDevice")
                .setParent(Context.current().with(Span.current()))
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("correlationId", correlationId)
                .setAttribute("deviceId", deviceId)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            // First check if the device is already cached in Redis
            String cacheKey = "notification:device:" + deviceId;
            Device cachedDevice = (Device) redisTemplate.opsForValue().get(cacheKey);
            if (cachedDevice != null) {
                LOGGER.debug("Using cached device information for device {}", deviceId);
                span.setAttribute("cache.hit", true);
                return CompletableFuture.completedFuture(cachedDevice);
            }
            span.setAttribute("cache.hit", false);

            // If not cached, retrieve the device using the message broker
            CompletableFuture<Device> future = new CompletableFuture<>();

            // Create a request message with the device ID and correlation ID
            Map<String, Object> request = new HashMap<>();
            request.put("type", "device");
            request.put("deviceId", deviceId);
            request.put("correlationId", correlationId);

            // Send the request to the context request topic
            kafkaTemplate.send(contextRequestTopic, correlationId, request);

            // Set up a timeout for the request
            CompletableFuture.delayedExecutor(contextRequestTimeout, java.util.concurrent.TimeUnit.MILLISECONDS)
                    .execute(() -> {
                        if (!future.isDone()) {
                            future.completeExceptionally(new MessageException("Device retrieval timed out"));
                        }
                    });

            // The response will be handled by a message listener that will complete the future
            // This is a simplified example - in a real implementation, you would need to set up a
            // message listener that matches responses to requests using the correlation ID

            return future.thenApply(device -> {
                // Cache the device in Redis for future use
                redisTemplate.opsForValue().set(cacheKey, device);
                return device;
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            return CompletableFuture.failedFuture(e);
        } finally {
            span.end();
        }
    }

    /**
     * Fallback method for device retrieval when the circuit breaker is open.
     * Creates a minimal device object with the ID to ensure notifications can still be sent.
     *
     * @param deviceId      The ID of the device to retrieve
     * @param correlationId The correlation ID for distributed tracing
     * @param e             The exception that triggered the fallback
     * @return A CompletableFuture containing a minimal device object
     */
    private CompletableFuture<Device> retrieveDeviceFallback(long deviceId, String correlationId, Exception e) {
        LOGGER.warn("Using fallback device retrieval due to error: {}", e.getMessage());
        Device fallbackDevice = new Device();
        fallbackDevice.setId(deviceId);
        fallbackDevice.setName("Device " + deviceId);
        return CompletableFuture.completedFuture(fallbackDevice);
    }

    /**
     * Asynchronously renders a template with the provided context data.
     * Uses the template engine to process the template and apply the context.
     *
     * @param template      The name of the template to render
     * @param context       The context data to apply to the template
     * @param correlationId The correlation ID for distributed tracing
     * @return A CompletableFuture containing the rendered template text
     */
    @CircuitBreaker(name = "templateRendering", fallbackMethod = "renderTemplateFallback")
    @Retry(name = "templateRendering")
    @Cacheable(value = "renderedTemplates", key = "#template + '-' + #correlationId")
    private CompletableFuture<String> renderTemplateAsync(
            String template, Map<String, Object> context, String correlationId) {
        Span span = tracer.spanBuilder("notification.renderTemplate")
                .setParent(Context.current().with(Span.current()))
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("correlationId", correlationId)
                .setAttribute("templateName", template)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            LOGGER.debug("Rendering template {} with context", template);
            return templateEngine.processAsync(template, context);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            return CompletableFuture.failedFuture(e);
        } finally {
            span.end();
        }
    }

    /**
     * Fallback method for template rendering when the circuit breaker is open.
     * Provides a simple text message to ensure notifications can still be sent.
     *
     * @param template      The name of the template to render
     * @param context       The context data to apply to the template
     * @param correlationId The correlation ID for distributed tracing
     * @param e             The exception that triggered the fallback
     * @return A CompletableFuture containing a simple notification message
     */
    private CompletableFuture<String> renderTemplateFallback(
            String template, Map<String, Object> context, String correlationId, Exception e) {
        LOGGER.warn("Using fallback template rendering due to error: {}", e.getMessage());
        Event event = (Event) context.get("event");
        String fallbackMessage = "Notification: " + (event != null ? event.getType() : "Unknown event type");
        return CompletableFuture.completedFuture(fallbackMessage);
    }
    
    /**
     * Formats a notification for the web interface, creating a structured data object
     * that can be serialized to JSON and sent to WebSocket clients.
     *
     * @param notification The notification to format
     * @param user The user receiving the notification
     * @return A map containing the formatted notification data
     */
    @CircuitBreaker(name = "webFormatting", fallbackMethod = "formatForWebFallback")
    public Map<String, Object> formatForWeb(Notification notification, User user) {
        Span span = tracer.spanBuilder("notification.formatForWeb")
                .setParent(Context.current().with(Span.current()))
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("userId", user.getId())
                .setAttribute("notificationId", notification.getId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            LOGGER.debug("Formatting web notification for user {}", user.getId());
            
            Map<String, Object> data = new HashMap<>();
            data.put("id", notification.getId());
            data.put("type", notification.getType());
            data.put("userId", user.getId());
            data.put("timestamp", System.currentTimeMillis());
            
            // Add notification-specific data
            if (notification.getAttributes() != null) {
                data.put("attributes", notification.getAttributes());
            }
            
            // Add event data if available
            if (notification.getEventId() > 0) {
                Map<String, Object> eventData = new HashMap<>();
                eventData.put("id", notification.getEventId());
                eventData.put("type", notification.getType());
                data.put("event", eventData);
            }
            
            // Add position data if available
            if (notification.getPositionId() > 0) {
                Map<String, Object> positionData = new HashMap<>();
                positionData.put("id", notification.getPositionId());
                data.put("position", positionData);
            }
            
            // Add device data if available
            if (notification.getDeviceId() > 0) {
                Map<String, Object> deviceData = new HashMap<>();
                deviceData.put("id", notification.getDeviceId());
                data.put("device", deviceData);
            }
            
            // Add formatted message if available
            if (notification.getMessage() != null && !notification.getMessage().isEmpty()) {
                data.put("message", notification.getMessage());
            } else if (notification.getType() != null) {
                // Generate a default message based on notification type
                data.put("message", "New " + notification.getType() + " notification");
            }
            
            return data;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Fallback method for web notification formatting when the circuit breaker is open.
     * Provides a minimal notification object to ensure notifications can still be sent.
     *
     * @param notification The notification to format
     * @param user The user receiving the notification
     * @param e The exception that triggered the fallback
     * @return A map containing minimal notification data
     */
    public Map<String, Object> formatForWebFallback(Notification notification, User user, Exception e) {
        LOGGER.warn("Using fallback web notification formatting due to error: {}", e.getMessage());
        
        Map<String, Object> fallbackData = new HashMap<>();
        fallbackData.put("id", notification.getId());
        fallbackData.put("type", notification.getType() != null ? notification.getType() : "unknown");
        fallbackData.put("userId", user.getId());
        fallbackData.put("timestamp", System.currentTimeMillis());
        fallbackData.put("message", "Notification received");
        fallbackData.put("fallback", true);
        
        return fallbackData;
    }
}