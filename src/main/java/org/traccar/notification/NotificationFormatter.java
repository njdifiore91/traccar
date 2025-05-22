/*
 * Copyright 2016 - 2022 Anton Tananaev (anton@traccar.org)
 * Copyright 2017 - 2018 Andrey Kunitsyn (andrey@traccar.org)
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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.velocity.VelocityContext;
import org.traccar.helper.model.UserUtil;
import org.traccar.messaging.MessageBrokerManager;
import org.traccar.model.Device;
import org.traccar.model.Driver;
import org.traccar.model.Event;
import org.traccar.model.Geofence;
import org.traccar.model.Maintenance;
import org.traccar.model.Notification;
import org.traccar.model.Position;
import org.traccar.model.Server;
import org.traccar.model.User;
import org.traccar.session.cache.CacheManager;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

/**
 * Formats notifications for delivery through various channels.
 * Integrates with MessageBrokerManager for asynchronous notification delivery
 * and includes distributed tracing context propagation.
 */
@Singleton
public class NotificationFormatter {

    private final CacheManager cacheManager;
    private final TextTemplateFormatter textTemplateFormatter;
    private final MessageBrokerManager messageBrokerManager;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;

    /**
     * Timer for measuring notification formatting performance
     */
    private final Timer formatTimer;

    /**
     * Creates a new NotificationFormatter with required dependencies.
     *
     * @param cacheManager Cache manager for retrieving related objects
     * @param textTemplateFormatter Template formatter for notification content
     * @param messageBrokerManager Message broker for asynchronous notification delivery
     * @param meterRegistry Metrics registry for performance monitoring
     * @param tracer OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public NotificationFormatter(
            CacheManager cacheManager, 
            TextTemplateFormatter textTemplateFormatter,
            MessageBrokerManager messageBrokerManager,
            MeterRegistry meterRegistry,
            Tracer tracer) {
        this.cacheManager = cacheManager;
        this.textTemplateFormatter = textTemplateFormatter;
        this.messageBrokerManager = messageBrokerManager;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        
        // Initialize performance metrics
        this.formatTimer = Timer.builder("notification.format.time")
                .description("Time taken to format notification messages")
                .register(meterRegistry);
        
        // Register counter for total notifications formatted
        meterRegistry.counter("notification.format.count", "type", "total");
    }

    /**
     * Formats a notification message with the given parameters.
     * Includes distributed tracing context and metrics collection.
     *
     * @param notification Notification configuration
     * @param user User receiving the notification
     * @param event Event triggering the notification
     * @param position Position associated with the event (may be null)
     * @param templatePath Path to the notification template
     * @return Formatted notification message with tracing context
     */
    public NotificationMessage formatMessage(
            Notification notification, User user, Event event, Position position, String templatePath) {
        
        // Create a span for the formatting operation
        Span span = tracer.spanBuilder("notification.format")
                .setAttribute("event.type", event.getType())
                .setAttribute("notification.type", notification.getType())
                .setAttribute("user.id", String.valueOf(user.getId()))
                .setAttribute("device.id", String.valueOf(event.getDeviceId()))
                .startSpan();
        
        // Use the timer to measure formatting performance
        return formatTimer.record(() -> {
            try (Scope scope = span.makeCurrent()) {
                // Increment the counter for this notification type
                meterRegistry.counter("notification.format.count", 
                        "type", event.getType(),
                        "user", String.valueOf(user.getId()))
                        .increment();
                
                Server server = cacheManager.getServer();
                Device device = cacheManager.getObject(Device.class, event.getDeviceId());

                VelocityContext velocityContext = textTemplateFormatter.prepareContext(server, user);

                velocityContext.put("notification", notification);
                velocityContext.put("device", device);
                velocityContext.put("event", event);
                if (position != null) {
                    velocityContext.put("position", position);
                    velocityContext.put("speedUnit", UserUtil.getSpeedUnit(server, user));
                    velocityContext.put("distanceUnit", UserUtil.getDistanceUnit(server, user));
                    velocityContext.put("volumeUnit", UserUtil.getVolumeUnit(server, user));
                }
                if (event.getGeofenceId() != 0) {
                    velocityContext.put("geofence", cacheManager.getObject(Geofence.class, event.getGeofenceId()));
                }
                if (event.getMaintenanceId() != 0) {
                    velocityContext.put("maintenance", cacheManager.getObject(Maintenance.class, event.getMaintenanceId()));
                }
                String driverUniqueId = event.getString(Position.KEY_DRIVER_UNIQUE_ID);
                if (driverUniqueId != null) {
                    velocityContext.put("driver", cacheManager.getDeviceObjects(device.getId(), Driver.class).stream()
                            .filter(driver -> driver.getUniqueId().equals(driverUniqueId)).findFirst().orElse(null));
                }

                // Format the message using the template formatter
                NotificationMessage message = textTemplateFormatter.formatMessage(velocityContext, event.getType(), templatePath);
                
                // Add tracing context to the message for correlation across services
                enrichMessageWithTracingContext(message, span.getSpanContext());
                
                // Mark the span as successful
                span.setStatus(StatusCode.OK);
                
                return message;
            } catch (Exception e) {
                // Record the error in the span
                span.recordException(e)
                     .setStatus(StatusCode.ERROR, e.getMessage());
                
                // Record metric for formatting errors
                meterRegistry.counter("notification.format.errors", 
                        "type", event.getType(),
                        "error", e.getClass().getSimpleName())
                        .increment();
                
                throw e;
            } finally {
                // End the span regardless of success or failure
                span.end();
            }
        });
    }
    
    /**
     * Enriches the notification message with tracing context for correlation across services.
     * 
     * @param message The notification message to enrich
     * @param spanContext The current span context to propagate
     */
    private void enrichMessageWithTracingContext(NotificationMessage message, SpanContext spanContext) {
        if (message == null || !spanContext.isValid()) {
            return;
        }
        
        // Create a map of tracing metadata
        Map<String, String> tracingContext = new HashMap<>();
        tracingContext.put("traceId", spanContext.getTraceId());
        tracingContext.put("spanId", spanContext.getSpanId());
        tracingContext.put("traceFlags", String.valueOf(spanContext.getTraceFlags().asHex()));
        
        // Set the correlation ID (trace ID) directly on the message for easy access
        message.setCorrelationId(spanContext.getTraceId());
        
        // Add the full tracing context to the message metadata
        message.setTracingContext(tracingContext);
    }
    
    /**
     * Formats and publishes a notification message asynchronously via the message broker.
     * 
     * @param notification Notification configuration
     * @param user User receiving the notification
     * @param event Event triggering the notification
     * @param position Position associated with the event (may be null)
     * @param templatePath Path to the notification template
     * @param topic The message broker topic to publish to
     */
    public void formatAndPublishMessage(
            Notification notification, User user, Event event, Position position, 
            String templatePath, String topic) {
        
        // Create a parent span for the async operation
        Span parentSpan = tracer.spanBuilder("notification.publish")
                .setAttribute("event.type", event.getType())
                .setAttribute("notification.type", notification.getType())
                .setAttribute("topic", topic)
                .startSpan();
        
        try (Scope scope = parentSpan.makeCurrent()) {
            // Format the message with the current tracing context
            NotificationMessage message = formatMessage(notification, user, event, position, templatePath);
            
            // Publish the message to the broker asynchronously
            messageBrokerManager.publishAsync(topic, message)
                .thenRun(() -> {
                    // Record successful publishing metric
                    meterRegistry.counter("notification.publish.success", 
                            "topic", topic,
                            "type", event.getType())
                            .increment();
                })
                .exceptionally(ex -> {
                    // Record failed publishing metric
                    meterRegistry.counter("notification.publish.errors", 
                            "topic", topic,
                            "type", event.getType(),
                            "error", ex.getClass().getSimpleName())
                            .increment();
                    return null;
                });
            
            parentSpan.setStatus(StatusCode.OK);
        } catch (Exception e) {
            parentSpan.recordException(e)
                     .setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            parentSpan.end();
        }
    }
}