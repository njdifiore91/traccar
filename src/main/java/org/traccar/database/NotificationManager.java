/*
 * Copyright 2016 - 2022 Anton Tananaev (anton@traccar.org)
 * Copyright 2016 - 2018 Andrey Kunitsyn (andrey@traccar.org)
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
package org.traccar.database;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.forward.EventData;
import org.traccar.forward.EventForwarder;
import org.traccar.geocoder.Geocoder;
import org.traccar.helper.DateUtil;
import org.traccar.messaging.NotificationProducer;
import org.traccar.model.Calendar;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Geofence;
import org.traccar.model.Maintenance;
import org.traccar.model.Position;
import org.traccar.notification.MessageException;
import org.traccar.notification.NotificatorManager;
import org.traccar.session.cache.CacheManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Request;

import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

@Singleton
public class NotificationManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationManager.class);

    private final Storage storage;
    private final CacheManager cacheManager;
    private final EventForwarder eventForwarder;
    private final NotificatorManager notificatorManager;
    private final Geocoder geocoder;
    private final NotificationProducer notificationProducer;
    private final ServiceDiscovery serviceDiscovery;

    private final boolean geocodeOnRequest;
    private final long timeThreshold;
    private final Set<Long> blockedUsers = new HashSet<>();

    // OpenTelemetry components
    private final Tracer tracer;
    private final Meter meter;
    private final LongCounter notificationAttemptCounter;
    private final LongCounter notificationSuccessCounter;
    private final LongCounter notificationFailureCounter;

    // Resilience4j components
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;

    @Inject
    public NotificationManager(
            Config config, Storage storage, CacheManager cacheManager, @Nullable EventForwarder eventForwarder,
            NotificatorManager notificatorManager, @Nullable Geocoder geocoder,
            @Nullable NotificationProducer notificationProducer, @Nullable ServiceDiscovery serviceDiscovery) {
        this.storage = storage;
        this.cacheManager = cacheManager;
        this.eventForwarder = eventForwarder;
        this.notificatorManager = notificatorManager;
        this.geocoder = geocoder;
        this.notificationProducer = notificationProducer;
        this.serviceDiscovery = serviceDiscovery;
        geocodeOnRequest = config.getBoolean(Keys.GEOCODER_ON_REQUEST);
        timeThreshold = config.getLong(Keys.NOTIFICATOR_TIME_THRESHOLD);
        String blockedUsersString = config.getString(Keys.NOTIFICATION_BLOCK_USERS);
        if (blockedUsersString != null) {
            for (String userIdString : blockedUsersString.split(",")) {
                blockedUsers.add(Long.parseLong(userIdString));
            }
        }

        // Initialize OpenTelemetry
        tracer = GlobalOpenTelemetry.getTracer("org.traccar.notification");
        meter = GlobalOpenTelemetry.getMeter("org.traccar.notification");
        
        // Create metrics
        notificationAttemptCounter = meter.counterBuilder("notification.attempts")
                .setDescription("Total number of notification delivery attempts")
                .build();
        
        notificationSuccessCounter = meter.counterBuilder("notification.success")
                .setDescription("Number of successful notification deliveries")
                .build();
        
        notificationFailureCounter = meter.counterBuilder("notification.failures")
                .setDescription("Number of failed notification deliveries")
                .build();

        // Configure Circuit Breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to open circuit
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds in OPEN state
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10) // Count-based sliding window with 10 calls
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in HALF_OPEN state
                .recordExceptions(MessageException.class, TimeoutException.class)
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        circuitBreaker = circuitBreakerRegistry.circuitBreaker("notificationDelivery");

        // Configure Retry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Maximum 3 attempts
                .waitDuration(Duration.ofMillis(1000)) // Initial wait duration of 1 second
                .retryExceptions(MessageException.class, TimeoutException.class)
                .intervalFunction(interval -> interval * 2) // Exponential backoff
                .build();

        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        retry = retryRegistry.retry("notificationDelivery");

        // Register event listeners for circuit breaker state transitions
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker state changed from {} to {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));
    }

    private void updateEvent(Event event, Position position) {
        // Create a span for event processing
        Span span = tracer.spanBuilder("notification.process_event")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("event.type", event.getType())
                .setAttribute("event.deviceId", event.getDeviceId())
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            try {
                event.setId(storage.addObject(event, new Request(new Columns.Exclude("id"))));
                span.addEvent("Event saved to storage");
            } catch (StorageException error) {
                LOGGER.warn("Event save error", error);
                span.setStatus(StatusCode.ERROR, "Failed to save event");
                span.recordException(error);
            }

            forwardEvent(event, position);

            if (System.currentTimeMillis() - event.getEventTime().getTime() > timeThreshold) {
                LOGGER.info("Skipping notifications for old event");
                span.addEvent("Skipped notifications for old event");
                return;
            }

            var notifications = cacheManager.getDeviceNotifications(event.getDeviceId()).stream()
                    .filter(notification -> notification.getType().equals(event.getType()))
                    .filter(notification -> {
                        if (event.getType().equals(Event.TYPE_ALARM)) {
                            String alarmsAttribute = notification.getString("alarms");
                            if (alarmsAttribute != null) {
                                return Arrays.asList(alarmsAttribute.split(","))
                                        .contains(event.getString(Position.KEY_ALARM));
                            }
                            return false;
                        }
                        return true;
                    })
                    .filter(notification -> {
                        long calendarId = notification.getCalendarId();
                        Calendar calendar = calendarId != 0 ? cacheManager.getObject(Calendar.class, calendarId) : null;
                        return calendar == null || calendar.checkMoment(event.getEventTime());
                    })
                    .toList();

            Device device = cacheManager.getObject(Device.class, event.getDeviceId());
            LOGGER.info(
                    "Event id: {}, time: {}, type: {}, notifications: {}",
                    device.getUniqueId(),
                    DateUtil.formatDate(event.getEventTime(), false),
                    event.getType(),
                    notifications.size());

            span.setAttribute("device.id", device.getUniqueId());
            span.setAttribute("notifications.count", notifications.size());

            if (!notifications.isEmpty()) {
                if (position != null && position.getAddress() == null && geocodeOnRequest && geocoder != null) {
                    position.setAddress(geocoder.getAddress(position.getLatitude(), position.getLongitude(), null));
                    span.addEvent("Address geocoded");
                }

                // Process notifications
                processNotifications(notifications, event, position, device, span);
            }
        } finally {
            span.end();
        }
    }

    private void processNotifications(java.util.List<org.traccar.model.Notification> notifications, 
                                     Event event, Position position, Device device, Span parentSpan) {
        // If notification service is available, send via message broker
        if (notificationProducer != null && serviceDiscovery != null) {
            Span span = tracer.spanBuilder("notification.send_to_service")
                    .setParent(Context.current().with(parentSpan))
                    .setSpanKind(SpanKind.PRODUCER)
                    .setAttribute("notifications.count", notifications.size())
                    .startSpan();
            
            try (Scope scope = span.makeCurrent()) {
                // Find notification service endpoint
                String serviceEndpoint = serviceDiscovery.getServiceEndpoint("notification-service");
                if (serviceEndpoint != null) {
                    span.setAttribute("service.endpoint", serviceEndpoint);
                    
                    // Create notification data to send to the service
                    Map<String, Object> notificationData = Map.of(
                            "event", event,
                            "position", position,
                            "device", device,
                            "notifications", notifications
                    );
                    
                    // Send to message broker
                    notificationProducer.sendNotification(notificationData, (success, throwable) -> {
                        if (success) {
                            span.addEvent("Notification data sent to broker");
                            notificationSuccessCounter.add(1, 
                                    Attributes.of(AttributeKey.stringKey("delivery.method"), "broker"));
                        } else {
                            span.setStatus(StatusCode.ERROR, "Failed to send notification data to broker");
                            span.recordException(throwable);
                            notificationFailureCounter.add(1, 
                                    Attributes.of(AttributeKey.stringKey("delivery.method"), "broker"));
                            
                            // Fallback to direct delivery if broker fails
                            LOGGER.warn("Broker delivery failed, falling back to direct delivery", throwable);
                            deliverNotificationsDirectly(notifications, event, position, parentSpan);
                        }
                    });
                } else {
                    span.setStatus(StatusCode.ERROR, "Notification service not found");
                    LOGGER.warn("Notification service not found, falling back to direct delivery");
                    // Fallback to direct delivery if service discovery fails
                    deliverNotificationsDirectly(notifications, event, position, parentSpan);
                }
            } finally {
                span.end();
            }
        } else {
            // Fallback to direct delivery if broker or service discovery is not available
            deliverNotificationsDirectly(notifications, event, position, parentSpan);
        }
    }

    private void deliverNotificationsDirectly(java.util.List<org.traccar.model.Notification> notifications, 
                                            Event event, Position position, Span parentSpan) {
        Span span = tracer.spanBuilder("notification.deliver_directly")
                .setParent(Context.current().with(parentSpan))
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("notifications.count", notifications.size())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            notifications.forEach(notification -> {
                cacheManager.getNotificationUsers(notification.getId(), event.getDeviceId()).forEach(user -> {
                    if (blockedUsers.contains(user.getId())) {
                        LOGGER.info("User {} notification blocked", user.getId());
                        span.addEvent("User notification blocked", 
                                Attributes.of(AttributeKey.longKey("user.id"), user.getId()));
                        return;
                    }
                    
                    for (String notificator : notification.getNotificatorsTypes()) {
                        notificationAttemptCounter.add(1, Attributes.of(
                                AttributeKey.stringKey("notificator"), notificator,
                                AttributeKey.longKey("user.id"), user.getId()
                        ));
                        
                        // Create a resilient function for notification delivery with circuit breaker and retry
                        Supplier<Void> notificationFunction = () -> {
                            try {
                                notificatorManager.getNotificator(notificator).send(notification, user, event, position);
                                notificationSuccessCounter.add(1, Attributes.of(
                                        AttributeKey.stringKey("notificator"), notificator,
                                        AttributeKey.longKey("user.id"), user.getId()
                                ));
                                return null;
                            } catch (MessageException exception) {
                                notificationFailureCounter.add(1, Attributes.of(
                                        AttributeKey.stringKey("notificator"), notificator,
                                        AttributeKey.longKey("user.id"), user.getId(),
                                        AttributeKey.stringKey("error.type"), exception.getClass().getSimpleName()
                                ));
                                LOGGER.warn("Notification failed", exception);
                                span.recordException(exception);
                                throw new RuntimeException(exception);
                            }
                        };
                        
                        try {
                            // Apply circuit breaker and retry patterns
                            Supplier<Void> decoratedSupplier = Retry.decorateSupplier(retry, 
                                    CircuitBreaker.decorateSupplier(circuitBreaker, notificationFunction));
                            
                            // Execute with resilience patterns
                            decoratedSupplier.get();
                            
                        } catch (Exception e) {
                            LOGGER.error("Failed to deliver notification after retries", e);
                            span.setStatus(StatusCode.ERROR, "Failed to deliver notification after retries");
                            span.recordException(e);
                        }
                    }
                });
            });
        } finally {
            span.end();
        }
    }

    private void forwardEvent(Event event, Position position) {
        if (eventForwarder != null) {
            Span span = tracer.spanBuilder("notification.forward_event")
                    .setSpanKind(SpanKind.CLIENT)
                    .setAttribute("event.type", event.getType())
                    .setAttribute("event.deviceId", event.getDeviceId())
                    .startSpan();
            
            try (Scope scope = span.makeCurrent()) {
                EventData eventData = new EventData();
                eventData.setEvent(event);
                eventData.setPosition(position);
                eventData.setDevice(cacheManager.getObject(Device.class, event.getDeviceId()));
                if (event.getGeofenceId() != 0) {
                    eventData.setGeofence(cacheManager.getObject(Geofence.class, event.getGeofenceId()));
                }
                if (event.getMaintenanceId() != 0) {
                    eventData.setMaintenance(cacheManager.getObject(Maintenance.class, event.getMaintenanceId()));
                }
                
                eventForwarder.forward(eventData, (success, throwable) -> {
                    if (!success) {
                        LOGGER.warn("Event forwarding failed", throwable);
                        span.setStatus(StatusCode.ERROR, "Event forwarding failed");
                        span.recordException(throwable);
                    } else {
                        span.addEvent("Event forwarded successfully");
                    }
                });
            } finally {
                span.end();
            }
        }
    }

    public void updateEvents(Map<Event, Position> events) {
        Span parentSpan = tracer.spanBuilder("notification.update_events")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("events.count", events.size())
                .startSpan();
        
        try (Scope scope = parentSpan.makeCurrent()) {
            for (Entry<Event, Position> entry : events.entrySet()) {
                Event event = entry.getKey();
                Position position = entry.getValue();
                var key = new Object();
                try {
                    cacheManager.addDevice(event.getDeviceId(), key);
                    updateEvent(event, position);
                } catch (Exception e) {
                    parentSpan.recordException(e);
                    throw new RuntimeException(e);
                } finally {
                    cacheManager.removeDevice(event.getDeviceId(), key);
                }
            }
        } finally {
            parentSpan.end();
        }
    }
}