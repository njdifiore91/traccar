/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Collects and exposes metrics related to notification operations for monitoring and observability.
 * Tracks notification counts, delivery success/failure rates, latency, and external service health.
 * Integrates with OpenTelemetry for standardized metrics collection across the distributed system.
 */
@Singleton
public class NotificationMetrics {

    private final MeterRegistry registry;
    
    // Counters for notification attempts by type
    private final Map<String, Counter> notificationAttemptCounters = new ConcurrentHashMap<>();
    
    // Counters for notification successes by type
    private final Map<String, Counter> notificationSuccessCounters = new ConcurrentHashMap<>();
    
    // Counters for notification failures by type
    private final Map<String, Counter> notificationFailureCounters = new ConcurrentHashMap<>();
    
    // Timers for notification processing duration by type
    private final Map<String, Timer> notificationProcessingTimers = new ConcurrentHashMap<>();
    
    // Gauges for external service health status
    private final Map<String, Integer> serviceHealthStatus = new ConcurrentHashMap<>();

    /**
     * Constructs a new NotificationMetrics instance.
     *
     * @param registry the Micrometer registry for metrics collection
     */
    @Inject
    public NotificationMetrics(MeterRegistry registry) {
        this.registry = registry;
        
        // Initialize health status for known external services
        serviceHealthStatus.put("email", 1);
        serviceHealthStatus.put("sms", 1);
        serviceHealthStatus.put("firebase", 1);
        serviceHealthStatus.put("telegram", 1);
        serviceHealthStatus.put("pushover", 1);
        serviceHealthStatus.put("traccar", 1);
        
        // Register gauges for each service
        registerServiceHealthGauges();
    }
    
    /**
     * Registers health status gauges for all notification services.
     */
    private void registerServiceHealthGauges() {
        for (String service : serviceHealthStatus.keySet()) {
            Gauge.builder("notification_service_health", serviceHealthStatus, map -> map.get(service))
                .tags(Tags.of(Tag.of("service", "notification"), Tag.of("channel", service)))
                .description("Health status of notification service (1=healthy, 0=unhealthy)")
                .register(registry);
        }
    }

    /**
     * Records a notification attempt.
     *
     * @param type the notification type (e.g., "email", "sms", "push")
     */
    public void recordNotificationAttempt(String type) {
        getOrCreateAttemptCounter(type).increment();
    }

    /**
     * Records a successful notification delivery.
     *
     * @param type the notification type (e.g., "email", "sms", "push")
     */
    public void recordNotificationSuccess(String type) {
        getOrCreateSuccessCounter(type).increment();
    }

    /**
     * Records a failed notification delivery.
     *
     * @param type the notification type (e.g., "email", "sms", "push")
     * @param errorType the type of error that occurred
     */
    public void recordNotificationFailure(String type, String errorType) {
        getOrCreateFailureCounter(type, errorType).increment();
    }

    /**
     * Records the time taken to process and deliver a notification.
     *
     * @param type the notification type (e.g., "email", "sms", "push")
     * @param timeMs the processing time in milliseconds
     */
    public void recordNotificationProcessingTime(String type, long timeMs) {
        getOrCreateProcessingTimer(type).record(timeMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Updates the health status of an external notification service.
     *
     * @param service the service name
     * @param isHealthy true if the service is healthy, false otherwise
     */
    public void updateServiceHealth(String service, boolean isHealthy) {
        serviceHealthStatus.put(service, isHealthy ? 1 : 0);
    }

    /**
     * Gets or creates a counter for notification attempts.
     *
     * @param type the notification type
     * @return the counter for the specified type
     */
    private Counter getOrCreateAttemptCounter(String type) {
        return notificationAttemptCounters.computeIfAbsent(type, t -> 
            Counter.builder("notification_attempts_total")
                .tags(Tags.of(Tag.of("service", "notification"), Tag.of("type", t)))
                .description("Total number of notification delivery attempts")
                .register(registry));
    }

    /**
     * Gets or creates a counter for successful notifications.
     *
     * @param type the notification type
     * @return the counter for the specified type
     */
    private Counter getOrCreateSuccessCounter(String type) {
        return notificationSuccessCounters.computeIfAbsent(type, t -> 
            Counter.builder("notification_success_total")
                .tags(Tags.of(Tag.of("service", "notification"), Tag.of("type", t)))
                .description("Total number of successfully delivered notifications")
                .register(registry));
    }

    /**
     * Gets or creates a counter for failed notifications.
     *
     * @param type the notification type
     * @param errorType the type of error that occurred
     * @return the counter for the specified type and error
     */
    private Counter getOrCreateFailureCounter(String type, String errorType) {
        String key = type + "-" + errorType;
        return notificationFailureCounters.computeIfAbsent(key, k -> 
            Counter.builder("notification_failure_total")
                .tags(Tags.of(
                    Tag.of("service", "notification"), 
                    Tag.of("type", type),
                    Tag.of("error", errorType)))
                .description("Total number of failed notification deliveries")
                .register(registry));
    }

    /**
     * Gets or creates a timer for notification processing.
     *
     * @param type the notification type
     * @return the timer for the specified type
     */
    private Timer getOrCreateProcessingTimer(String type) {
        return notificationProcessingTimers.computeIfAbsent(type, t -> 
            Timer.builder("notification_processing_duration_seconds")
                .tags(Tags.of(Tag.of("service", "notification"), Tag.of("type", t)))
                .description("Time taken to process and deliver notifications")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry));
    }
}