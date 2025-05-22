/*
 * Copyright 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.push;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Collects and exposes metrics for push notification operations using Prometheus/OpenTelemetry.
 * This class tracks key performance indicators such as notification send rates, success/failure counts,
 * and latency distributions. It exposes these metrics through a standardized /metrics endpoint for
 * scraping by Prometheus.
 */
@Singleton
public class MetricsCollector {

    private final MeterRegistry registry;
    
    // Counters for tracking notification events
    private final Counter notificationsSentTotal;
    private final Counter notificationsFailedTotal;
    private final Counter tokenValidationFailedTotal;
    private final Counter tokenRefreshTotal;
    
    // Timer for measuring notification latency
    private final Timer notificationDuration;
    
    // Gauge for tracking Firebase connection status
    private final AtomicInteger firebaseConnectionStatus;
    private final AtomicInteger activeDevicesWithTokens;

    /**
     * Creates a new metrics collector with the provided meter registry.
     *
     * @param registry the Micrometer registry for recording metrics
     */
    @Inject
    public MetricsCollector(MeterRegistry registry) {
        this.registry = registry;
        
        // Initialize counters
        this.notificationsSentTotal = Counter.builder("push_notifications_sent_total")
                .description("Total number of push notifications successfully sent")
                .register(registry);
                
        this.notificationsFailedTotal = Counter.builder("push_notifications_failed_total")
                .description("Total number of push notifications that failed to send")
                .register(registry);
                
        this.tokenValidationFailedTotal = Counter.builder("push_token_validation_failed_total")
                .description("Total number of push notification token validations that failed")
                .register(registry);
                
        this.tokenRefreshTotal = Counter.builder("push_token_refresh_total")
                .description("Total number of push notification token refreshes")
                .register(registry);
        
        // Initialize timer for latency measurements
        this.notificationDuration = Timer.builder("push_notification_duration_seconds")
                .description("Push notification sending duration in seconds")
                .publishPercentiles(0.5, 0.95, 0.99) // Publish 50th, 95th, and 99th percentiles
                .publishPercentileHistogram()
                .register(registry);
        
        // Initialize gauge for Firebase connection status
        this.firebaseConnectionStatus = new AtomicInteger(1); // 1 = connected, 0 = disconnected
        Gauge.builder("push_firebase_connection_status", firebaseConnectionStatus::get)
                .description("Firebase connection status (1 = connected, 0 = disconnected)")
                .register(registry);
                
        // Initialize gauge for tracking active devices with notification tokens
        this.activeDevicesWithTokens = new AtomicInteger(0);
        Gauge.builder("push_active_devices_with_tokens", activeDevicesWithTokens::get)
                .description("Number of active devices with valid notification tokens")
                .register(registry);
    }
    
    /**
     * Records a successful push notification send operation.
     */
    public void recordNotificationSent() {
        notificationsSentTotal.increment();
    }
    
    /**
     * Records a failed push notification send operation.
     */
    public void recordNotificationFailed() {
        notificationsFailedTotal.increment();
    }
    
    /**
     * Records a successful push notification send operation with device information.
     * 
     * @param deviceId the ID of the device receiving the notification
     */
    public void recordNotificationSent(long deviceId) {
        Counter.builder("push_notifications_sent_total")
                .description("Total number of push notifications successfully sent")
                .tags(Tags.of(Tag.of("deviceId", String.valueOf(deviceId))))
                .register(registry)
                .increment();
        notificationsSentTotal.increment();
    }
    
    /**
     * Records a failed push notification send operation with device information.
     * 
     * @param deviceId the ID of the device that failed to receive the notification
     */
    public void recordNotificationFailed(long deviceId) {
        Counter.builder("push_notifications_failed_total")
                .description("Total number of push notifications that failed to send")
                .tags(Tags.of(Tag.of("deviceId", String.valueOf(deviceId))))
                .register(registry)
                .increment();
        notificationsFailedTotal.increment();
    }
    
    /**
     * Records the duration of a push notification operation.
     * 
     * @param durationMillis the duration of the operation in milliseconds
     */
    public void recordNotificationDuration(long durationMillis) {
        notificationDuration.record(durationMillis, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Records the duration of a push notification operation with device information.
     * 
     * @param deviceId the ID of the device receiving the notification
     * @param durationMillis the duration of the operation in milliseconds
     */
    public void recordNotificationDuration(long deviceId, long durationMillis) {
        Timer.builder("push_notification_duration_seconds")
                .description("Push notification sending duration in seconds")
                .tags(Tags.of(Tag.of("deviceId", String.valueOf(deviceId))))
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry)
                .record(durationMillis, TimeUnit.MILLISECONDS);
        notificationDuration.record(durationMillis, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Updates the Firebase connection status.
     * 
     * @param connected true if connected, false otherwise
     */
    public void setFirebaseConnectionStatus(boolean connected) {
        firebaseConnectionStatus.set(connected ? 1 : 0);
    }
    
    /**
     * Records metrics for a push notification operation in a single call.
     * 
     * @param deviceId the ID of the device receiving the notification
     * @param success whether the notification was sent successfully
     * @param durationMillis the duration of the operation in milliseconds
     */
    public void recordPushOperation(long deviceId, boolean success, long durationMillis) {
        if (success) {
            recordNotificationSent(deviceId);
        } else {
            recordNotificationFailed(deviceId);
        }
        recordNotificationDuration(deviceId, durationMillis);
    }
    
    /**
     * Records metrics for a batch push notification operation.
     * 
     * @param successCount number of successful notifications
     * @param failureCount number of failed notifications
     * @param durationMillis the duration of the operation in milliseconds
     */
    public void recordBatchPushOperation(int successCount, int failureCount, long durationMillis) {
        // Record successful notifications
        if (successCount > 0) {
            Counter.builder("push_notifications_batch_sent_total")
                    .description("Total number of push notifications successfully sent in batch operations")
                    .register(registry)
                    .increment(successCount);
            notificationsSentTotal.increment(successCount);
        }
        
        // Record failed notifications
        if (failureCount > 0) {
            Counter.builder("push_notifications_batch_failed_total")
                    .description("Total number of push notifications that failed to send in batch operations")
                    .register(registry)
                    .increment(failureCount);
            notificationsFailedTotal.increment(failureCount);
        }
        
        // Record batch operation duration
        Timer.builder("push_notification_batch_duration_seconds")
                .description("Push notification batch sending duration in seconds")
                .tags(Tags.of(
                        Tag.of("batchSize", String.valueOf(successCount + failureCount)),
                        Tag.of("successRate", String.format("%.2f", (double) successCount / (successCount + failureCount))))
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry)
                .record(durationMillis, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Records metrics for Firebase multicast message operations.
     * 
     * @param tokenCount number of tokens in the multicast message
     * @param successCount number of successful deliveries
     * @param failureCount number of failed deliveries
     * @param durationMillis the duration of the operation in milliseconds
     * @param commandType the type of command being sent (optional)
     */
    public void recordFirebaseMulticastOperation(int tokenCount, int successCount, int failureCount, 
                                               long durationMillis, String commandType) {
        Tags tags = Tags.of(
                Tag.of("tokenCount", String.valueOf(tokenCount)),
                Tag.of("commandType", commandType != null ? commandType : "unknown"));
                
        // Record successful notifications
        if (successCount > 0) {
            Counter.builder("push_firebase_multicast_sent_total")
                    .description("Total number of Firebase multicast messages successfully sent")
                    .tags(tags)
                    .register(registry)
                    .increment(successCount);
            notificationsSentTotal.increment(successCount);
        }
        
        // Record failed notifications
        if (failureCount > 0) {
            Counter.builder("push_firebase_multicast_failed_total")
                    .description("Total number of Firebase multicast messages that failed to send")
                    .tags(tags)
                    .register(registry)
                    .increment(failureCount);
            notificationsFailedTotal.increment(failureCount);
        }
        
        // Record multicast operation duration
        Timer.builder("push_firebase_multicast_duration_seconds")
                .description("Firebase multicast message sending duration in seconds")
                .tags(tags)
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry)
                .record(durationMillis, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Records a token validation failure.
     * 
     * @param deviceId the ID of the device with the invalid token
     * @param reason the reason for validation failure
     */
    public void recordTokenValidationFailed(long deviceId, String reason) {
        Counter.builder("push_token_validation_failed_total")
                .description("Total number of push notification token validations that failed")
                .tags(Tags.of(
                        Tag.of("deviceId", String.valueOf(deviceId)),
                        Tag.of("reason", reason)))
                .register(registry)
                .increment();
        tokenValidationFailedTotal.increment();
    }
    
    /**
     * Records a token refresh operation.
     * 
     * @param deviceId the ID of the device with the refreshed token
     */
    public void recordTokenRefresh(long deviceId) {
        Counter.builder("push_token_refresh_total")
                .description("Total number of push notification token refreshes")
                .tags(Tags.of(Tag.of("deviceId", String.valueOf(deviceId))))
                .register(registry)
                .increment();
        tokenRefreshTotal.increment();
    }
    
    /**
     * Updates the count of active devices with notification tokens.
     * 
     * @param count the current count of active devices with tokens
     */
    public void setActiveDevicesWithTokens(int count) {
        activeDevicesWithTokens.set(count);
    }
    
    /**
     * Increments the count of active devices with notification tokens.
     */
    public void incrementActiveDevicesWithTokens() {
        activeDevicesWithTokens.incrementAndGet();
    }
    
    /**
     * Decrements the count of active devices with notification tokens.
     */
    public void decrementActiveDevicesWithTokens() {
        activeDevicesWithTokens.decrementAndGet();
    }
    
    /**
     * Records a Firebase error by error type.
     * 
     * @param errorType the type of Firebase error
     */
    public void recordFirebaseError(String errorType) {
        Counter.builder("push_firebase_errors_total")
                .description("Total number of Firebase errors by type")
                .tags(Tags.of(Tag.of("errorType", errorType)))
                .register(registry)
                .increment();
    }
    
    /**
     * Records Firebase API quota metrics.
     * 
     * @param quotaType the type of quota (daily, minute, etc.)
     * @param currentUsage the current usage amount
     * @param limit the quota limit
     */
    public void recordFirebaseQuota(String quotaType, long currentUsage, long limit) {
        Tags tags = Tags.of(Tag.of("quotaType", quotaType));
        
        // Record current usage
        Gauge.builder("push_firebase_quota_usage", () -> currentUsage)
                .description("Current Firebase API quota usage")
                .tags(tags)
                .register(registry);
                
        // Record quota limit
        Gauge.builder("push_firebase_quota_limit", () -> limit)
                .description("Firebase API quota limit")
                .tags(tags)
                .register(registry);
                
        // Record usage percentage
        double percentage = limit > 0 ? (double) currentUsage / limit * 100 : 0;
        Gauge.builder("push_firebase_quota_usage_percent", () -> percentage)
                .description("Firebase API quota usage percentage")
                .tags(tags)
                .register(registry);
    }
    
    /**
     * Creates a timer for measuring the duration of a push notification operation.
     * This method returns a Timer.Sample that should be stopped using stopTimer().
     * 
     * @return a Timer.Sample for measuring operation duration
     */
    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }
    
    /**
     * Stops a timer started with startTimer() and records the duration.
     * 
     * @param sample the Timer.Sample returned by startTimer()
     * @param success whether the operation was successful
     * @param deviceId the ID of the device (optional)
     * @param commandType the type of command (optional)
     */
    public void stopTimer(Timer.Sample sample, boolean success, Long deviceId, String commandType) {
        Tags.Builder tagsBuilder = Tags.empty().and(Tag.of("success", String.valueOf(success)));
        
        if (deviceId != null) {
            tagsBuilder = tagsBuilder.and(Tag.of("deviceId", deviceId.toString()));
        }
        
        if (commandType != null) {
            tagsBuilder = tagsBuilder.and(Tag.of("commandType", commandType));
        }
        
        Timer timer = Timer.builder("push_operation_duration_seconds")
                .description("Duration of push notification operations")
                .tags(tagsBuilder.build())
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
                
        sample.stop(timer);
    }
}