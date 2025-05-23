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
package org.traccar.messaging;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.time.Duration;

/**
 * Provides metrics collection for messaging operations using Micrometer.
 * Tracks message counts, sizes, latencies, and error rates for monitoring and alerting.
 * 
 * This class implements the following metrics:
 * - Counters: Track the number of messages published and errors encountered
 * - Timers: Measure the latency of message publishing operations
 * - Gauges: Monitor the connection status to the message broker
 * - Distribution Summaries: Analyze the size distribution of published messages
 *
 * All metrics are tagged with relevant dimensions (e.g., topic) to enable detailed analysis
 * and are exposed in Prometheus-compatible format via the /metrics endpoint.
 */
@Singleton
public class MessagingMetrics {

    private final MeterRegistry registry;
    
    // Connection status tracking
    private final AtomicInteger brokerConnectionStatus = new AtomicInteger(0);
    
    // Counters for message operations
    private final Counter messagesPublishedTotal;
    private final Counter messagesPublishErrorsTotal;
    private final ConcurrentMap<String, Counter> topicMessagesPublishedTotal = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> topicMessagesPublishErrorsTotal = new ConcurrentHashMap<>();
    
    // Timers for latency tracking
    private final Timer messagePublishTimer;
    private final ConcurrentMap<String, Timer> topicMessagePublishTimer = new ConcurrentHashMap<>();
    
    // Distribution summaries for message sizes
    private final DistributionSummary messageSize;
    private final ConcurrentMap<String, DistributionSummary> topicMessageSize = new ConcurrentHashMap<>();
    
    // Metric name constants
    private static final String METRIC_BROKER_CONNECTED = "messaging_broker_connected";
    private static final String METRIC_MESSAGES_PUBLISHED = "messaging_messages_published_total";
    private static final String METRIC_PUBLISH_ERRORS = "messaging_messages_publish_errors_total";
    private static final String METRIC_PUBLISH_DURATION = "messaging_publish_duration_seconds";
    private static final String METRIC_MESSAGE_SIZE = "messaging_message_size_bytes";
    
    // Tag name constants
    private static final String TAG_TOPIC = "topic";
    private static final String TAG_ERROR_TYPE = "error_type";
    private static final String TAG_SERVICE = "service";
    private static final String TAG_SERVICE_VALUE = "protocol-service";

    /**
     * Creates a new MessagingMetrics instance.
     *
     * @param registry the Micrometer registry for metrics collection
     */
    @Inject
    public MessagingMetrics(MeterRegistry registry) {
        this.registry = registry;
        
        // Register connection status gauge
        Gauge.builder(METRIC_BROKER_CONNECTED, brokerConnectionStatus, AtomicInteger::get)
                .description("Indicates if the connection to the message broker is established")
                .tag(TAG_SERVICE, TAG_SERVICE_VALUE)
                .register(registry);
        
        // Initialize global counters
        messagesPublishedTotal = Counter.builder(METRIC_MESSAGES_PUBLISHED)
                .description("Total number of messages published to the broker")
                .tag(TAG_SERVICE, TAG_SERVICE_VALUE)
                .register(registry);
        
        messagesPublishErrorsTotal = Counter.builder(METRIC_PUBLISH_ERRORS)
                .description("Total number of message publish errors")
                .tag(TAG_SERVICE, TAG_SERVICE_VALUE)
                .register(registry);
        
        // Initialize global timer
        messagePublishTimer = Timer.builder(METRIC_PUBLISH_DURATION)
                .description("Time taken to publish messages to the broker")
                .publishPercentiles(0.5, 0.95, 0.99)
                .serviceLevelObjectives(
                    Duration.ofMillis(10),
                    Duration.ofMillis(50),
                    Duration.ofMillis(100),
                    Duration.ofMillis(200),
                    Duration.ofMillis(500))
                .tag(TAG_SERVICE, TAG_SERVICE_VALUE)
                .register(registry);
        
        // Initialize global message size summary
        messageSize = DistributionSummary.builder(METRIC_MESSAGE_SIZE)
                .description("Size of published messages in bytes")
                .baseUnit("bytes")
                .publishPercentiles(0.5, 0.95, 0.99)
                .tag(TAG_SERVICE, TAG_SERVICE_VALUE)
                .register(registry);
    }
    
    /**
     * Updates the broker connection status.
     *
     * @param connected true if connected, false otherwise
     */
    public void setBrokerConnected(boolean connected) {
        brokerConnectionStatus.set(connected ? 1 : 0);
    }
    
    /**
     * Records a successful message publish operation.
     *
     * @param topic the message topic
     */
    public void recordMessagePublished(String topic) {
        messagesPublishedTotal.increment();
        getOrCreateTopicPublishedCounter(topic).increment();
    }
    
    /**
     * Records a message publish error.
     *
     * @param topic the message topic
     * @param errorType the type of error that occurred
     */
    public void recordMessagePublishError(String topic, String errorType) {
        messagesPublishErrorsTotal.increment();
        getOrCreateTopicPublishErrorCounter(topic, errorType).increment();
    }
    
    /**
     * Records a batch of successful message publish operations.
     *
     * @param topic the message topic
     * @param count the number of messages published
     */
    public void recordMessagesPublished(String topic, long count) {
        messagesPublishedTotal.increment(count);
        getOrCreateTopicPublishedCounter(topic).increment(count);
    }
    
    /**
     * Records the time taken to publish a message.
     *
     * @param topic the message topic
     * @param supplier the operation to time
     * @return the result of the operation
     * @param <T> the return type of the operation
     */
    public <T> T recordPublishTime(String topic, Supplier<T> supplier) {
        return messagePublishTimer.record(() -> {
            Timer topicTimer = getOrCreateTopicPublishTimer(topic);
            return topicTimer.record(supplier);
        });
    }
    
    /**
     * Records the time taken to publish a message.
     *
     * @param topic the message topic
     * @param runnable the operation to time
     */
    public void recordPublishTime(String topic, Runnable runnable) {
        messagePublishTimer.record(() -> {
            Timer topicTimer = getOrCreateTopicPublishTimer(topic);
            topicTimer.record(runnable);
        });
    }
    
    /**
     * Records the size of a published message.
     *
     * @param topic the message topic
     * @param bytes the size of the message in bytes
     */
    public void recordMessageSize(String topic, long bytes) {
        messageSize.record(bytes);
        getOrCreateTopicMessageSize(topic).record(bytes);
    }
    
    /**
     * Gets or creates a counter for messages published to a specific topic.
     *
     * @param topic the message topic
     * @return the counter for the topic
     */
    private Counter getOrCreateTopicPublishedCounter(String topic) {
        return topicMessagesPublishedTotal.computeIfAbsent(topic, t -> {
            return Counter.builder(METRIC_MESSAGES_PUBLISHED)
                    .tags(Tags.of(
                            Tag.of(TAG_TOPIC, t),
                            Tag.of(TAG_SERVICE, TAG_SERVICE_VALUE)))
                    .description("Total number of messages published to the broker for a specific topic")
                    .register(registry);
        });
    }
    
    /**
     * Gets or creates a counter for message publish errors for a specific topic and error type.
     *
     * @param topic the message topic
     * @param errorType the type of error that occurred
     * @return the counter for the topic and error type
     */
    private Counter getOrCreateTopicPublishErrorCounter(String topic, String errorType) {
        String key = topic + "_" + errorType;
        return topicMessagesPublishErrorsTotal.computeIfAbsent(key, k -> {
            return Counter.builder(METRIC_PUBLISH_ERRORS)
                    .tags(Tags.of(
                            Tag.of(TAG_TOPIC, topic),
                            Tag.of(TAG_ERROR_TYPE, errorType),
                            Tag.of(TAG_SERVICE, TAG_SERVICE_VALUE)))
                    .description("Total number of message publish errors for a specific topic")
                    .register(registry);
        });
    }
    
    /**
     * Gets or creates a timer for message publish operations for a specific topic.
     *
     * @param topic the message topic
     * @return the timer for the topic
     */
    private Timer getOrCreateTopicPublishTimer(String topic) {
        return topicMessagePublishTimer.computeIfAbsent(topic, t -> {
            return Timer.builder(METRIC_PUBLISH_DURATION)
                    .tags(Tags.of(
                            Tag.of(TAG_TOPIC, t),
                            Tag.of(TAG_SERVICE, TAG_SERVICE_VALUE)))
                    .description("Time taken to publish messages to the broker for a specific topic")
                    .publishPercentiles(0.5, 0.95, 0.99)
                    .serviceLevelObjectives(
                        Duration.ofMillis(10),
                        Duration.ofMillis(50),
                        Duration.ofMillis(100),
                        Duration.ofMillis(200),
                        Duration.ofMillis(500))
                    .register(registry);
        });
    }
    
    /**
     * Gets or creates a distribution summary for message sizes for a specific topic.
     *
     * @param topic the message topic
     * @return the distribution summary for the topic
     */
    private DistributionSummary getOrCreateTopicMessageSize(String topic) {
        return topicMessageSize.computeIfAbsent(topic, t -> {
            return DistributionSummary.builder(METRIC_MESSAGE_SIZE)
                    .tags(Tags.of(
                            Tag.of(TAG_TOPIC, t),
                            Tag.of(TAG_SERVICE, TAG_SERVICE_VALUE)))
                    .description("Size of published messages in bytes for a specific topic")
                    .baseUnit("bytes")
                    .publishPercentiles(0.5, 0.95, 0.99)
                    .register(registry);
        });
    }
}