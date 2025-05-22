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
package org.traccar;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.api.security.ServiceAccountUser;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides Micrometer-based metrics collection for monitoring service health and performance.
 * This component exposes standardized metrics for resource utilization, business-level indicators,
 * and service health, enabling comprehensive monitoring during the transition to microservices architecture.
 */
@Singleton
public class MicrometerMetricsManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MicrometerMetricsManager.class);

    private final Config config;
    private final MeterRegistry registry;

    private final Set<Long> users = new HashSet<>();
    private final Map<Long, String> deviceProtocols = new HashMap<>();
    private final Map<Long, Integer> deviceMessages = new HashMap<>();
    private final Map<String, Counter> protocolCounters = new ConcurrentHashMap<>();

    // Business metrics counters
    private final Counter requestsCounter;
    private final Counter messagesReceivedCounter;
    private final Counter messagesStoredCounter;
    private final Counter mailSentCounter;
    private final Counter smsSentCounter;
    private final Counter geocoderRequestsCounter;
    private final Counter geolocationRequestsCounter;

    // Gauges
    private final AtomicInteger activeUsersGauge;
    private final AtomicInteger activeDevicesGauge;

    /**
     * Initializes the metrics manager with the provided registry and configuration.
     * Sets up system metrics, JVM metrics, and business metrics.
     *
     * @param config The application configuration
     * @param registry The Micrometer meter registry
     */
    @Inject
    public MicrometerMetricsManager(Config config, MeterRegistry registry) {
        this.config = config;
        this.registry = registry;

        // Add common tags to all metrics
        configureCommonTags();

        // Register JVM and system metrics
        registerJvmMetrics();
        registerSystemMetrics();

        // Initialize business metrics
        requestsCounter = Counter.builder("traccar.requests.total")
                .description("Total number of API requests")
                .register(registry);

        messagesReceivedCounter = Counter.builder("traccar.messages.received.total")
                .description("Total number of messages received from devices")
                .register(registry);

        messagesStoredCounter = Counter.builder("traccar.messages.stored.total")
                .description("Total number of messages stored in the database")
                .register(registry);

        mailSentCounter = Counter.builder("traccar.mail.sent.total")
                .description("Total number of emails sent")
                .register(registry);

        smsSentCounter = Counter.builder("traccar.sms.sent.total")
                .description("Total number of SMS messages sent")
                .register(registry);

        geocoderRequestsCounter = Counter.builder("traccar.geocoder.requests.total")
                .description("Total number of geocoder requests")
                .register(registry);

        geolocationRequestsCounter = Counter.builder("traccar.geolocation.requests.total")
                .description("Total number of geolocation requests")
                .register(registry);

        // Initialize gauges
        activeUsersGauge = registry.gauge("traccar.users.active", 
                Tags.empty(), new AtomicInteger(0));
        
        activeDevicesGauge = registry.gauge("traccar.devices.active", 
                Tags.empty(), new AtomicInteger(0));

        LOGGER.info("Micrometer metrics collection initialized");
    }

    /**
     * Configures common tags to be applied to all metrics.
     * These tags help with filtering and grouping metrics in monitoring systems.
     */
    private void configureCommonTags() {
        String serviceName = config.getString(Keys.TELEMETRY_SERVICE_NAME, "traccar");
        String serviceVersion = getClass().getPackage().getImplementationVersion();
        if (serviceVersion == null) {
            serviceVersion = "unknown";
        }

        registry.config().commonTags(
                "service", serviceName,
                "version", serviceVersion);
    }

    /**
     * Registers JVM-related metrics including memory, garbage collection, threads, and class loading.
     */
    private void registerJvmMetrics() {
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ClassLoaderMetrics().bindTo(registry);
    }

    /**
     * Registers system-level metrics including CPU usage, file descriptors, and uptime.
     */
    private void registerSystemMetrics() {
        new ProcessorMetrics().bindTo(registry);
        new FileDescriptorMetrics().bindTo(registry);
        new UptimeMetrics().bindTo(registry);
    }

    /**
     * Registers a user request and increments the request counter.
     * Also tracks unique active users.
     *
     * @param userId The ID of the user making the request
     */
    public synchronized void registerRequest(long userId) {
        requestsCounter.increment();
        if (userId != 0 && userId != ServiceAccountUser.ID) {
            users.add(userId);
            activeUsersGauge.set(users.size());
        }
    }

    /**
     * Registers a received message and increments the message received counter.
     */
    public void registerMessageReceived() {
        messagesReceivedCounter.increment();
    }

    /**
     * Registers a stored message and increments the message stored counter.
     * Also tracks device protocols and message counts per device.
     *
     * @param deviceId The ID of the device sending the message
     * @param protocol The protocol used by the device
     */
    public synchronized void registerMessageStored(long deviceId, String protocol) {
        messagesStoredCounter.increment();
        
        // Track protocol-specific metrics
        Counter protocolCounter = protocolCounters.computeIfAbsent(protocol, p -> 
            Counter.builder("traccar.protocol.messages.total")
                .description("Total number of messages by protocol")
                .tag("protocol", p)
                .register(registry));
        protocolCounter.increment();
        
        if (deviceId != 0) {
            deviceProtocols.put(deviceId, protocol);
            deviceMessages.merge(deviceId, 1, Integer::sum);
            activeDevicesGauge.set(deviceProtocols.size());
        }
    }

    /**
     * Returns the total count of stored messages.
     *
     * @return The count of stored messages
     */
    public synchronized int messageStoredCount() {
        return deviceMessages.values().stream().mapToInt(Integer::intValue).sum();
    }

    /**
     * Returns the count of stored messages for a specific device.
     *
     * @param deviceId The ID of the device
     * @return The count of stored messages for the device
     */
    public synchronized int messageStoredCount(long deviceId) {
        return deviceMessages.getOrDefault(deviceId, 0);
    }

    /**
     * Registers a sent email and increments the mail sent counter.
     */
    public void registerMail() {
        mailSentCounter.increment();
    }

    /**
     * Registers a sent SMS and increments the SMS sent counter.
     */
    public void registerSms() {
        smsSentCounter.increment();
    }

    /**
     * Registers a geocoder request and increments the geocoder requests counter.
     */
    public void registerGeocoderRequest() {
        geocoderRequestsCounter.increment();
    }

    /**
     * Registers a geolocation request and increments the geolocation requests counter.
     */
    public void registerGeolocationRequest() {
        geolocationRequestsCounter.increment();
    }

    /**
     * Creates and registers a timer for measuring the execution time of operations.
     *
     * @param name The name of the timer
     * @param description The description of what the timer measures
     * @param tags Additional tags to associate with the timer
     * @return A Timer instance that can be used to record timing information
     */
    public Timer createTimer(String name, String description, Tag... tags) {
        return Timer.builder(name)
                .description(description)
                .tags(tags)
                .register(registry);
    }

    /**
     * Creates and registers a counter for counting events or operations.
     *
     * @param name The name of the counter
     * @param description The description of what the counter measures
     * @param tags Additional tags to associate with the counter
     * @return A Counter instance that can be used to count events
     */
    public Counter createCounter(String name, String description, Tag... tags) {
        return Counter.builder(name)
                .description(description)
                .tags(tags)
                .register(registry);
    }

    /**
     * Creates and registers a gauge for measuring a value that can increase or decrease.
     *
     * @param name The name of the gauge
     * @param description The description of what the gauge measures
     * @param obj The object to measure
     * @param valueFunction A function that extracts the numeric value from the object
     * @param tags Additional tags to associate with the gauge
     * @param <T> The type of object being measured
     * @return The object being measured
     */
    public <T> T createGauge(String name, String description, T obj, 
                            java.util.function.ToDoubleFunction<T> valueFunction, 
                            Tag... tags) {
        Gauge.builder(name, obj, valueFunction)
                .description(description)
                .tags(tags)
                .register(registry);
        return obj;
    }

    /**
     * Returns the meter registry for direct access to Micrometer APIs.
     *
     * @return The MeterRegistry instance
     */
    public MeterRegistry getRegistry() {
        return registry;
    }
}