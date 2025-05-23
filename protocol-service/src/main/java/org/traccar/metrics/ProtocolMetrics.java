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
package org.traccar.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Central registry for all protocol-related metrics in the Protocol Service.
 * This class defines and manages Prometheus metrics for tracking protocol operations,
 * following standardized naming conventions. It serves as the main entry point for
 * accessing all metrics categories (connection, message, health) and provides the
 * collector registry for the metrics HTTP endpoint.
 */
@Singleton
public class ProtocolMetrics {

    private final PrometheusMeterRegistry registry;
    private final ConnectionMetrics connectionMetrics;
    private final MessageMetrics messageMetrics;
    private final HealthMetrics healthMetrics;

    /**
     * Creates a new ProtocolMetrics instance with the specified configuration.
     * Initializes all metric categories and registers JVM and system metrics.
     */
    @Inject
    public ProtocolMetrics() {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        
        // Register JVM and system metrics
        new JvmMemoryMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        
        // Initialize metric categories
        connectionMetrics = new ConnectionMetrics(registry);
        messageMetrics = new MessageMetrics(registry);
        healthMetrics = new HealthMetrics(registry);
        
        // Set common tags for all metrics
        registry.config().commonTags(
                "service", "protocol-service",
                "component", "protocol");
    }

    /**
     * Gets the Prometheus meter registry.
     *
     * @return The Prometheus meter registry
     */
    public PrometheusMeterRegistry getRegistry() {
        return registry;
    }

    /**
     * Gets the connection metrics.
     *
     * @return The connection metrics
     */
    public ConnectionMetrics getConnectionMetrics() {
        return connectionMetrics;
    }

    /**
     * Gets the message metrics.
     *
     * @return The message metrics
     */
    public MessageMetrics getMessageMetrics() {
        return messageMetrics;
    }

    /**
     * Gets the health metrics.
     *
     * @return The health metrics
     */
    public HealthMetrics getHealthMetrics() {
        return healthMetrics;
    }

    /**
     * Metrics related to device connections.
     */
    public static class ConnectionMetrics {
        private final MeterRegistry registry;
        private final AtomicInteger activeConnections;
        private final Counter connectionAttempts;
        private final Counter connectionFailures;
        private final Map<String, Counter> protocolConnectionAttempts;
        private final Map<String, Counter> protocolConnectionFailures;

        /**
         * Creates a new ConnectionMetrics instance.
         *
         * @param registry The meter registry
         */
        public ConnectionMetrics(MeterRegistry registry) {
            this.registry = registry;
            
            // Active connections gauge
            activeConnections = new AtomicInteger(0);
            Gauge.builder("traccar_protocol_connections_active", activeConnections, AtomicInteger::get)
                    .description("Number of active device connections")
                    .register(registry);
            
            // Connection attempts counter
            connectionAttempts = Counter.builder("traccar_protocol_connections_attempts_total")
                    .description("Total number of connection attempts")
                    .register(registry);
            
            // Connection failures counter
            connectionFailures = Counter.builder("traccar_protocol_connections_failures_total")
                    .description("Total number of connection failures")
                    .register(registry);
            
            // Protocol-specific connection metrics
            protocolConnectionAttempts = new ConcurrentHashMap<>();
            protocolConnectionFailures = new ConcurrentHashMap<>();
        }

        /**
         * Increments the active connections count.
         */
        public void incrementActiveConnections() {
            activeConnections.incrementAndGet();
        }

        /**
         * Decrements the active connections count.
         */
        public void decrementActiveConnections() {
            activeConnections.decrementAndGet();
        }

        /**
         * Records a connection attempt.
         *
         * @param protocol The protocol name
         */
        public void recordConnectionAttempt(String protocol) {
            connectionAttempts.increment();
            protocolConnectionAttempts.computeIfAbsent(protocol, p -> 
                Counter.builder("traccar_protocol_connections_attempts_total")
                    .description("Total number of connection attempts by protocol")
                    .tag("protocol", p)
                    .register(registry)
            ).increment();
        }

        /**
         * Records a connection failure.
         *
         * @param protocol The protocol name
         * @param reason The failure reason
         */
        public void recordConnectionFailure(String protocol, String reason) {
            connectionFailures.increment();
            protocolConnectionFailures.computeIfAbsent(protocol, p -> 
                Counter.builder("traccar_protocol_connections_failures_total")
                    .description("Total number of connection failures by protocol")
                    .tag("protocol", p)
                    .register(registry)
            ).increment();
        }

        /**
         * Gets the current number of active connections.
         *
         * @return The number of active connections
         */
        public int getActiveConnections() {
            return activeConnections.get();
        }
    }

    /**
     * Metrics related to message processing.
     */
    public static class MessageMetrics {
        private final MeterRegistry registry;
        private final Counter messagesReceived;
        private final Counter messagesProcessed;
        private final Counter messagesRejected;
        private final Map<String, Counter> protocolMessagesReceived;
        private final Map<String, Counter> protocolMessagesProcessed;
        private final Map<String, Counter> protocolMessagesRejected;
        private final Timer messageProcessingTime;

        /**
         * Creates a new MessageMetrics instance.
         *
         * @param registry The meter registry
         */
        public MessageMetrics(MeterRegistry registry) {
            this.registry = registry;
            
            // Messages received counter
            messagesReceived = Counter.builder("traccar_protocol_messages_received_total")
                    .description("Total number of messages received")
                    .register(registry);
            
            // Messages processed counter
            messagesProcessed = Counter.builder("traccar_protocol_messages_processed_total")
                    .description("Total number of messages successfully processed")
                    .register(registry);
            
            // Messages rejected counter
            messagesRejected = Counter.builder("traccar_protocol_messages_rejected_total")
                    .description("Total number of messages rejected due to errors")
                    .register(registry);
            
            // Message processing time timer
            messageProcessingTime = Timer.builder("traccar_protocol_message_processing_duration_seconds")
                    .description("Time taken to process messages")
                    .publishPercentiles(0.5, 0.95, 0.99)
                    .register(registry);
            
            // Protocol-specific message metrics
            protocolMessagesReceived = new ConcurrentHashMap<>();
            protocolMessagesProcessed = new ConcurrentHashMap<>();
            protocolMessagesRejected = new ConcurrentHashMap<>();
        }

        /**
         * Records a message received event.
         *
         * @param protocol The protocol name
         */
        public void recordMessageReceived(String protocol) {
            messagesReceived.increment();
            protocolMessagesReceived.computeIfAbsent(protocol, p -> 
                Counter.builder("traccar_protocol_messages_received_total")
                    .description("Total number of messages received by protocol")
                    .tag("protocol", p)
                    .register(registry)
            ).increment();
        }

        /**
         * Records a message processed event.
         *
         * @param protocol The protocol name
         */
        public void recordMessageProcessed(String protocol) {
            messagesProcessed.increment();
            protocolMessagesProcessed.computeIfAbsent(protocol, p -> 
                Counter.builder("traccar_protocol_messages_processed_total")
                    .description("Total number of messages successfully processed by protocol")
                    .tag("protocol", p)
                    .register(registry)
            ).increment();
        }

        /**
         * Records a message rejected event.
         *
         * @param protocol The protocol name
         * @param reason The rejection reason
         */
        public void recordMessageRejected(String protocol, String reason) {
            messagesRejected.increment();
            protocolMessagesRejected.computeIfAbsent(protocol, p -> 
                Counter.builder("traccar_protocol_messages_rejected_total")
                    .description("Total number of messages rejected by protocol")
                    .tag("protocol", p)
                    .tag("reason", reason)
                    .register(registry)
            ).increment();
        }

        /**
         * Creates a timer for measuring message processing time.
         *
         * @param protocol The protocol name
         * @return A timer sample that can be stopped to record processing time
         */
        public Timer.Sample startMessageProcessingTimer(String protocol) {
            return Timer.start(registry);
        }

        /**
         * Stops the message processing timer and records the duration.
         *
         * @param sample The timer sample created by startMessageProcessingTimer
         * @param protocol The protocol name
         */
        public void stopMessageProcessingTimer(Timer.Sample sample, String protocol) {
            sample.stop(Timer.builder("traccar_protocol_message_processing_duration_seconds")
                    .description("Time taken to process messages by protocol")
                    .tag("protocol", protocol)
                    .publishPercentiles(0.5, 0.95, 0.99)
                    .register(registry));
        }
    }

    /**
     * Metrics related to service health.
     */
    public static class HealthMetrics {
        private final MeterRegistry registry;
        private final AtomicInteger serviceStatus;
        private final Map<String, AtomicInteger> componentStatus;

        /**
         * Creates a new HealthMetrics instance.
         *
         * @param registry The meter registry
         */
        public HealthMetrics(MeterRegistry registry) {
            this.registry = registry;
            
            // Service health status gauge (0 = down, 1 = up)
            serviceStatus = new AtomicInteger(1);
            Gauge.builder("traccar_protocol_service_up", serviceStatus, AtomicInteger::get)
                    .description("Indicates if the protocol service is operational (0 = down, 1 = up)")
                    .register(registry);
            
            // Component health status gauges
            componentStatus = new ConcurrentHashMap<>();
        }

        /**
         * Sets the service health status.
         *
         * @param isUp True if the service is up, false otherwise
         */
        public void setServiceStatus(boolean isUp) {
            serviceStatus.set(isUp ? 1 : 0);
        }

        /**
         * Sets the health status of a specific component.
         *
         * @param component The component name
         * @param isUp True if the component is up, false otherwise
         */
        public void setComponentStatus(String component, boolean isUp) {
            componentStatus.computeIfAbsent(component, c -> {
                AtomicInteger status = new AtomicInteger(isUp ? 1 : 0);
                Gauge.builder("traccar_protocol_component_up", status, AtomicInteger::get)
                        .description("Indicates if a protocol service component is operational (0 = down, 1 = up)")
                        .tag("component", c)
                        .register(registry);
                return status;
            }).set(isUp ? 1 : 0);
        }

        /**
         * Gets the current service health status.
         *
         * @return True if the service is up, false otherwise
         */
        public boolean isServiceUp() {
            return serviceStatus.get() == 1;
        }

        /**
         * Gets the current health status of a specific component.
         *
         * @param component The component name
         * @return True if the component is up, false otherwise
         */
        public boolean isComponentUp(String component) {
            AtomicInteger status = componentStatus.get(component);
            return status != null && status.get() == 1;
        }
    }
}