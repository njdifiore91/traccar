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

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.LifecycleObject;
import org.traccar.config.Config;

import org.traccar.config.ConfigKey;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Configuration for the metrics subsystem in the Protocol Service.
 * This class sets up the Prometheus registry and exposes the metrics HTTP endpoint.
 * It initializes the collector registry, registers JVM metrics, and manages the lifecycle
 * of the metrics HTTP server. It also provides service and instance identification for
 * consistent metric labeling.
 */
@Singleton
public class MetricsConfiguration implements LifecycleObject {

    /**
     * Configuration key for enabling metrics endpoint.
     */
    public static final ConfigKey METRICS_ENABLE = new ConfigKey(
            "metrics.enable", Boolean.class, Boolean.TRUE);

    /**
     * Configuration key for metrics HTTP endpoint port.
     */
    public static final ConfigKey METRICS_PORT = new ConfigKey(
            "metrics.port", Integer.class, 9400);

    /**
     * Configuration key for service name used in metrics labels.
     */
    public static final ConfigKey METRICS_SERVICE_NAME = new ConfigKey(
            "metrics.service.name", String.class);

    /**
     * Configuration key for instance ID used in metrics labels.
     */
    public static final ConfigKey METRICS_INSTANCE_ID = new ConfigKey(
            "metrics.instance.id", String.class);

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsConfiguration.class);

    private final Config config;
    private final CollectorRegistry registry;
    private HTTPServer server;
    
    // Core metrics for the Protocol Service
    private final Counter messagesReceived;
    private final Counter messagesProcessed;
    private final Counter messagesRejected;
    private final Gauge activeConnections;

    /**
     * Constructs a new MetricsConfiguration with the specified configuration.
     * Initializes the Prometheus collector registry and registers JVM metrics.
     *
     * @param config The application configuration
     */
    @Inject
    public MetricsConfiguration(Config config) {
        this.config = config;
        this.registry = CollectorRegistry.defaultRegistry;
        
        // Register JVM metrics
        DefaultExports.initialize();
        
        // Add common labels for all metrics
        CommonLabels.initialize(getServiceName(), getInstanceId());
        
        // Initialize core metrics
        messagesReceived = Counter.build()
                .name("protocol_messages_received_total")
                .help("Total number of messages received by protocol")
                .labelNames("protocol")
                .register();
        
        messagesProcessed = Counter.build()
                .name("protocol_messages_processed_total")
                .help("Total number of messages successfully processed by protocol")
                .labelNames("protocol")
                .register();
        
        messagesRejected = Counter.build()
                .name("protocol_messages_rejected_total")
                .help("Total number of messages rejected by protocol")
                .labelNames("protocol", "reason")
                .register();
        
        activeConnections = Gauge.build()
                .name("protocol_active_connections")
                .help("Number of active device connections by protocol")
                .labelNames("protocol")
                .register();
        
        LOGGER.info("Metrics subsystem initialized with service={}, instance={}", 
                getServiceName(), getInstanceId());
    }

    /**
     * Starts the metrics HTTP server to expose the metrics endpoint.
     * The server is configured with the port specified in the configuration.
     * If the metrics endpoint is disabled in the configuration, this method does nothing.
     *
     * @throws Exception if there is an error starting the server
     */
    @Override
    public void start() throws Exception {
        if (!config.getBoolean(METRICS_ENABLE)) {
            LOGGER.info("Metrics endpoint is disabled");
            return;
        }

        int port = config.getInteger(METRICS_PORT);
        try {
            server = new HTTPServer(new InetSocketAddress(port), registry);
            LOGGER.info("Metrics endpoint started on port {}", port);
        } catch (IOException e) {
            LOGGER.error("Failed to start metrics endpoint on port {}", port, e);
            throw e;
        }
    }

    /**
     * Stops the metrics HTTP server if it is running.
     * This method is called when the application is shutting down.
     */
    @Override
    public void stop() {
        if (server != null) {
            server.stop();
            LOGGER.info("Metrics endpoint stopped");
        }
    }

    /**
     * Gets the Prometheus collector registry used by this configuration.
     * This can be used by other components to register custom metrics.
     *
     * @return the collector registry
     */
    public CollectorRegistry getRegistry() {
        return registry;
    }
    
    /**
     * Increments the messages received counter for the specified protocol.
     *
     * @param protocol the protocol name
     */
    public void incrementMessagesReceived(String protocol) {
        messagesReceived.labels(protocol).inc();
    }
    
    /**
     * Increments the messages processed counter for the specified protocol.
     *
     * @param protocol the protocol name
     */
    public void incrementMessagesProcessed(String protocol) {
        messagesProcessed.labels(protocol).inc();
    }
    
    /**
     * Increments the messages rejected counter for the specified protocol and reason.
     *
     * @param protocol the protocol name
     * @param reason the rejection reason
     */
    public void incrementMessagesRejected(String protocol, String reason) {
        messagesRejected.labels(protocol, reason).inc();
    }
    
    /**
     * Sets the number of active connections for the specified protocol.
     *
     * @param protocol the protocol name
     * @param count the number of active connections
     */
    public void setActiveConnections(String protocol, int count) {
        activeConnections.labels(protocol).set(count);
    }
    
    /**
     * Increments the active connections count for the specified protocol.
     *
     * @param protocol the protocol name
     */
    public void incrementActiveConnections(String protocol) {
        activeConnections.labels(protocol).inc();
    }
    
    /**
     * Decrements the active connections count for the specified protocol.
     *
     * @param protocol the protocol name
     */
    public void decrementActiveConnections(String protocol) {
        activeConnections.labels(protocol).dec();
    }

    /**
     * Gets the service name for metric labeling.
     * This provides consistent identification of the service in metrics.
     *
     * @return the service name
     */
    public String getServiceName() {
        return config.getString(METRICS_SERVICE_NAME, "protocol-service");
    }

    /**
     * Gets the instance identifier for metric labeling.
     * This provides consistent identification of the service instance in metrics.
     *
     * @return the instance identifier
     */
    public String getInstanceId() {
        return config.getString(METRICS_INSTANCE_ID, getDefaultInstanceId());
    }

    /**
     * Generates a default instance identifier based on the hostname and a random suffix.
     * This is used if no instance ID is specified in the configuration.
     *
     * @return the default instance identifier
     */
    private String getDefaultInstanceId() {
        try {
            String hostname = java.net.InetAddress.getLocalHost().getHostName();
            return hostname + "-" + System.currentTimeMillis() % 1000;
        } catch (Exception e) {
            return "unknown-" + System.currentTimeMillis() % 1000;
        }
    }
}