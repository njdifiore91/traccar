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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.traccar.LifecycleObject;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Configuration for the metrics subsystem in the Position Service.
 * This class sets up the Prometheus registry and exposes the metrics HTTP endpoint.
 * It initializes the collector registry, registers JVM metrics, and manages the lifecycle
 * of the metrics HTTP server. It also provides service and instance identification for
 * consistent metric labeling.
 */
@Configuration
public class MetricsConfiguration implements LifecycleObject {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsConfiguration.class);

    @Value("${metrics.port:8081}")
    private int metricsPort;

    @Value("${metrics.address:0.0.0.0}")
    private String metricsAddress;

    @Value("${metrics.service.name:position-service}")
    private String serviceName;

    @Value("${metrics.instance.id:${HOSTNAME:unknown}}")
    private String instanceId;

    private HTTPServer server;
    private final CollectorRegistry registry;

    public MetricsConfiguration() {
        registry = CollectorRegistry.defaultRegistry;
    }

    /**
     * Creates and configures the Prometheus meter registry.
     * This registry is used to collect and expose metrics in Prometheus format.
     *
     * @return The configured Prometheus meter registry
     */
    @Bean
    public MeterRegistry meterRegistry() {
        PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        meterRegistry.config()
                .commonTags("service", serviceName, "instance", instanceId);
        return meterRegistry;
    }

    /**
     * Initializes the metrics subsystem.
     * This method registers JVM metrics and starts the metrics HTTP server.
     */
    @PostConstruct
    public void init() {
        // Register JVM metrics
        DefaultExports.initialize();
        LOGGER.info("Initialized metrics with service name: {} and instance ID: {}", serviceName, instanceId);
    }

    /**
     * Starts the metrics HTTP server.
     * This method exposes the metrics endpoint on the configured port and address.
     */
    @Override
    public void start() throws Exception {
        try {
            server = new HTTPServer(new InetSocketAddress(metricsAddress, metricsPort), registry);
            LOGGER.info("Started metrics server on {}:{}", metricsAddress, metricsPort);
        } catch (IOException e) {
            LOGGER.error("Failed to start metrics server", e);
            throw e;
        }
    }

    /**
     * Stops the metrics HTTP server.
     * This method is called during application shutdown to clean up resources.
     */
    @Override
    @PreDestroy
    public void stop() throws Exception {
        if (server != null) {
            server.stop();
            LOGGER.info("Stopped metrics server");
        }
    }

    /**
     * Gets the collector registry.
     * This registry is used to register and collect metrics.
     *
     * @return The collector registry
     */
    public CollectorRegistry getRegistry() {
        return registry;
    }

    /**
     * Gets the service name.
     * This name is used for labeling metrics.
     *
     * @return The service name
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Gets the instance ID.
     * This ID is used for labeling metrics to distinguish between service instances.
     *
     * @return The instance ID
     */
    public String getInstanceId() {
        return instanceId;
    }
}