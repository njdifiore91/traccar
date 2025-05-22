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
package org.traccar.config;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.logging.LogbackMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration class for metrics collection and exposure in the microservices architecture.
 * This class provides configuration options for Prometheus metrics endpoints, metric naming
 * conventions, and custom metric registration.
 */
@Singleton
public class MetricsConfig {

    private final Config config;
    private final PrometheusMeterRegistry prometheusRegistry;
    private final CompositeMeterRegistry registry;
    private final String serviceName;
    private final String serviceVersion;
    private final String environment;

    /**
     * Configuration keys for metrics settings.
     */
    public static final class Keys {
        /**
         * Enable metrics collection and exposure.
         */
        public static final ConfigKey<Boolean> METRICS_ENABLE = new BooleanConfigKey(
                "metrics.enable",
                List.of(KeyType.CONFIG),
                true);

        /**
         * Metrics endpoint path for Prometheus scraping.
         */
        public static final ConfigKey<String> METRICS_PATH = new StringConfigKey(
                "metrics.path",
                List.of(KeyType.CONFIG),
                "/metrics");

        /**
         * Service name to be used as a tag in metrics.
         */
        public static final ConfigKey<String> METRICS_SERVICE_NAME = new StringConfigKey(
                "metrics.service.name",
                List.of(KeyType.CONFIG),
                "traccar");

        /**
         * Service version to be used as a tag in metrics.
         */
        public static final ConfigKey<String> METRICS_SERVICE_VERSION = new StringConfigKey(
                "metrics.service.version",
                List.of(KeyType.CONFIG),
                "unknown");

        /**
         * Environment name (e.g., production, staging, development) to be used as a tag in metrics.
         */
        public static final ConfigKey<String> METRICS_ENVIRONMENT = new StringConfigKey(
                "metrics.environment",
                List.of(KeyType.CONFIG),
                "production");

        /**
         * Namespace prefix for all metrics.
         */
        public static final ConfigKey<String> METRICS_NAMESPACE = new StringConfigKey(
                "metrics.namespace",
                List.of(KeyType.CONFIG),
                "traccar");

        /**
         * Enable JVM metrics collection.
         */
        public static final ConfigKey<Boolean> METRICS_JVM_ENABLE = new BooleanConfigKey(
                "metrics.jvm.enable",
                List.of(KeyType.CONFIG),
                true);

        /**
         * Enable system metrics collection.
         */
        public static final ConfigKey<Boolean> METRICS_SYSTEM_ENABLE = new BooleanConfigKey(
                "metrics.system.enable",
                List.of(KeyType.CONFIG),
                true);

        /**
         * Enable trace correlation in metrics.
         */
        public static final ConfigKey<Boolean> METRICS_TRACE_CORRELATION = new BooleanConfigKey(
                "metrics.trace.correlation",
                List.of(KeyType.CONFIG),
                true);
                
        /**
         * Enable logging metrics collection.
         */
        public static final ConfigKey<Boolean> METRICS_LOGGING_ENABLE = new BooleanConfigKey(
                "metrics.logging.enable",
                List.of(KeyType.CONFIG),
                true);

        /**
         * Prometheus Pushgateway URL for pushing metrics (optional).
         */
        public static final ConfigKey<String> METRICS_PUSHGATEWAY_URL = new StringConfigKey(
                "metrics.pushgateway.url",
                List.of(KeyType.CONFIG));

        /**
         * Prometheus Pushgateway push interval in seconds (if pushgateway URL is configured).
         */
        public static final ConfigKey<Integer> METRICS_PUSHGATEWAY_INTERVAL = new IntegerConfigKey(
                "metrics.pushgateway.interval",
                List.of(KeyType.CONFIG),
                60);
    }

    /**
     * Constructs a new MetricsConfig instance.
     *
     * @param config The application configuration.
     */
    @Inject
    public MetricsConfig(Config config) {
        this.config = config;
        this.serviceName = config.getString(Keys.METRICS_SERVICE_NAME);
        this.serviceVersion = config.getString(Keys.METRICS_SERVICE_VERSION);
        this.environment = config.getString(Keys.METRICS_ENVIRONMENT);

        // Create the composite registry for multiple metric backends
        registry = new CompositeMeterRegistry(Clock.SYSTEM);
        
        // Create the Prometheus registry with default configuration
        prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT, Clock.SYSTEM);
        
        // Add Prometheus registry to the composite registry
        registry.add(prometheusRegistry);

        // Configure common tags for all metrics
        configureCommonTags();

        // Register JVM and system metrics if enabled
        registerJvmMetrics();
        registerSystemMetrics();
        registerLoggingMetrics();
    }

    /**
     * Configures common tags that will be applied to all metrics.
     */
    private void configureCommonTags() {
        List<Tag> commonTags = new ArrayList<>();
        commonTags.add(Tag.of("service", serviceName));
        commonTags.add(Tag.of("version", serviceVersion));
        commonTags.add(Tag.of("namespace", config.getString(Keys.METRICS_NAMESPACE)));
        commonTags.add(Tag.of("environment", environment));
        commonTags.add(Tag.of("instance", getInstanceId()));

        registry.config().commonTags(commonTags);
    }

    /**
     * Registers JVM metrics if enabled in configuration.
     */
    private void registerJvmMetrics() {
        if (config.getBoolean(Keys.METRICS_JVM_ENABLE)) {
            new ClassLoaderMetrics().bindTo(registry);
            new JvmMemoryMetrics().bindTo(registry);
            new JvmGcMetrics().bindTo(registry);
            new JvmThreadMetrics().bindTo(registry);
        }
    }

    /**
     * Registers system metrics if enabled in configuration.
     */
    private void registerSystemMetrics() {
        if (config.getBoolean(Keys.METRICS_SYSTEM_ENABLE)) {
            new ProcessorMetrics().bindTo(registry);
        }
    }
    
    /**
     * Registers logging metrics if enabled in configuration.
     */
    private void registerLoggingMetrics() {
        if (config.getBoolean(Keys.METRICS_LOGGING_ENABLE)) {
            new LogbackMetrics().bindTo(registry);
        }
    }
    
    /**
     * Generates a unique instance ID for this service instance.
     * Uses hostname or a random UUID if hostname is not available.
     * 
     * @return A unique instance identifier.
     */
    private String getInstanceId() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return java.util.UUID.randomUUID().toString();
        }
    }

    /**
     * Gets the Prometheus meter registry.
     *
     * @return The Prometheus meter registry.
     */
    public MeterRegistry getRegistry() {
        return registry;
    }

    /**
     * Gets the Prometheus registry for direct access to Prometheus-specific functionality.
     *
     * @return The Prometheus registry.
     */
    public PrometheusMeterRegistry getPrometheusRegistry() {
        return prometheusRegistry;
    }

    /**
     * Scrapes the current metrics in Prometheus format.
     *
     * @return The metrics in Prometheus text format.
     */
    public String scrape() {
        return prometheusRegistry.scrape();
    }

    /**
     * Scrapes the current metrics in OpenMetrics format.
     *
     * @return The metrics in OpenMetrics format.
     */
    public String scrapeOpenMetrics() {
        return prometheusRegistry.scrape("application/openmetrics-text");
    }

    /**
     * Gets the configured metrics endpoint path.
     *
     * @return The metrics endpoint path.
     */
    public String getMetricsPath() {
        return config.getString(Keys.METRICS_PATH);
    }

    /**
     * Checks if metrics collection and exposure is enabled.
     *
     * @return True if metrics are enabled, false otherwise.
     */
    public boolean isMetricsEnabled() {
        return config.getBoolean(Keys.METRICS_ENABLE);
    }

    /**
     * Gets the service name used for metrics tagging.
     *
     * @return The service name.
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Gets the service version used for metrics tagging.
     *
     * @return The service version.
     */
    public String getServiceVersion() {
        return serviceVersion;
    }

    /**
     * Gets the environment name used for metrics tagging.
     *
     * @return The environment name.
     */
    public String getEnvironment() {
        return environment;
    }

    /**
     * Checks if trace correlation in metrics is enabled.
     *
     * @return True if trace correlation is enabled, false otherwise.
     */
    public boolean isTraceCorrelationEnabled() {
        return config.getBoolean(Keys.METRICS_TRACE_CORRELATION);
    }

    /**
     * Gets the Prometheus Pushgateway URL if configured.
     *
     * @return The Pushgateway URL or null if not configured.
     */
    public String getPushgatewayUrl() {
        return config.getString(Keys.METRICS_PUSHGATEWAY_URL);
    }

    /**
     * Gets the Prometheus Pushgateway push interval in seconds.
     *
     * @return The push interval in seconds.
     */
    public int getPushgatewayInterval() {
        return config.getInteger(Keys.METRICS_PUSHGATEWAY_INTERVAL);
    }
    
    /**
     * Adds a custom meter registry to the composite registry.
     * This allows for extending the metrics collection to additional backends.
     *
     * @param registry The meter registry to add.
     */
    public void addRegistry(MeterRegistry registry) {
        this.registry.add(registry);
    }
    
    /**
     * Creates a metric name following the standardized naming convention.
     * Format: {namespace}_{domain}_{entity}_{action}
     *
     * @param domain The domain area (e.g., "http", "database", "cache")
     * @param entity The entity being measured (e.g., "requests", "connections")
     * @param action The action or property being measured (e.g., "total", "duration")
     * @return A standardized metric name
     */
    public String createMetricName(String domain, String entity, String action) {
        String namespace = config.getString(Keys.METRICS_NAMESPACE);
        return String.format("%s_%s_%s_%s", namespace, domain, entity, action);
    }
}