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
package org.traccar.discovery;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.traccar.config.Config;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for service registry settings.
 * Supports both Consul and Kubernetes service registries.
 */
@Singleton
public class ServiceRegistryConfig {

    /**
     * Supported service registry types.
     */
    public enum RegistryType {
        CONSUL,
        KUBERNETES,
        NONE
    }

    private final RegistryType registryType;
    private final String serviceId;
    private final String serviceName;
    private final String serviceHost;
    private final int servicePort;
    private final String registryHost;
    private final int registryPort;
    private final int healthCheckInterval;
    private final int healthCheckTimeout;
    private final int healthCheckFailThreshold;
    private final Map<String, String> serviceMetadata;

    /**
     * Constructs a new ServiceRegistryConfig with settings from the provided Config.
     *
     * @param config The application configuration
     */
    @Inject
    public ServiceRegistryConfig(Config config) {
        // Registry type (CONSUL, KUBERNETES, or NONE)
        String registryTypeStr = config.getString("service.registry.type", "NONE");
        try {
            this.registryType = RegistryType.valueOf(registryTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid service registry type: " + registryTypeStr +
                    ". Supported types are: CONSUL, KUBERNETES, NONE", e);
        }

        // Service identification
        this.serviceId = config.getString("service.id", generateDefaultServiceId());
        this.serviceName = config.getString("service.name", "protocol-service");
        this.serviceHost = config.getString("service.host", "localhost");
        this.servicePort = config.getInteger("service.port", 8082);

        // Registry connection details
        this.registryHost = config.getString("service.registry.host", "localhost");
        this.registryPort = config.getInteger("service.registry.port", getDefaultRegistryPort());

        // Health check settings
        this.healthCheckInterval = config.getInteger("service.healthCheck.interval", 10);
        this.healthCheckTimeout = config.getInteger("service.healthCheck.timeout", 5);
        this.healthCheckFailThreshold = config.getInteger("service.healthCheck.failThreshold", 3);

        // Service metadata
        this.serviceMetadata = new HashMap<>();
        String metadataStr = config.getString("service.metadata", "");
        if (!metadataStr.isEmpty()) {
            String[] entries = metadataStr.split(",");
            for (String entry : entries) {
                String[] keyValue = entry.split("=");
                if (keyValue.length == 2) {
                    serviceMetadata.put(keyValue[0].trim(), keyValue[1].trim());
                }
            }
        }

        // Add environment tag if available
        String environment = config.getString("service.environment", "production");
        serviceMetadata.put("environment", environment);

        // Validate configuration if service discovery is enabled
        if (registryType != RegistryType.NONE) {
            validateConfiguration();
        }
    }

    /**
     * Validates that all required configuration properties are set.
     * Throws IllegalArgumentException if validation fails.
     */
    private void validateConfiguration() {
        if (serviceId == null || serviceId.isEmpty()) {
            throw new IllegalArgumentException("service.id is required");
        }
        if (serviceName == null || serviceName.isEmpty()) {
            throw new IllegalArgumentException("service.name is required");
        }
        if (serviceHost == null || serviceHost.isEmpty()) {
            throw new IllegalArgumentException("service.host is required");
        }
        if (servicePort <= 0) {
            throw new IllegalArgumentException("service.port must be a positive integer");
        }
        if (registryHost == null || registryHost.isEmpty()) {
            throw new IllegalArgumentException("service.registry.host is required");
        }
        if (registryPort <= 0) {
            throw new IllegalArgumentException("service.registry.port must be a positive integer");
        }
        if (healthCheckInterval <= 0) {
            throw new IllegalArgumentException("service.healthCheck.interval must be a positive integer");
        }
        if (healthCheckTimeout <= 0) {
            throw new IllegalArgumentException("service.healthCheck.timeout must be a positive integer");
        }
        if (healthCheckFailThreshold <= 0) {
            throw new IllegalArgumentException("service.healthCheck.failThreshold must be a positive integer");
        }
    }

    /**
     * Generates a default service ID if none is provided.
     * Uses the service name and a random suffix for uniqueness.
     *
     * @return A generated service ID
     */
    private String generateDefaultServiceId() {
        return "protocol-service-" + System.currentTimeMillis() % 10000;
    }

    /**
     * Returns the default port for the selected registry type.
     *
     * @return The default port number
     */
    private int getDefaultRegistryPort() {
        return switch (registryType) {
            case CONSUL -> 8500;
            case KUBERNETES -> 443;
            default -> 0;
        };
    }

    /**
     * Gets the configured registry type.
     *
     * @return The registry type
     */
    @NotNull
    public RegistryType getRegistryType() {
        return registryType;
    }

    /**
     * Gets the service ID used for registration.
     *
     * @return The service ID
     */
    @NotBlank
    public String getServiceId() {
        return serviceId;
    }

    /**
     * Gets the service name.
     *
     * @return The service name
     */
    @NotBlank
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Gets the service host address.
     *
     * @return The service host
     */
    @NotBlank
    public String getServiceHost() {
        return serviceHost;
    }

    /**
     * Gets the service port number.
     *
     * @return The service port
     */
    @Positive
    public int getServicePort() {
        return servicePort;
    }

    /**
     * Gets the registry host address.
     *
     * @return The registry host
     */
    @NotBlank
    public String getRegistryHost() {
        return registryHost;
    }

    /**
     * Gets the registry port number.
     *
     * @return The registry port
     */
    @Positive
    public int getRegistryPort() {
        return registryPort;
    }

    /**
     * Gets the health check interval in seconds.
     *
     * @return The health check interval
     */
    @Positive
    public int getHealthCheckInterval() {
        return healthCheckInterval;
    }

    /**
     * Gets the health check timeout in seconds.
     *
     * @return The health check timeout
     */
    @Positive
    public int getHealthCheckTimeout() {
        return healthCheckTimeout;
    }

    /**
     * Gets the number of consecutive health check failures before a service is marked as unhealthy.
     *
     * @return The health check failure threshold
     */
    @Positive
    public int getHealthCheckFailThreshold() {
        return healthCheckFailThreshold;
    }

    /**
     * Gets the service metadata map.
     *
     * @return The service metadata
     */
    public Map<String, String> getServiceMetadata() {
        return new HashMap<>(serviceMetadata);
    }

    /**
     * Checks if service discovery is enabled.
     *
     * @return true if service discovery is enabled, false otherwise
     */
    public boolean isEnabled() {
        return registryType != RegistryType.NONE;
    }
}