/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Configuration class for service registry settings, including registry type (Consul or Kubernetes),
 * connection details, health check settings, and service metadata. This class centralizes all
 * configuration related to service discovery and registration.
 */
@Singleton
public class ServiceRegistryConfig {

    private String registryType;
    private String namespace;
    
    // Consul specific configuration
    private String consulHost;
    private int consulPort;
    private String consulScheme;
    private String consulAclToken;
    
    // Health check configuration
    private boolean healthCheckEnabled;
    private String healthCheckEndpoint;
    private int healthCheckInterval;
    private int healthCheckTimeout;
    private int deregisterCriticalServiceAfter;
    
    /**
     * Default constructor with default values.
     */
    public ServiceRegistryConfig() {
        // Default values
        this.registryType = ServiceRegistryFactory.DEFAULT_REGISTRY_TYPE;
        this.namespace = "default";
        this.consulHost = "localhost";
        this.consulPort = 8500;
        this.consulScheme = "http";
        this.healthCheckEnabled = true;
        this.healthCheckEndpoint = "/health";
        this.healthCheckInterval = 10;
        this.healthCheckTimeout = 5;
        this.deregisterCriticalServiceAfter = 30;
    }

    /**
     * Gets the registry type (consul or kubernetes).
     *
     * @return The registry type
     */
    public String getRegistryType() {
        return registryType;
    }

    /**
     * Sets the registry type (consul or kubernetes).
     *
     * @param registryType The registry type to set
     */
    public void setRegistryType(String registryType) {
        this.registryType = registryType;
    }

    /**
     * Gets the namespace (for Kubernetes) or datacenter (for Consul).
     *
     * @return The namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Sets the namespace (for Kubernetes) or datacenter (for Consul).
     *
     * @param namespace The namespace to set
     */
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * Gets the Consul host.
     *
     * @return The Consul host
     */
    public String getConsulHost() {
        return consulHost;
    }

    /**
     * Sets the Consul host.
     *
     * @param consulHost The Consul host to set
     */
    public void setConsulHost(String consulHost) {
        this.consulHost = consulHost;
    }

    /**
     * Gets the Consul port.
     *
     * @return The Consul port
     */
    public int getConsulPort() {
        return consulPort;
    }

    /**
     * Sets the Consul port.
     *
     * @param consulPort The Consul port to set
     */
    public void setConsulPort(int consulPort) {
        this.consulPort = consulPort;
    }

    /**
     * Gets the Consul scheme (http or https).
     *
     * @return The Consul scheme
     */
    public String getConsulScheme() {
        return consulScheme;
    }

    /**
     * Sets the Consul scheme (http or https).
     *
     * @param consulScheme The Consul scheme to set
     */
    public void setConsulScheme(String consulScheme) {
        this.consulScheme = consulScheme;
    }

    /**
     * Gets the Consul ACL token.
     *
     * @return The Consul ACL token
     */
    public String getConsulAclToken() {
        return consulAclToken;
    }

    /**
     * Sets the Consul ACL token.
     *
     * @param consulAclToken The Consul ACL token to set
     */
    public void setConsulAclToken(String consulAclToken) {
        this.consulAclToken = consulAclToken;
    }

    /**
     * Checks if health checks are enabled.
     *
     * @return true if health checks are enabled, false otherwise
     */
    public boolean isHealthCheckEnabled() {
        return healthCheckEnabled;
    }

    /**
     * Sets whether health checks are enabled.
     *
     * @param healthCheckEnabled true to enable health checks, false to disable
     */
    public void setHealthCheckEnabled(boolean healthCheckEnabled) {
        this.healthCheckEnabled = healthCheckEnabled;
    }

    /**
     * Gets the health check endpoint.
     *
     * @return The health check endpoint
     */
    public String getHealthCheckEndpoint() {
        return healthCheckEndpoint;
    }

    /**
     * Sets the health check endpoint.
     *
     * @param healthCheckEndpoint The health check endpoint to set
     */
    public void setHealthCheckEndpoint(String healthCheckEndpoint) {
        this.healthCheckEndpoint = healthCheckEndpoint;
    }

    /**
     * Gets the health check interval in seconds.
     *
     * @return The health check interval
     */
    public int getHealthCheckInterval() {
        return healthCheckInterval;
    }

    /**
     * Sets the health check interval in seconds.
     *
     * @param healthCheckInterval The health check interval to set
     */
    public void setHealthCheckInterval(int healthCheckInterval) {
        this.healthCheckInterval = healthCheckInterval;
    }

    /**
     * Gets the health check timeout in seconds.
     *
     * @return The health check timeout
     */
    public int getHealthCheckTimeout() {
        return healthCheckTimeout;
    }

    /**
     * Sets the health check timeout in seconds.
     *
     * @param healthCheckTimeout The health check timeout to set
     */
    public void setHealthCheckTimeout(int healthCheckTimeout) {
        this.healthCheckTimeout = healthCheckTimeout;
    }

    /**
     * Gets the time in minutes after which a critical service is deregistered.
     *
     * @return The deregister critical service after time
     */
    public int getDeregisterCriticalServiceAfter() {
        return deregisterCriticalServiceAfter;
    }

    /**
     * Sets the time in minutes after which a critical service is deregistered.
     *
     * @param deregisterCriticalServiceAfter The deregister critical service after time to set
     */
    public void setDeregisterCriticalServiceAfter(int deregisterCriticalServiceAfter) {
        this.deregisterCriticalServiceAfter = deregisterCriticalServiceAfter;
    }
}