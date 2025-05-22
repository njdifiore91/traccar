/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for service discovery and registration mechanisms.
 * Supports both Consul and Kubernetes service discovery.
 */
@Singleton
public class ServiceDiscoveryConfig {

    private final Config config;
    private final String serviceId;
    private final String serviceName;
    private final String serviceHost;
    private final int servicePort;
    private final Map<String, String> serviceTags;
    private final Map<String, String> serviceMetadata;
    private final String healthCheckEndpoint;
    private final int healthCheckInterval;
    private final int healthCheckTimeout;
    private final String discoveryType;

    /**
     * Constructs a new ServiceDiscoveryConfig with the specified configuration.
     *
     * @param config The system configuration
     */
    @Inject
    public ServiceDiscoveryConfig(Config config) {
        this.config = config;
        this.discoveryType = config.getString(Keys.SERVICE_DISCOVERY_TYPE, "kubernetes");
        this.serviceName = config.getString(Keys.SERVICE_DISCOVERY_SERVICE_NAME, "traccar");
        this.serviceId = generateServiceId();
        this.serviceHost = resolveServiceHost();
        this.servicePort = config.getInteger(Keys.WEB_PORT);
        this.serviceTags = parseServiceTags();
        this.serviceMetadata = parseServiceMetadata();
        this.healthCheckEndpoint = config.getString(Keys.SERVICE_DISCOVERY_HEALTH_ENDPOINT, "/api/health");
        this.healthCheckInterval = config.getInteger(Keys.SERVICE_DISCOVERY_HEALTH_INTERVAL, 10);
        this.healthCheckTimeout = config.getInteger(Keys.SERVICE_DISCOVERY_HEALTH_TIMEOUT, 3);
    }

    /**
     * Generates a unique service ID for registration.
     * Uses service name and hostname to ensure uniqueness.
     *
     * @return A unique service ID
     */
    private String generateServiceId() {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "unknown";
        }
        return serviceName + "-" + hostname + "-" + System.currentTimeMillis();
    }

    /**
     * Resolves the service host address for registration.
     * Uses configured address or falls back to auto-detection.
     *
     * @return The service host address
     */
    private String resolveServiceHost() {
        String configuredHost = config.getString(Keys.SERVICE_DISCOVERY_SERVICE_HOST);
        if (configuredHost != null && !configuredHost.isEmpty()) {
            return configuredHost;
        }

        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "127.0.0.1";
        }
    }

    /**
     * Parses service tags from configuration.
     *
     * @return Map of service tags
     */
    private Map<String, String> parseServiceTags() {
        Map<String, String> tags = new HashMap<>();
        String tagsString = config.getString(Keys.SERVICE_DISCOVERY_SERVICE_TAGS);
        if (tagsString != null && !tagsString.isEmpty()) {
            for (String tag : tagsString.split(",")) {
                String[] parts = tag.split("=", 2);
                if (parts.length == 2) {
                    tags.put(parts[0].trim(), parts[1].trim());
                } else {
                    tags.put(parts[0].trim(), "");
                }
            }
        }
        return tags;
    }

    /**
     * Parses service metadata from configuration.
     *
     * @return Map of service metadata
     */
    private Map<String, String> parseServiceMetadata() {
        Map<String, String> metadata = new HashMap<>();
        String metadataString = config.getString(Keys.SERVICE_DISCOVERY_SERVICE_METADATA);
        if (metadataString != null && !metadataString.isEmpty()) {
            for (String entry : metadataString.split(",")) {
                String[] parts = entry.split("=", 2);
                if (parts.length == 2) {
                    metadata.put(parts[0].trim(), parts[1].trim());
                }
            }
        }
        return metadata;
    }

    /**
     * Gets the service discovery type (consul or kubernetes).
     *
     * @return The service discovery type
     */
    public String getDiscoveryType() {
        return discoveryType;
    }

    /**
     * Gets the service ID for registration.
     *
     * @return The service ID
     */
    public String getServiceId() {
        return serviceId;
    }

    /**
     * Gets the service name for registration.
     *
     * @return The service name
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Gets the service host address for registration.
     *
     * @return The service host address
     */
    public String getServiceHost() {
        return serviceHost;
    }

    /**
     * Gets the service port for registration.
     *
     * @return The service port
     */
    public int getServicePort() {
        return servicePort;
    }

    /**
     * Gets the service tags for registration.
     *
     * @return Map of service tags
     */
    public Map<String, String> getServiceTags() {
        return serviceTags;
    }

    /**
     * Gets the service metadata for registration.
     *
     * @return Map of service metadata
     */
    public Map<String, String> getServiceMetadata() {
        return serviceMetadata;
    }

    /**
     * Gets the health check endpoint for service health monitoring.
     *
     * @return The health check endpoint
     */
    public String getHealthCheckEndpoint() {
        return healthCheckEndpoint;
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
     * Gets the health check timeout in seconds.
     *
     * @return The health check timeout
     */
    public int getHealthCheckTimeout() {
        return healthCheckTimeout;
    }

    /**
     * Checks if Consul service discovery is enabled.
     *
     * @return true if Consul is enabled, false otherwise
     */
    public boolean isConsulEnabled() {
        return "consul".equalsIgnoreCase(discoveryType);
    }

    /**
     * Checks if Kubernetes service discovery is enabled.
     *
     * @return true if Kubernetes is enabled, false otherwise
     */
    public boolean isKubernetesEnabled() {
        return "kubernetes".equalsIgnoreCase(discoveryType);
    }

    /**
     * Gets the Consul host address for connecting to Consul.
     *
     * @return The Consul host address
     */
    public String getConsulHost() {
        return config.getString(Keys.SERVICE_DISCOVERY_CONSUL_HOST, "localhost");
    }

    /**
     * Gets the Consul port for connecting to Consul.
     *
     * @return The Consul port
     */
    public int getConsulPort() {
        return config.getInteger(Keys.SERVICE_DISCOVERY_CONSUL_PORT, 8500);
    }

    /**
     * Gets the Consul ACL token for authentication.
     *
     * @return The Consul ACL token
     */
    public String getConsulToken() {
        return config.getString(Keys.SERVICE_DISCOVERY_CONSUL_TOKEN);
    }

    /**
     * Gets the Consul data center name.
     *
     * @return The Consul data center name
     */
    public String getConsulDatacenter() {
        return config.getString(Keys.SERVICE_DISCOVERY_CONSUL_DATACENTER, "dc1");
    }
    
    /**
     * Gets the Consul DNS port for DNS-based service discovery.
     *
     * @return The Consul DNS port
     */
    public int getConsulDnsPort() {
        return config.getInteger(Keys.SERVICE_DISCOVERY_CONSUL_DNS_PORT, 8600);
    }
    
    /**
     * Gets the Consul HTTP API endpoint for HTTP-based service discovery.
     *
     * @return The Consul HTTP API endpoint
     */
    public String getConsulHttpEndpoint() {
        return config.getString(Keys.SERVICE_DISCOVERY_CONSUL_HTTP_ENDPOINT, "/v1/catalog/service/");
    }

    /**
     * Gets the Kubernetes namespace for service discovery.
     *
     * @return The Kubernetes namespace
     */
    public String getKubernetesNamespace() {
        return config.getString(Keys.SERVICE_DISCOVERY_KUBERNETES_NAMESPACE, "default");
    }

    /**
     * Gets the Kubernetes service account token path.
     *
     * @return The Kubernetes service account token path
     */
    public String getKubernetesTokenPath() {
        return config.getString(Keys.SERVICE_DISCOVERY_KUBERNETES_TOKEN_PATH, 
                "/var/run/secrets/kubernetes.io/serviceaccount/token");
    }

    /**
     * Gets the DNS TTL (Time To Live) for service discovery records.
     *
     * @return The DNS TTL in seconds
     */
    public int getDnsTtl() {
        return config.getInteger(Keys.SERVICE_DISCOVERY_DNS_TTL, 30);
    }

    /**
     * Checks if service registration is enabled.
     *
     * @return true if service registration is enabled, false otherwise
     */
    public boolean isRegistrationEnabled() {
        return config.getBoolean(Keys.SERVICE_DISCOVERY_REGISTRATION_ENABLED, true);
    }

    /**
     * Gets the service deregistration timeout in seconds.
     * This is the time after which a service is deregistered if it fails health checks.
     *
     * @return The service deregistration timeout
     */
    public int getDeregistrationTimeout() {
        return config.getInteger(Keys.SERVICE_DISCOVERY_DEREGISTRATION_TIMEOUT, 60);
    }

    /**
     * Gets the preferred instance selection strategy for service discovery.
     * Options include "random", "round-robin", and "nearest".
     *
     * @return The preferred instance selection strategy
     */
    public String getPreferredInstanceStrategy() {
        return config.getString(Keys.SERVICE_DISCOVERY_PREFERRED_INSTANCE_STRATEGY, "random");
    }

    /**
     * Gets the service discovery refresh interval in seconds.
     * This is how often the service discovery cache is refreshed.
     *
     * @return The service discovery refresh interval
     */
    public int getDiscoveryRefreshInterval() {
        return config.getInteger(Keys.SERVICE_DISCOVERY_REFRESH_INTERVAL, 30);
    }

    /**
     * Gets the service discovery cache TTL in seconds.
     * This is how long service discovery results are cached.
     *
     * @return The service discovery cache TTL
     */
    public int getDiscoveryCacheTtl() {
        return config.getInteger(Keys.SERVICE_DISCOVERY_CACHE_TTL, 60);
    }

    /**
     * Gets the service discovery failure timeout in seconds.
     * This is how long to wait before considering a service discovery operation failed.
     *
     * @return The service discovery failure timeout
     */
    public int getDiscoveryFailureTimeout() {
        return config.getInteger(Keys.SERVICE_DISCOVERY_FAILURE_TIMEOUT, 10);
    }

    /**
     * Gets the service discovery retry count.
     * This is how many times to retry a failed service discovery operation.
     *
     * @return The service discovery retry count
     */
    public int getDiscoveryRetryCount() {
        return config.getInteger(Keys.SERVICE_DISCOVERY_RETRY_COUNT, 3);
    }

    /**
     * Gets the service discovery retry delay in seconds.
     * This is how long to wait between retry attempts.
     *
     * @return The service discovery retry delay
     */
    public int getDiscoveryRetryDelay() {
        return config.getInteger(Keys.SERVICE_DISCOVERY_RETRY_DELAY, 5);
    }
}