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
package org.traccar.discovery;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents a service instance with its metadata for the Event Processing Service.
 * This class is used by the service registry to track registered services and their current state,
 * enabling service discovery and load balancing for the Event Processing Service.
 */
public class ServiceInstance {

    private final String serviceId;
    private final String host;
    private final int port;
    private final boolean secure;
    private final Map<String, String> metadata;
    private HealthStatus healthStatus;

    /**
     * Health status of a service instance.
     */
    public enum HealthStatus {
        UP,         // Service is healthy and operational
        DOWN,       // Service is not operational
        STARTING,   // Service is starting up
        STOPPING,   // Service is shutting down
        UNKNOWN     // Health status could not be determined
    }

    /**
     * Private constructor used by the Builder.
     */
    private ServiceInstance(String serviceId, String host, int port, boolean secure, Map<String, String> metadata) {
        this.serviceId = serviceId;
        this.host = host;
        this.port = port;
        this.secure = secure;
        this.metadata = new HashMap<>(metadata);
        this.healthStatus = HealthStatus.UNKNOWN;
    }

    /**
     * Gets the unique identifier for this service instance.
     *
     * @return The service instance ID
     */
    public String getServiceId() {
        return serviceId;
    }

    /**
     * Gets the hostname or IP address where this service instance is running.
     *
     * @return The host name or IP address
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets the port number where this service instance is listening.
     *
     * @return The port number
     */
    public int getPort() {
        return port;
    }

    /**
     * Checks if this service instance uses secure communication (HTTPS).
     *
     * @return true if secure, false otherwise
     */
    public boolean isSecure() {
        return secure;
    }

    /**
     * Gets the URI for this service instance.
     *
     * @return The URI in the format http(s)://host:port
     */
    public String getUri() {
        return (secure ? "https://" : "http://") + host + ":" + port;
    }

    /**
     * Gets the metadata associated with this service instance.
     *
     * @return An unmodifiable map of metadata
     */
    public Map<String, String> getMetadata() {
        return Collections.unmodifiableMap(metadata);
    }

    /**
     * Gets a specific metadata value by key.
     *
     * @param key The metadata key
     * @return The metadata value, or null if not found
     */
    public String getMetadata(String key) {
        return metadata.get(key);
    }

    /**
     * Gets the current health status of this service instance.
     *
     * @return The health status
     */
    public HealthStatus getHealthStatus() {
        return healthStatus;
    }

    /**
     * Sets the health status of this service instance.
     *
     * @param healthStatus The new health status
     */
    public void setHealthStatus(HealthStatus healthStatus) {
        this.healthStatus = healthStatus;
    }

    /**
     * Checks if this service instance is healthy (UP status).
     *
     * @return true if healthy, false otherwise
     */
    public boolean isHealthy() {
        return healthStatus == HealthStatus.UP;
    }

    /**
     * Updates the health status based on event processing specific checks.
     * This method can be extended to include more sophisticated health checks.
     *
     * @param eventHandlersActive Whether event handlers are active and processing events
     * @param databaseConnected Whether database connection is established
     * @param brokerConnected Whether message broker connection is established
     */
    public void updateHealthStatus(boolean eventHandlersActive, boolean databaseConnected, boolean brokerConnected) {
        if (!databaseConnected || !brokerConnected) {
            setHealthStatus(HealthStatus.DOWN);
        } else if (!eventHandlersActive) {
            setHealthStatus(HealthStatus.STARTING);
        } else {
            setHealthStatus(HealthStatus.UP);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceInstance that = (ServiceInstance) o;
        return port == that.port &&
                secure == that.secure &&
                Objects.equals(serviceId, that.serviceId) &&
                Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceId, host, port, secure);
    }

    @Override
    public String toString() {
        return "ServiceInstance{" +
                "serviceId='" + serviceId + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", secure=" + secure +
                ", healthStatus=" + healthStatus +
                ", metadata=" + metadata +
                '}';
    }

    /**
     * Builder for creating ServiceInstance objects.
     */
    public static class Builder {
        private String serviceId;
        private String host;
        private int port;
        private boolean secure;
        private final Map<String, String> metadata = new HashMap<>();

        /**
         * Creates a new Builder instance.
         */
        public Builder() {
            // Generate a random service ID if not provided
            this.serviceId = "event-service-" + UUID.randomUUID().toString();
            // Default to non-secure HTTP
            this.secure = false;
        }

        /**
         * Sets the service ID.
         *
         * @param serviceId The service ID
         * @return This builder for method chaining
         */
        public Builder serviceId(String serviceId) {
            this.serviceId = serviceId;
            return this;
        }

        /**
         * Sets the host name or IP address.
         *
         * @param host The host name or IP address
         * @return This builder for method chaining
         */
        public Builder host(String host) {
            this.host = host;
            return this;
        }

        /**
         * Sets the port number.
         *
         * @param port The port number
         * @return This builder for method chaining
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets whether the service uses secure communication (HTTPS).
         *
         * @param secure true for HTTPS, false for HTTP
         * @return This builder for method chaining
         */
        public Builder secure(boolean secure) {
            this.secure = secure;
            return this;
        }

        /**
         * Adds a metadata entry.
         *
         * @param key The metadata key
         * @param value The metadata value
         * @return This builder for method chaining
         */
        public Builder addMetadata(String key, String value) {
            this.metadata.put(key, value);
            return this;
        }

        /**
         * Adds multiple metadata entries.
         *
         * @param metadata The metadata map to add
         * @return This builder for method chaining
         */
        public Builder metadata(Map<String, String> metadata) {
            this.metadata.putAll(metadata);
            return this;
        }

        /**
         * Adds event service specific metadata with default values.
         *
         * @return This builder for method chaining
         */
        public Builder withEventServiceDefaults() {
            this.metadata.put("service-type", "event-processing");
            this.metadata.put("api-version", "v1");
            this.metadata.put("supports-event-types", "geofence,overspeed,motion,device,driver,alarm,media");
            return this;
        }

        /**
         * Builds a new ServiceInstance with the configured values.
         *
         * @return A new ServiceInstance
         * @throws IllegalStateException if host or port is not set
         */
        public ServiceInstance build() {
            if (host == null || host.isEmpty()) {
                throw new IllegalStateException("Host must be set");
            }
            if (port <= 0) {
                throw new IllegalStateException("Port must be a positive number");
            }
            return new ServiceInstance(serviceId, host, port, secure, metadata);
        }
    }
}