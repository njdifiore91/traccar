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
 * Model class representing a service instance with its metadata, including service ID, host, port,
 * health status, and additional attributes. This class is used by the service registry to track
 * registered services and their current state, enabling service discovery and load balancing.
 */
public class ServiceInstance {

    private final String instanceId;
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
        /**
         * Service is healthy and can receive traffic.
         */
        UP,
        
        /**
         * Service is starting up and not ready for traffic.
         */
        STARTING,
        
        /**
         * Service is experiencing issues but still operational.
         */
        DEGRADED,
        
        /**
         * Service is unhealthy and should not receive traffic.
         */
        DOWN,
        
        /**
         * Service is being taken out of service.
         */
        OUT_OF_SERVICE
    }

    private ServiceInstance(Builder builder) {
        this.instanceId = builder.instanceId != null ? builder.instanceId : UUID.randomUUID().toString();
        this.serviceId = Objects.requireNonNull(builder.serviceId, "serviceId cannot be null");
        this.host = Objects.requireNonNull(builder.host, "host cannot be null");
        this.port = builder.port;
        this.secure = builder.secure;
        this.metadata = Collections.unmodifiableMap(new HashMap<>(builder.metadata));
        this.healthStatus = builder.healthStatus != null ? builder.healthStatus : HealthStatus.STARTING;
    }

    /**
     * Get the unique identifier for this service instance.
     *
     * @return The instance ID
     */
    public String getInstanceId() {
        return instanceId;
    }

    /**
     * Get the service identifier (service name).
     *
     * @return The service ID
     */
    public String getServiceId() {
        return serviceId;
    }

    /**
     * Get the host where this service instance is running.
     *
     * @return The host name or IP address
     */
    public String getHost() {
        return host;
    }

    /**
     * Get the port on which this service instance is listening.
     *
     * @return The port number
     */
    public int getPort() {
        return port;
    }

    /**
     * Check if this service instance uses secure communication (HTTPS/TLS).
     *
     * @return true if secure, false otherwise
     */
    public boolean isSecure() {
        return secure;
    }

    /**
     * Get the URI for this service instance.
     *
     * @return The URI in the format http(s)://host:port
     */
    public String getUri() {
        return (secure ? "https://" : "http://") + host + ":" + port;
    }

    /**
     * Get the metadata associated with this service instance.
     *
     * @return An unmodifiable map of metadata key-value pairs
     */
    public Map<String, String> getMetadata() {
        return metadata;
    }

    /**
     * Get the current health status of this service instance.
     *
     * @return The health status
     */
    public HealthStatus getHealthStatus() {
        return healthStatus;
    }

    /**
     * Update the health status of this service instance.
     *
     * @param healthStatus The new health status
     */
    public void setHealthStatus(HealthStatus healthStatus) {
        this.healthStatus = healthStatus;
    }

    /**
     * Check if this service instance is healthy and can receive traffic.
     *
     * @return true if the instance is healthy, false otherwise
     */
    public boolean isHealthy() {
        return healthStatus == HealthStatus.UP || healthStatus == HealthStatus.DEGRADED;
    }

    /**
     * Builder for creating ServiceInstance objects.
     */
    public static class Builder {
        private String instanceId;
        private String serviceId;
        private String host;
        private int port;
        private boolean secure;
        private Map<String, String> metadata = new HashMap<>();
        private HealthStatus healthStatus;

        /**
         * Set the instance ID for the service instance.
         *
         * @param instanceId The unique instance identifier
         * @return This builder for method chaining
         */
        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        /**
         * Set the service ID (service name) for the service instance.
         *
         * @param serviceId The service identifier
         * @return This builder for method chaining
         */
        public Builder serviceId(String serviceId) {
            this.serviceId = serviceId;
            return this;
        }

        /**
         * Set the host for the service instance.
         *
         * @param host The host name or IP address
         * @return This builder for method chaining
         */
        public Builder host(String host) {
            this.host = host;
            return this;
        }

        /**
         * Set the port for the service instance.
         *
         * @param port The port number
         * @return This builder for method chaining
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Set whether the service instance uses secure communication.
         *
         * @param secure true if secure (HTTPS/TLS), false otherwise
         * @return This builder for method chaining
         */
        public Builder secure(boolean secure) {
            this.secure = secure;
            return this;
        }

        /**
         * Add a metadata entry to the service instance.
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
         * Set all metadata for the service instance.
         *
         * @param metadata A map of metadata key-value pairs
         * @return This builder for method chaining
         */
        public Builder metadata(Map<String, String> metadata) {
            this.metadata = new HashMap<>(metadata);
            return this;
        }

        /**
         * Set the initial health status for the service instance.
         *
         * @param healthStatus The health status
         * @return This builder for method chaining
         */
        public Builder healthStatus(HealthStatus healthStatus) {
            this.healthStatus = healthStatus;
            return this;
        }

        /**
         * Build a new ServiceInstance with the configured properties.
         *
         * @return A new ServiceInstance
         */
        public ServiceInstance build() {
            return new ServiceInstance(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceInstance that = (ServiceInstance) o;
        return Objects.equals(instanceId, that.instanceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId);
    }

    @Override
    public String toString() {
        return "ServiceInstance{" +
                "instanceId='" + instanceId + '\'' +
                ", serviceId='" + serviceId + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", secure=" + secure +
                ", healthStatus=" + healthStatus +
                ", metadata=" + metadata +
                '}';
    }
}