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

import java.util.HashMap;
import java.util.Map;

/**
 * Model class representing a service instance with its metadata, including service ID,
 * host, port, health status, and additional attributes. This class is used by the
 * service registry to track registered services and their current state, enabling
 * service discovery and load balancing.
 */
public class ServiceInstance {

    private String serviceId;
    private String host;
    private int port;
    private boolean secure;
    private boolean healthy;
    private Map<String, String> metadata;

    /**
     * Default constructor.
     */
    public ServiceInstance() {
        this.metadata = new HashMap<>();
        this.healthy = true; // Default to healthy
    }

    /**
     * Constructor with essential service information.
     *
     * @param serviceId The unique identifier for the service
     * @param host      The hostname or IP address of the service
     * @param port      The port number the service is listening on
     */
    public ServiceInstance(String serviceId, String host, int port) {
        this();
        this.serviceId = serviceId;
        this.host = host;
        this.port = port;
    }

    /**
     * Gets the service ID.
     *
     * @return The service ID
     */
    public String getServiceId() {
        return serviceId;
    }

    /**
     * Sets the service ID.
     *
     * @param serviceId The service ID to set
     */
    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    /**
     * Gets the host (hostname or IP address).
     *
     * @return The host
     */
    public String getHost() {
        return host;
    }

    /**
     * Sets the host (hostname or IP address).
     *
     * @param host The host to set
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Gets the port number.
     *
     * @return The port number
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port number.
     *
     * @param port The port number to set
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Checks if the service uses a secure connection (HTTPS).
     *
     * @return true if secure, false otherwise
     */
    public boolean isSecure() {
        return secure;
    }

    /**
     * Sets whether the service uses a secure connection (HTTPS).
     *
     * @param secure true for secure, false otherwise
     */
    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    /**
     * Checks if the service is healthy.
     *
     * @return true if healthy, false otherwise
     */
    public boolean isHealthy() {
        return healthy;
    }

    /**
     * Sets the health status of the service.
     *
     * @param healthy true for healthy, false otherwise
     */
    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }

    /**
     * Gets the metadata associated with the service.
     *
     * @return The metadata map
     */
    public Map<String, String> getMetadata() {
        return metadata;
    }

    /**
     * Sets the metadata associated with the service.
     *
     * @param metadata The metadata map to set
     */
    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata != null ? metadata : new HashMap<>();
    }

    /**
     * Adds a metadata entry.
     *
     * @param key   The metadata key
     * @param value The metadata value
     */
    public void addMetadata(String key, String value) {
        if (this.metadata == null) {
            this.metadata = new HashMap<>();
        }
        this.metadata.put(key, value);
    }

    @Override
    public String toString() {
        return "ServiceInstance{" +
                "serviceId='" + serviceId + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", secure=" + secure +
                ", healthy=" + healthy +
                ", metadata=" + metadata +
                '}';
    }
}