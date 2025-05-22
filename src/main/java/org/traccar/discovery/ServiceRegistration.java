/*
 * Copyright 2022 - 2023 Anton Tananaev (anton@traccar.org)
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

import java.util.Map;

/**
 * Represents a service registration with a service discovery system.
 */
public class ServiceRegistration {

    private final String id;
    private final String name;
    private final String host;
    private final int port;
    private final Map<String, String> metadata;

    /**
     * Constructs a new ServiceRegistration.
     *
     * @param id The service ID
     * @param name The service name
     * @param host The service host
     * @param port The service port
     * @param metadata Additional metadata for the service
     */
    public ServiceRegistration(String id, String name, String host, int port, Map<String, String> metadata) {
        this.id = id;
        this.name = name;
        this.host = host;
        this.port = port;
        this.metadata = metadata;
    }

    /**
     * Get the service ID.
     *
     * @return The service ID
     */
    public String getId() {
        return id;
    }

    /**
     * Get the service name.
     *
     * @return The service name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the service host.
     *
     * @return The service host
     */
    public String getHost() {
        return host;
    }

    /**
     * Get the service port.
     *
     * @return The service port
     */
    public int getPort() {
        return port;
    }

    /**
     * Get the service metadata.
     *
     * @return The service metadata
     */
    public Map<String, String> getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "ServiceRegistration{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", metadata=" + metadata +
                '}';
    }
}