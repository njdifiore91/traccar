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
package org.traccar.geolocation;

import java.util.Map;

/**
 * Interface for service discovery operations in a microservices architecture.
 * Provides methods for registering, discovering, and managing services.
 */
public interface ServiceDiscoveryManager {

    /**
     * Registers a service with the service discovery system.
     *
     * @param serviceName Name of the service to register
     * @param servicePort Port the service is running on
     * @return true if registration was successful, false otherwise
     */
    boolean registerService(String serviceName, int servicePort);

    /**
     * Registers a service with the service discovery system with additional metadata.
     *
     * @param serviceName Name of the service to register
     * @param servicePort Port the service is running on
     * @param metadata Additional metadata for the service
     * @return true if registration was successful, false otherwise
     */
    boolean registerService(String serviceName, int servicePort, Map<String, String> metadata);

    /**
     * Discovers a service by name.
     *
     * @param serviceName Name of the service to discover
     * @return Service endpoint URL or null if not found
     */
    String discoverService(String serviceName);

    /**
     * Updates the health status of a registered service.
     *
     * @param serviceName Name of the service to update
     * @param isHealthy true if the service is healthy, false otherwise
     * @return true if the update was successful, false otherwise
     */
    boolean updateServiceHealth(String serviceName, boolean isHealthy);

    /**
     * Deregisters a service from the service discovery system.
     *
     * @param serviceName Name of the service to deregister
     * @return true if deregistration was successful, false otherwise
     */
    boolean deregisterService(String serviceName);
}