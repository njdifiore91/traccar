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

import java.io.Closeable;
import java.util.List;

/**
 * Core interface that defines the contract for service registration and discovery.
 * It provides methods for registering and deregistering services with the service registry
 * (Consul or Kubernetes), as well as discovering other services. This is essential for
 * enabling the Protocol Service to participate in the microservices ecosystem.
 */
public interface ServiceRegistry extends Closeable {

    /**
     * Registers a service instance with the service registry.
     *
     * @param serviceInstance The service instance to register
     */
    void register(ServiceInstance serviceInstance);

    /**
     * Deregisters a service from the service registry.
     *
     * @param serviceId The ID of the service to deregister
     */
    void deregister(String serviceId);

    /**
     * Updates the health status of a registered service.
     *
     * @param serviceId The ID of the service to update
     * @param status    The health status (true for healthy, false for unhealthy)
     */
    void updateStatus(String serviceId, boolean status);

    /**
     * Gets a list of service instances for a given service name.
     *
     * @param serviceName The name of the service to look up
     * @return A list of service instances
     */
    List<ServiceInstance> getServiceInstances(String serviceName);

    /**
     * Closes the service registry, releasing any resources and deregistering services.
     */
    @Override
    void close();
}