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

import java.util.List;
import java.util.Map;

/**
 * ServiceRegistry defines the contract for service registration and discovery.
 * It provides methods for registering and deregistering services with the service registry
 * (Consul or Kubernetes), as well as discovering other services.
 * <p>
 * This interface is essential for enabling the Protocol Service to participate in the
 * microservices ecosystem by registering itself and discovering other services it needs
 * to communicate with.
 */
public interface ServiceRegistry {

    /**
     * Registers the current service with the service registry.
     *
     * @param serviceName    The name of the service to register
     * @param serviceId      A unique identifier for this service instance
     * @param host           The hostname or IP address where this service is running
     * @param port           The port on which this service is listening
     * @param metadata       Additional metadata about the service (e.g., protocol information)
     * @return               True if registration was successful, false otherwise
     */
    boolean register(String serviceName, String serviceId, String host, int port, Map<String, String> metadata);

    /**
     * Registers the current service with the service registry using default values for host and port.
     *
     * @param serviceName    The name of the service to register
     * @param serviceId      A unique identifier for this service instance
     * @param metadata       Additional metadata about the service (e.g., protocol information)
     * @return               True if registration was successful, false otherwise
     */
    boolean register(String serviceName, String serviceId, Map<String, String> metadata);

    /**
     * Deregisters the service from the service registry.
     *
     * @param serviceId      The unique identifier of the service instance to deregister
     * @return               True if deregistration was successful, false otherwise
     */
    boolean deregister(String serviceId);

    /**
     * Registers a health check for the service.
     *
     * @param serviceId      The unique identifier of the service instance
     * @param checkId        A unique identifier for this health check
     * @param checkName      A human-readable name for this health check
     * @param checkUrl       The URL to call for HTTP-based health checks
     * @param interval       The interval in seconds between health checks
     * @param timeout        The timeout in seconds for each health check
     * @return               True if health check registration was successful, false otherwise
     */
    boolean registerHealthCheck(String serviceId, String checkId, String checkName, 
                               String checkUrl, int interval, int timeout);

    /**
     * Discovers instances of a service by name.
     *
     * @param serviceName    The name of the service to discover
     * @return               A list of ServiceInstance objects representing available instances of the service
     */
    List<ServiceInstance> getServiceInstances(String serviceName);

    /**
     * Discovers instances of a service by name and filters them by metadata.
     *
     * @param serviceName    The name of the service to discover
     * @param metadata       Metadata key-value pairs to filter service instances
     * @return               A list of ServiceInstance objects that match the metadata criteria
     */
    List<ServiceInstance> getServiceInstances(String serviceName, Map<String, String> metadata);

    /**
     * Updates the service metadata.
     *
     * @param serviceId      The unique identifier of the service instance
     * @param metadata       The new metadata to associate with the service
     * @return               True if metadata update was successful, false otherwise
     */
    boolean updateMetadata(String serviceId, Map<String, String> metadata);

    /**
     * Reports the current health status of the service.
     *
     * @param serviceId      The unique identifier of the service instance
     * @param status         The current health status (e.g., "passing", "warning", "critical")
     * @param output         Additional details about the current health status
     * @return               True if health status update was successful, false otherwise
     */
    boolean reportHealth(String serviceId, String status, String output);

    /**
     * Gets the type of service registry being used (e.g., "consul", "kubernetes").
     *
     * @return               The type of service registry
     */
    String getRegistryType();
}