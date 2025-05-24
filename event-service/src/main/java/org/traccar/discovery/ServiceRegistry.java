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
 * This interface is essential for enabling the Event Processing Service to participate
 * in the microservices ecosystem and allow other services to locate it dynamically.
 */
public interface ServiceRegistry {

    /**
     * Represents a service instance with its metadata.
     */
    interface ServiceInstance {
        /**
         * Gets the unique identifier of the service instance.
         *
         * @return the service instance ID
         */
        String getInstanceId();

        /**
         * Gets the service name.
         *
         * @return the service name
         */
        String getServiceName();

        /**
         * Gets the host where the service is running.
         *
         * @return the host address
         */
        String getHost();

        /**
         * Gets the port on which the service is listening.
         *
         * @return the port number
         */
        int getPort();

        /**
         * Gets the service metadata.
         *
         * @return a map of metadata key-value pairs
         */
        Map<String, String> getMetadata();

        /**
         * Checks if the service instance is healthy.
         *
         * @return true if the service is healthy, false otherwise
         */
        boolean isHealthy();
    }

    /**
     * Registers the current service with the service registry.
     *
     * @param serviceName the name of the service
     * @param host        the host where the service is running
     * @param port        the port on which the service is listening
     * @param metadata    additional metadata for the service
     * @return the registered service instance
     * @throws ServiceRegistryException if registration fails
     */
    ServiceInstance register(String serviceName, String host, int port, Map<String, String> metadata)
            throws ServiceRegistryException;

    /**
     * Registers the current service with the service registry and configures a health check.
     *
     * @param serviceName       the name of the service
     * @param host              the host where the service is running
     * @param port              the port on which the service is listening
     * @param healthCheckPath   the path for the health check endpoint (e.g., "/actuator/health")
     * @param healthCheckInterval the interval in seconds between health checks
     * @param metadata          additional metadata for the service
     * @return the registered service instance
     * @throws ServiceRegistryException if registration fails
     */
    ServiceInstance registerWithHealthCheck(String serviceName, String host, int port,
                                           String healthCheckPath, int healthCheckInterval,
                                           Map<String, String> metadata)
            throws ServiceRegistryException;

    /**
     * Deregisters the service from the service registry.
     *
     * @param instanceId the ID of the service instance to deregister
     * @throws ServiceRegistryException if deregistration fails
     */
    void deregister(String instanceId) throws ServiceRegistryException;

    /**
     * Gets all instances of a specific service.
     *
     * @param serviceName the name of the service to look up
     * @return a list of service instances
     * @throws ServiceRegistryException if the lookup fails
     */
    List<ServiceInstance> getServiceInstances(String serviceName) throws ServiceRegistryException;

    /**
     * Gets all healthy instances of a specific service.
     *
     * @param serviceName the name of the service to look up
     * @return a list of healthy service instances
     * @throws ServiceRegistryException if the lookup fails
     */
    List<ServiceInstance> getHealthyServiceInstances(String serviceName) throws ServiceRegistryException;

    /**
     * Gets a service instance by its ID.
     *
     * @param instanceId the ID of the service instance to look up
     * @return the service instance, or null if not found
     * @throws ServiceRegistryException if the lookup fails
     */
    ServiceInstance getServiceInstance(String instanceId) throws ServiceRegistryException;

    /**
     * Updates the metadata for a service instance.
     *
     * @param instanceId the ID of the service instance to update
     * @param metadata   the new metadata to set
     * @throws ServiceRegistryException if the update fails
     */
    void updateMetadata(String instanceId, Map<String, String> metadata) throws ServiceRegistryException;

    /**
     * Updates the health status of a service instance.
     *
     * @param instanceId the ID of the service instance to update
     * @param isHealthy  the new health status
     * @throws ServiceRegistryException if the update fails
     */
    void updateHealthStatus(String instanceId, boolean isHealthy) throws ServiceRegistryException;

    /**
     * Registers an event-specific health check for the service.
     * This allows the service registry to monitor event processing capabilities.
     *
     * @param instanceId         the ID of the service instance
     * @param eventProcessingPath the path for the event processing health check endpoint
     * @param checkInterval      the interval in seconds between health checks
     * @throws ServiceRegistryException if the registration fails
     */
    void registerEventProcessingHealthCheck(String instanceId, String eventProcessingPath, int checkInterval)
            throws ServiceRegistryException;

    /**
     * Exception thrown when service registry operations fail.
     */
    class ServiceRegistryException extends Exception {
        public ServiceRegistryException(String message) {
            super(message);
        }

        public ServiceRegistryException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}