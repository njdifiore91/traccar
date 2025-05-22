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
package org.traccar.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.discovery.ServiceInstance;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Provides service discovery capabilities for the API Gateway to locate and route requests
 * to appropriate backend microservices. It integrates with Consul or Kubernetes service registry,
 * dynamically resolves service endpoints, and supports load balancing across service instances.
 */
@Singleton
public class ServiceDiscovery {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscovery.class);

    private final org.traccar.discovery.ServiceDiscovery serviceDiscovery;
    private final Map<String, AtomicInteger> serviceCounters = new ConcurrentHashMap<>();

    /**
     * Constructs a new ServiceDiscovery instance.
     *
     * @param serviceDiscovery The underlying service discovery implementation (Consul or Kubernetes)
     */
    @Inject
    public ServiceDiscovery(org.traccar.discovery.ServiceDiscovery serviceDiscovery) {
        this.serviceDiscovery = serviceDiscovery;
        LOGGER.info("Service discovery initialized with {}", serviceDiscovery.getClass().getSimpleName());
    }

    /**
     * Discovers all instances of a service by name.
     *
     * @param serviceName The name of the service to discover
     * @return A list of service instances
     */
    public List<ServiceInstance> getInstances(String serviceName) {
        List<ServiceInstance> instances = serviceDiscovery.getInstances(serviceName);
        LOGGER.debug("Discovered {} instances of service {}", instances.size(), serviceName);
        return instances;
    }

    /**
     * Gets a service instance using round-robin load balancing.
     *
     * @param serviceName The name of the service to discover
     * @return A service instance or null if none available
     */
    public ServiceInstance getInstance(String serviceName) {
        List<ServiceInstance> instances = getHealthyInstances(serviceName);
        if (instances.isEmpty()) {
            LOGGER.warn("No healthy instances found for service {}", serviceName);
            return null;
        }

        // Use round-robin load balancing
        AtomicInteger counter = serviceCounters.computeIfAbsent(serviceName, k -> new AtomicInteger(0));
        int index = counter.getAndIncrement() % instances.size();
        if (counter.get() > 10000) { // Reset counter to avoid overflow
            counter.set(0);
        }

        ServiceInstance instance = instances.get(index);
        LOGGER.debug("Selected instance {} for service {}", instance.getInstanceId(), serviceName);
        return instance;
    }

    /**
     * Gets a service instance with a specific instance ID.
     *
     * @param serviceName The name of the service
     * @param instanceId The specific instance ID to find
     * @return The service instance or null if not found
     */
    public ServiceInstance getInstance(String serviceName, String instanceId) {
        List<ServiceInstance> instances = getInstances(serviceName);
        return instances.stream()
                .filter(instance -> instance.getInstanceId().equals(instanceId))
                .findFirst()
                .orElse(null);
    }

    /**
     * Gets only healthy instances of a service.
     *
     * @param serviceName The name of the service
     * @return A list of healthy service instances
     */
    public List<ServiceInstance> getHealthyInstances(String serviceName) {
        List<ServiceInstance> allInstances = getInstances(serviceName);
        List<ServiceInstance> healthyInstances = allInstances.stream()
                .filter(this::isHealthy)
                .collect(Collectors.toList());
        
        LOGGER.debug("Found {}/{} healthy instances for service {}", 
                healthyInstances.size(), allInstances.size(), serviceName);
        return healthyInstances;
    }

    /**
     * Checks if a service instance is healthy based on its metadata.
     *
     * @param instance The service instance to check
     * @return true if the instance is healthy, false otherwise
     */
    private boolean isHealthy(ServiceInstance instance) {
        Map<String, String> metadata = instance.getMetadata();
        
        // Check for explicit health status in metadata
        if (metadata.containsKey("health")) {
            return "UP".equalsIgnoreCase(metadata.get("health"));
        }
        
        // Check for Kubernetes health status
        if (metadata.containsKey("kubernetes.io/health")) {
            return "healthy".equalsIgnoreCase(metadata.get("kubernetes.io/health"));
        }
        
        // Check for Consul health status
        if (metadata.containsKey("consul.health")) {
            return "passing".equalsIgnoreCase(metadata.get("consul.health"));
        }
        
        // Default to true if no health indicators are present
        return true;
    }

    /**
     * Gets the URL for a service instance.
     *
     * @param serviceName The name of the service
     * @return The URL of a service instance or null if none available
     */
    public String getServiceUrl(String serviceName) {
        ServiceInstance instance = getInstance(serviceName);
        if (instance == null) {
            return null;
        }
        return instance.getUri().toString();
    }

    /**
     * Gets the URL for a specific path on a service.
     *
     * @param serviceName The name of the service
     * @param path The path to append to the service URL
     * @return The complete URL including the path or null if no service instance available
     */
    public String getServiceUrl(String serviceName, String path) {
        String serviceUrl = getServiceUrl(serviceName);
        if (serviceUrl == null) {
            return null;
        }
        return serviceUrl + (path.startsWith("/") ? path : "/" + path);
    }

    /**
     * Checks if a service exists in the registry.
     *
     * @param serviceName The name of the service to check
     * @return true if the service exists, false otherwise
     */
    public boolean serviceExists(String serviceName) {
        return !getInstances(serviceName).isEmpty();
    }
}