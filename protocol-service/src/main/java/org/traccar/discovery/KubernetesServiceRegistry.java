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

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1DeleteOptions;
import io.kubernetes.client.openapi.models.V1Endpoints;
import io.kubernetes.client.openapi.models.V1EndpointsPort;
import io.kubernetes.client.openapi.models.V1EndpointsBuilder;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceBuilder;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.util.Config;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of the ServiceRegistry interface for Kubernetes.
 * It leverages the Kubernetes API for service registration and discovery,
 * using Kubernetes Services and Endpoints. This implementation is used when
 * the Protocol Service is deployed in a Kubernetes environment.
 */
@Singleton
public class KubernetesServiceRegistry implements ServiceRegistry {

    private static final Logger LOGGER = Logger.getLogger(KubernetesServiceRegistry.class.getName());

    private final CoreV1Api api;
    private final String namespace;
    private final HealthCheck healthCheck;
    private final Map<String, ServiceInstance> registeredServices;

    /**
     * Constructs a new KubernetesServiceRegistry with the specified configuration and health check.
     *
     * @param config      The service registry configuration
     * @param healthCheck The health check implementation to use for service health status
     * @throws IOException If there is an error initializing the Kubernetes client
     */
    @Inject
    public KubernetesServiceRegistry(ServiceRegistryConfig config, HealthCheck healthCheck) throws IOException {
        this.healthCheck = healthCheck;
        this.registeredServices = new ConcurrentHashMap<>();

        // Initialize Kubernetes client
        ApiClient client = Config.defaultClient();
        Configuration.setDefaultApiClient(client);
        this.api = new CoreV1Api();

        // Get namespace from environment or config
        String ns = System.getenv("KUBERNETES_NAMESPACE");
        if (ns == null || ns.isEmpty()) {
            ns = System.getenv("POD_NAMESPACE");
        }
        if (ns == null || ns.isEmpty()) {
            ns = config.getNamespace();
        }
        if (ns == null || ns.isEmpty()) {
            // Default to "default" namespace if not specified
            ns = "default";
        }
        this.namespace = ns;

        LOGGER.info("Initialized Kubernetes Service Registry in namespace: " + namespace);
    }

    @Override
    public void register(ServiceInstance serviceInstance) {
        try {
            // Create or update Kubernetes Service
            createOrUpdateService(serviceInstance);

            // Create or update Kubernetes Endpoints
            createOrUpdateEndpoints(serviceInstance);

            // Store the registered service
            registeredServices.put(serviceInstance.getServiceId(), serviceInstance);

            LOGGER.info("Registered service: " + serviceInstance.getServiceId());
        } catch (ApiException e) {
            LOGGER.log(Level.SEVERE, "Failed to register service: " + serviceInstance.getServiceId(), e);
            throw new RuntimeException("Failed to register service", e);
        }
    }

    @Override
    public void deregister(String serviceId) {
        try {
            // Delete Kubernetes Service
            api.deleteNamespacedService(
                    serviceId,
                    namespace,
                    null,
                    null,
                    null,
                    null,
                    null,
                    new V1DeleteOptions());

            // Delete Kubernetes Endpoints
            api.deleteNamespacedEndpoints(
                    serviceId,
                    namespace,
                    null,
                    null,
                    null,
                    null,
                    null,
                    new V1DeleteOptions());

            // Remove from registered services
            registeredServices.remove(serviceId);

            LOGGER.info("Deregistered service: " + serviceId);
        } catch (ApiException e) {
            LOGGER.log(Level.SEVERE, "Failed to deregister service: " + serviceId, e);
            throw new RuntimeException("Failed to deregister service", e);
        }
    }

    @Override
    public void updateStatus(String serviceId, boolean status) {
        ServiceInstance serviceInstance = registeredServices.get(serviceId);
        if (serviceInstance != null) {
            try {
                // Update the service instance status
                serviceInstance.setHealthy(status);

                // Update Kubernetes Endpoints to reflect health status
                if (status) {
                    // If healthy, ensure endpoints are available
                    createOrUpdateEndpoints(serviceInstance);
                } else {
                    // If unhealthy, remove endpoints
                    removeEndpoints(serviceId);
                }

                LOGGER.info("Updated service status: " + serviceId + " to " + (status ? "healthy" : "unhealthy"));
            } catch (ApiException e) {
                LOGGER.log(Level.SEVERE, "Failed to update service status: " + serviceId, e);
                throw new RuntimeException("Failed to update service status", e);
            }
        } else {
            LOGGER.warning("Attempted to update status for unregistered service: " + serviceId);
        }
    }

    @Override
    public List<ServiceInstance> getServiceInstances(String serviceName) {
        try {
            // Get service from Kubernetes API
            V1Service service = api.readNamespacedService(serviceName, namespace, null);
            if (service == null) {
                return Collections.emptyList();
            }

            // Get endpoints from Kubernetes API
            V1Endpoints endpoints = api.readNamespacedEndpoints(serviceName, namespace, null);
            if (endpoints == null || endpoints.getSubsets() == null || endpoints.getSubsets().isEmpty()) {
                return Collections.emptyList();
            }

            // Convert to ServiceInstance objects
            List<ServiceInstance> instances = new ArrayList<>();
            endpoints.getSubsets().forEach(subset -> {
                if (subset.getAddresses() != null) {
                    subset.getAddresses().forEach(address -> {
                        ServiceInstance instance = new ServiceInstance();
                        instance.setServiceId(serviceName);
                        instance.setHost(address.getIp());
                        
                        // Get the first port from the subset
                        if (subset.getPorts() != null && !subset.getPorts().isEmpty()) {
                            instance.setPort(subset.getPorts().get(0).getPort());
                        }
                        
                        // Set metadata from service labels
                        if (service.getMetadata() != null && service.getMetadata().getLabels() != null) {
                            instance.setMetadata(new HashMap<>(service.getMetadata().getLabels()));
                        }
                        
                        // Set as healthy since it's in the endpoints list
                        instance.setHealthy(true);
                        
                        instances.add(instance);
                    });
                }
            });

            return instances;
        } catch (ApiException e) {
            LOGGER.log(Level.SEVERE, "Failed to get service instances: " + serviceName, e);
            return Collections.emptyList();
        }
    }

    @Override
    public void close() {
        // Deregister all services on shutdown
        new ArrayList<>(registeredServices.keySet()).forEach(this::deregister);
        LOGGER.info("Closed Kubernetes Service Registry");
    }

    /**
     * Creates or updates a Kubernetes Service for the given service instance.
     *
     * @param serviceInstance The service instance to create or update
     * @throws ApiException If there is an error communicating with the Kubernetes API
     */
    private void createOrUpdateService(ServiceInstance serviceInstance) throws ApiException {
        String serviceName = serviceInstance.getServiceId();
        
        // Prepare service ports
        List<V1ServicePort> ports = new ArrayList<>();
        V1ServicePort port = new V1ServicePort()
                .name("http")
                .port(serviceInstance.getPort())
                .targetPort(serviceInstance.getPort());
        ports.add(port);
        
        // Prepare service metadata
        Map<String, String> labels = new HashMap<>();
        labels.put("app", serviceName);
        labels.put("managed-by", "traccar-protocol-service");
        
        // Add custom metadata if available
        if (serviceInstance.getMetadata() != null) {
            labels.putAll(serviceInstance.getMetadata());
        }
        
        // Create service object
        V1Service service = new V1ServiceBuilder()
                .withNewMetadata()
                    .withName(serviceName)
                    .withNamespace(namespace)
                    .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                    .withSelector(Collections.singletonMap("app", serviceName))
                    .withPorts(ports)
                    .withType("ClusterIP")
                .endSpec()
                .build();
        
        try {
            // Try to get existing service
            api.readNamespacedService(serviceName, namespace, null);
            
            // Service exists, update it
            api.replaceNamespacedService(serviceName, namespace, service, null, null, null);
            LOGGER.info("Updated Kubernetes Service: " + serviceName);
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                // Service doesn't exist, create it
                api.createNamespacedService(namespace, service, null, null, null);
                LOGGER.info("Created Kubernetes Service: " + serviceName);
            } else {
                // Other API error
                throw e;
            }
        }
    }

    /**
     * Creates or updates Kubernetes Endpoints for the given service instance.
     *
     * @param serviceInstance The service instance to create or update endpoints for
     * @throws ApiException If there is an error communicating with the Kubernetes API
     */
    private void createOrUpdateEndpoints(ServiceInstance serviceInstance) throws ApiException {
        String serviceName = serviceInstance.getServiceId();
        
        // Only create endpoints if the service is healthy
        if (!serviceInstance.isHealthy() && healthCheck != null && !healthCheck.isHealthy()) {
            removeEndpoints(serviceName);
            return;
        }
        
        // Prepare endpoint ports
        List<V1EndpointsPort> ports = new ArrayList<>();
        V1EndpointsPort port = new V1EndpointsPort()
                .name("http")
                .port(serviceInstance.getPort());
        ports.add(port);
        
        // Create endpoints object
        V1Endpoints endpoints = new V1EndpointsBuilder()
                .withNewMetadata()
                    .withName(serviceName)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSubset()
                    .withNewAddresses()
                        .withNewIp(serviceInstance.getHost())
                    .endAddresses()
                    .withPorts(ports)
                .endSubset()
                .build();
        
        try {
            // Try to get existing endpoints
            api.readNamespacedEndpoints(serviceName, namespace, null);
            
            // Endpoints exist, update them
            api.replaceNamespacedEndpoints(serviceName, namespace, endpoints, null, null, null);
            LOGGER.info("Updated Kubernetes Endpoints: " + serviceName);
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                // Endpoints don't exist, create them
                api.createNamespacedEndpoints(namespace, endpoints, null, null, null);
                LOGGER.info("Created Kubernetes Endpoints: " + serviceName);
            } else {
                // Other API error
                throw e;
            }
        }
    }

    /**
     * Removes Kubernetes Endpoints for the given service ID.
     *
     * @param serviceId The service ID to remove endpoints for
     * @throws ApiException If there is an error communicating with the Kubernetes API
     */
    private void removeEndpoints(String serviceId) throws ApiException {
        try {
            // Create empty endpoints (no addresses)
            V1Endpoints endpoints = new V1Endpoints();
            V1ObjectMeta metadata = new V1ObjectMeta();
            metadata.setName(serviceId);
            metadata.setNamespace(namespace);
            endpoints.setMetadata(metadata);
            
            // Update endpoints to have no addresses
            api.replaceNamespacedEndpoints(serviceId, namespace, endpoints, null, null, null);
            LOGGER.info("Removed addresses from Kubernetes Endpoints: " + serviceId);
        } catch (ApiException e) {
            if (e.getCode() != 404) {
                // Only throw if it's not a 404 (not found) error
                throw e;
            }
        }
    }
}