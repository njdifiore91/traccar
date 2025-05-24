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

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.util.Config;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the ServiceRegistry interface for Kubernetes.
 * This class provides functionality to register the event service with Kubernetes,
 * update its health status, and deregister it when shutting down.
 */
@Singleton
public class KubernetesServiceRegistry implements ServiceRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesServiceRegistry.class);

    private static final String SERVICE_NAME = "event-service";
    private static final String SERVICE_TYPE = "ClusterIP";
    private static final int SERVICE_PORT = 8080;
    private static final String SERVICE_PROTOCOL = "TCP";

    private final CoreV1Api api;
    private final String namespace;
    private final Map<String, String> serviceLabels;
    private final Map<String, String> serviceAnnotations;
    private boolean registered;

    /**
     * Constructs a new KubernetesServiceRegistry.
     * Initializes the Kubernetes API client and sets up the service labels and annotations.
     */
    @Inject
    public KubernetesServiceRegistry() {
        try {
            // Initialize Kubernetes client
            ApiClient client = Config.defaultClient();
            Configuration.setDefaultApiClient(client);
            api = new CoreV1Api();
            
            // Get namespace from environment or default to "default"
            namespace = System.getenv("KUBERNETES_NAMESPACE") != null 
                ? System.getenv("KUBERNETES_NAMESPACE") 
                : "default";
            
            // Initialize service labels
            serviceLabels = new HashMap<>();
            serviceLabels.put("app", SERVICE_NAME);
            serviceLabels.put("service", "event-processing");
            
            // Initialize service annotations
            serviceAnnotations = new HashMap<>();
            serviceAnnotations.put("prometheus.io/scrape", "true");
            serviceAnnotations.put("prometheus.io/port", String.valueOf(SERVICE_PORT));
            serviceAnnotations.put("prometheus.io/path", "/metrics");
            
            registered = false;
            
            LOGGER.info("Kubernetes Service Registry initialized for namespace: {}", namespace);
        } catch (IOException e) {
            LOGGER.error("Failed to initialize Kubernetes client", e);
            throw new RuntimeException("Failed to initialize Kubernetes client", e);
        }
    }

    /**
     * Registers the event service with Kubernetes.
     * Creates a Service resource in the Kubernetes cluster.
     *
     * @param serviceName The name of the service to register
     * @param host The host address of the service
     * @param port The port number of the service
     * @param metadata Additional metadata for the service
     * @return true if registration was successful, false otherwise
     */
    @Override
    public boolean register(String serviceName, String host, int port, Map<String, String> metadata) {
        if (registered) {
            LOGGER.info("Service already registered: {}", SERVICE_NAME);
            return true;
        }
        
        try {
            // Create service metadata
            V1ObjectMeta objectMeta = new V1ObjectMeta()
                .name(SERVICE_NAME)
                .labels(serviceLabels)
                .annotations(serviceAnnotations);
            
            // Add any additional metadata as annotations
            if (metadata != null && !metadata.isEmpty()) {
                Map<String, String> annotations = objectMeta.getAnnotations();
                annotations.putAll(metadata);
                objectMeta.setAnnotations(annotations);
            }
            
            // Create service port
            V1ServicePort servicePort = new V1ServicePort()
                .name("http")
                .port(SERVICE_PORT)
                .targetPort(new io.kubernetes.client.custom.IntOrString(port))
                .protocol(SERVICE_PROTOCOL);
            
            // Create service spec
            V1ServiceSpec serviceSpec = new V1ServiceSpec()
                .type(SERVICE_TYPE)
                .selector(serviceLabels)
                .ports(Collections.singletonList(servicePort));
            
            // Create service
            V1Service service = new V1Service()
                .apiVersion("v1")
                .kind("Service")
                .metadata(objectMeta)
                .spec(serviceSpec);
            
            // Create or update the service in Kubernetes
            try {
                api.createNamespacedService(namespace, service, null, null, null, null);
                LOGGER.info("Service registered successfully: {}", SERVICE_NAME);
            } catch (ApiException e) {
                if (e.getCode() == 409) { // Conflict - service already exists
                    api.replaceNamespacedService(SERVICE_NAME, namespace, service, null, null, null, null);
                    LOGGER.info("Service updated successfully: {}", SERVICE_NAME);
                } else {
                    throw e;
                }
            }
            
            registered = true;
            return true;
        } catch (ApiException e) {
            LOGGER.error("Failed to register service: {} - {}", SERVICE_NAME, e.getResponseBody(), e);
            return false;
        }
    }

    /**
     * Updates the health status of the service in Kubernetes.
     * This is typically used to update the readiness and liveness status.
     *
     * @param serviceName The name of the service to update
     * @param status The new health status
     * @return true if the update was successful, false otherwise
     */
    @Override
    public boolean updateStatus(String serviceName, String status) {
        if (!registered) {
            LOGGER.warn("Cannot update status for unregistered service: {}", serviceName);
            return false;
        }
        
        try {
            // Get the current service
            V1Service service = api.readNamespacedService(SERVICE_NAME, namespace, null);
            
            // Update annotations with health status
            Map<String, String> annotations = service.getMetadata().getAnnotations();
            if (annotations == null) {
                annotations = new HashMap<>();
                service.getMetadata().setAnnotations(annotations);
            }
            
            annotations.put("health.status", status);
            
            // Update the service in Kubernetes
            api.replaceNamespacedService(SERVICE_NAME, namespace, service, null, null, null, null);
            LOGGER.debug("Service health status updated: {} - {}", SERVICE_NAME, status);
            
            return true;
        } catch (ApiException e) {
            LOGGER.error("Failed to update service status: {} - {}", SERVICE_NAME, e.getResponseBody(), e);
            return false;
        }
    }

    /**
     * Deregisters the service from Kubernetes.
     * Removes the Service resource from the Kubernetes cluster.
     *
     * @param serviceName The name of the service to deregister
     * @return true if deregistration was successful, false otherwise
     */
    @Override
    public boolean deregister(String serviceName) {
        if (!registered) {
            LOGGER.info("Service not registered, nothing to deregister: {}", serviceName);
            return true;
        }
        
        try {
            api.deleteNamespacedService(
                SERVICE_NAME,
                namespace,
                null,
                null,
                null,
                null,
                null,
                null);
            
            LOGGER.info("Service deregistered successfully: {}", SERVICE_NAME);
            registered = false;
            return true;
        } catch (ApiException e) {
            if (e.getCode() == 404) { // Not found - service already deleted
                LOGGER.info("Service already deregistered: {}", SERVICE_NAME);
                registered = false;
                return true;
            }
            LOGGER.error("Failed to deregister service: {} - {}", SERVICE_NAME, e.getResponseBody(), e);
            return false;
        }
    }

    /**
     * Retrieves a list of registered services from Kubernetes.
     * This method is not fully implemented as it's typically handled by the ServiceDiscovery interface.
     *
     * @param serviceName Optional service name filter
     * @return A list of service instances matching the filter
     */
    @Override
    public List<ServiceInstance> getServices(String serviceName) {
        // This method would typically be implemented by the ServiceDiscovery interface,
        // not the ServiceRegistry. Including a basic implementation for completeness.
        LOGGER.warn("getServices called on ServiceRegistry - this is typically handled by ServiceDiscovery");
        return Collections.emptyList();
    }

    /**
     * Checks if a service is registered with Kubernetes.
     *
     * @param serviceName The name of the service to check
     * @return true if the service is registered, false otherwise
     */
    @Override
    public boolean isRegistered(String serviceName) {
        if (!serviceName.equals(SERVICE_NAME)) {
            return false;
        }
        
        if (registered) {
            try {
                api.readNamespacedService(SERVICE_NAME, namespace, null);
                return true;
            } catch (ApiException e) {
                if (e.getCode() == 404) { // Not found
                    registered = false;
                    return false;
                }
                LOGGER.error("Error checking service registration: {} - {}", SERVICE_NAME, e.getResponseBody(), e);
            }
        }
        
        return registered;
    }
}