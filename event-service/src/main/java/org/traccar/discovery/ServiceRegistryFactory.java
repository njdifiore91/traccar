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

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class for creating ServiceRegistry instances based on configuration for the Event Processing Service.
 * This factory determines whether to use Consul or Kubernetes for service registry based on the configuration,
 * and creates the appropriate implementation. It ensures that only one instance of ServiceRegistry is created
 * and used throughout the Event Processing Service.
 */
@Singleton
public class ServiceRegistryFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistryFactory.class);
    private static final String REGISTRY_TYPE_CONSUL = "consul";
    private static final String REGISTRY_TYPE_KUBERNETES = "kubernetes";

    private final ServiceRegistryConfig config;
    private ServiceRegistry serviceRegistry;

    /**
     * Constructs a new ServiceRegistryFactory with the specified configuration.
     * The factory uses this configuration to determine which type of service registry to create
     * and how to configure it for the Event Processing Service.
     *
     * @param config The service registry configuration containing registry type and connection details
     */
    @Inject
    public ServiceRegistryFactory(ServiceRegistryConfig config) {
        this.config = config;
    }

    /**
     * Creates and returns a ServiceRegistry instance based on the configuration.
     * If a registry has already been created, returns the existing instance to ensure
     * consistent registry access throughout the Event Processing Service.
     *
     * @return The ServiceRegistry instance (either ConsulServiceRegistry or KubernetesServiceRegistry)
     */
    public synchronized ServiceRegistry getServiceRegistry() {
        if (serviceRegistry == null) {
            serviceRegistry = createServiceRegistry();
        }
        return serviceRegistry;
    }

    /**
     * Creates a new ServiceRegistry instance based on the configuration.
     * Supports both Consul and Kubernetes registry types, with fallback to Consul
     * if the specified type is unknown or not supported.
     *
     * @return The created ServiceRegistry instance configured for the Event Processing Service
     */
    private ServiceRegistry createServiceRegistry() {
        String registryType = config.getRegistryType();
        
        LOGGER.info("Creating service registry of type: {} for Event Processing Service", registryType);
        
        switch (registryType.toLowerCase()) {
            case REGISTRY_TYPE_CONSUL:
                LOGGER.debug("Initializing Consul service registry with host: {}, port: {}", 
                        config.getConsulHost(), config.getConsulPort());
                return new ConsulServiceRegistry(config);
                
            case REGISTRY_TYPE_KUBERNETES:
                LOGGER.debug("Initializing Kubernetes service registry with namespace: {}", 
                        config.getKubernetesNamespace());
                return new KubernetesServiceRegistry(config);
                
            default:
                LOGGER.warn("Unknown registry type: {}, falling back to Consul for Event Processing Service", 
                        registryType);
                return new ConsulServiceRegistry(config);
        }
    }
}