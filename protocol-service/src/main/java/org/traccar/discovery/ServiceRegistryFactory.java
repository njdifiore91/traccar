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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class for creating ServiceRegistry instances based on configuration.
 * This factory determines whether to use Consul or Kubernetes for service registry
 * based on the configuration, and creates the appropriate implementation.
 */
public class ServiceRegistryFactory {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistryFactory.class);
    
    // Registry type constants
    public static final String REGISTRY_TYPE_CONSUL = "consul";
    public static final String REGISTRY_TYPE_KUBERNETES = "kubernetes";
    public static final String DEFAULT_REGISTRY_TYPE = REGISTRY_TYPE_CONSUL;
    
    // Singleton instances for each registry type
    private static ServiceRegistry consulRegistry;
    private static ServiceRegistry kubernetesRegistry;
    
    private ServiceRegistryFactory() {
        // Private constructor to prevent instantiation
    }
    
    /**
     * Creates and returns a ServiceRegistry instance based on the provided configuration.
     * If a registry of the specified type already exists, returns the existing instance.
     *
     * @param config The service registry configuration
     * @return The appropriate ServiceRegistry implementation
     */
    public static ServiceRegistry create(ServiceRegistryConfig config) {
        String registryType = config.getRegistryType();
        
        if (registryType == null || registryType.isEmpty()) {
            LOGGER.info("Registry type not specified, using default: {}", DEFAULT_REGISTRY_TYPE);
            registryType = DEFAULT_REGISTRY_TYPE;
        }
        
        LOGGER.debug("Creating service registry of type: {}", registryType);
        
        switch (registryType.toLowerCase()) {
            case REGISTRY_TYPE_CONSUL:
                return getConsulRegistry(config);
            case REGISTRY_TYPE_KUBERNETES:
                return getKubernetesRegistry(config);
            default:
                LOGGER.warn("Unknown registry type: {}, falling back to default: {}", 
                        registryType, DEFAULT_REGISTRY_TYPE);
                return getConsulRegistry(config);
        }
    }
    
    /**
     * Gets or creates a ConsulServiceRegistry instance.
     *
     * @param config The service registry configuration
     * @return The ConsulServiceRegistry instance
     */
    private static synchronized ServiceRegistry getConsulRegistry(ServiceRegistryConfig config) {
        if (consulRegistry == null) {
            LOGGER.info("Initializing Consul service registry");
            consulRegistry = new ConsulServiceRegistry(config);
        }
        return consulRegistry;
    }
    
    /**
     * Gets or creates a KubernetesServiceRegistry instance.
     *
     * @param config The service registry configuration
     * @return The KubernetesServiceRegistry instance
     */
    private static synchronized ServiceRegistry getKubernetesRegistry(ServiceRegistryConfig config) {
        if (kubernetesRegistry == null) {
            LOGGER.info("Initializing Kubernetes service registry");
            kubernetesRegistry = new KubernetesServiceRegistry(config);
        }
        return kubernetesRegistry;
    }
    
    /**
     * Resets all registry instances. Primarily used for testing.
     */
    public static synchronized void reset() {
        consulRegistry = null;
        kubernetesRegistry = null;
        LOGGER.debug("Service registry instances have been reset");
    }
}