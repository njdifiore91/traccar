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

import org.traccar.config.Config;
import org.traccar.config.Keys;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service Discovery Manager for registering services with service discovery systems
 * like Consul or Kubernetes.
 */
@Singleton
public class ServiceDiscoveryManager {

    private static final Logger LOGGER = Logger.getLogger(ServiceDiscoveryManager.class.getName());

    private final Config config;
    private final Map<String, ServiceRegistration> registrations = new ConcurrentHashMap<>();
    private final ServiceRegistry serviceRegistry;

    /**
     * Constructs a new ServiceDiscoveryManager.
     *
     * @param config The configuration
     */
    @Inject
    public ServiceDiscoveryManager(Config config) {
        this.config = config;
        
        // Determine which service registry to use based on configuration
        String registryType = config.getString(Keys.SERVICE_DISCOVERY_TYPE);
        if ("consul".equalsIgnoreCase(registryType)) {
            serviceRegistry = new ConsulServiceRegistry(config);
        } else if ("kubernetes".equalsIgnoreCase(registryType)) {
            serviceRegistry = new KubernetesServiceRegistry(config);
        } else {
            // Default to no-op registry if not configured
            serviceRegistry = new NoOpServiceRegistry();
        }
        
        LOGGER.info("Initialized ServiceDiscoveryManager with " + serviceRegistry.getClass().getSimpleName());
    }

    /**
     * Register a service with the service discovery system.
     *
     * @param name The service name
     * @param host The service host
     * @param port The service port
     * @param protocol The service protocol (tcp, udp)
     * @param secure Whether the service is secure (SSL/TLS)
     * @return The service ID
     */
    public String register(String name, String host, int port, String protocol, boolean secure) {
        try {
            // Generate a unique ID for this service instance
            String id = name + "-" + UUID.randomUUID().toString().substring(0, 8);
            
            // Resolve hostname if needed
            String resolvedHost = host;
            if ("0.0.0.0".equals(host)) {
                try {
                    resolvedHost = InetAddress.getLocalHost().getHostAddress();
                } catch (UnknownHostException e) {
                    LOGGER.log(Level.WARNING, "Could not resolve local hostname, using fallback", e);
                    resolvedHost = config.getString(Keys.WEB_ADDRESS, host);
                }
            }
            
            // Create metadata for the service
            Map<String, String> metadata = new HashMap<>();
            metadata.put("protocol", protocol);
            metadata.put("secure", String.valueOf(secure));
            metadata.put("version", config.getString(Keys.VERSION));
            
            // Create service registration
            ServiceRegistration registration = new ServiceRegistration(id, name, resolvedHost, port, metadata);
            
            // Register with the service registry
            serviceRegistry.register(registration);
            
            // Store the registration
            registrations.put(id, registration);
            
            return id;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to register service: " + name, e);
            return null;
        }
    }

    /**
     * Deregister a service from the service discovery system.
     *
     * @param id The service ID
     */
    public void deregister(String id) {
        try {
            ServiceRegistration registration = registrations.remove(id);
            if (registration != null) {
                serviceRegistry.deregister(registration);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to deregister service: " + id, e);
        }
    }

    /**
     * Deregister all services.
     */
    public void deregisterAll() {
        for (String id : registrations.keySet()) {
            deregister(id);
        }
    }
}