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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of the ServiceRegistry interface for Consul.
 * It handles service registration, deregistration, and discovery using the Consul API.
 * This implementation enables the Protocol Service to integrate with Consul for
 * service discovery in environments where Consul is the preferred registry.
 */
@Singleton
public class ConsulServiceRegistry implements ServiceRegistry {

    private static final Logger LOGGER = Logger.getLogger(ConsulServiceRegistry.class.getName());
    
    private static final String DEFAULT_CONSUL_HOST = "localhost";
    private static final int DEFAULT_CONSUL_PORT = 8500;
    private static final String DEFAULT_CONSUL_SCHEME = "http";
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);
    private static final String API_VERSION = "v1";
    
    private final String consulUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final HealthCheck healthCheck;
    private final Map<String, ServiceInstance> registeredServices;
    private final Map<String, String> serviceChecks;
    private final ServiceRegistryConfig config;

    /**
     * Constructs a new ConsulServiceRegistry with the specified configuration and health check.
     *
     * @param config      The service registry configuration
     * @param healthCheck The health check implementation to use for service health status
     */
    @Inject
    public ConsulServiceRegistry(ServiceRegistryConfig config, HealthCheck healthCheck) {
        this.config = config;
        this.healthCheck = healthCheck;
        this.registeredServices = new ConcurrentHashMap<>();
        this.serviceChecks = new ConcurrentHashMap<>();
        
        // Initialize HTTP client
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(DEFAULT_TIMEOUT)
                .build();
        
        // Initialize JSON mapper
        this.objectMapper = new ObjectMapper();
        this.objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        
        // Build Consul URL
        String host = config.getConsulHost();
        if (host == null || host.isEmpty()) {
            host = DEFAULT_CONSUL_HOST;
        }
        
        int port = config.getConsulPort();
        if (port <= 0) {
            port = DEFAULT_CONSUL_PORT;
        }
        
        String scheme = config.getConsulScheme();
        if (scheme == null || scheme.isEmpty()) {
            scheme = DEFAULT_CONSUL_SCHEME;
        }
        
        this.consulUrl = String.format("%s://%s:%d/%s", scheme, host, port, API_VERSION);
        
        LOGGER.info("Initialized Consul Service Registry at " + consulUrl);
    }
    
    /**
     * Constructs a new ConsulServiceRegistry with the specified configuration.
     *
     * @param config The service registry configuration
     */
    public ConsulServiceRegistry(ServiceRegistryConfig config) {
        this(config, new DefaultHealthCheck());
    }

    @Override
    public void register(ServiceInstance serviceInstance) {
        try {
            // Create service registration payload
            ConsulService service = createConsulService(serviceInstance);
            String payload = objectMapper.writeValueAsString(service);
            
            // Register service with Consul
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(consulUrl + "/agent/service/register"))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(payload))
                    .build();
            
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                // Store the registered service
                registeredServices.put(serviceInstance.getServiceId(), serviceInstance);
                
                // Store check ID if health check was registered
                if (service.check != null) {
                    String checkId = "service:" + serviceInstance.getServiceId();
                    serviceChecks.put(serviceInstance.getServiceId(), checkId);
                }
                
                LOGGER.info("Registered service with Consul: " + serviceInstance.getServiceId());
            } else {
                LOGGER.severe("Failed to register service with Consul: " + response.statusCode() + " - " + response.body());
                throw new RuntimeException("Failed to register service with Consul: " + response.statusCode());
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.log(Level.SEVERE, "Error registering service with Consul", e);
            throw new RuntimeException("Error registering service with Consul", e);
        }
    }

    @Override
    public void deregister(String serviceId) {
        try {
            // Deregister service from Consul
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(consulUrl + "/agent/service/deregister/" + serviceId))
                    .PUT(HttpRequest.BodyPublishers.noBody())
                    .build();
            
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                // Remove from registered services
                registeredServices.remove(serviceId);
                serviceChecks.remove(serviceId);
                
                LOGGER.info("Deregistered service from Consul: " + serviceId);
            } else {
                LOGGER.severe("Failed to deregister service from Consul: " + response.statusCode() + " - " + response.body());
                throw new RuntimeException("Failed to deregister service from Consul: " + response.statusCode());
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.log(Level.SEVERE, "Error deregistering service from Consul", e);
            throw new RuntimeException("Error deregistering service from Consul", e);
        }
    }

    @Override
    public void updateStatus(String serviceId, boolean status) {
        String checkId = serviceChecks.get(serviceId);
        if (checkId == null) {
            LOGGER.warning("No health check found for service: " + serviceId);
            return;
        }
        
        try {
            // Determine the endpoint based on status
            String endpoint = status ? "/agent/check/pass/" : "/agent/check/fail/";
            
            // Update check status in Consul
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(consulUrl + endpoint + checkId))
                    .PUT(HttpRequest.BodyPublishers.noBody())
                    .build();
            
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                // Update local service instance status
                ServiceInstance instance = registeredServices.get(serviceId);
                if (instance != null) {
                    instance.setHealthy(status);
                }
                
                LOGGER.info("Updated service status in Consul: " + serviceId + " to " + (status ? "passing" : "failing"));
            } else {
                LOGGER.severe("Failed to update service status in Consul: " + response.statusCode() + " - " + response.body());
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.log(Level.SEVERE, "Error updating service status in Consul", e);
        }
    }

    @Override
    public List<ServiceInstance> getServiceInstances(String serviceName) {
        try {
            // Query Consul for healthy service instances
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(consulUrl + "/health/service/" + serviceName + "?passing=true"))
                    .GET()
                    .build();
            
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                // Parse response to get service instances
                ConsulHealthService[] healthServices = objectMapper.readValue(response.body(), ConsulHealthService[].class);
                
                List<ServiceInstance> instances = new ArrayList<>();
                for (ConsulHealthService healthService : healthServices) {
                    if (healthService.service != null) {
                        ServiceInstance instance = new ServiceInstance();
                        instance.setServiceId(healthService.service.id);
                        instance.setHost(healthService.service.address != null && !healthService.service.address.isEmpty() 
                                ? healthService.service.address : healthService.node.address);
                        instance.setPort(healthService.service.port);
                        instance.setSecure(false); // Default to non-secure
                        
                        // Set metadata from service tags and meta
                        Map<String, String> metadata = new HashMap<>();
                        if (healthService.service.tags != null) {
                            for (String tag : healthService.service.tags) {
                                // Convert tags to metadata entries
                                if (tag.contains("=")) {
                                    String[] parts = tag.split("=", 2);
                                    metadata.put(parts[0], parts[1]);
                                } else {
                                    metadata.put(tag, "true");
                                }
                            }
                        }
                        
                        // Add service meta if available
                        if (healthService.service.meta != null) {
                            metadata.putAll(healthService.service.meta);
                        }
                        
                        instance.setMetadata(metadata);
                        instance.setHealthy(true); // Only passing services are returned
                        
                        instances.add(instance);
                    }
                }
                
                return instances;
            } else {
                LOGGER.severe("Failed to get service instances from Consul: " + response.statusCode() + " - " + response.body());
                return Collections.emptyList();
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.log(Level.SEVERE, "Error getting service instances from Consul", e);
            return Collections.emptyList();
        }
    }

    @Override
    public void close() {
        // Deregister all services on shutdown
        new ArrayList<>(registeredServices.keySet()).forEach(this::deregister);
        LOGGER.info("Closed Consul Service Registry");
    }
    
    /**
     * Creates a ConsulService object from a ServiceInstance for registration.
     *
     * @param serviceInstance The service instance to convert
     * @return A ConsulService object ready for registration
     */
    private ConsulService createConsulService(ServiceInstance serviceInstance) {
        ConsulService service = new ConsulService();
        service.id = serviceInstance.getServiceId();
        service.name = serviceInstance.getServiceId();
        service.address = serviceInstance.getHost();
        service.port = serviceInstance.getPort();
        
        // Convert metadata to tags and meta
        if (serviceInstance.getMetadata() != null && !serviceInstance.getMetadata().isEmpty()) {
            service.meta = new HashMap<>(serviceInstance.getMetadata());
            
            // Extract tags from metadata if specified
            if (service.meta.containsKey("tags")) {
                String tagsStr = service.meta.remove("tags");
                if (tagsStr != null && !tagsStr.isEmpty()) {
                    service.tags = List.of(tagsStr.split(","));
                }
            }
        }
        
        // Add health check if enabled
        if (config.isHealthCheckEnabled()) {
            service.check = new ConsulCheck();
            
            // Determine check type based on configuration
            if (config.getHealthCheckEndpoint() != null && !config.getHealthCheckEndpoint().isEmpty()) {
                // HTTP check
                String protocol = serviceInstance.isSecure() ? "https" : "http";
                service.check.http = String.format("%s://%s:%d%s", 
                        protocol, serviceInstance.getHost(), serviceInstance.getPort(), config.getHealthCheckEndpoint());
                service.check.method = "GET";
            } else {
                // TTL check
                service.check.ttl = config.getHealthCheckInterval() + "s";
            }
            
            service.check.interval = config.getHealthCheckInterval() + "s";
            service.check.timeout = config.getHealthCheckTimeout() + "s";
            service.check.deregisterCriticalServiceAfter = config.getDeregisterCriticalServiceAfter() + "m";
        }
        
        return service;
    }
    
    /**
     * Default implementation of HealthCheck that always returns healthy.
     */
    private static class DefaultHealthCheck implements HealthCheck {
        @Override
        public boolean isHealthy() {
            return true;
        }
        
        @Override
        public Map<String, Object> getStatus() {
            Map<String, Object> status = new HashMap<>();
            status.put("status", "UP");
            return status;
        }
    }
    
    /**
     * Consul service registration model.
     */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    private static class ConsulService {
        public String id;
        public String name;
        public String address;
        public int port;
        public List<String> tags;
        public Map<String, String> meta;
        public ConsulCheck check;
    }
    
    /**
     * Consul health check model.
     */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    private static class ConsulCheck {
        public String id;
        public String name;
        public String http;
        public String method;
        public String ttl;
        public String interval;
        public String timeout;
        public String deregisterCriticalServiceAfter;
    }
    
    /**
     * Consul health service response model.
     */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    private static class ConsulHealthService {
        public ConsulNode node;
        public ConsulServiceInfo service;
        public List<ConsulCheckInfo> checks;
    }
    
    /**
     * Consul node model.
     */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    private static class ConsulNode {
        public String id;
        public String node;
        public String address;
        public String datacenter;
    }
    
    /**
     * Consul service info model.
     */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    private static class ConsulServiceInfo {
        public String id;
        public String service;
        public String name;
        public String address;
        public int port;
        public List<String> tags;
        public Map<String, String> meta;
    }
    
    /**
     * Consul check info model.
     */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    private static class ConsulCheckInfo {
        public String id;
        public String name;
        public String status;
        public String output;
        public String serviceId;
        public String serviceName;
    }
}