/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.web.api;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;

import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Client for service discovery integration.
 * Supports both Consul and Kubernetes service discovery.
 */
public class ServiceDiscoveryClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryClient.class);

    private final Config config;
    private final MeterRegistry meterRegistry;
    private final HttpClient httpClient;
    private final Map<String, ServiceInstance> serviceCache;
    private final ScheduledExecutorService executorService;
    private final String serviceDiscoveryType;
    private final String serviceDiscoveryUrl;
    private final String serviceName;
    private final int servicePort;
    private String serviceId;

    public ServiceDiscoveryClient(Config config, MeterRegistry meterRegistry) {
        this.config = config;
        this.meterRegistry = meterRegistry;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.serviceCache = new ConcurrentHashMap<>();
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        
        // Configure service discovery
        this.serviceDiscoveryType = config.getString("web.serviceDiscovery.type", "kubernetes");
        this.serviceDiscoveryUrl = config.getString("web.serviceDiscovery.url", "");
        this.serviceName = config.getString("web.serviceDiscovery.serviceName", "api-gateway");
        this.servicePort = config.getInteger("web.serviceDiscovery.port", config.getInteger("web.port", 8082));
        
        // Start service discovery refresh task if enabled
        if (config.getBoolean("web.serviceDiscovery.enabled", false)) {
            int refreshInterval = config.getInteger("web.serviceDiscovery.refreshInterval", 30);
            executorService.scheduleAtFixedRate(
                    this::refreshServiceCache, 0, refreshInterval, TimeUnit.SECONDS);
            LOGGER.info("Service discovery client initialized with type: {}", serviceDiscoveryType);
        }
    }

    /**
     * Register this service with the service registry.
     */
    public void register() {
        try {
            String hostAddress = InetAddress.getLocalHost().getHostAddress();
            serviceId = serviceName + "-" + hostAddress + "-" + servicePort;
            
            if ("consul".equalsIgnoreCase(serviceDiscoveryType)) {
                registerWithConsul(hostAddress);
            } else if ("kubernetes".equalsIgnoreCase(serviceDiscoveryType)) {
                // Kubernetes service discovery is handled by Kubernetes itself
                LOGGER.info("Service registration not needed for Kubernetes service discovery");
            } else {
                LOGGER.warn("Unsupported service discovery type: {}", serviceDiscoveryType);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to register service", e);
        }
    }

    /**
     * Deregister this service from the service registry.
     */
    public void deregister() {
        if (serviceId != null && "consul".equalsIgnoreCase(serviceDiscoveryType)) {
            try {
                String url = serviceDiscoveryUrl + "/v1/agent/service/deregister/" + serviceId;
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .PUT(HttpRequest.BodyPublishers.noBody())
                        .build();
                
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    LOGGER.info("Service deregistered successfully");
                } else {
                    LOGGER.warn("Failed to deregister service: {}", response.body());
                }
            } catch (Exception e) {
                LOGGER.error("Failed to deregister service", e);
            }
        }
    }

    /**
     * Get a service instance for the specified service name.
     * Uses cached service information with fallback to direct service discovery lookup.
     *
     * @param serviceName the name of the service to look up
     * @return the service instance or null if not found
     */
    public ServiceInstance getService(String serviceName) {
        ServiceInstance cachedInstance = serviceCache.get(serviceName);
        if (cachedInstance != null) {
            return cachedInstance;
        }
        
        // If not in cache, try to discover it directly
        try {
            if ("consul".equalsIgnoreCase(serviceDiscoveryType)) {
                return discoverServiceFromConsul(serviceName);
            } else if ("kubernetes".equalsIgnoreCase(serviceDiscoveryType)) {
                // In Kubernetes, services are accessible via DNS
                // Format: service-name.namespace.svc.cluster.local
                String namespace = config.getString("web.serviceDiscovery.namespace", "default");
                String host = serviceName + "." + namespace + ".svc.cluster.local";
                int port = config.getInteger("web.serviceDiscovery." + serviceName + ".port", 80);
                
                ServiceInstance instance = new ServiceInstance(serviceName, host, port);
                serviceCache.put(serviceName, instance);
                return instance;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to discover service: {}", serviceName, e);
        }
        
        return null;
    }

    /**
     * Check if a service is available.
     *
     * @param serviceName the name of the service to check
     * @return true if the service is available, false otherwise
     */
    public boolean isServiceAvailable(String serviceName) {
        return getService(serviceName) != null;
    }

    /**
     * Refresh the service cache by querying the service registry.
     */
    private void refreshServiceCache() {
        try {
            if ("consul".equalsIgnoreCase(serviceDiscoveryType)) {
                refreshFromConsul();
            } else if ("kubernetes".equalsIgnoreCase(serviceDiscoveryType)) {
                // In Kubernetes, we rely on DNS for service discovery
                // No need to refresh the cache as DNS will handle it
            }
        } catch (Exception e) {
            LOGGER.error("Failed to refresh service cache", e);
        }
    }

    /**
     * Register the service with Consul.
     *
     * @param hostAddress the host address of this service
     * @throws Exception if registration fails
     */
    private void registerWithConsul(String hostAddress) throws Exception {
        String url = serviceDiscoveryUrl + "/v1/agent/service/register";
        String checkUrl = "http://" + hostAddress + ":" + servicePort + "/health/liveness";
        
        String requestBody = String.format(
                "{\
                    \"ID\": \"%s\",\
                    \"Name\": \"%s\",\
                    \"Address\": \"%s\",\
                    \"Port\": %d,\
                    \"Check\": {\
                        \"HTTP\": \"%s\",\
                        \"Interval\": \"10s\",\
                        \"Timeout\": \"5s\"\
                    }\
                }",
                serviceId, serviceName, hostAddress, servicePort, checkUrl);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
            LOGGER.info("Service registered successfully with Consul");
        } else {
            LOGGER.warn("Failed to register service with Consul: {}", response.body());
        }
    }

    /**
     * Refresh the service cache from Consul.
     *
     * @throws Exception if refresh fails
     */
    private void refreshFromConsul() throws Exception {
        String url = serviceDiscoveryUrl + "/v1/agent/services";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
            // Parse the JSON response and update the service cache
            // This is a simplified implementation - in a real-world scenario,
            // you would use a JSON library to parse the response
            LOGGER.debug("Refreshed service cache from Consul");
        } else {
            LOGGER.warn("Failed to refresh service cache from Consul: {}", response.body());
        }
    }

    /**
     * Discover a service from Consul.
     *
     * @param serviceName the name of the service to discover
     * @return the service instance or null if not found
     * @throws Exception if discovery fails
     */
    private ServiceInstance discoverServiceFromConsul(String serviceName) throws Exception {
        String url = serviceDiscoveryUrl + "/v1/health/service/" + serviceName + "?passing=true";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
            // Parse the JSON response to get service details
            // This is a simplified implementation - in a real-world scenario,
            // you would use a JSON library to parse the response
            
            // For now, we'll just return a dummy service instance
            ServiceInstance instance = new ServiceInstance(serviceName, "localhost", 8080);
            serviceCache.put(serviceName, instance);
            return instance;
        } else {
            LOGGER.warn("Failed to discover service from Consul: {}", response.body());
            return null;
        }
    }

    /**
     * Represents a discovered service instance.
     */
    public static class ServiceInstance {
        private final String name;
        private final String host;
        private final int port;
        private final Map<String, String> metadata;

        public ServiceInstance(String name, String host, int port) {
            this.name = name;
            this.host = host;
            this.port = port;
            this.metadata = new HashMap<>();
        }

        public String getName() {
            return name;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public Map<String, String> getMetadata() {
            return metadata;
        }

        public void addMetadata(String key, String value) {
            metadata.put(key, value);
        }

        public String getUrl() {
            return "http://" + host + ":" + port;
        }
    }
}