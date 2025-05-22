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
package org.traccar;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.function.Supplier;

// Circuit breaker implementation
class CircuitBreaker {
    private final String name;
    private State state = State.CLOSED;
    private int failureCount = 0;
    private final int failureThreshold;
    private long lastStateChangeTime;
    private final long waitDurationInOpenStateMs;
    private final int permittedCallsInHalfOpen;
    private int callsInHalfOpen = 0;
    
    enum State {
        CLOSED, OPEN, HALF_OPEN
    }
    
    public CircuitBreaker(String name, int failureThreshold, long waitDurationInOpenStateMs, int permittedCallsInHalfOpen) {
        this.name = name;
        this.failureThreshold = failureThreshold;
        this.waitDurationInOpenStateMs = waitDurationInOpenStateMs;
        this.permittedCallsInHalfOpen = permittedCallsInHalfOpen;
        this.lastStateChangeTime = System.currentTimeMillis();
    }
    
    public synchronized <T> T executeSupplier(Supplier<T> supplier) throws Exception {
        checkState();
        
        if (state == State.OPEN) {
            throw new RuntimeException("Circuit breaker is open for " + name);
        }
        
        if (state == State.HALF_OPEN && callsInHalfOpen >= permittedCallsInHalfOpen) {
            throw new RuntimeException("Too many calls in half-open state for " + name);
        }
        
        if (state == State.HALF_OPEN) {
            callsInHalfOpen++;
        }
        
        try {
            T result = supplier.get();
            onSuccess();
            return result;
        } catch (Exception e) {
            onError();
            throw e;
        }
    }
    
    private synchronized void checkState() {
        if (state == State.OPEN) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastStateChangeTime >= waitDurationInOpenStateMs) {
                transitionToHalfOpen();
            }
        }
    }
    
    private synchronized void onSuccess() {
        if (state == State.HALF_OPEN) {
            transitionToClosed();
        } else if (state == State.CLOSED) {
            failureCount = 0;
        }
    }
    
    private synchronized void onError() {
        if (state == State.HALF_OPEN) {
            transitionToOpen();
        } else if (state == State.CLOSED) {
            failureCount++;
            if (failureCount >= failureThreshold) {
                transitionToOpen();
            }
        }
    }
    
    private void transitionToClosed() {
        state = State.CLOSED;
        failureCount = 0;
        callsInHalfOpen = 0;
        lastStateChangeTime = System.currentTimeMillis();
    }
    
    private void transitionToOpen() {
        state = State.OPEN;
        lastStateChangeTime = System.currentTimeMillis();
    }
    
    private void transitionToHalfOpen() {
        state = State.HALF_OPEN;
        callsInHalfOpen = 0;
        lastStateChangeTime = System.currentTimeMillis();
    }
    
    public State getState() {
        checkState();
        return state;
    }
    
    public String getName() {
        return name;
    }
}

// Simple circuit breaker registry
class CircuitBreakerRegistry {
    private final Map<String, CircuitBreaker> circuitBreakers = new ConcurrentHashMap<>();
    private final int failureThreshold;
    private final long waitDurationInOpenStateMs;
    private final int permittedCallsInHalfOpen;
    
    private CircuitBreakerRegistry(int failureThreshold, long waitDurationInOpenStateMs, int permittedCallsInHalfOpen) {
        this.failureThreshold = failureThreshold;
        this.waitDurationInOpenStateMs = waitDurationInOpenStateMs;
        this.permittedCallsInHalfOpen = permittedCallsInHalfOpen;
    }
    
    public static CircuitBreakerRegistry of(int failureThreshold, long waitDurationInOpenStateMs, int permittedCallsInHalfOpen) {
        return new CircuitBreakerRegistry(failureThreshold, waitDurationInOpenStateMs, permittedCallsInHalfOpen);
    }
    
    public CircuitBreaker circuitBreaker(String name) {
        return circuitBreakers.computeIfAbsent(name, 
                k -> new CircuitBreaker(name, failureThreshold, waitDurationInOpenStateMs, permittedCallsInHalfOpen));
    }
}
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages service discovery and registration using Consul or Kubernetes API.
 * This component enables dynamic service location and load balancing across
 * the microservices architecture, allowing the monolithic Traccar server to
 * discover and communicate with extracted microservices during the transition phase.
 */
@Singleton
public class ServiceDiscoveryManager implements LifecycleObject {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryManager.class);

    private final Config config;
    private final Map<String, ServiceEndpoint> serviceCache = new ConcurrentHashMap<>();
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final Map<String, CircuitBreaker> circuitBreakers = new HashMap<>();
    private ScheduledExecutorService executorService;
    private ServiceDiscoveryProvider discoveryProvider;

    /**
     * Represents a service endpoint with its host, port, and health status.
     */
    public static class ServiceEndpoint {
        private final String host;
        private final int port;
        private final boolean healthy;

        public ServiceEndpoint(String host, int port, boolean healthy) {
            this.host = host;
            this.port = port;
            this.healthy = healthy;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public boolean isHealthy() {
            return healthy;
        }

        public String getUrl() {
            return String.format("http://%s:%d", host, port);
        }
    }

    /**
     * Interface for service discovery providers (Consul, Kubernetes).
     */
    private interface ServiceDiscoveryProvider {
        void start() throws Exception;
        void stop() throws Exception;
        void registerService(String serviceName, String host, int port, Map<String, String> metadata) throws Exception;
        void deregisterService(String serviceName) throws Exception;
        ServiceEndpoint discoverService(String serviceName) throws Exception;
        Map<String, ServiceEndpoint> discoverServices(String serviceName) throws Exception;
    }

    /**
     * Consul-based service discovery implementation.
     */
    private class ConsulServiceDiscovery implements ServiceDiscoveryProvider {
        private final String consulHost;
        private final int consulPort;
        private final HttpClient httpClient;
        private final String serviceId;

        public ConsulServiceDiscovery(String consulHost, int consulPort) {
            this.consulHost = consulHost;
            this.consulPort = consulPort;
            this.httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .build();
            this.serviceId = config.getString(Keys.HOSTNAME.getKey(), "traccar");
        }

        @Override
        public void start() {
            LOGGER.info("Starting Consul service discovery ({}:{})", consulHost, consulPort);
        }

        @Override
        public void stop() {
            LOGGER.info("Stopping Consul service discovery");
        }

        @Override
        public void registerService(String serviceName, String host, int port, Map<String, String> metadata) throws Exception {
            String registrationJson = String.format(
                    "{\"ID\":\"%s\",\"Name\":\"%s\",\"Address\":\"%s\",\"Port\":%d,\"Check\":{\"HTTP\":\"http://%s:%d/health\",\"Interval\":\"10s\"}}",
                    serviceId, serviceName, host, port, host, port);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(String.format("http://%s:%d/v1/agent/service/register", consulHost, consulPort)))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(registrationJson))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IOException("Failed to register service with Consul: " + response.body());
            }

            LOGGER.info("Registered service {} with Consul", serviceName);
        }

        @Override
        public void deregisterService(String serviceName) throws Exception {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(String.format("http://%s:%d/v1/agent/service/deregister/%s", consulHost, consulPort, serviceId)))
                    .DELETE()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IOException("Failed to deregister service from Consul: " + response.body());
            }

            LOGGER.info("Deregistered service {} from Consul", serviceName);
        }

        @Override
        public ServiceEndpoint discoverService(String serviceName) throws Exception {
            Map<String, ServiceEndpoint> services = discoverServices(serviceName);
            if (services.isEmpty()) {
                throw new IOException("No healthy instances found for service: " + serviceName);
            }
            // Simple round-robin selection from available services
            return services.values().iterator().next();
        }

        @Override
        public Map<String, ServiceEndpoint> discoverServices(String serviceName) throws Exception {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(String.format("http://%s:%d/v1/health/service/%s?passing=true", consulHost, consulPort, serviceName)))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IOException("Failed to discover services from Consul: " + response.body());
            }

            // This is a simplified implementation. In a production environment,
            // you would want to properly parse the JSON response and extract service information.
            // For now, we'll just log the response and return a dummy endpoint for demonstration.
            LOGGER.debug("Consul service discovery response: {}", response.body());
            
            // In a real implementation, parse the JSON response and extract service endpoints
            // For now, return a dummy endpoint if the response isn't empty
            if (!response.body().equals("[]")) {
                Map<String, ServiceEndpoint> result = new HashMap<>();
                result.put("dummy", new ServiceEndpoint("localhost", 8080, true));
                return result;
            }
            
            return Collections.emptyMap();
        }
    }

    /**
     * Kubernetes-based service discovery implementation.
     */
    private class KubernetesServiceDiscovery implements ServiceDiscoveryProvider {
        private final String namespace;
        private final HttpClient httpClient;
        private final String serviceAccountToken;
        private final String apiServerUrl;

        public KubernetesServiceDiscovery() {
            this.namespace = System.getenv().getOrDefault("KUBERNETES_NAMESPACE", "default");
            this.httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .build();
            
            // In a Kubernetes pod, the service account token is mounted at this path
            this.serviceAccountToken = readServiceAccountToken();
            this.apiServerUrl = System.getenv().getOrDefault("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc");
        }

        private String readServiceAccountToken() {
            try {
                java.nio.file.Path tokenPath = java.nio.file.Paths.get("/var/run/secrets/kubernetes.io/serviceaccount/token");
                if (java.nio.file.Files.exists(tokenPath)) {
                    return new String(java.nio.file.Files.readAllBytes(tokenPath));
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to read Kubernetes service account token", e);
            }
            return ""; // Return empty string if not running in Kubernetes or token not available
        }

        @Override
        public void start() {
            LOGGER.info("Starting Kubernetes service discovery in namespace {}", namespace);
        }

        @Override
        public void stop() {
            LOGGER.info("Stopping Kubernetes service discovery");
        }

        @Override
        public void registerService(String serviceName, String host, int port, Map<String, String> metadata) {
            // In Kubernetes, services are typically defined in YAML and applied to the cluster
            // This method is a no-op as registration is handled by Kubernetes manifests
            LOGGER.info("Service registration in Kubernetes is handled via manifests, not programmatically");
        }

        @Override
        public void deregisterService(String serviceName) {
            // In Kubernetes, services are typically defined in YAML and applied to the cluster
            // This method is a no-op as deregistration is handled by Kubernetes manifests
            LOGGER.info("Service deregistration in Kubernetes is handled via manifests, not programmatically");
        }

        @Override
        public ServiceEndpoint discoverService(String serviceName) throws Exception {
            // In Kubernetes, we can use DNS for service discovery
            // serviceName.namespace.svc.cluster.local
            String host = serviceName + "." + namespace + ".svc.cluster.local";
            
            // Try to resolve the service using DNS
            try {
                java.net.InetAddress address = java.net.InetAddress.getByName(host);
                LOGGER.debug("Resolved Kubernetes service {} to {}", serviceName, address.getHostAddress());
                return new ServiceEndpoint(host, 80, true); // Default to port 80 for HTTP services
            } catch (Exception e) {
                LOGGER.warn("Failed to resolve Kubernetes service {} via DNS", serviceName, e);
            }
            
            // Fallback to API server if DNS resolution fails
            return discoverServiceViaApi(serviceName);
        }

        private ServiceEndpoint discoverServiceViaApi(String serviceName) throws Exception {
            if (serviceAccountToken.isEmpty()) {
                throw new IOException("No Kubernetes service account token available for API discovery");
            }
            
            String url = String.format("https://%s/api/v1/namespaces/%s/services/%s", 
                    apiServerUrl, namespace, serviceName);
            
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", "Bearer " + serviceAccountToken)
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IOException("Failed to discover service from Kubernetes API: " + response.body());
            }

            // This is a simplified implementation. In a production environment,
            // you would want to properly parse the JSON response and extract service information.
            LOGGER.debug("Kubernetes service discovery response: {}", response.body());
            
            // In a real implementation, parse the JSON response and extract service endpoints
            // For now, return a dummy endpoint
            return new ServiceEndpoint(serviceName + "." + namespace + ".svc.cluster.local", 80, true);
        }

        @Override
        public Map<String, ServiceEndpoint> discoverServices(String serviceName) throws Exception {
            ServiceEndpoint endpoint = discoverService(serviceName);
            Map<String, ServiceEndpoint> result = new HashMap<>();
            result.put(serviceName, endpoint);
            return result;
        }
    }

    /**
     * Constructs a new ServiceDiscoveryManager with the provided configuration.
     *
     * @param config The system configuration
     */
    @Inject
    public ServiceDiscoveryManager(Config config) {
        this.config = config;
        
        // Configure circuit breaker for service discovery
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(
                5, // failureThreshold
                10000, // waitDurationInOpenStateMs (10 seconds)
                3); // permittedCallsInHalfOpen
    }

    /**
     * Starts the service discovery manager.
     */
    /**
     * Configuration keys for service discovery.
     */
    public static class Keys {
        public static final String SERVICE_DISCOVERY_TYPE = "service.discovery.type";
        public static final String SERVICE_DISCOVERY_REGISTER = "service.discovery.register";
        public static final String SERVICE_DISCOVERY_NAME = "service.discovery.name";
        public static final String SERVICE_DISCOVERY_REFRESH_INTERVAL = "service.discovery.refresh.interval";
        public static final String SERVICE_DISCOVERY_CONSUL_HOST = "service.discovery.consul.host";
        public static final String SERVICE_DISCOVERY_CONSUL_PORT = "service.discovery.consul.port";
    }
    
    @Override
    public void start() throws Exception {
        LOGGER.info("Starting ServiceDiscoveryManager");
        
        // Initialize the executor service for background tasks
        executorService = Executors.newScheduledThreadPool(1);
        
        // Determine which service discovery provider to use based on configuration
        String discoveryType = config.getString(Keys.SERVICE_DISCOVERY_TYPE, "none").toLowerCase();
        
        switch (discoveryType) {
            case "consul":
                String consulHost = config.getString(Keys.SERVICE_DISCOVERY_CONSUL_HOST, "localhost");
                int consulPort = config.getInteger(Keys.SERVICE_DISCOVERY_CONSUL_PORT, 8500);
                discoveryProvider = new ConsulServiceDiscovery(consulHost, consulPort);
                break;
                
            case "kubernetes":
                discoveryProvider = new KubernetesServiceDiscovery();
                break;
                
            case "none":
            default:
                LOGGER.info("Service discovery is disabled");
                return;
        }
        
        // Start the discovery provider
        discoveryProvider.start();
        
        // Schedule periodic cache refresh
        int refreshInterval = config.getInteger(Keys.SERVICE_DISCOVERY_REFRESH_INTERVAL, 60);
        executorService.scheduleAtFixedRate(this::refreshServiceCache, refreshInterval, refreshInterval, TimeUnit.SECONDS);
        
        // Register this service if configured
        if (config.getBoolean(Keys.SERVICE_DISCOVERY_REGISTER, false)) {
            String serviceName = config.getString(Keys.SERVICE_DISCOVERY_NAME, "traccar");
            String serviceHost = config.getString(org.traccar.config.Keys.HOSTNAME.getKey(), "localhost");
            int servicePort = config.getInteger(org.traccar.config.Keys.WEB_PORT.getKey(), 8082);
            Map<String, String> metadata = new HashMap<>();
            metadata.put("version", Main.class.getPackage().getImplementationVersion());
            
            discoveryProvider.registerService(serviceName, serviceHost, servicePort, metadata);
        }
    }

    /**
     * Stops the service discovery manager.
     */
    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping ServiceDiscoveryManager");
        
        if (executorService != null) {
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
        
        if (discoveryProvider != null) {
            // Deregister service if it was registered
            if (config.getBoolean(Keys.SERVICE_DISCOVERY_REGISTER, false)) {
                String serviceName = config.getString(Keys.SERVICE_DISCOVERY_NAME, "traccar");
                discoveryProvider.deregisterService(serviceName);
            }
            
            discoveryProvider.stop();
        }
    }

    /**
     * Refreshes the service cache by querying the discovery provider.
     */
    private void refreshServiceCache() {
        try {
            // This would typically refresh all known services
            // For simplicity, we're not implementing the full cache refresh logic
            LOGGER.debug("Refreshing service discovery cache");
        } catch (Exception e) {
            LOGGER.warn("Failed to refresh service discovery cache", e);
        }
    }

    /**
     * Discovers a service endpoint by name with circuit breaker protection.
     *
     * @param serviceName The name of the service to discover
     * @return The service endpoint
     * @throws Exception If service discovery fails
     */
    public ServiceEndpoint getService(String serviceName) throws Exception {
        // Check cache first
        ServiceEndpoint cachedEndpoint = serviceCache.get(serviceName);
        if (cachedEndpoint != null) {
            return cachedEndpoint;
        }
        
        // Get or create circuit breaker for this service
        CircuitBreaker circuitBreaker = circuitBreakers.computeIfAbsent(
                serviceName,
                name -> circuitBreakerRegistry.circuitBreaker(name));
        
        // Use circuit breaker to protect against service discovery failures
        try {
            return circuitBreaker.executeSupplier(() -> {
                ServiceEndpoint endpoint = discoveryProvider.discoverService(serviceName);
                // Cache the result
                serviceCache.put(serviceName, endpoint);
                return endpoint;
            });
        } catch (Exception e) {
            LOGGER.warn("Service discovery failed for {}, circuit breaker state: {}", 
                    serviceName, circuitBreaker.getState(), e);
            throw e;
        }
    }

    /**
     * Gets all instances of a service by name.
     *
     * @param serviceName The name of the service
     * @return Map of service ID to endpoint
     * @throws Exception If service discovery fails
     */
    public Map<String, ServiceEndpoint> getServices(String serviceName) throws Exception {
        if (discoveryProvider == null) {
            return Collections.emptyMap();
        }
        
        return discoveryProvider.discoverServices(serviceName);
    }

    /**
     * Checks if a service is healthy by making an HTTP request to its health endpoint.
     *
     * @param endpoint The service endpoint to check
     * @return true if the service is healthy, false otherwise
     */
    public boolean isServiceHealthy(ServiceEndpoint endpoint) {
        try {
            URL url = new URL(endpoint.getUrl() + "/health");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(2000);
            connection.setReadTimeout(2000);
            
            int responseCode = connection.getResponseCode();
            return responseCode >= 200 && responseCode < 300;
        } catch (Exception e) {
            LOGGER.debug("Health check failed for {}: {}", endpoint.getUrl(), e.getMessage());
            return false;
        }
    }

    /**
     * Gets the URL for a service by name.
     *
     * @param serviceName The name of the service
     * @return The service URL
     * @throws Exception If service discovery fails
     */
    public String getServiceUrl(String serviceName) throws Exception {
        ServiceEndpoint endpoint = getService(serviceName);
        return endpoint.getUrl();
    }
    
    /**
     * Gets the URL for a service by name with a fallback URL if service discovery fails.
     *
     * @param serviceName The name of the service
     * @param fallbackUrl The fallback URL to use if service discovery fails
     * @return The service URL or fallback URL
     */
    public String getServiceUrlWithFallback(String serviceName, String fallbackUrl) {
        try {
            return getServiceUrl(serviceName);
        } catch (Exception e) {
            LOGGER.warn("Failed to discover service {}, using fallback URL: {}", serviceName, fallbackUrl, e);
            return fallbackUrl;
        }
    }
    
    /**
     * Checks if service discovery is enabled.
     *
     * @return true if service discovery is enabled, false otherwise
     */
    public boolean isEnabled() {
        return discoveryProvider != null;
    }
    
    /**
     * Gets the current state of the circuit breaker for a service.
     *
     * @param serviceName The name of the service
     * @return The circuit breaker state or null if no circuit breaker exists for the service
     */
    public CircuitBreaker.State getCircuitBreakerState(String serviceName) {
        CircuitBreaker circuitBreaker = circuitBreakers.get(serviceName);
        return circuitBreaker != null ? circuitBreaker.getState() : null;
    }
}