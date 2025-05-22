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
package org.traccar.geolocation;

import com.google.inject.Inject;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.traccar.config.Config;
// Using our own Keys class
import org.traccar.config.ConfigKey;
import org.traccar.model.Network;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;

/**
 * Manages geolocation service providers and integrates with service discovery.
 * Responsible for registering, discovering, and selecting appropriate geolocation services
 * based on availability and health in a microservices architecture.
 *
 * This class provides the following functionality:
 * - Integration with ServiceDiscoveryManager for geolocation service discovery
 * - Dynamic selection of geolocation providers based on health and availability
 * - Service registration with ServiceDiscoveryManager
 * - Service health monitoring and reporting
 * - Caching of service endpoints with configurable TTL
 * - Circuit breaker pattern implementation for resilience
 */
public class GeolocationServiceManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeolocationServiceManager.class);

    private final Config config;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    
    private final Map<String, GeolocationProvider> providers = new HashMap<>();
    private final Map<String, CircuitBreaker> circuitBreakers = new HashMap<>();
    private final Map<String, ServiceEndpoint> endpointCache = new ConcurrentHashMap<>();
    
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    
    private static final long DEFAULT_CACHE_TTL_MS = 60000; // 1 minute default TTL
    private final long cacheTtlMs;
    
    /**
     * Configuration keys for geolocation service.
     */
    public static class Keys {
        /**
         * Geolocation service cache TTL in milliseconds.
         */
        public static final ConfigKey GEOLOCATION_CACHE_TTL = new ConfigKey(
                "geolocation.cache.ttl", Long.class, DEFAULT_CACHE_TTL_MS);

        /**
         * Geolocation service name for registration with service discovery.
         */
        public static final ConfigKey GEOLOCATION_SERVICE_NAME = new ConfigKey(
                "geolocation.service.name", String.class);

        /**
         * Geolocation service port for registration with service discovery.
         */
        public static final ConfigKey GEOLOCATION_SERVICE_PORT = new ConfigKey(
                "geolocation.service.port", Integer.class);
    }

    /**
     * Represents a service endpoint with caching metadata.
     */
    private static class ServiceEndpoint {
        private final String url;
        private final long expirationTime;
        
        public ServiceEndpoint(String url, long ttlMs) {
            this.url = url;
            this.expirationTime = System.currentTimeMillis() + ttlMs;
        }
        
        public String getUrl() {
            return url;
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() > expirationTime;
        }
    }

    @Inject
    public GeolocationServiceManager(
            Config config,
            ServiceDiscoveryManager serviceDiscoveryManager) {
        this.config = config;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.cacheTtlMs = config.getLong(Keys.GEOLOCATION_CACHE_TTL.getKey(), DEFAULT_CACHE_TTL_MS);
        
        // Initialize circuit breaker registry with default configuration
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        
        // Register this service with service discovery
        registerWithServiceDiscovery();
    }

    /**
     * Registers this service with the service discovery system.
     */
    private void registerWithServiceDiscovery() {
        try {
            String serviceName = config.getString(Keys.GEOLOCATION_SERVICE_NAME.getKey(), "geolocation-service");
            int servicePort = config.getInteger(Keys.GEOLOCATION_SERVICE_PORT.getKey(), 8080);
            
            // Add additional metadata for containerized environments
            Map<String, String> metadata = new HashMap<>();
            metadata.put("version", getClass().getPackage().getImplementationVersion());
            metadata.put("environment", config.getString("geolocation.environment", "production"));
            
            // Check if running in a container
            if (isRunningInContainer()) {
                metadata.put("containerized", "true");
                // Add Kubernetes-specific metadata if available
                String podName = System.getenv("HOSTNAME");
                if (podName != null) {
                    metadata.put("pod", podName);
                }
            }
            
            serviceDiscoveryManager.registerService(serviceName, servicePort, metadata);
            LOGGER.info("Registered geolocation service with service discovery: {}:{}", serviceName, servicePort);
            
            // Schedule health reporting
            scheduleHealthReporting();
        } catch (Exception e) {
            LOGGER.error("Failed to register with service discovery", e);
        }
    }
    
    /**
     * Checks if the application is running in a container environment.
     * 
     * @return true if running in a container, false otherwise
     */
    private boolean isRunningInContainer() {
        // Check for container environment variables
        return System.getenv("KUBERNETES_SERVICE_HOST") != null || 
               System.getenv("DOCKER_CONTAINER") != null ||
               new java.io.File("/.dockerenv").exists();
    }

    /**
     * Schedules periodic health reporting to the service discovery system.
     */
    private void scheduleHealthReporting() {
        int healthCheckInterval = config.getInteger("geolocation.health.interval", 30); // seconds
        
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // Report health status to service discovery
                String serviceName = config.getString(Keys.GEOLOCATION_SERVICE_NAME.getKey(), "geolocation-service");
                boolean isHealthy = checkHealth();
                
                serviceDiscoveryManager.updateServiceHealth(serviceName, isHealthy);
                
                if (isHealthy) {
                    LOGGER.debug("Reported healthy status to service discovery");
                } else {
                    LOGGER.warn("Reported unhealthy status to service discovery");
                }
            } catch (Exception e) {
                LOGGER.error("Failed to report health status", e);
            }
        }, healthCheckInterval, healthCheckInterval, TimeUnit.SECONDS);
        
        LOGGER.info("Health reporting scheduled for geolocation service with interval {} seconds", healthCheckInterval);
    }
    
    /**
     * Checks the health of this service.
     * 
     * @return true if the service is healthy, false otherwise
     */
    private boolean checkHealth() {
        // Implement health check logic here
        // For example, check if all critical dependencies are available
        // and if the service can perform its core functions
        
        // For now, we'll just return true as a placeholder
        return true;
    }

    /**
     * Registers a geolocation provider with the manager.
     *
     * @param name     Provider name
     * @param provider Provider implementation
     */
    public void registerProvider(String name, GeolocationProvider provider) {
        providers.put(name, provider);
        circuitBreakers.put(name, circuitBreakerRegistry.circuitBreaker(name));
        LOGGER.info("Registered geolocation provider: {}", name);
    }

    /**
     * Gets a geolocation provider by name, with fallback to other providers if the requested one is unavailable.
     *
     * @param name Provider name
     * @return GeolocationProvider instance or null if none available
     */
    public GeolocationProvider getProvider(String name) {
        // Try to get the requested provider first
        if (name != null && providers.containsKey(name)) {
            CircuitBreaker circuitBreaker = circuitBreakers.get(name);
            if (circuitBreaker.getState() != CircuitBreaker.State.OPEN) {
                return providers.get(name);
            } else {
                LOGGER.warn("Provider {} circuit is open, falling back to available provider", name);
            }
        }

        // Fallback to any available provider
        for (Map.Entry<String, GeolocationProvider> entry : providers.entrySet()) {
            CircuitBreaker circuitBreaker = circuitBreakers.get(entry.getKey());
            if (circuitBreaker.getState() != CircuitBreaker.State.OPEN) {
                LOGGER.info("Falling back to provider: {}", entry.getKey());
                return entry.getValue();
            }
        }

        LOGGER.warn("No available geolocation providers found");
        return null;
    }

    /**
     * Gets the service endpoint for a specific geolocation service type.
     *
     * @param serviceType The type of geolocation service to discover
     * @return The service endpoint URL or null if not found
     */
    public String getServiceEndpoint(String serviceType) {
        // Check cache first
        ServiceEndpoint cachedEndpoint = endpointCache.get(serviceType);
        if (cachedEndpoint != null && !cachedEndpoint.isExpired()) {
            return cachedEndpoint.getUrl();
        }
        
        // Cache miss or expired, discover service
        try {
            String endpoint = serviceDiscoveryManager.discoverService(serviceType);
            if (endpoint != null) {
                // Cache the discovered endpoint
                endpointCache.put(serviceType, new ServiceEndpoint(endpoint, cacheTtlMs));
                return endpoint;
            }
        } catch (Exception e) {
            LOGGER.error("Error discovering service: {}", serviceType, e);
        }
        
        return null;
    }

    /**
     * Gets the location using the specified provider or falls back to any available provider.
     *
     * @param network  Network information
     * @param callback Callback for location result
     * @param provider Provider name (optional, can be null for automatic selection)
     */
    public void getLocation(Network network, GeolocationProvider.LocationProviderCallback callback, String provider) {
        GeolocationProvider geolocationProvider = getProvider(provider);
        if (geolocationProvider != null) {
            String providerName = provider != null ? provider : "default";
            CircuitBreaker circuitBreaker = circuitBreakers.get(providerName);
            
            try {
                // Create a wrapper callback to track success/failure for circuit breaker
                GeolocationProvider.LocationProviderCallback circuitBreakerCallback = new GeolocationProvider.LocationProviderCallback() {
                    @Override
                    public void onSuccess(double latitude, double longitude, double accuracy) {
                        reportSuccess(providerName);
                        callback.onSuccess(latitude, longitude, accuracy);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        reportFailure(providerName, e);
                        callback.onFailure(e);
                    }
                };
                
                circuitBreaker.decorateRunnable(() -> {
                    geolocationProvider.getLocation(network, circuitBreakerCallback);
                }).run();
            } catch (Exception e) {
                LOGGER.error("Error getting location from provider: {}", providerName, e);
                reportFailure(providerName, e);
                callback.onFailure(e);
            }
        } else {
            callback.onFailure(new RuntimeException("No geolocation provider available"));
        }
    }

    /**
     * Gets the location using any available provider.
     *
     * @param network  Network information
     * @param callback Callback for location result
     */
    public void getLocation(Network network, GeolocationProvider.LocationProviderCallback callback) {
        getLocation(network, callback, null);
    }

    /**
     * Reports a provider failure to the circuit breaker.
     *
     * @param providerName Name of the provider that failed
     * @param exception    The exception that occurred
     */
    public void reportFailure(String providerName, Throwable exception) {
        if (providerName != null && circuitBreakers.containsKey(providerName)) {
            CircuitBreaker circuitBreaker = circuitBreakers.get(providerName);
            circuitBreaker.onError(0, TimeUnit.MILLISECONDS, exception);
            LOGGER.warn("Reported failure for provider: {}", providerName, exception);
        }
    }

    /**
     * Reports a provider success to the circuit breaker.
     *
     * @param providerName Name of the provider that succeeded
     */
    public void reportSuccess(String providerName) {
        if (providerName != null && circuitBreakers.containsKey(providerName)) {
            CircuitBreaker circuitBreaker = circuitBreakers.get(providerName);
            circuitBreaker.onSuccess(0, TimeUnit.MILLISECONDS);
        }
    }
    
    /**
     * Cleans up resources when the service is shutting down.
     */
    @PreDestroy
    public void shutdown() {
        try {
            // Deregister from service discovery
            String serviceName = config.getString(Keys.GEOLOCATION_SERVICE_NAME.getKey(), "geolocation-service");
            serviceDiscoveryManager.deregisterService(serviceName);
            LOGGER.info("Deregistered geolocation service from service discovery");
            
            // Shutdown scheduler
            scheduler.shutdown();
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (Exception e) {
            LOGGER.error("Error during shutdown", e);
            scheduler.shutdownNow();
        }
    }
}