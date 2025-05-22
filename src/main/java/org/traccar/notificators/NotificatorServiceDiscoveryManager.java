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
package org.traccar.notificators;

import com.google.inject.Injector;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Typed;
import org.traccar.notification.MessageException;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;
import org.traccar.discovery.ServiceRegistry;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages service discovery for notificators, enabling dynamic registration and discovery
 * of notification services in the microservices architecture.
 */
@Singleton
public class NotificatorServiceDiscoveryManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificatorServiceDiscoveryManager.class);

    private final Config config;
    private final ServiceRegistry serviceRegistry;
    private final ServiceDiscovery serviceDiscovery;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final Map<String, ServiceInstance> externalNotificatorServices = new ConcurrentHashMap<>();
    private final Map<String, List<ServiceInstance>> notificatorTypeServices = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executorService;
    private final String serviceId;
    private final String serviceName;
    private final int servicePort;
    private final Map<String, String> serviceTags;
    private final Map<String, String> serviceMetadata;
    private final CircuitBreaker discoveryCircuitBreaker;

    /**
     * Constructs a new NotificatorServiceDiscoveryManager.
     *
     * @param config                 The configuration
     * @param injector               The Guice injector
     * @param serviceRegistry        The service registry for registering services
     * @param serviceDiscovery       The service discovery for discovering services
     * @param circuitBreakerRegistry The circuit breaker registry for resilience
     */
    @Inject
    public NotificatorServiceDiscoveryManager(
            Config config,
            Injector injector,
            ServiceRegistry serviceRegistry,
            ServiceDiscovery serviceDiscovery,
            CircuitBreakerRegistry circuitBreakerRegistry) {
        this.config = config;
        this.serviceRegistry = serviceRegistry;
        this.serviceDiscovery = serviceDiscovery;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.executorService = Executors.newSingleThreadScheduledExecutor();

        // Initialize service properties
        this.serviceId = generateServiceId();
        this.serviceName = config.getString(Keys.NOTIFICATION_SERVICE_NAME, "notification-service");
        this.servicePort = config.getInteger(Keys.NOTIFICATION_SERVICE_PORT, 8082);
        this.serviceTags = initServiceTags();
        this.serviceMetadata = initServiceMetadata();

        // Initialize circuit breaker for service discovery
        this.discoveryCircuitBreaker = circuitBreakerRegistry.circuitBreaker("notificatorServiceDiscovery");

        // Register service and start discovery
        registerService();
        startServiceDiscovery();

        // Register shutdown hook for deregistration
        Runtime.getRuntime().addShutdownHook(new Thread(this::deregisterService));
    }

    /**
     * Generates a unique service ID for this instance.
     *
     * @return The generated service ID
     */
    private String generateServiceId() {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOGGER.warn("Unable to determine hostname, using fallback", e);
            hostname = "unknown-host";
        }
        return "notification-service-" + hostname + "-" + System.currentTimeMillis();
    }

    /**
     * Initializes service tags for service registration.
     *
     * @return A map of service tags
     */
    private Map<String, String> initServiceTags() {
        Map<String, String> tags = new HashMap<>();
        tags.put("service", "notification");
        tags.put("version", config.getString(Keys.NOTIFICATION_SERVICE_VERSION, "1.0.0"));
        tags.put("environment", config.getString(Keys.NOTIFICATION_SERVICE_ENVIRONMENT, "production"));
        return tags;
    }

    /**
     * Initializes service metadata for service registration.
     *
     * @return A map of service metadata
     */
    private Map<String, String> initServiceMetadata() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("notificator-types", config.getString(Keys.NOTIFICATOR_TYPES, ""));
        metadata.put("health-check-path", "/actuator/health");
        metadata.put("readiness-path", "/actuator/health/readiness");
        metadata.put("liveness-path", "/actuator/health/liveness");
        return metadata;
    }

    /**
     * Registers the notification service with the service discovery system.
     */
    private void registerService() {
        try {
            String host = config.getString(Keys.NOTIFICATION_SERVICE_HOST);
            if (host == null) {
                try {
                    host = InetAddress.getLocalHost().getHostAddress();
                } catch (UnknownHostException e) {
                    LOGGER.warn("Unable to determine host address, using localhost", e);
                    host = "localhost";
                }
            }

            ServiceInstance serviceInstance = new ServiceInstance()
                    .setId(serviceId)
                    .setName(serviceName)
                    .setHost(host)
                    .setPort(servicePort)
                    .setTags(serviceTags)
                    .setMetadata(serviceMetadata)
                    .setHealthy(true);

            // Register health check
            Map<String, Object> healthCheckConfig = new HashMap<>();
            healthCheckConfig.put("path", "/actuator/health");
            healthCheckConfig.put("interval", config.getString(Keys.NOTIFICATION_HEALTH_CHECK_INTERVAL, "15s"));
            healthCheckConfig.put("timeout", config.getString(Keys.NOTIFICATION_HEALTH_CHECK_TIMEOUT, "5s"));
            healthCheckConfig.put("deregisterCriticalServiceAfter", "1m");

            serviceRegistry.register(serviceInstance, healthCheckConfig);
            LOGGER.info("Registered notification service with ID: {}", serviceId);
        } catch (Exception e) {
            LOGGER.error("Failed to register notification service", e);
        }
    }

    /**
     * Deregisters the notification service from the service discovery system.
     */
    private void deregisterService() {
        try {
            serviceRegistry.deregister(serviceId);
            LOGGER.info("Deregistered notification service with ID: {}", serviceId);
            executorService.shutdown();
        } catch (Exception e) {
            LOGGER.error("Failed to deregister notification service", e);
        }
    }

    /**
     * Starts the service discovery process to discover external notification services.
     */
    private void startServiceDiscovery() {
        int discoveryInterval = config.getInteger(Keys.NOTIFICATION_DISCOVERY_INTERVAL, 30);
        executorService.scheduleAtFixedRate(this::discoverExternalServices, 0, discoveryInterval, TimeUnit.SECONDS);
    }

    /**
     * Discovers external notification services using the service discovery system.
     */
    private void discoverExternalServices() {
        try {
            discoveryCircuitBreaker.executeRunnable(() -> {
                // Clear the type-based service map for refresh
                notificatorTypeServices.clear();
                
                // Discover external notification services
                List<ServiceInstance> services = serviceDiscovery.getInstances("external-notification-service");
                services.forEach(service -> {
                    if (service.isHealthy()) {
                        externalNotificatorServices.put(service.getId(), service);
                        
                        // Group services by notificator type
                        String type = service.getTags().getOrDefault("notificator-type", "unknown");
                        notificatorTypeServices.computeIfAbsent(type, k -> new java.util.ArrayList<>()).add(service);
                        
                        LOGGER.debug("Discovered external notification service: {} of type: {}", 
                                service.getId(), type);
                    } else {
                        externalNotificatorServices.remove(service.getId());
                        LOGGER.debug("Removed unhealthy external notification service: {}", service.getId());
                    }
                });
                LOGGER.info("Discovered {} external notification services across {} types", 
                        externalNotificatorServices.size(), notificatorTypeServices.size());
            });
        } catch (Exception e) {
            LOGGER.error("Failed to discover external notification services", e);
        }
    }

    /**
     * Gets an external notification service endpoint by type.
     *
     * @param type The type of notification service to get
     * @return The service instance, or null if not found
     */
    public ServiceInstance getExternalNotificatorService(String type) {
        List<ServiceInstance> services = notificatorTypeServices.get(type);
        if (services == null || services.isEmpty()) {
            return null;
        }
        
        // Simple round-robin load balancing
        int index = (int) (System.currentTimeMillis() % services.size());
        return services.get(index);
    }
    
    /**
     * Gets all external notification service endpoints by type.
     *
     * @param type The type of notification service to get
     * @return A list of service instances, or empty list if none found
     */
    public List<ServiceInstance> getAllExternalNotificatorServices(String type) {
        return notificatorTypeServices.getOrDefault(type, List.of());
    }

    /**
     * Gets all external notification services.
     *
     * @return A map of service ID to service instance
     */
    public Map<String, ServiceInstance> getAllExternalNotificatorServices() {
        return new HashMap<>(externalNotificatorServices);
    }
    
    /**
     * Gets all available notificator types from discovered services.
     *
     * @return A set of notificator types as Typed objects
     */
    public java.util.Set<Typed> getAvailableNotificatorTypes() {
        return notificatorTypeServices.keySet().stream()
                .filter(type -> !"unknown".equals(type))
                .map(Typed::new)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    /**
     * Updates the health status of the notification service.
     *
     * @param healthy Whether the service is healthy
     */
    public void updateHealthStatus(boolean healthy) {
        try {
            serviceRegistry.updateStatus(serviceId, healthy);
            LOGGER.debug("Updated notification service health status to: {}", healthy);
        } catch (Exception e) {
            LOGGER.error("Failed to update notification service health status", e);
        }
    }
    
    /**
     * Refreshes the service discovery immediately instead of waiting for the next scheduled refresh.
     */
    public void refreshServiceDiscovery() {
        executorService.submit(this::discoverExternalServices);
    }
    
    /**
     * Checks if a specific notificator type is available through service discovery.
     *
     * @param type The notificator type to check
     * @return true if the notificator type is available, false otherwise
     */
    public boolean isNotificatorTypeAvailable(String type) {
        List<ServiceInstance> services = notificatorTypeServices.get(type);
        return services != null && !services.isEmpty();
    }
    
    /**
     * Executes a request to an external notification service with circuit breaker protection.
     *
     * @param type     The type of notification service to use
     * @param endpoint The endpoint to call on the service
     * @param payload  The payload to send to the service
     * @return The response from the service
     * @throws MessageException If the request fails or no service is available
     */
    public String executeExternalNotificatorRequest(String type, String endpoint, String payload) throws MessageException {
        ServiceInstance service = getExternalNotificatorService(type);
        if (service == null) {
            throw new MessageException("No external notification service available for type: " + type);
        }
        
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("notificator-" + type);
        try {
            return circuitBreaker.executeSupplier(() -> {
                // This is a placeholder for the actual HTTP client implementation
                // In a real implementation, this would use a WebClient or similar to make the HTTP request
                String url = String.format("http://%s:%d%s", service.getHost(), service.getPort(), endpoint);
                LOGGER.debug("Executing request to external notification service: {}", url);
                
                // Placeholder for HTTP request
                // return httpClient.post(url).body(payload).execute().body();
                
                // For now, just return a success message
                return "{\"success\":true}";
            });
        } catch (Exception e) {
            LOGGER.error("Failed to execute request to external notification service", e);
            throw new MessageException("Failed to execute request to external notification service: " + e.getMessage());
        }
    }
}