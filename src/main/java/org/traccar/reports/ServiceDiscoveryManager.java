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
package org.traccar.reports;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ConsulServiceDiscovery;
import org.traccar.discovery.ConsulServiceRegistry;
import org.traccar.discovery.KubernetesServiceDiscovery;
import org.traccar.discovery.KubernetesServiceRegistry;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;
import org.traccar.discovery.ServiceRegistry;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Manages service registration and discovery for the Reporting Service.
 * <p>
 * This class provides the following capabilities:
 * <ul>
 *   <li>Service registration with Consul or Kubernetes service registry at startup</li>
 *   <li>Periodic health status updates to the service registry</li>
 *   <li>Client-side service discovery for locating other services</li>
 *   <li>Circuit breaker pattern implementation for service resilience</li>
 *   <li>Dynamic endpoint resolution for service-to-service communication</li>
 * </ul>
 */
@Singleton
public class ServiceDiscoveryManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryManager.class);

    private static final String SERVICE_NAME = "reporting-service";
    private static final int DEFAULT_SERVICE_PORT = 8080;
    private static final int DEFAULT_HEALTH_CHECK_INTERVAL = 10; // seconds
    private static final int DEFAULT_HEARTBEAT_INTERVAL = 5; // seconds

    private final Config config;
    private final Tracer tracer;
    
    private ServiceRegistry serviceRegistry;
    private ServiceDiscovery serviceDiscovery;
    private String serviceId;
    private int servicePort;
    private ScheduledExecutorService scheduler;
    private CircuitBreakerRegistry circuitBreakerRegistry;
    private RetryRegistry retryRegistry;

    /**
     * Constructs a new ServiceDiscoveryManager with the specified dependencies.
     *
     * @param config the application configuration
     * @param tracer the OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public ServiceDiscoveryManager(Config config, Tracer tracer) {
        this.config = config;
        this.tracer = tracer;
        this.serviceId = SERVICE_NAME + "-" + UUID.randomUUID().toString();
        this.servicePort = config.getInteger("service.port", DEFAULT_SERVICE_PORT);
        this.scheduler = Executors.newScheduledThreadPool(1);
        
        // Initialize circuit breaker registry with default configuration
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        
        // Initialize retry registry with default configuration
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(Exception.class)
                .build();
        this.retryRegistry = RetryRegistry.of(retryConfig);
    }

    /**
     * Initializes the service discovery manager and registers the service with the registry.
     * This method is called automatically after dependency injection.
     */
    @PostConstruct
    public void init() {
        String discoveryType = config.getString("service.discovery.type", "consul").toLowerCase();
        
        try {
            // Initialize service registry and discovery based on configuration
            if ("kubernetes".equals(discoveryType)) {
                initializeKubernetesDiscovery();
            } else {
                // Default to Consul
                initializeConsulDiscovery();
            }
            
            // Register service with the registry
            registerService();
            
            // Start heartbeat scheduler
            startHeartbeat();
            
            LOGGER.info("Service discovery initialized with {} registry. Service registered as {}", 
                    discoveryType, serviceId);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize service discovery", e);
        }
    }

    /**
     * Initializes Kubernetes-based service registry and discovery.
     */
    private void initializeKubernetesDiscovery() {
        this.serviceRegistry = new KubernetesServiceRegistry(config);
        this.serviceDiscovery = new KubernetesServiceDiscovery(config);
    }

    /**
     * Initializes Consul-based service registry and discovery.
     */
    private void initializeConsulDiscovery() {
        this.serviceRegistry = new ConsulServiceRegistry(config);
        this.serviceDiscovery = new ConsulServiceDiscovery(config);
    }

    /**
     * Registers the Reporting Service with the service registry.
     */
    private void registerService() {
        String host = config.getString("service.host", "localhost");
        Map<String, String> metadata = Map.of(
                "version", config.getString("service.version", "1.0.0"),
                "environment", config.getString("service.environment", "production")
        );
        
        // Register service with health check endpoint
        String healthCheckEndpoint = "/health";
        int healthCheckInterval = config.getInteger("service.healthCheck.interval", DEFAULT_HEALTH_CHECK_INTERVAL);
        
        serviceRegistry.register(serviceId, SERVICE_NAME, host, servicePort, metadata, healthCheckEndpoint, healthCheckInterval);
        LOGGER.info("Registered service: {} at {}:{} with health check endpoint: {}", 
                SERVICE_NAME, host, servicePort, healthCheckEndpoint);
    }

    /**
     * Starts a scheduled task to send heartbeats to the service registry.
     */
    private void startHeartbeat() {
        int heartbeatInterval = config.getInteger("service.heartbeat.interval", DEFAULT_HEARTBEAT_INTERVAL);
        
        scheduler.scheduleAtFixedRate(() -> {
            try {
                serviceRegistry.updateStatus(serviceId, true);
                LOGGER.debug("Sent heartbeat for service: {}", serviceId);
            } catch (Exception e) {
                LOGGER.warn("Failed to send heartbeat", e);
            }
        }, heartbeatInterval, heartbeatInterval, TimeUnit.SECONDS);
    }

    /**
     * Discovers service instances by service name.
     *
     * @param serviceName the name of the service to discover
     * @return a list of service instances, or an empty list if none found
     */
    public List<ServiceInstance> discoverService(String serviceName) {
        Span span = tracer.spanBuilder("discoverService").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("service.name", serviceName);
            
            CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("discovery-" + serviceName);
            Retry retry = retryRegistry.retry("discovery-" + serviceName);
            
            Supplier<List<ServiceInstance>> discoverySupplier = () -> {
                try {
                    List<ServiceInstance> instances = serviceDiscovery.findServiceInstances(serviceName);
                    span.setAttribute("service.instances.count", instances.size());
                    return instances;
                } catch (Exception e) {
                    LOGGER.warn("Failed to discover service: {}", serviceName, e);
                    span.recordException(e);
                    throw e;
                }
            };
            
            // Apply circuit breaker and retry patterns
            return Retry.decorateSupplier(retry, 
                    CircuitBreaker.decorateSupplier(circuitBreaker, discoverySupplier))
                    .get();
        } catch (Exception e) {
            LOGGER.error("Service discovery failed for: {}", serviceName, e);
            span.recordException(e);
            span.end();
            return Collections.emptyList();
        } finally {
            if (span != null) {
                span.end();
            }
        }
    }

    /**
     * Discovers healthy service instances by service name.
     *
     * @param serviceName the name of the service to discover
     * @return a list of healthy service instances, or an empty list if none found
     */
    public List<ServiceInstance> discoverHealthyService(String serviceName) {
        Span span = tracer.spanBuilder("discoverHealthyService").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("service.name", serviceName);
            
            CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("discovery-healthy-" + serviceName);
            Retry retry = retryRegistry.retry("discovery-healthy-" + serviceName);
            
            Supplier<List<ServiceInstance>> discoverySupplier = () -> {
                try {
                    List<ServiceInstance> instances = serviceDiscovery.findHealthyServiceInstances(serviceName);
                    span.setAttribute("service.instances.count", instances.size());
                    return instances;
                } catch (Exception e) {
                    LOGGER.warn("Failed to discover healthy service: {}", serviceName, e);
                    span.recordException(e);
                    throw e;
                }
            };
            
            // Apply circuit breaker and retry patterns
            return Retry.decorateSupplier(retry, 
                    CircuitBreaker.decorateSupplier(circuitBreaker, discoverySupplier))
                    .get();
        } catch (Exception e) {
            LOGGER.error("Healthy service discovery failed for: {}", serviceName, e);
            span.recordException(e);
            span.end();
            return Collections.emptyList();
        } finally {
            if (span != null) {
                span.end();
            }
        }
    }

    /**
     * Gets a service instance for the specified service name.
     * This method applies load balancing to select one instance from available healthy instances.
     *
     * @param serviceName the name of the service to discover
     * @return a service instance, or null if none found
     */
    public ServiceInstance getServiceInstance(String serviceName) {
        List<ServiceInstance> instances = discoverHealthyService(serviceName);
        if (instances.isEmpty()) {
            LOGGER.warn("No healthy instances found for service: {}", serviceName);
            return null;
        }
        
        // Simple round-robin load balancing based on the current time
        int index = (int) (System.currentTimeMillis() % instances.size());
        return instances.get(index);
    }

    /**
     * Gets the URL for a service instance.
     *
     * @param serviceName the name of the service to discover
     * @param path the path to append to the service URL (optional)
     * @return the service URL, or null if no instance found
     */
    public String getServiceUrl(String serviceName, String path) {
        ServiceInstance instance = getServiceInstance(serviceName);
        if (instance == null) {
            return null;
        }
        
        String baseUrl = String.format("http://%s:%d", instance.getHost(), instance.getPort());
        if (path != null && !path.isEmpty()) {
            return path.startsWith("/") ? baseUrl + path : baseUrl + "/" + path;
        }
        return baseUrl;
    }

    /**
     * Executes a function with circuit breaker and retry patterns applied.
     *
     * @param <T> the return type of the function
     * @param serviceName the name of the service being called (for circuit breaker naming)
     * @param operation the operation name (for tracing)
     * @param supplier the function to execute
     * @return the result of the function, or null if execution fails
     */
    public <T> T executeWithResilience(String serviceName, String operation, Supplier<T> supplier) {
        Span span = tracer.spanBuilder(operation).startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("service.name", serviceName);
            span.setAttribute("operation", operation);
            
            CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("service-" + serviceName);
            Retry retry = retryRegistry.retry("service-" + serviceName);
            
            return Retry.decorateSupplier(retry, 
                    CircuitBreaker.decorateSupplier(circuitBreaker, supplier))
                    .get();
        } catch (Exception e) {
            LOGGER.error("Operation {} failed for service: {}", operation, serviceName, e);
            span.recordException(e);
            return null;
        } finally {
            span.end();
        }
    }

    /**
     * Cleans up resources when the service is shutting down.
     * This method is called automatically before the bean is destroyed.
     */
    @PreDestroy
    public void shutdown() {
        try {
            // Deregister service from the registry
            if (serviceRegistry != null && serviceId != null) {
                serviceRegistry.deregister(serviceId);
                LOGGER.info("Deregistered service: {}", serviceId);
            }
            
            // Shutdown scheduler
            if (scheduler != null) {
                scheduler.shutdown();
                try {
                    if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    scheduler.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error during service discovery shutdown", e);
        }
    }
}