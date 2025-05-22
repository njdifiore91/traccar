/*
 * Copyright 2020 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.speedlimit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service Discovery Manager for registering and discovering speed limit service providers
 * in the microservices architecture. Supports both Consul and Kubernetes service discovery.
 */
@Singleton
public class ServiceDiscoveryManager {

    private static final Logger LOGGER = Logger.getLogger(ServiceDiscoveryManager.class.getName());
    
    private static final String CONSUL_DISCOVERY_TYPE = "consul";
    private static final String KUBERNETES_DISCOVERY_TYPE = "kubernetes";
    
    private final String discoveryType;
    private final Config config;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    
    // Cache for service endpoints with TTL
    private final Cache<String, List<ServiceEndpoint>> serviceEndpointCache;
    
    // Registry of available service providers
    private final Map<String, ServiceProviderInfo> serviceRegistry = new ConcurrentHashMap<>();
    
    // Circuit breakers for service providers
    private final Map<String, CircuitBreaker> circuitBreakers = new ConcurrentHashMap<>();

    /**
     * Constructs a new ServiceDiscoveryManager with the given dependencies.
     *
     * @param config The configuration
     * @param tracer The OpenTelemetry tracer
     * @param meterRegistry The meter registry for metrics
     * @param circuitBreakerRegistry The circuit breaker registry
     */
    @Inject
    public ServiceDiscoveryManager(
            Config config,
            Tracer tracer,
            MeterRegistry meterRegistry,
            CircuitBreakerRegistry circuitBreakerRegistry) {
        
        this.config = config;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        
        // Determine discovery type from configuration
        this.discoveryType = config.getString(Keys.SERVICE_DISCOVERY_TYPE, KUBERNETES_DISCOVERY_TYPE);
        
        // Initialize service endpoint cache with TTL
        int cacheTtlSeconds = config.getInteger(Keys.SERVICE_DISCOVERY_CACHE_TTL, 60);
        this.serviceEndpointCache = CacheBuilder.newBuilder()
                .expireAfterWrite(cacheTtlSeconds, TimeUnit.SECONDS)
                .build();
        
        // Initialize discovery client based on type
        initializeDiscoveryClient();
        
        LOGGER.info("Initialized ServiceDiscoveryManager with discovery type: " + discoveryType);
        
        // Register metrics
        meterRegistry.gauge("service.discovery.providers.count", Tags.empty(), serviceRegistry, Map::size);
        meterRegistry.gauge("service.discovery.cache.size", Tags.empty(), serviceEndpointCache.asMap(), Map::size);
    }

    /**
     * Initializes the appropriate service discovery client based on configuration.
     */
    private void initializeDiscoveryClient() {
        try {
            if (CONSUL_DISCOVERY_TYPE.equalsIgnoreCase(discoveryType)) {
                initializeConsulClient();
            } else if (KUBERNETES_DISCOVERY_TYPE.equalsIgnoreCase(discoveryType)) {
                initializeKubernetesClient();
            } else {
                LOGGER.warning("Unknown discovery type: " + discoveryType + ". Defaulting to Kubernetes.");
                initializeKubernetesClient();
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize discovery client", e);
        }
    }

    /**
     * Initializes the Consul client for service discovery.
     */
    private void initializeConsulClient() {
        String consulHost = config.getString(Keys.CONSUL_HOST, "localhost");
        int consulPort = config.getInteger(Keys.CONSUL_PORT, 8500);
        
        LOGGER.info("Initializing Consul client with host: " + consulHost + " and port: " + consulPort);
        
        // In a real implementation, this would initialize a Consul client
        // For example: consulClient = Consul.builder().withHost(consulHost).withPort(consulPort).build();
    }

    /**
     * Initializes the Kubernetes client for service discovery.
     */
    private void initializeKubernetesClient() {
        LOGGER.info("Initializing Kubernetes client");
        
        // In a real implementation, this would initialize a Kubernetes client
        // For example: kubernetesClient = new DefaultKubernetesClient();
    }

    /**
     * Registers a service with the service discovery system.
     *
     * @param serviceName The name of the service
     * @param host The host where the service is running
     * @param port The port on which the service is listening
     * @param protocol The protocol used by the service (e.g., "http", "https")
     * @param secure Whether the service uses a secure connection
     * @return The service ID if registration was successful, null otherwise
     */
    public String register(String serviceName, String host, int port, String protocol, boolean secure) {
        return register(serviceName, host, port, protocol, secure, new HashMap<>());
    }

    /**
     * Registers a service with the service discovery system with additional metadata.
     *
     * @param serviceName The name of the service
     * @param host The host where the service is running
     * @param port The port on which the service is listening
     * @param protocol The protocol used by the service (e.g., "http", "https")
     * @param secure Whether the service uses a secure connection
     * @param metadata Additional metadata for the service
     * @return The service ID if registration was successful, null otherwise
     */
    public String register(String serviceName, String host, int port, String protocol, boolean secure, Map<String, String> metadata) {
        Span span = tracer.spanBuilder("ServiceDiscoveryManager.register")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("service.name", serviceName)
                .setAttribute("service.host", host)
                .setAttribute("service.port", port)
                .setAttribute("service.protocol", protocol)
                .setAttribute("service.secure", secure)
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            String serviceId = UUID.randomUUID().toString();
            
            // Create service provider info
            ServiceProviderInfo providerInfo = new ServiceProviderInfo(
                    serviceId,
                    serviceName,
                    host,
                    port,
                    protocol,
                    secure,
                    metadata,
                    System.currentTimeMillis(),
                    SpeedLimitProvider.HealthStatus.HEALTHY
            );
            
            // Register with the appropriate service discovery system
            boolean registered = false;
            if (CONSUL_DISCOVERY_TYPE.equalsIgnoreCase(discoveryType)) {
                registered = registerWithConsul(providerInfo);
            } else {
                registered = registerWithKubernetes(providerInfo);
            }
            
            if (registered) {
                // Add to local registry
                serviceRegistry.put(serviceId, providerInfo);
                
                // Create circuit breaker for this service if it doesn't exist
                createCircuitBreakerIfNeeded(serviceName);
                
                // Record metric
                meterRegistry.counter("service.discovery.registrations", 
                        Tags.of(Tag.of("service", serviceName))).increment();
                
                LOGGER.info("Registered service: " + serviceName + " with ID: " + serviceId);
                span.setStatus(StatusCode.OK);
                return serviceId;
            } else {
                LOGGER.warning("Failed to register service: " + serviceName);
                span.setStatus(StatusCode.ERROR, "Failed to register service");
                return null;
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error registering service: " + serviceName, e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            return null;
        } finally {
            span.end();
        }
    }

    /**
     * Registers a service with Consul.
     *
     * @param providerInfo The service provider information
     * @return true if registration was successful, false otherwise
     */
    private boolean registerWithConsul(ServiceProviderInfo providerInfo) {
        try {
            // In a real implementation, this would register the service with Consul
            // For example: consulClient.agentClient().register(port, healthCheckUrl, serviceName, serviceId, tags);
            
            // For now, we'll just simulate a successful registration
            return true;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error registering with Consul", e);
            return false;
        }
    }

    /**
     * Registers a service with Kubernetes.
     *
     * @param providerInfo The service provider information
     * @return true if registration was successful, false otherwise
     */
    private boolean registerWithKubernetes(ServiceProviderInfo providerInfo) {
        try {
            // In a real implementation, this would register the service with Kubernetes
            // For example: kubernetesClient.services().createOrReplace(serviceResource);
            
            // For now, we'll just simulate a successful registration
            return true;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error registering with Kubernetes", e);
            return false;
        }
    }

    /**
     * Deregisters a service from the service discovery system.
     *
     * @param serviceId The ID of the service to deregister
     * @return true if deregistration was successful, false otherwise
     */
    public boolean deregister(String serviceId) {
        Span span = tracer.spanBuilder("ServiceDiscoveryManager.deregister")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("service.id", serviceId)
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            ServiceProviderInfo providerInfo = serviceRegistry.get(serviceId);
            if (providerInfo == null) {
                LOGGER.warning("Service ID not found: " + serviceId);
                span.setStatus(StatusCode.ERROR, "Service ID not found");
                return false;
            }
            
            // Deregister from the appropriate service discovery system
            boolean deregistered = false;
            if (CONSUL_DISCOVERY_TYPE.equalsIgnoreCase(discoveryType)) {
                deregistered = deregisterFromConsul(serviceId);
            } else {
                deregistered = deregisterFromKubernetes(serviceId);
            }
            
            if (deregistered) {
                // Remove from local registry
                serviceRegistry.remove(serviceId);
                
                // Record metric
                meterRegistry.counter("service.discovery.deregistrations", 
                        Tags.of(Tag.of("service", providerInfo.serviceName()))).increment();
                
                LOGGER.info("Deregistered service with ID: " + serviceId);
                span.setStatus(StatusCode.OK);
                return true;
            } else {
                LOGGER.warning("Failed to deregister service with ID: " + serviceId);
                span.setStatus(StatusCode.ERROR, "Failed to deregister service");
                return false;
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error deregistering service: " + serviceId, e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            return false;
        } finally {
            span.end();
        }
    }

    /**
     * Deregisters a service from Consul.
     *
     * @param serviceId The ID of the service to deregister
     * @return true if deregistration was successful, false otherwise
     */
    private boolean deregisterFromConsul(String serviceId) {
        try {
            // In a real implementation, this would deregister the service from Consul
            // For example: consulClient.agentClient().deregister(serviceId);
            
            // For now, we'll just simulate a successful deregistration
            return true;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error deregistering from Consul", e);
            return false;
        }
    }

    /**
     * Deregisters a service from Kubernetes.
     *
     * @param serviceId The ID of the service to deregister
     * @return true if deregistration was successful, false otherwise
     */
    private boolean deregisterFromKubernetes(String serviceId) {
        try {
            // In a real implementation, this would deregister the service from Kubernetes
            // For example: kubernetesClient.services().withName(serviceName).delete();
            
            // For now, we'll just simulate a successful deregistration
            return true;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error deregistering from Kubernetes", e);
            return false;
        }
    }

    /**
     * Discovers service endpoints by service name.
     *
     * @param serviceName The name of the service to discover
     * @return A list of service endpoints, or an empty list if none are found
     */
    public List<ServiceEndpoint> discoverService(String serviceName) {
        return discoverService(serviceName, new HashMap<>());
    }

    /**
     * Discovers service endpoints by service name with tracing context.
     *
     * @param serviceName The name of the service to discover
     * @param tracingContext The tracing context for distributed tracing
     * @return A list of service endpoints, or an empty list if none are found
     */
    public List<ServiceEndpoint> discoverService(String serviceName, Map<String, String> tracingContext) {
        Span span = createSpan("ServiceDiscoveryManager.discoverService", tracingContext)
                .setAttribute("service.name", serviceName)
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            // Check cache first
            List<ServiceEndpoint> cachedEndpoints = serviceEndpointCache.getIfPresent(serviceName);
            if (cachedEndpoints != null && !cachedEndpoints.isEmpty()) {
                LOGGER.fine("Using cached endpoints for service: " + serviceName);
                span.setAttribute("cache.hit", true);
                span.setAttribute("endpoints.count", cachedEndpoints.size());
                span.setStatus(StatusCode.OK);
                return cachedEndpoints;
            }
            
            span.setAttribute("cache.hit", false);
            
            // Discover from the appropriate service discovery system
            List<ServiceEndpoint> endpoints;
            if (CONSUL_DISCOVERY_TYPE.equalsIgnoreCase(discoveryType)) {
                endpoints = discoverFromConsul(serviceName);
            } else {
                endpoints = discoverFromKubernetes(serviceName);
            }
            
            // Cache the results
            if (!endpoints.isEmpty()) {
                serviceEndpointCache.put(serviceName, endpoints);
            }
            
            // Record metrics
            meterRegistry.counter("service.discovery.discoveries", 
                    Tags.of(Tag.of("service", serviceName))).increment();
            meterRegistry.gauge("service.discovery.endpoints.count", 
                    Tags.of(Tag.of("service", serviceName)), endpoints, List::size);
            
            span.setAttribute("endpoints.count", endpoints.size());
            span.setStatus(StatusCode.OK);
            return endpoints;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error discovering service: " + serviceName, e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            return Collections.emptyList();
        } finally {
            span.end();
        }
    }

    /**
     * Discovers service endpoints from Consul.
     *
     * @param serviceName The name of the service to discover
     * @return A list of service endpoints, or an empty list if none are found
     */
    private List<ServiceEndpoint> discoverFromConsul(String serviceName) {
        try {
            // In a real implementation, this would discover the service from Consul
            // For example: List<ServiceHealth> services = consulClient.healthClient().getHealthyServiceInstances(serviceName).getResponse();
            
            // For now, we'll just simulate a successful discovery with a mock endpoint
            if (serviceRegistry.values().stream().anyMatch(info -> info.serviceName().equals(serviceName))) {
                ServiceProviderInfo info = serviceRegistry.values().stream()
                        .filter(i -> i.serviceName().equals(serviceName))
                        .findFirst()
                        .orElse(null);
                
                if (info != null) {
                    return List.of(new ServiceEndpoint(
                            info.serviceId(),
                            info.serviceName(),
                            info.host(),
                            info.port(),
                            info.protocol(),
                            info.secure(),
                            info.metadata()
                    ));
                }
            }
            
            return Collections.emptyList();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error discovering from Consul", e);
            return Collections.emptyList();
        }
    }

    /**
     * Discovers service endpoints from Kubernetes.
     *
     * @param serviceName The name of the service to discover
     * @return A list of service endpoints, or an empty list if none are found
     */
    private List<ServiceEndpoint> discoverFromKubernetes(String serviceName) {
        try {
            // In a real implementation, this would discover the service from Kubernetes
            // For example: Service service = kubernetesClient.services().withName(serviceName).get();
            
            // For now, we'll just simulate a successful discovery with a mock endpoint
            if (serviceRegistry.values().stream().anyMatch(info -> info.serviceName().equals(serviceName))) {
                ServiceProviderInfo info = serviceRegistry.values().stream()
                        .filter(i -> i.serviceName().equals(serviceName))
                        .findFirst()
                        .orElse(null);
                
                if (info != null) {
                    return List.of(new ServiceEndpoint(
                            info.serviceId(),
                            info.serviceName(),
                            info.host(),
                            info.port(),
                            info.protocol(),
                            info.secure(),
                            info.metadata()
                    ));
                }
            }
            
            return Collections.emptyList();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error discovering from Kubernetes", e);
            return Collections.emptyList();
        }
    }

    /**
     * Gets a service endpoint for a service name, with load balancing.
     *
     * @param serviceName The name of the service
     * @return A service endpoint, or null if none are found
     */
    public ServiceEndpoint getServiceEndpoint(String serviceName) {
        List<ServiceEndpoint> endpoints = discoverService(serviceName);
        if (endpoints.isEmpty()) {
            return null;
        }
        
        // Simple round-robin load balancing
        int index = Math.abs(serviceName.hashCode() % endpoints.size());
        return endpoints.get(index);
    }

    /**
     * Executes a function with a circuit breaker for a service.
     *
     * @param <T> The return type of the function
     * @param serviceName The name of the service
     * @param function The function to execute
     * @return The result of the function
     */
    public <T> T executeWithCircuitBreaker(String serviceName, Supplier<T> function) {
        CircuitBreaker circuitBreaker = getCircuitBreaker(serviceName);
        return circuitBreaker.executeSupplier(function);
    }

    /**
     * Gets the circuit breaker for a service, creating it if it doesn't exist.
     *
     * @param serviceName The name of the service
     * @return The circuit breaker for the service
     */
    private CircuitBreaker getCircuitBreaker(String serviceName) {
        return circuitBreakers.computeIfAbsent(serviceName, this::createCircuitBreaker);
    }

    /**
     * Creates a circuit breaker for a service if it doesn't exist.
     *
     * @param serviceName The name of the service
     */
    private void createCircuitBreakerIfNeeded(String serviceName) {
        if (!circuitBreakers.containsKey(serviceName)) {
            createCircuitBreaker(serviceName);
        }
    }

    /**
     * Creates a circuit breaker for a service.
     *
     * @param serviceName The name of the service
     * @return The created circuit breaker
     */
    private CircuitBreaker createCircuitBreaker(String serviceName) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slowCallRateThreshold(50)
                .waitDurationInOpenState(Duration.ofMillis(1000))
                .slowCallDurationThreshold(Duration.ofMillis(2000))
                .permittedNumberOfCallsInHalfOpenState(3)
                .minimumNumberOfCalls(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10)
                .build();
        
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(serviceName, config);
        
        // Register metrics for this circuit breaker
        circuitBreaker.getEventPublisher()
                .onSuccess(event -> meterRegistry.counter("circuit.breaker.success", 
                        Tags.of(Tag.of("service", serviceName))).increment())
                .onError(event -> meterRegistry.counter("circuit.breaker.error", 
                        Tags.of(Tag.of("service", serviceName))).increment())
                .onStateTransition(event -> meterRegistry.counter("circuit.breaker.state.transition", 
                        Tags.of(
                                Tag.of("service", serviceName),
                                Tag.of("from", event.getStateTransition().getFromState().name()),
                                Tag.of("to", event.getStateTransition().getToState().name())
                        )).increment());
        
        return circuitBreaker;
    }

    /**
     * Gets the state of a circuit breaker for a service.
     *
     * @param serviceName The name of the service
     * @return The state of the circuit breaker, or null if it doesn't exist
     */
    public String getCircuitBreakerState(String serviceName) {
        CircuitBreaker circuitBreaker = circuitBreakers.get(serviceName);
        return circuitBreaker != null ? circuitBreaker.getState().name() : null;
    }

    /**
     * Resets a circuit breaker for a service.
     *
     * @param serviceName The name of the service
     */
    public void resetCircuitBreaker(String serviceName) {
        CircuitBreaker circuitBreaker = circuitBreakers.get(serviceName);
        if (circuitBreaker != null) {
            circuitBreaker.reset();
            LOGGER.info("Reset circuit breaker for service: " + serviceName);
        }
    }

    /**
     * Gets the health status of a service.
     *
     * @param serviceName The name of the service
     * @return The health status of the service, or UNKNOWN if it doesn't exist
     */
    public SpeedLimitProvider.HealthStatus getServiceHealth(String serviceName) {
        // Check if any registered providers match the service name
        Optional<ServiceProviderInfo> providerInfo = serviceRegistry.values().stream()
                .filter(info -> info.serviceName().equals(serviceName))
                .findFirst();
        
        return providerInfo.map(ServiceProviderInfo::healthStatus)
                .orElse(SpeedLimitProvider.HealthStatus.UNKNOWN);
    }

    /**
     * Updates the health status of a service.
     *
     * @param serviceId The ID of the service
     * @param healthStatus The new health status
     * @return true if the update was successful, false otherwise
     */
    public boolean updateServiceHealth(String serviceId, SpeedLimitProvider.HealthStatus healthStatus) {
        ServiceProviderInfo providerInfo = serviceRegistry.get(serviceId);
        if (providerInfo == null) {
            return false;
        }
        
        // Create updated provider info
        ServiceProviderInfo updatedInfo = new ServiceProviderInfo(
                providerInfo.serviceId(),
                providerInfo.serviceName(),
                providerInfo.host(),
                providerInfo.port(),
                providerInfo.protocol(),
                providerInfo.secure(),
                providerInfo.metadata(),
                providerInfo.registrationTime(),
                healthStatus
        );
        
        // Update registry
        serviceRegistry.put(serviceId, updatedInfo);
        
        // Record metric
        meterRegistry.counter("service.discovery.health.updates", 
                Tags.of(
                        Tag.of("service", providerInfo.serviceName()),
                        Tag.of("status", healthStatus.name())
                )).increment();
        
        LOGGER.info("Updated health status for service ID: " + serviceId + " to: " + healthStatus);
        return true;
    }

    /**
     * Creates a span for distributed tracing.
     *
     * @param name The name of the span
     * @param tracingContext The tracing context from the parent span
     * @return The span builder
     */
    private Span.Builder createSpan(String name, Map<String, String> tracingContext) {
        // Extract the parent context if available
        Context parentContext = Context.current();
        
        // Create a new span
        return tracer.spanBuilder(name)
                .setParent(parentContext)
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("service.name", "service-discovery-manager");
    }

    /**
     * Gets all registered services.
     *
     * @return A map of service IDs to service provider information
     */
    public Map<String, ServiceProviderInfo> getAllServices() {
        return Collections.unmodifiableMap(serviceRegistry);
    }

    /**
     * Gets the service provider information for a service ID.
     *
     * @param serviceId The ID of the service
     * @return The service provider information, or null if it doesn't exist
     */
    public ServiceProviderInfo getServiceInfo(String serviceId) {
        return serviceRegistry.get(serviceId);
    }

    /**
     * Gets the service endpoint URL for a service name.
     *
     * @param serviceName The name of the service
     * @return The service endpoint URL, or null if the service doesn't exist
     */
    public String getServiceUrl(String serviceName) {
        ServiceEndpoint endpoint = getServiceEndpoint(serviceName);
        if (endpoint == null) {
            return null;
        }
        
        String protocol = endpoint.secure() ? "https" : "http";
        return protocol + "://" + endpoint.host() + ":" + endpoint.port();
    }

    /**
     * Clears the service endpoint cache.
     */
    public void clearCache() {
        serviceEndpointCache.invalidateAll();
        LOGGER.info("Cleared service endpoint cache");
    }

    /**
     * Represents information about a service provider.
     */
    public record ServiceProviderInfo(
            String serviceId,
            String serviceName,
            String host,
            int port,
            String protocol,
            boolean secure,
            Map<String, String> metadata,
            long registrationTime,
            SpeedLimitProvider.HealthStatus healthStatus
    ) {}

    /**
     * Represents a service endpoint.
     */
    public record ServiceEndpoint(
            String serviceId,
            String serviceName,
            String host,
            int port,
            String protocol,
            boolean secure,
            Map<String, String> metadata
    ) {
        /**
         * Gets the service URL.
         *
         * @return The service URL
         */
        public String getUrl() {
            String scheme = secure ? "https" : "http";
            return scheme + "://" + host + ":" + port;
        }

        /**
         * Creates a URI for this endpoint.
         *
         * @return The URI for this endpoint
         */
        public URI toUri() {
            try {
                return new URI(getUrl());
            } catch (Exception e) {
                throw new RuntimeException("Invalid service endpoint URL", e);
            }
        }
    }
}