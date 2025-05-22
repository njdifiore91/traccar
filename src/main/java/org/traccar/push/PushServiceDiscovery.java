/*
 * Copyright 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.push;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;
import org.traccar.discovery.ServiceRegistry;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Implements service discovery integration for push notification services.
 * This class registers the push notification service with the service discovery mechanism
 * (Consul/Kubernetes) and provides methods to discover other services.
 */
@Singleton
public class PushServiceDiscovery {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushServiceDiscovery.class);
    private static final String CIRCUIT_BREAKER_NAME = "pushServiceDiscovery";
    private static final String SERVICE_NAME = "push-service";
    private static final String HEALTH_CHECK_PATH = "/health";
    private static final int HEALTH_CHECK_INTERVAL_SECONDS = 30;

    private final ServiceRegistry serviceRegistry;
    private final ServiceDiscovery serviceDiscovery;
    private final Config config;
    private final CircuitBreaker circuitBreaker;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final String instanceId;

    /**
     * Constructor for PushServiceDiscovery with circuit breaker, metrics, and tracing support.
     *
     * @param serviceRegistry Service registry for registering the push service
     * @param serviceDiscovery Service discovery for discovering other services
     * @param config Configuration for the push service
     * @param meterRegistry Metrics registry for collecting performance metrics
     * @param tracer OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public PushServiceDiscovery(ServiceRegistry serviceRegistry, ServiceDiscovery serviceDiscovery,
                               Config config, MeterRegistry meterRegistry, Tracer tracer) {
        this.serviceRegistry = serviceRegistry;
        this.serviceDiscovery = serviceDiscovery;
        this.config = config;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        
        // Generate a unique instance ID for this service instance
        this.instanceId = SERVICE_NAME + "-" + System.currentTimeMillis();
        
        // Initialize circuit breaker with custom configuration
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .slidingWindowSize(10) // Consider last 10 calls for failure rate calculation
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Register circuit breaker state transition listener for logging
        this.circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker '{}' changed state from {} to {}",
                        event.getCircuitBreakerName(), event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));
        
        // Register the push service with the service discovery mechanism
        registerService();
    }

    /**
     * Registers the push notification service with the service discovery mechanism.
     * This method is called during initialization to make the service discoverable by other services.
     */
    private void registerService() {
        Span span = tracer.spanBuilder("push.service.register")
                .startSpan();
        try {
            LOGGER.info("Registering push service with service discovery mechanism");
            
            // Get service host and port from configuration
            String host = config.getString(Keys.PUSH_SERVICE_HOST, "localhost");
            int port = config.getInteger(Keys.PUSH_SERVICE_PORT, 8082);
            
            // Create metadata for the service
            Map<String, String> metadata = new HashMap<>();
            metadata.put("version", getVersion());
            metadata.put("type", "push");
            metadata.put("protocols", "firebase,apns");
            
            // Create a service instance with the required information
            ServiceInstance serviceInstance = ServiceInstance.builder()
                    .id(instanceId)
                    .name(SERVICE_NAME)
                    .host(host)
                    .port(port)
                    .metadata(metadata)
                    .healthCheckPath(HEALTH_CHECK_PATH)
                    .healthCheckInterval(Duration.ofSeconds(HEALTH_CHECK_INTERVAL_SECONDS))
                    .build();
            
            // Register the service with the service registry
            serviceRegistry.register(serviceInstance);
            
            LOGGER.info("Push service registered successfully with ID: {}", instanceId);
        } catch (Exception e) {
            LOGGER.error("Failed to register push service with service discovery mechanism", e);
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Discovers a service by name using the service discovery mechanism.
     * This method uses a circuit breaker to prevent cascading failures.
     *
     * @param serviceName Name of the service to discover
     * @return List of service instances for the requested service, or empty list if none found
     */
    public List<ServiceInstance> discoverService(String serviceName) {
        Span span = tracer.spanBuilder("push.service.discover")
                .setAttribute("service.name", serviceName)
                .startSpan();
        try {
            LOGGER.debug("Discovering service: {}", serviceName);
            
            // Use circuit breaker to prevent cascading failures
            Callable<List<ServiceInstance>> discoveryCallable = () -> serviceDiscovery.getInstances(serviceName);
            List<ServiceInstance> instances = circuitBreaker.executeCallable(discoveryCallable);
            
            LOGGER.debug("Found {} instances of service: {}", instances.size(), serviceName);
            span.setAttribute("service.instances.count", instances.size());
            
            return instances;
        } catch (Exception e) {
            LOGGER.error("Failed to discover service: {}", serviceName, e);
            span.recordException(e);
            throw new RuntimeException("Failed to discover service: " + serviceName, e);
        } finally {
            span.end();
        }
    }

    /**
     * Gets a service URL for the specified service name.
     * This method returns the URL of the first available instance of the service.
     *
     * @param serviceName Name of the service to discover
     * @return URL of the service, or empty if no instances found
     */
    public Optional<String> getServiceUrl(String serviceName) {
        List<ServiceInstance> instances = discoverService(serviceName);
        if (instances.isEmpty()) {
            LOGGER.warn("No instances found for service: {}", serviceName);
            return Optional.empty();
        }
        
        // Get the first available instance
        ServiceInstance instance = instances.get(0);
        String url = String.format("http://%s:%d", instance.getHost(), instance.getPort());
        LOGGER.debug("Using service URL: {} for service: {}", url, serviceName);
        
        return Optional.of(url);
    }

    /**
     * Checks if a service is available in the service discovery registry.
     *
     * @param serviceName Name of the service to check
     * @return true if the service is available, false otherwise
     */
    public boolean isServiceAvailable(String serviceName) {
        try {
            List<ServiceInstance> instances = discoverService(serviceName);
            return !instances.isEmpty();
        } catch (Exception e) {
            LOGGER.warn("Error checking service availability: {}", serviceName, e);
            return false;
        }
    }

    /**
     * Deregisters the push service from the service discovery mechanism.
     * This method should be called during service shutdown to clean up the registration.
     */
    public void deregisterService() {
        Span span = tracer.spanBuilder("push.service.deregister")
                .startSpan();
        try {
            LOGGER.info("Deregistering push service from service discovery mechanism");
            serviceRegistry.deregister(instanceId);
            LOGGER.info("Push service deregistered successfully");
        } catch (Exception e) {
            LOGGER.error("Failed to deregister push service", e);
            span.recordException(e);
        } finally {
            span.end();
        }
    }

    /**
     * Gets the version of the push service.
     * This is used in service metadata for versioning.
     *
     * @return Version string of the push service
     */
    private String getVersion() {
        // Try to get version from package information or configuration
        return Optional.ofNullable(getClass().getPackage().getImplementationVersion())
                .orElse(config.getString(Keys.VERSION, "1.0.0"));
    }
}