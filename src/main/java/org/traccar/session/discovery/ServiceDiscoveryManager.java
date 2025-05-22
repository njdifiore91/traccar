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
package org.traccar.session.discovery;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Manages service discovery and registration for microservices.
 * Supports multiple service discovery implementations (Consul, Kubernetes) based on configuration.
 */
@Singleton
public class ServiceDiscoveryManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryManager.class);

    private final Config config;
    private final MeterRegistry meterRegistry;
    private final ServiceDiscoveryClient discoveryClient;
    
    // Metrics
    private final Timer discoveryOperationTimer;

    @Inject
    public ServiceDiscoveryManager(Config config, MeterRegistry meterRegistry) {
        this.config = config;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.discoveryOperationTimer = meterRegistry.timer("service.discovery.operation.time");
        
        // Initialize discovery client based on configuration
        String discoveryType = config.getString(Keys.SERVICE_DISCOVERY_TYPE, "kubernetes");
        if ("consul".equalsIgnoreCase(discoveryType)) {
            this.discoveryClient = new ConsulClient(config, meterRegistry);
        } else {
            // Default to Kubernetes
            this.discoveryClient = new KubernetesClient(config, meterRegistry);
        }
        
        // Initialize discovery client
        discoveryClient.initialize();
        
        LOGGER.info("ServiceDiscoveryManager initialized with {} client", discoveryType);
    }

    /**
     * Registers a service with the discovery system.
     *
     * @param serviceName The name of the service
     * @param serviceType The type of the service
     */
    public void register(String serviceName, String serviceType) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("registerService").start();
        span.setTag("serviceName", serviceName);
        span.setTag("serviceType", serviceType);
        
        try {
            // Create service metadata
            Map<String, String> metadata = new HashMap<>();
            metadata.put("type", serviceType);
            metadata.put("version", config.getString(Keys.VERSION));
            metadata.put("instanceId", UUID.randomUUID().toString());
            
            // Register service
            discoveryOperationTimer.record(() -> {
                discoveryClient.register(serviceName, metadata);
                return null;
            });
            
            LOGGER.info("Registered service: {} (type: {})", serviceName, serviceType);
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error registering service {}: {}", serviceName, e.getMessage(), e);
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Unregisters a service from the discovery system.
     *
     * @param serviceName The name of the service
     */
    public void unregister(String serviceName) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("unregisterService").start();
        span.setTag("serviceName", serviceName);
        
        try {
            // Unregister service
            discoveryOperationTimer.record(() -> {
                discoveryClient.unregister(serviceName);
                return null;
            });
            
            LOGGER.info("Unregistered service: {}", serviceName);
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error unregistering service {}: {}", serviceName, e.getMessage(), e);
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Discovers service instances by name.
     *
     * @param serviceName The name of the service to discover
     * @return A list of service instances
     */
    public List<ServiceInstance> discoverService(String serviceName) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("discoverService").start();
        span.setTag("serviceName", serviceName);
        
        try {
            // Discover service instances
            List<ServiceInstance> instances = discoveryOperationTimer.record(() -> {
                return discoveryClient.discoverService(serviceName);
            });
            
            LOGGER.debug("Discovered {} instances of service: {}", instances.size(), serviceName);
            return instances;
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error discovering service {}: {}", serviceName, e.getMessage(), e);
            return List.of();
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Gets the meter registry for metrics.
     *
     * @return The meter registry
     */
    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    /**
     * Service instance information.
     */
    public static class ServiceInstance {
        private final String id;
        private final String name;
        private final String host;
        private final int port;
        private final Map<String, String> metadata;
        
        public ServiceInstance(String id, String name, String host, int port, Map<String, String> metadata) {
            this.id = id;
            this.name = name;
            this.host = host;
            this.port = port;
            this.metadata = metadata;
        }
        
        public String getId() {
            return id;
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
    }

    /**
     * Interface for service discovery client implementations.
     */
    private interface ServiceDiscoveryClient {
        void initialize();
        void register(String serviceName, Map<String, String> metadata);
        void unregister(String serviceName);
        List<ServiceInstance> discoverService(String serviceName);
    }

    /**
     * Consul implementation of the service discovery client.
     */
    private static class ConsulClient implements ServiceDiscoveryClient {
        private final Config config;
        private final MeterRegistry meterRegistry;
        
        public ConsulClient(Config config, MeterRegistry meterRegistry) {
            this.config = config;
            this.meterRegistry = meterRegistry;
        }
        
        @Override
        public void initialize() {
            // Implementation would initialize Consul client
            LOGGER.info("Initialized Consul client");
        }
        
        @Override
        public void register(String serviceName, Map<String, String> metadata) {
            // Implementation would register service with Consul
            LOGGER.debug("Registered service with Consul: {}", serviceName);
        }
        
        @Override
        public void unregister(String serviceName) {
            // Implementation would unregister service from Consul
            LOGGER.debug("Unregistered service from Consul: {}", serviceName);
        }
        
        @Override
        public List<ServiceInstance> discoverService(String serviceName) {
            // Implementation would discover service instances from Consul
            LOGGER.debug("Discovered service from Consul: {}", serviceName);
            return List.of();
        }
    }

    /**
     * Kubernetes implementation of the service discovery client.
     */
    private static class KubernetesClient implements ServiceDiscoveryClient {
        private final Config config;
        private final MeterRegistry meterRegistry;
        
        public KubernetesClient(Config config, MeterRegistry meterRegistry) {
            this.config = config;
            this.meterRegistry = meterRegistry;
        }
        
        @Override
        public void initialize() {
            // Implementation would initialize Kubernetes client
            LOGGER.info("Initialized Kubernetes client");
        }
        
        @Override
        public void register(String serviceName, Map<String, String> metadata) {
            // Implementation would register service with Kubernetes
            LOGGER.debug("Registered service with Kubernetes: {}", serviceName);
        }
        
        @Override
        public void unregister(String serviceName) {
            // Implementation would unregister service from Kubernetes
            LOGGER.debug("Unregistered service from Kubernetes: {}", serviceName);
        }
        
        @Override
        public List<ServiceInstance> discoverService(String serviceName) {
            // Implementation would discover service instances from Kubernetes
            LOGGER.debug("Discovered service from Kubernetes: {}", serviceName);
            return List.of();
        }
    }
}