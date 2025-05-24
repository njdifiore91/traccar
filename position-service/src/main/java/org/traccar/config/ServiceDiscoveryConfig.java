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
package org.traccar.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.traccar.discovery.ConsulServiceDiscovery;
import org.traccar.discovery.ConsulServiceRegistry;
import org.traccar.discovery.KubernetesServiceDiscovery;
import org.traccar.discovery.KubernetesServiceRegistry;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;
import org.traccar.discovery.ServiceRegistry;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for service discovery in the Position Processing Service.
 * Supports both Consul and Kubernetes as service registry options.
 */
@Configuration
public class ServiceDiscoveryConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryConfig.class);

    @Value("${service.discovery.type:kubernetes}")
    private String serviceDiscoveryType;

    @Value("${service.name:position-service}")
    private String serviceName;

    @Value("${service.port:8080}")
    private int servicePort;

    @Value("${service.discovery.consul.host:localhost}")
    private String consulHost;

    @Value("${service.discovery.consul.port:8500}")
    private int consulPort;

    @Value("${service.discovery.kubernetes.namespace:default}")
    private String kubernetesNamespace;

    @Value("${service.discovery.kubernetes.labelSelector:app=position-service}")
    private String kubernetesLabelSelector;

    /**
     * Creates a ServiceRegistry bean based on the configured service discovery type.
     * This bean is responsible for registering the service with the service registry.
     *
     * @param meterRegistry The meter registry for collecting metrics
     * @return A ServiceRegistry implementation (Consul or Kubernetes)
     */
    @Bean
    public ServiceRegistry serviceRegistry(MeterRegistry meterRegistry) {
        LOGGER.info("Configuring service registry with type: {}", serviceDiscoveryType);

        ServiceRegistry registry;
        if ("consul".equalsIgnoreCase(serviceDiscoveryType)) {
            registry = createConsulServiceRegistry();
            meterRegistry.gauge("service.discovery.consul.registry.status", registry, r -> r.isActive() ? 1 : 0);
        } else {
            registry = createKubernetesServiceRegistry();
            meterRegistry.gauge("service.discovery.kubernetes.registry.status", registry, r -> r.isActive() ? 1 : 0);
        }

        // Register service instance with metadata
        Map<String, String> metadata = new HashMap<>();
        metadata.put("version", getClass().getPackage().getImplementationVersion());
        metadata.put("type", "position-service");

        ServiceInstance instance = ServiceInstance.builder()
                .id(serviceName)
                .name(serviceName)
                .host(getHostName())
                .port(servicePort)
                .metadata(metadata)
                .build();

        registry.register(instance);
        return registry;
    }

    /**
     * Creates a ServiceDiscovery bean based on the configured service discovery type.
     * This bean is responsible for discovering other services in the system.
     *
     * @param meterRegistry The meter registry for collecting metrics
     * @return A ServiceDiscovery implementation (Consul or Kubernetes)
     */
    @Bean
    public ServiceDiscovery serviceDiscovery(MeterRegistry meterRegistry) {
        LOGGER.info("Configuring service discovery with type: {}", serviceDiscoveryType);

        ServiceDiscovery discovery;
        if ("consul".equalsIgnoreCase(serviceDiscoveryType)) {
            discovery = createConsulServiceDiscovery();
            meterRegistry.gauge("service.discovery.consul.discovery.status", discovery, d -> d.isActive() ? 1 : 0);
        } else {
            discovery = createKubernetesServiceDiscovery();
            meterRegistry.gauge("service.discovery.kubernetes.discovery.status", discovery, d -> d.isActive() ? 1 : 0);
        }

        return discovery;
    }

    /**
     * Creates a health indicator for the service registry connection.
     * This is used by Spring Boot Actuator to expose health information.
     *
     * @param registry The service registry bean
     * @return A HealthIndicator for the service registry
     */
    @Bean
    public HealthIndicator serviceRegistryHealthIndicator(ServiceRegistry registry) {
        return () -> {
            if (registry.isActive()) {
                return Health.up()
                        .withDetail("type", serviceDiscoveryType)
                        .withDetail("serviceName", serviceName)
                        .build();
            } else {
                return Health.down()
                        .withDetail("type", serviceDiscoveryType)
                        .withDetail("serviceName", serviceName)
                        .withDetail("reason", "Service registry connection is not active")
                        .build();
            }
        };
    }

    /**
     * Creates a health indicator for the service discovery connection.
     * This is used by Spring Boot Actuator to expose health information.
     *
     * @param discovery The service discovery bean
     * @return A HealthIndicator for the service discovery
     */
    @Bean
    public HealthIndicator serviceDiscoveryHealthIndicator(ServiceDiscovery discovery) {
        return () -> {
            if (discovery.isActive()) {
                return Health.up()
                        .withDetail("type", serviceDiscoveryType)
                        .build();
            } else {
                return Health.down()
                        .withDetail("type", serviceDiscoveryType)
                        .withDetail("reason", "Service discovery connection is not active")
                        .build();
            }
        };
    }

    /**
     * Creates a Consul service registry.
     * This is used when the service discovery type is set to "consul".
     *
     * @return A ConsulServiceRegistry instance
     */
    private ConsulServiceRegistry createConsulServiceRegistry() {
        LOGGER.info("Creating Consul service registry with host: {} and port: {}", consulHost, consulPort);
        return new ConsulServiceRegistry(consulHost, consulPort);
    }

    /**
     * Creates a Kubernetes service registry.
     * This is used when the service discovery type is set to "kubernetes".
     *
     * @return A KubernetesServiceRegistry instance
     */
    private KubernetesServiceRegistry createKubernetesServiceRegistry() {
        LOGGER.info("Creating Kubernetes service registry with namespace: {}", kubernetesNamespace);
        return new KubernetesServiceRegistry(kubernetesNamespace);
    }

    /**
     * Creates a Consul service discovery.
     * This is used when the service discovery type is set to "consul".
     *
     * @return A ConsulServiceDiscovery instance
     */
    private ConsulServiceDiscovery createConsulServiceDiscovery() {
        LOGGER.info("Creating Consul service discovery with host: {} and port: {}", consulHost, consulPort);
        return new ConsulServiceDiscovery(consulHost, consulPort);
    }

    /**
     * Creates a Kubernetes service discovery.
     * This is used when the service discovery type is set to "kubernetes".
     *
     * @return A KubernetesServiceDiscovery instance
     */
    private KubernetesServiceDiscovery createKubernetesServiceDiscovery() {
        LOGGER.info("Creating Kubernetes service discovery with namespace: {} and labelSelector: {}", 
                kubernetesNamespace, kubernetesLabelSelector);
        return new KubernetesServiceDiscovery(kubernetesNamespace, kubernetesLabelSelector);
    }

    /**
     * Gets the hostname of the current machine.
     * This is used for service registration.
     *
     * @return The hostname of the current machine
     */
    private String getHostName() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            LOGGER.warn("Failed to get hostname", e);
            return "unknown";
        }
    }
}