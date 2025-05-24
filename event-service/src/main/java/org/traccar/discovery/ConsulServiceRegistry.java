/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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

import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.NotRegisteredException;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import com.orbitz.consul.model.health.ServiceHealth;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implementation of the ServiceRegistry interface for Consul specifically for the Event Processing Service.
 * It handles service registration, deregistration, and discovery using the Consul API.
 * This implementation enables the Event Processing Service to integrate with Consul for service discovery
 * in environments where Consul is the preferred registry, allowing other services to locate and communicate with it.
 */
public class ConsulServiceRegistry implements ServiceRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsulServiceRegistry.class);

    private final Consul consul;
    private final ServiceRegistryConfig config;
    private final HealthCheck healthCheck;
    private final Map<String, ServiceInstance> registeredServices;
    private final ScheduledExecutorService scheduler;

    /**
     * Constructs a new ConsulServiceRegistry with the specified configuration and health check.
     *
     * @param config      The service registry configuration
     * @param healthCheck The health check implementation for the service
     */
    @Inject
    public ConsulServiceRegistry(ServiceRegistryConfig config, HealthCheck healthCheck) {
        this.config = config;
        this.healthCheck = healthCheck;
        this.registeredServices = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(1);

        // Create Consul client with configured host and port
        Consul.Builder builder = Consul.builder();
        if (config.getConsulHost() != null && !config.getConsulHost().isEmpty()) {
            builder.withUrl(String.format("http://%s:%d", config.getConsulHost(), config.getConsulPort()));
        }
        this.consul = builder.build();

        LOGGER.info("Initialized Consul Service Registry with host: {}, port: {}",
                config.getConsulHost(), config.getConsulPort());
    }

    /**
     * Registers a service instance with Consul.
     *
     * @param serviceInstance The service instance to register
     * @return true if registration was successful, false otherwise
     */
    @Override
    public boolean register(ServiceInstance serviceInstance) {
        try {
            // Build the registration with event service specific configuration
            Registration.RegCheck regCheck;
            if (config.isHttpHealthCheckEnabled()) {
                // HTTP health check for event service
                regCheck = Registration.RegCheck.http(
                        String.format("http://%s:%d%s", 
                                serviceInstance.getHost(),
                                serviceInstance.getPort(),
                                config.getHealthCheckPath()),
                        config.getHealthCheckInterval());
            } else {
                // TTL health check as fallback
                regCheck = Registration.RegCheck.ttl(config.getHealthCheckTtl());
            }

            // Create the registration with event service specific metadata
            Registration registration = ImmutableRegistration.builder()
                    .id(serviceInstance.getId())
                    .name(serviceInstance.getName())
                    .address(serviceInstance.getHost())
                    .port(serviceInstance.getPort())
                    .check(regCheck)
                    .tags(new ArrayList<>(serviceInstance.getTags()))
                    .meta(serviceInstance.getMetadata())
                    .build();

            // Register the service with Consul
            consul.agentClient().register(registration);
            registeredServices.put(serviceInstance.getId(), serviceInstance);

            // If using TTL health check, schedule regular check-ins
            if (!config.isHttpHealthCheckEnabled()) {
                scheduleTtlHealthUpdates(serviceInstance.getId());
            }

            LOGGER.info("Successfully registered event service: {} ({})", 
                    serviceInstance.getName(), serviceInstance.getId());
            return true;
        } catch (ConsulException e) {
            LOGGER.error("Failed to register event service with Consul: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Deregisters a service instance from Consul.
     *
     * @param serviceId The ID of the service to deregister
     * @return true if deregistration was successful, false otherwise
     */
    @Override
    public boolean deregister(String serviceId) {
        try {
            consul.agentClient().deregister(serviceId);
            registeredServices.remove(serviceId);
            LOGGER.info("Successfully deregistered event service: {}", serviceId);
            return true;
        } catch (ConsulException e) {
            LOGGER.error("Failed to deregister event service from Consul: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Retrieves service instances by name from Consul.
     *
     * @param serviceName The name of the service to look up
     * @return A list of service instances with the specified name
     */
    @Override
    public List<ServiceInstance> getServiceInstances(String serviceName) {
        try {
            // Query Consul for healthy service instances
            List<ServiceHealth> healthyServices = consul.healthClient()
                    .getHealthyServiceInstances(serviceName).getResponse();

            // Convert Consul service health objects to ServiceInstance objects
            return healthyServices.stream()
                    .map(serviceHealth -> {
                        String id = serviceHealth.getService().getId();
                        String name = serviceHealth.getService().getService();
                        String host = serviceHealth.getService().getAddress();
                        int port = serviceHealth.getService().getPort();
                        List<String> tags = serviceHealth.getService().getTags();
                        Map<String, String> meta = serviceHealth.getService().getMeta();

                        return ServiceInstance.builder()
                                .id(id)
                                .name(name)
                                .host(host)
                                .port(port)
                                .tags(tags)
                                .metadata(meta)
                                .build();
                    })
                    .collect(Collectors.toList());
        } catch (ConsulException e) {
            LOGGER.error("Failed to retrieve event service instances from Consul: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    /**
     * Schedules regular TTL health check updates for a service.
     *
     * @param serviceId The ID of the service to update health status for
     */
    private void scheduleTtlHealthUpdates(String serviceId) {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (healthCheck.isHealthy()) {
                    consul.agentClient().pass(serviceId);
                    LOGGER.debug("Updated TTL health check for event service: {}", serviceId);
                } else {
                    consul.agentClient().warn(serviceId, "Event service health check warning");
                    LOGGER.warn("Event service health check warning for: {}", serviceId);
                }
            } catch (NotRegisteredException e) {
                LOGGER.error("Failed to update TTL health check for event service: {}", e.getMessage(), e);
            }
        }, 0, config.getHealthCheckUpdateInterval(), TimeUnit.SECONDS);
    }

    /**
     * Updates the health status of a registered service.
     *
     * @param serviceId The ID of the service to update
     * @param status    The new health status
     * @return true if the update was successful, false otherwise
     */
    @Override
    public boolean updateHealth(String serviceId, String status) {
        try {
            switch (status.toLowerCase()) {
                case "passing":
                    consul.agentClient().pass(serviceId);
                    break;
                case "warning":
                    consul.agentClient().warn(serviceId, "Event service health check warning");
                    break;
                case "critical":
                    consul.agentClient().fail(serviceId, "Event service health check critical");
                    break;
                default:
                    LOGGER.warn("Unknown health status: {}", status);
                    return false;
            }
            return true;
        } catch (NotRegisteredException e) {
            LOGGER.error("Failed to update health status for event service: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Shuts down the service registry and deregisters all services.
     */
    @Override
    public void shutdown() {
        // Shutdown the scheduler to prevent further health check updates
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }

        // Deregister all services
        for (String serviceId : registeredServices.keySet()) {
            try {
                consul.agentClient().deregister(serviceId);
                LOGGER.info("Deregistered event service during shutdown: {}", serviceId);
            } catch (ConsulException e) {
                LOGGER.error("Failed to deregister event service during shutdown: {}", e.getMessage(), e);
            }
        }
        registeredServices.clear();
    }
}