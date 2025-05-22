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
package org.traccar.discovery;

import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.model.health.ServiceHealth;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;

import java.util.List;
import java.util.Random;

/**
 * Service discovery implementation using Consul.
 * This class provides functionality to locate and communicate with other services
 * in the distributed architecture.
 */
@Singleton
public class ServiceDiscovery {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscovery.class);
    
    private final Consul consulClient;
    private final Random random = new Random();
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    
    /**
     * Constructor for ServiceDiscovery.
     * 
     * @param config Configuration for Consul connection
     * @param meterRegistry Registry for metrics collection
     * @param tracer OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public ServiceDiscovery(Config config, MeterRegistry meterRegistry, Tracer tracer) {
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        
        String consulHost = config.getString("consul.host", "localhost");
        int consulPort = config.getInteger("consul.port", 8500);
        
        LOGGER.info("Initializing Consul service discovery client at {}:{}", consulHost, consulPort);
        
        consulClient = Consul.builder()
                .withUrl(String.format("http://%s:%d", consulHost, consulPort))
                .build();
        
        // Register metrics
        Counter.builder("service.discovery.lookup")
                .description("Number of service discovery lookups")
                .register(meterRegistry);
    }
    
    /**
     * Get the URL for a service by name.
     * Uses Consul health checks to find healthy instances of the service.
     * 
     * @param serviceName Name of the service to locate
     * @return URL of the service instance
     * @throws ServiceDiscoveryException if no healthy instances are found
     */
    public String getServiceUrl(String serviceName) throws ServiceDiscoveryException {
        Span span = tracer.spanBuilder("service.discovery.lookup")
                .setParent(Context.current())
                .startSpan();
        
        try {
            span.setAttribute("service.name", serviceName);
            
            // Increment the lookup counter
            meterRegistry.counter("service.discovery.lookup", "service", serviceName).increment();
            
            HealthClient healthClient = consulClient.healthClient();
            List<ServiceHealth> instances = healthClient.getHealthyServiceInstances(serviceName).getResponse();
            
            if (instances.isEmpty()) {
                String errorMsg = "No healthy instances found for service: " + serviceName;
                LOGGER.error(errorMsg);
                span.setAttribute("error", true);
                span.setAttribute("error.message", errorMsg);
                throw new ServiceDiscoveryException(errorMsg);
            }
            
            // Select a random instance for basic load balancing
            ServiceHealth instance = instances.get(random.nextInt(instances.size()));
            
            String address = instance.getService().getAddress();
            int port = instance.getService().getPort();
            
            String serviceUrl = String.format("http://%s:%d", address, port);
            span.setAttribute("service.url", serviceUrl);
            
            LOGGER.debug("Found service {} at {}", serviceName, serviceUrl);
            return serviceUrl;
        } catch (Exception e) {
            String errorMsg = "Error looking up service: " + serviceName;
            LOGGER.error(errorMsg, e);
            span.recordException(e);
            span.setAttribute("error", true);
            throw new ServiceDiscoveryException(errorMsg, e);
        } finally {
            span.end();
        }
    }
    
    /**
     * Exception thrown when service discovery fails.
     */
    public static class ServiceDiscoveryException extends Exception {
        public ServiceDiscoveryException(String message) {
            super(message);
        }
        
        public ServiceDiscoveryException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}