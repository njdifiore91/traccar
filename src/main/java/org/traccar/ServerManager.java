/*
 * Copyright 2012 - 2022 Anton Tananaev (anton@traccar.org)
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

import com.google.inject.Injector;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;
import org.traccar.discovery.ServiceRegistry;
import org.traccar.helper.ClassScanner;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Server manager that handles protocol connectors and service registration.
 * Provides resilient communication with circuit breakers and distributed tracing.
 */
@Singleton
public class ServerManager implements LifecycleObject {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerManager.class);

    private final List<TrackerConnector> connectorList = new LinkedList<>();
    private final Map<String, BaseProtocol> protocolList = new ConcurrentHashMap<>();
    
    private final Config config;
    private final ServiceRegistry serviceRegistry;
    private final ServiceDiscovery serviceDiscovery;
    private final OpenTelemetry openTelemetry;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final MeterRegistry meterRegistry;
    
    // Metrics
    private LongCounter protocolMessagesCounter;
    private LongCounter connectionCounter;
    private LongCounter errorCounter;
    
    // Service instance for this server
    private ServiceInstance serviceInstance;
    
    // Circuit breaker for service discovery
    private CircuitBreaker discoveryCircuitBreaker;

    /**
     * Constructs a new ServerManager with dependencies injected.
     *
     * @param injector Guice injector for creating protocol instances
     * @param config Configuration for the server
     * @param serviceRegistry Service registry for registering this server
     * @param serviceDiscovery Service discovery for finding other services
     * @param openTelemetry OpenTelemetry for distributed tracing and metrics
     * @param meterRegistry Micrometer registry for metrics collection
     * @throws IOException If there is an error loading protocols
     * @throws URISyntaxException If there is an error with URIs
     * @throws ReflectiveOperationException If there is an error creating protocol instances
     */
    @Inject
    public ServerManager(
            Injector injector, 
            Config config, 
            ServiceRegistry serviceRegistry,
            ServiceDiscovery serviceDiscovery,
            OpenTelemetry openTelemetry,
            MeterRegistry meterRegistry) throws IOException, URISyntaxException, ReflectiveOperationException {
        
        this.config = config;
        this.serviceRegistry = serviceRegistry;
        this.serviceDiscovery = serviceDiscovery;
        this.openTelemetry = openTelemetry;
        this.meterRegistry = meterRegistry;
        
        // Initialize circuit breaker registry
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(10)
                .slidingWindowSize(100)
                .build();
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.discoveryCircuitBreaker = circuitBreakerRegistry.circuitBreaker("service-discovery");
        
        // Initialize metrics
        initializeMetrics();
        
        // Load protocols from configuration
        loadProtocols(injector);
        
        // Load protocols from service discovery if enabled
        if (config.getBoolean(Keys.SERVER_DYNAMIC_PROTOCOL_RESOLUTION_ENABLED)) {
            loadProtocolsFromServiceDiscovery(injector);
        }
    }
    
    /**
     * Initializes metrics for monitoring server performance.
     */
    private void initializeMetrics() {
        // Register JVM and system metrics
        new JvmMemoryMetrics().bindTo(meterRegistry);
        new ProcessorMetrics().bindTo(meterRegistry);
        
        // Create OpenTelemetry meter and metrics
        Meter meter = openTelemetry.getMeter("org.traccar.server");
        
        protocolMessagesCounter = meter.counterBuilder("traccar.protocol.messages")
                .setDescription("Number of protocol messages processed")
                .setUnit("messages")
                .build();
        
        connectionCounter = meter.counterBuilder("traccar.connections")
                .setDescription("Number of device connections")
                .setUnit("connections")
                .build();
        
        errorCounter = meter.counterBuilder("traccar.errors")
                .setDescription("Number of errors encountered")
                .setUnit("errors")
                .build();
    }
    
    /**
     * Loads protocols from the classpath based on configuration.
     *
     * @param injector Guice injector for creating protocol instances
     * @throws IOException If there is an error loading protocols
     * @throws URISyntaxException If there is an error with URIs
     * @throws ReflectiveOperationException If there is an error creating protocol instances
     */
    private void loadProtocols(Injector injector) throws IOException, URISyntaxException, ReflectiveOperationException {
        Set<String> enabledProtocols = null;
        if (config.hasKey(Keys.PROTOCOLS_ENABLE)) {
            enabledProtocols = new HashSet<>(Arrays.asList(config.getString(Keys.PROTOCOLS_ENABLE).split("[, ]")));
        }
        
        for (Class<?> protocolClass : ClassScanner.findSubclasses(BaseProtocol.class, "org.traccar.protocol")) {
            String protocolName = BaseProtocol.nameFromClass(protocolClass);
            if (enabledProtocols == null || enabledProtocols.contains(protocolName)) {
                if (config.getInteger(Keys.PROTOCOL_PORT.withPrefix(protocolName)) > 0) {
                    BaseProtocol protocol = (BaseProtocol) injector.getInstance(protocolClass);
                    connectorList.addAll(protocol.getConnectorList());
                    protocolList.put(protocol.getName(), protocol);
                    LOGGER.info("Loaded protocol: {}", protocolName);
                }
            }
        }
    }
    
    /**
     * Loads protocols from service discovery.
     *
     * @param injector Guice injector for creating protocol instances
     */
    private void loadProtocolsFromServiceDiscovery(Injector injector) {
        try {
            // Use circuit breaker to protect against service discovery failures
            List<ServiceInstance> protocolServices = discoveryCircuitBreaker.executeSupplier(() -> 
                    serviceDiscovery.findServicesByType("protocol"));
            
            for (ServiceInstance serviceInstance : protocolServices) {
                String protocolName = serviceInstance.getName();
                if (!protocolList.containsKey(protocolName)) {
                    LOGGER.info("Discovered protocol service: {}", protocolName);
                    // Protocol implementation details would be fetched from the service instance
                    // This is a simplified implementation - actual implementation would depend on
                    // how protocols are registered and discovered in the microservices architecture
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to load protocols from service discovery", e);
            errorCounter.add(1, Attributes.builder().put("type", "service_discovery").build());
        }
    }

    /**
     * Gets a protocol by name.
     *
     * @param name Protocol name
     * @return Protocol instance or null if not found
     */
    public BaseProtocol getProtocol(String name) {
        BaseProtocol protocol = protocolList.get(name);
        
        // If protocol not found locally, try to discover it
        if (protocol == null && config.getBoolean(Keys.SERVER_DYNAMIC_PROTOCOL_RESOLUTION_ENABLED)) {
            try {
                // Use circuit breaker to protect against service discovery failures
                ServiceInstance protocolService = discoveryCircuitBreaker.executeSupplier(() -> 
                        serviceDiscovery.findServiceByName(name));
                
                if (protocolService != null) {
                    LOGGER.info("Dynamically discovered protocol: {}", name);
                    // Protocol would be fetched from the service - simplified implementation
                    // Actual implementation would depend on the microservices architecture
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to discover protocol: {}", name, e);
                errorCounter.add(1, Attributes.builder().put("type", "protocol_discovery").build());
            }
        }
        
        return protocol;
    }
    
    /**
     * Performs a health check for this server.
     *
     * @return true if the server is healthy, false otherwise
     */
    public boolean isHealthy() {
        // Check if all connectors are running
        for (TrackerConnector connector : connectorList) {
            if (!connector.isRunning()) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Registers this server with the service registry.
     */
    private void registerService() {
        try {
            String hostname = config.getString(Keys.SERVER_HOSTNAME);
            int port = config.getInteger(Keys.SERVER_PORT);
            
            serviceInstance = ServiceInstance.builder()
                    .name("traccar-server")
                    .host(hostname)
                    .port(port)
                    .addMetadata("version", Main.getVersion())
                    .build();
            
            serviceRegistry.register(serviceInstance);
            LOGGER.info("Registered server with service registry: {}:{}", hostname, port);
        } catch (Exception e) {
            LOGGER.warn("Failed to register with service registry", e);
            errorCounter.add(1, Attributes.builder().put("type", "service_registration").build());
        }
    }
    
    /**
     * Deregisters this server from the service registry.
     */
    private void deregisterService() {
        if (serviceInstance != null) {
            try {
                serviceRegistry.deregister(serviceInstance);
                LOGGER.info("Deregistered server from service registry");
            } catch (Exception e) {
                LOGGER.warn("Failed to deregister from service registry", e);
                errorCounter.add(1, Attributes.builder().put("type", "service_deregistration").build());
            }
        }
    }

    @Override
    public void start() throws Exception {
        // Get tracer for start operation
        Tracer tracer = openTelemetry.getTracer("org.traccar.ServerManager");
        
        // Start all connectors with tracing and metrics
        tracer.spanBuilder("ServerManager.start").startSpan().run(() -> {
            for (TrackerConnector connector: connectorList) {
                try {
                    connector.start();
                    connectionCounter.add(1);
                } catch (BindException e) {
                    LOGGER.warn("Port disabled due to conflict", e);
                    errorCounter.add(1, Attributes.builder().put("type", "bind_exception").build());
                } catch (ConnectException e) {
                    LOGGER.warn("Connection failed", e);
                    errorCounter.add(1, Attributes.builder().put("type", "connect_exception").build());
                } catch (Exception e) {
                    LOGGER.warn("Failed to start connector", e);
                    errorCounter.add(1, Attributes.builder().put("type", "connector_start_failure").build());
                }
            }
        });
        
        // Register with service registry
        registerService();
    }

    @Override
    public void stop() throws Exception {
        // Get tracer for stop operation
        Tracer tracer = openTelemetry.getTracer("org.traccar.ServerManager");
        
        // Deregister from service registry first to stop receiving new connections
        deregisterService();
        
        // Stop all connectors with tracing and metrics
        tracer.spanBuilder("ServerManager.stop").startSpan().run(() -> {
            // Implement graceful shutdown
            LOGGER.info("Initiating graceful shutdown of server");
            
            // Give time for in-flight requests to complete
            try {
                // Wait for a configurable grace period
                int gracePeriod = config.getInteger(Keys.SERVER_GRACEFUL_SHUTDOWN_PERIOD, 10);
                LOGGER.info("Waiting {} seconds for in-flight requests to complete", gracePeriod);
                TimeUnit.SECONDS.sleep(gracePeriod);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Graceful shutdown interrupted");
            }
            
            // Stop all connectors
            for (TrackerConnector connector : connectorList) {
                try {
                    connector.stop();
                } catch (Exception e) {
                    LOGGER.warn("Failed to stop connector", e);
                    errorCounter.add(1, Attributes.builder().put("type", "connector_stop_failure").build());
                }
            }
            
            LOGGER.info("Server shutdown completed");
        });
    }

    /**
     * Wraps a supplier with a circuit breaker for resilient service calls.
     *
     * @param <T> The type of the result
     * @param name The name of the circuit breaker
     * @param supplier The supplier to execute
     * @return The result of the supplier
     */
    public <T> T executeWithCircuitBreaker(String name, Supplier<T> supplier) {
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(name);
        return circuitBreaker.executeSupplier(supplier);
    }
}