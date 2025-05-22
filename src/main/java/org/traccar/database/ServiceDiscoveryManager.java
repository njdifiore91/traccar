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
package org.traccar.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * ServiceDiscoveryManager handles service discovery for database operations, enabling dynamic resolution
 * of database endpoints in the microservices architecture. It integrates with Kubernetes or Consul for
 * service registration and discovery, provides health check endpoints for orchestration, and manages
 * service-to-service communication for database operations.
 */
@Singleton
public class ServiceDiscoveryManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryManager.class);

    private static final String CIRCUIT_BREAKER_NAME = "serviceDiscoveryCircuitBreaker";
    private static final String RETRY_NAME = "serviceDiscoveryRetry";
    private static final String CACHE_PREFIX = "service:";
    private static final int CACHE_TTL_SECONDS = 60; // 1 minute cache TTL
    private static final int HEALTH_CHECK_INTERVAL_SECONDS = 30;

    private final Config config;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Timer discoveryTimer;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Counter cacheHitCounter;
    private final Counter cacheMissCounter;
    private final ScheduledExecutorService healthCheckExecutor;

    private final Map<String, ServiceEndpoint> serviceCache = new ConcurrentHashMap<>();
    private final Map<String, List<ServiceEndpoint>> serviceRegistry = new ConcurrentHashMap<>();
    private final String discoveryType;
    private final String consulHost;
    private final int consulPort;
    private final String kubernetesNamespace;
    private final boolean kubernetesEnabled;
    private final boolean consulEnabled;

    /**
     * Represents a service endpoint with health status and load information.
     */
    public static class ServiceEndpoint {
        private final String serviceName;
        private final String host;
        private final int port;
        private boolean healthy;
        private long lastChecked;
        private int currentLoad;
        private final Map<String, String> metadata;

        public ServiceEndpoint(String serviceName, String host, int port) {
            this.serviceName = serviceName;
            this.host = host;
            this.port = port;
            this.healthy = true; // Assume healthy until proven otherwise
            this.lastChecked = System.currentTimeMillis();
            this.currentLoad = 0;
            this.metadata = new HashMap<>();
        }

        public String getServiceName() {
            return serviceName;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public boolean isHealthy() {
            return healthy;
        }

        public void setHealthy(boolean healthy) {
            this.healthy = healthy;
        }

        public long getLastChecked() {
            return lastChecked;
        }

        public void setLastChecked(long lastChecked) {
            this.lastChecked = lastChecked;
        }

        public int getCurrentLoad() {
            return currentLoad;
        }

        public void setCurrentLoad(int currentLoad) {
            this.currentLoad = currentLoad;
        }

        public Map<String, String> getMetadata() {
            return metadata;
        }

        public void addMetadata(String key, String value) {
            metadata.put(key, value);
        }

        public String getConnectionString() {
            return host + ":" + port;
        }

        @Override
        public String toString() {
            return "ServiceEndpoint{" +
                    "serviceName='" + serviceName + '\'' +
                    ", host='" + host + '\'' +
                    ", port=" + port +
                    ", healthy=" + healthy +
                    ", currentLoad=" + currentLoad +
                    '}';
        }
    }

    /**
     * Constructs a new ServiceDiscoveryManager with the specified dependencies.
     *
     * @param config Configuration for service discovery settings
     * @param httpClient HTTP client for making service discovery requests
     * @param objectMapper JSON object mapper for parsing discovery responses
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry for monitoring
     */
    @Inject
    public ServiceDiscoveryManager(Config config, HttpClient httpClient, ObjectMapper objectMapper,
                                  Tracer tracer, MeterRegistry meterRegistry) {
        this.config = config;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;

        // Initialize discovery configuration
        this.discoveryType = config.getString("service.discovery.type", "kubernetes");
        this.consulHost = config.getString("service.discovery.consul.host", "localhost");
        this.consulPort = config.getInteger("service.discovery.consul.port", 8500);
        this.kubernetesNamespace = config.getString("service.discovery.kubernetes.namespace", "default");
        this.kubernetesEnabled = "kubernetes".equalsIgnoreCase(discoveryType);
        this.consulEnabled = "consul".equalsIgnoreCase(discoveryType);

        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to open circuit
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10) // Consider last 10 calls for failure rate
                .minimumNumberOfCalls(5) // Minimum calls before calculating failure rate
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 calls in half-open state
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);

        // Initialize retry policy
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Maximum 3 attempts
                .waitDuration(Duration.ofMillis(500)) // Initial wait 500ms
                .retryExceptions(IOException.class, RuntimeException.class) // Retry on these exceptions
                .exponentialBackoff(Duration.ofMillis(500), Duration.ofSeconds(5), 2.0) // Exponential backoff
                .build();

        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry(RETRY_NAME);

        // Initialize metrics
        this.discoveryTimer = Timer.builder("service.discovery.duration")
                .description("Time taken for service discovery operations")
                .register(meterRegistry);

        this.successCounter = Counter.builder("service.discovery.success")
                .description("Number of successful service discovery operations")
                .register(meterRegistry);

        this.failureCounter = Counter.builder("service.discovery.failure")
                .description("Number of failed service discovery operations")
                .register(meterRegistry);

        this.cacheHitCounter = Counter.builder("service.discovery.cache.hit")
                .description("Number of cache hits during service discovery")
                .register(meterRegistry);

        this.cacheMissCounter = Counter.builder("service.discovery.cache.miss")
                .description("Number of cache misses during service discovery")
                .register(meterRegistry);

        // Initialize health check executor
        this.healthCheckExecutor = Executors.newScheduledThreadPool(1);
        startHealthChecks();

        // Register this service if auto-registration is enabled
        if (config.getBoolean("service.discovery.autoRegister", true)) {
            registerSelf();
        }

        // Initial discovery of services
        refreshServiceRegistry();
    }

    /**
     * Registers the current service with the service discovery system.
     */
    private void registerSelf() {
        String serviceName = config.getString("service.name", "traccar");
        String serviceHost = config.getString("service.host", "localhost");
        int servicePort = config.getInteger("service.port", 8082);
        String healthCheckPath = config.getString("service.healthCheckPath", "/api/health");

        try {
            if (consulEnabled) {
                registerWithConsul(serviceName, serviceHost, servicePort, healthCheckPath);
            } else if (kubernetesEnabled) {
                LOGGER.info("Service auto-registration not required with Kubernetes - using service discovery");
            } else {
                LOGGER.warn("Unknown service discovery type: {}", discoveryType);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to register service with discovery system", e);
        }
    }

    /**
     * Registers the service with Consul.
     */
    private void registerWithConsul(String serviceName, String serviceHost, int servicePort, String healthCheckPath) {
        try {
            String registrationUrl = String.format("http://%s:%d/v1/agent/service/register", consulHost, consulPort);
            
            Map<String, Object> registration = new HashMap<>();
            registration.put("ID", serviceName + "-" + serviceHost + "-" + servicePort);
            registration.put("Name", serviceName);
            registration.put("Address", serviceHost);
            registration.put("Port", servicePort);
            
            Map<String, Object> check = new HashMap<>();
            check.put("HTTP", "http://" + serviceHost + ":" + servicePort + healthCheckPath);
            check.put("Interval", "15s");
            registration.put("Check", check);
            
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(registrationUrl))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(registration)))
                    .build();
            
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                LOGGER.info("Successfully registered service {} with Consul", serviceName);
            } else {
                LOGGER.error("Failed to register service with Consul: {} - {}", response.statusCode(), response.body());
            }
        } catch (Exception e) {
            LOGGER.error("Error registering service with Consul", e);
        }
    }

    /**
     * Refreshes the service registry by querying the service discovery system.
     */
    public void refreshServiceRegistry() {
        Span span = tracer.spanBuilder("service.discovery.refresh")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();

        try {
            if (consulEnabled) {
                refreshFromConsul(span);
            } else if (kubernetesEnabled) {
                refreshFromKubernetes(span);
            } else {
                span.setStatus(StatusCode.ERROR, "Unknown service discovery type: " + discoveryType);
                LOGGER.warn("Unknown service discovery type: {}", discoveryType);
            }
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error refreshing service registry", e);
            failureCounter.increment();
        } finally {
            span.end();
        }
    }

    /**
     * Refreshes the service registry from Consul.
     */
    private void refreshFromConsul(Span parentSpan) {
        Span span = tracer.spanBuilder("service.discovery.refresh.consul")
                .setParent(parentSpan.getSpanContext())
                .startSpan();

        try {
            String servicesUrl = String.format("http://%s:%d/v1/catalog/services", consulHost, consulPort);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(servicesUrl))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                Map<String, Object> services = objectMapper.readValue(response.body(), Map.class);

                for (String serviceName : services.keySet()) {
                    fetchServiceNodes(serviceName, span);
                }

                successCounter.increment();
            } else {
                span.setStatus(StatusCode.ERROR, "Failed to get services from Consul: " + response.statusCode());
                LOGGER.error("Failed to get services from Consul: {} - {}", response.statusCode(), response.body());
                failureCounter.increment();
            }
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error refreshing from Consul", e);
            failureCounter.increment();
        } finally {
            span.end();
        }
    }

    /**
     * Fetches service nodes from Consul for a specific service.
     */
    private void fetchServiceNodes(String serviceName, Span parentSpan) {
        Span span = tracer.spanBuilder("service.discovery.fetch.nodes")
                .setParent(parentSpan.getSpanContext())
                .setAttribute("service.name", serviceName)
                .startSpan();

        try {
            String nodesUrl = String.format("http://%s:%d/v1/catalog/service/%s", consulHost, consulPort, serviceName);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(nodesUrl))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                List<Map<String, Object>> nodes = objectMapper.readValue(response.body(), List.class);
                List<ServiceEndpoint> endpoints = new ArrayList<>();

                for (Map<String, Object> node : nodes) {
                    String host = (String) node.get("ServiceAddress");
                    if (host == null || host.isEmpty()) {
                        host = (String) node.get("Address");
                    }
                    Integer port = (Integer) node.get("ServicePort");
                    String id = (String) node.get("ServiceID");

                    if (host != null && port != null) {
                        ServiceEndpoint endpoint = new ServiceEndpoint(serviceName, host, port);
                        if (id != null) {
                            endpoint.addMetadata("id", id);
                        }
                        endpoints.add(endpoint);
                    }
                }

                if (!endpoints.isEmpty()) {
                    serviceRegistry.put(serviceName, endpoints);
                    LOGGER.debug("Updated service registry for {} with {} endpoints", serviceName, endpoints.size());
                }
            } else {
                span.setStatus(StatusCode.ERROR, "Failed to get service nodes from Consul: " + response.statusCode());
                LOGGER.error("Failed to get service nodes from Consul: {} - {}", response.statusCode(), response.body());
            }
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error fetching service nodes from Consul", e);
        } finally {
            span.end();
        }
    }

    /**
     * Refreshes the service registry from Kubernetes.
     */
    private void refreshFromKubernetes(Span parentSpan) {
        Span span = tracer.spanBuilder("service.discovery.refresh.kubernetes")
                .setParent(parentSpan.getSpanContext())
                .startSpan();

        try {
            // In a real implementation, this would use the Kubernetes API
            // For simplicity, we'll use environment variables and DNS for Kubernetes service discovery
            // This is a common pattern in Kubernetes - services are accessible via DNS
            // e.g., service-name.namespace.svc.cluster.local

            // For demonstration, we'll add some example database services
            // In a real implementation, this would be discovered from Kubernetes API
            List<ServiceEndpoint> dbServices = new ArrayList<>();
            
            // Primary database
            ServiceEndpoint primary = new ServiceEndpoint("database", "database-primary." + kubernetesNamespace + ".svc.cluster.local", 5432);
            primary.addMetadata("role", "primary");
            dbServices.add(primary);
            
            // Read replicas
            ServiceEndpoint replica1 = new ServiceEndpoint("database", "database-replica-0." + kubernetesNamespace + ".svc.cluster.local", 5432);
            replica1.addMetadata("role", "replica");
            dbServices.add(replica1);
            
            ServiceEndpoint replica2 = new ServiceEndpoint("database", "database-replica-1." + kubernetesNamespace + ".svc.cluster.local", 5432);
            replica2.addMetadata("role", "replica");
            dbServices.add(replica2);
            
            serviceRegistry.put("database", dbServices);
            LOGGER.debug("Updated service registry for database with {} endpoints", dbServices.size());
            
            successCounter.increment();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error refreshing from Kubernetes", e);
            failureCounter.increment();
        } finally {
            span.end();
        }
    }

    /**
     * Starts periodic health checks for all registered services.
     */
    private void startHealthChecks() {
        healthCheckExecutor.scheduleAtFixedRate(this::checkAllServicesHealth, 
                0, HEALTH_CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Checks the health of all registered services.
     */
    private void checkAllServicesHealth() {
        for (Map.Entry<String, List<ServiceEndpoint>> entry : serviceRegistry.entrySet()) {
            String serviceName = entry.getKey();
            List<ServiceEndpoint> endpoints = entry.getValue();

            for (ServiceEndpoint endpoint : endpoints) {
                checkServiceHealth(endpoint);
            }
        }
    }

    /**
     * Checks the health of a specific service endpoint.
     */
    private void checkServiceHealth(ServiceEndpoint endpoint) {
        Span span = tracer.spanBuilder("service.discovery.health.check")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("service.name", endpoint.getServiceName())
                .setAttribute("service.host", endpoint.getHost())
                .setAttribute("service.port", endpoint.getPort())
                .startSpan();

        try {
            String healthCheckPath = config.getString("service." + endpoint.getServiceName() + ".healthCheckPath", "/health");
            String healthCheckUrl = "http://" + endpoint.getHost() + ":" + endpoint.getPort() + healthCheckPath;

            URL url = new URL(healthCheckUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(2000); // 2 seconds timeout
            connection.setReadTimeout(2000);
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();
            boolean healthy = responseCode >= 200 && responseCode < 300;

            endpoint.setHealthy(healthy);
            endpoint.setLastChecked(System.currentTimeMillis());

            if (healthy) {
                LOGGER.debug("Health check passed for {} at {}", endpoint.getServiceName(), endpoint.getConnectionString());
            } else {
                LOGGER.warn("Health check failed for {} at {}: {}", 
                        endpoint.getServiceName(), endpoint.getConnectionString(), responseCode);
            }

            span.setAttribute("health.status", healthy ? "UP" : "DOWN");
            span.setAttribute("health.responseCode", responseCode);
        } catch (Exception e) {
            endpoint.setHealthy(false);
            endpoint.setLastChecked(System.currentTimeMillis());

            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.setAttribute("health.status", "DOWN");

            LOGGER.warn("Health check error for {} at {}: {}", 
                    endpoint.getServiceName(), endpoint.getConnectionString(), e.getMessage());
        } finally {
            span.end();
        }
    }

    /**
     * Gets a service endpoint for the specified service name.
     * Uses load balancing to select the best endpoint based on health and load.
     *
     * @param serviceName The name of the service to discover
     * @return A service endpoint or null if none available
     */
    public ServiceEndpoint getServiceEndpoint(String serviceName) {
        return discoveryTimer.record(() -> {
            Span span = tracer.spanBuilder("service.discovery.get.endpoint")
                    .setSpanKind(SpanKind.INTERNAL)
                    .setAttribute("service.name", serviceName)
                    .startSpan();

            try {
                // Check cache first
                ServiceEndpoint cachedEndpoint = serviceCache.get(CACHE_PREFIX + serviceName);
                if (cachedEndpoint != null && cachedEndpoint.isHealthy() && 
                        (System.currentTimeMillis() - cachedEndpoint.getLastChecked() < CACHE_TTL_SECONDS * 1000)) {
                    cacheHitCounter.increment();
                    span.setAttribute("cache.hit", true);
                    return cachedEndpoint;
                }

                cacheMissCounter.increment();
                span.setAttribute("cache.hit", false);

                // Use circuit breaker and retry for service discovery
                Supplier<ServiceEndpoint> decoratedSupplier = CircuitBreaker.decorateSupplier(
                        circuitBreaker,
                        () -> findBestServiceEndpoint(serviceName, span));

                decoratedSupplier = Retry.decorateSupplier(retry, decoratedSupplier);

                ServiceEndpoint endpoint = decoratedSupplier.get();

                if (endpoint != null) {
                    // Cache the endpoint for future lookups
                    serviceCache.put(CACHE_PREFIX + serviceName, endpoint);
                    successCounter.increment();
                } else {
                    failureCounter.increment();
                }

                return endpoint;
            } catch (Exception e) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                failureCounter.increment();
                LOGGER.error("Error getting service endpoint for {}", serviceName, e);
                return null;
            } finally {
                span.end();
            }
        });
    }

    /**
     * Finds the best service endpoint based on health and load.
     */
    private ServiceEndpoint findBestServiceEndpoint(String serviceName, Span parentSpan) {
        Span span = tracer.spanBuilder("service.discovery.find.best.endpoint")
                .setParent(parentSpan.getSpanContext())
                .setAttribute("service.name", serviceName)
                .startSpan();

        try {
            List<ServiceEndpoint> endpoints = serviceRegistry.get(serviceName);

            if (endpoints == null || endpoints.isEmpty()) {
                // If no endpoints found, try to refresh the registry
                refreshServiceRegistry();
                endpoints = serviceRegistry.get(serviceName);

                if (endpoints == null || endpoints.isEmpty()) {
                    span.setStatus(StatusCode.ERROR, "No endpoints found for service: " + serviceName);
                    LOGGER.warn("No endpoints found for service: {}", serviceName);
                    return null;
                }
            }

            // Filter for healthy endpoints
            List<ServiceEndpoint> healthyEndpoints = new ArrayList<>();
            for (ServiceEndpoint endpoint : endpoints) {
                if (endpoint.isHealthy()) {
                    healthyEndpoints.add(endpoint);
                }
            }

            if (healthyEndpoints.isEmpty()) {
                span.setStatus(StatusCode.ERROR, "No healthy endpoints found for service: " + serviceName);
                LOGGER.warn("No healthy endpoints found for service: {}", serviceName);
                return null;
            }

            // Find the endpoint with the lowest load
            ServiceEndpoint bestEndpoint = healthyEndpoints.get(0);
            for (int i = 1; i < healthyEndpoints.size(); i++) {
                ServiceEndpoint current = healthyEndpoints.get(i);
                if (current.getCurrentLoad() < bestEndpoint.getCurrentLoad()) {
                    bestEndpoint = current;
                }
            }

            // Increment the load counter for the selected endpoint
            bestEndpoint.setCurrentLoad(bestEndpoint.getCurrentLoad() + 1);

            span.setAttribute("selected.endpoint", bestEndpoint.getConnectionString());
            LOGGER.debug("Selected endpoint {} for service {}", bestEndpoint.getConnectionString(), serviceName);

            return bestEndpoint;
        } finally {
            span.end();
        }
    }

    /**
     * Gets a database connection string for the specified service.
     *
     * @param serviceName The name of the database service
     * @param readOnly Whether a read-only connection is acceptable
     * @return A connection string or null if none available
     */
    public String getDatabaseConnectionString(String serviceName, boolean readOnly) {
        ServiceEndpoint endpoint = getServiceEndpoint(serviceName);
        if (endpoint == null) {
            return null;
        }

        // For read-only connections, we might prefer replicas over primary
        if (readOnly && "database".equals(serviceName)) {
            // Try to find a replica first
            List<ServiceEndpoint> endpoints = serviceRegistry.get(serviceName);
            if (endpoints != null) {
                for (ServiceEndpoint ep : endpoints) {
                    if (ep.isHealthy() && "replica".equals(ep.getMetadata().get("role"))) {
                        endpoint = ep;
                        break;
                    }
                }
            }
        }

        // Format the connection string based on the service type
        // This is a simplified example - in a real implementation, you would have more sophisticated connection string building
        String connectionString = "jdbc:postgresql://" + endpoint.getHost() + ":" + endpoint.getPort() + "/traccar";
        
        // Record the connection attempt
        endpoint.setCurrentLoad(endpoint.getCurrentLoad() + 1);
        
        return connectionString;
    }

    /**
     * Releases a connection for the specified service, reducing its load counter.
     *
     * @param serviceName The name of the service
     * @param connectionString The connection string that was used
     */
    public void releaseConnection(String serviceName, String connectionString) {
        List<ServiceEndpoint> endpoints = serviceRegistry.get(serviceName);
        if (endpoints != null) {
            for (ServiceEndpoint endpoint : endpoints) {
                if (connectionString.contains(endpoint.getHost() + ":" + endpoint.getPort())) {
                    if (endpoint.getCurrentLoad() > 0) {
                        endpoint.setCurrentLoad(endpoint.getCurrentLoad() - 1);
                    }
                    break;
                }
            }
        }
    }

    /**
     * Invalidates the cache for a specific service.
     *
     * @param serviceName The name of the service to invalidate
     */
    public void invalidateCache(String serviceName) {
        serviceCache.remove(CACHE_PREFIX + serviceName);
    }

    /**
     * Invalidates the entire service cache.
     */
    public void invalidateAllCaches() {
        serviceCache.clear();
    }

    /**
     * Shuts down the service discovery manager.
     */
    public void shutdown() {
        healthCheckExecutor.shutdown();
        try {
            if (!healthCheckExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                healthCheckExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            healthCheckExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}