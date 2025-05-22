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
package org.traccar.web.api;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Servlet that provides health check endpoints for Kubernetes liveness and readiness probes.
 */
public class HealthCheckServlet extends HttpServlet {

    private static final Logger LOGGER = LoggerFactory.getLogger(HealthCheckServlet.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Config config;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final ServiceDiscoveryClient serviceDiscoveryClient;
    private final AtomicBoolean isReady = new AtomicBoolean(false);

    public HealthCheckServlet(Config config, CircuitBreakerRegistry circuitBreakerRegistry,
                             ServiceDiscoveryClient serviceDiscoveryClient) {
        this.config = config;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.serviceDiscoveryClient = serviceDiscoveryClient;
        
        // Mark as ready after initialization delay
        long readyDelay = config.getLong("web.health.readyDelay", 10000);
        if (readyDelay > 0) {
            new Thread(() -> {
                try {
                    Thread.sleep(readyDelay);
                    isReady.set(true);
                    LOGGER.info("Health check marked as ready");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        } else {
            isReady.set(true);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String path = req.getPathInfo();
        if (path == null) {
            path = "/";
        }

        resp.setContentType("application/json");

        switch (path) {
            case "/liveness":
                handleLivenessCheck(resp);
                break;
            case "/readiness":
                handleReadinessCheck(resp);
                break;
            case "/startup":
                handleStartupCheck(resp);
                break;
            default:
                handleFullHealthCheck(resp);
                break;
        }
    }

    /**
     * Handle liveness check - basic check that the application is running.
     * This should only fail if the application is completely unresponsive.
     */
    private void handleLivenessCheck(HttpServletResponse resp) throws IOException {
        Map<String, Object> result = new HashMap<>();
        result.put("status", "UP");
        result.put("timestamp", System.currentTimeMillis());

        resp.setStatus(HttpServletResponse.SC_OK);
        OBJECT_MAPPER.writeValue(resp.getOutputStream(), result);
    }

    /**
     * Handle readiness check - check if the application is ready to serve traffic.
     * This should fail if the application is still starting up or if required dependencies are unavailable.
     */
    private void handleReadinessCheck(HttpServletResponse resp) throws IOException {
        Map<String, Object> result = new HashMap<>();
        Map<String, Object> components = new HashMap<>();
        boolean allReady = isReady.get();

        // Check circuit breakers
        Map<String, Object> circuitBreakers = new HashMap<>();
        for (CircuitBreaker circuitBreaker : circuitBreakerRegistry.getAllCircuitBreakers()) {
            String name = circuitBreaker.getName();
            CircuitBreaker.State state = circuitBreaker.getState();
            circuitBreakers.put(name, Map.of(
                    "state", state.name(),
                    "metrics", Map.of(
                            "failureRate", circuitBreaker.getMetrics().getFailureRate(),
                            "slowCallRate", circuitBreaker.getMetrics().getSlowCallRate(),
                            "numberOfBufferedCalls", circuitBreaker.getMetrics().getNumberOfBufferedCalls(),
                            "numberOfFailedCalls", circuitBreaker.getMetrics().getNumberOfFailedCalls()
                    )
            ));
            
            // If any circuit breaker is OPEN, the service is not ready
            if (state == CircuitBreaker.State.OPEN) {
                allReady = false;
            }
        }
        components.put("circuitBreakers", circuitBreakers);

        // Check required services if service discovery is enabled
        if (config.getBoolean("web.serviceDiscovery.enabled", false)) {
            Map<String, Object> services = new HashMap<>();
            String[] requiredServices = config.getString("web.health.requiredServices", "").split(",");
            
            for (String serviceName : requiredServices) {
                if (!serviceName.trim().isEmpty()) {
                    boolean available = serviceDiscoveryClient.isServiceAvailable(serviceName.trim());
                    services.put(serviceName.trim(), Map.of("available", available));
                    
                    // If any required service is unavailable, the service is not ready
                    if (!available) {
                        allReady = false;
                    }
                }
            }
            components.put("services", services);
        }

        result.put("status", allReady ? "UP" : "DOWN");
        result.put("components", components);
        result.put("timestamp", System.currentTimeMillis());

        resp.setStatus(allReady ? HttpServletResponse.SC_OK : HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        OBJECT_MAPPER.writeValue(resp.getOutputStream(), result);
    }

    /**
     * Handle startup check - check if the application has completed startup.
     * This is similar to readiness but specifically for initial startup.
     */
    private void handleStartupCheck(HttpServletResponse resp) throws IOException {
        Map<String, Object> result = new HashMap<>();
        result.put("status", isReady.get() ? "UP" : "DOWN");
        result.put("timestamp", System.currentTimeMillis());

        resp.setStatus(isReady.get() ? HttpServletResponse.SC_OK : HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        OBJECT_MAPPER.writeValue(resp.getOutputStream(), result);
    }

    /**
     * Handle full health check - comprehensive health check with detailed information.
     */
    private void handleFullHealthCheck(HttpServletResponse resp) throws IOException {
        Map<String, Object> result = new HashMap<>();
        Map<String, Object> components = new HashMap<>();
        boolean allHealthy = true;

        // Check circuit breakers
        Map<String, Object> circuitBreakers = new HashMap<>();
        for (CircuitBreaker circuitBreaker : circuitBreakerRegistry.getAllCircuitBreakers()) {
            String name = circuitBreaker.getName();
            CircuitBreaker.State state = circuitBreaker.getState();
            circuitBreakers.put(name, Map.of(
                    "state", state.name(),
                    "metrics", Map.of(
                            "failureRate", circuitBreaker.getMetrics().getFailureRate(),
                            "slowCallRate", circuitBreaker.getMetrics().getSlowCallRate(),
                            "numberOfBufferedCalls", circuitBreaker.getMetrics().getNumberOfBufferedCalls(),
                            "numberOfFailedCalls", circuitBreaker.getMetrics().getNumberOfFailedCalls()
                    )
            ));
            
            if (state == CircuitBreaker.State.OPEN) {
                allHealthy = false;
            }
        }
        components.put("circuitBreakers", circuitBreakers);

        // Check services if service discovery is enabled
        if (config.getBoolean("web.serviceDiscovery.enabled", false)) {
            Map<String, Object> services = new HashMap<>();
            String[] monitoredServices = config.getString("web.health.monitoredServices", "").split(",");
            
            for (String serviceName : monitoredServices) {
                if (!serviceName.trim().isEmpty()) {
                    boolean available = serviceDiscoveryClient.isServiceAvailable(serviceName.trim());
                    services.put(serviceName.trim(), Map.of("available", available));
                    
                    // Only required services affect overall health
                    if (config.getString("web.health.requiredServices", "").contains(serviceName.trim()) && !available) {
                        allHealthy = false;
                    }
                }
            }
            components.put("services", services);
        }

        // Add system information
        Map<String, Object> system = new HashMap<>();
        system.put("jvm", Map.of(
                "version", System.getProperty("java.version"),
                "vendor", System.getProperty("java.vendor"),
                "memory", Map.of(
                        "free", Runtime.getRuntime().freeMemory(),
                        "total", Runtime.getRuntime().totalMemory(),
                        "max", Runtime.getRuntime().maxMemory()
                )
        ));
        system.put("os", Map.of(
                "name", System.getProperty("os.name"),
                "version", System.getProperty("os.version"),
                "arch", System.getProperty("os.arch")
        ));
        components.put("system", system);

        result.put("status", allHealthy ? "UP" : "DOWN");
        result.put("components", components);
        result.put("timestamp", System.currentTimeMillis());

        resp.setStatus(HttpServletResponse.SC_OK); // Always return 200 for full health check
        OBJECT_MAPPER.writeValue(resp.getOutputStream(), result);
    }
}