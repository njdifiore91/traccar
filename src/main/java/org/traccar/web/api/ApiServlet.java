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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * API Gateway servlet that routes requests to appropriate microservices.
 * Implements circuit breaker pattern for resilient service communication.
 */
public class ApiServlet extends HttpServlet {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiServlet.class);

    private final Config config;
    private final ServiceDiscoveryClient serviceDiscoveryClient;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final MeterRegistry meterRegistry;
    private final HttpClient httpClient;
    private final Map<String, String> serviceRoutes;

    public ApiServlet(Config config, ServiceDiscoveryClient serviceDiscoveryClient,
                      CircuitBreakerRegistry circuitBreakerRegistry, MeterRegistry meterRegistry) {
        this.config = config;
        this.serviceDiscoveryClient = serviceDiscoveryClient;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.meterRegistry = meterRegistry;
        
        // Initialize HTTP client
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(config.getInteger("web.api.timeout", 10)))
                .build();
        
        // Initialize service routes
        this.serviceRoutes = new HashMap<>();
        loadServiceRoutes();
    }

    /**
     * Load service routes from configuration.
     * Format: web.api.routes.{path-prefix}={service-name}
     * Example: web.api.routes.devices=device-service
     */
    private void loadServiceRoutes() {
        // Default routes
        serviceRoutes.put("devices", "device-service");
        serviceRoutes.put("positions", "position-service");
        serviceRoutes.put("events", "event-service");
        serviceRoutes.put("notifications", "notification-service");
        serviceRoutes.put("reports", "reporting-service");
        
        // Load custom routes from configuration
        for (Object key : Collections.list(config.getKeys())) {
            String keyStr = key.toString();
            if (keyStr.startsWith("web.api.routes.")) {
                String pathPrefix = keyStr.substring("web.api.routes.".length());
                String serviceName = config.getString(keyStr);
                serviceRoutes.put(pathPrefix, serviceName);
                LOGGER.info("Added API route: {} -> {}", pathPrefix, serviceName);
            }
        }
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String path = req.getPathInfo();
        if (path == null) {
            path = "/";
        }
        
        // Extract the service prefix from the path
        String[] pathParts = path.split("/");
        String servicePrefix = pathParts.length > 1 ? pathParts[1] : "";
        
        // Find the target service
        String serviceName = serviceRoutes.get(servicePrefix);
        if (serviceName == null) {
            LOGGER.warn("No service found for path: {}", path);
            resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Service not found");
            return;
        }
        
        // Get service instance from service discovery
        ServiceDiscoveryClient.ServiceInstance serviceInstance = serviceDiscoveryClient.getService(serviceName);
        if (serviceInstance == null) {
            LOGGER.warn("Service instance not found for service: {}", serviceName);
            resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Service unavailable");
            return;
        }
        
        // Create metrics
        Timer requestTimer = meterRegistry.timer("api.request.duration", "service", serviceName, "method", req.getMethod());
        Counter requestCounter = meterRegistry.counter("api.request.count", "service", serviceName, "method", req.getMethod());
        
        // Get or create circuit breaker for the service
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(serviceName, "default");
        
        // Execute the request with circuit breaker
        try {
            requestCounter.increment();
            
            // Create a supplier that executes the HTTP request
            Supplier<HttpResponse<InputStream>> supplier = () -> {
                try {
                    return executeRequest(req, serviceInstance);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to execute request", e);
                }
            };
            
            // Execute the request with circuit breaker and timer
            HttpResponse<InputStream> response = requestTimer.record(() -> {
                try {
                    return circuitBreaker.executeSupplier(supplier);
                } catch (Exception e) {
                    LOGGER.error("Circuit breaker execution failed", e);
                    return null;
                }
            });
            
            if (response != null) {
                // Forward the response to the client
                forwardResponse(response, resp);
            } else {
                // Handle circuit breaker open or execution failure
                handleCircuitBreakerFailure(resp, serviceName, circuitBreaker.getState().name());
            }
        } catch (Exception e) {
            LOGGER.error("Error processing request", e);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Internal server error");
        }
    }

    /**
     * Execute the HTTP request to the target service.
     *
     * @param req the original HTTP request
     * @param serviceInstance the target service instance
     * @return the HTTP response from the target service
     * @throws Exception if the request fails
     */
    private HttpResponse<InputStream> executeRequest(HttpServletRequest req, 
                                                    ServiceDiscoveryClient.ServiceInstance serviceInstance) throws Exception {
        // Build the target URL
        String targetUrl = serviceInstance.getUrl() + req.getPathInfo();
        if (req.getQueryString() != null) {
            targetUrl += "?" + req.getQueryString();
        }
        
        // Create the HTTP request builder
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl))
                .timeout(Duration.ofSeconds(config.getInteger("web.api.timeout", 10)));
        
        // Set the HTTP method
        String method = req.getMethod();
        switch (method) {
            case "GET":
                requestBuilder.GET();
                break;
            case "POST":
                requestBuilder.POST(HttpRequest.BodyPublishers.ofInputStream(() -> {
                    try {
                        return req.getInputStream();
                    } catch (IOException e) {
                        return InputStream.nullInputStream();
                    }
                }));
                break;
            case "PUT":
                requestBuilder.PUT(HttpRequest.BodyPublishers.ofInputStream(() -> {
                    try {
                        return req.getInputStream();
                    } catch (IOException e) {
                        return InputStream.nullInputStream();
                    }
                }));
                break;
            case "DELETE":
                requestBuilder.DELETE();
                break;
            default:
                requestBuilder.method(method, HttpRequest.BodyPublishers.noBody());
        }
        
        // Copy headers from the original request
        Enumeration<String> headerNames = req.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            // Skip some headers that should not be forwarded
            if (!headerName.equalsIgnoreCase("host") && !headerName.equalsIgnoreCase("connection")) {
                Enumeration<String> headerValues = req.getHeaders(headerName);
                while (headerValues.hasMoreElements()) {
                    requestBuilder.header(headerName, headerValues.nextElement());
                }
            }
        }
        
        // Add API Gateway headers
        requestBuilder.header("X-Forwarded-For", req.getRemoteAddr());
        requestBuilder.header("X-Forwarded-Proto", req.getScheme());
        requestBuilder.header("X-Forwarded-Host", req.getServerName());
        requestBuilder.header("X-Forwarded-Port", String.valueOf(req.getServerPort()));
        
        // Execute the request
        HttpRequest request = requestBuilder.build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
    }

    /**
     * Forward the response from the target service to the client.
     *
     * @param response the HTTP response from the target service
     * @param resp the HTTP servlet response to the client
     * @throws IOException if an I/O error occurs
     */
    private void forwardResponse(HttpResponse<InputStream> response, HttpServletResponse resp) throws IOException {
        // Set the status code
        resp.setStatus(response.statusCode());
        
        // Copy headers from the response
        response.headers().map().forEach((name, values) -> {
            // Skip some headers that should not be forwarded
            if (!name.equalsIgnoreCase("connection") && !name.equalsIgnoreCase("transfer-encoding")) {
                values.forEach(value -> resp.addHeader(name, value));
            }
        });
        
        // Copy the response body
        try (InputStream in = response.body();
             OutputStream out = resp.getOutputStream()) {
            byte[] buffer = new byte[8192];
            int length;
            while ((length = in.read(buffer)) > 0) {
                out.write(buffer, 0, length);
            }
        }
    }

    /**
     * Handle circuit breaker failure by returning an appropriate error response.
     *
     * @param resp the HTTP servlet response to the client
     * @param serviceName the name of the service that failed
     * @param state the state of the circuit breaker
     * @throws IOException if an I/O error occurs
     */
    private void handleCircuitBreakerFailure(HttpServletResponse resp, String serviceName, String state) throws IOException {
        LOGGER.warn("Circuit breaker for service {} is in state {}", serviceName, state);
        
        resp.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        resp.setContentType("application/json");
        resp.getWriter().write(String.format(
                "{\"error\":\"Service unavailable\",\"service\":\"%s\",\"state\":\"%s\"}",
                serviceName, state));
    }
}