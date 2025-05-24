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
package org.traccar.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.api.gateway.ApiGateway;
import org.traccar.api.gateway.CircuitBreaker;
import org.traccar.api.gateway.RouteRegistry;
import org.traccar.api.gateway.ServiceClient;
import org.traccar.api.gateway.ServiceDiscovery;
import org.traccar.api.gateway.ServiceRoute;
import org.traccar.api.gateway.ServiceUnavailableException;

/**
 * Tests the API Gateway's request routing functionality to ensure that client requests 
 * are properly routed to the appropriate backend microservices.
 */
@ExtendWith(MockitoExtension.class)
public class ResourceRoutingTest {

    @Mock
    private ServiceDiscovery serviceDiscovery;
    
    @Mock
    private RouteRegistry routeRegistry;
    
    @Mock
    private ServiceClient serviceClient;
    
    @Mock
    private CircuitBreaker circuitBreaker;
    
    private ApiGateway apiGateway;
    
    @BeforeEach
    public void setUp() {
        apiGateway = new ApiGateway(serviceDiscovery, routeRegistry, serviceClient, circuitBreaker);
    }
    
    @Test
    public void testPositionResourceRouting() throws IOException {
        // Setup route registry to return position service for position resources
        ServiceRoute positionRoute = new ServiceRoute("position-service", "/api/positions", "GET", "v1");
        when(routeRegistry.findRoute(eq("/api/positions"), eq("GET"), anyString()))
            .thenReturn(positionRoute);
        
        // Setup service discovery to return position service endpoint
        when(serviceDiscovery.getServiceUrl("position-service"))
            .thenReturn(URI.create("http://position-service:8080"));
        
        // Setup circuit breaker to allow the request
        when(circuitBreaker.isOpen("position-service")).thenReturn(false);
        
        // Setup mock response from service client
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        when(serviceClient.forward(any(URI.class), eq("GET"), any(), any()))
            .thenReturn(new ServiceClient.Response(200, "{\"positions\":[]}\n", headers));
        
        // Execute request
        ServiceClient.Response response = apiGateway.routeRequest(
                "/api/positions", "GET", Collections.emptyMap(), "", "v1");
        
        // Verify response
        assertNotNull(response);
        assertEquals(200, response.getStatusCode());
        assertEquals("{\"positions\":[]}\n", response.getBody());
        assertEquals("application/json", response.getHeaders().get("Content-Type"));
        
        // Verify service client was called with correct URI
        verify(serviceClient).forward(
                eq(URI.create("http://position-service:8080/api/positions")), 
                eq("GET"), 
                any(), 
                any());
    }
    
    @Test
    public void testDeviceResourceRouting() throws IOException {
        // Setup route registry to return device service for device resources
        ServiceRoute deviceRoute = new ServiceRoute("position-service", "/api/devices", "GET", "v1");
        when(routeRegistry.findRoute(eq("/api/devices"), eq("GET"), anyString()))
            .thenReturn(deviceRoute);
        
        // Setup service discovery to return device service endpoint
        when(serviceDiscovery.getServiceUrl("position-service"))
            .thenReturn(URI.create("http://position-service:8080"));
        
        // Setup circuit breaker to allow the request
        when(circuitBreaker.isOpen("position-service")).thenReturn(false);
        
        // Setup mock response from service client
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        when(serviceClient.forward(any(URI.class), eq("GET"), any(), any()))
            .thenReturn(new ServiceClient.Response(200, "{\"devices\":[]}\n", headers));
        
        // Execute request
        ServiceClient.Response response = apiGateway.routeRequest(
                "/api/devices", "GET", Collections.emptyMap(), "", "v1");
        
        // Verify response
        assertNotNull(response);
        assertEquals(200, response.getStatusCode());
        assertEquals("{\"devices\":[]}\n", response.getBody());
        assertEquals("application/json", response.getHeaders().get("Content-Type"));
        
        // Verify service client was called with correct URI
        verify(serviceClient).forward(
                eq(URI.create("http://position-service:8080/api/devices")), 
                eq("GET"), 
                any(), 
                any());
    }
    
    @Test
    public void testCommandResourceRouting() throws IOException {
        // Setup route registry to return command service for command resources
        ServiceRoute commandRoute = new ServiceRoute("protocol-service", "/api/commands", "POST", "v1");
        when(routeRegistry.findRoute(eq("/api/commands"), eq("POST"), anyString()))
            .thenReturn(commandRoute);
        
        // Setup service discovery to return command service endpoint
        when(serviceDiscovery.getServiceUrl("protocol-service"))
            .thenReturn(URI.create("http://protocol-service:8080"));
        
        // Setup circuit breaker to allow the request
        when(circuitBreaker.isOpen("protocol-service")).thenReturn(false);
        
        // Setup mock response from service client
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        when(serviceClient.forward(any(URI.class), eq("POST"), any(), any()))
            .thenReturn(new ServiceClient.Response(200, "{\"success\":true}\n", headers));
        
        // Request headers
        Map<String, String> requestHeaders = new HashMap<>();
        requestHeaders.put("Content-Type", "application/json");
        requestHeaders.put("Authorization", "Bearer token123");
        
        // Request body
        String requestBody = "{\"deviceId\":123,\"type\":\"engineStop\"}";
        
        // Execute request
        ServiceClient.Response response = apiGateway.routeRequest(
                "/api/commands", "POST", requestHeaders, requestBody, "v1");
        
        // Verify response
        assertNotNull(response);
        assertEquals(200, response.getStatusCode());
        assertEquals("{\"success\":true}\n", response.getBody());
        assertEquals("application/json", response.getHeaders().get("Content-Type"));
        
        // Verify service client was called with correct URI, method, headers, and body
        verify(serviceClient).forward(
                eq(URI.create("http://protocol-service:8080/api/commands")), 
                eq("POST"), 
                eq(requestHeaders), 
                eq(requestBody));
    }
    
    @Test
    public void testHttpMethodHandling() throws IOException {
        // Test different HTTP methods (GET, POST, PUT, DELETE)
        String[] methods = {"GET", "POST", "PUT", "DELETE"};
        String path = "/api/devices/123";
        
        for (String method : methods) {
            // Setup route registry for each method
            ServiceRoute route = new ServiceRoute("position-service", path, method, "v1");
            when(routeRegistry.findRoute(eq(path), eq(method), anyString()))
                .thenReturn(route);
            
            // Setup service discovery
            when(serviceDiscovery.getServiceUrl("position-service"))
                .thenReturn(URI.create("http://position-service:8080"));
            
            // Setup circuit breaker
            when(circuitBreaker.isOpen("position-service")).thenReturn(false);
            
            // Setup mock response
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", "application/json");
            when(serviceClient.forward(any(URI.class), eq(method), any(), any()))
                .thenReturn(new ServiceClient.Response(200, "{}", headers));
            
            // Execute request
            ServiceClient.Response response = apiGateway.routeRequest(
                    path, method, Collections.emptyMap(), "", "v1");
            
            // Verify response
            assertNotNull(response);
            assertEquals(200, response.getStatusCode());
            
            // Verify service client was called with correct method
            verify(serviceClient).forward(
                    eq(URI.create("http://position-service:8080" + path)), 
                    eq(method), 
                    any(), 
                    any());
        }
    }
    
    @Test
    public void testHeaderPropagation() throws IOException {
        // Setup route registry
        ServiceRoute route = new ServiceRoute("position-service", "/api/positions", "GET", "v1");
        when(routeRegistry.findRoute(eq("/api/positions"), eq("GET"), anyString()))
            .thenReturn(route);
        
        // Setup service discovery
        when(serviceDiscovery.getServiceUrl("position-service"))
            .thenReturn(URI.create("http://position-service:8080"));
        
        // Setup circuit breaker
        when(circuitBreaker.isOpen("position-service")).thenReturn(false);
        
        // Setup request headers
        Map<String, String> requestHeaders = new HashMap<>();
        requestHeaders.put("Authorization", "Bearer token123");
        requestHeaders.put("Accept-Language", "en-US");
        requestHeaders.put("User-Agent", "Test Client");
        requestHeaders.put("X-Custom-Header", "custom-value");
        
        // Setup response headers
        Map<String, String> responseHeaders = new HashMap<>();
        responseHeaders.put("Content-Type", "application/json");
        responseHeaders.put("Cache-Control", "no-cache");
        responseHeaders.put("X-Response-Header", "response-value");
        
        // Setup mock response
        when(serviceClient.forward(any(URI.class), anyString(), any(), any()))
            .thenReturn(new ServiceClient.Response(200, "{}", responseHeaders));
        
        // Execute request
        ServiceClient.Response response = apiGateway.routeRequest(
                "/api/positions", "GET", requestHeaders, "", "v1");
        
        // Verify response headers are propagated back
        assertNotNull(response);
        assertEquals("application/json", response.getHeaders().get("Content-Type"));
        assertEquals("no-cache", response.getHeaders().get("Cache-Control"));
        assertEquals("response-value", response.getHeaders().get("X-Response-Header"));
        
        // Verify request headers are propagated to the service
        verify(serviceClient).forward(
                any(URI.class), 
                anyString(), 
                eq(requestHeaders), 
                any());
    }
    
    @Test
    public void testApiVersioning() throws IOException {
        // Test different API versions
        String[] versions = {"v1", "v2"};
        String path = "/api/devices";
        String method = "GET";
        
        for (String version : versions) {
            // Setup route registry for each version
            ServiceRoute route = new ServiceRoute("position-service", path, method, version);
            when(routeRegistry.findRoute(eq(path), eq(method), eq(version)))
                .thenReturn(route);
            
            // Setup service discovery
            when(serviceDiscovery.getServiceUrl("position-service"))
                .thenReturn(URI.create("http://position-service:8080"));
            
            // Setup circuit breaker
            when(circuitBreaker.isOpen("position-service")).thenReturn(false);
            
            // Setup mock response
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", "application/json");
            when(serviceClient.forward(any(URI.class), eq(method), any(), any()))
                .thenReturn(new ServiceClient.Response(200, "{}", headers));
            
            // Execute request
            ServiceClient.Response response = apiGateway.routeRequest(
                    path, method, Collections.emptyMap(), "", version);
            
            // Verify response
            assertNotNull(response);
            assertEquals(200, response.getStatusCode());
            
            // Verify service client was called with correct URI
            verify(serviceClient).forward(
                    eq(URI.create("http://position-service:8080" + path)), 
                    eq(method), 
                    any(), 
                    any());
        }
    }
    
    @Test
    public void testCircuitBreakerOpen() {
        // Setup route registry
        ServiceRoute route = new ServiceRoute("position-service", "/api/positions", "GET", "v1");
        when(routeRegistry.findRoute(eq("/api/positions"), eq("GET"), anyString()))
            .thenReturn(route);
        
        // Setup service discovery
        when(serviceDiscovery.getServiceUrl("position-service"))
            .thenReturn(URI.create("http://position-service:8080"));
        
        // Setup circuit breaker to be open (service unavailable)
        when(circuitBreaker.isOpen("position-service")).thenReturn(true);
        
        // Execute request and expect exception
        assertThrows(ServiceUnavailableException.class, () -> {
            apiGateway.routeRequest("/api/positions", "GET", Collections.emptyMap(), "", "v1");
        });
        
        // Verify service client was not called
        verify(serviceClient, times(0)).forward(any(), any(), any(), any());
    }
    
    @Test
    public void testServiceDiscoveryFailure() {
        // Setup route registry
        ServiceRoute route = new ServiceRoute("position-service", "/api/positions", "GET", "v1");
        when(routeRegistry.findRoute(eq("/api/positions"), eq("GET"), anyString()))
            .thenReturn(route);
        
        // Setup service discovery to fail
        when(serviceDiscovery.getServiceUrl("position-service"))
            .thenThrow(new RuntimeException("Service discovery failed"));
        
        // Setup circuit breaker
        when(circuitBreaker.isOpen("position-service")).thenReturn(false);
        
        // Execute request and expect exception
        Exception exception = assertThrows(RuntimeException.class, () -> {
            apiGateway.routeRequest("/api/positions", "GET", Collections.emptyMap(), "", "v1");
        });
        
        // Verify exception message
        assertTrue(exception.getMessage().contains("Service discovery failed"));
        
        // Verify service client was not called
        verify(serviceClient, times(0)).forward(any(), any(), any(), any());
    }
    
    @Test
    public void testRouteNotFound() {
        // Setup route registry to return null (route not found)
        when(routeRegistry.findRoute(anyString(), anyString(), anyString()))
            .thenReturn(null);
        
        // Execute request and expect exception
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            apiGateway.routeRequest("/api/unknown", "GET", Collections.emptyMap(), "", "v1");
        });
        
        // Verify exception message
        assertTrue(exception.getMessage().contains("No route found"));
        
        // Verify service discovery and service client were not called
        verify(serviceDiscovery, times(0)).getServiceUrl(anyString());
        verify(serviceClient, times(0)).forward(any(), any(), any(), any());
    }
    
    @Test
    public void testServiceClientFailure() throws IOException {
        // Setup route registry
        ServiceRoute route = new ServiceRoute("position-service", "/api/positions", "GET", "v1");
        when(routeRegistry.findRoute(eq("/api/positions"), eq("GET"), anyString()))
            .thenReturn(route);
        
        // Setup service discovery
        when(serviceDiscovery.getServiceUrl("position-service"))
            .thenReturn(URI.create("http://position-service:8080"));
        
        // Setup circuit breaker
        when(circuitBreaker.isOpen("position-service")).thenReturn(false);
        
        // Setup service client to fail
        when(serviceClient.forward(any(URI.class), anyString(), any(), any()))
            .thenThrow(new IOException("Connection refused"));
        
        // Execute request and expect exception
        Exception exception = assertThrows(IOException.class, () -> {
            apiGateway.routeRequest("/api/positions", "GET", Collections.emptyMap(), "", "v1");
        });
        
        // Verify exception message
        assertTrue(exception.getMessage().contains("Connection refused"));
        
        // Verify circuit breaker was notified of the failure
        verify(circuitBreaker).recordFailure("position-service");
    }
}