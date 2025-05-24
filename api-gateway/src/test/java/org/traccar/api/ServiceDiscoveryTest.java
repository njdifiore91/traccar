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
package org.traccar.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests the service discovery integration in the API Gateway to ensure dynamic resolution
 * of backend service endpoints.
 */
@ExtendWith(MockitoExtension.class)
public class ServiceDiscoveryTest {

    @Mock
    private ServiceDiscovery serviceDiscovery;

    @InjectMocks
    private ApiGatewayRouting apiGatewayRouting;
    
    /**
     * Custom exception thrown when a service is unavailable.
     */
    public static class ServiceUnavailableException extends RuntimeException {
        public ServiceUnavailableException(String message) {
            super(message);
        }
        
        public ServiceUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private ServiceInstance positionServiceInstance;
    private ServiceInstance eventServiceInstance;
    private ServiceInstance notificationServiceInstance;

    @BeforeEach
    public void setUp() {
        // Create test service instances
        positionServiceInstance = new ServiceInstance(
                "position-service",
                "position-service",
                "position-service.traccar.svc.cluster.local",
                8080,
                true,
                Map.of("version", "1.0.0", "environment", "test")
        );

        eventServiceInstance = new ServiceInstance(
                "event-service",
                "event-service",
                "event-service.traccar.svc.cluster.local",
                8080,
                true,
                Map.of("version", "1.0.0", "environment", "test")
        );

        notificationServiceInstance = new ServiceInstance(
                "notification-service",
                "notification-service",
                "notification-service.traccar.svc.cluster.local",
                8080,
                true,
                Map.of("version", "1.0.0", "environment", "test")
        );
    }

    /**
     * Tests that the API Gateway can detect when services are registered with the service discovery system.
     */
    @Test
    public void testServiceRegistrationDetection() {
        // Setup
        when(serviceDiscovery.getInstances("position-service")).thenReturn(List.of(positionServiceInstance));
        when(serviceDiscovery.getInstances("event-service")).thenReturn(List.of(eventServiceInstance));
        when(serviceDiscovery.getInstances("notification-service")).thenReturn(List.of(notificationServiceInstance));

        // Execute
        boolean positionServiceAvailable = apiGatewayRouting.isServiceAvailable("position-service");
        boolean eventServiceAvailable = apiGatewayRouting.isServiceAvailable("event-service");
        boolean notificationServiceAvailable = apiGatewayRouting.isServiceAvailable("notification-service");
        boolean nonExistentServiceAvailable = apiGatewayRouting.isServiceAvailable("non-existent-service");

        // Verify
        assertTrue(positionServiceAvailable, "Position service should be available");
        assertTrue(eventServiceAvailable, "Event service should be available");
        assertTrue(notificationServiceAvailable, "Notification service should be available");
        assertFalse(nonExistentServiceAvailable, "Non-existent service should not be available");

        verify(serviceDiscovery, times(1)).getInstances("position-service");
        verify(serviceDiscovery, times(1)).getInstances("event-service");
        verify(serviceDiscovery, times(1)).getInstances("notification-service");
        verify(serviceDiscovery, times(1)).getInstances("non-existent-service");
    }

    /**
     * Tests that the API Gateway can resolve service endpoints correctly.
     */
    @Test
    public void testEndpointResolution() {
        // Setup
        when(serviceDiscovery.getInstances("position-service")).thenReturn(List.of(positionServiceInstance));

        // Execute
        URI serviceUri = apiGatewayRouting.resolveServiceUrl("position-service");

        // Verify
        assertNotNull(serviceUri, "Service URI should not be null");
        assertEquals("http://position-service.traccar.svc.cluster.local:8080", serviceUri.toString(),
                "Service URI should match the expected endpoint");

        verify(serviceDiscovery, times(1)).getInstances("position-service");
    }

    /**
     * Tests that the API Gateway uses health status information from service discovery.
     */
    @Test
    public void testHealthCheckIntegration() {
        // Setup - create a healthy and unhealthy instance
        ServiceInstance healthyInstance = new ServiceInstance(
                "position-service-1",
                "position-service",
                "position-service-1.traccar.svc.cluster.local",
                8080,
                true,  // healthy
                Collections.emptyMap()
        );

        ServiceInstance unhealthyInstance = new ServiceInstance(
                "position-service-2",
                "position-service",
                "position-service-2.traccar.svc.cluster.local",
                8080,
                false,  // unhealthy
                Collections.emptyMap()
        );

        when(serviceDiscovery.getInstances("position-service"))
                .thenReturn(Arrays.asList(healthyInstance, unhealthyInstance));

        // Execute
        URI serviceUri = apiGatewayRouting.resolveServiceUrl("position-service");

        // Verify - should only select the healthy instance
        assertNotNull(serviceUri, "Service URI should not be null");
        assertEquals("http://position-service-1.traccar.svc.cluster.local:8080", serviceUri.toString(),
                "Service URI should match the healthy instance endpoint");

        verify(serviceDiscovery, times(1)).getInstances("position-service");
    }

    /**
     * Tests that the API Gateway can handle service instances being added or removed.
     */
    @Test
    public void testHandlingServiceChanges() {
        // Setup - initially one instance
        ServiceInstance instance1 = new ServiceInstance(
                "position-service-1",
                "position-service",
                "position-service-1.traccar.svc.cluster.local",
                8080,
                true,
                Collections.emptyMap()
        );

        when(serviceDiscovery.getInstances("position-service"))
                .thenReturn(List.of(instance1));

        // Execute first call
        URI serviceUri1 = apiGatewayRouting.resolveServiceUrl("position-service");

        // Verify first call
        assertEquals("http://position-service-1.traccar.svc.cluster.local:8080", serviceUri1.toString());

        // Setup - now two instances
        ServiceInstance instance2 = new ServiceInstance(
                "position-service-2",
                "position-service",
                "position-service-2.traccar.svc.cluster.local",
                8080,
                true,
                Collections.emptyMap()
        );

        when(serviceDiscovery.getInstances("position-service"))
                .thenReturn(Arrays.asList(instance1, instance2));

        // Execute second call - should load balance between instances
        apiGatewayRouting.refreshServiceCache();
        URI serviceUri2 = apiGatewayRouting.resolveServiceUrl("position-service");

        // Verify second call - should have called service discovery again
        assertNotNull(serviceUri2, "Service URI should not be null");
        verify(serviceDiscovery, times(2)).getInstances("position-service");

        // Setup - now instance1 is gone
        when(serviceDiscovery.getInstances("position-service"))
                .thenReturn(List.of(instance2));

        // Execute third call
        apiGatewayRouting.refreshServiceCache();
        URI serviceUri3 = apiGatewayRouting.resolveServiceUrl("position-service");

        // Verify third call
        assertEquals("http://position-service-2.traccar.svc.cluster.local:8080", serviceUri3.toString(),
                "Service URI should match the remaining instance endpoint");
        verify(serviceDiscovery, times(3)).getInstances("position-service");
    }

    /**
     * Tests that the API Gateway handles service unavailability gracefully.
     */
    @Test
    public void testServiceUnavailabilityScenarios() {
        // Setup - no instances available
        when(serviceDiscovery.getInstances("position-service"))
                .thenReturn(Collections.emptyList());

        // Execute
        Exception exception = assertThrows(ServiceUnavailableException.class, () -> {
            apiGatewayRouting.resolveServiceUrl("position-service");
        });

        // Verify
        assertTrue(exception.getMessage().contains("No healthy instances available"),
                "Exception message should indicate no healthy instances");
        verify(serviceDiscovery, times(1)).getInstances("position-service");

        // Setup - service discovery throws exception
        when(serviceDiscovery.getInstances("position-service"))
                .thenThrow(new RuntimeException("Service discovery unavailable"));

        // Execute
        exception = assertThrows(ServiceUnavailableException.class, () -> {
            apiGatewayRouting.resolveServiceUrl("position-service");
        });

        // Verify
        assertTrue(exception.getMessage().contains("Error resolving service"),
                "Exception message should indicate service discovery error");
        verify(serviceDiscovery, times(2)).getInstances("position-service");
    }
}