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
package org.traccar.geolocation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;
import org.traccar.model.Network;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class GeolocationServiceManagerTest {

    @Mock
    private ServiceDiscovery serviceDiscovery;

    @Mock
    private GeolocationProvider geolocationProvider;

    @Mock
    private GeolocationProvider.LocationProviderCallback locationCallback;

    private GeolocationServiceManager geolocationServiceManager;

    private ServiceInstance healthyService;
    private ServiceInstance unhealthyService;
    private Network testNetwork;

    @BeforeEach
    public void setUp() {
        // Create test service instances
        healthyService = new ServiceInstance("geo-1", "geolocation-service", "geo-host-1", 8080);
        healthyService.setHealthy(true);
        healthyService.setAttribute("provider", "google");

        unhealthyService = new ServiceInstance("geo-2", "geolocation-service", "geo-host-2", 8080);
        unhealthyService.setHealthy(false);
        unhealthyService.setAttribute("provider", "opencellid");

        // Create test network for geolocation
        testNetwork = new Network();

        // Initialize the GeolocationServiceManager with mocked ServiceDiscovery
        geolocationServiceManager = new GeolocationServiceManager(serviceDiscovery);
    }

    @Test
    public void testServiceRegistrationAndDiscovery() {
        // Setup mock to return a list of service instances
        when(serviceDiscovery.findServiceInstances(eq("geolocation-service")))
                .thenReturn(Arrays.asList(healthyService, unhealthyService));

        // Test service discovery
        List<ServiceInstance> services = geolocationServiceManager.discoverGeolocationServices();

        // Verify service discovery was called
        verify(serviceDiscovery).findServiceInstances(eq("geolocation-service"));

        // Verify the correct services were returned
        assertEquals(2, services.size());
        assertTrue(services.contains(healthyService));
        assertTrue(services.contains(unhealthyService));
    }

    @Test
    public void testServiceSelectionBasedOnHealth() {
        // Setup mock to return a list of service instances
        when(serviceDiscovery.findServiceInstances(eq("geolocation-service")))
                .thenReturn(Arrays.asList(healthyService, unhealthyService));

        // Test service selection based on health
        ServiceInstance selectedService = geolocationServiceManager.selectHealthyGeolocationService();

        // Verify service discovery was called
        verify(serviceDiscovery).findServiceInstances(eq("geolocation-service"));

        // Verify the healthy service was selected
        assertEquals(healthyService, selectedService);
    }

    @Test
    public void testServiceSelectionWhenNoHealthyServices() {
        // Setup mock to return a list with only unhealthy services
        when(serviceDiscovery.findServiceInstances(eq("geolocation-service")))
                .thenReturn(Collections.singletonList(unhealthyService));

        // Test service selection when no healthy services are available
        ServiceInstance selectedService = geolocationServiceManager.selectHealthyGeolocationService();

        // Verify service discovery was called
        verify(serviceDiscovery).findServiceInstances(eq("geolocation-service"));

        // Verify no service was selected
        assertNull(selectedService);
    }

    @Test
    public void testServiceEndpointCaching() {
        // Setup mock to return a list of service instances
        when(serviceDiscovery.findServiceInstances(eq("geolocation-service")))
                .thenReturn(Arrays.asList(healthyService, unhealthyService));

        // First call should query the service discovery
        ServiceInstance service1 = geolocationServiceManager.selectHealthyGeolocationService();
        assertEquals(healthyService, service1);
        verify(serviceDiscovery, times(1)).findServiceInstances(anyString());

        // Second call within TTL should use cached result
        ServiceInstance service2 = geolocationServiceManager.selectHealthyGeolocationService();
        assertEquals(healthyService, service2);
        verify(serviceDiscovery, times(1)).findServiceInstances(anyString()); // Still only called once

        // Simulate TTL expiration
        geolocationServiceManager.clearCache();

        // Third call after TTL expiration should query service discovery again
        ServiceInstance service3 = geolocationServiceManager.selectHealthyGeolocationService();
        assertEquals(healthyService, service3);
        verify(serviceDiscovery, times(2)).findServiceInstances(anyString()); // Called twice now
    }

    @Test
    public void testGeolocationWithServiceDiscovery() throws ExecutionException, InterruptedException, TimeoutException {
        // Setup mock to return a list of service instances
        when(serviceDiscovery.findServiceInstances(eq("geolocation-service")))
                .thenReturn(Collections.singletonList(healthyService));

        // Setup mock for the GeolocationProvider
        when(geolocationServiceManager.createGeolocationProvider(any(ServiceInstance.class)))
                .thenReturn(geolocationProvider);

        // Create a CompletableFuture to capture the callback result
        CompletableFuture<Boolean> callbackInvoked = new CompletableFuture<>();

        // Setup the geolocationProvider mock to invoke the callback
        doAnswer(invocation -> {
            GeolocationProvider.LocationProviderCallback callback = invocation.getArgument(1);
            callback.onSuccess(1.0, 2.0, 3.0);
            callbackInvoked.complete(true);
            return null;
        }).when(geolocationProvider).getLocation(eq(testNetwork), any(GeolocationProvider.LocationProviderCallback.class));

        // Test geolocation with service discovery
        geolocationServiceManager.getLocation(testNetwork, locationCallback);

        // Wait for the callback to be invoked
        assertTrue(callbackInvoked.get(1, TimeUnit.SECONDS));

        // Verify service discovery was called
        verify(serviceDiscovery).findServiceInstances(eq("geolocation-service"));

        // Verify the geolocation provider was created and used
        verify(geolocationServiceManager).createGeolocationProvider(eq(healthyService));
        verify(geolocationProvider).getLocation(eq(testNetwork), any(GeolocationProvider.LocationProviderCallback.class));

        // Verify the callback was invoked with the correct values
        verify(locationCallback).onSuccess(1.0, 2.0, 3.0);
    }

    @Test
    public void testGeolocationFailureHandling() throws ExecutionException, InterruptedException, TimeoutException {
        // Setup mock to return a list of service instances
        when(serviceDiscovery.findServiceInstances(eq("geolocation-service")))
                .thenReturn(Collections.singletonList(healthyService));

        // Setup mock for the GeolocationProvider
        when(geolocationServiceManager.createGeolocationProvider(any(ServiceInstance.class)))
                .thenReturn(geolocationProvider);

        // Create a CompletableFuture to capture the callback result
        CompletableFuture<Boolean> callbackInvoked = new CompletableFuture<>();

        // Setup the geolocationProvider mock to invoke the failure callback
        GeolocationException testException = new GeolocationException("Test geolocation failure");
        doAnswer(invocation -> {
            GeolocationProvider.LocationProviderCallback callback = invocation.getArgument(1);
            callback.onFailure(testException);
            callbackInvoked.complete(true);
            return null;
        }).when(geolocationProvider).getLocation(eq(testNetwork), any(GeolocationProvider.LocationProviderCallback.class));

        // Test geolocation failure handling
        geolocationServiceManager.getLocation(testNetwork, locationCallback);

        // Wait for the callback to be invoked
        assertTrue(callbackInvoked.get(1, TimeUnit.SECONDS));

        // Verify service discovery was called
        verify(serviceDiscovery).findServiceInstances(eq("geolocation-service"));

        // Verify the geolocation provider was created and used
        verify(geolocationServiceManager).createGeolocationProvider(eq(healthyService));
        verify(geolocationProvider).getLocation(eq(testNetwork), any(GeolocationProvider.LocationProviderCallback.class));

        // Verify the failure callback was invoked with the correct exception
        verify(locationCallback).onFailure(testException);
    }

    @Test
    public void testNoAvailableGeolocationServices() {
        // Setup mock to return an empty list of service instances
        when(serviceDiscovery.findServiceInstances(eq("geolocation-service")))
                .thenReturn(Collections.emptyList());

        // Test geolocation when no services are available
        geolocationServiceManager.getLocation(testNetwork, locationCallback);

        // Verify service discovery was called
        verify(serviceDiscovery).findServiceInstances(eq("geolocation-service"));

        // Verify the failure callback was invoked with a GeolocationException
        verify(locationCallback).onFailure(any(GeolocationException.class));
    }

    @Test
    public void testServiceRefreshAfterFailure() {
        // First call - return a healthy service
        when(serviceDiscovery.findServiceInstances(eq("geolocation-service")))
                .thenReturn(Collections.singletonList(healthyService));

        // Get the service - should be cached
        ServiceInstance service1 = geolocationServiceManager.selectHealthyGeolocationService();
        assertEquals(healthyService, service1);
        verify(serviceDiscovery, times(1)).findServiceInstances(anyString());

        // Simulate a service failure
        geolocationServiceManager.reportServiceFailure(healthyService);

        // Second call - should refresh the cache and call service discovery again
        ServiceInstance service2 = geolocationServiceManager.selectHealthyGeolocationService();
        verify(serviceDiscovery, times(2)).findServiceInstances(anyString());
    }
}