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
package org.traccar.discovery;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link ServiceRegistryFactory} to verify it correctly creates
 * the appropriate ServiceRegistry implementation based on configuration settings.
 */
@ExtendWith(MockitoExtension.class)
public class ServiceRegistryFactoryTest {

    @Mock
    private ServiceRegistryConfig config;

    @BeforeEach
    public void setUp() {
        // Default mock behavior
        when(config.getRegistryType()).thenReturn(ServiceRegistryFactory.REGISTRY_TYPE_CONSUL);
    }

    @AfterEach
    public void tearDown() {
        // Reset the factory to ensure each test starts with a clean state
        ServiceRegistryFactory.reset();
    }

    @Test
    public void testCreateConsulRegistry() {
        // Configure mock to return Consul registry type
        when(config.getRegistryType()).thenReturn(ServiceRegistryFactory.REGISTRY_TYPE_CONSUL);
        
        // Create registry using factory
        ServiceRegistry registry = ServiceRegistryFactory.create(config);
        
        // Verify correct implementation is returned
        assertNotNull(registry, "Registry should not be null");
        assertTrue(registry instanceof ConsulServiceRegistry, "Registry should be ConsulServiceRegistry");
        assertEquals("consul", registry.getRegistryType(), "Registry type should be 'consul'");
    }

    @Test
    public void testCreateKubernetesRegistry() {
        // Configure mock to return Kubernetes registry type
        when(config.getRegistryType()).thenReturn(ServiceRegistryFactory.REGISTRY_TYPE_KUBERNETES);
        
        // Create registry using factory
        ServiceRegistry registry = ServiceRegistryFactory.create(config);
        
        // Verify correct implementation is returned
        assertNotNull(registry, "Registry should not be null");
        assertTrue(registry instanceof KubernetesServiceRegistry, "Registry should be KubernetesServiceRegistry");
        assertEquals("kubernetes", registry.getRegistryType(), "Registry type should be 'kubernetes'");
    }

    @Test
    public void testDefaultFallbackWhenTypeNotSpecified() {
        // Configure mock to return null registry type (not specified)
        when(config.getRegistryType()).thenReturn(null);
        
        // Create registry using factory
        ServiceRegistry registry = ServiceRegistryFactory.create(config);
        
        // Verify fallback to default (Consul) registry
        assertNotNull(registry, "Registry should not be null");
        assertTrue(registry instanceof ConsulServiceRegistry, "Registry should fall back to ConsulServiceRegistry");
        assertEquals("consul", registry.getRegistryType(), "Registry type should be 'consul'");
    }

    @Test
    public void testFallbackToDefaultForUnknownType() {
        // Configure mock to return an unknown registry type
        when(config.getRegistryType()).thenReturn("unknown");
        
        // Create registry using factory
        ServiceRegistry registry = ServiceRegistryFactory.create(config);
        
        // Verify fallback to default (Consul) registry
        assertNotNull(registry, "Registry should not be null");
        assertTrue(registry instanceof ConsulServiceRegistry, "Registry should fall back to ConsulServiceRegistry");
        assertEquals("consul", registry.getRegistryType(), "Registry type should be 'consul'");
    }

    @Test
    public void testEmptyRegistryType() {
        // Configure mock to return empty registry type
        when(config.getRegistryType()).thenReturn("");
        
        // Create registry using factory
        ServiceRegistry registry = ServiceRegistryFactory.create(config);
        
        // Verify fallback to default (Consul) registry
        assertNotNull(registry, "Registry should not be null");
        assertTrue(registry instanceof ConsulServiceRegistry, "Registry should fall back to ConsulServiceRegistry");
        assertEquals("consul", registry.getRegistryType(), "Registry type should be 'consul'");
    }

    @Test
    public void testSingletonPattern() {
        // Configure mock to return Consul registry type
        when(config.getRegistryType()).thenReturn(ServiceRegistryFactory.REGISTRY_TYPE_CONSUL);
        
        // Create first registry instance
        ServiceRegistry firstRegistry = ServiceRegistryFactory.create(config);
        
        // Create second registry instance with same config
        ServiceRegistry secondRegistry = ServiceRegistryFactory.create(config);
        
        // Verify both references point to the same instance (singleton pattern)
        assertSame(firstRegistry, secondRegistry, "Factory should reuse existing registry instances");
        
        // Reset factory
        ServiceRegistryFactory.reset();
        
        // Create new registry after reset
        ServiceRegistry thirdRegistry = ServiceRegistryFactory.create(config);
        
        // Verify new instance is created after reset
        assertNotSame(firstRegistry, thirdRegistry, "Factory should create new instance after reset");
    }

    @Test
    public void testSeparateInstancesForDifferentTypes() {
        // Configure mock to return Consul registry type
        when(config.getRegistryType()).thenReturn(ServiceRegistryFactory.REGISTRY_TYPE_CONSUL);
        
        // Create Consul registry
        ServiceRegistry consulRegistry = ServiceRegistryFactory.create(config);
        
        // Reconfigure mock to return Kubernetes registry type
        when(config.getRegistryType()).thenReturn(ServiceRegistryFactory.REGISTRY_TYPE_KUBERNETES);
        
        // Create Kubernetes registry
        ServiceRegistry kubernetesRegistry = ServiceRegistryFactory.create(config);
        
        // Verify different instances are created for different registry types
        assertNotSame(consulRegistry, kubernetesRegistry, 
                "Factory should create separate instances for different registry types");
        
        // Verify correct implementation types
        assertTrue(consulRegistry instanceof ConsulServiceRegistry, 
                "First registry should be ConsulServiceRegistry");
        assertTrue(kubernetesRegistry instanceof KubernetesServiceRegistry, 
                "Second registry should be KubernetesServiceRegistry");
    }
}