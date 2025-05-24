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
package org.traccar.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.BaseTest;
import org.traccar.api.security.PermissionsService;
import org.traccar.api.security.UserPrincipal;
import org.traccar.storage.Storage;

import jakarta.ws.rs.core.SecurityContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the BaseResource class which serves as the foundation for all API resources
 * in the API Gateway. Tests dependency injection, security context handling, and the getUserId()
 * method under various authentication scenarios.
 */
@ExtendWith(MockitoExtension.class)
public class BaseResourceTest extends BaseTest {

    @Mock
    private SecurityContext securityContext;

    @Mock
    private UserPrincipal userPrincipal;

    @Mock
    private Storage storage;

    @Mock
    private PermissionsService permissionsService;

    private BaseResource baseResource;

    @BeforeEach
    public void setUp() {
        super.setUp();
        baseResource = new TestBaseResource();
    }

    /**
     * Test implementation of BaseResource that exposes protected fields for testing
     */
    private class TestBaseResource extends BaseResource {
        public SecurityContext getSecurityContext() {
            return securityContext;
        }

        public Storage getStorage() {
            return storage;
        }

        public PermissionsService getPermissionsService() {
            return permissionsService;
        }

        @Override
        public long getUserId() {
            return super.getUserId();
        }
    }

    @Test
    public void testDependencyInjection() {
        // Set the fields using reflection to simulate dependency injection
        java.lang.reflect.Field securityContextField;
        java.lang.reflect.Field storageField;
        java.lang.reflect.Field permissionsServiceField;

        try {
            securityContextField = BaseResource.class.getDeclaredField("securityContext");
            securityContextField.setAccessible(true);
            securityContextField.set(baseResource, securityContext);

            storageField = BaseResource.class.getDeclaredField("storage");
            storageField.setAccessible(true);
            storageField.set(baseResource, storage);

            permissionsServiceField = BaseResource.class.getDeclaredField("permissionsService");
            permissionsServiceField.setAccessible(true);
            permissionsServiceField.set(baseResource, permissionsService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set fields via reflection", e);
        }

        // Verify that the dependencies were injected correctly
        TestBaseResource testResource = (TestBaseResource) baseResource;
        assertEquals(securityContext, testResource.getSecurityContext());
        assertEquals(storage, testResource.getStorage());
        assertEquals(permissionsService, testResource.getPermissionsService());
    }

    @Test
    public void testSecurityContextHandling() {
        // Set the security context field using reflection
        java.lang.reflect.Field securityContextField;
        try {
            securityContextField = BaseResource.class.getDeclaredField("securityContext");
            securityContextField.setAccessible(true);
            securityContextField.set(baseResource, securityContext);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set securityContext field via reflection", e);
        }

        // Verify that the security context was set correctly
        TestBaseResource testResource = (TestBaseResource) baseResource;
        assertNotNull(testResource.getSecurityContext());
        assertEquals(securityContext, testResource.getSecurityContext());
    }

    @Test
    public void testGetUserIdWithAuthenticatedUser() {
        // Set up the security context with an authenticated user
        long expectedUserId = 123L;
        when(userPrincipal.getUserId()).thenReturn(expectedUserId);
        when(securityContext.getUserPrincipal()).thenReturn(userPrincipal);

        // Set the security context field using reflection
        java.lang.reflect.Field securityContextField;
        try {
            securityContextField = BaseResource.class.getDeclaredField("securityContext");
            securityContextField.setAccessible(true);
            securityContextField.set(baseResource, securityContext);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set securityContext field via reflection", e);
        }

        // Test the getUserId method
        long actualUserId = ((TestBaseResource) baseResource).getUserId();
        assertEquals(expectedUserId, actualUserId, "getUserId should return the ID of the authenticated user");
    }

    @Test
    public void testGetUserIdWithNoAuthentication() {
        // Set up the security context with no authenticated user
        when(securityContext.getUserPrincipal()).thenReturn(null);

        // Set the security context field using reflection
        java.lang.reflect.Field securityContextField;
        try {
            securityContextField = BaseResource.class.getDeclaredField("securityContext");
            securityContextField.setAccessible(true);
            securityContextField.set(baseResource, securityContext);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set securityContext field via reflection", e);
        }

        // Test the getUserId method
        long actualUserId = ((TestBaseResource) baseResource).getUserId();
        assertEquals(0L, actualUserId, "getUserId should return 0 when there is no authenticated user");
    }
}