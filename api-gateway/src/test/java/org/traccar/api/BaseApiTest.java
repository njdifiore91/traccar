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

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.BaseTest;
import org.traccar.api.security.SecurityRequestFilter;
import org.traccar.helper.LogAction;
import org.traccar.model.User;
import org.traccar.storage.StorageException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.Principal;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BaseApiTest extends JerseyTest {

    @Mock
    protected PermissionsService permissionsService;

    @Mock
    protected SecurityRequestFilter securityRequestFilter;

    @Mock
    protected Principal principal;

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        // Mock the principal to return a user ID
        when(principal.getName()).thenReturn("1");
        
        // Mock the security filter to return the principal
        when(securityRequestFilter.getPrincipal()).thenReturn(principal);
        
        // Mock permissions service to allow access by default
        when(permissionsService.checkPermission(anyString(), anyLong())).thenReturn(true);
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    protected Application configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        
        ResourceConfig config = new ResourceConfig();
        config.register(JacksonFeature.class);
        config.register(ObjectMapperProvider.class);
        config.register(ResourceErrorHandler.class);
        config.register(securityRequestFilter);
        config.register(LogAction.class);
        
        return config;
    }

    @Override
    protected void configureClient(ClientConfig config) {
        config.register(ObjectMapperProvider.class);
    }

    protected void initResource(BaseResource resource) {
        try {
            // Set the security context field in the BaseResource
            Field securityContextField = BaseResource.class.getDeclaredField("securityContext");
            securityContextField.setAccessible(true);
            securityContextField.set(resource, securityRequestFilter);
            
            // Call the init method if it exists
            try {
                Method initMethod = BaseResource.class.getDeclaredMethod("init");
                initMethod.setAccessible(true);
                initMethod.invoke(resource);
            } catch (NoSuchMethodException e) {
                // init method might not exist in all versions, ignore
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected User createUser() {
        User user = new User();
        user.setId(1L);
        user.setEmail("user@example.com");
        user.setName("Test User");
        return user;
    }
}