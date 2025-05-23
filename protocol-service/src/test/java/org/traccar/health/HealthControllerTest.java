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
package org.traccar.health;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthComponent;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link HealthController} class which exposes standardized health check endpoints
 * compatible with Kubernetes liveness, readiness, and startup probes.
 */
@ExtendWith(MockitoExtension.class)
public class HealthControllerTest {

    @Mock
    private HealthEndpoint healthEndpoint;

    @InjectMocks
    private HealthController healthController;

    private Health healthUp;
    private Health healthDown;
    private Health healthOutOfService;

    @BeforeEach
    public void setUp() {
        healthUp = Health.up().build();
        healthDown = Health.down().build();
        healthOutOfService = Health.outOfService().build();
    }

    @Test
    public void testLivenessEndpointWhenUp() {
        when(healthEndpoint.healthForPath("liveness")).thenReturn(healthUp);

        ResponseEntity<HealthComponent> response = healthController.getLivenessStatus();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Status.UP, response.getBody().getStatus());
    }

    @Test
    public void testLivenessEndpointWhenDown() {
        when(healthEndpoint.healthForPath("liveness")).thenReturn(healthDown);

        ResponseEntity<HealthComponent> response = healthController.getLivenessStatus();

        assertNotNull(response);
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
        assertEquals(Status.DOWN, response.getBody().getStatus());
    }

    @Test
    public void testLivenessEndpointWhenOutOfService() {
        when(healthEndpoint.healthForPath("liveness")).thenReturn(healthOutOfService);

        ResponseEntity<HealthComponent> response = healthController.getLivenessStatus();

        assertNotNull(response);
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
        assertEquals(Status.OUT_OF_SERVICE, response.getBody().getStatus());
    }

    @Test
    public void testReadinessEndpointWhenUp() {
        when(healthEndpoint.healthForPath("readiness")).thenReturn(healthUp);

        ResponseEntity<HealthComponent> response = healthController.getReadinessStatus();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Status.UP, response.getBody().getStatus());
    }

    @Test
    public void testReadinessEndpointWhenDown() {
        when(healthEndpoint.healthForPath("readiness")).thenReturn(healthDown);

        ResponseEntity<HealthComponent> response = healthController.getReadinessStatus();

        assertNotNull(response);
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
        assertEquals(Status.DOWN, response.getBody().getStatus());
    }

    @Test
    public void testReadinessEndpointWhenOutOfService() {
        when(healthEndpoint.healthForPath("readiness")).thenReturn(healthOutOfService);

        ResponseEntity<HealthComponent> response = healthController.getReadinessStatus();

        assertNotNull(response);
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
        assertEquals(Status.OUT_OF_SERVICE, response.getBody().getStatus());
    }

    @Test
    public void testStartupEndpointWhenUp() {
        when(healthEndpoint.healthForPath("startup")).thenReturn(healthUp);

        ResponseEntity<HealthComponent> response = healthController.getStartupStatus();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Status.UP, response.getBody().getStatus());
    }

    @Test
    public void testStartupEndpointWhenDown() {
        when(healthEndpoint.healthForPath("startup")).thenReturn(healthDown);

        ResponseEntity<HealthComponent> response = healthController.getStartupStatus();

        assertNotNull(response);
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
        assertEquals(Status.DOWN, response.getBody().getStatus());
    }

    @Test
    public void testStartupEndpointWhenOutOfService() {
        when(healthEndpoint.healthForPath("startup")).thenReturn(healthOutOfService);

        ResponseEntity<HealthComponent> response = healthController.getStartupStatus();

        assertNotNull(response);
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
        assertEquals(Status.OUT_OF_SERVICE, response.getBody().getStatus());
    }

    @Test
    public void testGeneralHealthEndpointWhenUp() {
        when(healthEndpoint.health()).thenReturn(healthUp);

        ResponseEntity<HealthComponent> response = healthController.getHealth();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Status.UP, response.getBody().getStatus());
    }

    @Test
    public void testGeneralHealthEndpointWhenDown() {
        when(healthEndpoint.health()).thenReturn(healthDown);

        ResponseEntity<HealthComponent> response = healthController.getHealth();

        assertNotNull(response);
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
        assertEquals(Status.DOWN, response.getBody().getStatus());
    }

    @Test
    public void testGeneralHealthEndpointWhenOutOfService() {
        when(healthEndpoint.health()).thenReturn(healthOutOfService);

        ResponseEntity<HealthComponent> response = healthController.getHealth();

        assertNotNull(response);
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
        assertEquals(Status.OUT_OF_SERVICE, response.getBody().getStatus());
    }

    @Test
    public void testHealthDetailsIncludedInResponse() {
        Health detailedHealth = Health.up()
                .withDetail("database", "UP")
                .withDetail("diskSpace", "UP")
                .withDetail("messageBroker", "UP")
                .build();

        when(healthEndpoint.health()).thenReturn(detailedHealth);

        ResponseEntity<HealthComponent> response = healthController.getHealth();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Status.UP, response.getBody().getStatus());
        
        Health responseHealth = (Health) response.getBody();
        assertNotNull(responseHealth.getDetails());
        assertEquals("UP", responseHealth.getDetails().get("database"));
        assertEquals("UP", responseHealth.getDetails().get("diskSpace"));
        assertEquals("UP", responseHealth.getDetails().get("messageBroker"));
    }
}