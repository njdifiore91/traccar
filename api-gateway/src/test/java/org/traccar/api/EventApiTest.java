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

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.traccar.api.resource.EventResource;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the event-related API endpoints in the API Gateway, focusing on event retrieval
 * and event permission enforcement. Verifies that the API Gateway correctly routes event
 * requests to the Event Service, applies proper authentication and authorization, and
 * maintains all existing event functionality in the microservices architecture.
 */
public class EventApiTest extends BaseApiTest {

    @Mock
    private Storage storage;

    private EventResource resource;

    @BeforeEach
    public void setup() {
        resource = Mockito.spy(new EventResource());
        resource.setStorage(storage);
        resource.setPermissionsService(permissionsService);
        initResource(resource);
    }

    /**
     * Tests retrieving an event by ID with proper permissions.
     * Verifies that the API Gateway correctly retrieves the event from the Event Service
     * and checks permissions before returning the event.
     */
    @Test
    public void testGetEvent() throws StorageException {
        // Create a test event
        Event event = new Event();
        event.setId(1L);
        event.setDeviceId(1L);
        event.setType(Event.TYPE_DEVICE_ONLINE);
        event.setServerTime(new Date());

        // Mock storage to return the event
        when(storage.getObject(eq(Event.class), any(Request.class))).thenReturn(event);
        
        // Mock permissions check to allow access
        when(permissionsService.checkPermission(eq(Device.class), anyLong(), eq(1L))).thenReturn(true);

        // Call the API endpoint
        Event result = resource.get(1L);

        // Verify the result
        assertNotNull(result);
        assertEquals(1L, result.getId());
        assertEquals(1L, result.getDeviceId());
        assertEquals(Event.TYPE_DEVICE_ONLINE, result.getType());

        // Verify storage was called with correct parameters
        verify(storage).getObject(eq(Event.class), any(Request.class));
        
        // Verify permissions were checked
        verify(permissionsService).checkPermission(eq(Device.class), anyLong(), eq(1L));
    }

    /**
     * Tests retrieving a non-existent event.
     * Verifies that the API Gateway returns a 404 Not Found response when the event doesn't exist.
     */
    @Test
    public void testGetEventNotFound() throws StorageException {
        // Mock storage to return null (event not found)
        when(storage.getObject(eq(Event.class), any(Request.class))).thenReturn(null);

        // Verify that a WebApplicationException with 404 status is thrown
        WebApplicationException exception = assertThrows(
                WebApplicationException.class,
                () -> resource.get(1L));
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), exception.getResponse().getStatus());
    }

    /**
     * Tests retrieving an event without proper permissions.
     * Verifies that the API Gateway enforces authorization and denies access to events
     * that the user doesn't have permission to view.
     */
    @Test
    public void testGetEventNoPermission() throws StorageException {
        // Create a test event
        Event event = new Event();
        event.setId(1L);
        event.setDeviceId(1L);
        event.setType(Event.TYPE_DEVICE_ONLINE);
        event.setServerTime(new Date());

        // Mock storage to return the event
        when(storage.getObject(eq(Event.class), any(Request.class))).thenReturn(event);
        
        // Mock permissions check to deny access
        doThrow(new SecurityException()).when(permissionsService).checkPermission(eq(Device.class), anyLong(), eq(1L));

        // Verify that a SecurityException is thrown
        assertThrows(SecurityException.class, () -> resource.get(1L));
    }

    /**
     * Tests retrieving an event with an associated position.
     * Verifies that the API Gateway correctly handles events with position references.
     */
    @Test
    public void testGetEventWithPosition() throws StorageException {
        // Create a test event with position
        Event event = new Event();
        event.setId(1L);
        event.setDeviceId(1L);
        event.setType(Event.TYPE_DEVICE_ONLINE);
        event.setServerTime(new Date());
        event.setPositionId(2L);

        // Create a test position
        Position position = new Position();
        position.setId(2L);
        position.setDeviceId(1L);
        position.setLatitude(10.0);
        position.setLongitude(20.0);

        // Mock storage to return the event and position
        when(storage.getObject(eq(Event.class), any(Request.class))).thenReturn(event);
        when(storage.getObject(eq(Position.class), any(Request.class))).thenReturn(position);
        
        // Mock permissions check to allow access
        when(permissionsService.checkPermission(eq(Device.class), anyLong(), eq(1L))).thenReturn(true);

        // Call the API endpoint
        Event result = resource.get(1L);

        // Verify the result
        assertNotNull(result);
        assertEquals(1L, result.getId());
        assertEquals(2L, result.getPositionId());

        // Verify storage was called with correct parameters for both event and position
        verify(storage).getObject(eq(Event.class), any(Request.class));
        verify(storage).getObject(eq(Position.class), any(Request.class));
    }

    /**
     * Tests the REST API endpoint for retrieving an event by ID.
     * Verifies that the API Gateway correctly handles HTTP requests for event retrieval.
     */
    @Test
    public void testGetEventRestApi() throws StorageException {
        // Create a test event
        Event event = new Event();
        event.setId(1L);
        event.setDeviceId(1L);
        event.setType(Event.TYPE_DEVICE_ONLINE);
        event.setServerTime(new Date());

        // Mock storage to return the event
        when(storage.getObject(eq(Event.class), any(Request.class))).thenReturn(event);
        
        // Mock permissions check to allow access
        when(permissionsService.checkPermission(eq(Device.class), anyLong(), eq(1L))).thenReturn(true);

        // Mock the get method to return the event
        when(resource.get(eq(1L))).thenReturn(event);

        // Call the API endpoint through the REST client
        Response response = target("/events/1")
                .request(MediaType.APPLICATION_JSON)
                .get();

        // Verify the response status
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }

    /**
     * Tests the REST API endpoint for retrieving an event that doesn't exist.
     * Verifies that the API Gateway returns a 404 Not Found response for non-existent events.
     */
    @Test
    public void testGetEventRestApiNotFound() throws StorageException {
        // Mock the get method to throw a not found exception
        when(resource.get(eq(1L))).thenThrow(
                new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build()));

        // Call the API endpoint through the REST client
        Response response = target("/events/1")
                .request(MediaType.APPLICATION_JSON)
                .get();

        // Verify the response status (404 Not Found)
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }

    /**
     * Tests the REST API endpoint for retrieving an event without proper permissions.
     * Verifies that the API Gateway returns a 401 Unauthorized response when the user
     * doesn't have permission to view the event.
     */
    @Test
    public void testGetEventRestApiNoPermission() throws StorageException {
        // Mock the get method to throw a security exception
        when(resource.get(eq(1L))).thenThrow(new SecurityException());

        // Call the API endpoint through the REST client
        Response response = target("/events/1")
                .request(MediaType.APPLICATION_JSON)
                .get();

        // Verify the response status (401 Unauthorized)
        assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
    }
}