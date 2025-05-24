package org.traccar.api;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.multipart.MultipartFile;
import org.traccar.BaseTest;
import org.traccar.api.resource.DeviceResource;
import org.traccar.config.Keys;
import org.traccar.model.Device;
import org.traccar.model.DeviceAccumulators;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the device-related API endpoints in the API Gateway, focusing on device CRUD operations,
 * device sharing, image uploads, and accumulator updates. Verifies that the API Gateway correctly
 * routes device requests to the appropriate backend services, applies proper authentication and
 * authorization, and maintains all existing device management functionality in the microservices
 * architecture.
 */
public class DeviceApiTest extends BaseTest {

    private DeviceResource deviceResource;

    @BeforeEach
    public void setUp() {
        super.setUp();
        deviceResource = new DeviceResource();
        
        // Use reflection to inject mocks into the resource
        try {
            java.lang.reflect.Field field = deviceResource.getClass().getDeclaredField("deviceService");
            field.setAccessible(true);
            field.set(deviceResource, deviceService);
            
            field = deviceResource.getClass().getDeclaredField("positionService");
            field.setAccessible(true);
            field.set(deviceResource, positionService);
            
            field = deviceResource.getClass().getDeclaredField("userService");
            field.setAccessible(true);
            field.set(deviceResource, userService);
            
            field = deviceResource.getClass().getDeclaredField("mediaManager");
            field.setAccessible(true);
            field.set(deviceResource, mock(org.traccar.database.MediaManager.class));
            
            field = deviceResource.getClass().getDeclaredField("tokenManager");
            field.setAccessible(true);
            field.set(deviceResource, mock(org.traccar.api.signature.TokenManager.class));
            
            field = deviceResource.getClass().getDeclaredField("config");
            field.setAccessible(true);
            field.set(deviceResource, config);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject mocks", e);
        }
    }

    @AfterEach
    public void tearDown() {
        clearAuthenticationContext();
    }

    @Test
    public void testGetDevices() throws StorageException {
        // Set up authentication context
        setAuthenticationContext(1, true);
        
        // Mock device service response
        List<Device> mockDevices = Arrays.asList(
            createDevice(1, "device1", "123456789012345"),
            createDevice(2, "device2", "987654321098765")
        );
        
        when(deviceService.getDevices(anyLong(), eq(false), eq(0L), eq(Collections.emptyList()), eq(Collections.emptyList())))
            .thenReturn(CompletableFuture.completedFuture(mockDevices));
        
        // Call the API
        List<Device> result = (List<Device>) deviceResource.get(false, 0, Collections.emptyList(), Collections.emptyList());
        
        // Verify the result
        assertEquals(2, result.size());
        assertEquals("device1", result.get(0).getName());
        assertEquals("device2", result.get(1).getName());
        
        // Verify service was called correctly
        verify(deviceService).getDevices(1L, false, 0L, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void testGetDevicesByUniqueId() throws StorageException {
        // Set up authentication context
        setAuthenticationContext(1, false);
        
        // Mock device service response
        Device mockDevice = createDevice(1, "device1", "123456789012345");
        List<String> uniqueIds = Collections.singletonList("123456789012345");
        
        when(deviceService.getDevicesByUniqueIds(eq(1L), eq(uniqueIds)))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(mockDevice)));
        
        // Call the API
        List<Device> result = (List<Device>) deviceResource.get(false, 0, uniqueIds, Collections.emptyList());
        
        // Verify the result
        assertEquals(1, result.size());
        assertEquals("device1", result.get(0).getName());
        assertEquals("123456789012345", result.get(0).getUniqueId());
        
        // Verify service was called correctly
        verify(deviceService).getDevicesByUniqueIds(1L, uniqueIds);
    }

    @Test
    public void testGetDevicesById() throws StorageException {
        // Set up authentication context
        setAuthenticationContext(1, false);
        
        // Mock device service response
        Device mockDevice = createDevice(1, "device1", "123456789012345");
        List<Long> deviceIds = Collections.singletonList(1L);
        
        when(deviceService.getDevicesByIds(eq(1L), eq(deviceIds)))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(mockDevice)));
        
        // Call the API
        List<Device> result = (List<Device>) deviceResource.get(false, 0, Collections.emptyList(), deviceIds);
        
        // Verify the result
        assertEquals(1, result.size());
        assertEquals("device1", result.get(0).getName());
        assertEquals(1L, result.get(0).getId());
        
        // Verify service was called correctly
        verify(deviceService).getDevicesByIds(1L, deviceIds);
    }

    @Test
    public void testGetAllDevicesAsAdmin() throws StorageException {
        // Set up authentication context as admin
        setAuthenticationContext(1, true);
        
        // Mock device service response
        List<Device> mockDevices = Arrays.asList(
            createDevice(1, "device1", "123456789012345"),
            createDevice(2, "device2", "987654321098765"),
            createDevice(3, "device3", "555555555555555")
        );
        
        when(deviceService.getAllDevices())
            .thenReturn(CompletableFuture.completedFuture(mockDevices));
        
        // Call the API
        List<Device> result = (List<Device>) deviceResource.get(true, 0, Collections.emptyList(), Collections.emptyList());
        
        // Verify the result
        assertEquals(3, result.size());
        
        // Verify service was called correctly
        verify(deviceService).getAllDevices();
    }

    @Test
    public void testGetDevicesForUser() throws StorageException {
        // Set up authentication context as admin
        setAuthenticationContext(1, true);
        
        // Mock device service response
        List<Device> mockDevices = Arrays.asList(
            createDevice(1, "device1", "123456789012345"),
            createDevice(2, "device2", "987654321098765")
        );
        
        when(deviceService.getDevicesForUser(eq(2L)))
            .thenReturn(CompletableFuture.completedFuture(mockDevices));
        
        // Call the API
        List<Device> result = (List<Device>) deviceResource.get(false, 2, Collections.emptyList(), Collections.emptyList());
        
        // Verify the result
        assertEquals(2, result.size());
        
        // Verify service was called correctly
        verify(deviceService).getDevicesForUser(2L);
    }

    @Test
    public void testGetDevicesForUserUnauthorized() throws StorageException {
        // Set up authentication context as regular user
        setAuthenticationContext(1, false);
        
        // Mock permission check to throw exception
        doThrow(new SecurityException("Not authorized"))
            .when(userService).checkUser(eq(1L), eq(2L));
        
        // Call the API and expect exception
        assertThrows(SecurityException.class, () -> {
            deviceResource.get(false, 2, Collections.emptyList(), Collections.emptyList());
        });
        
        // Verify service was called correctly
        verify(userService).checkUser(1L, 2L);
    }

    @Test
    public void testUpdateAccumulators() throws Exception {
        // Set up authentication context
        setAuthenticationContext(1, false);
        
        // Create test data
        DeviceAccumulators accumulators = new DeviceAccumulators();
        accumulators.setDeviceId(1);
        accumulators.setTotalDistance(15000.0);
        accumulators.setHours(10000L);
        
        // Mock position service response
        Position position = new Position();
        position.setId(100);
        position.setDeviceId(1);
        position.setLatitude(10.0);
        position.setLongitude(20.0);
        position.setFixTime(new Date());
        position.setValid(true);
        
        when(positionService.getLatestPosition(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(position));
        
        when(positionService.updatePosition(any(Position.class)))
            .thenReturn(CompletableFuture.completedFuture(position));
        
        when(deviceService.updateDevicePosition(eq(1L), eq(100L)))
            .thenReturn(CompletableFuture.completedFuture(true));
        
        // Call the API
        Response response = deviceResource.updateAccumulators(accumulators);
        
        // Verify the response
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
        
        // Verify service calls
        verify(positionService).getLatestPosition(1L);
        
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        verify(positionService).updatePosition(positionCaptor.capture());
        
        Position updatedPosition = positionCaptor.getValue();
        assertEquals(15000.0, updatedPosition.getAttributes().get(Position.KEY_TOTAL_DISTANCE));
        assertEquals(10000L, updatedPosition.getAttributes().get(Position.KEY_HOURS));
        
        verify(deviceService).updateDevicePosition(1L, 100L);
    }

    @Test
    public void testUpdateAccumulatorsNoPosition() throws Exception {
        // Set up authentication context
        setAuthenticationContext(1, false);
        
        // Create test data
        DeviceAccumulators accumulators = new DeviceAccumulators();
        accumulators.setDeviceId(1);
        accumulators.setTotalDistance(15000.0);
        
        // Mock position service to return null (no position)
        when(positionService.getLatestPosition(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(null));
        
        // Call the API and expect exception
        assertThrows(IllegalArgumentException.class, () -> {
            deviceResource.updateAccumulators(accumulators);
        });
        
        // Verify service was called
        verify(positionService).getLatestPosition(1L);
    }

    @Test
    public void testUpdateAccumulatorsUnauthorized() throws Exception {
        // Set up authentication context
        setAuthenticationContext(1, false);
        
        // Create test data
        DeviceAccumulators accumulators = new DeviceAccumulators();
        accumulators.setDeviceId(1);
        accumulators.setTotalDistance(15000.0);
        
        // Mock permission check to throw exception
        doThrow(new SecurityException("Not authorized"))
            .when(deviceService).checkDevicePermission(eq(1L), eq(1L));
        
        // Call the API and expect exception
        assertThrows(SecurityException.class, () -> {
            deviceResource.updateAccumulators(accumulators);
        });
        
        // Verify service was called
        verify(deviceService).checkDevicePermission(1L, 1L);
    }

    @Test
    public void testUploadDeviceImage() throws Exception {
        // Set up authentication context
        setAuthenticationContext(1, true);
        
        // Mock device service response
        Device device = createDevice(1, "device1", "123456789012345");
        when(deviceService.getDevice(eq(1L), eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Mock media manager
        org.traccar.database.MediaManager mediaManager = mock(org.traccar.database.MediaManager.class);
        java.io.OutputStream outputStream = mock(java.io.OutputStream.class);
        when(mediaManager.createFileStream(eq("123456789012345"), eq("device"), eq("jpg")))
            .thenReturn(outputStream);
        
        // Use reflection to set the media manager
        java.lang.reflect.Field field = deviceResource.getClass().getDeclaredField("mediaManager");
        field.setAccessible(true);
        field.set(deviceResource, mediaManager);
        
        // Create a temporary file for testing
        File tempFile = File.createTempFile("test", ".jpg");
        tempFile.deleteOnExit();
        
        // Call the API
        Response response = deviceResource.uploadImage(1, tempFile, "image/jpeg");
        
        // Verify the response
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        assertEquals("device.jpg", response.getEntity());
        
        // Verify service calls
        verify(deviceService).getDevice(1L, 1L);
        verify(mediaManager).createFileStream("123456789012345", "device", "jpg");
    }

    @Test
    public void testUploadDeviceImageDeviceNotFound() throws Exception {
        // Set up authentication context
        setAuthenticationContext(1, false);
        
        // Mock device service to return null (device not found)
        when(deviceService.getDevice(eq(1L), eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(null));
        
        // Create a temporary file for testing
        File tempFile = File.createTempFile("test", ".jpg");
        tempFile.deleteOnExit();
        
        // Call the API
        Response response = deviceResource.uploadImage(1, tempFile, "image/jpeg");
        
        // Verify the response
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
        
        // Verify service call
        verify(deviceService).getDevice(1L, 1L);
    }

    @Test
    public void testUploadDeviceImageUnsupportedType() throws Exception {
        // Set up authentication context
        setAuthenticationContext(1, true);
        
        // Mock device service response
        Device device = createDevice(1, "device1", "123456789012345");
        when(deviceService.getDevice(eq(1L), eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Create a temporary file for testing
        File tempFile = File.createTempFile("test", ".txt");
        tempFile.deleteOnExit();
        
        // Call the API and expect exception
        assertThrows(IllegalArgumentException.class, () -> {
            deviceResource.uploadImage(1, tempFile, "text/plain");
        });
        
        // Verify service call
        verify(deviceService).getDevice(1L, 1L);
    }

    @Test
    public void testShareDevice() throws Exception {
        // Set up authentication context
        setAuthenticationContext(1, false);
        
        // Mock config
        when(config.getBoolean(Keys.DEVICE_SHARE_DISABLE.getKey()))
            .thenReturn(false);
        
        // Mock user service
        User user = new User();
        user.setId(1);
        user.setEmail("test@example.com");
        user.setTemporary(false);
        
        when(userService.getUser(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(user));
        
        // Mock device service
        Device device = createDevice(1, "device1", "123456789012345");
        when(deviceService.getDevice(eq(1L), eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Mock user service for shared user
        when(userService.getUserByEmail(eq("test@example.com:123456789012345")))
            .thenReturn(CompletableFuture.completedFuture(null));
        
        User sharedUser = new User();
        sharedUser.setId(2);
        sharedUser.setEmail("test@example.com:123456789012345");
        
        when(userService.addUser(any(User.class)))
            .thenReturn(CompletableFuture.completedFuture(2L));
        
        // Mock token manager
        org.traccar.api.signature.TokenManager tokenManager = mock(org.traccar.api.signature.TokenManager.class);
        when(tokenManager.generateToken(eq(2L), any(Date.class)))
            .thenReturn("test-token-123");
        
        // Use reflection to set the token manager
        java.lang.reflect.Field field = deviceResource.getClass().getDeclaredField("tokenManager");
        field.setAccessible(true);
        field.set(deviceResource, tokenManager);
        
        // Call the API
        Date expiration = new Date(System.currentTimeMillis() + 86400000); // 1 day
        String token = deviceResource.shareDevice(1, expiration);
        
        // Verify the result
        assertEquals("test-token-123", token);
        
        // Verify service calls
        verify(userService).getUser(1L);
        verify(deviceService).getDevice(1L, 1L);
        verify(userService).getUserByEmail("test@example.com:123456789012345");
        
        ArgumentCaptor<User> userCaptor = ArgumentCaptor.forClass(User.class);
        verify(userService).addUser(userCaptor.capture());
        
        User capturedUser = userCaptor.getValue();
        assertEquals("test@example.com:123456789012345", capturedUser.getEmail());
        assertEquals(true, capturedUser.getTemporary());
        assertEquals(true, capturedUser.getReadonly());
        
        verify(deviceService).addDevicePermission(2L, 1L);
        verify(tokenManager).generateToken(eq(2L), any(Date.class));
    }

    @Test
    public void testShareDeviceExistingShare() throws Exception {
        // Set up authentication context
        setAuthenticationContext(1, false);
        
        // Mock config
        when(config.getBoolean(Keys.DEVICE_SHARE_DISABLE.getKey()))
            .thenReturn(false);
        
        // Mock user service
        User user = new User();
        user.setId(1);
        user.setEmail("test@example.com");
        user.setTemporary(false);
        
        when(userService.getUser(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(user));
        
        // Mock device service
        Device device = createDevice(1, "device1", "123456789012345");
        when(deviceService.getDevice(eq(1L), eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Mock user service for existing shared user
        User sharedUser = new User();
        sharedUser.setId(2);
        sharedUser.setEmail("test@example.com:123456789012345");
        
        when(userService.getUserByEmail(eq("test@example.com:123456789012345")))
            .thenReturn(CompletableFuture.completedFuture(sharedUser));
        
        // Mock token manager
        org.traccar.api.signature.TokenManager tokenManager = mock(org.traccar.api.signature.TokenManager.class);
        when(tokenManager.generateToken(eq(2L), any(Date.class)))
            .thenReturn("test-token-123");
        
        // Use reflection to set the token manager
        java.lang.reflect.Field field = deviceResource.getClass().getDeclaredField("tokenManager");
        field.setAccessible(true);
        field.set(deviceResource, tokenManager);
        
        // Call the API
        Date expiration = new Date(System.currentTimeMillis() + 86400000); // 1 day
        String token = deviceResource.shareDevice(1, expiration);
        
        // Verify the result
        assertEquals("test-token-123", token);
        
        // Verify service calls
        verify(userService).getUser(1L);
        verify(deviceService).getDevice(1L, 1L);
        verify(userService).getUserByEmail("test@example.com:123456789012345");
        
        // Verify we didn't try to create a new user
        verify(userService, times(0)).addUser(any(User.class));
        
        // Verify token generation
        verify(tokenManager).generateToken(eq(2L), any(Date.class));
    }

    @Test
    public void testShareDeviceDisabled() throws Exception {
        // Set up authentication context
        setAuthenticationContext(1, false);
        
        // Mock config to disable sharing
        when(config.getBoolean(Keys.DEVICE_SHARE_DISABLE.getKey()))
            .thenReturn(true);
        
        // Mock user service
        User user = new User();
        user.setId(1);
        user.setEmail("test@example.com");
        user.setTemporary(false);
        
        when(userService.getUser(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(user));
        
        // Call the API and expect exception
        Date expiration = new Date(System.currentTimeMillis() + 86400000); // 1 day
        assertThrows(SecurityException.class, () -> {
            deviceResource.shareDevice(1, expiration);
        });
        
        // Verify service call
        verify(userService).getUser(1L);
    }

    @Test
    public void testShareDeviceTemporaryUser() throws Exception {
        // Set up authentication context
        setAuthenticationContext(1, false);
        
        // Mock config
        when(config.getBoolean(Keys.DEVICE_SHARE_DISABLE.getKey()))
            .thenReturn(false);
        
        // Mock user service with temporary user
        User user = new User();
        user.setId(1);
        user.setEmail("test@example.com");
        user.setTemporary(true);
        
        when(userService.getUser(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(user));
        
        // Call the API and expect exception
        Date expiration = new Date(System.currentTimeMillis() + 86400000); // 1 day
        assertThrows(SecurityException.class, () -> {
            deviceResource.shareDevice(1, expiration);
        });
        
        // Verify service call
        verify(userService).getUser(1L);
    }

    /**
     * Helper method to create a test device
     */
    private Device createDevice(long id, String name, String uniqueId) {
        Device device = new Device();
        device.setId(id);
        device.setName(name);
        device.setUniqueId(uniqueId);
        device.setStatus(Device.STATUS_ONLINE);
        device.setLastUpdate(new Date());
        return device;
    }
}