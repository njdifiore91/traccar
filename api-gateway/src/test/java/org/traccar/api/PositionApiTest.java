package org.traccar.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.MockedStatic;
import org.traccar.api.resource.PositionResource;
import org.traccar.helper.model.PositionUtil;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.model.UserRestrictions;
import org.traccar.reports.CsvExportProvider;
import org.traccar.reports.GpxExportProvider;
import org.traccar.reports.KmlExportProvider;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mockStatic;

/**
 * Tests the position-related API endpoints in the API Gateway.
 * 
 * This test class verifies that the API Gateway correctly routes position requests to the Position Service,
 * applies proper authentication and authorization, and handles various query parameters and export formats
 * (JSON, KML, CSV, GPX).
 */
public class PositionApiTest {

    @Mock
    private Storage storage;

    @Mock
    private PermissionsService permissionsService;

    @Mock
    private KmlExportProvider kmlExportProvider;

    @Mock
    private CsvExportProvider csvExportProvider;

    @Mock
    private GpxExportProvider gpxExportProvider;
    
    private MockedStatic<PositionUtil> positionUtilMock;

    private PositionResource positionResource;

    /**
     * Sets up the test environment before each test.
     * 
     * Initializes mock objects and injects them into the PositionResource instance.
     * Also configures common mock behaviors needed across tests.
     */
    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        positionResource = new PositionResource();
        
        // Use reflection to set the mocked dependencies
        try {
            java.lang.reflect.Field storageField = BaseResource.class.getDeclaredField("storage");
            storageField.setAccessible(true);
            storageField.set(positionResource, storage);

            java.lang.reflect.Field permissionsField = BaseResource.class.getDeclaredField("permissionsService");
            permissionsField.setAccessible(true);
            permissionsField.set(positionResource, permissionsService);

            java.lang.reflect.Field kmlField = PositionResource.class.getDeclaredField("kmlExportProvider");
            kmlField.setAccessible(true);
            kmlField.set(positionResource, kmlExportProvider);

            java.lang.reflect.Field csvField = PositionResource.class.getDeclaredField("csvExportProvider");
            csvField.setAccessible(true);
            csvField.set(positionResource, csvExportProvider);

            java.lang.reflect.Field gpxField = PositionResource.class.getDeclaredField("gpxExportProvider");
            gpxField.setAccessible(true);
            gpxField.set(positionResource, gpxExportProvider);
        } catch (Exception e) {
            fail("Failed to set up test dependencies: " + e.getMessage());
        }

        // Mock getUserId method to return a test user ID
        doReturn(1L).when(permissionsService).getUserId();
        
        // Mock PositionUtil for static method calls
        positionUtilMock = mockStatic(PositionUtil.class);
    }

    /**
     * Tests retrieving positions by their IDs.
     * 
     * Verifies that the API Gateway correctly retrieves positions by ID list
     * and checks permissions for each position's device.
     */
    @Test
    public void testGetPositionsById() throws StorageException {
        // Setup
        List<Long> positionIds = Arrays.asList(1L, 2L);
        Position position1 = new Position();
        position1.setId(1L);
        position1.setDeviceId(100L);
        
        Position position2 = new Position();
        position2.setId(2L);
        position2.setDeviceId(101L);

        when(storage.getObject(eq(Position.class), any(Request.class)))
                .thenReturn(position1)
                .thenReturn(position2);

        // Execute
        Collection<Position> result = positionResource.getJson(0, positionIds, null, null);

        // Verify
        assertEquals(2, result.size());
        verify(permissionsService, times(1)).checkPermission(Device.class, 1L, 100L);
        verify(permissionsService, times(1)).checkPermission(Device.class, 1L, 101L);
        verify(storage, times(2)).getObject(eq(Position.class), any(Request.class));
    }

    /**
     * Tests retrieving positions by device ID.
     * 
     * Verifies that the API Gateway correctly retrieves positions for a specific device
     * and checks permissions for the device access.
     */
    @Test
    public void testGetPositionsByDeviceId() throws StorageException {
        // Setup
        long deviceId = 100L;
        List<Position> positions = new ArrayList<>();
        positions.add(new Position());
        positions.add(new Position());

        when(storage.getObjects(eq(Position.class), any(Request.class))).thenReturn(positions);

        // Execute
        Collection<Position> result = positionResource.getJson(deviceId, List.of(), null, null);

        // Verify
        assertEquals(2, result.size());
        verify(permissionsService, times(1)).checkPermission(Device.class, 1L, deviceId);
        verify(storage, times(1)).getObjects(eq(Position.class), any(Request.class));
    }

    /**
     * Tests retrieving positions by device ID with date range filtering.
     * 
     * Verifies that the API Gateway correctly retrieves positions for a specific device
     * within a given date range and checks both device permissions and report restrictions.
     */
    @Test
    public void testGetPositionsByDeviceIdWithDateRange() throws StorageException {
        // Setup
        long deviceId = 100L;
        Date from = new Date(System.currentTimeMillis() - 86400000); // 24 hours ago
        Date to = new Date();
        List<Position> positions = new ArrayList<>();
        positions.add(new Position());
        positions.add(new Position());

        // Mock the static PositionUtil method
        when(PositionUtil.getPositions(eq(storage), eq(deviceId), eq(from), eq(to)))
                .thenReturn(positions);

        // Execute
        Collection<Position> result = positionResource.getJson(deviceId, List.of(), from, to);

        // Verify
        assertEquals(2, result.size());
        verify(permissionsService, times(1)).checkPermission(Device.class, 1L, deviceId);
        verify(permissionsService, times(1)).checkRestriction(eq(1L), any());
    }

    /**
     * Tests retrieving latest positions for all accessible devices.
     * 
     * Verifies that the API Gateway correctly retrieves the latest positions
     * for all devices the user has access to.
     */
    @Test
    public void testGetLatestPositions() throws StorageException {
        // Setup
        List<Position> positions = new ArrayList<>();
        positions.add(new Position());
        positions.add(new Position());

        // Mock the static PositionUtil method
        when(PositionUtil.getLatestPositions(eq(storage), eq(1L)))
                .thenReturn(positions);

        // Execute
        Collection<Position> result = positionResource.getJson(0, List.of(), null, null);

        // Verify
        assertEquals(2, result.size());
        verify(positionUtilMock, times(1)).getLatestPositions(eq(storage), eq(1L));
    }

    /**
     * Tests deleting a position by its ID.
     * 
     * Verifies that the API Gateway correctly deletes a position by ID,
     * checks both readonly restriction and device permission before deletion.
     */
    @Test
    public void testRemovePositionById() throws StorageException {
        // Setup
        long positionId = 1L;
        Position position = new Position();
        position.setId(positionId);
        position.setDeviceId(100L);

        when(storage.getObject(eq(Position.class), any(Request.class))).thenReturn(position);

        // Execute
        Response response = positionResource.removeById(positionId);

        // Verify
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
        verify(permissionsService, times(1)).checkRestriction(eq(1L), any(UserRestrictions.class));
        verify(permissionsService, times(1)).checkPermission(Device.class, 1L, 100L);
        verify(storage, times(1)).removeObject(eq(Position.class), any(Request.class));
    }

    /**
     * Tests attempting to delete a non-existent position.
     * 
     * Verifies that the API Gateway returns a NOT_FOUND response when
     * attempting to delete a position that doesn't exist.
     */
    @Test
    public void testRemovePositionByIdNotFound() throws StorageException {
        // Setup
        long positionId = 1L;
        when(storage.getObject(eq(Position.class), any(Request.class))).thenReturn(null);

        // Execute
        Response response = positionResource.removeById(positionId);

        // Verify
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
        verify(permissionsService, times(1)).checkRestriction(eq(1L), any(UserRestrictions.class));
        verify(storage, never()).removeObject(eq(Position.class), any(Request.class));
    }

    /**
     * Tests deleting positions by device ID and date range.
     * 
     * Verifies that the API Gateway correctly deletes positions for a specific device
     * within a given date range, checking both device permission and readonly restriction.
     */
    @Test
    public void testRemovePositionsByDeviceIdAndDateRange() throws StorageException {
        // Setup
        long deviceId = 100L;
        Date from = new Date(System.currentTimeMillis() - 86400000); // 24 hours ago
        Date to = new Date();

        // Execute
        Response response = positionResource.remove(deviceId, from, to);

        // Verify
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
        verify(permissionsService, times(1)).checkPermission(Device.class, 1L, deviceId);
        verify(permissionsService, times(1)).checkRestriction(eq(1L), any(UserRestrictions.class));
        verify(storage, times(1)).removeObject(eq(Position.class), any(Request.class));
    }

    /**
     * Tests exporting positions to KML format.
     * 
     * Verifies that the API Gateway correctly exports positions for a specific device
     * to KML format, checking device permission and generating the expected content.
     */
    @Test
    public void testGetKml() throws StorageException, IOException {
        // Setup
        long deviceId = 100L;
        Date from = new Date(System.currentTimeMillis() - 86400000); // 24 hours ago
        Date to = new Date();

        doAnswer(invocation -> {
            OutputStream output = invocation.getArgument(0);
            output.write("KML test data".getBytes());
            return null;
        }).when(kmlExportProvider).generate(any(OutputStream.class), eq(deviceId), eq(from), eq(to));

        // Execute
        Response response = positionResource.getKml(deviceId, from, to);

        // Verify
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(permissionsService, times(1)).checkPermission(Device.class, 1L, deviceId);
        
        // Verify the response contains the expected data
        StreamingOutput streamingOutput = (StreamingOutput) response.getEntity();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        streamingOutput.write(outputStream);
        assertEquals("KML test data", outputStream.toString());
        
        // Verify content disposition header
        assertTrue(response.getHeaderString("Content-Disposition").contains("attachment; filename=positions.kml"));
    }

    /**
     * Tests exporting positions to CSV format.
     * 
     * Verifies that the API Gateway correctly exports positions for a specific device
     * to CSV format, checking device permission and generating the expected content.
     */
    @Test
    public void testGetCsv() throws StorageException, IOException {
        // Setup
        long deviceId = 100L;
        Date from = new Date(System.currentTimeMillis() - 86400000); // 24 hours ago
        Date to = new Date();

        doAnswer(invocation -> {
            OutputStream output = invocation.getArgument(0);
            output.write("CSV test data".getBytes());
            return null;
        }).when(csvExportProvider).generate(any(OutputStream.class), eq(deviceId), eq(from), eq(to));

        // Execute
        Response response = positionResource.getCsv(deviceId, from, to);

        // Verify
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(permissionsService, times(1)).checkPermission(Device.class, 1L, deviceId);
        
        // Verify the response contains the expected data
        StreamingOutput streamingOutput = (StreamingOutput) response.getEntity();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        streamingOutput.write(outputStream);
        assertEquals("CSV test data", outputStream.toString());
        
        // Verify content disposition header
        assertTrue(response.getHeaderString("Content-Disposition").contains("attachment; filename=positions.csv"));
    }

    /**
     * Tests exporting positions to GPX format.
     * 
     * Verifies that the API Gateway correctly exports positions for a specific device
     * to GPX format, checking device permission and generating the expected content.
     */
    @Test
    public void testGetGpx() throws StorageException, IOException {
        // Setup
        long deviceId = 100L;
        Date from = new Date(System.currentTimeMillis() - 86400000); // 24 hours ago
        Date to = new Date();

        doAnswer(invocation -> {
            OutputStream output = invocation.getArgument(0);
            output.write("GPX test data".getBytes());
            return null;
        }).when(gpxExportProvider).generate(any(OutputStream.class), eq(deviceId), eq(from), eq(to));

        // Execute
        Response response = positionResource.getGpx(deviceId, from, to);

        // Verify
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(permissionsService, times(1)).checkPermission(Device.class, 1L, deviceId);
        
        // Verify the response contains the expected data
        StreamingOutput streamingOutput = (StreamingOutput) response.getEntity();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        streamingOutput.write(outputStream);
        assertEquals("GPX test data", outputStream.toString());
        
        // Verify content disposition header
        assertTrue(response.getHeaderString("Content-Disposition").contains("attachment; filename=positions.gpx"));
    }

    /**
     * Tests handling of exceptions during export generation.
     * 
     * Verifies that the API Gateway correctly wraps StorageException in WebApplicationException
     * when an error occurs during export generation.
     */
    @Test
    public void testExportProviderException() throws StorageException {
        // Setup
        long deviceId = 100L;
        Date from = new Date(System.currentTimeMillis() - 86400000); // 24 hours ago
        Date to = new Date();

        doThrow(new StorageException("Test exception")).when(kmlExportProvider)
                .generate(any(OutputStream.class), eq(deviceId), eq(from), eq(to));

        // Execute and verify exception is wrapped
        Response response = positionResource.getKml(deviceId, from, to);
        StreamingOutput streamingOutput = (StreamingOutput) response.getEntity();
        
        WebApplicationException exception = assertThrows(WebApplicationException.class, () -> {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            streamingOutput.write(outputStream);
        });
        
        // Verify the cause of the exception
        assertTrue(exception.getCause() instanceof StorageException);
        assertEquals("Test exception", exception.getCause().getMessage());
    }

    /**
     * Tests unauthorized access to positions.
     * 
     * Verifies that the API Gateway correctly prevents access to positions
     * when the user doesn't have permission for the device.
     */
    @Test
    public void testUnauthorizedAccess() throws StorageException {
        // Setup
        long deviceId = 100L;
        doThrow(new SecurityException("Unauthorized access")).when(permissionsService)
                .checkPermission(Device.class, 1L, deviceId);

        // Execute and verify
        SecurityException exception = assertThrows(SecurityException.class, () -> {
            positionResource.getJson(deviceId, List.of(), null, null);
        });
        
        assertEquals("Unauthorized access", exception.getMessage());
        verify(permissionsService, times(1)).checkPermission(Device.class, 1L, deviceId);
        verify(storage, never()).getObjects(any(), any());
    }

    /**
     * Tests readonly restriction for position deletion.
     * 
     * Verifies that the API Gateway correctly prevents deletion of positions
     * when the user has readonly restriction.
     */
    @Test
    public void testReadonlyRestriction() throws StorageException {
        // Setup
        long positionId = 1L;
        doThrow(new SecurityException("Readonly user")).when(permissionsService)
                .checkRestriction(eq(1L), any(UserRestrictions.class));

        // Execute and verify
        SecurityException exception = assertThrows(SecurityException.class, () -> {
            positionResource.removeById(positionId);
        });
        
        assertEquals("Readonly user", exception.getMessage());
        verify(permissionsService, times(1)).checkRestriction(eq(1L), any(UserRestrictions.class));
        verify(storage, never()).removeObject(any(), any());
    }
    
    /**
     * Tests the API Gateway's handling of multiple query parameters.
     * 
     * Verifies that the API Gateway correctly processes requests with multiple
     * query parameters for position filtering.
     */
    @Test
    public void testMultipleQueryParameters() throws StorageException {
        // Setup
        long deviceId = 100L;
        List<Long> positionIds = Arrays.asList(1L, 2L);
        Date from = new Date(System.currentTimeMillis() - 86400000); // 24 hours ago
        Date to = new Date();
        
        // This test verifies that the API Gateway correctly handles the case where
        // multiple query parameters are provided but positionIds takes precedence
        Position position1 = new Position();
        position1.setId(1L);
        position1.setDeviceId(100L);
        
        Position position2 = new Position();
        position2.setId(2L);
        position2.setDeviceId(101L);

        when(storage.getObject(eq(Position.class), any(Request.class)))
                .thenReturn(position1)
                .thenReturn(position2);

        // Execute
        Collection<Position> result = positionResource.getJson(deviceId, positionIds, from, to);

        // Verify
        assertEquals(2, result.size());
        verify(permissionsService, times(1)).checkPermission(Device.class, 1L, 100L);
        verify(permissionsService, times(1)).checkPermission(Device.class, 1L, 101L);
        verify(storage, times(2)).getObject(eq(Position.class), any(Request.class));
        
        // Verify that the date range and deviceId parameters were ignored since positionIds were provided
        verify(positionUtilMock, never()).getPositions(any(), anyLong(), any(), any());
    }
    
    /**
     * Cleans up resources after each test.
     * 
     * Closes the mocked static PositionUtil to prevent memory leaks.
     */
    @AfterEach
    public void tearDown() {
        if (positionUtilMock != null) {
            positionUtilMock.close();
        }
    }
}