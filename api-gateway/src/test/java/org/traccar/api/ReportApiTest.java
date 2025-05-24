package org.traccar.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.traccar.BaseTest;
import org.traccar.api.resource.ReportResource;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.model.Report;
import org.traccar.model.UserRestrictions;
import org.traccar.reports.model.StopReportItem;
import org.traccar.reports.model.SummaryReportItem;
import org.traccar.reports.model.TripReportItem;
import org.traccar.service.PermissionsService;
import org.traccar.service.ReportService;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests the report-related API endpoints in the API Gateway, focusing on report generation,
 * formatting, and delivery. Verifies that the API Gateway correctly routes report requests
 * to the Reporting Service, applies proper authentication and authorization, and supports
 * all existing report types (route, events, summary, trips, stops) and formats (JSON, Excel)
 * in the microservices architecture.
 */
@ExtendWith({MockitoExtension.class, SpringExtension.class})
public class ReportApiTest extends BaseTest {

    @Mock
    private PermissionsService permissionsService;

    @InjectMocks
    private ReportResource reportResource;

    @Mock
    private HttpServletRequest request;

    private final long userId = 1L;
    private final Date from = new Date(System.currentTimeMillis() - 86400000); // 24 hours ago
    private final Date to = new Date();
    private final List<Long> deviceIds = Arrays.asList(1L, 2L);
    private final List<Long> groupIds = Arrays.asList(1L);

    @BeforeEach
    public void setup() {
        setAuthenticationContext(userId, false);
        when(reportService.isServiceAvailable()).thenReturn(true);
    }

    /**
     * Tests that the API Gateway correctly routes route report requests in JSON format
     * to the Reporting Service and returns the results.
     */
    @Test
    public void testGetRouteReport() throws Exception {
        // Prepare test data
        List<Position> positions = new ArrayList<>();
        Position position1 = new Position();
        position1.setId(1L);
        position1.setDeviceId(1L);
        position1.setLatitude(51.5074);
        position1.setLongitude(-0.1278);
        position1.setTime(new Date());
        positions.add(position1);

        // Mock the report service response
        when(reportService.getRouteReport(eq(userId), eq(deviceIds), eq(groupIds), eq(from), eq(to)))
                .thenReturn(CompletableFuture.completedFuture(positions));

        // Call the API endpoint
        List<Position> result = reportResource.getRoute(deviceIds, groupIds, from, to);

        // Verify the result
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(position1.getId(), result.get(0).getId());

        // Verify that the report service was called with correct parameters
        verify(reportService).getRouteReport(userId, deviceIds, groupIds, from, to);
        // Verify that permissions were checked
        verify(permissionsService).checkRestriction(eq(userId), any());
    }

    /**
     * Tests that the API Gateway correctly routes route report requests in Excel format
     * to the Reporting Service and returns the results as a streaming response.
     */
    @Test
    public void testGetRouteReportExcel() throws Exception {
        // Mock the report service to simulate Excel generation
        doAnswer(invocation -> {
            StreamingOutput output = invocation.getArgument(0);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            output.write(baos);
            return null;
        }).when(reportService).getRouteExcel(any(), eq(userId), eq(deviceIds), eq(groupIds), eq(from), eq(to));

        // Call the API endpoint
        Response response = reportResource.getRouteExcel(deviceIds, groupIds, from, to, false);

        // Verify the response
        assertNotNull(response);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        assertNotNull(response.getEntity());
        assertTrue(response.getEntity() instanceof StreamingOutput);

        // Verify that the report service was called with correct parameters
        verify(reportService).getRouteExcel(any(StreamingOutput.class), eq(userId), eq(deviceIds), eq(groupIds), eq(from), eq(to));
        // Verify that permissions were checked
        verify(permissionsService).checkRestriction(eq(userId), any());
    }

    /**
     * Tests that the API Gateway correctly routes route report email requests
     * to the Reporting Service and returns a success response.
     */
    @Test
    public void testGetRouteReportEmail() throws Exception {
        // Mock the report service to simulate email sending
        when(reportService.sendReportByEmail(eq(userId), any()))
                .thenReturn(CompletableFuture.completedFuture(true));

        // Call the API endpoint
        Response response = reportResource.getRouteExcel(deviceIds, groupIds, from, to, true);

        // Verify the response
        assertNotNull(response);
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());

        // Verify that the report service was called
        verify(reportService).sendReportByEmail(eq(userId), any());
        // Verify that permissions were checked
        verify(permissionsService).checkRestriction(eq(userId), any());
    }

    /**
     * Tests that the API Gateway correctly routes events report requests in JSON format
     * to the Reporting Service and returns the results.
     */
    @Test
    public void testGetEventsReport() throws Exception {
        // Prepare test data
        List<Event> events = new ArrayList<>();
        Event event1 = new Event();
        event1.setId(1L);
        event1.setDeviceId(1L);
        event1.setType(Event.TYPE_DEVICE_ONLINE);
        event1.setTime(new Date());
        events.add(event1);

        List<String> types = Arrays.asList(Event.TYPE_DEVICE_ONLINE);
        List<String> alarms = new ArrayList<>();

        // Mock the report service response
        when(reportService.getEventsReport(eq(userId), eq(deviceIds), eq(groupIds), eq(types), eq(alarms), eq(from), eq(to)))
                .thenReturn(CompletableFuture.completedFuture(events));

        // Call the API endpoint
        List<Event> result = reportResource.getEvents(deviceIds, groupIds, types, alarms, from, to);

        // Verify the result
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(event1.getId(), result.get(0).getId());
        assertEquals(Event.TYPE_DEVICE_ONLINE, result.get(0).getType());

        // Verify that the report service was called with correct parameters
        verify(reportService).getEventsReport(userId, deviceIds, groupIds, types, alarms, from, to);
        // Verify that permissions were checked
        verify(permissionsService).checkRestriction(eq(userId), any());
    }

    /**
     * Tests that the API Gateway correctly routes events report requests in Excel format
     * to the Reporting Service and returns the results as a streaming response.
     */
    @Test
    public void testGetEventsReportExcel() throws Exception {
        List<String> types = Arrays.asList(Event.TYPE_DEVICE_ONLINE);
        List<String> alarms = new ArrayList<>();

        // Mock the report service to simulate Excel generation
        doAnswer(invocation -> {
            StreamingOutput output = invocation.getArgument(0);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            output.write(baos);
            return null;
        }).when(reportService).getEventsExcel(any(), eq(userId), eq(deviceIds), eq(groupIds), eq(types), eq(alarms), eq(from), eq(to));

        // Call the API endpoint
        Response response = reportResource.getEventsExcel(deviceIds, groupIds, types, alarms, from, to, false);

        // Verify the response
        assertNotNull(response);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        assertNotNull(response.getEntity());
        assertTrue(response.getEntity() instanceof StreamingOutput);

        // Verify that the report service was called with correct parameters
        verify(reportService).getEventsExcel(any(StreamingOutput.class), eq(userId), eq(deviceIds), eq(groupIds), eq(types), eq(alarms), eq(from), eq(to));
        // Verify that permissions were checked
        verify(permissionsService).checkRestriction(eq(userId), any());
    }

    /**
     * Tests that the API Gateway correctly routes summary report requests in JSON format
     * to the Reporting Service and returns the results.
     */
    @Test
    public void testGetSummaryReport() throws Exception {
        // Prepare test data
        List<SummaryReportItem> summaryItems = new ArrayList<>();
        SummaryReportItem item1 = new SummaryReportItem();
        item1.setDeviceId(1L);
        item1.setDeviceName("Test Device");
        item1.setDistance(150.5);
        item1.setSpentFuel(10.2);
        summaryItems.add(item1);

        boolean daily = false;

        // Mock the report service response
        when(reportService.getSummaryReport(eq(userId), eq(deviceIds), eq(groupIds), eq(from), eq(to), eq(daily)))
                .thenReturn(CompletableFuture.completedFuture(summaryItems));

        // Call the API endpoint
        List<SummaryReportItem> result = reportResource.getSummary(deviceIds, groupIds, from, to, daily);

        // Verify the result
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(item1.getDeviceId(), result.get(0).getDeviceId());
        assertEquals(item1.getDistance(), result.get(0).getDistance());

        // Verify that the report service was called with correct parameters
        verify(reportService).getSummaryReport(userId, deviceIds, groupIds, from, to, daily);
        // Verify that permissions were checked
        verify(permissionsService).checkRestriction(eq(userId), any());
    }

    /**
     * Tests that the API Gateway correctly routes trips report requests in JSON format
     * to the Reporting Service and returns the results.
     */
    @Test
    public void testGetTripsReport() throws Exception {
        // Prepare test data
        List<TripReportItem> tripItems = new ArrayList<>();
        TripReportItem item1 = new TripReportItem();
        item1.setDeviceId(1L);
        item1.setDeviceName("Test Device");
        item1.setDistance(150.5);
        item1.setStartTime(from);
        item1.setEndTime(to);
        tripItems.add(item1);

        // Mock the report service response
        when(reportService.getTripsReport(eq(userId), eq(deviceIds), eq(groupIds), eq(from), eq(to)))
                .thenReturn(CompletableFuture.completedFuture(tripItems));

        // Call the API endpoint
        List<TripReportItem> result = reportResource.getTrips(deviceIds, groupIds, from, to);

        // Verify the result
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(item1.getDeviceId(), result.get(0).getDeviceId());
        assertEquals(item1.getDistance(), result.get(0).getDistance());

        // Verify that the report service was called with correct parameters
        verify(reportService).getTripsReport(userId, deviceIds, groupIds, from, to);
        // Verify that permissions were checked
        verify(permissionsService).checkRestriction(eq(userId), any());
    }

    /**
     * Tests that the API Gateway correctly routes stops report requests in JSON format
     * to the Reporting Service and returns the results.
     */
    @Test
    public void testGetStopsReport() throws Exception {
        // Prepare test data
        List<StopReportItem> stopItems = new ArrayList<>();
        StopReportItem item1 = new StopReportItem();
        item1.setDeviceId(1L);
        item1.setDeviceName("Test Device");
        item1.setAddress("Test Address");
        item1.setStartTime(from);
        item1.setEndTime(to);
        item1.setDuration(3600L); // 1 hour in seconds
        stopItems.add(item1);

        // Mock the report service response
        when(reportService.getStopsReport(eq(userId), eq(deviceIds), eq(groupIds), eq(from), eq(to)))
                .thenReturn(CompletableFuture.completedFuture(stopItems));

        // Call the API endpoint
        List<StopReportItem> result = reportResource.getStops(deviceIds, groupIds, from, to);

        // Verify the result
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(item1.getDeviceId(), result.get(0).getDeviceId());
        assertEquals(item1.getDuration(), result.get(0).getDuration());

        // Verify that the report service was called with correct parameters
        verify(reportService).getStopsReport(userId, deviceIds, groupIds, from, to);
        // Verify that permissions were checked
        verify(permissionsService).checkRestriction(eq(userId), any());
    }

    /**
     * Tests that the API Gateway correctly enforces authorization restrictions
     * for report access.
     */
    @Test
    public void testReportAccessRestriction() throws Exception {
        // Mock the permissions service to throw an exception for restricted users
        doThrow(new SecurityException("Reports disabled for this user"))
                .when(permissionsService).checkRestriction(eq(userId), any());

        // Attempt to call the API endpoint
        Exception exception = assertThrows(SecurityException.class, () -> {
            reportResource.getRoute(deviceIds, groupIds, from, to);
        });

        // Verify the exception message
        assertEquals("Reports disabled for this user", exception.getMessage());

        // Verify that permissions were checked
        verify(permissionsService).checkRestriction(eq(userId), any());
        // Verify that the report service was not called
        verify(reportService, never()).getRouteReport(anyLong(), anyList(), anyList(), any(), any());
    }

    /**
     * Tests that the API Gateway correctly handles the case when the Reporting Service
     * is unavailable.
     */
    @Test
    public void testReportServiceUnavailable() throws Exception {
        // Mock the report service to be unavailable
        when(reportService.isServiceAvailable()).thenReturn(false);

        // Attempt to call the API endpoint
        Exception exception = assertThrows(RuntimeException.class, () -> {
            reportResource.getRoute(deviceIds, groupIds, from, to);
        });

        // Verify the exception message contains service unavailable information
        assertTrue(exception.getMessage().contains("unavailable"));

        // Verify that permissions were checked
        verify(permissionsService).checkRestriction(eq(userId), any());
        // Verify that the report service availability was checked
        verify(reportService).isServiceAvailable();
        // Verify that the report method was not called
        verify(reportService, never()).getRouteReport(anyLong(), anyList(), anyList(), any(), any());
    }

    /**
     * Tests that the API Gateway correctly handles the case when the Reporting Service
     * returns an error.
     */
    @Test
    public void testReportServiceError() throws Exception {
        // Mock the report service to throw an exception
        when(reportService.getRouteReport(eq(userId), eq(deviceIds), eq(groupIds), eq(from), eq(to)))
                .thenReturn(CompletableFuture.failedFuture(new IOException("Database error")));

        // Attempt to call the API endpoint
        Exception exception = assertThrows(RuntimeException.class, () -> {
            reportResource.getRoute(deviceIds, groupIds, from, to);
        });

        // Verify the exception message
        assertTrue(exception.getMessage().contains("Database error"));

        // Verify that permissions were checked
        verify(permissionsService).checkRestriction(eq(userId), any());
        // Verify that the report service was called
        verify(reportService).getRouteReport(userId, deviceIds, groupIds, from, to);
    }

    /**
     * Tests that the API Gateway correctly handles the alternate path-based syntax
     * for Excel report generation.
     */
    @Test
    public void testPathBasedExcelReport() throws Exception {
        // Mock the report service to simulate Excel generation
        doAnswer(invocation -> {
            StreamingOutput output = invocation.getArgument(0);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            output.write(baos);
            return null;
        }).when(reportService).getRouteExcel(any(), eq(userId), eq(deviceIds), eq(groupIds), eq(from), eq(to));

        // Call the API endpoint with path-based syntax
        Response response = reportResource.getRouteExcel(deviceIds, groupIds, from, to, "xlsx");

        // Verify the response
        assertNotNull(response);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        assertNotNull(response.getEntity());
        assertTrue(response.getEntity() instanceof StreamingOutput);

        // Verify that the report service was called with correct parameters
        verify(reportService).getRouteExcel(any(StreamingOutput.class), eq(userId), eq(deviceIds), eq(groupIds), eq(from), eq(to));
    }

    /**
     * Tests that the API Gateway correctly handles the alternate path-based syntax
     * for email report delivery.
     */
    @Test
    public void testPathBasedEmailReport() throws Exception {
        // Mock the report service to simulate email sending
        when(reportService.sendReportByEmail(eq(userId), any()))
                .thenReturn(CompletableFuture.completedFuture(true));

        // Call the API endpoint with path-based syntax
        Response response = reportResource.getRouteExcel(deviceIds, groupIds, from, to, "mail");

        // Verify the response
        assertNotNull(response);
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());

        // Verify that the report service was called
        verify(reportService).sendReportByEmail(eq(userId), any());
    }

    /**
     * Tests that the API Gateway correctly handles the devices report endpoint.
     */
    @Test
    public void testGetDevicesReport() throws Exception {
        // Mock the report service to simulate Excel generation
        doAnswer(invocation -> {
            StreamingOutput output = invocation.getArgument(0);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            output.write(baos);
            return null;
        }).when(reportService).getDevicesExcel(any(), eq(userId));

        // Call the API endpoint
        Response response = reportResource.getDevicesExcel("xlsx");

        // Verify the response
        assertNotNull(response);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        assertNotNull(response.getEntity());
        assertTrue(response.getEntity() instanceof StreamingOutput);

        // Verify that the report service was called with correct parameters
        verify(reportService).getDevicesExcel(any(StreamingOutput.class), eq(userId));
        // Verify that permissions were checked
        verify(permissionsService).checkRestriction(eq(userId), any());
    }
}