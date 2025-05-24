package org.traccar.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.traccar.BaseTest;
import org.traccar.api.resource.*;
import org.traccar.model.*;
import org.traccar.service.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Contract tests for the API Gateway's external API to ensure that it maintains
 * the expected contract with clients. Verifies API endpoint signatures, request/response
 * formats, error handling, and backward compatibility.
 * 
 * This test class is essential for validating that the API Gateway's external interface
 * remains consistent and compatible with existing clients after the microservices refactoring.
 */
@ExtendWith({MockitoExtension.class, SpringExtension.class})
public class ApiContractTest extends BaseTest {

    private MockMvc mockMvc;
    
    @Mock
    private PositionResource positionResource;
    
    @Mock
    private DeviceResource deviceResource;
    
    @Mock
    private CommandResource commandResource;
    
    @Mock
    private EventResource eventResource;
    
    @Mock
    private ReportResource reportResource;

    @BeforeEach
    public void setup() {
        super.setUp();
        
        // Set up the MockMvc instance with all the resource controllers
        mockMvc = MockMvcBuilders.standaloneSetup(
                positionResource,
                deviceResource,
                commandResource,
                eventResource,
                reportResource)
            .build();
            
        // Set up authentication context for tests
        setAuthenticationContext(1L, true);
    }

    //
    // Position Resource Contract Tests
    //
    
    @Test
    public void testGetPositionsEndpoint() throws Exception {
        // Create test data
        Position position1 = new Position();
        position1.setId(1L);
        position1.setDeviceId(1L);
        position1.setLatitude(51.5074);
        position1.setLongitude(-0.1278);
        position1.setFixTime(new Date());
        
        Position position2 = new Position();
        position2.setId(2L);
        position2.setDeviceId(1L);
        position2.setLatitude(48.8566);
        position2.setLongitude(2.3522);
        position2.setFixTime(new Date());
        
        List<Position> positions = Arrays.asList(position1, position2);
        
        // Mock the service response
        when(positionService.getLatestPositions(anyLong()))
            .thenReturn(CompletableFuture.completedFuture(positions));
        
        // Verify the API contract for getting positions
        mockMvc.perform(get("/api/positions")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$", hasSize(2)))
            .andExpect(jsonPath("$[0].id", is(1)))
            .andExpect(jsonPath("$[0].deviceId", is(1)))
            .andExpect(jsonPath("$[0].latitude", is(51.5074)))
            .andExpect(jsonPath("$[0].longitude", is(-0.1278)))
            .andExpect(jsonPath("$[1].id", is(2)))
            .andExpect(jsonPath("$[1].deviceId", is(1)))
            .andExpect(jsonPath("$[1].latitude", is(48.8566)))
            .andExpect(jsonPath("$[1].longitude", is(2.3522)));
    }
    
    @Test
    public void testGetPositionsByDeviceIdEndpoint() throws Exception {
        // Create test data
        Position position = new Position();
        position.setId(1L);
        position.setDeviceId(1L);
        position.setLatitude(51.5074);
        position.setLongitude(-0.1278);
        position.setFixTime(new Date());
        
        List<Position> positions = Collections.singletonList(position);
        
        // Mock the service response
        when(positionService.getPositions(eq(1L), any(Date.class), any(Date.class)))
            .thenReturn(CompletableFuture.completedFuture(positions));
        
        // Verify the API contract for getting positions by device ID
        mockMvc.perform(get("/api/positions")
                .param("deviceId", "1")
                .param("from", "2023-01-01T00:00:00.000Z")
                .param("to", "2023-01-02T00:00:00.000Z")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$", hasSize(1)))
            .andExpect(jsonPath("$[0].id", is(1)))
            .andExpect(jsonPath("$[0].deviceId", is(1)))
            .andExpect(jsonPath("$[0].latitude", is(51.5074)))
            .andExpect(jsonPath("$[0].longitude", is(-0.1278)));
    }
    
    @Test
    public void testDeletePositionEndpoint() throws Exception {
        // Mock the service response
        when(positionService.removePosition(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(true));
        
        // Verify the API contract for deleting a position
        mockMvc.perform(delete("/api/positions/1")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isNoContent());
    }
    
    @Test
    public void testExportPositionsKmlEndpoint() throws Exception {
        // Mock the service response
        when(positionService.exportKml(eq(1L), any(Date.class), any(Date.class)))
            .thenReturn(CompletableFuture.completedFuture("<?xml version=\"1.0\" encoding=\"UTF-8\"?><kml></kml>"));
        
        // Verify the API contract for exporting positions as KML
        mockMvc.perform(get("/api/positions/kml")
                .param("deviceId", "1")
                .param("from", "2023-01-01T00:00:00.000Z")
                .param("to", "2023-01-02T00:00:00.000Z")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(header().string("Content-Disposition", "attachment; filename=positions.kml"))
            .andExpect(content().contentType("application/vnd.google-earth.kml+xml"))
            .andExpect(content().string(containsString("<?xml version=\"1.0\" encoding=\"UTF-8\"?><kml></kml>")));
    }
    
    @Test
    public void testExportPositionsCsvEndpoint() throws Exception {
        // Mock the service response
        when(positionService.exportCsv(eq(1L), any(Date.class), any(Date.class)))
            .thenReturn(CompletableFuture.completedFuture("id,deviceId,latitude,longitude\n1,1,51.5074,-0.1278"));
        
        // Verify the API contract for exporting positions as CSV
        mockMvc.perform(get("/api/positions/csv")
                .param("deviceId", "1")
                .param("from", "2023-01-01T00:00:00.000Z")
                .param("to", "2023-01-02T00:00:00.000Z")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(header().string("Content-Disposition", "attachment; filename=positions.csv"))
            .andExpect(content().contentType("text/csv"))
            .andExpect(content().string(containsString("id,deviceId,latitude,longitude")));
    }
    
    @Test
    public void testExportPositionsGpxEndpoint() throws Exception {
        // Mock the service response
        when(positionService.exportGpx(eq(1L), any(Date.class), any(Date.class)))
            .thenReturn(CompletableFuture.completedFuture("<?xml version=\"1.0\" encoding=\"UTF-8\"?><gpx></gpx>"));
        
        // Verify the API contract for exporting positions as GPX
        mockMvc.perform(get("/api/positions/gpx")
                .param("deviceId", "1")
                .param("from", "2023-01-01T00:00:00.000Z")
                .param("to", "2023-01-02T00:00:00.000Z")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(header().string("Content-Disposition", "attachment; filename=positions.gpx"))
            .andExpect(content().contentType("application/gpx+xml"))
            .andExpect(content().string(containsString("<?xml version=\"1.0\" encoding=\"UTF-8\"?><gpx></gpx>")));
    }

    //
    // Device Resource Contract Tests
    //
    
    @Test
    public void testGetDevicesEndpoint() throws Exception {
        // Create test data
        Device device1 = new Device();
        device1.setId(1L);
        device1.setName("Device 1");
        device1.setUniqueId("123456789");
        device1.setStatus(Device.STATUS_ONLINE);
        
        Device device2 = new Device();
        device2.setId(2L);
        device2.setName("Device 2");
        device2.setUniqueId("987654321");
        device2.setStatus(Device.STATUS_OFFLINE);
        
        List<Device> devices = Arrays.asList(device1, device2);
        
        // Mock the service response
        when(deviceService.getDevices(anyLong()))
            .thenReturn(CompletableFuture.completedFuture(devices));
        
        // Verify the API contract for getting devices
        mockMvc.perform(get("/api/devices")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$", hasSize(2)))
            .andExpect(jsonPath("$[0].id", is(1)))
            .andExpect(jsonPath("$[0].name", is("Device 1")))
            .andExpect(jsonPath("$[0].uniqueId", is("123456789")))
            .andExpect(jsonPath("$[0].status", is(Device.STATUS_ONLINE)))
            .andExpect(jsonPath("$[1].id", is(2)))
            .andExpect(jsonPath("$[1].name", is("Device 2")))
            .andExpect(jsonPath("$[1].uniqueId", is("987654321")))
            .andExpect(jsonPath("$[1].status", is(Device.STATUS_OFFLINE)));
    }
    
    @Test
    public void testGetDeviceByIdEndpoint() throws Exception {
        // Create test data
        Device device = new Device();
        device.setId(1L);
        device.setName("Device 1");
        device.setUniqueId("123456789");
        device.setStatus(Device.STATUS_ONLINE);
        
        // Mock the service response
        when(deviceService.getDevice(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Verify the API contract for getting a device by ID
        mockMvc.perform(get("/api/devices/1")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", is(1)))
            .andExpect(jsonPath("$.name", is("Device 1")))
            .andExpect(jsonPath("$.uniqueId", is("123456789")))
            .andExpect(jsonPath("$.status", is(Device.STATUS_ONLINE)));
    }
    
    @Test
    public void testCreateDeviceEndpoint() throws Exception {
        // Create test data
        Device device = new Device();
        device.setName("New Device");
        device.setUniqueId("123456789");
        
        Device createdDevice = new Device();
        createdDevice.setId(1L);
        createdDevice.setName("New Device");
        createdDevice.setUniqueId("123456789");
        createdDevice.setStatus(Device.STATUS_UNKNOWN);
        
        // Mock the service response
        when(deviceService.addDevice(any(Device.class)))
            .thenReturn(CompletableFuture.completedFuture(createdDevice));
        
        // Verify the API contract for creating a device
        mockMvc.perform(post("/api/devices")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"New Device\",\"uniqueId\":\"123456789\"}"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", is(1)))
            .andExpect(jsonPath("$.name", is("New Device")))
            .andExpect(jsonPath("$.uniqueId", is("123456789")))
            .andExpect(jsonPath("$.status", is(Device.STATUS_UNKNOWN)));
    }
    
    @Test
    public void testUpdateDeviceEndpoint() throws Exception {
        // Create test data
        Device device = new Device();
        device.setId(1L);
        device.setName("Updated Device");
        device.setUniqueId("123456789");
        
        // Mock the service response
        when(deviceService.updateDevice(any(Device.class)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Verify the API contract for updating a device
        mockMvc.perform(put("/api/devices/1")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"id\":1,\"name\":\"Updated Device\",\"uniqueId\":\"123456789\"}"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", is(1)))
            .andExpect(jsonPath("$.name", is("Updated Device")))
            .andExpect(jsonPath("$.uniqueId", is("123456789")));
    }
    
    @Test
    public void testDeleteDeviceEndpoint() throws Exception {
        // Mock the service response
        when(deviceService.removeDevice(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(true));
        
        // Verify the API contract for deleting a device
        mockMvc.perform(delete("/api/devices/1")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isNoContent());
    }
    
    @Test
    public void testUpdateDeviceAccumulatorsEndpoint() throws Exception {
        // Create test data
        DeviceAccumulators accumulators = new DeviceAccumulators();
        accumulators.setDeviceId(1L);
        accumulators.setTotalDistance(1000.0);
        accumulators.setHours(10.0);
        
        // Mock the service response
        when(deviceService.updateAccumulators(any(DeviceAccumulators.class)))
            .thenReturn(CompletableFuture.completedFuture(true));
        
        // Verify the API contract for updating device accumulators
        mockMvc.perform(put("/api/devices/1/accumulators")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"deviceId\":1,\"totalDistance\":1000.0,\"hours\":10.0}"))
            .andExpect(status().isNoContent());
    }

    //
    // Command Resource Contract Tests
    //
    
    @Test
    public void testGetCommandsEndpoint() throws Exception {
        // Create test data
        Command command1 = new Command();
        command1.setId(1L);
        command1.setDeviceId(1L);
        command1.setType(Command.TYPE_CUSTOM);
        command1.setTextChannel(true);
        
        Command command2 = new Command();
        command2.setId(2L);
        command2.setDeviceId(1L);
        command2.setType(Command.TYPE_POSITION_SINGLE);
        command2.setTextChannel(false);
        
        List<Command> commands = Arrays.asList(command1, command2);
        
        // Mock the service response
        when(commandService.getCommands())
            .thenReturn(CompletableFuture.completedFuture(commands));
        
        // Verify the API contract for getting commands
        mockMvc.perform(get("/api/commands")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$", hasSize(2)))
            .andExpect(jsonPath("$[0].id", is(1)))
            .andExpect(jsonPath("$[0].deviceId", is(1)))
            .andExpect(jsonPath("$[0].type", is(Command.TYPE_CUSTOM)))
            .andExpect(jsonPath("$[0].textChannel", is(true)))
            .andExpect(jsonPath("$[1].id", is(2)))
            .andExpect(jsonPath("$[1].deviceId", is(1)))
            .andExpect(jsonPath("$[1].type", is(Command.TYPE_POSITION_SINGLE)))
            .andExpect(jsonPath("$[1].textChannel", is(false)));
    }
    
    @Test
    public void testGetCommandTypesEndpoint() throws Exception {
        // Create test data
        List<Typed> commandTypes = Arrays.asList(
            new Typed(Command.TYPE_CUSTOM),
            new Typed(Command.TYPE_POSITION_SINGLE),
            new Typed(Command.TYPE_POSITION_PERIODIC)
        );
        
        // Mock the service response
        when(commandService.getCommandTypes(eq(1L), eq(true)))
            .thenReturn(CompletableFuture.completedFuture(commandTypes));
        
        // Verify the API contract for getting command types
        mockMvc.perform(get("/api/commands/types")
                .param("deviceId", "1")
                .param("textChannel", "true")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$", hasSize(3)))
            .andExpect(jsonPath("$[0].type", is(Command.TYPE_CUSTOM)))
            .andExpect(jsonPath("$[1].type", is(Command.TYPE_POSITION_SINGLE)))
            .andExpect(jsonPath("$[2].type", is(Command.TYPE_POSITION_PERIODIC)));
    }
    
    @Test
    public void testSendCommandEndpoint() throws Exception {
        // Create test data
        Command command = new Command();
        command.setDeviceId(1L);
        command.setType(Command.TYPE_CUSTOM);
        command.setTextChannel(true);
        
        QueuedCommand queuedCommand = new QueuedCommand();
        queuedCommand.setId(1L);
        queuedCommand.setDeviceId(1L);
        queuedCommand.setType(Command.TYPE_CUSTOM);
        queuedCommand.setTextChannel(true);
        
        // Mock the service response
        when(commandService.sendCommand(any(Command.class)))
            .thenReturn(CompletableFuture.completedFuture(queuedCommand));
        
        // Verify the API contract for sending a command
        mockMvc.perform(post("/api/commands/send")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"deviceId\":1,\"type\":\"custom\",\"textChannel\":true}"))
            .andExpect(status().isAccepted())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", is(1)))
            .andExpect(jsonPath("$.deviceId", is(1)))
            .andExpect(jsonPath("$.type", is(Command.TYPE_CUSTOM)))
            .andExpect(jsonPath("$.textChannel", is(true)));
    }

    //
    // Event Resource Contract Tests
    //
    
    @Test
    public void testGetEventByIdEndpoint() throws Exception {
        // Create test data
        Event event = new Event();
        event.setId(1L);
        event.setDeviceId(1L);
        event.setType(Event.TYPE_DEVICE_ONLINE);
        event.setServerTime(new Date());
        
        // Mock the service response
        when(eventService.getEvent(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(event));
        
        // Verify the API contract for getting an event by ID
        mockMvc.perform(get("/api/events/1")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", is(1)))
            .andExpect(jsonPath("$.deviceId", is(1)))
            .andExpect(jsonPath("$.type", is(Event.TYPE_DEVICE_ONLINE)));
    }
    
    @Test
    public void testGetEventNotFoundEndpoint() throws Exception {
        // Mock the service response for a non-existent event
        when(eventService.getEvent(eq(999L)))
            .thenReturn(CompletableFuture.completedFuture(null));
        
        // Verify the API contract for getting a non-existent event
        mockMvc.perform(get("/api/events/999")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isNotFound());
    }

    //
    // Report Resource Contract Tests
    //
    
    @Test
    public void testGetRouteReportEndpoint() throws Exception {
        // Create test data
        Position position1 = new Position();
        position1.setId(1L);
        position1.setDeviceId(1L);
        position1.setLatitude(51.5074);
        position1.setLongitude(-0.1278);
        position1.setFixTime(new Date());
        
        Position position2 = new Position();
        position2.setId(2L);
        position2.setDeviceId(1L);
        position2.setLatitude(48.8566);
        position2.setLongitude(2.3522);
        position2.setFixTime(new Date());
        
        List<Position> positions = Arrays.asList(position1, position2);
        
        // Mock the service response
        when(reportService.getRouteReport(any(List.class), any(List.class), any(Date.class), any(Date.class)))
            .thenReturn(CompletableFuture.completedFuture(positions));
        
        // Verify the API contract for getting a route report
        mockMvc.perform(get("/api/reports/route")
                .param("deviceId", "1")
                .param("from", "2023-01-01T00:00:00.000Z")
                .param("to", "2023-01-02T00:00:00.000Z")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$", hasSize(2)))
            .andExpect(jsonPath("$[0].id", is(1)))
            .andExpect(jsonPath("$[0].deviceId", is(1)))
            .andExpect(jsonPath("$[0].latitude", is(51.5074)))
            .andExpect(jsonPath("$[0].longitude", is(-0.1278)))
            .andExpect(jsonPath("$[1].id", is(2)))
            .andExpect(jsonPath("$[1].deviceId", is(1)))
            .andExpect(jsonPath("$[1].latitude", is(48.8566)))
            .andExpect(jsonPath("$[1].longitude", is(2.3522)));
    }
    
    @Test
    public void testGetEventsReportEndpoint() throws Exception {
        // Create test data
        Event event1 = new Event();
        event1.setId(1L);
        event1.setDeviceId(1L);
        event1.setType(Event.TYPE_DEVICE_ONLINE);
        event1.setServerTime(new Date());
        
        Event event2 = new Event();
        event2.setId(2L);
        event2.setDeviceId(1L);
        event2.setType(Event.TYPE_DEVICE_OFFLINE);
        event2.setServerTime(new Date());
        
        List<Event> events = Arrays.asList(event1, event2);
        
        // Mock the service response
        when(reportService.getEventsReport(
                any(List.class), any(List.class), any(List.class), any(List.class), any(Date.class), any(Date.class)))
            .thenReturn(CompletableFuture.completedFuture(events));
        
        // Verify the API contract for getting an events report
        mockMvc.perform(get("/api/reports/events")
                .param("deviceId", "1")
                .param("type", Event.TYPE_DEVICE_ONLINE)
                .param("from", "2023-01-01T00:00:00.000Z")
                .param("to", "2023-01-02T00:00:00.000Z")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$", hasSize(2)))
            .andExpect(jsonPath("$[0].id", is(1)))
            .andExpect(jsonPath("$[0].deviceId", is(1)))
            .andExpect(jsonPath("$[0].type", is(Event.TYPE_DEVICE_ONLINE)))
            .andExpect(jsonPath("$[1].id", is(2)))
            .andExpect(jsonPath("$[1].deviceId", is(1)))
            .andExpect(jsonPath("$[1].type", is(Event.TYPE_DEVICE_OFFLINE)));
    }
    
    @Test
    public void testGetSummaryReportEndpoint() throws Exception {
        // Create test data
        SummaryReportItem item1 = new SummaryReportItem();
        item1.setDeviceId(1L);
        item1.setDeviceName("Device 1");
        item1.setDistance(100.0);
        item1.setSpentFuel(5.0);
        
        SummaryReportItem item2 = new SummaryReportItem();
        item2.setDeviceId(2L);
        item2.setDeviceName("Device 2");
        item2.setDistance(200.0);
        item2.setSpentFuel(10.0);
        
        List<SummaryReportItem> items = Arrays.asList(item1, item2);
        
        // Mock the service response
        when(reportService.getSummaryReport(
                any(List.class), any(List.class), any(Date.class), any(Date.class), eq(false)))
            .thenReturn(CompletableFuture.completedFuture(items));
        
        // Verify the API contract for getting a summary report
        mockMvc.perform(get("/api/reports/summary")
                .param("deviceId", "1,2")
                .param("from", "2023-01-01T00:00:00.000Z")
                .param("to", "2023-01-02T00:00:00.000Z")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$", hasSize(2)))
            .andExpect(jsonPath("$[0].deviceId", is(1)))
            .andExpect(jsonPath("$[0].deviceName", is("Device 1")))
            .andExpect(jsonPath("$[0].distance", is(100.0)))
            .andExpect(jsonPath("$[0].spentFuel", is(5.0)))
            .andExpect(jsonPath("$[1].deviceId", is(2)))
            .andExpect(jsonPath("$[1].deviceName", is("Device 2")))
            .andExpect(jsonPath("$[1].distance", is(200.0)))
            .andExpect(jsonPath("$[1].spentFuel", is(10.0)));
    }
    
    @Test
    public void testGetTripsReportEndpoint() throws Exception {
        // Create test data
        TripReportItem item1 = new TripReportItem();
        item1.setDeviceId(1L);
        item1.setDeviceName("Device 1");
        item1.setStartTime(new Date());
        item1.setEndTime(new Date());
        item1.setDistance(100.0);
        item1.setAverageSpeed(50.0);
        
        TripReportItem item2 = new TripReportItem();
        item2.setDeviceId(1L);
        item2.setDeviceName("Device 1");
        item2.setStartTime(new Date());
        item2.setEndTime(new Date());
        item2.setDistance(200.0);
        item2.setAverageSpeed(60.0);
        
        List<TripReportItem> items = Arrays.asList(item1, item2);
        
        // Mock the service response
        when(reportService.getTripsReport(any(List.class), any(List.class), any(Date.class), any(Date.class)))
            .thenReturn(CompletableFuture.completedFuture(items));
        
        // Verify the API contract for getting a trips report
        mockMvc.perform(get("/api/reports/trips")
                .param("deviceId", "1")
                .param("from", "2023-01-01T00:00:00.000Z")
                .param("to", "2023-01-02T00:00:00.000Z")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$", hasSize(2)))
            .andExpect(jsonPath("$[0].deviceId", is(1)))
            .andExpect(jsonPath("$[0].deviceName", is("Device 1")))
            .andExpect(jsonPath("$[0].distance", is(100.0)))
            .andExpect(jsonPath("$[0].averageSpeed", is(50.0)))
            .andExpect(jsonPath("$[1].deviceId", is(1)))
            .andExpect(jsonPath("$[1].deviceName", is("Device 1")))
            .andExpect(jsonPath("$[1].distance", is(200.0)))
            .andExpect(jsonPath("$[1].averageSpeed", is(60.0)));
    }
    
    @Test
    public void testGetStopsReportEndpoint() throws Exception {
        // Create test data
        StopReportItem item1 = new StopReportItem();
        item1.setDeviceId(1L);
        item1.setDeviceName("Device 1");
        item1.setStartTime(new Date());
        item1.setEndTime(new Date());
        item1.setDuration(3600L); // 1 hour in seconds
        item1.setAddress("London, UK");
        
        StopReportItem item2 = new StopReportItem();
        item2.setDeviceId(1L);
        item2.setDeviceName("Device 1");
        item2.setStartTime(new Date());
        item2.setEndTime(new Date());
        item2.setDuration(7200L); // 2 hours in seconds
        item2.setAddress("Paris, France");
        
        List<StopReportItem> items = Arrays.asList(item1, item2);
        
        // Mock the service response
        when(reportService.getStopsReport(any(List.class), any(List.class), any(Date.class), any(Date.class)))
            .thenReturn(CompletableFuture.completedFuture(items));
        
        // Verify the API contract for getting a stops report
        mockMvc.perform(get("/api/reports/stops")
                .param("deviceId", "1")
                .param("from", "2023-01-01T00:00:00.000Z")
                .param("to", "2023-01-02T00:00:00.000Z")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$", hasSize(2)))
            .andExpect(jsonPath("$[0].deviceId", is(1)))
            .andExpect(jsonPath("$[0].deviceName", is("Device 1")))
            .andExpect(jsonPath("$[0].duration", is(3600)))
            .andExpect(jsonPath("$[0].address", is("London, UK")))
            .andExpect(jsonPath("$[1].deviceId", is(1)))
            .andExpect(jsonPath("$[1].deviceName", is("Device 1")))
            .andExpect(jsonPath("$[1].duration", is(7200)))
            .andExpect(jsonPath("$[1].address", is("Paris, France")));
    }
    
    //
    // Error Handling Contract Tests
    //
    
    @Test
    public void testResourceNotFoundErrorHandling() throws Exception {
        // Mock the service response for a non-existent resource
        when(deviceService.getDevice(eq(999L)))
            .thenReturn(CompletableFuture.completedFuture(null));
        
        // Verify the API contract for handling a resource not found error
        mockMvc.perform(get("/api/devices/999")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isNotFound());
    }
    
    @Test
    public void testUnauthorizedErrorHandling() throws Exception {
        // Clear the authentication context to simulate an unauthorized request
        clearAuthenticationContext();
        
        // Verify the API contract for handling an unauthorized error
        mockMvc.perform(get("/api/devices")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isUnauthorized());
    }
    
    @Test
    public void testForbiddenErrorHandling() throws Exception {
        // Set up a non-admin user for testing permission denied
        setAuthenticationContext(2L, false);
        
        // Mock the service response to throw a permission exception
        when(deviceService.getDevice(eq(1L)))
            .thenReturn(CompletableFuture.failedFuture(new SecurityException("Permission denied")));
        
        // Verify the API contract for handling a forbidden error
        mockMvc.perform(get("/api/devices/1")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isForbidden());
    }
    
    @Test
    public void testBadRequestErrorHandling() throws Exception {
        // Verify the API contract for handling a bad request error (invalid JSON)
        mockMvc.perform(post("/api/devices")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{invalid json}"))
            .andExpect(status().isBadRequest());
    }
    
    @Test
    public void testInternalServerErrorHandling() throws Exception {
        // Mock the service response to throw an unexpected exception
        when(deviceService.getDevices(anyLong()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Unexpected error")));
        
        // Verify the API contract for handling an internal server error
        mockMvc.perform(get("/api/devices")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isInternalServerError());
    }
    
    //
    // Backward Compatibility Tests
    //
    
    @Test
    public void testLegacyEndpointCompatibility() throws Exception {
        // Create test data
        Device device = new Device();
        device.setId(1L);
        device.setName("Device 1");
        device.setUniqueId("123456789");
        
        // Mock the service response
        when(deviceService.getDevice(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Verify that the legacy endpoint still works
        mockMvc.perform(get("/api/devices/1")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", is(1)))
            .andExpect(jsonPath("$.name", is("Device 1")))
            .andExpect(jsonPath("$.uniqueId", is("123456789")));
    }
    
    @Test
    public void testLegacyParameterCompatibility() throws Exception {
        // Create test data
        Position position = new Position();
        position.setId(1L);
        position.setDeviceId(1L);
        position.setLatitude(51.5074);
        position.setLongitude(-0.1278);
        position.setFixTime(new Date());
        
        List<Position> positions = Collections.singletonList(position);
        
        // Mock the service response
        when(positionService.getPositions(eq(1L), any(Date.class), any(Date.class)))
            .thenReturn(CompletableFuture.completedFuture(positions));
        
        // Verify that the legacy parameter names still work
        mockMvc.perform(get("/api/positions")
                .param("deviceId", "1")
                .param("from", "2023-01-01T00:00:00.000Z")
                .param("to", "2023-01-02T00:00:00.000Z")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$", hasSize(1)))
            .andExpect(jsonPath("$[0].id", is(1)))
            .andExpect(jsonPath("$[0].deviceId", is(1)))
            .andExpect(jsonPath("$[0].latitude", is(51.5074)))
            .andExpect(jsonPath("$[0].longitude", is(-0.1278)));
    }
    
    @Test
    public void testLegacyResponseFormatCompatibility() throws Exception {
        // Create test data
        Device device = new Device();
        device.setId(1L);
        device.setName("Device 1");
        device.setUniqueId("123456789");
        device.setStatus(Device.STATUS_ONLINE);
        device.setLastUpdate(new Date());
        device.setPositionId(100L);
        
        // Add legacy fields that should still be included in the response
        device.set("legacyField1", "legacy value 1");
        device.set("legacyField2", 123);
        
        // Mock the service response
        when(deviceService.getDevice(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Verify that the legacy fields are still included in the response
        mockMvc.perform(get("/api/devices/1")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", is(1)))
            .andExpect(jsonPath("$.name", is("Device 1")))
            .andExpect(jsonPath("$.uniqueId", is("123456789")))
            .andExpect(jsonPath("$.status", is(Device.STATUS_ONLINE)))
            .andExpect(jsonPath("$.positionId", is(100)))
            .andExpect(jsonPath("$.attributes.legacyField1", is("legacy value 1")))
            .andExpect(jsonPath("$.attributes.legacyField2", is(123)));
    }
}