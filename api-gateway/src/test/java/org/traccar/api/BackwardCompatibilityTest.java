package org.traccar.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.traccar.BaseTest;
import org.traccar.api.resource.DeviceResource;
import org.traccar.api.resource.PositionResource;
import org.traccar.api.resource.CommandResource;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.model.Command;
import org.traccar.model.QueuedCommand;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Tests the API Gateway's backward compatibility mechanisms to ensure that clients
 * using older API versions continue to function correctly. Verifies request/response
 * transformation, API versioning support, deprecated endpoint handling, and compatibility
 * with legacy clients.
 * 
 * This test class is crucial for validating that the microservices refactoring maintains
 * compatibility with existing integrations.
 */
@ExtendWith({MockitoExtension.class, SpringExtension.class})
public class BackwardCompatibilityTest extends BaseTest {

    private MockMvc mockMvc;
    
    @Mock
    private PositionResource positionResource;
    
    @Mock
    private DeviceResource deviceResource;
    
    @Mock
    private CommandResource commandResource;

    @BeforeEach
    public void setup() {
        super.setUp();
        
        // Set up the MockMvc instance with all the resource controllers
        mockMvc = MockMvcBuilders.standaloneSetup(
                positionResource,
                deviceResource,
                commandResource)
            .build();
            
        // Set up authentication context for tests
        setAuthenticationContext(1L, true);
    }

    //
    // API Versioning Tests
    //
    
    @Test
    public void testDefaultApiVersionSupport() throws Exception {
        // Create test data
        Device device = new Device();
        device.setId(1L);
        device.setName("Device 1");
        device.setUniqueId("123456789");
        device.setStatus(Device.STATUS_ONLINE);
        
        // Mock the service response
        when(deviceService.getDevice(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Verify that requests without explicit version use the default version
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
    public void testExplicitApiVersionSupport() throws Exception {
        // Create test data
        Device device = new Device();
        device.setId(1L);
        device.setName("Device 1");
        device.setUniqueId("123456789");
        device.setStatus(Device.STATUS_ONLINE);
        
        // Mock the service response
        when(deviceService.getDevice(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Verify that requests with explicit version header are handled correctly
        mockMvc.perform(get("/api/devices/1")
                .header("X-API-Version", "1.0")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", is(1)))
            .andExpect(jsonPath("$.name", is("Device 1")))
            .andExpect(jsonPath("$.uniqueId", is("123456789")))
            .andExpect(jsonPath("$.status", is(Device.STATUS_ONLINE)));
    }
    
    @Test
    public void testVersionedEndpointSupport() throws Exception {
        // Create test data
        Device device = new Device();
        device.setId(1L);
        device.setName("Device 1");
        device.setUniqueId("123456789");
        device.setStatus(Device.STATUS_ONLINE);
        
        // Mock the service response
        when(deviceService.getDevice(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Verify that versioned endpoints are supported
        mockMvc.perform(get("/api/v1/devices/1")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", is(1)))
            .andExpect(jsonPath("$.name", is("Device 1")))
            .andExpect(jsonPath("$.uniqueId", is("123456789")))
            .andExpect(jsonPath("$.status", is(Device.STATUS_ONLINE)));
    }

    //
    // Request Transformation Tests
    //
    
    @Test
    public void testLegacyParameterNameTransformation() throws Exception {
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
        
        // Verify that legacy parameter names are transformed correctly
        mockMvc.perform(get("/api/positions")
                .param("deviceId", "1")
                .param("deviceTime", "2023-01-01T00:00:00.000Z") // Legacy parameter name
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
    public void testLegacyRequestFormatTransformation() throws Exception {
        // Create test data
        Device device = new Device();
        device.setId(1L);
        device.setName("Updated Device");
        device.setUniqueId("123456789");
        
        // Mock the service response
        when(deviceService.updateDevice(any(Device.class)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Verify that legacy request format is transformed correctly
        mockMvc.perform(put("/api/devices/1")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"id\":1,\"name\":\"Updated Device\",\"uniqueId\":\"123456789\",\"status\":\"online\"}" +
                         // Legacy format included additional fields
                         ",\"legacyField\":\"legacy value\",\"oldAttribute\":123}"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", is(1)))
            .andExpect(jsonPath("$.name", is("Updated Device")))
            .andExpect(jsonPath("$.uniqueId", is("123456789")));
    }
    
    @Test
    public void testLegacyContentTypeSupport() throws Exception {
        // Create test data
        Device device = new Device();
        device.setId(1L);
        device.setName("Device 1");
        device.setUniqueId("123456789");
        device.setStatus(Device.STATUS_ONLINE);
        
        // Mock the service response
        when(deviceService.getDevice(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Verify that legacy content type is supported
        mockMvc.perform(get("/api/devices/1")
                .contentType("application/vnd.traccar.v1+json")) // Legacy vendor-specific content type
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", is(1)))
            .andExpect(jsonPath("$.name", is("Device 1")))
            .andExpect(jsonPath("$.uniqueId", is("123456789")))
            .andExpect(jsonPath("$.status", is(Device.STATUS_ONLINE)));
    }

    //
    // Response Transformation Tests
    //
    
    @Test
    public void testLegacyResponseFieldsIncluded() throws Exception {
        // Create test data
        Device device = new Device();
        device.setId(1L);
        device.setName("Device 1");
        device.setUniqueId("123456789");
        device.setStatus(Device.STATUS_ONLINE);
        
        // Add legacy fields that should still be included in the response
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("legacyField1", "legacy value 1");
        attributes.put("legacyField2", 123);
        device.setAttributes(attributes);
        
        // Mock the service response
        when(deviceService.getDevice(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Verify that legacy fields are still included in the response
        mockMvc.perform(get("/api/devices/1")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", is(1)))
            .andExpect(jsonPath("$.name", is("Device 1")))
            .andExpect(jsonPath("$.uniqueId", is("123456789")))
            .andExpect(jsonPath("$.status", is(Device.STATUS_ONLINE)))
            .andExpect(jsonPath("$.attributes.legacyField1", is("legacy value 1")))
            .andExpect(jsonPath("$.attributes.legacyField2", is(123)));
    }
    
    @Test
    public void testVersionSpecificResponseFormat() throws Exception {
        // Create test data
        Position position = new Position();
        position.setId(1L);
        position.setDeviceId(1L);
        position.setLatitude(51.5074);
        position.setLongitude(-0.1278);
        position.setFixTime(new Date());
        position.setSpeed(60.0);
        position.setCourse(90.0);
        
        // Add attributes that should be included in the response for specific versions
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("legacyAttribute", "legacy value");
        attributes.put("protocol", "tcp");
        attributes.put("batteryLevel", 75);
        position.setAttributes(attributes);
        
        List<Position> positions = Collections.singletonList(position);
        
        // Mock the service response
        when(positionService.getPositions(eq(1L), any(Date.class), any(Date.class)))
            .thenReturn(CompletableFuture.completedFuture(positions));
        
        // Verify that version 1.0 includes legacy attributes
        mockMvc.perform(get("/api/positions")
                .header("X-API-Version", "1.0")
                .param("deviceId", "1")
                .param("from", "2023-01-01T00:00:00.000Z")
                .param("to", "2023-01-02T00:00:00.000Z")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$[0].id", is(1)))
            .andExpect(jsonPath("$[0].deviceId", is(1)))
            .andExpect(jsonPath("$[0].latitude", is(51.5074)))
            .andExpect(jsonPath("$[0].longitude", is(-0.1278)))
            .andExpect(jsonPath("$[0].attributes.legacyAttribute", is("legacy value")));
        
        // Verify that version 2.0 includes new format with nested attributes
        mockMvc.perform(get("/api/positions")
                .header("X-API-Version", "2.0")
                .param("deviceId", "1")
                .param("from", "2023-01-01T00:00:00.000Z")
                .param("to", "2023-01-02T00:00:00.000Z")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$[0].id", is(1)))
            .andExpect(jsonPath("$[0].deviceId", is(1)))
            .andExpect(jsonPath("$[0].position.latitude", is(51.5074)))
            .andExpect(jsonPath("$[0].position.longitude", is(-0.1278)))
            .andExpect(jsonPath("$[0].attributes.protocol", is("tcp")))
            .andExpect(jsonPath("$[0].attributes.batteryLevel", is(75)));
    }

    //
    // Deprecated Endpoint Tests
    //
    
    @Test
    public void testDeprecatedEndpointStillWorks() throws Exception {
        // Create test data
        Device device = new Device();
        device.setId(1L);
        device.setName("Device 1");
        device.setUniqueId("123456789");
        device.setStatus(Device.STATUS_ONLINE);
        
        List<Device> devices = Collections.singletonList(device);
        
        // Mock the service response
        when(deviceService.getDevices(anyLong()))
            .thenReturn(CompletableFuture.completedFuture(devices));
        
        // Verify that deprecated endpoint still works
        mockMvc.perform(get("/api/device") // Deprecated endpoint (singular instead of plural)
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$", hasSize(1)))
            .andExpect(jsonPath("$[0].id", is(1)))
            .andExpect(jsonPath("$[0].name", is("Device 1")))
            .andExpect(jsonPath("$[0].uniqueId", is("123456789")))
            .andExpect(jsonPath("$[0].status", is(Device.STATUS_ONLINE)));
    }
    
    @Test
    public void testDeprecatedEndpointWithWarningHeader() throws Exception {
        // Create test data
        Device device = new Device();
        device.setId(1L);
        device.setName("Device 1");
        device.setUniqueId("123456789");
        device.setStatus(Device.STATUS_ONLINE);
        
        List<Device> devices = Collections.singletonList(device);
        
        // Mock the service response
        when(deviceService.getDevices(anyLong()))
            .thenReturn(CompletableFuture.completedFuture(devices));
        
        // Verify that deprecated endpoint includes warning header
        mockMvc.perform(get("/api/device") // Deprecated endpoint
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(header().string(HttpHeaders.WARNING, containsString("deprecated")))
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$", hasSize(1)))
            .andExpect(jsonPath("$[0].id", is(1)))
            .andExpect(jsonPath("$[0].name", is("Device 1")))
            .andExpect(jsonPath("$[0].uniqueId", is("123456789")))
            .andExpect(jsonPath("$[0].status", is(Device.STATUS_ONLINE)));
    }
    
    @Test
    public void testDeprecatedParameterStillWorks() throws Exception {
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
        
        // Verify that deprecated parameter still works
        mockMvc.perform(post("/api/commands/send")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"deviceId\":1,\"type\":\"custom\",\"textChannel\":true,\"oldParam\":\"value\"}")) // Deprecated parameter
            .andExpect(status().isAccepted())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", is(1)))
            .andExpect(jsonPath("$.deviceId", is(1)))
            .andExpect(jsonPath("$.type", is(Command.TYPE_CUSTOM)))
            .andExpect(jsonPath("$.textChannel", is(true)));
    }

    //
    // Legacy Client Compatibility Tests
    //
    
    @Test
    public void testLegacyClientUserAgent() throws Exception {
        // Create test data
        Device device = new Device();
        device.setId(1L);
        device.setName("Device 1");
        device.setUniqueId("123456789");
        device.setStatus(Device.STATUS_ONLINE);
        
        // Mock the service response
        when(deviceService.getDevice(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Verify that legacy client user agent is supported
        mockMvc.perform(get("/api/devices/1")
                .header("User-Agent", "Traccar Client v3.0") // Legacy client user agent
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", is(1)))
            .andExpect(jsonPath("$.name", is("Device 1")))
            .andExpect(jsonPath("$.uniqueId", is("123456789")))
            .andExpect(jsonPath("$.status", is(Device.STATUS_ONLINE)));
    }
    
    @Test
    public void testLegacyClientAcceptHeader() throws Exception {
        // Create test data
        Device device = new Device();
        device.setId(1L);
        device.setName("Device 1");
        device.setUniqueId("123456789");
        device.setStatus(Device.STATUS_ONLINE);
        
        // Mock the service response
        when(deviceService.getDevice(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(device));
        
        // Verify that legacy client accept header is supported
        mockMvc.perform(get("/api/devices/1")
                .header("Accept", "application/vnd.traccar.v1+json")) // Legacy accept header
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", is(1)))
            .andExpect(jsonPath("$.name", is("Device 1")))
            .andExpect(jsonPath("$.uniqueId", is("123456789")))
            .andExpect(jsonPath("$.status", is(Device.STATUS_ONLINE)));
    }
    
    @Test
    public void testLegacyClientAuthenticationMethod() throws Exception {
        // Create test data
        Device device = new Device();
        device.setId(1L);
        device.setName("Device 1");
        device.setUniqueId("123456789");
        device.setStatus(Device.STATUS_ONLINE);
        
        List<Device> devices = Collections.singletonList(device);
        
        // Mock the service response
        when(deviceService.getDevices(anyLong()))
            .thenReturn(CompletableFuture.completedFuture(devices));
        
        // Verify that legacy client authentication method is supported
        mockMvc.perform(get("/api/devices")
                .header("Authorization", "Basic dXNlcjpwYXNzd29yZA==") // Legacy basic auth
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$", hasSize(1)))
            .andExpect(jsonPath("$[0].id", is(1)))
            .andExpect(jsonPath("$[0].name", is("Device 1")))
            .andExpect(jsonPath("$[0].uniqueId", is("123456789")))
            .andExpect(jsonPath("$[0].status", is(Device.STATUS_ONLINE)));
    }
    
    @Test
    public void testLegacyClientErrorFormat() throws Exception {
        // Mock the service response to throw an exception
        when(deviceService.getDevice(eq(999L)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Device not found")));
        
        // Verify that legacy client error format is supported
        mockMvc.perform(get("/api/devices/999")
                .header("User-Agent", "Traccar Client v3.0") // Legacy client user agent
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isNotFound())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.status", is(404)))
            .andExpect(jsonPath("$.message", containsString("Device not found")));
    }
}