package org.traccar.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.websocket.api.Session;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.traccar.api.AsyncSocket;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.model.LogRecord;
import org.traccar.session.ConnectionManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;

/**
 * Tests the real-time update propagation from the message broker to WebSocket clients through the API Gateway.
 * Verifies that updates published to the message broker are correctly consumed by the API Gateway,
 * formatted as JSON, and delivered to the appropriate WebSocket clients.
 */
@ExtendWith(MockitoExtension.class)
public class WebSocketUpdatePropagationTest {

    @Mock
    private ConnectionManager connectionManager;

    @Mock
    private Storage storage;

    @Mock
    private Session webSocketSession;

    @Mock
    private MessageBrokerConsumer messageBrokerConsumer;

    @Mock
    private ConnectionRegistry connectionRegistry;

    @Mock
    private TextMapPropagator traceContextPropagator;

    private ObjectMapper objectMapper;
    private AsyncSocket asyncSocket;
    private long testUserId = 1L;

    @BeforeEach
    public void setUp() throws StorageException {
        objectMapper = new ObjectMapper();
        
        // Setup WebSocket session
        when(webSocketSession.isOpen()).thenReturn(true);
        when(webSocketSession.getRemote()).thenReturn(mock(Session.RemoteEndpoint.class));
        
        // Create AsyncSocket with mocked dependencies
        asyncSocket = new AsyncSocket(objectMapper, connectionManager, storage, testUserId);
        asyncSocket.onWebSocketConnect(webSocketSession);
        
        // Setup connection registry to return our test user's connections
        when(connectionRegistry.getUserConnections(testUserId))
            .thenReturn(Collections.singletonList(asyncSocket));
    }

    @Test
    public void testDeviceUpdatePropagation() throws Exception {
        // Create a test device update
        Device device = new Device();
        device.setId(100L);
        device.setName("Test Device");
        device.setUniqueId("test-device-123");
        device.setStatus(Device.STATUS_ONLINE);

        // Setup trace context
        SpanContext spanContext = createTestSpanContext();
        Map<String, String> traceHeaders = new HashMap<>();
        traceHeaders.put("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        
        // Mock trace context extraction
        when(traceContextPropagator.extract(any(Context.class), eq(traceHeaders), any(TextMapGetter.class)))
            .thenReturn(Context.current().with(Span.wrap(spanContext)));

        // Simulate message broker receiving a device update
        messageBrokerConsumer.onDeviceUpdate(device, traceHeaders);

        // Verify connection registry was queried for the device's owner
        verify(connectionRegistry).getUsersForDevice(device.getId());
        
        // Capture the JSON message sent to the WebSocket client
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        verify(webSocketSession.getRemote(), times(1)).sendString(messageCaptor.capture());
        
        // Parse and verify the JSON message
        String sentMessage = messageCaptor.getValue();
        JsonNode jsonMessage = objectMapper.readTree(sentMessage);
        
        // Verify message structure
        assertEquals("devices", jsonMessage.get("type").asText());
        assertNotNull(jsonMessage.get("data"));
        assertEquals(device.getId().longValue(), jsonMessage.get("data").get("id").asLong());
        assertEquals(device.getName(), jsonMessage.get("data").get("name").asText());
        assertEquals(device.getUniqueId(), jsonMessage.get("data").get("uniqueId").asText());
        
        // Verify trace context was added
        assertNotNull(jsonMessage.get("traceContext"));
        assertEquals("4bf92f3577b34da6a3ce929d0e0e4736", jsonMessage.get("traceContext").get("traceId").asText());
    }

    @Test
    public void testPositionUpdatePropagation() throws Exception {
        // Create a test position update
        Position position = new Position();
        position.setId(200L);
        position.setDeviceId(100L);
        position.setLatitude(47.6062);
        position.setLongitude(-122.3321);
        position.setValid(true);
        
        // Setup trace context
        Map<String, String> traceHeaders = new HashMap<>();
        traceHeaders.put("traceparent", "00-5bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");

        // Simulate message broker receiving a position update
        messageBrokerConsumer.onPositionUpdate(position, traceHeaders);

        // Verify connection registry was queried for the device's owner
        verify(connectionRegistry).getUsersForDevice(position.getDeviceId());
        
        // Capture the JSON message sent to the WebSocket client
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        verify(webSocketSession.getRemote(), times(1)).sendString(messageCaptor.capture());
        
        // Parse and verify the JSON message
        String sentMessage = messageCaptor.getValue();
        JsonNode jsonMessage = objectMapper.readTree(sentMessage);
        
        // Verify message structure
        assertEquals("positions", jsonMessage.get("type").asText());
        assertNotNull(jsonMessage.get("data"));
        assertEquals(position.getId().longValue(), jsonMessage.get("data").get("id").asLong());
        assertEquals(position.getDeviceId().longValue(), jsonMessage.get("data").get("deviceId").asLong());
        assertEquals(position.getLatitude(), jsonMessage.get("data").get("latitude").asDouble());
        assertEquals(position.getLongitude(), jsonMessage.get("data").get("longitude").asDouble());
        
        // Verify trace context was added
        assertNotNull(jsonMessage.get("traceContext"));
        assertEquals("5bf92f3577b34da6a3ce929d0e0e4736", jsonMessage.get("traceContext").get("traceId").asText());
    }

    @Test
    public void testEventUpdatePropagation() throws Exception {
        // Create a test event update
        Event event = new Event();
        event.setId(300L);
        event.setDeviceId(100L);
        event.setType(Event.TYPE_DEVICE_ONLINE);
        event.setPositionId(200L);
        
        // Setup trace context
        Map<String, String> traceHeaders = new HashMap<>();
        traceHeaders.put("traceparent", "00-6bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");

        // Simulate message broker receiving an event update
        messageBrokerConsumer.onEventUpdate(event, traceHeaders);

        // Verify connection registry was queried for the device's owner
        verify(connectionRegistry).getUsersForDevice(event.getDeviceId());
        
        // Capture the JSON message sent to the WebSocket client
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        verify(webSocketSession.getRemote(), times(1)).sendString(messageCaptor.capture());
        
        // Parse and verify the JSON message
        String sentMessage = messageCaptor.getValue();
        JsonNode jsonMessage = objectMapper.readTree(sentMessage);
        
        // Verify message structure
        assertEquals("events", jsonMessage.get("type").asText());
        assertNotNull(jsonMessage.get("data"));
        assertEquals(event.getId().longValue(), jsonMessage.get("data").get("id").asLong());
        assertEquals(event.getDeviceId().longValue(), jsonMessage.get("data").get("deviceId").asLong());
        assertEquals(event.getType(), jsonMessage.get("data").get("type").asText());
        
        // Verify trace context was added
        assertNotNull(jsonMessage.get("traceContext"));
        assertEquals("6bf92f3577b34da6a3ce929d0e0e4736", jsonMessage.get("traceContext").get("traceId").asText());
    }

    @Test
    public void testLogUpdatePropagation() throws Exception {
        // Create a test log update
        LogRecord logRecord = new LogRecord();
        logRecord.setId(400L);
        logRecord.setTime(System.currentTimeMillis());
        logRecord.setLevel("INFO");
        logRecord.setMessage("Test log message");
        
        // Setup trace context
        Map<String, String> traceHeaders = new HashMap<>();
        traceHeaders.put("traceparent", "00-7bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");

        // Enable log streaming for the test socket
        asyncSocket.onWebSocketText("{\"logs\":true}");

        // Simulate message broker receiving a log update
        messageBrokerConsumer.onLogUpdate(logRecord, traceHeaders);

        // Verify connection registry was queried for users with log access
        verify(connectionRegistry).getUsersWithLogAccess();
        
        // Capture the JSON message sent to the WebSocket client
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        verify(webSocketSession.getRemote(), times(1)).sendString(messageCaptor.capture());
        
        // Parse and verify the JSON message
        String sentMessage = messageCaptor.getValue();
        JsonNode jsonMessage = objectMapper.readTree(sentMessage);
        
        // Verify message structure
        assertEquals("logs", jsonMessage.get("type").asText());
        assertNotNull(jsonMessage.get("data"));
        assertEquals(logRecord.getId().longValue(), jsonMessage.get("data").get("id").asLong());
        assertEquals(logRecord.getLevel(), jsonMessage.get("data").get("level").asText());
        assertEquals(logRecord.getMessage(), jsonMessage.get("data").get("message").asText());
        
        // Verify trace context was added
        assertNotNull(jsonMessage.get("traceContext"));
        assertEquals("7bf92f3577b34da6a3ce929d0e0e4736", jsonMessage.get("traceContext").get("traceId").asText());
    }

    @Test
    public void testMultipleUserDelivery() throws Exception {
        // Create a test device update
        Device device = new Device();
        device.setId(100L);
        device.setName("Shared Device");
        device.setUniqueId("shared-device-123");
        
        // Setup multiple users with access to the device
        List<Long> userIds = Arrays.asList(1L, 2L, 3L);
        when(connectionRegistry.getUsersForDevice(device.getId())).thenReturn(userIds);
        
        // Setup trace context
        Map<String, String> traceHeaders = new HashMap<>();
        traceHeaders.put("traceparent", "00-8bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");

        // Simulate message broker receiving a device update
        messageBrokerConsumer.onDeviceUpdate(device, traceHeaders);

        // Verify connection registry was queried for the device's owners
        verify(connectionRegistry).getUsersForDevice(device.getId());
        
        // Verify that the connection registry was queried for each user's connections
        for (Long userId : userIds) {
            verify(connectionRegistry).getUserConnections(userId);
        }
    }

    @Test
    public void testNoConnectionDelivery() throws Exception {
        // Create a test position update
        Position position = new Position();
        position.setId(200L);
        position.setDeviceId(100L);
        
        // Setup user with no active connections
        when(connectionRegistry.getUsersForDevice(position.getDeviceId())).thenReturn(Collections.singletonList(999L));
        when(connectionRegistry.getUserConnections(999L)).thenReturn(Collections.emptyList());
        
        // Setup trace context
        Map<String, String> traceHeaders = new HashMap<>();

        // Simulate message broker receiving a position update
        messageBrokerConsumer.onPositionUpdate(position, traceHeaders);

        // Verify connection registry was queried
        verify(connectionRegistry).getUsersForDevice(position.getDeviceId());
        verify(connectionRegistry).getUserConnections(999L);
        
        // Verify no WebSocket messages were sent
        verify(webSocketSession.getRemote(), times(0)).sendString(anyString());
    }

    @Test
    public void testPermissionFilteredDelivery() throws Exception {
        // Create a test event update
        Event event = new Event();
        event.setId(300L);
        event.setDeviceId(100L);
        
        // Setup trace context
        Map<String, String> traceHeaders = new HashMap<>();

        // Mock permission check to deny access
        when(connectionRegistry.getUsersForDevice(event.getDeviceId())).thenReturn(Collections.emptyList());

        // Simulate message broker receiving an event update
        messageBrokerConsumer.onEventUpdate(event, traceHeaders);

        // Verify connection registry was queried for permissions
        verify(connectionRegistry).getUsersForDevice(event.getDeviceId());
        
        // Verify no WebSocket messages were sent due to permission filtering
        verify(webSocketSession.getRemote(), times(0)).sendString(anyString());
    }

    /**
     * Creates a test SpanContext for trace propagation testing
     */
    private SpanContext createTestSpanContext() {
        return SpanContext.create(
            "4bf92f3577b34da6a3ce929d0e0e4736", // traceId
            "00f067aa0ba902b7", // spanId
            TraceFlags.getSampled(), // traceFlags
            TraceState.getDefault() // traceState
        );
    }

    /**
     * Interface representing the message broker consumer that would be implemented
     * in the API Gateway to consume updates from the message broker topics.
     */
    interface MessageBrokerConsumer {
        void onDeviceUpdate(Device device, Map<String, String> traceHeaders);
        void onPositionUpdate(Position position, Map<String, String> traceHeaders);
        void onEventUpdate(Event event, Map<String, String> traceHeaders);
        void onLogUpdate(LogRecord logRecord, Map<String, String> traceHeaders);
    }

    /**
     * Interface representing the connection registry that would be implemented
     * in the API Gateway to manage WebSocket connections.
     */
    interface ConnectionRegistry {
        List<Long> getUsersForDevice(long deviceId);
        List<Long> getUsersWithLogAccess();
        List<AsyncSocket> getUserConnections(long userId);
    }
}