package org.traccar.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

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
import org.traccar.model.User;
import org.traccar.session.ConnectionManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;

/**
 * Tests the message subscription and filtering functionality of the API Gateway's WebSocket implementation.
 * Verifies that clients are correctly subscribed to appropriate topics based on user permissions,
 * that updates are filtered according to access control rules, and that subscription changes are
 * properly handled.
 */
@ExtendWith(MockitoExtension.class)
public class WebSocketMessageSubscriptionTest {

    @Mock
    private ConnectionManager connectionManager;

    @Mock
    private Storage storage;

    @Mock
    private Session webSocketSession;

    @Mock
    private MessageBrokerClient messageBrokerClient;

    @Mock
    private PermissionService permissionService;

    @Mock
    private ConnectionRegistry connectionRegistry;

    private ObjectMapper objectMapper;
    private WebSocketSubscriptionManager subscriptionManager;
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
            
        // Create subscription manager with mocked dependencies
        subscriptionManager = new WebSocketSubscriptionManager(
            messageBrokerClient, permissionService, connectionRegistry);
    }

    @Test
    public void testInitialTopicSubscription() {
        // Setup test user with device permissions
        Set<Long> deviceIds = new HashSet<>(Arrays.asList(100L, 101L, 102L));
        when(permissionService.getAllowedDevices(testUserId)).thenReturn(deviceIds);
        
        // Initialize subscriptions for the test user
        subscriptionManager.initializeUserSubscriptions(testUserId);
        
        // Verify that the message broker client was called to subscribe to the appropriate topics
        verify(messageBrokerClient).subscribeToDeviceUpdates(eq(deviceIds), eq(testUserId));
        verify(messageBrokerClient).subscribeToPositionUpdates(eq(deviceIds), eq(testUserId));
        verify(messageBrokerClient).subscribeToEventUpdates(eq(deviceIds), eq(testUserId));
        
        // Verify that the log updates subscription is based on user permissions
        verify(permissionService).hasPermission(testUserId, "logs");
    }

    @Test
    public void testLogSubscriptionWithPermission() {
        // Setup test user with log permission
        when(permissionService.hasPermission(testUserId, "logs")).thenReturn(true);
        
        // Initialize subscriptions for the test user
        subscriptionManager.initializeUserSubscriptions(testUserId);
        
        // Verify that the message broker client was called to subscribe to log updates
        verify(messageBrokerClient).subscribeToLogUpdates(testUserId);
    }

    @Test
    public void testLogSubscriptionWithoutPermission() {
        // Setup test user without log permission
        when(permissionService.hasPermission(testUserId, "logs")).thenReturn(false);
        
        // Initialize subscriptions for the test user
        subscriptionManager.initializeUserSubscriptions(testUserId);
        
        // Verify that the message broker client was NOT called to subscribe to log updates
        verify(messageBrokerClient, never()).subscribeToLogUpdates(testUserId);
    }

    @Test
    public void testDynamicSubscriptionChangesOnPermissionUpdate() {
        // Setup initial permissions
        Set<Long> initialDeviceIds = new HashSet<>(Arrays.asList(100L, 101L));
        when(permissionService.getAllowedDevices(testUserId)).thenReturn(initialDeviceIds);
        
        // Initialize subscriptions
        subscriptionManager.initializeUserSubscriptions(testUserId);
        
        // Verify initial subscriptions
        verify(messageBrokerClient).subscribeToDeviceUpdates(eq(initialDeviceIds), eq(testUserId));
        verify(messageBrokerClient).subscribeToPositionUpdates(eq(initialDeviceIds), eq(testUserId));
        verify(messageBrokerClient).subscribeToEventUpdates(eq(initialDeviceIds), eq(testUserId));
        
        // Update permissions - add a new device
        Set<Long> updatedDeviceIds = new HashSet<>(Arrays.asList(100L, 101L, 102L));
        when(permissionService.getAllowedDevices(testUserId)).thenReturn(updatedDeviceIds);
        
        // Simulate permission change event
        subscriptionManager.onPermissionChange(testUserId);
        
        // Verify that subscriptions were updated for the new device only
        verify(messageBrokerClient).subscribeToDeviceUpdates(eq(Collections.singleton(102L)), eq(testUserId));
        verify(messageBrokerClient).subscribeToPositionUpdates(eq(Collections.singleton(102L)), eq(testUserId));
        verify(messageBrokerClient).subscribeToEventUpdates(eq(Collections.singleton(102L)), eq(testUserId));
    }

    @Test
    public void testUnsubscribeOnPermissionRemoval() {
        // Setup initial permissions
        Set<Long> initialDeviceIds = new HashSet<>(Arrays.asList(100L, 101L, 102L));
        when(permissionService.getAllowedDevices(testUserId)).thenReturn(initialDeviceIds);
        
        // Initialize subscriptions
        subscriptionManager.initializeUserSubscriptions(testUserId);
        
        // Update permissions - remove a device
        Set<Long> updatedDeviceIds = new HashSet<>(Arrays.asList(100L, 101L));
        when(permissionService.getAllowedDevices(testUserId)).thenReturn(updatedDeviceIds);
        
        // Simulate permission change event
        subscriptionManager.onPermissionChange(testUserId);
        
        // Verify that unsubscribe was called for the removed device
        verify(messageBrokerClient).unsubscribeFromDeviceUpdates(eq(Collections.singleton(102L)), eq(testUserId));
        verify(messageBrokerClient).unsubscribeFromPositionUpdates(eq(Collections.singleton(102L)), eq(testUserId));
        verify(messageBrokerClient).unsubscribeFromEventUpdates(eq(Collections.singleton(102L)), eq(testUserId));
    }

    @Test
    public void testUnsubscribeOnLogPermissionRemoval() {
        // Setup initial permissions with log access
        when(permissionService.hasPermission(testUserId, "logs")).thenReturn(true);
        
        // Initialize subscriptions
        subscriptionManager.initializeUserSubscriptions(testUserId);
        
        // Verify initial log subscription
        verify(messageBrokerClient).subscribeToLogUpdates(testUserId);
        
        // Update permissions - remove log access
        when(permissionService.hasPermission(testUserId, "logs")).thenReturn(false);
        
        // Simulate permission change event
        subscriptionManager.onPermissionChange(testUserId);
        
        // Verify that unsubscribe was called for logs
        verify(messageBrokerClient).unsubscribeFromLogUpdates(testUserId);
    }

    @Test
    public void testCorrelationIdPropagation() {
        // Setup test device and user permissions
        long deviceId = 100L;
        Set<Long> deviceIds = Collections.singleton(deviceId);
        when(permissionService.getAllowedDevices(testUserId)).thenReturn(deviceIds);
        
        // Initialize subscriptions
        subscriptionManager.initializeUserSubscriptions(testUserId);
        
        // Create a test position update with correlation ID
        Position position = new Position();
        position.setId(200L);
        position.setDeviceId(deviceId);
        
        // Setup correlation ID
        String correlationId = "4bf92f3577b34da6a3ce929d0e0e4736";
        Map<String, String> traceHeaders = new HashMap<>();
        traceHeaders.put("traceparent", "00-" + correlationId + "-00f067aa0ba902b7-01");
        
        // Simulate message broker receiving a position update
        subscriptionManager.onPositionUpdate(position, traceHeaders);
        
        // Verify that the update was delivered to the user's connection
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        verify(webSocketSession.getRemote()).sendString(messageCaptor.capture());
        
        // Parse and verify the JSON message
        try {
            String sentMessage = messageCaptor.getValue();
            JsonNode jsonMessage = objectMapper.readTree(sentMessage);
            
            // Verify correlation ID was propagated
            assertNotNull(jsonMessage.get("traceContext"));
            assertEquals(correlationId, jsonMessage.get("traceContext").get("traceId").asText());
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JSON message", e);
        }
    }

    @Test
    public void testPermissionBasedFiltering() {
        // Setup test user with limited device permissions
        Set<Long> allowedDeviceIds = Collections.singleton(100L);
        when(permissionService.getAllowedDevices(testUserId)).thenReturn(allowedDeviceIds);
        
        // Initialize subscriptions
        subscriptionManager.initializeUserSubscriptions(testUserId);
        
        // Create a test event for a device the user doesn't have permission for
        Event event = new Event();
        event.setId(300L);
        event.setDeviceId(999L); // Not in allowed devices
        
        // Simulate message broker receiving an event update
        subscriptionManager.onEventUpdate(event, Collections.emptyMap());
        
        // Verify that no update was sent to the user's connection
        verify(webSocketSession.getRemote(), never()).sendString(anyString());
    }

    @Test
    public void testMultiUserSubscription() {
        // Setup multiple test users
        long user1Id = 1L;
        long user2Id = 2L;
        
        // Setup device permissions for each user
        Set<Long> user1Devices = new HashSet<>(Arrays.asList(100L, 101L));
        Set<Long> user2Devices = new HashSet<>(Arrays.asList(101L, 102L));
        
        when(permissionService.getAllowedDevices(user1Id)).thenReturn(user1Devices);
        when(permissionService.getAllowedDevices(user2Id)).thenReturn(user2Devices);
        
        // Initialize subscriptions for both users
        subscriptionManager.initializeUserSubscriptions(user1Id);
        subscriptionManager.initializeUserSubscriptions(user2Id);
        
        // Verify that each user is subscribed to their allowed devices
        verify(messageBrokerClient).subscribeToDeviceUpdates(eq(user1Devices), eq(user1Id));
        verify(messageBrokerClient).subscribeToPositionUpdates(eq(user1Devices), eq(user1Id));
        verify(messageBrokerClient).subscribeToEventUpdates(eq(user1Devices), eq(user1Id));
        
        verify(messageBrokerClient).subscribeToDeviceUpdates(eq(user2Devices), eq(user2Id));
        verify(messageBrokerClient).subscribeToPositionUpdates(eq(user2Devices), eq(user2Id));
        verify(messageBrokerClient).subscribeToEventUpdates(eq(user2Devices), eq(user2Id));
    }

    @Test
    public void testCleanupOnDisconnect() {
        // Setup test user with device permissions
        Set<Long> deviceIds = new HashSet<>(Arrays.asList(100L, 101L));
        when(permissionService.getAllowedDevices(testUserId)).thenReturn(deviceIds);
        
        // Initialize subscriptions
        subscriptionManager.initializeUserSubscriptions(testUserId);
        
        // Simulate WebSocket disconnection
        subscriptionManager.onWebSocketDisconnect(testUserId);
        
        // Verify that all subscriptions were cleaned up
        verify(messageBrokerClient).unsubscribeFromAllTopics(testUserId);
    }

    /**
     * Interface representing the message broker client that would be implemented
     * in the API Gateway to manage topic subscriptions.
     */
    interface MessageBrokerClient {
        void subscribeToDeviceUpdates(Set<Long> deviceIds, long userId);
        void subscribeToPositionUpdates(Set<Long> deviceIds, long userId);
        void subscribeToEventUpdates(Set<Long> deviceIds, long userId);
        void subscribeToLogUpdates(long userId);
        
        void unsubscribeFromDeviceUpdates(Set<Long> deviceIds, long userId);
        void unsubscribeFromPositionUpdates(Set<Long> deviceIds, long userId);
        void unsubscribeFromEventUpdates(Set<Long> deviceIds, long userId);
        void unsubscribeFromLogUpdates(long userId);
        void unsubscribeFromAllTopics(long userId);
    }

    /**
     * Interface representing the permission service that would be implemented
     * in the API Gateway to check user permissions.
     */
    interface PermissionService {
        Set<Long> getAllowedDevices(long userId);
        boolean hasPermission(long userId, String permission);
    }

    /**
     * Interface representing the connection registry that would be implemented
     * in the API Gateway to manage WebSocket connections.
     */
    interface ConnectionRegistry {
        List<AsyncSocket> getUserConnections(long userId);
    }

    /**
     * Class representing the WebSocket subscription manager that would be implemented
     * in the API Gateway to manage topic subscriptions.
     */
    class WebSocketSubscriptionManager {
        private final MessageBrokerClient messageBrokerClient;
        private final PermissionService permissionService;
        private final ConnectionRegistry connectionRegistry;
        
        // Store current subscriptions for efficient updates
        private final Map<Long, Set<Long>> userDeviceSubscriptions = new HashMap<>();
        private final Set<Long> userLogSubscriptions = new HashSet<>();
        
        public WebSocketSubscriptionManager(
                MessageBrokerClient messageBrokerClient,
                PermissionService permissionService,
                ConnectionRegistry connectionRegistry) {
            this.messageBrokerClient = messageBrokerClient;
            this.permissionService = permissionService;
            this.connectionRegistry = connectionRegistry;
        }
        
        public void initializeUserSubscriptions(long userId) {
            // Get allowed devices for the user
            Set<Long> allowedDevices = permissionService.getAllowedDevices(userId);
            
            // Subscribe to device-related topics
            messageBrokerClient.subscribeToDeviceUpdates(allowedDevices, userId);
            messageBrokerClient.subscribeToPositionUpdates(allowedDevices, userId);
            messageBrokerClient.subscribeToEventUpdates(allowedDevices, userId);
            
            // Store current subscriptions
            userDeviceSubscriptions.put(userId, new HashSet<>(allowedDevices));
            
            // Check log permission and subscribe if allowed
            if (permissionService.hasPermission(userId, "logs")) {
                messageBrokerClient.subscribeToLogUpdates(userId);
                userLogSubscriptions.add(userId);
            }
        }
        
        public void onPermissionChange(long userId) {
            // Get current and new allowed devices
            Set<Long> currentDevices = userDeviceSubscriptions.getOrDefault(userId, new HashSet<>());
            Set<Long> newAllowedDevices = permissionService.getAllowedDevices(userId);
            
            // Find devices to add and remove
            Set<Long> devicesToAdd = new HashSet<>(newAllowedDevices);
            devicesToAdd.removeAll(currentDevices);
            
            Set<Long> devicesToRemove = new HashSet<>(currentDevices);
            devicesToRemove.removeAll(newAllowedDevices);
            
            // Update device subscriptions
            if (!devicesToAdd.isEmpty()) {
                messageBrokerClient.subscribeToDeviceUpdates(devicesToAdd, userId);
                messageBrokerClient.subscribeToPositionUpdates(devicesToAdd, userId);
                messageBrokerClient.subscribeToEventUpdates(devicesToAdd, userId);
            }
            
            if (!devicesToRemove.isEmpty()) {
                messageBrokerClient.unsubscribeFromDeviceUpdates(devicesToRemove, userId);
                messageBrokerClient.unsubscribeFromPositionUpdates(devicesToRemove, userId);
                messageBrokerClient.unsubscribeFromEventUpdates(devicesToRemove, userId);
            }
            
            // Update stored subscriptions
            userDeviceSubscriptions.put(userId, newAllowedDevices);
            
            // Check log permission changes
            boolean hasLogPermission = permissionService.hasPermission(userId, "logs");
            boolean hadLogPermission = userLogSubscriptions.contains(userId);
            
            if (hasLogPermission && !hadLogPermission) {
                messageBrokerClient.subscribeToLogUpdates(userId);
                userLogSubscriptions.add(userId);
            } else if (!hasLogPermission && hadLogPermission) {
                messageBrokerClient.unsubscribeFromLogUpdates(userId);
                userLogSubscriptions.remove(userId);
            }
        }
        
        public void onWebSocketDisconnect(long userId) {
            // Clean up all subscriptions
            messageBrokerClient.unsubscribeFromAllTopics(userId);
            
            // Remove from tracking maps
            userDeviceSubscriptions.remove(userId);
            userLogSubscriptions.remove(userId);
        }
        
        // Methods to handle updates from the message broker
        
        public void onDeviceUpdate(Device device, Map<String, String> traceHeaders) {
            deliverUpdate("devices", device, device.getId(), traceHeaders);
        }
        
        public void onPositionUpdate(Position position, Map<String, String> traceHeaders) {
            deliverUpdate("positions", position, position.getDeviceId(), traceHeaders);
        }
        
        public void onEventUpdate(Event event, Map<String, String> traceHeaders) {
            deliverUpdate("events", event, event.getDeviceId(), traceHeaders);
        }
        
        public void onLogUpdate(LogRecord logRecord, Map<String, String> traceHeaders) {
            // Deliver to all users with log permission
            for (Long userId : userLogSubscriptions) {
                List<AsyncSocket> connections = connectionRegistry.getUserConnections(userId);
                for (AsyncSocket connection : connections) {
                    // In a real implementation, we would format the message and include trace context
                    // For the test, we'll just simulate the delivery
                    if (connection.isConnected()) {
                        try {
                            Map<String, Object> message = new HashMap<>();
                            message.put("type", "logs");
                            message.put("data", logRecord);
                            
                            // Extract and add trace context
                            if (traceHeaders != null && !traceHeaders.isEmpty()) {
                                String traceparent = traceHeaders.get("traceparent");
                                if (traceparent != null) {
                                    String traceId = traceparent.split("-")[1];
                                    Map<String, String> traceContext = new HashMap<>();
                                    traceContext.put("traceId", traceId);
                                    message.put("traceContext", traceContext);
                                }
                            }
                            
                            connection.getRemote().sendString(objectMapper.writeValueAsString(message));
                        } catch (Exception e) {
                            // Log error in real implementation
                        }
                    }
                }
            }
        }
        
        private void deliverUpdate(String type, Object data, long deviceId, Map<String, String> traceHeaders) {
            // Find all users subscribed to this device
            for (Map.Entry<Long, Set<Long>> entry : userDeviceSubscriptions.entrySet()) {
                long userId = entry.getKey();
                Set<Long> subscribedDevices = entry.getValue();
                
                // Check if user is subscribed to this device
                if (subscribedDevices.contains(deviceId)) {
                    // Deliver update to all connections for this user
                    List<AsyncSocket> connections = connectionRegistry.getUserConnections(userId);
                    for (AsyncSocket connection : connections) {
                        if (connection.isConnected()) {
                            try {
                                Map<String, Object> message = new HashMap<>();
                                message.put("type", type);
                                message.put("data", data);
                                
                                // Extract and add trace context
                                if (traceHeaders != null && !traceHeaders.isEmpty()) {
                                    String traceparent = traceHeaders.get("traceparent");
                                    if (traceparent != null) {
                                        String traceId = traceparent.split("-")[1];
                                        Map<String, String> traceContext = new HashMap<>();
                                        traceContext.put("traceId", traceId);
                                        message.put("traceContext", traceContext);
                                    }
                                }
                                
                                connection.getRemote().sendString(objectMapper.writeValueAsString(message));
                            } catch (Exception e) {
                                // Log error in real implementation
                            }
                        }
                    }
                }
            }
        }
    }
}