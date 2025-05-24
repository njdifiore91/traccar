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
package org.traccar.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.eclipse.jetty.websocket.server.JettyWebSocketServletFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.traccar.ApiGatewayWebSocketConfig;
import org.traccar.api.security.LoginService;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.LogRecord;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.session.ConnectionManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for WebSocket connection handling in the API Gateway.
 * 
 * This test class verifies that the API Gateway correctly manages WebSocket connections
 * for delivering position updates, events, and other real-time data to clients, with proper
 * authentication and permission checking for subscriptions.
 */
@ExtendWith(MockitoExtension.class)
public class WebSocketHandlerTest {

    @Mock
    private Config config;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private ConnectionManager connectionManager;

    @Mock
    private Storage storage;

    @Mock
    private LoginService loginService;

    @Mock
    private RedisConnectionFactory redisConnectionFactory;

    @Mock
    private RedisTemplate<String, Object> redisTemplate;

    @Mock
    private HashOperations<String, Object, Object> hashOperations;

    private ApiGatewayWebSocketConfig webSocketConfig;
    private AsyncSocketServlet socketServlet;

    @BeforeEach
    public void setUp() {
        when(config.getLong(Keys.WEB_TIMEOUT)).thenReturn(60000L);
        when(redisTemplate.opsForHash()).thenReturn(hashOperations);

        socketServlet = new AsyncSocketServlet(config, objectMapper, connectionManager, storage, loginService);
        webSocketConfig = new ApiGatewayWebSocketConfig();
        
        // Set dependencies using reflection
        try {
            java.lang.reflect.Field configField = ApiGatewayWebSocketConfig.class.getDeclaredField("config");
            configField.setAccessible(true);
            configField.set(webSocketConfig, config);
            
            java.lang.reflect.Field objectMapperField = ApiGatewayWebSocketConfig.class.getDeclaredField("objectMapper");
            objectMapperField.setAccessible(true);
            objectMapperField.set(webSocketConfig, objectMapper);
            
            java.lang.reflect.Field connectionManagerField = ApiGatewayWebSocketConfig.class.getDeclaredField("connectionManager");
            connectionManagerField.setAccessible(true);
            connectionManagerField.set(webSocketConfig, connectionManager);
            
            java.lang.reflect.Field storageField = ApiGatewayWebSocketConfig.class.getDeclaredField("storage");
            storageField.setAccessible(true);
            storageField.set(webSocketConfig, storage);
            
            java.lang.reflect.Field loginServiceField = ApiGatewayWebSocketConfig.class.getDeclaredField("loginService");
            loginServiceField.setAccessible(true);
            loginServiceField.set(webSocketConfig, loginService);
            
            java.lang.reflect.Field redisConnectionFactoryField = ApiGatewayWebSocketConfig.class.getDeclaredField("redisConnectionFactory");
            redisConnectionFactoryField.setAccessible(true);
            redisConnectionFactoryField.set(webSocketConfig, redisConnectionFactory);
            
            java.lang.reflect.Field redisTemplateField = ApiGatewayWebSocketConfig.class.getDeclaredField("webSocketRedisTemplate");
            redisTemplateField.setAccessible(true);
            redisTemplateField.set(webSocketConfig, redisTemplate);
            
            java.lang.reflect.Field idleTimeoutField = ApiGatewayWebSocketConfig.class.getDeclaredField("idleTimeout");
            idleTimeoutField.setAccessible(true);
            idleTimeoutField.set(webSocketConfig, 300000L);
            
            java.lang.reflect.Field maxConnectionsField = ApiGatewayWebSocketConfig.class.getDeclaredField("maxConnections");
            maxConnectionsField.setAccessible(true);
            maxConnectionsField.set(webSocketConfig, 10000);
            
            java.lang.reflect.Field heartbeatIntervalField = ApiGatewayWebSocketConfig.class.getDeclaredField("heartbeatInterval");
            heartbeatIntervalField.setAccessible(true);
            heartbeatIntervalField.set(webSocketConfig, 55000L);
            
            java.lang.reflect.Field localConnectionsField = ApiGatewayWebSocketConfig.class.getDeclaredField("localConnections");
            localConnectionsField.setAccessible(true);
            localConnectionsField.set(webSocketConfig, new HashMap<String, AsyncSocket>());
        } catch (Exception e) {
            throw new RuntimeException("Failed to set up test dependencies", e);
        }
    }

    @Test
    public void testWebSocketConnectionEstablishment() throws Exception {
        // Mock HTTP session and request
        HttpSession httpSession = mock(HttpSession.class);
        when(httpSession.getAttribute("userId")).thenReturn(1L);
        
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getSession()).thenReturn(httpSession);
        when(request.getParameterMap()).thenReturn(Collections.emptyMap());
        
        // Mock WebSocket factory
        JettyWebSocketServletFactory factory = mock(JettyWebSocketServletFactory.class);
        socketServlet.configure(factory);
        
        // Verify factory configuration
        verify(factory).setIdleTimeout(Duration.ofMillis(60000L));
        
        // Capture the WebSocket creator
        ArgumentCaptor<org.eclipse.jetty.websocket.server.JettyWebSocketCreator> creatorCaptor = 
                ArgumentCaptor.forClass(org.eclipse.jetty.websocket.server.JettyWebSocketCreator.class);
        verify(factory).setCreator(creatorCaptor.capture());
        
        // Create a WebSocket instance
        Object webSocket = creatorCaptor.getValue().createWebSocket(request, mock(HttpServletResponse.class));
        
        // Verify the WebSocket instance
        assertNotNull(webSocket);
        assertTrue(webSocket instanceof AsyncSocket);
    }

    @Test
    public void testWebSocketAuthenticationWithToken() throws Exception {
        // Mock token authentication
        Map<String, List<String>> parameterMap = new HashMap<>();
        parameterMap.put("token", List.of("valid-token"));
        
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameterMap()).thenReturn(parameterMap);
        
        // Mock login service
        User user = new User();
        user.setId(1L);
        LoginService.LoginResult loginResult = new LoginService.LoginResult();
        loginResult.setUser(user);
        when(loginService.login("valid-token")).thenReturn(loginResult);
        
        // Mock WebSocket factory
        JettyWebSocketServletFactory factory = mock(JettyWebSocketServletFactory.class);
        socketServlet.configure(factory);
        
        // Capture the WebSocket creator
        ArgumentCaptor<org.eclipse.jetty.websocket.server.JettyWebSocketCreator> creatorCaptor = 
                ArgumentCaptor.forClass(org.eclipse.jetty.websocket.server.JettyWebSocketCreator.class);
        verify(factory).setCreator(creatorCaptor.capture());
        
        // Create a WebSocket instance
        Object webSocket = creatorCaptor.getValue().createWebSocket(request, mock(HttpServletResponse.class));
        
        // Verify the WebSocket instance
        assertNotNull(webSocket);
        assertTrue(webSocket instanceof AsyncSocket);
        
        // Verify login service was called
        verify(loginService).login("valid-token");
    }

    @Test
    public void testWebSocketConnectionRejectionWithInvalidToken() throws Exception {
        // Mock token authentication with invalid token
        Map<String, List<String>> parameterMap = new HashMap<>();
        parameterMap.put("token", List.of("invalid-token"));
        
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameterMap()).thenReturn(parameterMap);
        when(request.getSession()).thenReturn(null);
        
        // Mock login service to throw exception for invalid token
        when(loginService.login("invalid-token")).thenThrow(new GeneralSecurityException("Invalid token"));
        
        // Mock WebSocket factory
        JettyWebSocketServletFactory factory = mock(JettyWebSocketServletFactory.class);
        socketServlet.configure(factory);
        
        // Capture the WebSocket creator
        ArgumentCaptor<org.eclipse.jetty.websocket.server.JettyWebSocketCreator> creatorCaptor = 
                ArgumentCaptor.forClass(org.eclipse.jetty.websocket.server.JettyWebSocketCreator.class);
        verify(factory).setCreator(creatorCaptor.capture());
        
        // Expect exception when creating WebSocket
        try {
            creatorCaptor.getValue().createWebSocket(request, mock(HttpServletResponse.class));
        } catch (RuntimeException e) {
            // Expected exception
            assertTrue(e.getCause() instanceof GeneralSecurityException);
        }
        
        // Verify login service was called
        verify(loginService).login("invalid-token");
    }

    @Test
    public void testWebSocketMessageMultiplexing() throws Exception {
        // Create AsyncSocket instance
        AsyncSocket asyncSocket = new AsyncSocket(objectMapper, connectionManager, storage, 1L);
        
        // Mock WebSocket session
        Session session = mock(Session.class);
        org.eclipse.jetty.websocket.api.RemoteEndpoint remoteEndpoint = mock(org.eclipse.jetty.websocket.api.RemoteEndpoint.class);
        when(session.getRemote()).thenReturn(remoteEndpoint);
        when(session.isOpen()).thenReturn(true);
        
        // Set session using reflection
        java.lang.reflect.Field sessionField = WebSocketAdapter.class.getDeclaredField("session");
        sessionField.setAccessible(true);
        sessionField.set(asyncSocket, session);
        
        // Mock storage for initial position loading
        when(storage.getObjects(eq(Position.class), any(Request.class))).thenReturn(Collections.emptyList());
        
        // Connect the WebSocket
        asyncSocket.onWebSocketConnect(session);
        
        // Verify connection manager listener was added
        verify(connectionManager).addListener(eq(1L), eq(asyncSocket));
        
        // Verify initial positions were sent
        verify(objectMapper).writeValueAsString(any(Map.class));
        verify(remoteEndpoint).sendString(anyString(), any());
        
        // Test position update
        Position position = new Position();
        position.setDeviceId(1L);
        asyncSocket.onUpdatePosition(position);
        
        // Verify position update was sent
        verify(objectMapper, times(2)).writeValueAsString(any(Map.class));
        verify(remoteEndpoint, times(2)).sendString(anyString(), any());
        
        // Test event update
        Event event = new Event("test", 1L);
        asyncSocket.onUpdateEvent(event);
        
        // Verify event update was sent
        verify(objectMapper, times(3)).writeValueAsString(any(Map.class));
        verify(remoteEndpoint, times(3)).sendString(anyString(), any());
        
        // Test device update
        Device device = new Device();
        device.setId(1L);
        asyncSocket.onUpdateDevice(device);
        
        // Verify device update was sent
        verify(objectMapper, times(4)).writeValueAsString(any(Map.class));
        verify(remoteEndpoint, times(4)).sendString(anyString(), any());
        
        // Test log update with logs disabled
        LogRecord logRecord = new LogRecord();
        asyncSocket.onUpdateLog(logRecord);
        
        // Verify no additional message was sent (logs disabled by default)
        verify(objectMapper, times(4)).writeValueAsString(any(Map.class));
        verify(remoteEndpoint, times(4)).sendString(anyString(), any());
        
        // Enable logs
        when(objectMapper.readTree(anyString())).thenReturn(com.fasterxml.jackson.databind.JsonNodeFactory.instance.objectNode().put("logs", true));
        asyncSocket.onWebSocketText("{\"logs\": true}");
        
        // Test log update with logs enabled
        asyncSocket.onUpdateLog(logRecord);
        
        // Verify log update was sent
        verify(objectMapper, times(5)).writeValueAsString(any(Map.class));
        verify(remoteEndpoint, times(5)).sendString(anyString(), any());
        
        // Test WebSocket close
        asyncSocket.onWebSocketClose(1000, "Normal closure");
        
        // Verify connection manager listener was removed
        verify(connectionManager).removeListener(eq(1L), eq(asyncSocket));
    }

    @Test
    public void testTopicSubscriptionBasedOnPermissions() throws StorageException {
        // Create AsyncSocket instance
        AsyncSocket asyncSocket = new AsyncSocket(objectMapper, connectionManager, storage, 1L);
        
        // Mock WebSocket session
        Session session = mock(Session.class);
        org.eclipse.jetty.websocket.api.RemoteEndpoint remoteEndpoint = mock(org.eclipse.jetty.websocket.api.RemoteEndpoint.class);
        when(session.getRemote()).thenReturn(remoteEndpoint);
        when(session.isOpen()).thenReturn(true);
        
        // Set session using reflection
        try {
            java.lang.reflect.Field sessionField = WebSocketAdapter.class.getDeclaredField("session");
            sessionField.setAccessible(true);
            sessionField.set(asyncSocket, session);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        // Mock storage for initial position loading
        when(storage.getObjects(eq(Position.class), any(Request.class))).thenReturn(Collections.emptyList());
        
        // Mock device permissions
        Device device1 = new Device();
        device1.setId(1L);
        Device device2 = new Device();
        device2.setId(2L);
        List<Device> devices = List.of(device1, device2);
        
        when(storage.getObjects(eq(Device.class), any(Request.class))).thenReturn(devices);
        
        // Capture the listener registration
        doAnswer(invocation -> {
            long userId = invocation.getArgument(0);
            ConnectionManager.UpdateListener listener = invocation.getArgument(1);
            
            // Simulate the behavior of ConnectionManager.addListener
            // by populating userDevices and deviceUsers maps
            Set<Long> userDeviceIds = new HashSet<>();
            userDeviceIds.add(1L);
            userDeviceIds.add(2L);
            
            // Use reflection to access private fields in ConnectionManager
            java.lang.reflect.Field userDevicesField = ConnectionManager.class.getDeclaredField("userDevices");
            userDevicesField.setAccessible(true);
            Map<Long, Set<Long>> userDevices = (Map<Long, Set<Long>>) userDevicesField.get(connectionManager);
            userDevices.put(userId, userDeviceIds);
            
            java.lang.reflect.Field deviceUsersField = ConnectionManager.class.getDeclaredField("deviceUsers");
            deviceUsersField.setAccessible(true);
            Map<Long, Set<Long>> deviceUsers = (Map<Long, Set<Long>>) deviceUsersField.get(connectionManager);
            
            Set<Long> device1Users = new HashSet<>();
            device1Users.add(userId);
            deviceUsers.put(1L, device1Users);
            
            Set<Long> device2Users = new HashSet<>();
            device2Users.add(userId);
            deviceUsers.put(2L, device2Users);
            
            return null;
        }).when(connectionManager).addListener(anyLong(), any(ConnectionManager.UpdateListener.class));
        
        // Connect the WebSocket
        asyncSocket.onWebSocketConnect(session);
        
        // Verify connection manager listener was added
        verify(connectionManager).addListener(eq(1L), eq(asyncSocket));
        
        // Test position update for device with permission
        Position position1 = new Position();
        position1.setDeviceId(1L);
        asyncSocket.onUpdatePosition(position1);
        
        // Verify position update was sent
        verify(objectMapper, times(2)).writeValueAsString(any(Map.class));
        
        // Test position update for device without permission
        Position position3 = new Position();
        position3.setDeviceId(3L); // User doesn't have permission for device 3
        asyncSocket.onUpdatePosition(position3);
        
        // Verify no additional message was sent (no permission)
        verify(objectMapper, times(2)).writeValueAsString(any(Map.class));
    }

    @Test
    public void testConnectionHeartbeatAndReconnection() throws Exception {
        // Create AsyncSocket instance
        AsyncSocket asyncSocket = Mockito.spy(new AsyncSocket(objectMapper, connectionManager, storage, 1L));
        
        // Mock WebSocket session
        Session session = mock(Session.class);
        org.eclipse.jetty.websocket.api.RemoteEndpoint remoteEndpoint = mock(org.eclipse.jetty.websocket.api.RemoteEndpoint.class);
        when(session.getRemote()).thenReturn(remoteEndpoint);
        when(session.isOpen()).thenReturn(true);
        
        // Set session using reflection
        java.lang.reflect.Field sessionField = WebSocketAdapter.class.getDeclaredField("session");
        sessionField.setAccessible(true);
        sessionField.set(asyncSocket, session);
        
        // Mock storage for initial position loading
        when(storage.getObjects(eq(Position.class), any(Request.class))).thenReturn(Collections.emptyList());
        
        // Connect the WebSocket
        asyncSocket.onWebSocketConnect(session);
        
        // Add the connection to the local connections map
        Map<String, AsyncSocket> localConnections = new HashMap<>();
        String connectionId = "test-connection-id";
        localConnections.put(connectionId, asyncSocket);
        
        // Set local connections using reflection
        java.lang.reflect.Field localConnectionsField = ApiGatewayWebSocketConfig.class.getDeclaredField("localConnections");
        localConnectionsField.setAccessible(true);
        localConnectionsField.set(webSocketConfig, localConnections);
        
        // Test heartbeat
        webSocketConfig.sendHeartbeat();
        
        // Verify heartbeat was sent
        verify(asyncSocket).onKeepalive();
        verify(redisTemplate).convertAndSend(eq("websocket:heartbeat"), any(Map.class));
        verify(redisTemplate.opsForHash()).put(anyString(), eq("lastActivity"), any(Long.class));
        
        // Test connection closure detection
        when(session.isOpen()).thenReturn(false);
        when(asyncSocket.isConnected()).thenReturn(false);
        
        // Send another heartbeat
        webSocketConfig.sendHeartbeat();
        
        // Verify connection was removed
        verify(redisTemplate).delete(anyString());
        assertTrue(localConnections.isEmpty());
        
        // Test cleanup of stale connections
        when(session.isOpen()).thenReturn(true);
        when(asyncSocket.isConnected()).thenReturn(true);
        localConnections.put(connectionId, asyncSocket);
        
        webSocketConfig.cleanupStaleConnections();
        
        // Verify stale connections were checked
        assertEquals(1, localConnections.size());
        
        // Simulate connection becoming stale
        when(asyncSocket.isConnected()).thenReturn(false);
        
        webSocketConfig.cleanupStaleConnections();
        
        // Verify stale connection was removed
        assertTrue(localConnections.isEmpty());
    }

    /**
     * Helper class to extend WebSocketAdapter for testing
     */
    private static class WebSocketAdapter implements WebSocketListener {
        private Session session;
        
        @Override
        public void onWebSocketBinary(byte[] payload, int offset, int length) {
        }
        
        @Override
        public void onWebSocketText(String message) {
        }
        
        @Override
        public void onWebSocketClose(int statusCode, String reason) {
        }
        
        @Override
        public void onWebSocketConnect(Session session) {
            this.session = session;
        }
        
        @Override
        public void onWebSocketError(Throwable cause) {
        }
        
        public boolean isConnected() {
            return session != null && session.isOpen();
        }
    }
}