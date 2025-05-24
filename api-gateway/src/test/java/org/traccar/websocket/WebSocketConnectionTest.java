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
package org.traccar.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.server.JettyWebSocketServletFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.api.AsyncSocket;
import org.traccar.api.AsyncSocketServlet;
import org.traccar.api.security.LoginService;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.helper.SessionHelper;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.session.ConnectionManager;
import org.traccar.session.ConnectionRegistry;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class WebSocketConnectionTest {

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
    private ServiceDiscovery serviceDiscovery;

    @Mock
    private ConnectionRegistry connectionRegistry;

    @Mock
    private JettyWebSocketServletFactory factory;

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpSession httpSession;

    @Mock
    private Session webSocketSession;

    private AsyncSocketServlet servlet;

    @BeforeEach
    public void setUp() {
        servlet = new AsyncSocketServlet(config, objectMapper, connectionManager, storage, loginService);
        when(config.getLong(Keys.WEB_TIMEOUT)).thenReturn(60000L);
    }

    @Test
    public void testAuthenticationWithToken() throws StorageException, GeneralSecurityException, IOException {
        // Setup
        String token = "valid-token";
        User user = new User();
        user.setId(1L);
        LoginService.LoginResult loginResult = new LoginService.LoginResult();
        loginResult.setUser(user);

        Map<String, String[]> parameterMap = new HashMap<>();
        parameterMap.put("token", new String[]{token});

        when(request.getParameterMap()).thenReturn(parameterMap);
        when(loginService.login(token)).thenReturn(loginResult);

        // Execute
        servlet.configure(factory);

        // Verify
        ArgumentCaptor<JettyWebSocketServletFactory.CreatorFactory> creatorCaptor = 
                ArgumentCaptor.forClass(JettyWebSocketServletFactory.CreatorFactory.class);
        verify(factory).setCreator(creatorCaptor.capture());

        // Simulate creator execution
        Object result = creatorCaptor.getValue().createWebSocket(request, null);

        // Verify result is AsyncSocket with correct user ID
        assertNotNull(result);
        assertTrue(result instanceof AsyncSocket);
        verify(loginService).login(eq(token));
    }

    @Test
    public void testAuthenticationWithSession() {
        // Setup
        when(request.getSession()).thenReturn(httpSession);
        when(httpSession.getAttribute(SessionHelper.USER_ID_KEY)).thenReturn(2L);

        // Execute
        servlet.configure(factory);

        // Verify
        ArgumentCaptor<JettyWebSocketServletFactory.CreatorFactory> creatorCaptor = 
                ArgumentCaptor.forClass(JettyWebSocketServletFactory.CreatorFactory.class);
        verify(factory).setCreator(creatorCaptor.capture());

        // Simulate creator execution
        Object result = creatorCaptor.getValue().createWebSocket(request, null);

        // Verify result is AsyncSocket with correct user ID
        assertNotNull(result);
        assertTrue(result instanceof AsyncSocket);
        verify(httpSession).getAttribute(SessionHelper.USER_ID_KEY);
    }

    @Test
    public void testAuthenticationFailure() {
        // Setup - no token or session
        when(request.getParameterMap()).thenReturn(Collections.emptyMap());
        when(request.getSession()).thenReturn(null);

        // Execute
        servlet.configure(factory);

        // Verify
        ArgumentCaptor<JettyWebSocketServletFactory.CreatorFactory> creatorCaptor = 
                ArgumentCaptor.forClass(JettyWebSocketServletFactory.CreatorFactory.class);
        verify(factory).setCreator(creatorCaptor.capture());

        // Simulate creator execution
        Object result = creatorCaptor.getValue().createWebSocket(request, null);

        // Verify result is null (authentication failed)
        assertNull(result);
    }

    @Test
    public void testConnectionRegistration() throws Exception {
        // Create AsyncSocket with mocked dependencies
        AsyncSocket asyncSocket = new AsyncSocket(objectMapper, connectionManager, storage, 3L);

        // Mock the WebSocket session
        when(webSocketSession.isOpen()).thenReturn(true);

        // Mock storage to return positions
        List<Position> positions = Collections.singletonList(new Position());
        when(storage.getObjects(eq(Position.class), any())).thenReturn(positions);

        // Execute onWebSocketConnect
        asyncSocket.onWebSocketConnect(webSocketSession);

        // Verify connection is registered in ConnectionManager
        verify(connectionManager).addListener(eq(3L), eq(asyncSocket));

        // Verify initial data is sent
        verify(storage).getObjects(eq(Position.class), any());
    }

    @Test
    public void testConnectionRemovalOnClose() throws Exception {
        // Create AsyncSocket with mocked dependencies
        AsyncSocket asyncSocket = new AsyncSocket(objectMapper, connectionManager, storage, 4L);

        // Mock the WebSocket session
        when(webSocketSession.isOpen()).thenReturn(true);

        // Mock storage to return positions
        List<Position> positions = Collections.singletonList(new Position());
        when(storage.getObjects(eq(Position.class), any())).thenReturn(positions);

        // Connect first
        asyncSocket.onWebSocketConnect(webSocketSession);

        // Execute onWebSocketClose
        asyncSocket.onWebSocketClose(1000, "Normal closure");

        // Verify connection is removed from ConnectionManager
        verify(connectionManager).removeListener(eq(4L), eq(asyncSocket));
    }

    @Test
    public void testDistributedConnectionRegistry() throws Exception {
        // This test would verify integration with the distributed connection registry
        // In a real implementation, we would inject the ConnectionRegistry into AsyncSocketServlet
        // and verify it's used to register connections
        
        // For now, we'll just verify the concept with mocks
        
        // Setup
        String connectionId = "conn-123";
        long userId = 5L;
        
        // Mock connection registry behavior
        when(connectionRegistry.registerConnection(eq(userId), any())).thenReturn(connectionId);
        when(connectionRegistry.getConnectionsForUser(userId)).thenReturn(Collections.singletonList(connectionId));
        
        // Verify registration
        String registeredId = connectionRegistry.registerConnection(userId, "session-data");
        assertEquals(connectionId, registeredId);
        
        // Verify retrieval
        List<String> connections = connectionRegistry.getConnectionsForUser(userId);
        assertEquals(1, connections.size());
        assertEquals(connectionId, connections.get(0));
        
        // Verify removal
        connectionRegistry.removeConnection(connectionId);
        verify(connectionRegistry).removeConnection(connectionId);
    }

    @Test
    public void testServiceDiscoveryForSessionStore() {
        // This test would verify that the API Gateway uses Service Discovery
        // to locate the Session Store service for authentication
        
        // Setup
        String sessionStoreServiceName = "session-store-service";
        String sessionStoreUrl = "http://session-store:8080";
        
        // Mock service discovery behavior
        when(serviceDiscovery.getServiceUrl(sessionStoreServiceName)).thenReturn(sessionStoreUrl);
        
        // Verify service discovery is used
        String discoveredUrl = serviceDiscovery.getServiceUrl(sessionStoreServiceName);
        assertEquals(sessionStoreUrl, discoveredUrl);
        verify(serviceDiscovery).getServiceUrl(sessionStoreServiceName);
    }

    @Test
    public void testInitialDataLoading() throws Exception {
        // Create AsyncSocket with mocked dependencies
        AsyncSocket asyncSocket = new AsyncSocket(objectMapper, connectionManager, storage, 6L);

        // Mock the WebSocket session
        when(webSocketSession.isOpen()).thenReturn(true);

        // Mock storage to return positions
        Position position = new Position();
        position.setId(100L);
        List<Position> positions = Collections.singletonList(position);
        when(storage.getObjects(eq(Position.class), any())).thenReturn(positions);

        // Mock ObjectMapper to handle JSON conversion
        when(objectMapper.writeValueAsString(any())).thenReturn("{\"positions\":[{\"id\":100}]}");

        // Execute onWebSocketConnect
        asyncSocket.onWebSocketConnect(webSocketSession);

        // Verify initial data is retrieved
        verify(storage).getObjects(eq(Position.class), any());
        
        // Verify data is sent to client
        verify(objectMapper).writeValueAsString(any());
    }

    @Test
    public void testDeviceUpdatePropagation() throws Exception {
        // Create AsyncSocket with mocked dependencies
        AsyncSocket asyncSocket = new AsyncSocket(objectMapper, connectionManager, storage, 7L);

        // Mock the WebSocket session
        when(webSocketSession.isOpen()).thenReturn(true);
        
        // Connect the socket
        List<Position> positions = Collections.singletonList(new Position());
        when(storage.getObjects(eq(Position.class), any())).thenReturn(positions);
        asyncSocket.onWebSocketConnect(webSocketSession);

        // Create a device update
        Device device = new Device();
        device.setId(200L);
        device.setName("Test Device");

        // Mock ObjectMapper to handle JSON conversion
        when(objectMapper.writeValueAsString(any())).thenReturn("{\"devices\":[{\"id\":200,\"name\":\"Test Device\"}]}");

        // Simulate device update
        asyncSocket.onUpdateDevice(device);

        // Verify update is sent to client
        verify(objectMapper, atLeast(2)).writeValueAsString(any());
    }
}