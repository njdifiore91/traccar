package org.traccar;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.traccar.api.security.SecurityUser;
import org.traccar.config.Config;
import org.traccar.discovery.ServiceDiscoveryClient;
import org.traccar.messaging.MessageBroker;
import org.traccar.model.User;
import org.traccar.service.PositionService;
import org.traccar.service.DeviceService;
import org.traccar.service.EventService;
import org.traccar.service.NotificationService;
import org.traccar.service.ReportService;
import org.traccar.service.UserService;
import org.traccar.web.WebSocketManager;

import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Base test class for API Gateway tests that provides common test utilities,
 * mock setup, and dependency injection. It centralizes the creation of mock objects
 * for service clients, message broker interactions, and authentication components.
 * 
 * This class is extended by all API Gateway test classes to ensure consistent
 * test environment setup and reduce code duplication.
 */
@ExtendWith({MockitoExtension.class, SpringExtension.class})
public class BaseTest {

    @Mock
    protected Config config;
    
    @Mock
    protected ServiceDiscoveryClient serviceDiscoveryClient;

    @Mock
    protected MessageBroker messageBroker;
    
    @Mock
    protected PositionService positionService;
    
    @Mock
    protected DeviceService deviceService;
    
    @Mock
    protected EventService eventService;
    
    @Mock
    protected NotificationService notificationService;
    
    @Mock
    protected ReportService reportService;
    
    @Mock
    protected UserService userService;
    
    @Mock
    protected WebSocketManager webSocketManager;
    
    protected Map<String, Object> serviceClients = new HashMap<>();

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        
        // Initialize service clients map for easy access in tests
        serviceClients.put("position", positionService);
        serviceClients.put("device", deviceService);
        serviceClients.put("event", eventService);
        serviceClients.put("notification", notificationService);
        serviceClients.put("report", reportService);
        serviceClients.put("user", userService);
        
        // Configure common mock behavior
        when(config.getString("api.key")).thenReturn("test_api_key");
        when(serviceDiscoveryClient.isServiceAvailable("position-service")).thenReturn(true);
        when(serviceDiscoveryClient.isServiceAvailable("device-service")).thenReturn(true);
        when(serviceDiscoveryClient.isServiceAvailable("event-service")).thenReturn(true);
        when(serviceDiscoveryClient.isServiceAvailable("notification-service")).thenReturn(true);
        when(serviceDiscoveryClient.isServiceAvailable("report-service")).thenReturn(true);
    }

    /**
     * Creates and sets a mock authentication context for testing secured endpoints.
     * 
     * @param userId The user ID to set in the authentication context
     * @param admin Whether the user should have admin privileges
     * @return The created authentication object
     */
    protected Authentication setAuthenticationContext(long userId, boolean admin) {
        User user = new User();
        user.setId(userId);
        user.setEmail("test@example.com");
        user.setName("Test User");
        user.setAdministrator(admin);
        
        SecurityUser securityUser = new SecurityUser(user);
        
        Authentication authentication = new UsernamePasswordAuthenticationToken(
                securityUser,
                null,
                admin ? Collections.singletonList(new SimpleGrantedAuthority("ROLE_ADMIN")) :
                       Collections.singletonList(new SimpleGrantedAuthority("ROLE_USER")));
        
        SecurityContext securityContext = SecurityContextHolder.createEmptyContext();
        securityContext.setAuthentication(authentication);
        SecurityContextHolder.setContext(securityContext);
        
        return authentication;
    }
    
    /**
     * Creates and sets a mock authentication context with custom roles for testing secured endpoints.
     * 
     * @param userId The user ID to set in the authentication context
     * @param roles List of roles to assign to the user
     * @return The created authentication object
     */
    protected Authentication setAuthenticationContext(long userId, String... roles) {
        User user = new User();
        user.setId(userId);
        user.setEmail("test@example.com");
        user.setName("Test User");
        user.setAdministrator(false);
        
        SecurityUser securityUser = new SecurityUser(user);
        
        java.util.List<SimpleGrantedAuthority> authorities = new java.util.ArrayList<>();
        for (String role : roles) {
            authorities.add(new SimpleGrantedAuthority(role));
        }
        
        Authentication authentication = new UsernamePasswordAuthenticationToken(
                securityUser,
                null,
                authorities);
        
        SecurityContext securityContext = SecurityContextHolder.createEmptyContext();
        securityContext.setAuthentication(authentication);
        SecurityContextHolder.setContext(securityContext);
        
        return authentication;
    }

    /**
     * Clears the authentication context after a test.
     */
    protected void clearAuthenticationContext() {
        SecurityContextHolder.clearContext();
    }

    /**
     * Creates a mock service client for the specified service.
     * 
     * @param serviceClass The class of the service client to create
     * @param <T> The type of the service client
     * @return A mock service client
     */
    protected <T> T createServiceClient(Class<T> serviceClass) {
        T mockClient = org.mockito.Mockito.mock(serviceClass);
        serviceClients.put(serviceClass.getSimpleName().toLowerCase(), mockClient);
        return mockClient;
    }
    
    /**
     * Retrieves a previously created service client mock.
     * 
     * @param serviceName The name of the service client to retrieve
     * @param <T> The type of the service client
     * @return The mock service client
     */
    @SuppressWarnings("unchecked")
    protected <T> T getServiceClient(String serviceName) {
        return (T) serviceClients.get(serviceName.toLowerCase());
    }

    /**
     * Creates a test WebSocket session for testing real-time updates.
     * 
     * @param userId The user ID associated with the WebSocket session
     * @return A mock WebSocket session
     */
    protected org.eclipse.jetty.websocket.api.Session createWebSocketSession(long userId) {
        org.eclipse.jetty.websocket.api.Session session = org.mockito.Mockito.mock(org.eclipse.jetty.websocket.api.Session.class);
        org.eclipse.jetty.websocket.api.RemoteEndpoint remote = org.mockito.Mockito.mock(org.eclipse.jetty.websocket.api.RemoteEndpoint.class);
        org.mockito.Mockito.when(session.getRemote()).thenReturn(remote);
        org.mockito.Mockito.when(session.isOpen()).thenReturn(true);
        
        // Associate the session with the user ID in the WebSocketManager
        webSocketManager.addSession(userId, session);
        
        return session;
    }

    /**
     * Utility method to simulate a message received from the message broker.
     * 
     * @param topic The topic the message was published to
     * @param message The message content
     * @param <T> The type of the message
     */
    protected <T> void simulateMessageReceived(String topic, T message) {
        // Find the appropriate message listener for the topic and invoke it
        org.traccar.messaging.MessageListener<T> listener = findMessageListener(topic);
        if (listener != null) {
            listener.onMessage(message);
        }
    }
    
    /**
     * Finds the appropriate message listener for a given topic.
     * This is a helper method for simulateMessageReceived.
     * 
     * @param topic The topic to find a listener for
     * @param <T> The type of messages on this topic
     * @return The message listener for the topic, or null if none is registered
     */
    @SuppressWarnings("unchecked")
    private <T> org.traccar.messaging.MessageListener<T> findMessageListener(String topic) {
        // In a real implementation, this would look up the registered listener for the topic
        // For testing purposes, we create a mock listener that can be verified
        return (org.traccar.messaging.MessageListener<T>) org.mockito.Mockito.mock(org.traccar.messaging.MessageListener.class);
    }
    
    /**
     * Sets up a mock response for a service method call.
     * 
     * @param serviceName The name of the service
     * @param methodName The name of the method
     * @param methodParams The parameter types of the method
     * @param returnValue The value to return when the method is called
     */
    protected void setupServiceResponse(String serviceName, String methodName, Class<?>[] methodParams, Object returnValue) {
        Object serviceClient = serviceClients.get(serviceName.toLowerCase());
        if (serviceClient == null) {
            throw new IllegalArgumentException("No mock service client found for: " + serviceName);
        }
        
        try {
            java.lang.reflect.Method method = serviceClient.getClass().getMethod(methodName, methodParams);
            if (returnValue instanceof CompletableFuture) {
                org.mockito.Mockito.when(method.invoke(serviceClient, new Object[methodParams.length]))
                    .thenReturn(returnValue);
            } else {
                org.mockito.Mockito.when(method.invoke(serviceClient, new Object[methodParams.length]))
                    .thenReturn(CompletableFuture.completedFuture(returnValue));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup service response", e);
        }
    }

    /**
     * Utility method to verify API routing to the correct service.
     * 
     * @param serviceName The name of the service that should be called
     * @param methodName The name of the method that should be called
     */
    protected void verifyServiceRouting(String serviceName, String methodName) {
        verifyServiceRouting(serviceName, methodName, new Class<?>[0], new Object[0]);
    }
    
    /**
     * Utility method to verify API routing to the correct service with parameters.
     * 
     * @param serviceName The name of the service that should be called
     * @param methodName The name of the method that should be called
     * @param paramTypes The parameter types of the method
     * @param params The parameters to verify
     */
    protected void verifyServiceRouting(String serviceName, String methodName, Class<?>[] paramTypes, Object[] params) {
        Object serviceClient = serviceClients.get(serviceName.toLowerCase());
        if (serviceClient == null) {
            throw new IllegalArgumentException("No mock service client found for: " + serviceName);
        }
        
        try {
            java.lang.reflect.Method method = serviceClient.getClass().getMethod(methodName, paramTypes);
            org.mockito.Mockito.verify(serviceClient, org.mockito.Mockito.times(1));
            method.invoke(serviceClient, params);
        } catch (Exception e) {
            throw new RuntimeException("Failed to verify service routing", e);
        }
    }
    
    /**
     * Creates a mock HTTP request for testing REST endpoints.
     * 
     * @param method The HTTP method (GET, POST, etc.)
     * @param path The request path
     * @return A mock HTTP request
     */
    protected javax.servlet.http.HttpServletRequest createMockRequest(String method, String path) {
        return createMockRequest(method, path, null);
    }
    
    /**
     * Creates a mock HTTP request with parameters for testing REST endpoints.
     * 
     * @param method The HTTP method (GET, POST, etc.)
     * @param path The request path
     * @param parameters Map of request parameters
     * @return A mock HTTP request
     */
    protected javax.servlet.http.HttpServletRequest createMockRequest(String method, String path, Map<String, String> parameters) {
        javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock(javax.servlet.http.HttpServletRequest.class);
        org.mockito.Mockito.when(request.getMethod()).thenReturn(method);
        org.mockito.Mockito.when(request.getRequestURI()).thenReturn(path);
        org.mockito.Mockito.when(request.getContextPath()).thenReturn("");
        
        if (parameters != null) {
            for (Map.Entry<String, String> entry : parameters.entrySet()) {
                org.mockito.Mockito.when(request.getParameter(entry.getKey())).thenReturn(entry.getValue());
            }
        }
        
        return request;
    }
    
    /**
     * Creates a mock HTTP response for testing REST endpoints.
     * 
     * @return A mock HTTP response with a captured writer for assertions
     */
    protected MockHttpServletResponseWithCapture createMockResponse() {
        return new MockHttpServletResponseWithCapture();
    }
    
    /**
     * Helper class that extends HttpServletResponse to capture the response output
     * for assertions in tests.
     */
    protected static class MockHttpServletResponseWithCapture implements javax.servlet.http.HttpServletResponse {
        private final java.io.StringWriter stringWriter = new java.io.StringWriter();
        private final java.io.PrintWriter writer = new java.io.PrintWriter(stringWriter);
        private int status = 200;
        private final Map<String, String> headers = new HashMap<>();
        private String contentType = "application/json";
        
        public String getContent() {
            writer.flush();
            return stringWriter.toString();
        }
        
        public int getStatus() {
            return status;
        }
        
        public Map<String, String> getHeaders() {
            return headers;
        }
        
        @Override
        public void setStatus(int status) {
            this.status = status;
        }
        
        @Override
        public void setHeader(String name, String value) {
            headers.put(name, value);
        }
        
        @Override
        public void addHeader(String name, String value) {
            headers.put(name, value);
        }
        
        @Override
        public void setContentType(String contentType) {
            this.contentType = contentType;
        }
        
        @Override
        public String getContentType() {
            return contentType;
        }
        
        @Override
        public java.io.PrintWriter getWriter() {
            return writer;
        }
        
        // Implement other required methods with minimal functionality
        @Override public void addCookie(javax.servlet.http.Cookie cookie) {}
        @Override public boolean containsHeader(String name) { return headers.containsKey(name); }
        @Override public String encodeURL(String url) { return url; }
        @Override public String encodeRedirectURL(String url) { return url; }
        @Override public String encodeUrl(String url) { return url; }
        @Override public String encodeRedirectUrl(String url) { return url; }
        @Override public void sendError(int sc, String msg) { this.status = sc; }
        @Override public void sendError(int sc) { this.status = sc; }
        @Override public void sendRedirect(String location) {}
        @Override public void setDateHeader(String name, long date) {}
        @Override public void addDateHeader(String name, long date) {}
        @Override public void setIntHeader(String name, int value) {}
        @Override public void addIntHeader(String name, int value) {}
        @Override public void setStatus(int sc, String sm) { this.status = sc; }
        @Override public int getBufferSize() { return 0; }
        @Override public void setBufferSize(int size) {}
        @Override public void flushBuffer() {}
        @Override public void resetBuffer() {}
        @Override public boolean isCommitted() { return false; }
        @Override public void reset() {}
        @Override public void setLocale(java.util.Locale loc) {}
        @Override public java.util.Locale getLocale() { return java.util.Locale.getDefault(); }
        @Override public javax.servlet.ServletOutputStream getOutputStream() { return null; }
        @Override public String getCharacterEncoding() { return "UTF-8"; }
        @Override public void setCharacterEncoding(String charset) {}
    }
}