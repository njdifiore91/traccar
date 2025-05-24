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
package org.traccar;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.websocket.server.JettyWebSocketServlet;
import org.eclipse.jetty.websocket.server.JettyWebSocketServletFactory;
import org.eclipse.jetty.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.traccar.api.AsyncSocket;
import org.traccar.api.AsyncSocketServlet;
import org.traccar.api.security.LoginService;
import org.traccar.config.Config;
import org.traccar.session.ConnectionManager;
import org.traccar.storage.Storage;

import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocket configuration for the API Gateway service.
 * 
 * This class configures WebSocket endpoints, connection handling, session management,
 * and implements a heartbeat mechanism for connection health monitoring.
 */
@Configuration
@EnableScheduling
public class ApiGatewayWebSocketConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiGatewayWebSocketConfig.class);
    
    private static final String CONNECTION_REGISTRY_PREFIX = "websocket:connections:";
    private static final String HEARTBEAT_CHANNEL = "websocket:heartbeat";
    
    @Autowired
    private Config config;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Autowired
    private ConnectionManager connectionManager;
    
    @Autowired
    private Storage storage;
    
    @Autowired
    private LoginService loginService;
    
    @Autowired
    private RedisConnectionFactory redisConnectionFactory;
    
    @Value("${websocket.idle.timeout:300000}")
    private long idleTimeout;
    
    @Value("${websocket.max.connections:10000}")
    private int maxConnections;
    
    @Value("${websocket.heartbeat.interval:55000}")
    private long heartbeatInterval;
    
    private final Map<String, AsyncSocket> localConnections = new ConcurrentHashMap<>();
    
    /**
     * Configures the WebSocket container initializer.
     * 
     * @return JettyWebSocketServletContainerInitializer for WebSocket configuration
     */
    @Bean
    public JettyWebSocketServletContainerInitializer webSocketInitializer() {
        return new JettyWebSocketServletContainerInitializer() {
            @Override
            public void onStartup(ServletContext servletContext) throws ServletException {
                super.onStartup(servletContext);
                
                LOGGER.info("Initializing WebSocket endpoints");
                
                // Register the AsyncSocketServlet for handling WebSocket connections
                servletContext.addServlet("WebSocketServlet", asyncSocketServlet())
                        .addMapping("/api/socket");
                
                // Configure WebSocket container
                configure(servletContext, (context, wsContainer) -> {
                    wsContainer.setIdleTimeout(Duration.ofMillis(idleTimeout));
                    wsContainer.setMaxTextMessageSize(65536);
                    wsContainer.setMaxBinaryMessageSize(65536);
                    wsContainer.setMaxFrameSize(65536);
                    wsContainer.setInputBufferSize(8192);
                    wsContainer.setOutputBufferSize(8192);
                    wsContainer.setMaxOutgoingFrames(64);
                });
                
                LOGGER.info("WebSocket endpoints initialized successfully");
            }
        };
    }
    
    /**
     * Creates the AsyncSocketServlet for handling WebSocket connections.
     * 
     * @return AsyncSocketServlet instance
     */
    @Bean
    public AsyncSocketServlet asyncSocketServlet() {
        return new AsyncSocketServlet(config, objectMapper, connectionManager, storage, loginService) {
            @Override
            public void configure(JettyWebSocketServletFactory factory) {
                super.configure(factory);
                
                // Register connection creation callback to track local connections
                factory.setCreator((req, resp) -> {
                    AsyncSocket socket = (AsyncSocket) super.getCreator().createWebSocket(req, resp);
                    if (socket != null) {
                        String connectionId = UUID.randomUUID().toString();
                        registerConnection(connectionId, socket);
                        return socket;
                    }
                    return null;
                });
            }
        };
    }
    
    /**
     * Configures Redis template for WebSocket connection registry.
     * 
     * @return RedisTemplate configured for WebSocket connections
     */
    @Bean
    public RedisTemplate<String, Object> webSocketRedisTemplate() {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new Jackson2JsonRedisSerializer<>(objectMapper, Object.class));
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new Jackson2JsonRedisSerializer<>(objectMapper, Object.class));
        return template;
    }
    
    /**
     * Registers a WebSocket connection in the distributed registry.
     * 
     * @param connectionId Unique identifier for the connection
     * @param socket The AsyncSocket instance
     */
    private void registerConnection(String connectionId, AsyncSocket socket) {
        try {
            // Store connection in local map
            localConnections.put(connectionId, socket);
            
            // Store connection metadata in Redis
            String key = CONNECTION_REGISTRY_PREFIX + connectionId;
            Map<String, Object> metadata = Map.of(
                "userId", socket.getUserId(),
                "createdAt", System.currentTimeMillis(),
                "lastActivity", System.currentTimeMillis(),
                "instanceId", getInstanceId()
            );
            
            webSocketRedisTemplate().opsForHash().putAll(key, metadata);
            webSocketRedisTemplate().expire(key, Duration.ofMillis(idleTimeout * 2));
            
            LOGGER.debug("Registered WebSocket connection: {}", connectionId);
        } catch (Exception e) {
            LOGGER.error("Failed to register WebSocket connection", e);
        }
    }
    
    /**
     * Unregisters a WebSocket connection from the distributed registry.
     * 
     * @param connectionId Unique identifier for the connection
     */
    private void unregisterConnection(String connectionId) {
        try {
            // Remove from local map
            localConnections.remove(connectionId);
            
            // Remove from Redis
            String key = CONNECTION_REGISTRY_PREFIX + connectionId;
            webSocketRedisTemplate().delete(key);
            
            LOGGER.debug("Unregistered WebSocket connection: {}", connectionId);
        } catch (Exception e) {
            LOGGER.error("Failed to unregister WebSocket connection", e);
        }
    }
    
    /**
     * Updates the last activity timestamp for a connection.
     * 
     * @param connectionId Unique identifier for the connection
     */
    private void updateConnectionActivity(String connectionId) {
        try {
            String key = CONNECTION_REGISTRY_PREFIX + connectionId;
            webSocketRedisTemplate().opsForHash().put(key, "lastActivity", System.currentTimeMillis());
            webSocketRedisTemplate().expire(key, Duration.ofMillis(idleTimeout * 2));
        } catch (Exception e) {
            LOGGER.error("Failed to update connection activity", e);
        }
    }
    
    /**
     * Generates a heartbeat for all active WebSocket connections.
     * This method is scheduled to run at a fixed rate defined by heartbeatInterval.
     */
    @Scheduled(fixedDelayString = "${websocket.heartbeat.interval:55000}")
    public void sendHeartbeat() {
        try {
            LOGGER.debug("Sending WebSocket heartbeat to {} local connections", localConnections.size());
            
            // Publish heartbeat event to Redis channel for distributed instances
            Map<String, Object> heartbeatEvent = Map.of(
                "timestamp", System.currentTimeMillis(),
                "instanceId", getInstanceId()
            );
            
            webSocketRedisTemplate().convertAndSend(HEARTBEAT_CHANNEL, heartbeatEvent);
            
            // Send heartbeat to local connections
            for (Map.Entry<String, AsyncSocket> entry : localConnections.entrySet()) {
                String connectionId = entry.getKey();
                AsyncSocket socket = entry.getValue();
                
                try {
                    if (socket.isConnected()) {
                        socket.onKeepalive();
                        updateConnectionActivity(connectionId);
                    } else {
                        unregisterConnection(connectionId);
                    }
                } catch (Exception e) {
                    LOGGER.warn("Error sending heartbeat to connection {}", connectionId, e);
                    unregisterConnection(connectionId);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error in WebSocket heartbeat", e);
        }
    }
    
    /**
     * Cleans up stale WebSocket connections.
     * This method is scheduled to run every 2 minutes.
     */
    @Scheduled(fixedRate = 120000)
    public void cleanupStaleConnections() {
        try {
            LOGGER.debug("Checking for stale WebSocket connections");
            
            long now = System.currentTimeMillis();
            long staleThreshold = now - idleTimeout;
            
            // Check local connections first
            localConnections.entrySet().removeIf(entry -> {
                AsyncSocket socket = entry.getValue();
                if (!socket.isConnected()) {
                    unregisterConnection(entry.getKey());
                    return true;
                }
                return false;
            });
            
            // Let Redis TTL handle most of the cleanup automatically
            // This is just an additional safety check
            
        } catch (Exception e) {
            LOGGER.error("Error cleaning up stale WebSocket connections", e);
        }
    }
    
    /**
     * Gets a unique identifier for this service instance.
     * 
     * @return String instance identifier
     */
    private String getInstanceId() {
        // In a real implementation, this would be a unique identifier for the service instance,
        // possibly from Kubernetes, Spring Cloud, or a configuration property
        return System.getProperty("instance.id", "api-gateway-" + System.identityHashCode(this));
    }
}