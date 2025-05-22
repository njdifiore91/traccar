/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.traccar.api.security.SecurityRequestFilter;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * WebSocketMultiplexer implements a multiplexer for WebSocket connections that routes messages
 * between clients and the message broker. It maintains a registry of active WebSocket connections,
 * subscribes to relevant topics in the message broker based on user permissions, and delivers
 * real-time updates to connected clients.
 */
@Singleton
public class WebSocketMultiplexer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketMultiplexer.class);
    
    private static final String REDIS_CONNECTION_PREFIX = "websocket:connection:";
    private static final String REDIS_USER_PREFIX = "websocket:user:";
    private static final int CONNECTION_TTL_SECONDS = 300; // 5 minutes
    
    private static final String TOPIC_DEVICE_UPDATES = "device-updates";
    private static final String TOPIC_POSITION_UPDATES = "position-updates";
    private static final String TOPIC_EVENT_UPDATES = "event-updates";
    private static final String TOPIC_LOG_UPDATES = "log-updates";
    private static final String TOPIC_HEARTBEAT = "heartbeat-events";
    
    private final ObjectMapper objectMapper;
    private final Config config;
    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> redisConnection;
    private final RedisCommands<String, String> redisCommands;
    private final Connection rabbitConnection;
    private final Channel rabbitChannel;
    private final Map<String, Set<Session>> localSessions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    /**
     * Constructs a new WebSocketMultiplexer with the specified dependencies.
     *
     * @param objectMapper JSON object mapper for serialization/deserialization
     * @param config System configuration
     * @throws Exception If connection to Redis or RabbitMQ fails
     */
    @Inject
    public WebSocketMultiplexer(ObjectMapper objectMapper, Config config) throws Exception {
        this.objectMapper = objectMapper;
        this.config = config;
        
        // Initialize Redis connection for distributed connection registry
        String redisHost = config.getString(Keys.WEB_REDIS_HOST.withPrefix("websocket"), "localhost");
        int redisPort = config.getInteger(Keys.WEB_REDIS_PORT.withPrefix("websocket"), 6379);
        String redisPassword = config.getString(Keys.WEB_REDIS_PASSWORD.withPrefix("websocket"));
        
        RedisURI.Builder redisUriBuilder = RedisURI.builder()
                .withHost(redisHost)
                .withPort(redisPort)
                .withTimeout(Duration.ofSeconds(5));
        
        if (redisPassword != null && !redisPassword.isEmpty()) {
            redisUriBuilder.withPassword(redisPassword.toCharArray());
        }
        
        redisClient = RedisClient.create(redisUriBuilder.build());
        redisConnection = redisClient.connect();
        redisCommands = redisConnection.sync();
        
        // Initialize RabbitMQ connection for message broker integration
        String rabbitHost = config.getString(Keys.WEB_RABBITMQ_HOST.withPrefix("websocket"), "localhost");
        int rabbitPort = config.getInteger(Keys.WEB_RABBITMQ_PORT.withPrefix("websocket"), 5672);
        String rabbitUsername = config.getString(Keys.WEB_RABBITMQ_USERNAME.withPrefix("websocket"), "guest");
        String rabbitPassword = config.getString(Keys.WEB_RABBITMQ_PASSWORD.withPrefix("websocket"), "guest");
        String rabbitVirtualHost = config.getString(Keys.WEB_RABBITMQ_VHOST.withPrefix("websocket"), "/");
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitHost);
        factory.setPort(rabbitPort);
        factory.setUsername(rabbitUsername);
        factory.setPassword(rabbitPassword);
        factory.setVirtualHost(rabbitVirtualHost);
        
        rabbitConnection = factory.newConnection();
        rabbitChannel = rabbitConnection.createChannel();
        
        // Declare topics (exchanges) for real-time updates
        rabbitChannel.exchangeDeclare(TOPIC_DEVICE_UPDATES, "topic", true);
        rabbitChannel.exchangeDeclare(TOPIC_POSITION_UPDATES, "topic", true);
        rabbitChannel.exchangeDeclare(TOPIC_EVENT_UPDATES, "topic", true);
        rabbitChannel.exchangeDeclare(TOPIC_LOG_UPDATES, "topic", true);
        rabbitChannel.exchangeDeclare(TOPIC_HEARTBEAT, "topic", true);
        
        // Create a queue for this instance with auto-delete and exclusive flags
        String queueName = "websocket-" + UUID.randomUUID().toString();
        rabbitChannel.queueDeclare(queueName, false, true, true, null);
        
        // Bind to all topics
        rabbitChannel.queueBind(queueName, TOPIC_DEVICE_UPDATES, "#");
        rabbitChannel.queueBind(queueName, TOPIC_POSITION_UPDATES, "#");
        rabbitChannel.queueBind(queueName, TOPIC_EVENT_UPDATES, "#");
        rabbitChannel.queueBind(queueName, TOPIC_LOG_UPDATES, "#");
        rabbitChannel.queueBind(queueName, TOPIC_HEARTBEAT, "#");
        
        // Set up consumer for the queue
        rabbitChannel.basicConsume(queueName, true, new DefaultConsumer(rabbitChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                processMessage(envelope.getExchange(), envelope.getRoutingKey(), properties, body);
            }
        });
        
        // Schedule periodic cleanup of stale connections
        scheduler.scheduleAtFixedRate(this::cleanupStaleConnections, 2, 2, TimeUnit.MINUTES);
        
        LOGGER.info("WebSocketMultiplexer initialized successfully");
    }
    
    /**
     * Registers a new WebSocket connection for a user.
     *
     * @param session WebSocket session to register
     * @param userId ID of the authenticated user
     * @return Connection ID assigned to this session
     */
    public String registerConnection(Session session, long userId) {
        String connectionId = UUID.randomUUID().toString();
        String userIdStr = String.valueOf(userId);
        
        // Store in local session map
        localSessions.computeIfAbsent(userIdStr, k -> new HashSet<>()).add(session);
        
        // Store in Redis for distributed access
        String connectionKey = REDIS_CONNECTION_PREFIX + connectionId;
        Map<String, String> connectionData = new HashMap<>();
        connectionData.put("userId", userIdStr);
        connectionData.put("instanceId", getInstanceId());
        connectionData.put("createdAt", String.valueOf(System.currentTimeMillis()));
        connectionData.put("lastActivity", String.valueOf(System.currentTimeMillis()));
        
        redisCommands.hmset(connectionKey, connectionData);
        redisCommands.expire(connectionKey, CONNECTION_TTL_SECONDS);
        
        // Add connection to user's set
        redisCommands.sadd(REDIS_USER_PREFIX + userIdStr, connectionId);
        
        LOGGER.info("Registered WebSocket connection {} for user {}", connectionId, userId);
        return connectionId;
    }
    
    /**
     * Unregisters a WebSocket connection.
     *
     * @param session WebSocket session to unregister
     * @param connectionId Connection ID to unregister
     */
    public void unregisterConnection(Session session, String connectionId) {
        // Get user ID from Redis
        String connectionKey = REDIS_CONNECTION_PREFIX + connectionId;
        String userId = redisCommands.hget(connectionKey, "userId");
        
        if (userId != null) {
            // Remove from local sessions
            Set<Session> userSessions = localSessions.get(userId);
            if (userSessions != null) {
                userSessions.remove(session);
                if (userSessions.isEmpty()) {
                    localSessions.remove(userId);
                }
            }
            
            // Remove from Redis
            redisCommands.del(connectionKey);
            redisCommands.srem(REDIS_USER_PREFIX + userId, connectionId);
            
            LOGGER.info("Unregistered WebSocket connection {} for user {}", connectionId, userId);
        } else {
            LOGGER.warn("Attempted to unregister unknown connection: {}", connectionId);
        }
    }
    
    /**
     * Updates the last activity timestamp for a connection to prevent it from being
     * considered stale.
     *
     * @param connectionId Connection ID to update
     */
    public void updateConnectionActivity(String connectionId) {
        String connectionKey = REDIS_CONNECTION_PREFIX + connectionId;
        redisCommands.hset(connectionKey, "lastActivity", String.valueOf(System.currentTimeMillis()));
        redisCommands.expire(connectionKey, CONNECTION_TTL_SECONDS);
    }
    
    /**
     * Processes a message received from the message broker and routes it to appropriate
     * WebSocket connections based on user permissions.
     *
     * @param exchange Exchange (topic) the message was published to
     * @param routingKey Routing key of the message
     * @param properties Message properties including headers
     * @param body Message body as byte array
     */
    private void processMessage(String exchange, String routingKey, 
                               AMQP.BasicProperties properties, byte[] body) {
        try {
            // Extract correlation ID for distributed tracing
            String correlationId = properties.getCorrelationId();
            if (correlationId != null) {
                MDC.put("correlationId", correlationId);
            }
            
            // Parse message body
            String messageContent = new String(body, StandardCharsets.UTF_8);
            JsonNode messageJson = objectMapper.readTree(messageContent);
            
            // Handle heartbeat messages specially
            if (TOPIC_HEARTBEAT.equals(exchange)) {
                processHeartbeat(messageJson);
                return;
            }
            
            // For other messages, determine target users and permissions
            Set<String> targetUserIds = determineTargetUsers(exchange, routingKey, messageJson);
            if (targetUserIds.isEmpty()) {
                LOGGER.debug("No target users for message on {}/{}", exchange, routingKey);
                return;
            }
            
            // Add correlation ID and exchange info to the message
            if (messageJson.isObject()) {
                ObjectNode objectNode = (ObjectNode) messageJson;
                if (correlationId != null) {
                    objectNode.put("correlationId", correlationId);
                }
                objectNode.put("source", exchange);
                objectNode.put("type", routingKey);
                messageContent = objectMapper.writeValueAsString(objectNode);
            }
            
            // Deliver to local sessions for target users
            for (String userId : targetUserIds) {
                Set<Session> sessions = localSessions.get(userId);
                if (sessions != null && !sessions.isEmpty()) {
                    for (Session session : sessions) {
                        if (session.isOpen()) {
                            try {
                                session.getRemote().sendString(messageContent);
                            } catch (IOException e) {
                                LOGGER.warn("Failed to send message to WebSocket session", e);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error processing message from broker", e);
        } finally {
            MDC.remove("correlationId");
        }
    }
    
    /**
     * Processes a heartbeat message from the message broker.
     *
     * @param messageJson Heartbeat message as JSON
     */
    private void processHeartbeat(JsonNode messageJson) {
        try {
            // Send heartbeat to all local sessions
            String heartbeatStr = objectMapper.writeValueAsString(messageJson);
            for (Set<Session> sessions : localSessions.values()) {
                for (Session session : sessions) {
                    if (session.isOpen()) {
                        try {
                            session.getRemote().sendString(heartbeatStr);
                        } catch (IOException e) {
                            LOGGER.warn("Failed to send heartbeat to WebSocket session", e);
                        }
                    }
                }
            }
        } catch (JsonProcessingException e) {
            LOGGER.error("Error serializing heartbeat message", e);
        }
    }
    
    /**
     * Determines which users should receive a message based on the exchange, routing key,
     * and message content, taking into account user permissions.
     *
     * @param exchange Exchange (topic) the message was published to
     * @param routingKey Routing key of the message
     * @param messageJson Message content as JSON
     * @return Set of user IDs that should receive the message
     */
    private Set<String> determineTargetUsers(String exchange, String routingKey, JsonNode messageJson) {
        Set<String> targetUserIds = new HashSet<>();
        
        // Extract relevant IDs from the message based on the exchange and routing key
        Long deviceId = null;
        Long groupId = null;
        Long geofenceId = null;
        
        if (messageJson.has("deviceId")) {
            deviceId = messageJson.get("deviceId").asLong();
        }
        
        if (messageJson.has("groupId")) {
            groupId = messageJson.get("groupId").asLong();
        }
        
        if (messageJson.has("geofenceId")) {
            geofenceId = messageJson.get("geofenceId").asLong();
        }
        
        // Get all users from Redis
        Set<String> allUserIds = new HashSet<>();
        for (String key : redisCommands.keys(REDIS_USER_PREFIX + "*")) {
            allUserIds.add(key.substring(REDIS_USER_PREFIX.length()));
        }
        
        // Filter users based on permissions
        for (String userId : allUserIds) {
            boolean hasPermission = false;
            
            // Check device permission
            if (deviceId != null) {
                hasPermission = SecurityRequestFilter.hasPermission(Long.parseLong(userId), deviceId);
            }
            
            // Check group permission
            if (!hasPermission && groupId != null) {
                hasPermission = SecurityRequestFilter.hasPermission(Long.parseLong(userId), groupId);
            }
            
            // Check geofence permission
            if (!hasPermission && geofenceId != null) {
                hasPermission = SecurityRequestFilter.hasPermission(Long.parseLong(userId), geofenceId);
            }
            
            // If no specific ID to check, or user has permission, add to target list
            if (deviceId == null && groupId == null && geofenceId == null || hasPermission) {
                targetUserIds.add(userId);
            }
        }
        
        return targetUserIds;
    }
    
    /**
     * Cleans up stale WebSocket connections that haven't been active recently.
     */
    private void cleanupStaleConnections() {
        try {
            long cutoffTime = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(CONNECTION_TTL_SECONDS);
            
            // Scan all connection keys
            for (String key : redisCommands.keys(REDIS_CONNECTION_PREFIX + "*")) {
                String connectionId = key.substring(REDIS_CONNECTION_PREFIX.length());
                String lastActivityStr = redisCommands.hget(key, "lastActivity");
                String instanceId = redisCommands.hget(key, "instanceId");
                String userId = redisCommands.hget(key, "userId");
                
                if (lastActivityStr != null) {
                    long lastActivity = Long.parseLong(lastActivityStr);
                    if (lastActivity < cutoffTime) {
                        // Connection is stale, remove it
                        LOGGER.info("Cleaning up stale connection: {}", connectionId);
                        redisCommands.del(key);
                        
                        if (userId != null) {
                            redisCommands.srem(REDIS_USER_PREFIX + userId, connectionId);
                        }
                        
                        // If this is a local connection, close the session
                        if (getInstanceId().equals(instanceId) && userId != null) {
                            Set<Session> sessions = localSessions.get(userId);
                            if (sessions != null) {
                                // We don't have a direct mapping from connectionId to Session,
                                // so we'll need to close all sessions for this user and let them reconnect
                                for (Session session : new HashSet<>(sessions)) {
                                    try {
                                        if (session.isOpen()) {
                                            session.close();
                                        }
                                    } catch (Exception e) {
                                        LOGGER.warn("Error closing stale WebSocket session", e);
                                    }
                                }
                                localSessions.remove(userId);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error cleaning up stale connections", e);
        }
    }
    
    /**
     * Gets a unique identifier for this server instance.
     *
     * @return Instance ID string
     */
    private String getInstanceId() {
        // Use hostname + process ID as a simple instance identifier
        return System.getProperty("hostname", "unknown") + "-" + ProcessHandle.current().pid();
    }
    
    /**
     * Publishes a message to the specified topic in the message broker.
     *
     * @param topic Topic to publish to
     * @param routingKey Routing key for the message
     * @param message Message content as JSON string
     * @throws IOException If publishing fails
     */
    public void publishMessage(String topic, String routingKey, String message) throws IOException {
        String correlationId = UUID.randomUUID().toString();
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .correlationId(correlationId)
                .build();
        
        rabbitChannel.basicPublish(topic, routingKey, properties, message.getBytes(StandardCharsets.UTF_8));
    }
    
    /**
     * Closes all connections and resources used by the multiplexer.
     */
    public void close() {
        try {
            scheduler.shutdown();
            
            if (rabbitChannel != null && rabbitChannel.isOpen()) {
                rabbitChannel.close();
            }
            
            if (rabbitConnection != null && rabbitConnection.isOpen()) {
                rabbitConnection.close();
            }
            
            if (redisConnection != null) {
                redisConnection.close();
            }
            
            if (redisClient != null) {
                redisClient.shutdown();
            }
            
            LOGGER.info("WebSocketMultiplexer closed successfully");
        } catch (Exception e) {
            LOGGER.error("Error closing WebSocketMultiplexer", e);
        }
    }
}