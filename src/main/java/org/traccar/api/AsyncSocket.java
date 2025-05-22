/*
 * Copyright 2015 - 2024 Anton Tananaev (anton@traccar.org)
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.traccar.helper.model.PositionUtil;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.LogRecord;
import org.traccar.model.Position;
import org.traccar.session.ConnectionManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class AsyncSocket extends WebSocketAdapter implements ConnectionManager.UpdateListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncSocket.class);

    private static final String KEY_DEVICES = "devices";
    private static final String KEY_POSITIONS = "positions";
    private static final String KEY_EVENTS = "events";
    private static final String KEY_LOGS = "logs";
    
    private static final String REDIS_CONNECTION_PREFIX = "websocket:connection:";
    private static final String REDIS_USER_PREFIX = "websocket:user:";
    private static final int REDIS_TTL_SECONDS = 300; // 5 minutes
    
    private static final String CORRELATION_ID_KEY = "correlationId";

    private final ObjectMapper objectMapper;
    private final ConnectionManager connectionManager;
    private final Storage storage;
    private final long userId;
    private final String connectionId;
    private final JedisPool jedisPool;
    private final CircuitBreaker circuitBreaker;
    private final MessageBrokerClient messageBrokerClient;

    private boolean includeLogs;

    public AsyncSocket(ObjectMapper objectMapper, ConnectionManager connectionManager, 
                      Storage storage, long userId, JedisPool jedisPool,
                      MessageBrokerClient messageBrokerClient) {
        this.objectMapper = objectMapper;
        this.connectionManager = connectionManager;
        this.storage = storage;
        this.userId = userId;
        this.jedisPool = jedisPool;
        this.messageBrokerClient = messageBrokerClient;
        this.connectionId = UUID.randomUUID().toString();
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofMillis(1000))
                .permittedNumberOfCallsInHalfOpenState(2)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("asyncSocketCircuitBreaker");
    }

    @Override
    public void onWebSocketConnect(Session session) {
        super.onWebSocketConnect(session);
        
        // Generate a correlation ID for this connection
        String correlationId = UUID.randomUUID().toString();
        MDC.put(CORRELATION_ID_KEY, correlationId);
        
        LOGGER.info("WebSocket connection established: {} for user: {}", connectionId, userId);

        try {
            // Register connection in Redis
            registerConnectionInRedis();
            
            // Subscribe to message broker topics
            subscribeToMessageBrokerTopics();
            
            // Send initial data with circuit breaker protection
            Supplier<Map<String, Collection<?>>> initialDataSupplier = () -> {
                try {
                    Map<String, Collection<?>> data = new HashMap<>();
                    data.put(KEY_POSITIONS, PositionUtil.getLatestPositions(storage, userId));
                    return data;
                } catch (StorageException e) {
                    LOGGER.error("Error getting initial positions", e);
                    throw new RuntimeException(e);
                }
            };
            
            Map<String, Collection<?>> initialData = circuitBreaker.executeSupplier(initialDataSupplier);
            sendData(initialData);
            
            // Add listener for local updates
            connectionManager.addListener(userId, this);
        } catch (Exception e) {
            LOGGER.error("Error during WebSocket connection setup", e);
            throw new RuntimeException(e);
        } finally {
            MDC.remove(CORRELATION_ID_KEY);
        }
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put(CORRELATION_ID_KEY, correlationId);
        
        LOGGER.info("WebSocket connection closed: {} for user: {}, status: {}, reason: {}", 
                connectionId, userId, statusCode, reason);
        
        try {
            // Unregister from Redis
            unregisterConnectionFromRedis();
            
            // Unsubscribe from message broker topics
            messageBrokerClient.unsubscribe(userId, connectionId);
            
            // Remove local listener
            connectionManager.removeListener(userId, this);
            
            super.onWebSocketClose(statusCode, reason);
        } catch (Exception e) {
            LOGGER.error("Error during WebSocket connection cleanup", e);
        } finally {
            MDC.remove(CORRELATION_ID_KEY);
        }
    }

    @Override
    public void onWebSocketText(String message) {
        super.onWebSocketText(message);

        String correlationId = UUID.randomUUID().toString();
        MDC.put(CORRELATION_ID_KEY, correlationId);
        
        try {
            includeLogs = objectMapper.readTree(message).get("logs").asBoolean();
            
            // Update Redis with the latest configuration
            try (Jedis jedis = jedisPool.getResource()) {
                Map<String, String> connectionData = new HashMap<>();
                connectionData.put("userId", String.valueOf(userId));
                connectionData.put("includeLogs", String.valueOf(includeLogs));
                connectionData.put("lastActivity", String.valueOf(System.currentTimeMillis()));
                connectionData.put("correlationId", correlationId);
                
                jedis.hset(REDIS_CONNECTION_PREFIX + connectionId, connectionData);
                jedis.expire(REDIS_CONNECTION_PREFIX + connectionId, REDIS_TTL_SECONDS);
            }
        } catch (JsonProcessingException e) {
            LOGGER.warn("Socket JSON parsing error", e);
        } finally {
            MDC.remove(CORRELATION_ID_KEY);
        }
    }

    @Override
    public void onKeepalive() {
        String correlationId = UUID.randomUUID().toString();
        MDC.put(CORRELATION_ID_KEY, correlationId);
        
        try {
            // Update last activity timestamp in Redis
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.hset(REDIS_CONNECTION_PREFIX + connectionId, "lastActivity", String.valueOf(System.currentTimeMillis()));
                jedis.expire(REDIS_CONNECTION_PREFIX + connectionId, REDIS_TTL_SECONDS);
            }
            
            // Send empty data as keepalive
            sendData(new HashMap<>());
        } catch (Exception e) {
            LOGGER.warn("Error updating keepalive information", e);
        } finally {
            MDC.remove(CORRELATION_ID_KEY);
        }
    }

    @Override
    public void onUpdateDevice(Device device) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put(CORRELATION_ID_KEY, correlationId);
        
        try {
            sendData(Map.of(KEY_DEVICES, List.of(device)), correlationId);
        } finally {
            MDC.remove(CORRELATION_ID_KEY);
        }
    }

    @Override
    public void onUpdatePosition(Position position) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put(CORRELATION_ID_KEY, correlationId);
        
        try {
            sendData(Map.of(KEY_POSITIONS, List.of(position)), correlationId);
        } finally {
            MDC.remove(CORRELATION_ID_KEY);
        }
    }

    @Override
    public void onUpdateEvent(Event event) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put(CORRELATION_ID_KEY, correlationId);
        
        try {
            sendData(Map.of(KEY_EVENTS, List.of(event)), correlationId);
        } finally {
            MDC.remove(CORRELATION_ID_KEY);
        }
    }

    @Override
    public void onUpdateLog(LogRecord record) {
        if (includeLogs) {
            String correlationId = UUID.randomUUID().toString();
            MDC.put(CORRELATION_ID_KEY, correlationId);
            
            try {
                sendData(Map.of(KEY_LOGS, List.of(record)), correlationId);
            } finally {
                MDC.remove(CORRELATION_ID_KEY);
            }
        }
    }

    private void sendData(Map<String, Collection<?>> data) {
        sendData(data, UUID.randomUUID().toString());
    }
    
    private void sendData(Map<String, Collection<?>> data, String correlationId) {
        if (isConnected()) {
            try {
                // Add correlation ID to the data for tracing
                Map<String, Object> enrichedData = new HashMap<>(data);
                enrichedData.put(CORRELATION_ID_KEY, correlationId);
                
                // Use circuit breaker to protect against serialization failures
                Supplier<String> serializeSupplier = () -> {
                    try {
                        return objectMapper.writeValueAsString(enrichedData);
                    } catch (JsonProcessingException e) {
                        LOGGER.warn("Socket JSON formatting error", e);
                        throw new RuntimeException(e);
                    }
                };
                
                String jsonData = circuitBreaker.executeSupplier(serializeSupplier);
                getRemote().sendString(jsonData, null);
                
                // Update last activity timestamp in Redis
                try (Jedis jedis = jedisPool.getResource()) {
                    jedis.hset(REDIS_CONNECTION_PREFIX + connectionId, "lastActivity", String.valueOf(System.currentTimeMillis()));
                    jedis.expire(REDIS_CONNECTION_PREFIX + connectionId, REDIS_TTL_SECONDS);
                }
            } catch (Exception e) {
                LOGGER.warn("Error sending data to WebSocket", e);
            }
        }
    }
    
    private void registerConnectionInRedis() {
        try (Jedis jedis = jedisPool.getResource()) {
            // Store connection details
            Map<String, String> connectionData = new HashMap<>();
            connectionData.put("userId", String.valueOf(userId));
            connectionData.put("includeLogs", String.valueOf(includeLogs));
            connectionData.put("lastActivity", String.valueOf(System.currentTimeMillis()));
            connectionData.put("remoteAddress", getSession().getRemoteAddress().toString());
            
            jedis.hset(REDIS_CONNECTION_PREFIX + connectionId, connectionData);
            jedis.expire(REDIS_CONNECTION_PREFIX + connectionId, REDIS_TTL_SECONDS);
            
            // Add connection to user's connection list
            jedis.sadd(REDIS_USER_PREFIX + userId, connectionId);
            jedis.expire(REDIS_USER_PREFIX + userId, REDIS_TTL_SECONDS);
            
            LOGGER.debug("Registered WebSocket connection {} for user {} in Redis", connectionId, userId);
        } catch (Exception e) {
            LOGGER.error("Failed to register WebSocket connection in Redis", e);
        }
    }
    
    private void unregisterConnectionFromRedis() {
        try (Jedis jedis = jedisPool.getResource()) {
            // Remove connection details
            jedis.del(REDIS_CONNECTION_PREFIX + connectionId);
            
            // Remove connection from user's connection list
            jedis.srem(REDIS_USER_PREFIX + userId, connectionId);
            
            LOGGER.debug("Unregistered WebSocket connection {} for user {} from Redis", connectionId, userId);
        } catch (Exception e) {
            LOGGER.error("Failed to unregister WebSocket connection from Redis", e);
        }
    }
    
    private void subscribeToMessageBrokerTopics() {
        try {
            messageBrokerClient.subscribe(userId, connectionId, (topic, message, msgCorrelationId) -> {
                MDC.put(CORRELATION_ID_KEY, msgCorrelationId);
                try {
                    // Process message based on topic
                    switch (topic) {
                        case "device-updates":
                            Device device = objectMapper.readValue(message, Device.class);
                            sendData(Map.of(KEY_DEVICES, List.of(device)), msgCorrelationId);
                            break;
                        case "position-updates":
                            Position position = objectMapper.readValue(message, Position.class);
                            sendData(Map.of(KEY_POSITIONS, List.of(position)), msgCorrelationId);
                            break;
                        case "event-updates":
                            Event event = objectMapper.readValue(message, Event.class);
                            sendData(Map.of(KEY_EVENTS, List.of(event)), msgCorrelationId);
                            break;
                        case "log-updates":
                            if (includeLogs) {
                                LogRecord logRecord = objectMapper.readValue(message, LogRecord.class);
                                sendData(Map.of(KEY_LOGS, List.of(logRecord)), msgCorrelationId);
                            }
                            break;
                        case "heartbeat-events":
                            // Process heartbeat - just update Redis TTL
                            try (Jedis jedis = jedisPool.getResource()) {
                                jedis.hset(REDIS_CONNECTION_PREFIX + connectionId, "lastActivity", 
                                        String.valueOf(System.currentTimeMillis()));
                                jedis.expire(REDIS_CONNECTION_PREFIX + connectionId, REDIS_TTL_SECONDS);
                            }
                            break;
                        default:
                            LOGGER.warn("Received message for unknown topic: {}", topic);
                    }
                } catch (Exception e) {
                    LOGGER.error("Error processing message from broker for topic: {}", topic, e);
                } finally {
                    MDC.remove(CORRELATION_ID_KEY);
                }
            });
            
            LOGGER.debug("Subscribed to message broker topics for user: {} with connection: {}", userId, connectionId);
        } catch (Exception e) {
            LOGGER.error("Failed to subscribe to message broker topics", e);
        }
    }
    
    /**
     * Interface for message broker client implementations
     */
    public interface MessageBrokerClient {
        /**
         * Subscribe to topics for a specific user and connection
         * 
         * @param userId User ID to subscribe for
         * @param connectionId Connection ID for this subscription
         * @param messageHandler Handler for received messages
         */
        void subscribe(long userId, String connectionId, MessageHandler messageHandler);
        
        /**
         * Unsubscribe from topics for a specific user and connection
         * 
         * @param userId User ID to unsubscribe
         * @param connectionId Connection ID to unsubscribe
         */
        void unsubscribe(long userId, String connectionId);
    }
    
    /**
     * Interface for handling messages from the message broker
     */
    public interface MessageHandler {
        /**
         * Handle a message from the message broker
         * 
         * @param topic Topic the message was received on
         * @param message Message content
         * @param correlationId Correlation ID for tracing
         */
        void onMessage(String topic, String message, String correlationId);
    }
}