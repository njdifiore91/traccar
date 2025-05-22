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
package org.traccar.session.store;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.session.ConnectionKey;
import org.traccar.session.DeviceSession;
import org.traccar.session.discovery.ServiceDiscoveryManager;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Provides distributed session storage capabilities using Redis.
 * This class manages device sessions and WebSocket sessions across multiple service instances.
 */
@Singleton
public class DistributedSessionStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedSessionStore.class);

    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final RedisClient redisClient;
    private final MeterRegistry meterRegistry;
    
    // Metrics
    private final Timer sessionOperationTimer;

    @Inject
    public DistributedSessionStore(ServiceDiscoveryManager serviceDiscoveryManager) {
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.redisClient = new RedisClient(serviceDiscoveryManager);
        this.meterRegistry = serviceDiscoveryManager.getMeterRegistry();
        
        // Initialize metrics
        this.sessionOperationTimer = meterRegistry.timer("session.store.operation.time");
        
        // Initialize Redis connection
        redisClient.initialize();
        
        LOGGER.info("DistributedSessionStore initialized");
    }

    /**
     * Stores a device session in the distributed store.
     *
     * @param deviceId The device ID
     * @param session  The device session to store
     */
    public void putSession(long deviceId, DeviceSession session) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("putSession").start();
        span.setTag("deviceId", deviceId);
        
        try {
            String key = "device:session:" + deviceId;
            String value = serializeSession(session);
            
            // Store session with TTL
            sessionOperationTimer.record(() -> {
                redisClient.set(key, value, 3600); // 1 hour TTL
                return null;
            });
            
            LOGGER.debug("Stored device session for device ID: {}", deviceId);
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error storing device session for device ID {}: {}", deviceId, e.getMessage(), e);
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Retrieves a device session from the distributed store.
     *
     * @param deviceId The device ID
     * @return The device session, or null if not found
     */
    public DeviceSession getSession(long deviceId) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("getSession").start();
        span.setTag("deviceId", deviceId);
        
        try {
            String key = "device:session:" + deviceId;
            
            // Get session and refresh TTL
            String value = sessionOperationTimer.record(() -> {
                String result = redisClient.get(key);
                if (result != null) {
                    redisClient.expire(key, 3600); // Refresh TTL
                }
                return result;
            });
            
            if (value != null) {
                DeviceSession session = deserializeSession(value);
                LOGGER.debug("Retrieved device session for device ID: {}", deviceId);
                return session;
            } else {
                LOGGER.debug("No device session found for device ID: {}", deviceId);
                return null;
            }
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error retrieving device session for device ID {}: {}", deviceId, e.getMessage(), e);
            return null;
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Removes a device session from the distributed store.
     *
     * @param deviceId The device ID
     * @return The removed device session, or null if not found
     */
    public DeviceSession removeSession(long deviceId) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("removeSession").start();
        span.setTag("deviceId", deviceId);
        
        try {
            String key = "device:session:" + deviceId;
            
            // Get session and then delete it
            DeviceSession session = getSession(deviceId);
            if (session != null) {
                sessionOperationTimer.record(() -> {
                    redisClient.del(key);
                    return null;
                });
                LOGGER.debug("Removed device session for device ID: {}", deviceId);
            }
            return session;
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error removing device session for device ID {}: {}", deviceId, e.getMessage(), e);
            return null;
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Stores sessions by endpoint in the distributed store.
     *
     * @param connectionKey The connection key
     * @param sessions      The sessions map to store
     */
    public void updateSessionsByEndpoint(ConnectionKey connectionKey, Map<String, DeviceSession> sessions) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("updateSessionsByEndpoint").start();
        span.setTag("connectionKey", connectionKey.toString());
        
        try {
            String key = "endpoint:sessions:" + connectionKey.toString();
            String value = serializeSessionMap(sessions);
            
            // Store sessions with TTL
            sessionOperationTimer.record(() -> {
                redisClient.set(key, value, 3600); // 1 hour TTL
                return null;
            });
            
            LOGGER.debug("Updated sessions for endpoint: {}", connectionKey);
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error updating sessions for endpoint {}: {}", connectionKey, e.getMessage(), e);
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Retrieves sessions by endpoint from the distributed store.
     *
     * @param connectionKey The connection key
     * @return The sessions map, or an empty map if not found
     */
    public Map<String, DeviceSession> getSessionsByEndpoint(ConnectionKey connectionKey) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("getSessionsByEndpoint").start();
        span.setTag("connectionKey", connectionKey.toString());
        
        try {
            String key = "endpoint:sessions:" + connectionKey.toString();
            
            // Get sessions and refresh TTL
            String value = sessionOperationTimer.record(() -> {
                String result = redisClient.get(key);
                if (result != null) {
                    redisClient.expire(key, 3600); // Refresh TTL
                }
                return result;
            });
            
            if (value != null) {
                Map<String, DeviceSession> sessions = deserializeSessionMap(value);
                LOGGER.debug("Retrieved {} sessions for endpoint: {}", sessions.size(), connectionKey);
                return sessions;
            } else {
                LOGGER.debug("No sessions found for endpoint: {}", connectionKey);
                return new HashMap<>();
            }
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error retrieving sessions for endpoint {}: {}", connectionKey, e.getMessage(), e);
            return new HashMap<>();
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Removes sessions by endpoint from the distributed store.
     *
     * @param connectionKey The connection key
     * @return The removed sessions map, or an empty map if not found
     */
    public Map<String, DeviceSession> removeSessionsByEndpoint(ConnectionKey connectionKey) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("removeSessionsByEndpoint").start();
        span.setTag("connectionKey", connectionKey.toString());
        
        try {
            String key = "endpoint:sessions:" + connectionKey.toString();
            
            // Get sessions and then delete them
            Map<String, DeviceSession> sessions = getSessionsByEndpoint(connectionKey);
            if (!sessions.isEmpty()) {
                sessionOperationTimer.record(() -> {
                    redisClient.del(key);
                    return null;
                });
                LOGGER.debug("Removed {} sessions for endpoint: {}", sessions.size(), connectionKey);
            }
            return sessions;
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error removing sessions for endpoint {}: {}", connectionKey, e.getMessage(), e);
            return new HashMap<>();
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Stores WebSocket session metadata in the distributed store.
     *
     * @param connectionId The WebSocket connection ID
     * @param metadata     The session metadata to store
     */
    public void storeWebSocketSession(String connectionId, Map<String, Object> metadata) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("storeWebSocketSession").start();
        span.setTag("connectionId", connectionId);
        
        try {
            String key = "websocket:session:" + connectionId;
            String value = serializeMetadata(metadata);
            
            // Store session with TTL
            sessionOperationTimer.record(() -> {
                redisClient.set(key, value, 3600); // 1 hour TTL
                return null;
            });
            
            LOGGER.debug("Stored WebSocket session for connection ID: {}", connectionId);
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error storing WebSocket session for connection ID {}: {}", connectionId, e.getMessage(), e);
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Updates WebSocket session metadata in the distributed store.
     *
     * @param connectionId The WebSocket connection ID
     * @param metadata     The session metadata to update
     */
    public void updateWebSocketSession(String connectionId, Map<String, Object> metadata) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("updateWebSocketSession").start();
        span.setTag("connectionId", connectionId);
        
        try {
            String key = "websocket:session:" + connectionId;
            
            // Get existing metadata
            Map<String, Object> existingMetadata = getWebSocketSession(connectionId);
            if (existingMetadata == null) {
                existingMetadata = new HashMap<>();
            }
            
            // Merge new metadata with existing metadata
            existingMetadata.putAll(metadata);
            String value = serializeMetadata(existingMetadata);
            
            // Update session with TTL
            sessionOperationTimer.record(() -> {
                redisClient.set(key, value, 3600); // 1 hour TTL
                return null;
            });
            
            LOGGER.debug("Updated WebSocket session for connection ID: {}", connectionId);
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error updating WebSocket session for connection ID {}: {}", connectionId, e.getMessage(), e);
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Retrieves WebSocket session metadata from the distributed store.
     *
     * @param connectionId The WebSocket connection ID
     * @return The session metadata, or null if not found
     */
    public Map<String, Object> getWebSocketSession(String connectionId) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("getWebSocketSession").start();
        span.setTag("connectionId", connectionId);
        
        try {
            String key = "websocket:session:" + connectionId;
            
            // Get session and refresh TTL
            String value = sessionOperationTimer.record(() -> {
                String result = redisClient.get(key);
                if (result != null) {
                    redisClient.expire(key, 3600); // Refresh TTL
                }
                return result;
            });
            
            if (value != null) {
                Map<String, Object> metadata = deserializeMetadata(value);
                LOGGER.debug("Retrieved WebSocket session for connection ID: {}", connectionId);
                return metadata;
            } else {
                LOGGER.debug("No WebSocket session found for connection ID: {}", connectionId);
                return null;
            }
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error retrieving WebSocket session for connection ID {}: {}", connectionId, e.getMessage(), e);
            return null;
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Removes WebSocket session metadata from the distributed store.
     *
     * @param connectionId The WebSocket connection ID
     * @return The removed session metadata, or null if not found
     */
    public Map<String, Object> removeWebSocketSession(String connectionId) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("removeWebSocketSession").start();
        span.setTag("connectionId", connectionId);
        
        try {
            String key = "websocket:session:" + connectionId;
            
            // Get session and then delete it
            Map<String, Object> metadata = getWebSocketSession(connectionId);
            if (metadata != null) {
                sessionOperationTimer.record(() -> {
                    redisClient.del(key);
                    return null;
                });
                LOGGER.debug("Removed WebSocket session for connection ID: {}", connectionId);
            }
            return metadata;
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error removing WebSocket session for connection ID {}: {}", connectionId, e.getMessage(), e);
            return null;
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    /**
     * Serializes a device session to a string.
     *
     * @param session The device session to serialize
     * @return The serialized session string
     */
    private String serializeSession(DeviceSession session) {
        // Implementation would serialize session to JSON or other format
        return "serialized:" + session.toString();
    }

    /**
     * Deserializes a string to a device session.
     *
     * @param value The serialized session string
     * @return The deserialized device session
     */
    private DeviceSession deserializeSession(String value) {
        // Implementation would deserialize JSON or other format to session
        return new DeviceSession(0, "dummy", null, null, null, null);
    }

    /**
     * Serializes a sessions map to a string.
     *
     * @param sessions The sessions map to serialize
     * @return The serialized sessions string
     */
    private String serializeSessionMap(Map<String, DeviceSession> sessions) {
        // Implementation would serialize sessions map to JSON or other format
        return "serialized:" + sessions.toString();
    }

    /**
     * Deserializes a string to a sessions map.
     *
     * @param value The serialized sessions string
     * @return The deserialized sessions map
     */
    private Map<String, DeviceSession> deserializeSessionMap(String value) {
        // Implementation would deserialize JSON or other format to sessions map
        return new HashMap<>();
    }

    /**
     * Serializes metadata to a string.
     *
     * @param metadata The metadata to serialize
     * @return The serialized metadata string
     */
    private String serializeMetadata(Map<String, Object> metadata) {
        // Implementation would serialize metadata to JSON or other format
        return "serialized:" + metadata.toString();
    }

    /**
     * Deserializes a string to metadata.
     *
     * @param value The serialized metadata string
     * @return The deserialized metadata
     */
    private Map<String, Object> deserializeMetadata(String value) {
        // Implementation would deserialize JSON or other format to metadata
        return new HashMap<>();
    }

    /**
     * Redis client implementation.
     */
    private static class RedisClient {
        private final ServiceDiscoveryManager serviceDiscoveryManager;
        
        public RedisClient(ServiceDiscoveryManager serviceDiscoveryManager) {
            this.serviceDiscoveryManager = serviceDiscoveryManager;
        }
        
        public void initialize() {
            // Implementation would initialize Redis connection
            LOGGER.info("Initialized Redis client");
        }
        
        public void set(String key, String value, int ttlSeconds) {
            // Implementation would set key-value pair with TTL in Redis
            LOGGER.debug("Set key in Redis: {}", key);
        }
        
        public String get(String key) {
            // Implementation would get value for key from Redis
            LOGGER.debug("Get key from Redis: {}", key);
            return null;
        }
        
        public void expire(String key, int ttlSeconds) {
            // Implementation would update TTL for key in Redis
            LOGGER.debug("Update TTL for key in Redis: {}", key);
        }
        
        public void del(String key) {
            // Implementation would delete key from Redis
            LOGGER.debug("Delete key from Redis: {}", key);
        }
        
        public void close() {
            // Implementation would close Redis connection
            LOGGER.info("Closed Redis client");
        }
    }
}