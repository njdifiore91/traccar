/*
 * Copyright 2018 Anton Tananaev (anton@traccar.org)
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

import java.io.Serializable;
import java.net.SocketAddress;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a network message with support for distributed tracing, correlation IDs,
 * message headers, priority, routing information, and time-to-live (TTL).
 * This class is designed to be compatible with message brokers and support
 * microservices communication patterns.
 */
public class NetworkMessage implements Serializable {

    private static final long serialVersionUID = 1L;
    
    // Standard message fields
    private final SocketAddress remoteAddress;
    private final Object message;
    
    // Distributed tracing and correlation fields
    private String traceId;          // OpenTelemetry trace ID
    private String spanId;           // OpenTelemetry span ID
    private byte traceFlags;         // OpenTelemetry trace flags (e.g., sampled)
    private String correlationId;    // Correlation ID for request tracking
    
    // Message metadata and routing
    private final Map<String, String> headers;  // Headers for metadata propagation
    private int priority;            // Message priority (higher = more important)
    private String routingKey;       // Routing information for message brokers
    private Instant expirationTime;  // Time when this message expires (TTL)
    
    /**
     * Creates a new NetworkMessage with default values.
     *
     * @param message The message payload
     * @param remoteAddress The remote socket address
     */
public NetworkMessage(Object message, SocketAddress remoteAddress) {
        this.message = message;
        this.remoteAddress = remoteAddress;
        this.headers = new HashMap<>();
        this.correlationId = UUID.randomUUID().toString();
        this.priority = 0; // Default priority
    }
    
    /**
     * Creates a new NetworkMessage with specified headers and correlation ID.
     *
     * @param message The message payload
     * @param remoteAddress The remote socket address
     * @param headers Message headers for metadata propagation
     * @param correlationId Correlation ID for request tracking
     */
    public NetworkMessage(Object message, SocketAddress remoteAddress, 
                         Map<String, String> headers, String correlationId) {
        this.message = message;
        this.remoteAddress = remoteAddress;
        this.headers = headers != null ? new HashMap<>(headers) : new HashMap<>();
        this.correlationId = correlationId != null ? correlationId : UUID.randomUUID().toString();
        this.priority = 0; // Default priority
    }
    
    /**
     * Creates a new NetworkMessage with full tracing context and message properties.
     *
     * @param message The message payload
     * @param remoteAddress The remote socket address
     * @param headers Message headers for metadata propagation
     * @param correlationId Correlation ID for request tracking
     * @param traceId OpenTelemetry trace ID
     * @param spanId OpenTelemetry span ID
     * @param traceFlags OpenTelemetry trace flags
     * @param priority Message priority
     * @param routingKey Routing information for message brokers
     * @param ttlMillis Time-to-live in milliseconds (0 means no expiration)
     */
    public NetworkMessage(Object message, SocketAddress remoteAddress, 
                         Map<String, String> headers, String correlationId,
                         String traceId, String spanId, byte traceFlags,
                         int priority, String routingKey, long ttlMillis) {
        this.message = message;
        this.remoteAddress = remoteAddress;
        this.headers = headers != null ? new HashMap<>(headers) : new HashMap<>();
        this.correlationId = correlationId != null ? correlationId : UUID.randomUUID().toString();
        this.traceId = traceId;
        this.spanId = spanId;
        this.traceFlags = traceFlags;
        this.priority = priority;
        this.routingKey = routingKey;
        this.expirationTime = ttlMillis > 0 ? Instant.now().plusMillis(ttlMillis) : null;
    }
    
    /**
     * Gets the remote socket address.
     *
     * @return The remote socket address
     */
    public SocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    /**
     * Gets the message payload.
     *
     * @return The message payload
     */
    public Object getMessage() {
        return message;
    }
    
    /**
     * Gets the OpenTelemetry trace ID.
     *
     * @return The trace ID
     */
    public String getTraceId() {
        return traceId;
    }
    
    /**
     * Sets the OpenTelemetry trace ID.
     *
     * @param traceId The trace ID to set
     */
    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }
    
    /**
     * Gets the OpenTelemetry span ID.
     *
     * @return The span ID
     */
    public String getSpanId() {
        return spanId;
    }
    
    /**
     * Sets the OpenTelemetry span ID.
     *
     * @param spanId The span ID to set
     */
    public void setSpanId(String spanId) {
        this.spanId = spanId;
    }
    
    /**
     * Gets the OpenTelemetry trace flags.
     *
     * @return The trace flags
     */
    public byte getTraceFlags() {
        return traceFlags;
    }
    
    /**
     * Sets the OpenTelemetry trace flags.
     *
     * @param traceFlags The trace flags to set
     */
    public void setTraceFlags(byte traceFlags) {
        this.traceFlags = traceFlags;
    }
    
    /**
     * Gets the correlation ID for request tracking.
     *
     * @return The correlation ID
     */
    public String getCorrelationId() {
        return correlationId;
    }
    
    /**
     * Sets the correlation ID for request tracking.
     *
     * @param correlationId The correlation ID to set
     */
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }
    
    /**
     * Gets the message headers for metadata propagation.
     *
     * @return An unmodifiable view of the headers map
     */
    public Map<String, String> getHeaders() {
        return Collections.unmodifiableMap(headers);
    }
    
    /**
     * Adds a header to the message.
     *
     * @param key The header key
     * @param value The header value
     */
    public void addHeader(String key, String value) {
        headers.put(key, value);
    }
    
    /**
     * Adds multiple headers to the message.
     *
     * @param headers The headers to add
     */
    public void addHeaders(Map<String, String> headers) {
        if (headers != null) {
            this.headers.putAll(headers);
        }
    }
    
    /**
     * Gets a specific header value.
     *
     * @param key The header key
     * @return The header value, or null if not found
     */
    public String getHeader(String key) {
        return headers.get(key);
    }
    
    /**
     * Gets the message priority.
     *
     * @return The message priority
     */
    public int getPriority() {
        return priority;
    }
    
    /**
     * Sets the message priority.
     *
     * @param priority The priority to set
     */
    public void setPriority(int priority) {
        this.priority = priority;
    }
    
    /**
     * Gets the routing key for message brokers.
     *
     * @return The routing key
     */
    public String getRoutingKey() {
        return routingKey;
    }
    
    /**
     * Sets the routing key for message brokers.
     *
     * @param routingKey The routing key to set
     */
    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }
    
    /**
     * Gets the expiration time (TTL).
     *
     * @return The expiration time, or null if the message doesn't expire
     */
    public Instant getExpirationTime() {
        return expirationTime;
    }
    
    /**
     * Sets the expiration time based on a TTL in milliseconds.
     *
     * @param ttlMillis Time-to-live in milliseconds (0 means no expiration)
     */
    public void setTimeToLive(long ttlMillis) {
        this.expirationTime = ttlMillis > 0 ? Instant.now().plusMillis(ttlMillis) : null;
    }
    
    /**
     * Checks if the message has expired.
     *
     * @return true if the message has expired, false otherwise
     */
    public boolean isExpired() {
        return expirationTime != null && Instant.now().isAfter(expirationTime);
    }
    
    /**
     * Creates a builder for NetworkMessage.
     *
     * @param message The message payload
     * @param remoteAddress The remote socket address
     * @return A new NetworkMessageBuilder
     */
    public static NetworkMessageBuilder builder(Object message, SocketAddress remoteAddress) {
        return new NetworkMessageBuilder(message, remoteAddress);
    }
    
    /**
     * Builder class for NetworkMessage to support fluent API.
     */
    public static class NetworkMessageBuilder {
        private final Object message;
        private final SocketAddress remoteAddress;
        private Map<String, String> headers = new HashMap<>();
        private String correlationId;
        private String traceId;
        private String spanId;
        private byte traceFlags;
        private int priority;
        private String routingKey;
        private long ttlMillis;
        
        private NetworkMessageBuilder(Object message, SocketAddress remoteAddress) {
            this.message = message;
            this.remoteAddress = remoteAddress;
        }
        
        public NetworkMessageBuilder headers(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }
        
        public NetworkMessageBuilder addHeader(String key, String value) {
            this.headers.put(key, value);
            return this;
        }
        
        public NetworkMessageBuilder correlationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }
        
        public NetworkMessageBuilder traceId(String traceId) {
            this.traceId = traceId;
            return this;
        }
        
        public NetworkMessageBuilder spanId(String spanId) {
            this.spanId = spanId;
            return this;
        }
        
        public NetworkMessageBuilder traceFlags(byte traceFlags) {
            this.traceFlags = traceFlags;
            return this;
        }
        
        public NetworkMessageBuilder priority(int priority) {
            this.priority = priority;
            return this;
        }
        
        public NetworkMessageBuilder routingKey(String routingKey) {
            this.routingKey = routingKey;
            return this;
        }
        
        public NetworkMessageBuilder timeToLive(long ttlMillis) {
            this.ttlMillis = ttlMillis;
            return this;
        }
        
        public NetworkMessage build() {
            return new NetworkMessage(
                message, remoteAddress, headers, correlationId,
                traceId, spanId, traceFlags, priority, routingKey, ttlMillis);
        }
    }
}