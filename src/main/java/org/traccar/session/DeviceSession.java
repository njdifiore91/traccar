/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.session;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import org.traccar.BasePipelineFactory;
import org.traccar.Protocol;
import org.traccar.model.Command;

import java.io.Serializable;
import java.net.SocketAddress;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class DeviceSession implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long deviceId;
    private final String uniqueId;
    private final String model;
    private final Protocol protocol;
    @JsonIgnore
    private final transient Channel channel;
    private final SocketAddress remoteAddress;
    
    // Added for distributed tracing
    private final String correlationId;
    
    // Added for session expiration and versioning
    private final Instant creationTime;
    private Instant lastAccessTime;
    private long version;

    public DeviceSession(
            long deviceId, String uniqueId, String model,
            Protocol protocol, Channel channel, SocketAddress remoteAddress) {
        this.deviceId = deviceId;
        this.uniqueId = uniqueId;
        this.model = model;
        this.protocol = protocol;
        this.channel = channel;
        this.remoteAddress = remoteAddress;
        
        // Initialize new fields
        this.correlationId = UUID.randomUUID().toString();
        this.creationTime = Instant.now();
        this.lastAccessTime = Instant.now();
        this.version = 0;
    }

    public long getDeviceId() {
        return deviceId;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public String getModel() {
        return model;
    }

    @JsonIgnore
    public Channel getChannel() {
        return channel;
    }

    public ConnectionKey getConnectionKey() {
        return new ConnectionKey(channel, remoteAddress);
    }

    public boolean supportsLiveCommands() {
        return BasePipelineFactory.getHandler(channel.pipeline(), HttpRequestDecoder.class) == null;
    }

    public void sendCommand(Command command) {
        // Update last access time when sending commands
        updateLastAccessTime();
        protocol.sendDataCommand(channel, remoteAddress, command);
    }
    
    /**
     * Get the correlation ID for distributed tracing
     * 
     * @return correlation ID as a string
     */
    public String getCorrelationId() {
        return correlationId;
    }
    
    /**
     * Get the creation time of this session
     * 
     * @return creation time as Instant
     */
    public Instant getCreationTime() {
        return creationTime;
    }
    
    /**
     * Get the last access time of this session
     * 
     * @return last access time as Instant
     */
    public Instant getLastAccessTime() {
        return lastAccessTime;
    }
    
    /**
     * Update the last access time to current time
     */
    public void updateLastAccessTime() {
        this.lastAccessTime = Instant.now();
    }
    
    /**
     * Get the current version of this session
     * 
     * @return version number
     */
    public long getVersion() {
        return version;
    }
    
    /**
     * Increment the version number
     * 
     * @return new version number
     */
    public long incrementVersion() {
        return ++version;
    }

    public static final String KEY_TIMEZONE = "timezone";

    // Changed to ConcurrentHashMap for thread safety in distributed environment
    private final Map<String, Object> locals = new ConcurrentHashMap<>();

    public boolean contains(String key) {
        updateLastAccessTime();
        return locals.containsKey(key);
    }

    public void set(String key, Object value) {
        updateLastAccessTime();
        incrementVersion();
        if (value != null) {
            locals.put(key, value);
        } else {
            locals.remove(key);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        updateLastAccessTime();
        return (T) locals.get(key);
    }
    
    /**
     * Get a copy of all local attributes
     * 
     * @return Map of all local attributes
     */
    public Map<String, Object> getLocals() {
        updateLastAccessTime();
        return new HashMap<>(locals);
    }
    
    /**
     * Set multiple local attributes at once
     * 
     * @param attributes Map of attributes to set
     */
    public void setLocals(Map<String, Object> attributes) {
        updateLastAccessTime();
        incrementVersion();
        if (attributes != null) {
            locals.putAll(attributes);
        }
    }
    
    /**
     * Migrate session data from another session
     * 
     * @param other The other session to migrate data from
     */
    public void migrateFrom(DeviceSession other) {
        if (other != null) {
            updateLastAccessTime();
            incrementVersion();
            locals.putAll(other.getLocals());
        }
    }
}