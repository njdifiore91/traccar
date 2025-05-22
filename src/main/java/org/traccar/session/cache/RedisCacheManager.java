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
package org.traccar.session.cache;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Redis implementation of the CacheManager interface.
 * This class provides a distributed cache implementation using Redis
 * for storing device-related data across multiple service instances.
 */
public class RedisCacheManager implements CacheManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisCacheManager.class);

    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;
    private final long ttlSeconds;

    /**
     * Constructor for RedisCacheManager.
     *
     * @param connection Redis connection
     * @param ttlSeconds Time-to-live in seconds for cache entries
     */
    public RedisCacheManager(StatefulRedisConnection<String, String> connection, long ttlSeconds) {
        this.connection = connection;
        this.commands = connection.sync();
        this.ttlSeconds = ttlSeconds;
        LOGGER.info("Redis cache manager initialized with TTL: {} seconds", ttlSeconds);
    }

    /**
     * Generate a Redis key for a device.
     *
     * @param deviceId Device ID
     * @return Redis key
     */
    private String getDeviceKey(long deviceId) {
        return "device:" + deviceId;
    }

    /**
     * Generate a Redis key for a device's clients.
     *
     * @param deviceId Device ID
     * @return Redis key for clients
     */
    private String getDeviceClientsKey(long deviceId) {
        return getDeviceKey(deviceId) + ":clients";
    }

    @Override
    public synchronized void addDevice(long deviceId, Object object) {
        String clientId = object.toString();
        String deviceClientsKey = getDeviceClientsKey(deviceId);
        
        // Add client to the set of clients for this device
        commands.sadd(deviceClientsKey, clientId);
        
        // Set or refresh TTL
        commands.expire(deviceClientsKey, ttlSeconds);
        
        LOGGER.debug("Added client {} to device {}", clientId, deviceId);
    }

    @Override
    public synchronized void removeDevice(long deviceId, Object object) {
        String clientId = object.toString();
        String deviceClientsKey = getDeviceClientsKey(deviceId);
        
        // Remove client from the set of clients for this device
        commands.srem(deviceClientsKey, clientId);
        
        // If no clients left, remove the key
        if (commands.scard(deviceClientsKey) == 0) {
            commands.del(deviceClientsKey);
            LOGGER.debug("Removed last client for device {}", deviceId);
        } else {
            LOGGER.debug("Removed client {} from device {}", clientId, deviceId);
        }
    }

    @Override
    public synchronized boolean containsDevice(long deviceId) {
        String deviceClientsKey = getDeviceClientsKey(deviceId);
        return commands.exists(deviceClientsKey) > 0;
    }

    @Override
    public synchronized Set<Long> getDevices() {
        Set<Long> devices = new HashSet<>();
        Set<String> keys = commands.keys("device:*:clients");
        
        for (String key : keys) {
            try {
                // Extract device ID from key pattern "device:<id>:clients"
                String deviceIdStr = key.split(":")[1];
                devices.add(Long.parseLong(deviceIdStr));
            } catch (Exception e) {
                LOGGER.warn("Failed to parse device ID from key: {}", key, e);
            }
        }
        
        return devices;
    }

    @Override
    public synchronized void updateDevice(long deviceId) {
        String deviceClientsKey = getDeviceClientsKey(deviceId);
        
        // Refresh TTL for the device clients key
        if (commands.exists(deviceClientsKey) > 0) {
            commands.expire(deviceClientsKey, ttlSeconds);
            LOGGER.debug("Updated TTL for device {}", deviceId);
        }
    }

    /**
     * Close the Redis connection when the cache manager is no longer needed.
     */
    public void close() {
        if (connection != null) {
            connection.close();
            LOGGER.info("Redis cache connection closed");
        }
    }
}