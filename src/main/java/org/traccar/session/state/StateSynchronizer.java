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
package org.traccar.session.state;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.session.cache.RedisManager;

/**
 * StateSynchronizer is responsible for synchronizing state across multiple instances
 * using Redis as a distributed cache. It handles the communication with Redis for
 * state updates and retrieval.
 */
@Singleton
public class StateSynchronizer {

    private static final Logger LOGGER = LoggerFactory.getLogger(StateSynchronizer.class);
    private static final String MOTION_STATE_KEY_PREFIX = "motion:state:";

    private final RedisManager redisManager;
    private final Config config;

    /**
     * Constructs a new StateSynchronizer with the required dependencies.
     *
     * @param redisManager For Redis operations
     * @param config System configuration
     */
    @Inject
    public StateSynchronizer(RedisManager redisManager, Config config) {
        this.redisManager = redisManager;
        this.config = config;
    }

    /**
     * Synchronizes the motion state for a device across all instances.
     *
     * @param state The motion state to synchronize
     * @param deviceId The device ID
     */
    public void synchronizeMotionState(MotionState state, long deviceId) {
        String key = MOTION_STATE_KEY_PREFIX + deviceId;
        
        // Serialize and store the state in Redis
        redisManager.set(key, serializeMotionState(state));
        
        // Set expiration time to avoid stale data
        int expirationTime = config.getInteger("state.redis.expirationTime", 86400); // Default: 1 day
        redisManager.expire(key, expirationTime);
        
        LOGGER.debug("Synchronized motion state for device {}", deviceId);
    }

    /**
     * Retrieves the motion state for a device from the distributed cache.
     *
     * @param deviceId The device ID
     * @return The motion state, or null if not found
     */
    public MotionState getMotionState(long deviceId) {
        String key = MOTION_STATE_KEY_PREFIX + deviceId;
        String serializedState = redisManager.get(key);
        
        if (serializedState != null) {
            return deserializeMotionState(serializedState);
        }
        
        return null;
    }

    /**
     * Serializes a motion state object to a string representation for storage.
     *
     * @param state The motion state to serialize
     * @return The serialized state as a string
     */
    private String serializeMotionState(MotionState state) {
        // Simple serialization format: motionState,motionStreak,motionTime,motionDistance
        StringBuilder builder = new StringBuilder();
        builder.append(state.getMotionState()).append(',');
        builder.append(state.getMotionStreak()).append(',');
        
        if (state.getMotionTime() != null) {
            builder.append(state.getMotionTime().getTime());
        } else {
            builder.append("null");
        }
        
        builder.append(',').append(state.getMotionDistance());
        
        return builder.toString();
    }

    /**
     * Deserializes a string representation back to a motion state object.
     *
     * @param serialized The serialized state string
     * @return The deserialized motion state
     */
    private MotionState deserializeMotionState(String serialized) {
        String[] parts = serialized.split(",");
        if (parts.length != 4) {
            throw new IllegalArgumentException("Invalid serialized motion state format");
        }
        
        MotionState state = new MotionState();
        state.setMotionState(Boolean.parseBoolean(parts[0]));
        state.setMotionStreak(Boolean.parseBoolean(parts[1]));
        
        if (!"null".equals(parts[2])) {
            state.setMotionTime(new java.util.Date(Long.parseLong(parts[2])));
        }
        
        state.setMotionDistance(Double.parseDouble(parts[3]));
        
        return state;
    }
}