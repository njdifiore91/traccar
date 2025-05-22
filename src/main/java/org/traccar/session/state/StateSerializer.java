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
package org.traccar.session.state;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides serialization and deserialization capabilities for state objects in the distributed microservices architecture.
 * This utility class handles the conversion of state objects (MotionState, OverspeedState) to and from serialized formats
 * for storage in Redis and transmission via message broker.
 */
public class StateSerializer {

    private static final Logger LOGGER = Logger.getLogger(StateSerializer.class.getName());
    
    private static final String VERSION_PREFIX = "v1:";
    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .build()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private StateSerializer() {
        // Utility class
    }

    /**
     * Serializes a MotionState object to a string representation for storage or transmission.
     * 
     * @param state The MotionState object to serialize
     * @return String representation of the serialized state, or null if serialization fails
     */
    public static String serializeMotionState(MotionState state) {
        if (state == null) {
            return null;
        }
        try {
            String json = OBJECT_MAPPER.writeValueAsString(state);
            return VERSION_PREFIX + Base64.getEncoder().encodeToString(json.getBytes());
        } catch (JsonProcessingException e) {
            LOGGER.log(Level.WARNING, "Failed to serialize MotionState", e);
            return null;
        }
    }

    /**
     * Deserializes a string representation back to a MotionState object.
     * 
     * @param serialized The serialized string representation of the state
     * @return The deserialized MotionState object, or null if deserialization fails
     */
    public static MotionState deserializeMotionState(String serialized) {
        if (serialized == null || serialized.isEmpty()) {
            return null;
        }
        
        try {
            // Handle versioning for backward compatibility
            String data = serialized;
            if (serialized.startsWith(VERSION_PREFIX)) {
                data = serialized.substring(VERSION_PREFIX.length());
                return deserializeV1MotionState(data);
            } else {
                // Legacy format handling if needed in the future
                return deserializeLegacyMotionState(data);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to deserialize MotionState", e);
            return null;
        }
    }

    private static MotionState deserializeV1MotionState(String data) throws IOException {
        byte[] decoded = Base64.getDecoder().decode(data);
        String json = new String(decoded);
        MotionState state = OBJECT_MAPPER.readValue(json, MotionState.class);
        validateMotionState(state);
        return state;
    }

    private static MotionState deserializeLegacyMotionState(String data) {
        // For future backward compatibility if serialization format changes
        // Currently just uses the current version format
        try {
            return deserializeV1MotionState(data);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to deserialize legacy MotionState format", e);
            return null;
        }
    }

    /**
     * Serializes an OverspeedState object to a string representation for storage or transmission.
     * 
     * @param state The OverspeedState object to serialize
     * @return String representation of the serialized state, or null if serialization fails
     */
    public static String serializeOverspeedState(OverspeedState state) {
        if (state == null) {
            return null;
        }
        try {
            String json = OBJECT_MAPPER.writeValueAsString(state);
            return VERSION_PREFIX + Base64.getEncoder().encodeToString(json.getBytes());
        } catch (JsonProcessingException e) {
            LOGGER.log(Level.WARNING, "Failed to serialize OverspeedState", e);
            return null;
        }
    }

    /**
     * Deserializes a string representation back to an OverspeedState object.
     * 
     * @param serialized The serialized string representation of the state
     * @return The deserialized OverspeedState object, or null if deserialization fails
     */
    public static OverspeedState deserializeOverspeedState(String serialized) {
        if (serialized == null || serialized.isEmpty()) {
            return null;
        }
        
        try {
            // Handle versioning for backward compatibility
            String data = serialized;
            if (serialized.startsWith(VERSION_PREFIX)) {
                data = serialized.substring(VERSION_PREFIX.length());
                return deserializeV1OverspeedState(data);
            } else {
                // Legacy format handling if needed in the future
                return deserializeLegacyOverspeedState(data);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to deserialize OverspeedState", e);
            return null;
        }
    }

    private static OverspeedState deserializeV1OverspeedState(String data) throws IOException {
        byte[] decoded = Base64.getDecoder().decode(data);
        String json = new String(decoded);
        OverspeedState state = OBJECT_MAPPER.readValue(json, OverspeedState.class);
        validateOverspeedState(state);
        return state;
    }

    private static OverspeedState deserializeLegacyOverspeedState(String data) {
        // For future backward compatibility if serialization format changes
        // Currently just uses the current version format
        try {
            return deserializeV1OverspeedState(data);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to deserialize legacy OverspeedState format", e);
            return null;
        }
    }

    /**
     * Validates a deserialized MotionState object to ensure it meets security requirements.
     * 
     * @param state The MotionState object to validate
     * @throws IllegalArgumentException if validation fails
     */
    private static void validateMotionState(MotionState state) {
        if (state == null) {
            throw new IllegalArgumentException("MotionState cannot be null");
        }
        
        // Add additional validation logic as needed
        // For example, checking for reasonable values or required fields
    }

    /**
     * Validates a deserialized OverspeedState object to ensure it meets security requirements.
     * 
     * @param state The OverspeedState object to validate
     * @throws IllegalArgumentException if validation fails
     */
    private static void validateOverspeedState(OverspeedState state) {
        if (state == null) {
            throw new IllegalArgumentException("OverspeedState cannot be null");
        }
        
        // Add additional validation logic as needed
        // For example, checking for reasonable values or required fields
    }
}