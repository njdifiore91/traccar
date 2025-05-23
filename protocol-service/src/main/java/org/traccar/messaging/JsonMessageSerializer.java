/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.messaging;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.traccar.model.Position;
import org.traccar.model.Device;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements the MessageSerializer interface for JSON, providing human-readable serialization of messages.
 * Uses Jackson for JSON processing and handles schema versioning and validation.
 */
@Singleton
public class JsonMessageSerializer implements MessageSerializer {

    private static final String CONTENT_TYPE = "application/json";
    private static final String SCHEMA_VERSION = "1.0";
    private static final String SCHEMA_VERSION_FIELD = "_schemaVersion";
    private static final String MESSAGE_TYPE_FIELD = "_messageType";
    private static final Logger LOGGER = Logger.getLogger(JsonMessageSerializer.class.getName());

    private final ObjectMapper objectMapper;
    private final boolean prettyPrint;

    /**
     * Constructs a JsonMessageSerializer with default settings.
     */
    @Inject
    public JsonMessageSerializer() {
        this(false);
    }

    /**
     * Constructs a JsonMessageSerializer with the specified pretty print setting.
     *
     * @param prettyPrint Whether to format the JSON output for human readability
     */
    public JsonMessageSerializer(boolean prettyPrint) {
        this.prettyPrint = prettyPrint;
        this.objectMapper = new ObjectMapper();

        // Configure ObjectMapper
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        if (prettyPrint) {
            objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        }
    }

    @Override
    public byte[] serialize(Object object) throws Exception {
        if (object == null) {
            throw new IllegalArgumentException("Cannot serialize null object");
        }

        try {
            // Create a wrapper object to include schema version and message type
            ObjectNode rootNode;
            if (object instanceof ObjectNode) {
                rootNode = (ObjectNode) object;
            } else {
                rootNode = objectMapper.valueToTree(object);
            }

            // Add schema version and message type
            rootNode.put(SCHEMA_VERSION_FIELD, SCHEMA_VERSION);
            rootNode.put(MESSAGE_TYPE_FIELD, getMessageType(object));

            // Validate before serializing
            if (!validate(object)) {
                LOGGER.warning("Serializing object that failed validation: " + object.getClass().getName());
            }

            return objectMapper.writeValueAsBytes(rootNode);
        } catch (JsonProcessingException e) {
            LOGGER.log(Level.SEVERE, "Failed to serialize object: " + object.getClass().getName(), e);
            throw new Exception("Failed to serialize object: " + e.getMessage(), e);
        }
    }

    @Override
    public String getContentType() {
        return CONTENT_TYPE;
    }

    @Override
    public String getSchemaVersion() {
        return SCHEMA_VERSION;
    }

    @Override
    public boolean validate(Object object) {
        if (object == null) {
            return false;
        }

        try {
            // For validation, we simply check if the object can be serialized to JSON
            // This ensures that the object is compatible with JSON serialization
            objectMapper.writeValueAsString(object);
            return true;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to validate object: " + object.getClass().getName(), e);
            return false;
        }
    }

    /**
     * Gets the message type string for an object.
     * This is used to identify the type of message during deserialization.
     *
     * @param object Object to get message type for
     * @return Message type string
     */
    private String getMessageType(Object object) {
        if (object instanceof Position) {
            return "position";
        } else if (object instanceof Device) {
            return "device";
        } else {
            return object.getClass().getSimpleName().toLowerCase();
        }
    }

    /**
     * Creates a new JsonMessageSerializer with pretty printing enabled.
     * Useful for debugging purposes.
     *
     * @return A new JsonMessageSerializer with pretty printing enabled
     */
    public JsonMessageSerializer withPrettyPrinting() {
        return new JsonMessageSerializer(true);
    }
    
    /**
     * Adds schema version information to a JSON node.
     * This is useful for ensuring backward compatibility when the schema changes.
     *
     * @param node The JSON node to add schema version information to
     * @param version The schema version to add
     * @return The JSON node with schema version information added
     */
    public static ObjectNode addSchemaVersion(ObjectNode node, String version) {
        if (node != null) {
            node.put(SCHEMA_VERSION_FIELD, version);
        }
        return node;
    }
    
    /**
     * Gets the content type with charset information.
     * This is useful for HTTP headers and other contexts where charset information is needed.
     *
     * @return Content type string with charset information
     */
    public String getContentTypeWithCharset() {
        return CONTENT_TYPE + "; charset=utf-8";
    }
    
    /**
     * Serializes an object to a pretty-printed JSON string for debugging purposes.
     *
     * @param object Object to serialize
     * @return Pretty-printed JSON string
     * @throws JsonProcessingException If there is an error during serialization
     */
    public String serializeForDebugging(Object object) throws JsonProcessingException {
        if (object == null) {
            throw new IllegalArgumentException("Cannot serialize null object");
        }
        
        ObjectMapper debugMapper = new ObjectMapper();
        debugMapper.enable(SerializationFeature.INDENT_OUTPUT);
        debugMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        
        return debugMapper.writeValueAsString(object);
    }
}