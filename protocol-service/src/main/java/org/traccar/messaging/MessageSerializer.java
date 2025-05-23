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
package org.traccar.messaging;

import java.util.Map;

/**
 * Interface for serializing and deserializing messages for transmission over a message broker.
 * <p>
 * This interface provides methods for converting Java objects to byte arrays and vice versa,
 * with support for schema versioning, content type information, and validation.
 * <p>
 * Implementations of this interface should handle serialization in different formats
 * (e.g., Protocol Buffers, JSON) and provide appropriate content type information.
 */
public interface MessageSerializer {

    /**
     * Serializes an object to a byte array for transmission over a message broker.
     *
     * @param object The object to serialize
     * @param <T>    The type of the object
     * @return The serialized byte array
     * @throws SerializationException If serialization fails
     */
    <T> byte[] serialize(T object) throws SerializationException;

    /**
     * Serializes an object to a byte array with additional headers.
     *
     * @param object  The object to serialize
     * @param headers Additional headers to include with the serialized message
     * @param <T>     The type of the object
     * @return The serialized byte array
     * @throws SerializationException If serialization fails
     */
    <T> byte[] serialize(T object, Map<String, Object> headers) throws SerializationException;

    /**
     * Deserializes a byte array to an object of the specified type.
     *
     * @param bytes The byte array to deserialize
     * @param type  The class of the object to deserialize to
     * @param <T>   The type of the object
     * @return The deserialized object
     * @throws SerializationException If deserialization fails
     */
    <T> T deserialize(byte[] bytes, Class<T> type) throws SerializationException;

    /**
     * Deserializes a byte array to an object of the specified type, with additional headers.
     *
     * @param bytes   The byte array to deserialize
     * @param type    The class of the object to deserialize to
     * @param headers Headers associated with the message, which may be used for deserialization
     * @param <T>     The type of the object
     * @return The deserialized object
     * @throws SerializationException If deserialization fails
     */
    <T> T deserialize(byte[] bytes, Class<T> type, Map<String, Object> headers) throws SerializationException;

    /**
     * Gets the content type of the serialized data.
     * <p>
     * This is used by message consumers to determine how to deserialize the message.
     * Examples include "application/json", "application/protobuf", etc.
     *
     * @return The content type string
     */
    String getContentType();

    /**
     * Gets the schema version of the serialized data.
     * <p>
     * This is used for backward compatibility when the schema evolves over time.
     *
     * @return The schema version string
     */
    String getSchemaVersion();

    /**
     * Validates that an object conforms to the expected schema.
     *
     * @param object The object to validate
     * @param <T>    The type of the object
     * @return true if the object is valid, false otherwise
     */
    <T> boolean validate(T object);

    /**
     * Validates that a serialized byte array conforms to the expected schema.
     *
     * @param bytes The serialized byte array to validate
     * @return true if the serialized data is valid, false otherwise
     */
    boolean validate(byte[] bytes);

    /**
     * Exception thrown when serialization or deserialization fails.
     */
    class SerializationException extends Exception {

        /**
         * Creates a new SerializationException with the specified message.
         *
         * @param message The exception message
         */
        public SerializationException(String message) {
            super(message);
        }

        /**
         * Creates a new SerializationException with the specified message and cause.
         *
         * @param message The exception message
         * @param cause   The cause of the exception
         */
        public SerializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}