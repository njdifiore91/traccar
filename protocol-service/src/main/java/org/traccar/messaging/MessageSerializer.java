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

/**
 * Interface for serializing messages to byte arrays for transmission over a message broker.
 * Supports multiple serialization formats (Protocol Buffers, JSON) and handles schema versioning.
 */
public interface MessageSerializer {

    /**
     * Serialize an object to a byte array.
     *
     * @param object Object to serialize
     * @return Serialized byte array
     * @throws Exception If there is an error during serialization
     */
    byte[] serialize(Object object) throws Exception;

    /**
     * Get the content type of the serialized data.
     * This is used by message consumers to determine how to deserialize the data.
     *
     * @return Content type string (e.g., "application/json", "application/protobuf")
     */
    String getContentType();

    /**
     * Get the schema version used for serialization.
     * This is used for backward compatibility when the schema changes.
     *
     * @return Schema version string
     */
    String getSchemaVersion();

    /**
     * Validate that an object conforms to the expected schema.
     *
     * @param object Object to validate
     * @return True if the object is valid, false otherwise
     */
    boolean validate(Object object);
}