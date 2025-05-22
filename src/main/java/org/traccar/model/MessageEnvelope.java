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
package org.traccar.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * A generic wrapper for model objects sent via message broker (Kafka/RabbitMQ).
 * This class provides a standardized container for any model object being transmitted
 * between microservices, including metadata for routing, versioning, and tracing.
 *
 * @param <T> The type of model object being wrapped
 */
public class MessageEnvelope<T> {

    /**
     * Current schema version for backward compatibility.
     */
    public static final String CURRENT_SCHEMA_VERSION = "1.0";

    private String messageId;
    private String messageType;
    private String schemaVersion;
    private Date timestamp;
    private Map<String, String> tracingContext;
    private Map<String, Object> headers;
    private T payload;

    /**
     * Default constructor for serialization frameworks.
     */
    public MessageEnvelope() {
        this.schemaVersion = CURRENT_SCHEMA_VERSION;
        this.timestamp = new Date();
        this.tracingContext = new HashMap<>();
        this.headers = new HashMap<>();
    }

    /**
     * Creates a new message envelope with the specified payload and type.
     *
     * @param payload The model object to wrap
     * @param messageType The type of message for routing and filtering
     */
    public MessageEnvelope(T payload, String messageType) {
        this();
        this.payload = payload;
        this.messageType = messageType;
    }

    /**
     * Creates a new message envelope with the specified payload, type, and message ID.
     *
     * @param payload The model object to wrap
     * @param messageType The type of message for routing and filtering
     * @param messageId Unique identifier for the message
     */
    public MessageEnvelope(T payload, String messageType, String messageId) {
        this(payload, messageType);
        this.messageId = messageId;
    }

    /**
     * Gets the unique identifier for this message.
     *
     * @return The message ID
     */
    public String getMessageId() {
        return messageId;
    }

    /**
     * Sets the unique identifier for this message.
     *
     * @param messageId The message ID
     */
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    /**
     * Gets the message type used for routing and filtering.
     *
     * @return The message type
     */
    public String getMessageType() {
        return messageType;
    }

    /**
     * Sets the message type used for routing and filtering.
     *
     * @param messageType The message type
     */
    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    /**
     * Gets the schema version for backward compatibility.
     *
     * @return The schema version
     */
    public String getSchemaVersion() {
        return schemaVersion;
    }

    /**
     * Sets the schema version for backward compatibility.
     *
     * @param schemaVersion The schema version
     */
    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    /**
     * Gets the timestamp when this message was created.
     *
     * @return The message timestamp
     */
    public Date getTimestamp() {
        return timestamp;
    }

    /**
     * Sets the timestamp when this message was created.
     *
     * @param timestamp The message timestamp
     */
    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Gets the distributed tracing context for request correlation.
     *
     * @return The tracing context map
     */
    public Map<String, String> getTracingContext() {
        return tracingContext;
    }

    /**
     * Sets the distributed tracing context for request correlation.
     *
     * @param tracingContext The tracing context map
     */
    public void setTracingContext(Map<String, String> tracingContext) {
        this.tracingContext = tracingContext;
    }

    /**
     * Adds a tracing context entry for distributed request correlation.
     *
     * @param key The context key
     * @param value The context value
     */
    public void addTracingContext(String key, String value) {
        if (this.tracingContext == null) {
            this.tracingContext = new HashMap<>();
        }
        this.tracingContext.put(key, value);
    }

    /**
     * Gets the message headers for additional metadata.
     *
     * @return The headers map
     */
    public Map<String, Object> getHeaders() {
        return headers;
    }

    /**
     * Sets the message headers for additional metadata.
     *
     * @param headers The headers map
     */
    public void setHeaders(Map<String, Object> headers) {
        this.headers = headers;
    }

    /**
     * Adds a header entry for additional message metadata.
     *
     * @param key The header key
     * @param value The header value
     */
    public void addHeader(String key, Object value) {
        if (this.headers == null) {
            this.headers = new HashMap<>();
        }
        this.headers.put(key, value);
    }

    /**
     * Gets the wrapped model object payload.
     *
     * @return The payload
     */
    public T getPayload() {
        return payload;
    }

    /**
     * Sets the wrapped model object payload.
     *
     * @param payload The payload
     */
    public void setPayload(T payload) {
        this.payload = payload;
    }
}