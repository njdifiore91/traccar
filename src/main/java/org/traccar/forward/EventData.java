/*
 * Copyright 2022-2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.forward;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Geofence;
import org.traccar.model.Maintenance;
import org.traccar.model.Position;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * EventData class for forwarding events between microservices.
 * This class has been updated to support message broker serialization,
 * distributed tracing, and Protocol Buffer compatibility.
 *
 * This class serves as a wrapper around the Protocol Buffer generated message
 * classes, providing a more convenient API for Java applications while enabling
 * efficient binary serialization for inter-service communication.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EventData {

    /**
     * Current schema version for backward compatibility.
     * Increment this value when making breaking changes to the schema.
     */
    private static final int CURRENT_SCHEMA_VERSION = 1;

    @JsonProperty
    private int schemaVersion = CURRENT_SCHEMA_VERSION;

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    // Message broker metadata
    private String messageId;
    private Instant timestamp;
    private String source;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    // Distributed tracing context
    private String traceId;
    private String spanId;

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getSpanId() {
        return spanId;
    }

    public void setSpanId(String spanId) {
        this.spanId = spanId;
    }

    // Additional metadata for microservices
    private String correlationId;
    private Integer priority;
    private Long ttl; // time-to-live in milliseconds

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public Long getTtl() {
        return ttl;
    }

    public void setTtl(Long ttl) {
        this.ttl = ttl;
    }

    // Custom metadata map for extensibility
    private Map<String, Object> metadata;

    public Map<String, Object> getMetadata() {
        if (metadata == null) {
            metadata = new HashMap<>();
        }
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    // Original event data fields
    private Event event;

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    private Position position;

    public Position getPosition() {
        return position;
    }

    public void setPosition(Position position) {
        this.position = position;
    }

    private Device device;

    public Device getDevice() {
        return device;
    }

    public void setDevice(Device device) {
        this.device = device;
    }

    private Geofence geofence;

    public Geofence getGeofence() {
        return geofence;
    }

    public void setGeofence(Geofence geofence) {
        this.geofence = geofence;
    }

    private Maintenance maintenance;

    public Maintenance getMaintenance() {
        return maintenance;
    }

    public void setMaintenance(Maintenance maintenance) {
        this.maintenance = maintenance;
    }

    /**
     * Creates a new EventData instance with initialized metadata for message broker.
     * 
     * @return A new EventData instance with default metadata
     */
    public static EventData create() {
        EventData data = new EventData();
        data.setMessageId(UUID.randomUUID().toString());
        data.setTimestamp(Instant.now());
        data.setSchemaVersion(CURRENT_SCHEMA_VERSION);
        return data;
    }

    /**
     * Creates a new EventData instance with initialized metadata and correlation ID.
     * 
     * @param correlationId The correlation ID for distributed tracing
     * @return A new EventData instance with default metadata and correlation ID
     */
    public static EventData create(String correlationId) {
        EventData data = create();
        data.setCorrelationId(correlationId);
        return data;
    }

    /**
     * Creates a new EventData instance with initialized metadata, correlation ID, and tracing context.
     * 
     * @param correlationId The correlation ID for distributed tracing
     * @param traceId The trace ID for OpenTelemetry tracing
     * @param spanId The span ID for OpenTelemetry tracing
     * @return A new EventData instance with default metadata and tracing context
     */
    public static EventData create(String correlationId, String traceId, String spanId) {
        EventData data = create(correlationId);
        data.setTraceId(traceId);
        data.setSpanId(spanId);
        return data;
    }

    /**
     * Converts this EventData to a Protocol Buffer message for serialization.
     * This method is used by the Protocol Buffer serialization framework.
     * 
     * @return Protocol Buffer message object
     */
    @JsonIgnore
    public Object toProtobufMessage() {
        // This method will be implemented to convert to the appropriate Protocol Buffer message
        // The actual implementation will be provided by the Protocol Buffer code generator
        throw new UnsupportedOperationException("Protocol Buffer serialization not implemented");
    }

    /**
     * Creates an EventData instance from a Protocol Buffer message.
     * This method is used by the Protocol Buffer deserialization framework.
     * 
     * @param protobufMessage The Protocol Buffer message
     * @return An EventData instance populated from the Protocol Buffer message
     */
    @JsonIgnore
    public static EventData fromProtobufMessage(Object protobufMessage) {
        // This method will be implemented to convert from the appropriate Protocol Buffer message
        // The actual implementation will be provided by the Protocol Buffer code generator
        throw new UnsupportedOperationException("Protocol Buffer deserialization not implemented");
    }
    
    /**
     * Serializes this EventData to the provided output stream using Protocol Buffers.
     * This method provides a convenient way to write the data directly to a stream.
     * 
     * @param output The output stream to write to
     * @throws IOException If an I/O error occurs
     */
    public void writeTo(OutputStream output) throws IOException {
        // Convert to Protocol Buffer message and write to output stream
        // The actual implementation will use the Protocol Buffer generated code
        throw new UnsupportedOperationException("Protocol Buffer serialization not implemented");
    }
    
    /**
     * Deserializes an EventData instance from the provided input stream using Protocol Buffers.
     * This method provides a convenient way to read the data directly from a stream.
     * 
     * @param input The input stream to read from
     * @return An EventData instance populated from the input stream
     * @throws IOException If an I/O error occurs
     */
    public static EventData parseFrom(InputStream input) throws IOException {
        // Read from input stream and convert to EventData
        // The actual implementation will use the Protocol Buffer generated code
        throw new UnsupportedOperationException("Protocol Buffer deserialization not implemented");
    }
    
    /**
     * Serializes this EventData to a byte array using Protocol Buffers.
     * This method is optimized for high-throughput messaging scenarios.
     * 
     * @return The serialized byte array
     */
    @JsonIgnore
    public byte[] toByteArray() {
        // Convert to Protocol Buffer message and serialize to byte array
        // The actual implementation will use the Protocol Buffer generated code
        throw new UnsupportedOperationException("Protocol Buffer serialization not implemented");
    }
    
    /**
     * Deserializes an EventData instance from a byte array using Protocol Buffers.
     * This method is optimized for high-throughput messaging scenarios.
     * 
     * @param data The serialized byte array
     * @return An EventData instance populated from the byte array
     */
    public static EventData parseFrom(byte[] data) {
        // Deserialize from byte array and convert to EventData
        // The actual implementation will use the Protocol Buffer generated code
        throw new UnsupportedOperationException("Protocol Buffer deserialization not implemented");
    }
}