/*
 * Copyright 2022 - 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.broadcast;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.protobuf.Any;
import com.google.protobuf.Message;

import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.ObjectOperation;
import org.traccar.model.Position;

import java.util.HashMap;
import java.util.Map;

/**
 * BroadcastMessage is used for asynchronous communication between services via a message broker.
 * It supports both JSON and Protocol Buffers serialization formats and includes metadata for
 * distributed tracing, message versioning, and routing.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BroadcastMessage {

    /**
     * Message schema version for backward compatibility.
     * Increment this when making breaking changes to the message structure.
     */
    private static final int CURRENT_SCHEMA_VERSION = 1;

    /**
     * Schema version of this message instance.
     */
    private int schemaVersion = CURRENT_SCHEMA_VERSION;

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    /**
     * Distributed tracing context for tracking message flow across services.
     */
    private Map<String, String> tracingContext;

    public Map<String, String> getTracingContext() {
        if (tracingContext == null) {
            tracingContext = new HashMap<>();
        }
        return tracingContext;
    }

    public void setTracingContext(Map<String, String> tracingContext) {
        this.tracingContext = tracingContext;
    }

    /**
     * Message routing metadata for message broker configuration.
     */
    private Map<String, String> routingMetadata;

    public Map<String, String> getRoutingMetadata() {
        if (routingMetadata == null) {
            routingMetadata = new HashMap<>();
        }
        return routingMetadata;
    }

    public void setRoutingMetadata(Map<String, String> routingMetadata) {
        this.routingMetadata = routingMetadata;
    }

    /**
     * Unique message identifier for deduplication and tracking.
     */
    private String messageId;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    /**
     * Timestamp when the message was created.
     */
    private long timestamp;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Protocol buffer message for binary serialization.
     * This field is transient and not serialized to JSON.
     */
    @JsonIgnore
    private transient Message protobufMessage;

    @JsonIgnore
    public Message getProtobufMessage() {
        return protobufMessage;
    }

    @JsonIgnore
    public void setProtobufMessage(Message protobufMessage) {
        this.protobufMessage = protobufMessage;
    }

    /**
     * Serialized protocol buffer message for JSON serialization.
     * This is only used when serializing to JSON and the message contains a protobuf payload.
     */
    private Any serializedProtobufMessage;

    @JsonProperty("protobufMessage")
    public Any getSerializedProtobufMessage() {
        if (serializedProtobufMessage == null && protobufMessage != null) {
            serializedProtobufMessage = Any.pack(protobufMessage);
        }
        return serializedProtobufMessage;
    }

    @JsonProperty("protobufMessage")
    public void setSerializedProtobufMessage(Any serializedProtobufMessage) {
        this.serializedProtobufMessage = serializedProtobufMessage;
    }

    // Original fields

    private Device device;

    public Device getDevice() {
        return device;
    }

    public void setDevice(Device device) {
        this.device = device;
    }

    private Position position;

    public Position getPosition() {
        return position;
    }

    public void setPosition(Position position) {
        this.position = position;
    }

    private Long userId;

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    private Event event;

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    private Long commandDeviceId;

    public Long getCommandDeviceId() {
        return commandDeviceId;
    }

    public void setCommandDeviceId(Long commandDeviceId) {
        this.commandDeviceId = commandDeviceId;
    }

    public static class InvalidateObject {

        private String clazz;

        public String getClazz() {
            return clazz;
        }

        public void setClazz(String clazz) {
            this.clazz = clazz;
        }

        private long id;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        private ObjectOperation operation;

        public ObjectOperation getOperation() {
            return operation;
        }

        public void setOperation(ObjectOperation operation) {
            this.operation = operation;
        }

    }

    private InvalidateObject invalidateObject;

    public InvalidateObject getInvalidateObject() {
        return invalidateObject;
    }

    public void setInvalidateObject(InvalidateObject invalidateObject) {
        this.invalidateObject = invalidateObject;
    }

    public static class InvalidatePermission {

        private String clazz1;

        public String getClazz1() {
            return clazz1;
        }

        public void setClazz1(String clazz1) {
            this.clazz1 = clazz1;
        }

        private long id1;

        public long getId1() {
            return id1;
        }

        public void setId1(long id1) {
            this.id1 = id1;
        }

        private String clazz2;

        public String getClazz2() {
            return clazz2;
        }

        public void setClazz2(String clazz2) {
            this.clazz2 = clazz2;
        }

        private long id2;

        public long getId2() {
            return id2;
        }

        public void setId2(long id2) {
            this.id2 = id2;
        }

        private boolean link;

        public boolean getLink() {
            return link;
        }

        public void setLink(boolean link) {
            this.link = link;
        }

    }

    private InvalidatePermission invalidatePermission;

    public InvalidatePermission getInvalidatePermission() {
        return invalidatePermission;
    }

    public void setInvalidatePermission(InvalidatePermission invalidatePermission) {
        this.invalidatePermission = invalidatePermission;
    }

    /**
     * Helper method to set a routing key for message broker partitioning.
     * This is typically used to ensure messages for the same device are processed in order.
     *
     * @param key The routing key name
     * @param value The routing key value
     */
    public void setRoutingKey(String key, String value) {
        getRoutingMetadata().put(key, value);
    }

    /**
     * Helper method to set a device ID as the routing key.
     * This ensures all messages for the same device are processed by the same consumer.
     *
     * @param deviceId The device ID to use for routing
     */
    public void setDeviceIdRoutingKey(long deviceId) {
        setRoutingKey("deviceId", String.valueOf(deviceId));
    }

    /**
     * Helper method to set distributed tracing context from OpenTelemetry.
     *
     * @param traceId The trace ID from the current span context
     * @param spanId The span ID from the current span context
     */
    public void setTracingIds(String traceId, String spanId) {
        Map<String, String> context = getTracingContext();
        context.put("traceId", traceId);
        context.put("spanId", spanId);
    }

    /**
     * Helper method to set the message timestamp to the current time.
     */
    public void setCurrentTimestamp() {
        this.timestamp = System.currentTimeMillis();
    }
}