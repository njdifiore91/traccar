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

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Timestamp;
import org.traccar.model.Device;
import org.traccar.model.Position;

import java.io.ByteArrayOutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implements the MessageSerializer interface for Protocol Buffers, providing efficient binary
 * serialization of messages. It handles schema versioning and validation to ensure message integrity.
 * 
 * This implementation supports serialization of Position and Device objects to their Protocol Buffer
 * equivalents, with optimized performance through buffer reuse.
 */
public class ProtobufMessageSerializer implements MessageSerializer {

    private static final String CONTENT_TYPE = "application/protobuf";
    private static final String SCHEMA_VERSION = "1.0.0";
    
    // Thread-safe cache for reusing ByteArrayOutputStream instances to reduce GC pressure
    private final ThreadLocal<ByteArrayOutputStream> outputStreamCache = 
            ThreadLocal.withInitial(() -> new ByteArrayOutputStream(4096));
    
    // Cache for mapping Java model classes to their Protocol Buffer serializers
    private final Map<Class<?>, ProtobufSerializer<?>> serializerMap;
    
    /**
     * Constructs a new ProtobufMessageSerializer with serializers for supported message types.
     */
    public ProtobufMessageSerializer() {
        serializerMap = new HashMap<>();
        serializerMap.put(Position.class, new PositionSerializer());
        serializerMap.put(Device.class, new DeviceSerializer());
    }

    @Override
    public byte[] serialize(Object object) throws Exception {
        if (object == null) {
            throw new IllegalArgumentException("Cannot serialize null object");
        }
        
        @SuppressWarnings("unchecked")
        ProtobufSerializer<Object> serializer = (ProtobufSerializer<Object>) serializerMap.get(object.getClass());
        if (serializer == null) {
            throw new IllegalArgumentException("Unsupported object type for serialization: " + object.getClass().getName());
        }
        
        // Get cached ByteArrayOutputStream or create a new one if not available
        ByteArrayOutputStream outputStream = outputStreamCache.get();
        outputStream.reset(); // Clear any previous data
        
        // Convert the object to a Protocol Buffer message and serialize it
        MessageLite message = serializer.toProtobuf(object);
        message.writeTo(outputStream);
        
        return outputStream.toByteArray();
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
        
        ProtobufSerializer<?> serializer = serializerMap.get(object.getClass());
        return serializer != null && serializer.validate(object);
    }
    
    /**
     * Interface for converting Java model objects to Protocol Buffer messages.
     *
     * @param <T> The type of Java model object to serialize
     */
    private interface ProtobufSerializer<T> {
        /**
         * Converts a Java model object to its Protocol Buffer equivalent.
         *
         * @param object The Java model object to convert
         * @return The Protocol Buffer message
         */
        MessageLite toProtobuf(T object);
        
        /**
         * Validates that a Java model object can be properly serialized to Protocol Buffers.
         *
         * @param object The Java model object to validate
         * @return true if the object is valid for serialization, false otherwise
         */
        boolean validate(T object);
    }
    
    /**
     * Serializer for Position objects.
     */
    private static class PositionSerializer implements ProtobufSerializer<Position> {
        
        @Override
        public MessageLite toProtobuf(Position position) {
            // Create a Protocol Buffer Position message from the Java Position object
            org.traccar.proto.Position.Builder builder = org.traccar.proto.Position.newBuilder()
                    .setDeviceId(position.getDeviceId())
                    .setProtocol(position.getProtocol())
                    .setValid(position.getValid())
                    .setLatitude(position.getLatitude())
                    .setLongitude(position.getLongitude())
                    .setAltitude(position.getAltitude())
                    .setSpeed(position.getSpeed())
                    .setCourse(position.getCourse());
            
            // Set timestamps if available
            if (position.getServerTime() != null) {
                builder.setServerTime(dateToTimestamp(position.getServerTime()));
            }
            if (position.getDeviceTime() != null) {
                builder.setDeviceTime(dateToTimestamp(position.getDeviceTime()));
            }
            if (position.getFixTime() != null) {
                builder.setFixTime(dateToTimestamp(position.getFixTime()));
            }
            
            // Set address if available
            if (position.getAddress() != null) {
                builder.setAddress(position.getAddress());
            }
            
            // Set accuracy if available
            if (position.getAccuracy() > 0) {
                builder.setAccuracy(position.getAccuracy());
            }
            
            // Add all attributes as key-value pairs
            if (position.getAttributes() != null) {
                for (Map.Entry<String, Object> entry : position.getAttributes().entrySet()) {
                    if (entry.getValue() != null) {
                        addAttributeToBuilder(builder, entry.getKey(), entry.getValue());
                    }
                }
            }
            
            return builder.build();
        }
        
        @Override
        public boolean validate(Position position) {
            // Basic validation to ensure required fields are present
            return position != null 
                    && position.getDeviceId() > 0
                    && position.getProtocol() != null
                    && !position.getProtocol().isEmpty();
        }
    }
    
    /**
     * Serializer for Device objects.
     */
    private static class DeviceSerializer implements ProtobufSerializer<Device> {
        
        @Override
        public MessageLite toProtobuf(Device device) {
            // Create a Protocol Buffer Device message from the Java Device object
            org.traccar.proto.Device.Builder builder = org.traccar.proto.Device.newBuilder()
                    .setId(device.getId())
                    .setName(device.getName())
                    .setUniqueId(device.getUniqueId())
                    .setStatus(device.getStatus())
                    .setDisabled(device.getDisabled());
            
            // Set optional fields if available
            if (device.getLastUpdate() != null) {
                builder.setLastUpdate(dateToTimestamp(device.getLastUpdate()));
            }
            
            if (device.getPositionId() > 0) {
                builder.setPositionId(device.getPositionId());
            }
            
            if (device.getGroupId() > 0) {
                builder.setGroupId(device.getGroupId());
            }
            
            if (device.getPhone() != null) {
                builder.setPhone(device.getPhone());
            }
            
            if (device.getModel() != null) {
                builder.setModel(device.getModel());
            }
            
            if (device.getContact() != null) {
                builder.setContact(device.getContact());
            }
            
            if (device.getCategory() != null) {
                builder.setCategory(device.getCategory());
            }
            
            // Add attributes if available
            if (device.getAttributes() != null) {
                for (Map.Entry<String, Object> entry : device.getAttributes().entrySet()) {
                    if (entry.getValue() != null) {
                        addAttributeToBuilder(builder, entry.getKey(), entry.getValue());
                    }
                }
            }
            
            return builder.build();
        }
        
        @Override
        public boolean validate(Device device) {
            // Basic validation to ensure required fields are present
            return device != null 
                    && device.getId() > 0
                    && device.getUniqueId() != null
                    && !device.getUniqueId().isEmpty();
        }
    }
    
    /**
     * Converts a Java Date object to a Protocol Buffer Timestamp.
     *
     * @param date The Date to convert
     * @return The Protocol Buffer Timestamp
     */
    private static Timestamp dateToTimestamp(Date date) {
        long millis = date.getTime();
        return Timestamp.newBuilder()
                .setSeconds(millis / 1000)
                .setNanos((int) ((millis % 1000) * 1000000))
                .build();
    }
    
    /**
     * Adds an attribute to a Protocol Buffer message builder.
     *
     * @param builder The message builder to add the attribute to
     * @param key The attribute key
     * @param value The attribute value
     */
    private static void addAttributeToBuilder(Message.Builder builder, String key, Object value) {
        if (value instanceof Boolean) {
            builder.getFieldBuilder(builder.getDescriptorForType().findFieldByName("attributes"))
                    .setField(
                            builder.getDescriptorForType().findFieldByName("attributes").getMessageType().findFieldByName("bool_value"),
                            ((Boolean) value));
        } else if (value instanceof Integer || value instanceof Long) {
            builder.getFieldBuilder(builder.getDescriptorForType().findFieldByName("attributes"))
                    .setField(
                            builder.getDescriptorForType().findFieldByName("attributes").getMessageType().findFieldByName("int_value"),
                            ((Number) value).longValue());
        } else if (value instanceof Float || value instanceof Double) {
            builder.getFieldBuilder(builder.getDescriptorForType().findFieldByName("attributes"))
                    .setField(
                            builder.getDescriptorForType().findFieldByName("attributes").getMessageType().findFieldByName("double_value"),
                            ((Number) value).doubleValue());
        } else if (value instanceof String) {
            builder.getFieldBuilder(builder.getDescriptorForType().findFieldByName("attributes"))
                    .setField(
                            builder.getDescriptorForType().findFieldByName("attributes").getMessageType().findFieldByName("string_value"),
                            value.toString());
        } else if (value instanceof byte[]) {
            builder.getFieldBuilder(builder.getDescriptorForType().findFieldByName("attributes"))
                    .setField(
                            builder.getDescriptorForType().findFieldByName("attributes").getMessageType().findFieldByName("bytes_value"),
                            ByteString.copyFrom((byte[]) value));
        }
    }
}