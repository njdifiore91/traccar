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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.proto.PositionOuterClass;
import org.traccar.proto.EventOuterClass;

import java.util.Date;
import java.util.Map;

/**
 * Provides utilities for converting between message formats used in the Event Processing Service.
 * Handles serialization and deserialization using Protocol Buffers, JSON, or other formats
 * depending on configuration. Also manages schema versioning and compatibility for backward compatibility.
 */
@Singleton
public class MessageConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConverter.class);

    private final Config config;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;

    /**
     * Message format options for serialization/deserialization.
     */
    public enum MessageFormat {
        PROTOBUF,  // Protocol Buffers binary format
        JSON       // JSON text format
    }

    @Inject
    public MessageConverter(Config config, ObjectMapper objectMapper, Tracer tracer) {
        this.config = config;
        this.objectMapper = objectMapper;
        this.tracer = tracer;
    }

    /**
     * Converts a broker message to a Position object.
     *
     * @param message The message bytes from the broker
     * @param format The format of the message (PROTOBUF or JSON)
     * @return The Position object
     * @throws MessageConversionException If conversion fails
     */
    public Position positionFromMessage(byte[] message, MessageFormat format) throws MessageConversionException {
        Span span = tracer.spanBuilder("positionFromMessage")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("message.format", format.name())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Position position;
            
            switch (format) {
                case PROTOBUF:
                    position = positionFromProtobuf(message);
                    break;
                case JSON:
                    position = positionFromJson(message);
                    break;
                default:
                    throw new MessageConversionException("Unsupported message format: " + format);
            }
            
            span.setAttribute("position.deviceId", position.getDeviceId());
            span.setAttribute("position.id", position.getId());
            return position;
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw new MessageConversionException("Failed to convert message to Position", e);
        } finally {
            span.end();
        }
    }

    /**
     * Converts a Position object to a broker message.
     *
     * @param position The Position object
     * @param format The format to convert to (PROTOBUF or JSON)
     * @return The message bytes for the broker
     * @throws MessageConversionException If conversion fails
     */
    public byte[] positionToMessage(Position position, MessageFormat format) throws MessageConversionException {
        Span span = tracer.spanBuilder("positionToMessage")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("message.format", format.name())
                .setAttribute("position.deviceId", position.getDeviceId())
                .setAttribute("position.id", position.getId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            switch (format) {
                case PROTOBUF:
                    return positionToProtobuf(position);
                case JSON:
                    return positionToJson(position);
                default:
                    throw new MessageConversionException("Unsupported message format: " + format);
            }
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw new MessageConversionException("Failed to convert Position to message", e);
        } finally {
            span.end();
        }
    }

    /**
     * Converts a broker message to an Event object.
     *
     * @param message The message bytes from the broker
     * @param format The format of the message (PROTOBUF or JSON)
     * @return The Event object
     * @throws MessageConversionException If conversion fails
     */
    public Event eventFromMessage(byte[] message, MessageFormat format) throws MessageConversionException {
        Span span = tracer.spanBuilder("eventFromMessage")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("message.format", format.name())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Event event;
            
            switch (format) {
                case PROTOBUF:
                    event = eventFromProtobuf(message);
                    break;
                case JSON:
                    event = eventFromJson(message);
                    break;
                default:
                    throw new MessageConversionException("Unsupported message format: " + format);
            }
            
            span.setAttribute("event.deviceId", event.getDeviceId());
            span.setAttribute("event.type", event.getType());
            return event;
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw new MessageConversionException("Failed to convert message to Event", e);
        } finally {
            span.end();
        }
    }

    /**
     * Converts an Event object to a broker message.
     *
     * @param event The Event object
     * @param format The format to convert to (PROTOBUF or JSON)
     * @return The message bytes for the broker
     * @throws MessageConversionException If conversion fails
     */
    public byte[] eventToMessage(Event event, MessageFormat format) throws MessageConversionException {
        Span span = tracer.spanBuilder("eventToMessage")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("message.format", format.name())
                .setAttribute("event.deviceId", event.getDeviceId())
                .setAttribute("event.type", event.getType())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            switch (format) {
                case PROTOBUF:
                    return eventToProtobuf(event);
                case JSON:
                    return eventToJson(event);
                default:
                    throw new MessageConversionException("Unsupported message format: " + format);
            }
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw new MessageConversionException("Failed to convert Event to message", e);
        } finally {
            span.end();
        }
    }

    /**
     * Converts a Position object to Protocol Buffers format.
     *
     * @param position The Position object
     * @return The Protocol Buffers message bytes
     * @throws MessageConversionException If conversion fails
     */
    private byte[] positionToProtobuf(Position position) throws MessageConversionException {
        try {
            PositionOuterClass.Position.Builder builder = PositionOuterClass.Position.newBuilder()
                    .setId(position.getId())
                    .setDeviceId(position.getDeviceId());

            if (position.getProtocol() != null) {
                builder.setProtocol(position.getProtocol());
            }

            if (position.getServerTime() != null) {
                builder.setServerTime(position.getServerTime().getTime());
            }

            if (position.getDeviceTime() != null) {
                builder.setDeviceTime(position.getDeviceTime().getTime());
            }

            if (position.getFixTime() != null) {
                builder.setFixTime(position.getFixTime().getTime());
            }

            builder.setValid(position.getValid());
            builder.setLatitude(position.getLatitude());
            builder.setLongitude(position.getLongitude());

            if (position.getAltitude() != 0) {
                builder.setAltitude(position.getAltitude());
            }

            if (position.getSpeed() != 0) {
                builder.setSpeed(position.getSpeed());
            }

            if (position.getCourse() != 0) {
                builder.setCourse(position.getCourse());
            }

            if (position.getAddress() != null) {
                builder.setAddress(position.getAddress());
            }

            // Add all attributes
            if (position.getAttributes() != null) {
                for (Map.Entry<String, Object> entry : position.getAttributes().entrySet()) {
                    if (entry.getValue() != null) {
                        if (entry.getValue() instanceof Boolean) {
                            builder.putAttributes(entry.getKey(), String.valueOf(entry.getValue()));
                        } else if (entry.getValue() instanceof Integer) {
                            builder.putAttributes(entry.getKey(), String.valueOf(entry.getValue()));
                        } else if (entry.getValue() instanceof Long) {
                            builder.putAttributes(entry.getKey(), String.valueOf(entry.getValue()));
                        } else if (entry.getValue() instanceof Float) {
                            builder.putAttributes(entry.getKey(), String.valueOf(entry.getValue()));
                        } else if (entry.getValue() instanceof Double) {
                            builder.putAttributes(entry.getKey(), String.valueOf(entry.getValue()));
                        } else if (entry.getValue() instanceof String) {
                            builder.putAttributes(entry.getKey(), (String) entry.getValue());
                        } else {
                            builder.putAttributes(entry.getKey(), objectMapper.writeValueAsString(entry.getValue()));
                        }
                    }
                }
            }

            // Add schema version for backward compatibility
            builder.setSchemaVersion(1);

            return builder.build().toByteArray();
        } catch (Exception e) {
            throw new MessageConversionException("Failed to convert Position to Protocol Buffers", e);
        }
    }

    /**
     * Converts Protocol Buffers message bytes to a Position object.
     *
     * @param bytes The Protocol Buffers message bytes
     * @return The Position object
     * @throws MessageConversionException If conversion fails
     */
    private Position positionFromProtobuf(byte[] bytes) throws MessageConversionException {
        try {
            PositionOuterClass.Position protoPosition = PositionOuterClass.Position.parseFrom(bytes);
            
            // Check schema version for compatibility
            int schemaVersion = protoPosition.getSchemaVersion();
            if (schemaVersion > 1) {
                LOGGER.warn("Received position message with newer schema version: {}", schemaVersion);
            }
            
            Position position = new Position();
            position.setId(protoPosition.getId());
            position.setDeviceId(protoPosition.getDeviceId());
            
            if (protoPosition.hasProtocol()) {
                position.setProtocol(protoPosition.getProtocol());
            }
            
            if (protoPosition.hasServerTime()) {
                position.setServerTime(new Date(protoPosition.getServerTime()));
            }
            
            if (protoPosition.hasDeviceTime()) {
                position.setDeviceTime(new Date(protoPosition.getDeviceTime()));
            }
            
            if (protoPosition.hasFixTime()) {
                position.setFixTime(new Date(protoPosition.getFixTime()));
            }
            
            position.setValid(protoPosition.getValid());
            position.setLatitude(protoPosition.getLatitude());
            position.setLongitude(protoPosition.getLongitude());
            
            if (protoPosition.hasAltitude()) {
                position.setAltitude(protoPosition.getAltitude());
            }
            
            if (protoPosition.hasSpeed()) {
                position.setSpeed(protoPosition.getSpeed());
            }
            
            if (protoPosition.hasCourse()) {
                position.setCourse(protoPosition.getCourse());
            }
            
            if (protoPosition.hasAddress()) {
                position.setAddress(protoPosition.getAddress());
            }
            
            // Process attributes
            for (Map.Entry<String, String> entry : protoPosition.getAttributesMap().entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                
                // Try to convert to appropriate type
                if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
                    position.set(key, Boolean.parseBoolean(value));
                } else {
                    try {
                        // Try as integer
                        position.set(key, Integer.parseInt(value));
                    } catch (NumberFormatException e1) {
                        try {
                            // Try as long
                            position.set(key, Long.parseLong(value));
                        } catch (NumberFormatException e2) {
                            try {
                                // Try as double
                                position.set(key, Double.parseDouble(value));
                            } catch (NumberFormatException e3) {
                                // Store as string
                                position.set(key, value);
                            }
                        }
                    }
                }
            }
            
            return position;
        } catch (InvalidProtocolBufferException e) {
            throw new MessageConversionException("Failed to parse Protocol Buffers message", e);
        }
    }

    /**
     * Converts a Position object to JSON format.
     *
     * @param position The Position object
     * @return The JSON message bytes
     * @throws MessageConversionException If conversion fails
     */
    private byte[] positionToJson(Position position) throws MessageConversionException {
        try {
            return objectMapper.writeValueAsBytes(position);
        } catch (JsonProcessingException e) {
            throw new MessageConversionException("Failed to convert Position to JSON", e);
        }
    }

    /**
     * Converts JSON message bytes to a Position object.
     *
     * @param bytes The JSON message bytes
     * @return The Position object
     * @throws MessageConversionException If conversion fails
     */
    private Position positionFromJson(byte[] bytes) throws MessageConversionException {
        try {
            return objectMapper.readValue(bytes, Position.class);
        } catch (Exception e) {
            throw new MessageConversionException("Failed to parse JSON message", e);
        }
    }

    /**
     * Converts an Event object to Protocol Buffers format.
     *
     * @param event The Event object
     * @return The Protocol Buffers message bytes
     * @throws MessageConversionException If conversion fails
     */
    private byte[] eventToProtobuf(Event event) throws MessageConversionException {
        try {
            EventOuterClass.Event.Builder builder = EventOuterClass.Event.newBuilder()
                    .setId(event.getId())
                    .setDeviceId(event.getDeviceId())
                    .setType(event.getType());

            if (event.getServerTime() != null) {
                builder.setServerTime(event.getServerTime().getTime());
            }

            if (event.getPositionId() > 0) {
                builder.setPositionId(event.getPositionId());
            }

            if (event.getGeofenceId() > 0) {
                builder.setGeofenceId(event.getGeofenceId());
            }

            if (event.getMaintenanceId() > 0) {
                builder.setMaintenanceId(event.getMaintenanceId());
            }

            // Add all attributes
            if (event.getAttributes() != null) {
                for (Map.Entry<String, Object> entry : event.getAttributes().entrySet()) {
                    if (entry.getValue() != null) {
                        if (entry.getValue() instanceof Boolean) {
                            builder.putAttributes(entry.getKey(), String.valueOf(entry.getValue()));
                        } else if (entry.getValue() instanceof Integer) {
                            builder.putAttributes(entry.getKey(), String.valueOf(entry.getValue()));
                        } else if (entry.getValue() instanceof Long) {
                            builder.putAttributes(entry.getKey(), String.valueOf(entry.getValue()));
                        } else if (entry.getValue() instanceof Float) {
                            builder.putAttributes(entry.getKey(), String.valueOf(entry.getValue()));
                        } else if (entry.getValue() instanceof Double) {
                            builder.putAttributes(entry.getKey(), String.valueOf(entry.getValue()));
                        } else if (entry.getValue() instanceof String) {
                            builder.putAttributes(entry.getKey(), (String) entry.getValue());
                        } else {
                            builder.putAttributes(entry.getKey(), objectMapper.writeValueAsString(entry.getValue()));
                        }
                    }
                }
            }

            // Add schema version for backward compatibility
            builder.setSchemaVersion(1);

            return builder.build().toByteArray();
        } catch (Exception e) {
            throw new MessageConversionException("Failed to convert Event to Protocol Buffers", e);
        }
    }

    /**
     * Converts Protocol Buffers message bytes to an Event object.
     *
     * @param bytes The Protocol Buffers message bytes
     * @return The Event object
     * @throws MessageConversionException If conversion fails
     */
    private Event eventFromProtobuf(byte[] bytes) throws MessageConversionException {
        try {
            EventOuterClass.Event protoEvent = EventOuterClass.Event.parseFrom(bytes);
            
            // Check schema version for compatibility
            int schemaVersion = protoEvent.getSchemaVersion();
            if (schemaVersion > 1) {
                LOGGER.warn("Received event message with newer schema version: {}", schemaVersion);
            }
            
            Event event = new Event(protoEvent.getType(), protoEvent.getDeviceId());
            event.setId(protoEvent.getId());
            
            if (protoEvent.hasServerTime()) {
                event.setServerTime(new Date(protoEvent.getServerTime()));
            }
            
            if (protoEvent.hasPositionId()) {
                event.setPositionId(protoEvent.getPositionId());
            }
            
            if (protoEvent.hasGeofenceId()) {
                event.setGeofenceId(protoEvent.getGeofenceId());
            }
            
            if (protoEvent.hasMaintenanceId()) {
                event.setMaintenanceId(protoEvent.getMaintenanceId());
            }
            
            // Process attributes
            for (Map.Entry<String, String> entry : protoEvent.getAttributesMap().entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                
                // Try to convert to appropriate type
                if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
                    event.set(key, Boolean.parseBoolean(value));
                } else {
                    try {
                        // Try as integer
                        event.set(key, Integer.parseInt(value));
                    } catch (NumberFormatException e1) {
                        try {
                            // Try as long
                            event.set(key, Long.parseLong(value));
                        } catch (NumberFormatException e2) {
                            try {
                                // Try as double
                                event.set(key, Double.parseDouble(value));
                            } catch (NumberFormatException e3) {
                                // Store as string
                                event.set(key, value);
                            }
                        }
                    }
                }
            }
            
            return event;
        } catch (InvalidProtocolBufferException e) {
            throw new MessageConversionException("Failed to parse Protocol Buffers message", e);
        }
    }

    /**
     * Converts an Event object to JSON format.
     *
     * @param event The Event object
     * @return The JSON message bytes
     * @throws MessageConversionException If conversion fails
     */
    private byte[] eventToJson(Event event) throws MessageConversionException {
        try {
            return objectMapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new MessageConversionException("Failed to convert Event to JSON", e);
        }
    }

    /**
     * Converts JSON message bytes to an Event object.
     *
     * @param bytes The JSON message bytes
     * @return The Event object
     * @throws MessageConversionException If conversion fails
     */
    private Event eventFromJson(byte[] bytes) throws MessageConversionException {
        try {
            return objectMapper.readValue(bytes, Event.class);
        } catch (Exception e) {
            throw new MessageConversionException("Failed to parse JSON message", e);
        }
    }

    /**
     * Converts a Protocol Buffers message to JSON format.
     *
     * @param protoMessage The Protocol Buffers message
     * @return The JSON string representation
     * @throws MessageConversionException If conversion fails
     */
    public String protoToJson(com.google.protobuf.Message protoMessage) throws MessageConversionException {
        try {
            return JsonFormat.printer().print(protoMessage);
        } catch (Exception e) {
            throw new MessageConversionException("Failed to convert Protocol Buffers to JSON", e);
        }
    }

    /**
     * Determines the message format to use based on configuration.
     *
     * @return The configured message format (PROTOBUF or JSON)
     */
    public MessageFormat getConfiguredMessageFormat() {
        String format = config.getString(Keys.EVENT_MESSAGE_FORMAT, "PROTOBUF");
        try {
            return MessageFormat.valueOf(format.toUpperCase());
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Invalid message format configuration: {}, using PROTOBUF", format);
            return MessageFormat.PROTOBUF;
        }
    }

    /**
     * Exception thrown when message conversion fails.
     */
    public static class MessageConversionException extends Exception {
        public MessageConversionException(String message) {
            super(message);
        }

        public MessageConversionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}