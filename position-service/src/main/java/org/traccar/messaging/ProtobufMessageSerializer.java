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

import com.google.protobuf.InvalidProtocolBufferException;
import org.traccar.proto.PositionOuterClass.Position;

/**
 * Serializer for converting between PositionMessage objects and Protocol Buffers format.
 * This implementation handles serialization and deserialization of position messages
 * for efficient binary transport between services.
 */
public class ProtobufMessageSerializer {

    /**
     * Serializes a PositionMessage to Protocol Buffers binary format.
     *
     * @param message The PositionMessage to serialize
     * @return The serialized message as a byte array
     * @throws MessageException If there is an error during serialization
     */
    public byte[] serialize(PositionMessage message) throws MessageException {
        try {
            Position.Builder builder = Position.newBuilder()
                    .setId(message.getId())
                    .setDeviceId(message.getDeviceId())
                    .setProtocol(message.getProtocol())
                    .setDeviceTime(message.getDeviceTime())
                    .setServerTime(message.getServerTime())
                    .setFixTime(message.getFixTime())
                    .setValid(message.isValid())
                    .setLatitude(message.getLatitude())
                    .setLongitude(message.getLongitude());

            if (message.getAltitude() != null) {
                builder.setAltitude(message.getAltitude());
            }
            if (message.getSpeed() != null) {
                builder.setSpeed(message.getSpeed());
            }
            if (message.getCourse() != null) {
                builder.setCourse(message.getCourse());
            }
            if (message.getAddress() != null) {
                builder.setAddress(message.getAddress());
            }
            if (message.getAccuracy() != null) {
                builder.setAccuracy(message.getAccuracy());
            }
            if (message.getNetwork() != null) {
                builder.setNetwork(message.getNetwork());
            }

            // Add attributes
            if (message.getAttributes() != null) {
                message.getAttributes().forEach((key, value) -> {
                    if (value instanceof Boolean) {
                        builder.putAttributes(key, String.valueOf(value));
                    } else if (value instanceof Number) {
                        builder.putAttributes(key, String.valueOf(value));
                    } else if (value instanceof String) {
                        builder.putAttributes(key, (String) value);
                    } else if (value != null) {
                        builder.putAttributes(key, value.toString());
                    }
                });
            }

            // Add correlation ID if available
            if (message.getCorrelationId() != null) {
                builder.setCorrelationId(message.getCorrelationId());
            }

            return builder.build().toByteArray();
        } catch (Exception e) {
            throw new MessageException("Failed to serialize position message", e);
        }
    }

    /**
     * Deserializes a Protocol Buffers binary message to a PositionMessage.
     *
     * @param data The serialized message as a byte array
     * @return The deserialized PositionMessage
     * @throws MessageException If there is an error during deserialization
     */
    public PositionMessage deserialize(byte[] data) throws MessageException {
        try {
            Position position = Position.parseFrom(data);
            PositionMessage message = new PositionMessage();

            message.setId(position.getId());
            message.setDeviceId(position.getDeviceId());
            message.setProtocol(position.getProtocol());
            message.setDeviceTime(position.getDeviceTime());
            message.setServerTime(position.getServerTime());
            message.setFixTime(position.getFixTime());
            message.setValid(position.getValid());
            message.setLatitude(position.getLatitude());
            message.setLongitude(position.getLongitude());

            if (position.hasAltitude()) {
                message.setAltitude(position.getAltitude());
            }
            if (position.hasSpeed()) {
                message.setSpeed(position.getSpeed());
            }
            if (position.hasCourse()) {
                message.setCourse(position.getCourse());
            }
            if (position.hasAddress()) {
                message.setAddress(position.getAddress());
            }
            if (position.hasAccuracy()) {
                message.setAccuracy(position.getAccuracy());
            }
            if (position.hasNetwork()) {
                message.setNetwork(position.getNetwork());
            }

            // Add attributes
            position.getAttributesMap().forEach(message::setAttribute);

            // Set correlation ID if available
            if (position.hasCorrelationId()) {
                message.setCorrelationId(position.getCorrelationId());
            }

            return message;
        } catch (InvalidProtocolBufferException e) {
            throw new MessageException("Failed to deserialize position message", e);
        }
    }
}