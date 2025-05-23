/*
 * Copyright 2015 - 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramChannel;

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Network message for communication between services.
 * Supports serialization for message broker compatibility and distributed tracing.
 */
public class NetworkMessage {

    private final Object message;
    private final Channel channel;
    private final InetSocketAddress remoteAddress;
    private final String correlationId;

    /**
     * Creates a new NetworkMessage with a generated correlation ID.
     *
     * @param message Message object
     * @param channel Communication channel
     */
    public NetworkMessage(Object message, Channel channel) {
        this.message = message;
        this.channel = channel;
        this.remoteAddress = null;
        this.correlationId = UUID.randomUUID().toString();
    }

    /**
     * Creates a new NetworkMessage with a generated correlation ID.
     *
     * @param message Message object
     * @param remoteAddress Remote socket address
     */
    public NetworkMessage(Object message, InetSocketAddress remoteAddress) {
        this.message = message;
        this.channel = null;
        this.remoteAddress = remoteAddress;
        this.correlationId = UUID.randomUUID().toString();
    }

    /**
     * Creates a new NetworkMessage with a specified correlation ID.
     *
     * @param message Message object
     * @param channel Communication channel
     * @param correlationId Correlation ID for distributed tracing
     */
    public NetworkMessage(Object message, Channel channel, String correlationId) {
        this.message = message;
        this.channel = channel;
        this.remoteAddress = null;
        this.correlationId = correlationId;
    }

    /**
     * Creates a new NetworkMessage with a specified correlation ID.
     *
     * @param message Message object
     * @param remoteAddress Remote socket address
     * @param correlationId Correlation ID for distributed tracing
     */
    public NetworkMessage(Object message, InetSocketAddress remoteAddress, String correlationId) {
        this.message = message;
        this.channel = null;
        this.remoteAddress = remoteAddress;
        this.correlationId = correlationId;
    }

    /**
     * Get message object.
     *
     * @return Message object
     */
    public Object getMessage() {
        return message;
    }

    /**
     * Get communication channel.
     *
     * @return Communication channel
     */
    public Channel getChannel() {
        return channel;
    }

    /**
     * Get remote socket address.
     *
     * @return Remote socket address
     */
    public InetSocketAddress getRemoteAddress() {
        if (remoteAddress != null) {
            return remoteAddress;
        } else if (channel instanceof DatagramChannel) {
            return (InetSocketAddress) channel.remoteAddress();
        } else {
            return null;
        }
    }

    /**
     * Get correlation ID for distributed tracing.
     *
     * @return Correlation ID
     */
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * Creates a new NetworkMessage with the same correlation ID but a different message.
     *
     * @param message New message object
     * @return New NetworkMessage with the same correlation ID
     */
    public NetworkMessage withMessage(Object message) {
        if (channel != null) {
            return new NetworkMessage(message, channel, correlationId);
        } else {
            return new NetworkMessage(message, remoteAddress, correlationId);
        }
    }

    /**
     * Serializes this NetworkMessage to a ByteBuf for transmission via a message broker.
     * This method should be implemented based on the specific Protocol Buffer schema
     * defined for inter-service communication.
     *
     * @return ByteBuf containing the serialized message
     */
    public ByteBuf serialize() {
        // This would be implemented using Protocol Buffers serialization
        // For example:
        // NetworkMessageProto.Builder builder = NetworkMessageProto.newBuilder();
        // builder.setCorrelationId(correlationId);
        // 
        // if (message instanceof Position) {
        //     Position position = (Position) message;
        //     builder.setPositionMessage(convertToPositionProto(position));
        // } else if (message instanceof Command) {
        //     Command command = (Command) message;
        //     builder.setCommandMessage(convertToCommandProto(command));
        // }
        //
        // NetworkMessageProto proto = builder.build();
        // ByteBuf buffer = Unpooled.buffer(proto.getSerializedSize());
        // buffer.writeBytes(proto.toByteArray());
        // return buffer;
        
        throw new UnsupportedOperationException("Serialization must be implemented with Protocol Buffers");
    }

    /**
     * Deserializes a ByteBuf into a NetworkMessage.
     * This method should be implemented based on the specific Protocol Buffer schema
     * defined for inter-service communication.
     *
     * @param buffer ByteBuf containing the serialized message
     * @return Deserialized NetworkMessage
     */
    public static NetworkMessage deserialize(ByteBuf buffer) {
        // This would be implemented using Protocol Buffers deserialization
        // For example:
        // byte[] bytes = new byte[buffer.readableBytes()];
        // buffer.readBytes(bytes);
        // NetworkMessageProto proto = NetworkMessageProto.parseFrom(bytes);
        // 
        // String correlationId = proto.getCorrelationId();
        // Object message = null;
        //
        // if (proto.hasPositionMessage()) {
        //     message = convertFromPositionProto(proto.getPositionMessage());
        // } else if (proto.hasCommandMessage()) {
        //     message = convertFromCommandProto(proto.getCommandMessage());
        // }
        //
        // return new NetworkMessage(message, (Channel) null, correlationId);
        
        throw new UnsupportedOperationException("Deserialization must be implemented with Protocol Buffers");
    }
}