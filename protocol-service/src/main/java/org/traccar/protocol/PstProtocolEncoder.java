/*
 * Copyright 2020 Anton Tananaev (anton@traccar.org)
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
package org.traccar.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.traccar.BaseProtocolEncoder;
import org.traccar.Protocol;
import org.traccar.helper.Checksum;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Command;

/**
 * PST protocol encoder
 * Supports both direct encoding and message broker integration for microservices architecture
 */
public class PstProtocolEncoder extends BaseProtocolEncoder {

    private final MessageProducer messageProducer;

    public PstProtocolEncoder(Protocol protocol) {
        this(protocol, null);
    }

    public PstProtocolEncoder(Protocol protocol, MessageProducer messageProducer) {
        super(protocol);
        this.messageProducer = messageProducer;
    }

    private ByteBuf encodeContent(long deviceId, int type, int data1, int data2) {

        ByteBuf buf = Unpooled.buffer();

        buf.writeInt((int) Long.parseLong(getUniqueId(deviceId)));
        buf.writeByte(0x06); // version

        buf.writeInt(1); // index
        buf.writeByte(PstProtocolDecoder.MSG_COMMAND);
        buf.writeShort(type);
        buf.writeShort(data1);
        buf.writeShort(data2);

        buf.writeShort(Checksum.crc16(Checksum.CRC16_XMODEM, buf.nioBuffer()));

        return buf;
    }

    @Override
    protected Object encodeCommand(Command command) {
        ByteBuf encodedCommand = null;
        
        switch (command.getType()) {
            case Command.TYPE_ENGINE_STOP:
                encodedCommand = encodeContent(command.getDeviceId(), 0x0002, 0xffff, 0xffff);
                break;
            case Command.TYPE_ENGINE_RESUME:
                encodedCommand = encodeContent(command.getDeviceId(), 0x0001, 0xffff, 0xffff);
                break;
            default:
                return null;
        }
        
        // If message broker is available, publish the command
        if (messageProducer != null && messageProducer.isConnected()) {
            // Create a command message for the broker
            CommandMessage commandMessage = new CommandMessage(
                command.getDeviceId(),
                command.getType(),
                encodedCommand.copy());
                
            // Publish to the command topic
            messageProducer.publish("protocol.commands", commandMessage);
        }
        
        return encodedCommand;
    }
    
    /**
     * Internal class for command messages sent to the message broker
     */
    private static class CommandMessage {
        private final long deviceId;
        private final String type;
        private final ByteBuf data;
        
        public CommandMessage(long deviceId, String type, ByteBuf data) {
            this.deviceId = deviceId;
            this.type = type;
            this.data = data;
        }
        
        public long getDeviceId() {
            return deviceId;
        }
        
        public String getType() {
            return type;
        }
        
        public ByteBuf getData() {
            return data;
        }
    }
}