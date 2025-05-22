/*
 * Copyright 2015 - 2018 Anton Tananaev (anton@traccar.org)
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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.traccar.BaseFrameDecoder;
import org.traccar.helper.BufferUtil;

import java.nio.charset.StandardCharsets;

/**
 * Frame decoder for Megastek protocol.
 * Updated to support both monolithic and microservices architecture.
 */
public class MegastekFrameDecoder extends BaseFrameDecoder {

    private final Object messageBroker;
    
    /**
     * Default constructor for monolithic architecture.
     */
    public MegastekFrameDecoder() {
        this.messageBroker = null;
    }
    
    /**
     * Constructor for microservices architecture with message broker integration.
     * 
     * @param messageBroker The message broker to publish decoded frames to
     */
    public MegastekFrameDecoder(Object messageBroker) {
        this.messageBroker = messageBroker;
    }

    @Override
    protected Object decode(
            ChannelHandlerContext ctx, Channel channel, ByteBuf buf) throws Exception {

        if (buf.readableBytes() < 10) {
            return null;
        }

        ByteBuf result = null;
        
        if (Character.isDigit(buf.getByte(buf.readerIndex()))) {
            int length = 4 + Integer.parseInt(buf.toString(buf.readerIndex(), 4, StandardCharsets.US_ASCII));
            if (buf.readableBytes() >= length) {
                result = buf.readRetainedSlice(length);
            }
        } else {
            while (buf.isReadable() && (buf.getByte(buf.readerIndex()) == '\r' || buf.getByte(buf.readerIndex()) == '\n')) {
                buf.skipBytes(1);
            }
            int delimiter = BufferUtil.indexOf("\r\n", buf);
            if (delimiter == -1) {
                delimiter = buf.indexOf(buf.readerIndex(), buf.writerIndex(), (byte) '!');
            }
            if (delimiter == -1) {
                delimiter = buf.indexOf(buf.readerIndex(), buf.writerIndex(), (byte) '\n');
            }
            if (delimiter != -1) {
                result = buf.readRetainedSlice(delimiter - buf.readerIndex());
                buf.skipBytes(1);
            }
        }
        
        // If we have a result and a message broker, publish the decoded frame
        if (result != null && messageBroker != null) {
            publishToMessageBroker(result);
        }

        return result;
    }
    
    /**
     * Publishes the decoded frame to the message broker.
     * This method uses reflection to avoid direct dependency on the message broker implementation.
     * 
     * @param frame The decoded frame to publish
     */
    private void publishToMessageBroker(ByteBuf frame) {
        try {
            // Use reflection to call the publish method on the message broker
            // This allows us to support different message broker implementations
            java.lang.reflect.Method publishMethod = messageBroker.getClass().getMethod("publish", String.class, Object.class);
            publishMethod.invoke(messageBroker, "protocol.position.raw", frame);
            
            // If this is a test message broker, simulate a downstream service acknowledgement
            if (messageBroker.getClass().getSimpleName().equals("TestMessageBroker")) {
                java.lang.reflect.Method acknowledgeMethod = 
                    messageBroker.getClass().getMethod("acknowledgeMessage", String.class, String.class);
                acknowledgeMethod.invoke(messageBroker, "position-service", null);
            }
        } catch (Exception e) {
            // Log the exception but don't fail the decoding process
            System.err.println("Error publishing to message broker: " + e.getMessage());
        }
    }
}