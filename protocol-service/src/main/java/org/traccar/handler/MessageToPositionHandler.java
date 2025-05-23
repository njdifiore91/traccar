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
package org.traccar.handler;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Position;
import org.traccar.messaging.MessageProducer;

import io.opentelemetry.api.trace.Span;

/**
 * Handler that converts decoded messages to Position objects and publishes them to a message broker.
 * This replaces the direct handler chain with message broker integration for microservices architecture.
 */
@ChannelHandler.Sharable
public class MessageToPositionHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageToPositionHandler.class);
    private final MessageProducer messageProducer;

    /**
     * Construct message handler with provided message producer.
     *
     * @param messageProducer Message broker producer for publishing positions
     */
    public MessageToPositionHandler(MessageProducer messageProducer) {
        this.messageProducer = messageProducer;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof Position) {
            Position position = (Position) msg;
            
            // Get current span from OpenTelemetry context for correlation
            Span span = Span.current();
            span.setAttribute("position.deviceId", position.getDeviceId());
            span.setAttribute("position.protocol", position.getProtocol());
            
            try {
                // Publish position to message broker instead of direct processing
                messageProducer.sendPosition(position);
                LOGGER.debug("Position published to message broker: {}", position.getDeviceId());
            } catch (Exception e) {
                LOGGER.warn("Failed to publish position to message broker", e);
                span.recordException(e);
            }
        } else {
            // Pass through non-position messages
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.warn("Error processing message", cause);
        Span.current().recordException(cause);
    }
}