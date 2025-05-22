/*
 * Copyright 2023 Hossain Mohammad Seym (seym45@gmail.com)
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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.traccar.BaseProtocolEncoder;
import org.traccar.Protocol;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.helper.Checksum;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Command;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Protocol encoder for Gator GPS devices.
 * This encoder supports both direct and service-based communication modes.
 */
public class GatorProtocolEncoder extends BaseProtocolEncoder {

    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Timer encodeTimer;

    /**
     * Constructor for direct communication mode (backward compatibility).
     *
     * @param protocol The protocol instance
     */
    public GatorProtocolEncoder(Protocol protocol) {
        this(protocol, null, null, null, null);
    }

    /**
     * Constructor for service-based communication mode with full microservices support.
     *
     * @param protocol               The protocol instance
     * @param serviceDiscoveryManager Service discovery manager for locating protocol services
     * @param messageProducer        Message producer for asynchronous position publishing
     * @param tracer                 OpenTelemetry tracer for distributed tracing
     * @param meterRegistry          Micrometer registry for metrics collection
     */
    public GatorProtocolEncoder(Protocol protocol, 
                               ServiceDiscoveryManager serviceDiscoveryManager,
                               MessageProducer messageProducer,
                               Tracer tracer,
                               MeterRegistry meterRegistry) {
        super(protocol);
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics if registry is provided
        if (meterRegistry != null) {
            this.encodeTimer = Timer.builder("protocol.gator.encode")
                    .description("Time taken to encode Gator protocol messages")
                    .tag("protocol", "gator")
                    .register(meterRegistry);
        } else {
            this.encodeTimer = null;
        }
    }

    /**
     * Encodes device ID into a ByteBuf according to Gator protocol specifications.
     *
     * @param deviceId The device ID to encode
     * @return ByteBuf containing the encoded device ID
     */
    public ByteBuf encodeId(long deviceId) {
        Span span = null;
        if (tracer != null) {
            span = tracer.spanBuilder("GatorProtocolEncoder.encodeId").startSpan();
            span.setAttribute("deviceId", deviceId);
        }
        
        try {
            ByteBuf buf = Unpooled.buffer();

            String id = getUniqueId(deviceId);

            int firstDigit = Integer.parseInt(id.substring(1, 3)) - 30;

            buf.writeByte(Integer.parseInt(id.substring(3, 5)) | (((firstDigit >> 3) & 1) << 7));
            buf.writeByte(Integer.parseInt(id.substring(5, 7)) | (((firstDigit >> 2) & 1) << 7));
            buf.writeByte(Integer.parseInt(id.substring(7, 9)) | (((firstDigit >> 1) & 1) << 7));
            buf.writeByte(Integer.parseInt(id.substring(9)) | ((firstDigit & 1) << 7));

            return buf;
        } finally {
            if (span != null) {
                span.end();
            }
        }
    }

    /**
     * Encodes content into a ByteBuf according to Gator protocol specifications.
     *
     * @param deviceId The device ID
     * @param type The message type
     * @param content The content to encode (can be null)
     * @return ByteBuf containing the encoded content
     */
    private ByteBuf encodeContent(long deviceId, int type, ByteBuf content) {
        Span span = null;
        if (tracer != null) {
            span = tracer.spanBuilder("GatorProtocolEncoder.encodeContent").startSpan();
            span.setAttribute("deviceId", deviceId);
            span.setAttribute("messageType", type);
        }
        
        try {
            ByteBuf buf = Unpooled.buffer();

            buf.writeByte(0x24);
            buf.writeByte(0x24);
            buf.writeByte(type);
            buf.writeByte(0x00);

            buf.writeByte(4 + 1 + (content != null ? content.readableBytes() : 0) + 1); // length

            ByteBuf pseudoIPAddress = encodeId(deviceId);
            buf.writeBytes(pseudoIPAddress);

            if (content != null) {
                buf.writeBytes(content);
            }

            int checksum = Checksum.xor(buf.nioBuffer());
            buf.writeByte(checksum);

            buf.writeByte(0x0D);

            return buf;
        } finally {
            if (span != null) {
                span.end();
            }
        }
    }

    /**
     * Encodes a command into a ByteBuf according to Gator protocol specifications.
     *
     * @param command The command to encode
     * @return ByteBuf containing the encoded command
     */
    @Override
    protected Object encodeCommand(Command command) {
        if (encodeTimer != null) {
            return encodeTimer.record(() -> doEncodeCommand(command));
        } else {
            return doEncodeCommand(command);
        }
    }
    
    /**
     * Internal method to encode a command with tracing support.
     *
     * @param command The command to encode
     * @return ByteBuf containing the encoded command
     */
    private Object doEncodeCommand(Command command) {
        Span span = null;
        if (tracer != null) {
            span = tracer.spanBuilder("GatorProtocolEncoder.encodeCommand").startSpan();
            span.setAttribute("deviceId", command.getDeviceId());
            span.setAttribute("type", command.getType());
        }
        
        try {
            ByteBuf content = Unpooled.buffer();
            Object result = null;
            
            // Encode command based on type
            result = switch (command.getType()) {
                case Command.TYPE_POSITION_SINGLE ->
                        encodeContent(command.getDeviceId(), GatorProtocolDecoder.MSG_POSITION_REQUEST, null);
                case Command.TYPE_ENGINE_STOP ->
                        encodeContent(command.getDeviceId(), GatorProtocolDecoder.MSG_CLOSE_OIL_DUCT, null);
                case Command.TYPE_ENGINE_RESUME ->
                        encodeContent(command.getDeviceId(), GatorProtocolDecoder.MSG_RESTORE_OIL_DUCT, null);
                case Command.TYPE_SET_SPEED_LIMIT -> {
                    content.writeByte(command.getInteger(Command.KEY_DATA));
                    yield encodeContent(command.getDeviceId(), GatorProtocolDecoder.MSG_RESET_MILEAGE, content);
                }
                case Command.TYPE_SET_ODOMETER -> {
                    content.writeShort(command.getInteger(Command.KEY_DATA));
                    yield encodeContent(command.getDeviceId(), GatorProtocolDecoder.MSG_OVERSPEED_ALARM, content);
                }
                default -> null;
            };
            
            // Publish command event to message broker if available
            if (messageProducer != null && result != null) {
                messageProducer.send("protocol.commands", 
                    Map.of(
                        "deviceId", command.getDeviceId(),
                        "type", command.getType(),
                        "protocol", getProtocolName()
                    ));
            }
            
            // Record metrics if registry is available
            if (meterRegistry != null) {
                meterRegistry.counter("protocol.gator.commands", 
                        "type", command.getType(), 
                        "protocol", getProtocolName())
                    .increment();
            }
            
            return result;
        } finally {
            if (span != null) {
                span.end();
            }
        }
    }
    
    /**
     * Gets the protocol name for logging and metrics.
     *
     * @return The protocol name
     */
    private String getProtocolName() {
        return getProtocol() != null ? getProtocol().getName() : "gator";
    }
}