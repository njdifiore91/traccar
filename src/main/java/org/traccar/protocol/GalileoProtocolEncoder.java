/*
 * Copyright 2017 - 2019 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.context.Scope;
import org.traccar.BaseProtocolEncoder;
import org.traccar.Protocol;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.helper.Checksum;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Command;

import java.nio.charset.StandardCharsets;

/**
 * Protocol encoder for Galileo devices.
 * Supports both direct communication and service-based communication through message broker.
 */
public class GalileoProtocolEncoder extends BaseProtocolEncoder {

    private final ServiceDiscovery serviceDiscovery;
    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Timer encodeTimer;

    /**
     * Constructor for the Galileo protocol encoder.
     *
     * @param protocol The protocol instance
     * @param serviceDiscovery Service discovery manager for protocol service discovery
     * @param messageProducer Message producer for asynchronous message publishing
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Meter registry for metrics collection
     */
    public GalileoProtocolEncoder(
            Protocol protocol,
            ServiceDiscovery serviceDiscovery,
            MessageProducer messageProducer,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        super(protocol);
        this.serviceDiscovery = serviceDiscovery;
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.encodeTimer = Timer.builder("protocol.galileo.encode")
                .description("Time taken to encode Galileo protocol messages")
                .register(meterRegistry);
    }

    /**
     * Legacy constructor for backward compatibility.
     * 
     * @param protocol The protocol instance
     */
    public GalileoProtocolEncoder(Protocol protocol) {
        super(protocol);
        this.serviceDiscovery = null;
        this.messageProducer = null;
        this.tracer = null;
        this.meterRegistry = null;
        this.encodeTimer = null;
    }

    /**
     * Encodes a text message for a Galileo device.
     *
     * @param uniqueId Device unique identifier
     * @param text Text message to encode
     * @return Encoded ByteBuf
     */
    private ByteBuf encodeText(String uniqueId, String text) {
        Span span = null;
        Scope scope = null;
        Timer.Sample timerSample = null;
        
        try {
            // Start tracing if tracer is available
            if (tracer != null) {
                span = tracer.spanBuilder("galileo.encode.text")
                        .setAttribute("device.id", uniqueId)
                        .setAttribute("message.type", "text")
                        .setAttribute("message.length", text.length())
                        .startSpan();
                scope = span.makeCurrent();
            }
            
            // Start timing if metrics are available
            if (encodeTimer != null) {
                timerSample = Timer.start(meterRegistry);
            }

            ByteBuf buf = Unpooled.buffer(256);

            buf.writeByte(0x01);
            buf.writeShortLE(uniqueId.length() + text.length() + 11);

            buf.writeByte(0x03); // imei tag
            buf.writeBytes(uniqueId.getBytes(StandardCharsets.US_ASCII));

            buf.writeByte(0x04); // device id tag
            buf.writeShortLE(0); // not needed if imei provided

            buf.writeByte(0xE0); // index tag
            buf.writeIntLE(0); // index

            buf.writeByte(0xE1); // command text tag
            buf.writeByte(text.length());
            buf.writeBytes(text.getBytes(StandardCharsets.US_ASCII));

            buf.writeShortLE(Checksum.crc16(Checksum.CRC16_MODBUS, buf.nioBuffer(0, buf.writerIndex())));

            return buf;
        } finally {
            // Record metrics if available
            if (timerSample != null && encodeTimer != null) {
                timerSample.stop(encodeTimer);
            }
            
            // End tracing span if available
            if (scope != null) {
                scope.close();
            }
            if (span != null) {
                span.end();
            }
        }
    }

    @Override
    protected Object encodeCommand(Command command) {
        Span span = null;
        Scope scope = null;
        Timer.Sample timerSample = null;
        
        try {
            // Start tracing if tracer is available
            if (tracer != null) {
                span = tracer.spanBuilder("galileo.encode.command")
                        .setAttribute("device.id", String.valueOf(command.getDeviceId()))
                        .setAttribute("command.type", command.getType())
                        .startSpan();
                scope = span.makeCurrent();
            }
            
            // Start timing if metrics are available
            if (encodeTimer != null) {
                timerSample = Timer.start(meterRegistry);
            }

            ByteBuf result = switch (command.getType()) {
                case Command.TYPE_CUSTOM -> encodeText(getUniqueId(command.getDeviceId()),
                        command.getString(Command.KEY_DATA));
                case Command.TYPE_OUTPUT_CONTROL -> encodeText(getUniqueId(command.getDeviceId()),
                        "Out " + command.getInteger(Command.KEY_INDEX) + "," + command.getString(Command.KEY_DATA));
                default -> null;
            };
            
            // If message broker is available and result is not null, publish the command asynchronously
            if (messageProducer != null && result != null) {
                messageProducer.send("command.galileo", command.getDeviceId(), result);
            }
            
            return result;
        } finally {
            // Record metrics if available
            if (timerSample != null && encodeTimer != null) {
                timerSample.stop(encodeTimer);
            }
            
            // End tracing span if available
            if (scope != null) {
                scope.close();
            }
            if (span != null) {
                span.end();
            }
        }
    }

}