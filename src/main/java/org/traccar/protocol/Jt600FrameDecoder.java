/*
 * Copyright 2012 - 2025 Anton Tananaev (anton@traccar.org)
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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.traccar.BaseFrameDecoder;

import java.text.ParseException;

// Service discovery imports
import org.traccar.discovery.ServiceDiscoveryManager;

// Distributed tracing imports
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

// Metrics imports
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Frame decoder for JT600 protocol.
 * Supports both direct and service-based communication modes.
 */
public class Jt600FrameDecoder extends BaseFrameDecoder {

    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    
    // Metrics
    private final Counter messagesCounter;
    private final Counter invalidMessagesCounter;
    private final Timer decodeTimer;
    
    /**
     * Constructs a new JT600 frame decoder with service discovery and telemetry.
     *
     * @param serviceDiscoveryManager Service discovery manager for protocol service discovery
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     */
    public Jt600FrameDecoder(ServiceDiscoveryManager serviceDiscoveryManager, Tracer tracer, MeterRegistry meterRegistry) {
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.messagesCounter = Counter.builder("protocol.jt600.messages")
                .description("Number of JT600 messages received")
                .register(meterRegistry);
        
        this.invalidMessagesCounter = Counter.builder("protocol.jt600.messages.invalid")
                .description("Number of invalid JT600 messages received")
                .register(meterRegistry);
        
        this.decodeTimer = Timer.builder("protocol.jt600.decode.time")
                .description("Time taken to decode JT600 messages")
                .register(meterRegistry);
    }
    
    /**
     * Default constructor for backward compatibility with direct mode.
     */
    public Jt600FrameDecoder() {
        this.serviceDiscoveryManager = null;
        this.tracer = null;
        this.meterRegistry = null;
        this.messagesCounter = null;
        this.invalidMessagesCounter = null;
        this.decodeTimer = null;
    }

    @Override
    protected Object decode(
            ChannelHandlerContext ctx, Channel channel, ByteBuf buf) throws Exception {
        
        // Start tracing span if tracer is available
        Span span = null;
        Scope scope = null;
        Timer.Sample timerSample = null;
        
        if (tracer != null) {
            span = tracer.spanBuilder("jt600.decode").startSpan();
            scope = span.makeCurrent();
            span.setAttribute("protocol", "jt600");
        }
        
        if (meterRegistry != null) {
            timerSample = Timer.start(meterRegistry);
        }
        
        try {
            if (messagesCounter != null) {
                messagesCounter.increment();
            }
            
            if (buf.readableBytes() < 10) {
                if (span != null) {
                    span.setAttribute("incomplete", true);
                }
                return null;
            }

            char type = (char) buf.getByte(buf.readerIndex());

            if (type == '$') {
                boolean longFormat = Jt600ProtocolDecoder.isLongFormat(buf);
                int length = buf.getUnsignedShort(buf.readerIndex() + (longFormat ? 8 : 7)) + 10;
                if (length <= buf.readableBytes()) {
                    if (span != null) {
                        span.setAttribute("message.type", "binary");
                        span.setAttribute("message.format", longFormat ? "long" : "short");
                        span.setAttribute("message.length", length);
                    }
                    return buf.readRetainedSlice(length);
                }
            } else if (type == '(') {
                int endIndex = buf.indexOf(buf.readerIndex(), buf.writerIndex(), (byte) ')');
                if (endIndex >= 0) {
                    ByteBuf result = Unpooled.buffer(endIndex + 1 - buf.readerIndex());

                    while (buf.readerIndex() <= endIndex) {
                        int b = buf.readUnsignedByte();
                        if (b == 0x3d) {
                            int ext = buf.readUnsignedByte();
                            if (ext == 0x15) {
                                result.writeByte(0x28);
                            } else if (ext == 0x14) {
                                result.writeByte(0x29);
                            } else if (ext == 0x11) {
                                result.writeByte(0x2c);
                            } else if (ext == 0x00) {
                                result.writeByte(0x3d);
                            }
                        } else {
                            result.writeByte(b);
                        }
                    }
                    
                    if (span != null) {
                        span.setAttribute("message.type", "text");
                        span.setAttribute("message.length", result.readableBytes());
                    }

                    return result;
                }
            } else {
                if (invalidMessagesCounter != null) {
                    invalidMessagesCounter.increment();
                }
                if (span != null) {
                    span.setAttribute("error", true);
                    span.setAttribute("error.type", "unknown_message_type");
                }
                throw new ParseException(null, 0); // unknown message
            }

            return null;
        } finally {
            // Record metrics and end tracing span
            if (timerSample != null) {
                timerSample.stop(decodeTimer);
            }
            
            if (scope != null) {
                scope.close();
            }
            
            if (span != null) {
                span.end();
            }
        }
    }

}