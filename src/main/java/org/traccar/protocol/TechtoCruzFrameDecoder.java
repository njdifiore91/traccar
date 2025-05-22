/*
 * Copyright 2021 - 2024 Anton Tananaev (anton@traccar.org)
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

import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.BaseFrameDecoder;
import org.traccar.MicrometerMetricsManager;
import org.traccar.OpenTelemetryManager;

import java.nio.charset.StandardCharsets;

/**
 * Frame decoder for TechtoCruz protocol.
 * Extracts frames from the incoming ByteBuf based on a length field in the message.
 * Supports distributed tracing and metrics collection in a microservices architecture.
 */
public class TechtoCruzFrameDecoder extends BaseFrameDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(TechtoCruzFrameDecoder.class);
    
    private final OpenTelemetryManager openTelemetryManager;
    private final MicrometerMetricsManager metricsManager;
    private final Timer frameDecodingTimer;
    
    /**
     * Default constructor for backward compatibility and direct communication mode.
     */
    public TechtoCruzFrameDecoder() {
        this(null, null);
    }
    
    /**
     * Constructor with telemetry and metrics support for microservices architecture.
     * 
     * @param openTelemetryManager The OpenTelemetry manager for distributed tracing
     * @param metricsManager The metrics manager for performance monitoring
     */
    public TechtoCruzFrameDecoder(OpenTelemetryManager openTelemetryManager, MicrometerMetricsManager metricsManager) {
        this.openTelemetryManager = openTelemetryManager;
        this.metricsManager = metricsManager;
        
        // Initialize metrics if metrics manager is provided
        if (metricsManager != null) {
            this.frameDecodingTimer = metricsManager.createTimer(
                    "traccar.protocol.frame.decoding",
                    "Time taken to decode TechtoCruz protocol frames",
                    io.micrometer.core.instrument.Tag.of("protocol", "techtocruz"));
        } else {
            this.frameDecodingTimer = null;
        }
    }

    @Override
    protected Object decode(
            ChannelHandlerContext ctx, Channel channel, ByteBuf buf) throws Exception {
        
        // Start metrics timer if available
        Timer.Sample timerSample = null;
        if (frameDecodingTimer != null) {
            timerSample = Timer.start(metricsManager.getRegistry());
        }
        
        // Start span for distributed tracing if available
        Span span = null;
        if (openTelemetryManager != null) {
            span = openTelemetryManager.createSpan("techtocruz.frame.decode");
            span.setAttribute("protocol", "techtocruz");
            span.setAttribute("readable_bytes", buf.readableBytes());
        }
        
        try {
            // Original frame decoding logic
            int lengthStart = buf.readerIndex() + 3;
            int lengthEnd = buf.indexOf(lengthStart, buf.writerIndex(), (byte) ',');
            
            if (lengthEnd > 0) {
                int length = lengthStart
                        + Integer.parseInt(buf.toString(lengthStart, lengthEnd - lengthStart, StandardCharsets.US_ASCII));
                
                if (span != null) {
                    span.setAttribute("frame_length", length);
                }
                
                if (buf.readableBytes() >= length) {
                    // Record successful frame decoding in span
                    if (span != null) {
                        span.setStatus(StatusCode.OK);
                    }
                    
                    // Stop and record metrics if available
                    if (timerSample != null) {
                        timerSample.stop(frameDecodingTimer);
                    }
                    
                    return buf.readRetainedSlice(length);
                }
            }
            
            // Record incomplete frame in span
            if (span != null) {
                span.setAttribute("incomplete_frame", true);
                span.setStatus(StatusCode.OK);
            }
            
            return null;
            
        } catch (Exception e) {
            // Record error in span
            if (span != null) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
            }
            
            LOGGER.warn("Frame decoding error", e);
            throw e;
            
        } finally {
            // End span if it was created
            if (span != null) {
                span.end();
            }
        }
    }
}