/*
 * Copyright 2018 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.MeterRegistry;
import org.traccar.BaseFrameDecoder;
import org.traccar.helper.BitUtil;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.messaging.MessageProducer;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

/**
 * Frame decoder for Tek protocol devices.
 * Integrates with distributed tracing, metrics collection, and service discovery.
 * Supports both direct and service-based communication through the ServiceDiscoveryManager.
 */
public class TekFrameDecoder extends BaseFrameDecoder {

    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final MessageProducer messageProducer;
    private final Counter framesDecodedCounter;
    private final Counter invalidFramesCounter;
    private final Timer decodeTimer;

    /**
     * Constructs a new TekFrameDecoder with dependencies injected.
     *
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param meterRegistry The metrics registry for performance monitoring
     * @param serviceDiscoveryManager The service discovery manager for protocol service discovery
     * @param messageProducer The message producer for asynchronous position publishing
     */
    @Inject
    public TekFrameDecoder(
            Tracer tracer,
            MeterRegistry meterRegistry,
            ServiceDiscoveryManager serviceDiscoveryManager,
            MessageProducer messageProducer) {
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.messageProducer = messageProducer;
        
        // Initialize metrics counters
        this.framesDecodedCounter = Counter.builder("protocol.tek.frames.decoded")
                .description("Number of Tek frames successfully decoded")
                .tag("protocol", "tek")
                .register(meterRegistry);
        
        this.invalidFramesCounter = Counter.builder("protocol.tek.frames.invalid")
                .description("Number of invalid Tek frames encountered")
                .tag("protocol", "tek")
                .register(meterRegistry);
                
        this.decodeTimer = Timer.builder("protocol.tek.decode.time")
                .description("Time taken to decode Tek frames")
                .tag("protocol", "tek")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
    }

    @Override
    protected Object decode(
            ChannelHandlerContext ctx, Channel channel, ByteBuf buf) throws Exception {

        // Create a span for this decode operation with appropriate attributes
        Span span = tracer.spanBuilder("tek.frame.decode")
                .setParent(Context.current())
                .setAttribute("protocol.name", "tek")
                .setAttribute("protocol.type", "gps")
                .setAttribute("channel.id", ctx.channel().id().asShortText())
                .setAttribute("buffer.readable.bytes", buf.readableBytes())
                .setAttribute("service.name", "protocol-service")
                .startSpan();
        
        // Use timer to measure decode performance
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            // Check if we have enough data to decode the frame
            if (buf.readableBytes() < 17) {
                span.setAttribute("decode.result", "insufficient_data");
                return null;
            }

            // Calculate expected frame length
            int length = 17 + buf.getUnsignedByte(16) + (BitUtil.from(buf.getUnsignedByte(15), 6) << 6);
            span.setAttribute("frame.expected.length", length);
            
            // Check if we have a complete frame
            if (buf.readableBytes() >= length) {
                // Successfully decoded the frame
                span.setAttribute("decode.result", "success");
                span.setStatus(StatusCode.OK);
                framesDecodedCounter.increment();
                
                // If service discovery is available, log the service information
                if (serviceDiscoveryManager != null && serviceDiscoveryManager.isAvailable()) {
                    span.setAttribute("service.discovery.available", true);
                    span.setAttribute("service.discovery.provider", 
                            serviceDiscoveryManager.getProviderType());
                } else {
                    span.setAttribute("service.discovery.available", false);
                }
                
                return buf.readRetainedSlice(length);
            } else {
                // Frame is incomplete, need more data
                span.setAttribute("decode.result", "incomplete_frame");
                return null;
            }
        } catch (Exception e) {
            // Record exception and error details in the span
            span.recordException(e);
            span.setAttribute("decode.result", "error");
            span.setAttribute("error.type", e.getClass().getName());
            span.setAttribute("error.message", e.getMessage() != null ? e.getMessage() : "null");
            span.setStatus(StatusCode.ERROR, e.getMessage() != null ? e.getMessage() : "Decode error");
            
            // Increment error counter for monitoring
            invalidFramesCounter.increment();
            throw e;
        } finally {
            // Record timing metrics and end the span
            sample.stop(decodeTimer);
            span.setAttribute("metrics.recorded", true);
            span.end();
        }
    }
}