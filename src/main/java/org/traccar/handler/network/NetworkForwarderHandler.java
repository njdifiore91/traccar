/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.handler.network;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.forward.NetworkForwarder;

import jakarta.inject.Inject;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Optional;

/**
 * Netty channel handler that forwards network traffic to another destination.
 * Implements circuit breaker pattern, distributed tracing, and metrics collection.
 */
public class NetworkForwarderHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkForwarderHandler.class);
    private static final String METRIC_PREFIX = "traccar.network.forwarder.handler";

    private final int port;
    private NetworkForwarder networkForwarder;
    private CircuitBreaker circuitBreaker;
    private Tracer tracer;
    private MeterRegistry meterRegistry;
    private Counter forwardSuccessCounter;
    private Counter forwardFailureCounter;
    private Timer forwardLatencyTimer;

    public NetworkForwarderHandler(int port) {
        this.port = port;
    }

    @Inject
    public void setNetworkForwarder(NetworkForwarder networkForwarder) {
        this.networkForwarder = networkForwarder;
    }

    @Inject
    public void setCircuitBreakerRegistry(CircuitBreakerRegistry circuitBreakerRegistry) {
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("networkForwarderHandler");
    }

    @Inject
    public void setMeterRegistry(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.forwardSuccessCounter = meterRegistry.counter(METRIC_PREFIX + ".forward.success");
        this.forwardFailureCounter = meterRegistry.counter(METRIC_PREFIX + ".forward.failure");
        this.forwardLatencyTimer = meterRegistry.timer(METRIC_PREFIX + ".forward.latency");
    }

    @Inject
    public void setTracer(Optional<Tracer> tracer) {
        this.tracer = tracer.orElse(GlobalOpenTelemetry.getTracer("org.traccar.handler.network.NetworkForwarderHandler"));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // Create a span for this operation
        Span span = tracer.spanBuilder("network.forwarder.handler.channelRead")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("network.port", port)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            boolean datagram = ctx.channel() instanceof DatagramChannel;
            SocketAddress remoteAddress;
            ByteBuf buffer;
            
            // Extract information based on channel type
            if (datagram) {
                DatagramPacket message = (DatagramPacket) msg;
                remoteAddress = message.recipient();
                buffer = message.content();
                span.setAttribute("network.protocol", "udp");
            } else {
                remoteAddress = ctx.channel().remoteAddress();
                buffer = (ByteBuf) msg;
                span.setAttribute("network.protocol", "tcp");
            }

            // Add remote address information to span
            if (remoteAddress instanceof InetSocketAddress) {
                InetSocketAddress inetAddress = (InetSocketAddress) remoteAddress;
                span.setAttribute("network.peer.address", inetAddress.getAddress().getHostAddress());
                span.setAttribute("network.peer.port", inetAddress.getPort());
            }

            // Extract data from buffer
            byte[] data = new byte[buffer.readableBytes()];
            buffer.getBytes(buffer.readerIndex(), data);
            span.setAttribute("data.size", data.length);

            // Forward data with circuit breaker pattern and metrics
            Timer.Sample timerSample = Timer.start(meterRegistry);
            try {
                boolean result = circuitBreaker.executeSupplier(() -> 
                    networkForwarder.forward((InetSocketAddress) remoteAddress, port, datagram, data));
                
                if (result) {
                    span.setStatus(StatusCode.OK);
                    forwardSuccessCounter.increment();
                } else {
                    span.setStatus(StatusCode.ERROR, "Forward operation failed");
                    forwardFailureCounter.increment();
                    LOGGER.warn("Failed to forward data to destination");
                }
            } catch (Exception e) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                forwardFailureCounter.increment();
                LOGGER.error("Error forwarding data", e);
                
                // If circuit breaker is open, we'll get a CallNotPermittedException
                if (e.getClass().getSimpleName().equals("CallNotPermittedException")) {
                    LOGGER.warn("Circuit breaker is open, forwarding temporarily disabled");
                }
            } finally {
                timerSample.stop(forwardLatencyTimer);
            }
            
            // Continue processing the message in the pipeline
            super.channelRead(ctx, msg);
        } finally {
            span.end();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // Create a span for this operation
        Span span = tracer.spanBuilder("network.forwarder.handler.channelInactive")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            if (!(ctx.channel() instanceof DatagramChannel)) {
                InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
                
                // Add remote address information to span
                span.setAttribute("network.peer.address", remoteAddress.getAddress().getHostAddress());
                span.setAttribute("network.peer.port", remoteAddress.getPort());
                
                try {
                    // Execute with circuit breaker
                    circuitBreaker.executeRunnable(() -> 
                        networkForwarder.disconnect(remoteAddress));
                    span.setStatus(StatusCode.OK);
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    LOGGER.error("Error disconnecting channel", e);
                }
            }
            super.channelInactive(ctx);
        } finally {
            span.end();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("network.forwarder.handler.exceptionCaught")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            span.recordException(cause);
            span.setStatus(StatusCode.ERROR, cause.getMessage());
            
            LOGGER.error("Exception in NetworkForwarderHandler", cause);
            ctx.fireExceptionCaught(cause);
        } finally {
            span.end();
        }
    }
}