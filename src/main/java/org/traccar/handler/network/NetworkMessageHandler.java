/*
 * Copyright 2019 - 2025 Anton Tananaev (anton@traccar.org)
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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.NetworkMessage;
import org.traccar.messaging.MessageBroker;

import jakarta.inject.Inject;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles network message conversion between raw Netty messages and NetworkMessage objects.
 * Supports both direct processing and service-based processing via message broker.
 * Implements distributed tracing and metrics collection.
 */
public class NetworkMessageHandler extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkMessageHandler.class);
    
    private final Tracer tracer;
    private final MessageBroker messageBroker;
    private final MeterRegistry meterRegistry;
    private final boolean useDirectProcessing;
    
    /**
     * Default constructor for backward compatibility.
     * Uses direct processing without message broker integration or telemetry.
     */
    public NetworkMessageHandler() {
        this.tracer = null;
        this.messageBroker = null;
        this.meterRegistry = null;
        this.useDirectProcessing = true;
        LOGGER.debug("Created NetworkMessageHandler with direct processing (backward compatibility mode)");
    }
    
    /**
     * Constructs a NetworkMessageHandler with full microservices support.
     *
     * @param messageBroker The message broker for asynchronous processing
     * @param meterRegistry The meter registry for metrics collection
     * @param useDirectProcessing Whether to use direct processing or service-based processing
     */
    @Inject
    public NetworkMessageHandler(
            MessageBroker messageBroker,
            MeterRegistry meterRegistry,
            @org.traccar.config.ConfigProperty("network.directProcessing") boolean useDirectProcessing) {
        this.tracer = GlobalOpenTelemetry.getTracer("org.traccar.handler.network");
        this.messageBroker = messageBroker;
        this.meterRegistry = meterRegistry;
        this.useDirectProcessing = useDirectProcessing;
        LOGGER.info("Created NetworkMessageHandler with {} processing mode", 
                useDirectProcessing ? "direct" : "service-based");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Span span = null;
        Scope scope = null;
        Timer.Sample timerSample = null;
        
        try {
            // Start metrics timing
            if (meterRegistry != null) {
                timerSample = Timer.start(meterRegistry);
            }
            
            // Create tracing span
            if (tracer != null) {
                span = tracer.spanBuilder("network.message.read")
                        .setSpanKind(SpanKind.SERVER)
                        .startSpan();
                scope = span.makeCurrent();
            }
            
            NetworkMessage networkMessage = null;
            
            // Convert raw Netty message to NetworkMessage
            if (ctx.channel() instanceof DatagramChannel) {
                DatagramPacket packet = (DatagramPacket) msg;
                networkMessage = new NetworkMessage(packet.content(), packet.sender());
                
                if (span != null) {
                    span.setAttribute("network.transport", "udp");
                    span.setAttribute("network.peer.address", packet.sender().getAddress().getHostAddress());
                    span.setAttribute("network.peer.port", packet.sender().getPort());
                }
            } else if (msg instanceof ByteBuf buffer) {
                networkMessage = new NetworkMessage(buffer, ctx.channel().remoteAddress());
                
                if (span != null) {
                    span.setAttribute("network.transport", "tcp");
                    if (ctx.channel().remoteAddress() instanceof InetSocketAddress) {
                        InetSocketAddress address = (InetSocketAddress) ctx.channel().remoteAddress();
                        span.setAttribute("network.peer.address", address.getAddress().getHostAddress());
                        span.setAttribute("network.peer.port", address.getPort());
                    }
                }
            }
            
            // Generate correlation ID if not present
            if (networkMessage != null && networkMessage.getCorrelationId() == null) {
                networkMessage.setCorrelationId(java.util.UUID.randomUUID().toString());
            }
            
            // Add trace context to NetworkMessage
            if (span != null && networkMessage != null) {
                span.setAttribute("messaging.message.id", networkMessage.getCorrelationId());
                
                // Extract current trace context
                networkMessage.setTraceId(span.getSpanContext().getTraceId());
                networkMessage.setSpanId(span.getSpanContext().getSpanId());
                networkMessage.setTraceFlags(span.getSpanContext().getTraceFlags().asByte());
            }
            
            // Process the message
            if (networkMessage != null) {
                if (useDirectProcessing || messageBroker == null) {
                    // Direct processing - fire the event through the pipeline
                    ctx.fireChannelRead(networkMessage);
                    
                    if (span != null) {
                        span.setAttribute("processing.mode", "direct");
                    }
                } else {
                    // Service-based processing - publish to message broker
                    try {
                        Map<String, String> headers = new HashMap<>();
                        headers.put("protocol", ctx.channel().attr(org.traccar.TrackerConnector.PROTOCOL_NAME).get());
                        
                        networkMessage.addHeaders(headers);
                        networkMessage.setRoutingKey("protocol.inbound");
                        
                        messageBroker.publish("protocol.inbound", networkMessage);
                        
                        if (span != null) {
                            span.setAttribute("processing.mode", "broker");
                            span.setAttribute("messaging.destination", "protocol.inbound");
                            span.setAttribute("messaging.system", "rabbitmq");
                        }
                        
                        LOGGER.debug("Published message with correlation ID {} to broker", 
                                networkMessage.getCorrelationId());
                    } catch (Exception e) {
                        LOGGER.error("Failed to publish message to broker", e);
                        if (span != null) {
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, "Failed to publish message to broker");
                        }
                        
                        // Fallback to direct processing
                        LOGGER.warn("Falling back to direct processing due to broker failure");
                        ctx.fireChannelRead(networkMessage);
                    }
                }
            } else {
                // Unknown message type, pass through
                ctx.fireChannelRead(msg);
                
                if (span != null) {
                    span.setAttribute("message.type", "unknown");
                }
            }
            
            // Record metrics
            if (timerSample != null && meterRegistry != null) {
                timerSample.stop(meterRegistry.timer("network.message.read", 
                        "transport", ctx.channel() instanceof DatagramChannel ? "udp" : "tcp",
                        "processing", useDirectProcessing ? "direct" : "broker"));
            }
            
            // Complete span successfully
            if (span != null) {
                span.setStatus(StatusCode.OK);
            }
            
        } catch (Exception e) {
            LOGGER.error("Error processing network message", e);
            
            // Record error in span
            if (span != null) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
            }
            
            // Continue with pipeline
            ctx.fireChannelRead(msg);
        } finally {
            // Close tracing scope and span
            if (scope != null) {
                scope.close();
            }
            if (span != null) {
                span.end();
            }
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        Span span = null;
        Scope scope = null;
        Timer.Sample timerSample = null;
        
        try {
            // Start metrics timing
            if (meterRegistry != null) {
                timerSample = Timer.start(meterRegistry);
            }
            
            // Create tracing span
            if (tracer != null && msg instanceof NetworkMessage) {
                NetworkMessage networkMessage = (NetworkMessage) msg;
                
                // Create a new span or continue from existing trace context
                Context parentContext = null;
                if (networkMessage.getTraceId() != null && networkMessage.getSpanId() != null) {
                    // Extract parent context from message
                    io.opentelemetry.api.trace.SpanContext spanContext = 
                            io.opentelemetry.api.trace.SpanContext.createFromRemoteParent(
                                    networkMessage.getTraceId(),
                                    networkMessage.getSpanId(),
                                    networkMessage.getTraceFlags(),
                                    io.opentelemetry.api.trace.TraceState.getDefault());
                    
                    parentContext = Context.current().with(Span.wrap(spanContext));
                }
                
                io.opentelemetry.api.trace.SpanBuilder spanBuilder = tracer.spanBuilder("network.message.write")
                        .setSpanKind(SpanKind.CLIENT);
                
                if (parentContext != null) {
                    spanBuilder.setParent(parentContext);
                }
                
                span = spanBuilder.startSpan();
                scope = span.makeCurrent();
                
                // Add attributes to span
                span.setAttribute("messaging.message.id", networkMessage.getCorrelationId());
                if (ctx.channel() instanceof DatagramChannel) {
                    span.setAttribute("network.transport", "udp");
                } else {
                    span.setAttribute("network.transport", "tcp");
                }
            }
            
            // Process the message
            if (msg instanceof NetworkMessage message) {
                if (ctx.channel() instanceof DatagramChannel) {
                    InetSocketAddress recipient = (InetSocketAddress) message.getRemoteAddress();
                    InetSocketAddress sender = (InetSocketAddress) ctx.channel().localAddress();
                    
                    if (span != null) {
                        span.setAttribute("network.peer.address", recipient.getAddress().getHostAddress());
                        span.setAttribute("network.peer.port", recipient.getPort());
                    }
                    
                    ctx.write(new DatagramPacket((ByteBuf) message.getMessage(), recipient, sender), promise);
                } else {
                    if (span != null && ctx.channel().remoteAddress() instanceof InetSocketAddress) {
                        InetSocketAddress address = (InetSocketAddress) ctx.channel().remoteAddress();
                        span.setAttribute("network.peer.address", address.getAddress().getHostAddress());
                        span.setAttribute("network.peer.port", address.getPort());
                    }
                    
                    ctx.write(message.getMessage(), promise);
                }
            } else {
                ctx.write(msg, promise);
                
                if (span != null) {
                    span.setAttribute("message.type", "unknown");
                }
            }
            
            // Record metrics
            if (timerSample != null && meterRegistry != null) {
                timerSample.stop(meterRegistry.timer("network.message.write", 
                        "transport", ctx.channel() instanceof DatagramChannel ? "udp" : "tcp"));
            }
            
            // Complete span successfully
            if (span != null) {
                span.setStatus(StatusCode.OK);
            }
            
        } catch (Exception e) {
            LOGGER.error("Error writing network message", e);
            
            // Record error in span
            if (span != null) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
            }
            
            // Continue with pipeline
            ctx.write(msg, promise);
        } finally {
            // Close tracing scope and span
            if (scope != null) {
                scope.close();
            }
            if (span != null) {
                span.end();
            }
        }
    }
}