/*
 * Copyright 2012 - 2024 Anton Tananaev (anton@traccar.org)
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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.DatagramChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.timeout.IdleStateEvent;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.BasePipelineFactory;
import org.traccar.BaseProtocolDecoder;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.helper.NetworkUtil;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHeaders;
import org.traccar.messaging.MessageProducer;
import org.traccar.session.ConnectionManager;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main event handler for network connections.
 * Handles connection lifecycle events and integrates with messaging, tracing, and metrics.
 */
@Singleton
@ChannelHandler.Sharable
public class MainEventHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MainEventHandler.class);
    private static final String SPAN_NAME_PREFIX = "network.connection";
    private static final String CONNECTION_TOPIC = "connection-events";

    private final ConnectionManager connectionManager;
    private final Set<String> connectionlessProtocols = new HashSet<>();
    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final ServiceDiscovery serviceDiscovery;
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private final ConcurrentHashMap<Channel, Span> activeSpans = new ConcurrentHashMap<>();

    // Metrics
    private final Counter connectionsCounter;
    private final Counter disconnectionsCounter;
    private final Counter errorsCounter;
    private final Counter timeoutsCounter;

    /**
     * Constructs a new MainEventHandler with required dependencies.
     *
     * @param config Configuration for the handler
     * @param connectionManager Manager for device connections
     * @param messageProducer Producer for sending messages to the message broker
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Registry for metrics collection
     * @param serviceDiscovery Service discovery for locating other services
     */
    @Inject
    public MainEventHandler(
            Config config, 
            ConnectionManager connectionManager,
            MessageProducer messageProducer,
            Tracer tracer,
            MeterRegistry meterRegistry,
            ServiceDiscovery serviceDiscovery) {
        this.connectionManager = connectionManager;
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.serviceDiscovery = serviceDiscovery;
        
        String connectionlessProtocolList = config.getString(Keys.STATUS_IGNORE_OFFLINE);
        if (connectionlessProtocolList != null) {
            connectionlessProtocols.addAll(Arrays.asList(connectionlessProtocolList.split("[, ]")));
        }
        
        // Initialize metrics
        Tags commonTags = Tags.of("component", "network");
        this.connectionsCounter = Counter.builder("traccar.connections.total")
                .description("Total number of connections")
                .tags(commonTags)
                .register(meterRegistry);
        
        this.disconnectionsCounter = Counter.builder("traccar.disconnections.total")
                .description("Total number of disconnections")
                .tags(commonTags)
                .register(meterRegistry);
        
        this.errorsCounter = Counter.builder("traccar.connection.errors.total")
                .description("Total number of connection errors")
                .tags(commonTags)
                .register(meterRegistry);
        
        this.timeoutsCounter = Counter.builder("traccar.connection.timeouts.total")
                .description("Total number of connection timeouts")
                .tags(commonTags)
                .register(meterRegistry);
        
        // Register gauge for active connections
        Gauge.builder("traccar.connections.active", activeConnections::get)
                .description("Number of active connections")
                .tags(commonTags)
                .register(meterRegistry);
        
        // Register shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::handleGracefulShutdown));
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (isShuttingDown.get()) {
            // Don't accept new connections during shutdown
            ctx.close();
            return;
        }
        
        if (!(ctx.channel() instanceof DatagramChannel)) {
            String sessionId = NetworkUtil.session(ctx.channel());
            LOGGER.info("[{}] connected", sessionId);
            
            // Increment active connections counter
            activeConnections.incrementAndGet();
            connectionsCounter.increment();
            
            // Create and start a span for this connection
            Span span = tracer.spanBuilder(SPAN_NAME_PREFIX + ".active")
                    .setSpanKind(SpanKind.SERVER)
                    .setAttribute("session.id", sessionId)
                    .setAttribute("remote.address", ctx.channel().remoteAddress().toString())
                    .startSpan();
            
            // Store the span for later use
            activeSpans.put(ctx.channel(), span);
            
            // Publish connection event to message broker
            publishConnectionEvent(ctx.channel(), "connected");
        }
        
        // Call the parent implementation
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        String sessionId = NetworkUtil.session(ctx.channel());
        LOGGER.info("[{}] disconnected", sessionId);
        
        // Complete the span for this connection if it exists
        Span span = activeSpans.remove(ctx.channel());
        if (span != null) {
            span.setAttribute("session.duration_ms", System.currentTimeMillis() - span.getStartEpochNanos() / 1_000_000);
            span.setStatus(StatusCode.OK);
            span.end();
        }
        
        // Decrement active connections counter if not a datagram channel
        if (!(ctx.channel() instanceof DatagramChannel)) {
            activeConnections.decrementAndGet();
            disconnectionsCounter.increment();
            
            // Publish disconnection event to message broker
            publishConnectionEvent(ctx.channel(), "disconnected");
        }
        
        closeChannel(ctx.channel());

        boolean supportsOffline = BasePipelineFactory.getHandler(ctx.pipeline(), HttpRequestDecoder.class) == null
                && !connectionlessProtocols.contains(ctx.pipeline().get(BaseProtocolDecoder.class).getProtocolName());
        connectionManager.deviceDisconnected(ctx.channel(), supportsOffline);
        
        // Call the parent implementation
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        
        String sessionId = NetworkUtil.session(ctx.channel());
        LOGGER.warn("[{}] error", sessionId, cause);
        
        // Record error in metrics
        errorsCounter.increment();
        
        // Complete the span for this connection with error status
        Span span = activeSpans.remove(ctx.channel());
        if (span != null) {
            span.recordException(cause);
            span.setStatus(StatusCode.ERROR, cause.getMessage());
            span.end();
        }
        
        // Publish error event to message broker
        publishConnectionEvent(ctx.channel(), "error", cause.getMessage());
        
        closeChannel(ctx.channel());
        
        // Call the parent implementation
        ctx.fireExceptionCaught(cause);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            String sessionId = NetworkUtil.session(ctx.channel());
            LOGGER.info("[{}] timed out", sessionId);
            
            // Record timeout in metrics
            timeoutsCounter.increment();
            
            // Complete the span for this connection with timeout status
            Span span = activeSpans.remove(ctx.channel());
            if (span != null) {
                span.setAttribute("timeout", true);
                span.setStatus(StatusCode.ERROR, "Connection timed out");
                span.end();
            }
            
            // Publish timeout event to message broker
            publishConnectionEvent(ctx.channel(), "timeout");
            
            closeChannel(ctx.channel());
        }
        
        // Call the parent implementation
        ctx.fireUserEventTriggered(evt);
    }

    private void closeChannel(Channel channel) {
        if (!(channel instanceof DatagramChannel)) {
            channel.close();
        }
    }
    
    /**
     * Publishes a connection event to the message broker.
     *
     * @param channel The channel associated with the event
     * @param eventType The type of event (connected, disconnected, error, timeout)
     */
    private void publishConnectionEvent(Channel channel, String eventType) {
        publishConnectionEvent(channel, eventType, null);
    }
    
    /**
     * Publishes a connection event to the message broker with additional details.
     *
     * @param channel The channel associated with the event
     * @param eventType The type of event (connected, disconnected, error, timeout)
     * @param details Additional details about the event (optional)
     */
    private void publishConnectionEvent(Channel channel, String eventType, String details) {
        try {
            String sessionId = NetworkUtil.session(channel);
            
            // Create message headers with trace context for distributed tracing
            MessageHeaders headers = new MessageHeaders();
            headers.put("session.id", sessionId);
            headers.put("event.type", eventType);
            headers.put("remote.address", channel.remoteAddress().toString());
            
            if (details != null) {
                headers.put("event.details", details);
            }
            
            // Create message payload
            ConnectionEvent event = new ConnectionEvent(
                    sessionId,
                    eventType,
                    channel.remoteAddress().toString(),
                    System.currentTimeMillis(),
                    details
            );
            
            // Create message envelope and publish to broker
            MessageEnvelope<ConnectionEvent> envelope = new MessageEnvelope<>(event, headers);
            messageProducer.send(CONNECTION_TOPIC, envelope);
        } catch (Exception e) {
            LOGGER.warn("Failed to publish connection event", e);
        }
    }
    
    /**
     * Handles graceful shutdown of the handler.
     * This method is called when the JVM is shutting down.
     */
    private void handleGracefulShutdown() {
        LOGGER.info("Initiating graceful shutdown of network connections");
        isShuttingDown.set(true);
        
        // Create a span for the shutdown process
        Span shutdownSpan = tracer.spanBuilder(SPAN_NAME_PREFIX + ".shutdown")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try {
            // Allow time for active connections to complete their work
            LOGGER.info("Waiting for active connections to complete ({} active)", activeConnections.get());
            
            // Wait for active connections to finish (with a timeout)
            long startTime = System.currentTimeMillis();
            long timeout = 30000; // 30 seconds timeout
            
            while (activeConnections.get() > 0 && (System.currentTimeMillis() - startTime) < timeout) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            
            // Complete all remaining spans
            for (Span span : activeSpans.values()) {
                span.setStatus(StatusCode.ERROR, "Connection terminated during shutdown");
                span.end();
            }
            activeSpans.clear();
            
            LOGGER.info("Network connections shutdown completed, {} connections were active", 
                    activeConnections.get());
            
            shutdownSpan.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.error("Error during graceful shutdown", e);
            shutdownSpan.recordException(e);
            shutdownSpan.setStatus(StatusCode.ERROR, "Error during shutdown: " + e.getMessage());
        } finally {
            shutdownSpan.end();
        }
    }
    
    /**
     * Data class representing a connection event for publishing to the message broker.
     */
    private static class ConnectionEvent {
        private final String sessionId;
        private final String eventType;
        private final String remoteAddress;
        private final long timestamp;
        private final String details;
        
        public ConnectionEvent(String sessionId, String eventType, String remoteAddress, 
                              long timestamp, String details) {
            this.sessionId = sessionId;
            this.eventType = eventType;
            this.remoteAddress = remoteAddress;
            this.timestamp = timestamp;
            this.details = details;
        }
        
        public String getSessionId() {
            return sessionId;
        }
        
        public String getEventType() {
            return eventType;
        }
        
        public String getRemoteAddress() {
            return remoteAddress;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        public String getDetails() {
            return details;
        }
    }
}