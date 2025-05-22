/*
 * Copyright 2019 - 2021 Anton Tananaev (anton@traccar.org)
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
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.traccar.TrackerConnector;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.messaging.MessageBroker;

import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Channel handler that manages device connections with distributed tracing, metrics collection,
 * service discovery integration, and message broker integration for channel state sharing.
 */
public class OpenChannelHandler extends ChannelDuplexHandler {

    private final TrackerConnector connector;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final ServiceDiscovery serviceDiscovery;
    private final MessageBroker messageBroker;
    private final AtomicInteger activeChannelsCount = new AtomicInteger(0);
    private final Counter channelActiveCounter;
    private final Counter channelInactiveCounter;
    private final String serviceName;
    private final String serviceId;
    private volatile boolean shuttingDown = false;
    
    /**
     * Creates a new OpenChannelHandler with OpenTelemetry, metrics, service discovery and message broker integration.
     *
     * @param connector The tracker connector instance
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     * @param serviceDiscovery Service discovery client for dynamic service location
     * @param messageBroker Message broker for channel state sharing
     * @param serviceName Name of this service for registration
     */
    @Inject
    public OpenChannelHandler(
            TrackerConnector connector,
            Tracer tracer,
            MeterRegistry meterRegistry,
            ServiceDiscovery serviceDiscovery,
            MessageBroker messageBroker,
            @Named("serviceName") String serviceName) {
        this.connector = connector;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.serviceDiscovery = serviceDiscovery;
        this.messageBroker = messageBroker;
        this.serviceName = serviceName;
        this.serviceId = serviceName + "-" + System.currentTimeMillis();
        
        // Initialize metrics
        this.channelActiveCounter = Counter.builder("traccar.channel.active")
                .description("Number of channel activations")
                .tag("service", serviceName)
                .register(meterRegistry);
        
        this.channelInactiveCounter = Counter.builder("traccar.channel.inactive")
                .description("Number of channel deactivations")
                .tag("service", serviceName)
                .register(meterRegistry);
        
        // Register gauge for active channels count
        Gauge.builder("traccar.channel.active.count", activeChannelsCount::get)
                .description("Current number of active channels")
                .tag("service", serviceName)
                .register(meterRegistry);
        
        // Register with service discovery
        registerService();
        
        // Register shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }
    
    /**
     * Registers this service instance with the service discovery system.
     */
    private void registerService() {
        try {
            serviceDiscovery.register(serviceName, serviceId, "protocol-service");
        } catch (Exception e) {
            // Log but don't fail startup
            System.err.println("Failed to register with service discovery: " + e.getMessage());
        }
    }
    
    /**
     * Performs graceful shutdown operations.
     */
    public void shutdown() {
        shuttingDown = true;
        try {
            // Deregister from service discovery
            serviceDiscovery.deregister(serviceId);
            
            // Allow time for existing operations to complete
            CompletableFuture.delayedExecutor(2, TimeUnit.SECONDS).execute(() -> {
                // Publish final shutdown state to message broker
                try {
                    messageBroker.publish("service.state", 
                            String.format("{\"service\":\"%s\",\"id\":\"%s\",\"state\":\"shutdown\",\"channels\":0}", 
                                    serviceName, serviceId));
                } catch (Exception e) {
                    System.err.println("Error publishing shutdown state: " + e.getMessage());
                }
            });
        } catch (Exception e) {
            System.err.println("Error during shutdown: " + e.getMessage());
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // Create a span for channel activation
        Span span = tracer.spanBuilder("channel.active")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("channel.id", ctx.channel().id().asShortText())
                .setAttribute("remote.address", ctx.channel().remoteAddress().toString())
                .setAttribute("service.name", serviceName)
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            // Call parent implementation
            super.channelActive(ctx);
            
            // Add channel to the group
            connector.getChannelGroup().add(ctx.channel());
            
            // Update metrics
            channelActiveCounter.increment();
            int currentCount = activeChannelsCount.incrementAndGet();
            
            // Publish channel state to message broker
            publishChannelState(ctx.channel(), "active", currentCount);
            
            span.addEvent("Channel added to group");
        } finally {
            span.end();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // Create a span for channel deactivation
        Span span = tracer.spanBuilder("channel.inactive")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("channel.id", ctx.channel().id().asShortText())
                .setAttribute("remote.address", 
                        ctx.channel().remoteAddress() != null ? 
                        ctx.channel().remoteAddress().toString() : "unknown")
                .setAttribute("service.name", serviceName)
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            // Call parent implementation
            super.channelInactive(ctx);
            
            // Remove channel from the group
            connector.getChannelGroup().remove(ctx.channel());
            
            // Update metrics
            channelInactiveCounter.increment();
            int currentCount = activeChannelsCount.decrementAndGet();
            
            // Publish channel state to message broker
            publishChannelState(ctx.channel(), "inactive", currentCount);
            
            span.addEvent("Channel removed from group");
        } finally {
            span.end();
        }
    }
    
    /**
     * Publishes channel state changes to the message broker.
     *
     * @param channel The channel whose state changed
     * @param state The new state ("active" or "inactive")
     * @param currentCount Current count of active channels
     */
    private void publishChannelState(Channel channel, String state, int currentCount) {
        if (shuttingDown) {
            return; // Don't publish during shutdown
        }
        
        try {
            String message = String.format(
                    "{\"service\":\"%s\",\"id\":\"%s\",\"channelId\":\"%s\",\"state\":\"%s\",\"activeCount\":%d}",
                    serviceName,
                    serviceId,
                    channel.id().asShortText(),
                    state,
                    currentCount);
            
            messageBroker.publish("channel.state", message);
        } catch (Exception e) {
            // Log but don't fail channel operations
            System.err.println("Failed to publish channel state: " + e.getMessage());
        }
    }
    
    /**
     * Returns the current count of active channels for health checks.
     *
     * @return Current count of active channels
     */
    public int getActiveChannelsCount() {
        return activeChannelsCount.get();
    }
    
    /**
     * Checks if the handler is healthy based on connection state.
     *
     * @return true if the handler is in a healthy state
     */
    public boolean isHealthy() {
        // Consider healthy if not shutting down and service discovery is available
        return !shuttingDown && serviceDiscovery.isAvailable();
    }

}