/*
 * Copyright 2012 - 2022 Anton Tananaev (anton@traccar.org)
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
package org.traccar;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.health.HealthCheckManager;

import javax.inject.Inject;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public abstract class TrackerServer implements TrackerConnector {

    private static final Logger LOGGER = Logger.getLogger(TrackerServer.class.getName());

    private final boolean datagram;
    private final boolean secure;

    @SuppressWarnings("rawtypes")
    private final AbstractBootstrap bootstrap;

    private final int port;
    private final String address;
    private final String protocol;

    private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final HealthCheckManager healthCheckManager;
    private final Tracer tracer;
    private final Meter meter;
    
    private LongUpDownCounter activeConnectionsCounter;
    private LongCounter totalConnectionsCounter;
    private LongCounter messagesReceivedCounter;
    private LongCounter messagesBytesCounter;
    private LongCounter errorCounter;
    
    private Channel channel;
    private String serviceId;

    @Override
    public boolean isDatagram() {
        return datagram;
    }

    @Override
    public boolean isSecure() {
        return secure;
    }

    @Inject
    public TrackerServer(
            Config config, 
            String protocol, 
            boolean datagram, 
            ServiceDiscoveryManager serviceDiscoveryManager,
            HealthCheckManager healthCheckManager,
            Tracer tracer,
            Meter meter) {
        this.protocol = protocol;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.healthCheckManager = healthCheckManager;
        this.tracer = tracer;
        this.meter = meter;
        
        secure = config.getBoolean(Keys.PROTOCOL_SSL.withPrefix(protocol));
        address = config.getString(Keys.PROTOCOL_ADDRESS.withPrefix(protocol));
        port = config.getInteger(Keys.PROTOCOL_PORT.withPrefix(protocol));

        initializeMetrics();
        
        BasePipelineFactory pipelineFactory = new BasePipelineFactory(this, config, protocol) {
            @Override
            protected void addTransportHandlers(PipelineBuilder pipeline) {
                try {
                    if (isSecure()) {
                        SSLEngine engine = SSLContext.getDefault().createSSLEngine();
                        pipeline.addLast(new SslHandler(engine));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline) {
                TrackerServer.this.addProtocolHandlers(pipeline, config);
            }
        };

        this.datagram = datagram;
        if (datagram) {
            bootstrap = new Bootstrap()
                    .group(EventLoopGroupFactory.getWorkerGroup())
                    .channel(NioDatagramChannel.class)
                    .handler(pipelineFactory);
        } else {
            bootstrap = new ServerBootstrap()
                    .group(EventLoopGroupFactory.getBossGroup(), EventLoopGroupFactory.getWorkerGroup())
                    .channel(NioServerSocketChannel.class)
                    .childHandler(pipelineFactory);
        }
        
        // Register health check for this server
        healthCheckManager.register("server-" + protocol, this::checkHealth);
    }
    
    private void initializeMetrics() {
        // Initialize metrics counters
        activeConnectionsCounter = meter
                .upDownCounterBuilder("traccar.connections.active")
                .setDescription("Number of active connections")
                .setUnit("connections")
                .build();
        
        totalConnectionsCounter = meter
                .counterBuilder("traccar.connections.total")
                .setDescription("Total number of connections established")
                .setUnit("connections")
                .build();
        
        messagesReceivedCounter = meter
                .counterBuilder("traccar.messages.received")
                .setDescription("Number of messages received")
                .setUnit("messages")
                .build();
        
        messagesBytesCounter = meter
                .counterBuilder("traccar.messages.bytes")
                .setDescription("Total bytes of messages received")
                .setUnit("bytes")
                .build();
        
        errorCounter = meter
                .counterBuilder("traccar.errors")
                .setDescription("Number of errors encountered")
                .setUnit("errors")
                .build();
    }
    
    /**
     * Health check method for this server
     * @return true if server is healthy, false otherwise
     */
    private boolean checkHealth() {
        boolean isChannelActive = channel != null && channel.isActive();
        boolean hasAcceptableConnectionCount = true;
        
        // Check if we have too many connections (could indicate a problem)
        // This is a simple example - in production you might want to make this configurable
        if (channelGroup.size() > 10000) {
            hasAcceptableConnectionCount = false;
        }
        
        return isChannelActive && hasAcceptableConnectionCount;
    }

    protected abstract void addProtocolHandlers(PipelineBuilder pipeline, Config config);

    public int getPort() {
        return port;
    }

    public String getAddress() {
        return address;
    }
    
    public String getProtocol() {
        return protocol;
    }

    @Override
    public ChannelGroup getChannelGroup() {
        return channelGroup;
    }
    
    /**
     * Increment the active connections counter and create a connection span
     * 
     * @param remoteAddress The remote address of the connection
     * @return The created span for this connection
     */
    public Span incrementActiveConnections(String remoteAddress) {
        activeConnectionsCounter.add(1);
        totalConnectionsCounter.add(1);
        
        // Create a span for connection tracking
        Span span = tracer.spanBuilder("connection." + protocol)
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("protocol", protocol)
                .setAttribute("remote.address", remoteAddress)
                .setAttribute("server.port", port)
                .startSpan();
        
        return span;
    }
    
    /**
     * Decrement the active connections counter and end the connection span
     * 
     * @param span The span to end
     * @param reason The reason for disconnection
     */
    public void decrementActiveConnections(Span span, String reason) {
        activeConnectionsCounter.add(-1);
        
        // End the connection span
        if (span != null) {
            span.setAttribute("connection.close.reason", reason);
            span.end();
        }
    }
    
    /**
     * Increment the messages received counter
     * 
     * @param bytesReceived The number of bytes received
     * @param connectionSpan The parent connection span
     * @return A new span for this message
     */
    public Span incrementMessagesReceived(int bytesReceived, Span connectionSpan) {
        messagesReceivedCounter.add(1);
        messagesBytesCounter.add(bytesReceived);
        
        // Create a child span for message processing
        Span messageSpan = tracer.spanBuilder("message." + protocol)
                .setParent(Context.current().with(connectionSpan))
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("protocol", protocol)
                .setAttribute("message.size", bytesReceived)
                .startSpan();
        
        return messageSpan;
    }
    
    /**
     * Record an error in metrics and tracing
     * 
     * @param span The span to record the error on
     * @param error The error that occurred
     */
    public void recordError(Span span, Throwable error) {
        errorCounter.add(1);
        
        if (span != null) {
            span.recordException(error);
            span.setStatus(StatusCode.ERROR, error.getMessage());
        }
    }

    @Override
    public void start() throws Exception {
        // Create a span for server startup
        Span startupSpan = tracer.spanBuilder("server.start." + protocol)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("protocol", protocol)
                .setAttribute("port", port)
                .setAttribute("transport", datagram ? "udp" : "tcp")
                .setAttribute("secure", secure)
                .startSpan();
        
        try (Scope scope = startupSpan.makeCurrent()) {
            InetSocketAddress endpoint;
            if (address == null) {
                endpoint = new InetSocketAddress(port);
            } else {
                endpoint = new InetSocketAddress(address, port);
            }

            channel = bootstrap.bind(endpoint).syncUninterruptibly().channel();
            if (channel != null) {
                getChannelGroup().add(channel);
                
                // Register with service discovery
                serviceId = serviceDiscoveryManager.register(
                        protocol, 
                        address != null ? address : "0.0.0.0", 
                        port, 
                        datagram ? "udp" : "tcp",
                        secure);
                
                startupSpan.setAttribute("service.id", serviceId);
                LOGGER.info("Started " + protocol + " server on " + endpoint + ", registered as " + serviceId);
                
                // Add detailed health check information
                healthCheckManager.addDetails("server-" + protocol, "endpoint", endpoint.toString());
                healthCheckManager.addDetails("server-" + protocol, "serviceId", serviceId);
                healthCheckManager.addDetails("server-" + protocol, "secure", String.valueOf(secure));
            }
            startupSpan.setStatus(StatusCode.OK);
        } catch (Exception e) {
            startupSpan.recordException(e);
            startupSpan.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            startupSpan.end();
        }
    }

    @Override
    public void stop() {
        // Create a span for server shutdown
        Span shutdownSpan = tracer.spanBuilder("server.stop." + protocol)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("protocol", protocol)
                .setAttribute("port", port)
                .setAttribute("service.id", serviceId != null ? serviceId : "unknown")
                .startSpan();
        
        try (Scope scope = shutdownSpan.makeCurrent()) {
            // Deregister from service discovery
            if (serviceId != null) {
                serviceDiscoveryManager.deregister(serviceId);
                LOGGER.info("Deregistered " + protocol + " server with ID " + serviceId);
            }
            
            // Deregister health check
            healthCheckManager.deregister("server-" + protocol);
            
            // Record metrics before shutdown
            int activeConnections = channelGroup.size();
            shutdownSpan.setAttribute("connections.active", activeConnections);
            
            // Implement graceful shutdown
            // First, stop accepting new connections
            if (channel != null) {
                channel.close().syncUninterruptibly();
                shutdownSpan.addEvent("stopped.accepting.connections");
            }
            
            // Then wait for existing connections to complete (with timeout)
            if (!channelGroup.isEmpty()) {
                LOGGER.info("Shutting down " + protocol + " server, waiting for " + activeConnections + " connections to close");
                shutdownSpan.addEvent("waiting.for.connections.to.close", 
                        Attributes.of(AttributeKey.longKey("connections.count"), (long) activeConnections));
                
                boolean allClosed = channelGroup.close().await(30, TimeUnit.SECONDS);
                shutdownSpan.setAttribute("shutdown.complete", allClosed);
                if (!allClosed) {
                    LOGGER.warning("Not all connections closed gracefully for " + protocol + " server");
                    shutdownSpan.addEvent("forced.connection.close");
                }
            }
            
            LOGGER.info("Stopped " + protocol + " server");
            shutdownSpan.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.warning("Error stopping " + protocol + " server: " + e.getMessage());
            shutdownSpan.recordException(e);
            shutdownSpan.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            shutdownSpan.end();
        }
    }
}