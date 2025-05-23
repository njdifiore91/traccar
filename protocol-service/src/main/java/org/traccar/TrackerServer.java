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
package org.traccar;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.discovery.ServiceInstance;
import org.traccar.discovery.ServiceRegistry;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracker server that handles device connections using Netty.
 * Optimized for containerized deployment with Kubernetes.
 */
public class TrackerServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TrackerServer.class);

    private final String protocol;
    private final String host;
    private final int port;
    private final boolean secure;
    private final String serviceId;

    private final MeterRegistry meterRegistry;
    private final ServiceRegistry serviceRegistry;
    private final PipelineFactory pipelineFactory;

    private Channel channel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final Counter connectionAcceptedCounter;
    private final Counter connectionRejectedCounter;
    private final Counter connectionClosedCounter;
    private final Timer connectionDurationTimer;

    private ServiceInstance serviceInstance;

    /**
     * Constructs a new TrackerServer with the specified parameters.
     *
     * @param protocol The protocol name
     * @param host The host to bind to
     * @param port The port to bind to
     * @param secure Whether to use SSL/TLS
     * @param pipelineFactory The pipeline factory for creating channel pipelines
     * @param meterRegistry The meter registry for metrics collection
     * @param serviceRegistry The service registry for service discovery
     */
    @Inject
    public TrackerServer(
            String protocol,
            String host,
            int port,
            boolean secure,
            PipelineFactory pipelineFactory,
            MeterRegistry meterRegistry,
            ServiceRegistry serviceRegistry) {

        this.protocol = protocol;
        this.host = host;
        this.port = port;
        this.secure = secure;
        this.pipelineFactory = pipelineFactory;
        this.meterRegistry = meterRegistry;
        this.serviceRegistry = serviceRegistry;
        this.serviceId = "protocol-" + protocol + "-" + (secure ? "secure" : "standard");

        // Initialize metrics
        Gauge.builder("tracker_server_active_connections", activeConnections, AtomicInteger::get)
                .description("Number of active connections")
                .tag("protocol", protocol)
                .tag("secure", String.valueOf(secure))
                .register(meterRegistry);

        connectionAcceptedCounter = Counter.builder("tracker_server_connections_accepted_total")
                .description("Total number of accepted connections")
                .tag("protocol", protocol)
                .tag("secure", String.valueOf(secure))
                .register(meterRegistry);

        connectionRejectedCounter = Counter.builder("tracker_server_connections_rejected_total")
                .description("Total number of rejected connections")
                .tag("protocol", protocol)
                .tag("secure", String.valueOf(secure))
                .register(meterRegistry);

        connectionClosedCounter = Counter.builder("tracker_server_connections_closed_total")
                .description("Total number of closed connections")
                .tag("protocol", protocol)
                .tag("secure", String.valueOf(secure))
                .register(meterRegistry);

        connectionDurationTimer = Timer.builder("tracker_server_connection_duration")
                .description("Duration of connections")
                .tag("protocol", protocol)
                .tag("secure", String.valueOf(secure))
                .register(meterRegistry);
    }

    /**
     * Starts the server and registers it with the service registry.
     *
     * @return A ChannelFuture that will be notified when the server is bound
     */
    public ChannelFuture start() {
        LOGGER.info("Starting {} server on {}:{}", protocol, host, port);

        // Configure thread pools with container-aware sizing
        int bossThreads = Integer.getInteger("traccar.server.bossThreads", 1);
        int workerThreads = Integer.getInteger("traccar.server.workerThreads", 0); // 0 means use Netty default

        bossGroup = new NioEventLoopGroup(bossThreads);
        workerGroup = new NioEventLoopGroup(workerThreads);

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(pipelineFactory.create(this))
                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true);

        // Bind to the specified host and port
        ChannelFuture future = bootstrap.bind(new InetSocketAddress(host, port));
        channel = future.channel();

        // Register with service discovery after successful binding
        future.addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                if (future.isSuccess()) {
                    registerWithServiceDiscovery();
                    LOGGER.info("{} server started successfully on {}:{}", protocol, host, port);
                } else {
                    LOGGER.error("Failed to start {} server on {}:{}", protocol, host, port, future.cause());
                }
            }
        });

        return future;
    }

    /**
     * Registers the server with the service registry.
     */
    private void registerWithServiceDiscovery() {
        // Create service instance with health check endpoint
        serviceInstance = ServiceInstance.builder()
                .id(serviceId)
                .name("protocol-service")
                .host(getActualHost())
                .port(port)
                .metadata("protocol", protocol)
                .metadata("secure", String.valueOf(secure))
                .healthCheckEndpoint("/health/liveness")
                .build();

        // Register with service registry
        serviceRegistry.register(serviceInstance);
        LOGGER.info("Registered {} server with service registry as {}", protocol, serviceId);
    }

    /**
     * Gets the actual host to register with service discovery.
     * In Kubernetes, this should be the pod IP or service name.
     *
     * @return The host to register
     */
    private String getActualHost() {
        // In Kubernetes, use pod IP from environment variable if available
        String podIp = System.getenv("POD_IP");
        if (podIp != null && !podIp.isEmpty()) {
            return podIp;
        }

        // Fall back to configured host
        return host;
    }

    /**
     * Stops the server and deregisters it from the service registry.
     *
     * @return A Future that will be notified when the server is stopped
     */
    public Future<?> stop() {
        LOGGER.info("Stopping {} server", protocol);

        // Deregister from service registry
        if (serviceInstance != null) {
            serviceRegistry.deregister(serviceInstance);
            LOGGER.info("Deregistered {} server from service registry", protocol);
        }

        // Implement graceful shutdown for Kubernetes
        // First stop accepting new connections
        Future<?> channelCloseFuture = null;
        if (channel != null) {
            channelCloseFuture = channel.close();
        }

        // Allow time for existing connections to complete
        // This is important for graceful shutdown in Kubernetes
        final int gracePeriodSeconds = Integer.getInteger("traccar.server.gracePeriodSeconds", 30);
        LOGGER.info("Allowing {} seconds for connections to complete", gracePeriodSeconds);

        // Schedule the final shutdown after grace period
        final Future<?> finalChannelCloseFuture = channelCloseFuture;
        return workerGroup.schedule(() -> {
            Future<?> shutdownFuture = shutdownEventLoopGroups();
            
            // If we have a channel close future, chain it
            if (finalChannelCloseFuture != null) {
                finalChannelCloseFuture.addListener(f -> LOGGER.info("Channel for {} server closed", protocol));
            }
            
            return shutdownFuture;
        }, gracePeriodSeconds, TimeUnit.SECONDS);
    }

    /**
     * Shuts down the event loop groups.
     *
     * @return A Future that will be notified when the event loop groups are shut down
     */
    private Future<?> shutdownEventLoopGroups() {
        Future<?> bossFuture = null;
        if (bossGroup != null) {
            bossFuture = bossGroup.shutdownGracefully();
            bossGroup = null;
        }

        Future<?> workerFuture = null;
        if (workerGroup != null) {
            workerFuture = workerGroup.shutdownGracefully();
            workerGroup = null;
        }

        if (bossFuture != null && workerFuture != null) {
            return bossFuture.addListener(f -> {
                workerFuture.addListener(f2 -> {
                    LOGGER.info("{} server stopped", protocol);
                });
            });
        } else if (bossFuture != null) {
            return bossFuture.addListener(f -> LOGGER.info("{} server stopped", protocol));
        } else if (workerFuture != null) {
            return workerFuture.addListener(f -> LOGGER.info("{} server stopped", protocol));
        } else {
            LOGGER.info("{} server stopped", protocol);
            return null;
        }
    }

    /**
     * Gets the protocol name.
     *
     * @return The protocol name
     */
    public String getProtocol() {
        return protocol;
    }

    /**
     * Gets whether the server uses SSL/TLS.
     *
     * @return Whether the server uses SSL/TLS
     */
    public boolean isSecure() {
        return secure;
    }

    /**
     * Called when a connection is accepted.
     */
    public void connectionAccepted() {
        activeConnections.incrementAndGet();
        connectionAcceptedCounter.increment();
    }

    /**
     * Called when a connection is rejected.
     */
    public void connectionRejected() {
        connectionRejectedCounter.increment();
    }

    /**
     * Called when a connection is closed.
     *
     * @param durationMillis The duration of the connection in milliseconds
     */
    public void connectionClosed(long durationMillis) {
        activeConnections.decrementAndGet();
        connectionClosedCounter.increment();
        connectionDurationTimer.record(durationMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Gets the number of active connections.
     *
     * @return The number of active connections
     */
    public int getActiveConnections() {
        return activeConnections.get();
    }
}