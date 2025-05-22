/*
 * Copyright 2022 - 2025 Anton Tananaev (anton@traccar.org)
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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.resilience4j.circuitbreaker.CircuitBreaker;
import io.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;

import javax.inject.Inject;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Abstract client for connecting to remote servers.
 * Implements service discovery, circuit breaker, distributed tracing, and metrics collection.
 */
public abstract class TrackerClient implements TrackerConnector {

    private final boolean secure;
    private final long interval;

    private final Bootstrap bootstrap;

    private final String serviceName;
    private final String[] devices;

    private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    
    // Service discovery components
    private final ServiceDiscovery serviceDiscovery;
    private final AtomicReference<List<ServiceInstance>> serviceInstances = new AtomicReference<>();
    private final Map<String, InetSocketAddress> endpointCache = new ConcurrentHashMap<>();
    
    // Circuit breaker components
    private final CircuitBreaker circuitBreaker;
    
    // Distributed tracing components
    private final Tracer tracer;
    private final TextMapPropagator propagator;
    
    // Metrics collection components
    private final Counter connectionAttempts;
    private final Counter connectionSuccesses;
    private final Counter connectionFailures;
    private final Timer connectionDuration;

    @Override
    public boolean isDatagram() {
        return false;
    }

    @Override
    public boolean isSecure() {
        return secure;
    }

    /**
     * Creates a new TrackerClient with the specified configuration.
     *
     * @param config The configuration for the client
     * @param protocol The protocol name
     * @param serviceDiscovery The service discovery component
     * @param tracer The OpenTelemetry tracer
     * @param propagator The OpenTelemetry context propagator
     * @param meterRegistry The metrics registry
     */
    @Inject
    public TrackerClient(Config config, String protocol, 
                        ServiceDiscovery serviceDiscovery,
                        Tracer tracer,
                        TextMapPropagator propagator,
                        MeterRegistry meterRegistry) {
        this.secure = config.getBoolean(Keys.PROTOCOL_SSL.withPrefix(protocol));
        this.interval = config.getLong(Keys.PROTOCOL_INTERVAL.withPrefix(protocol));
        this.serviceName = config.getString(Keys.PROTOCOL_SERVICE.withPrefix(protocol), protocol);
        this.devices = config.getString(Keys.PROTOCOL_DEVICES.withPrefix(protocol)).split("[, ]");
        
        // Initialize service discovery
        this.serviceDiscovery = serviceDiscovery;
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        this.circuitBreaker = CircuitBreakerRegistry.of(circuitBreakerConfig)
                .circuitBreaker(protocol + "-client");
        
        // Initialize distributed tracing
        this.tracer = tracer;
        this.propagator = propagator;
        
        // Initialize metrics
        this.connectionAttempts = meterRegistry.counter("tracker.client.connection.attempts", 
                "protocol", protocol, "service", serviceName);
        this.connectionSuccesses = meterRegistry.counter("tracker.client.connection.successes", 
                "protocol", protocol, "service", serviceName);
        this.connectionFailures = meterRegistry.counter("tracker.client.connection.failures", 
                "protocol", protocol, "service", serviceName);
        this.connectionDuration = meterRegistry.timer("tracker.client.connection.duration", 
                "protocol", protocol, "service", serviceName);

        BasePipelineFactory pipelineFactory = new BasePipelineFactory(this, config, protocol) {
            @Override
            protected void addTransportHandlers(PipelineBuilder pipeline) {
                try {
                    if (isSecure()) {
                        SSLEngine engine = SSLContext.getDefault().createSSLEngine();
                        engine.setUseClientMode(true);
                        pipeline.addLast(new SslHandler(engine));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline) {
                try {
                    TrackerClient.this.addProtocolHandlers(pipeline, config);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        bootstrap = new Bootstrap()
                .group(EventLoopGroupFactory.getWorkerGroup())
                .channel(NioSocketChannel.class)
                .handler(pipelineFactory);
    }

    protected abstract void addProtocolHandlers(PipelineBuilder pipeline, Config config) throws Exception;

    public String[] getDevices() {
        return devices;
    }

    @Override
    public ChannelGroup getChannelGroup() {
        return channelGroup;
    }
    
    /**
     * Resolves the endpoint address for the service using service discovery.
     * Uses circuit breaker pattern for resilient connections.
     *
     * @return The resolved endpoint address
     * @throws Exception If the endpoint cannot be resolved
     */
    private InetSocketAddress resolveEndpoint() throws Exception {
        // Create a span for the endpoint resolution
        Span span = tracer.spanBuilder("resolveEndpoint")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("service.name", serviceName)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Use circuit breaker to prevent cascading failures
            return circuitBreaker.executeSupplier(new Supplier<InetSocketAddress>() {
                @Override
                public InetSocketAddress get() {
                    try {
                        // Check if we have cached instances
                        if (serviceInstances.get() == null) {
                            // Discover service instances
                            List<ServiceInstance> instances = serviceDiscovery.findServiceInstances(serviceName);
                            if (instances.isEmpty()) {
                                span.addEvent("No service instances found");
                                throw new Exception("No service instances found for " + serviceName);
                            }
                            serviceInstances.set(instances);
                        }
                        
                        // Select an instance (simple round-robin for now)
                        List<ServiceInstance> instances = serviceInstances.get();
                        ServiceInstance instance = instances.get((int) (System.currentTimeMillis() % instances.size()));
                        
                        // Create and cache the endpoint address
                        String key = instance.getHost() + ":" + instance.getPort();
                        return endpointCache.computeIfAbsent(key, 
                                k -> new InetSocketAddress(instance.getHost(), instance.getPort()));
                    } catch (Exception e) {
                        span.recordException(e);
                        span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
                        throw new RuntimeException("Failed to resolve endpoint for " + serviceName, e);
                    }
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }

    @Override
    public void start() throws Exception {
        // Create a span for the connection attempt
        Span span = tracer.spanBuilder("trackerClient.connect")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("service.name", serviceName)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Record connection attempt metric
            connectionAttempts.increment();
            
            // Resolve the endpoint using service discovery
            InetSocketAddress endpoint = resolveEndpoint();
            span.setAttribute("peer.host", endpoint.getHostString());
            span.setAttribute("peer.port", endpoint.getPort());
            
            // Start the timer for connection duration
            Timer.Sample sample = Timer.start();
            
            // Connect to the resolved endpoint
            ChannelFuture future = bootstrap.connect(endpoint);
            future.syncUninterruptibly().channel().closeFuture().addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) {
                    // Record connection duration metric
                    sample.stop(connectionDuration);
                    
                    if (future.isSuccess()) {
                        // Record connection success metric
                        connectionSuccesses.increment();
                    } else {
                        // Record connection failure metric
                        connectionFailures.increment();
                        
                        // Record the exception in the span
                        if (future.cause() != null) {
                            span.recordException(future.cause());
                            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, future.cause().getMessage());
                        }
                    }
                    
                    // Schedule reconnection if interval is set
                    if (interval > 0) {
                        GlobalEventExecutor.INSTANCE.schedule(() -> {
                            try {
                                // Clear service instances cache to force rediscovery
                                serviceInstances.set(null);
                                
                                // Resolve the endpoint using service discovery
                                InetSocketAddress newEndpoint = resolveEndpoint();
                                
                                // Connect to the resolved endpoint
                                bootstrap.connect(newEndpoint)
                                        .syncUninterruptibly().channel().closeFuture().addListener(this);
                            } catch (Exception e) {
                                // Record connection failure metric
                                connectionFailures.increment();
                                
                                // Schedule another reconnection attempt
                                GlobalEventExecutor.INSTANCE.schedule(() -> {
                                    try {
                                        start();
                                    } catch (Exception ex) {
                                        // Ignore and retry later
                                    }
                                }, interval, TimeUnit.SECONDS);
                            }
                        }, interval, TimeUnit.SECONDS);
                    }
                }
            });
        } finally {
            span.end();
        }
    }

    @Override
    public void stop() {
        // Create a span for the stop operation
        Span span = tracer.spanBuilder("trackerClient.stop")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("service.name", serviceName)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Close all channels
            channelGroup.close().awaitUninterruptibly();
            
            // Clear caches
            serviceInstances.set(null);
            endpointCache.clear();
        } finally {
            span.end();
        }
    }

    /**
     * Refreshes the service instances from the service discovery system.
     * This can be called periodically to update the list of available instances.
     */
    public void refreshServiceInstances() {
        try {
            List<ServiceInstance> instances = serviceDiscovery.findServiceInstances(serviceName);
            if (!instances.isEmpty()) {
                serviceInstances.set(instances);
            }
        } catch (Exception e) {
            // Log the error but don't throw it to avoid disrupting the client
        }
    }
}