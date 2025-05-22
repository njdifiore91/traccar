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
package org.traccar;

import com.google.inject.Injector;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.netty.NettyEventExecutorMetrics;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.handler.network.AcknowledgementHandler;
import org.traccar.handler.network.MainEventHandler;
import org.traccar.handler.network.NetworkForwarderHandler;
import org.traccar.handler.network.NetworkMessageHandler;
import org.traccar.handler.network.OpenChannelHandler;
import org.traccar.handler.network.RemoteAddressHandler;
import org.traccar.handler.network.StandardLoggingHandler;
import org.traccar.observability.CircuitBreakerHandler;
import org.traccar.observability.MicrometerMetricsHandler;
import org.traccar.observability.OpenTelemetryTracingHandler;
import org.traccar.observability.ServiceDiscoveryHandler;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public abstract class BasePipelineFactory extends ChannelInitializer<Channel> {

    private final Injector injector;
    private final TrackerConnector connector;
    private final Config config;
    private final String protocol;
    private final int timeout;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final CircuitBreakerRegistry circuitBreakerRegistry;

    public BasePipelineFactory(TrackerConnector connector, Config config, String protocol) {
        this.injector = Main.getInjector();
        this.connector = connector;
        this.config = config;
        this.protocol = protocol;
        int timeout = config.getInteger(Keys.PROTOCOL_TIMEOUT.withPrefix(protocol));
        if (timeout == 0) {
            this.timeout = config.getInteger(Keys.SERVER_TIMEOUT);
        } else {
            this.timeout = timeout;
        }
        
        // Initialize Micrometer registry
        this.meterRegistry = Metrics.globalRegistry;
        
        // Initialize OpenTelemetry tracer
        OpenTelemetry openTelemetry = injector.getInstance(OpenTelemetry.class);
        this.tracer = openTelemetry.getTracer("org.traccar." + protocol);
        
        // Initialize Circuit Breaker registry
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(10)
                .slidingWindowSize(100)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
    }

    protected abstract void addTransportHandlers(PipelineBuilder pipeline);

    protected abstract void addProtocolHandlers(PipelineBuilder pipeline);

    @SuppressWarnings("unchecked")
    public static <T extends ChannelHandler> T getHandler(ChannelPipeline pipeline, Class<T> clazz) {
        for (Map.Entry<String, ChannelHandler> handlerEntry : pipeline) {
            ChannelHandler handler = handlerEntry.getValue();
            if (handler instanceof WrapperInboundHandler wrapperHandler) {
                handler = wrapperHandler.getWrappedHandler();
            } else if (handler instanceof WrapperOutboundHandler wrapperHandler) {
                handler = wrapperHandler.getWrappedHandler();
            }
            if (clazz.isAssignableFrom(handler.getClass())) {
                return (T) handler;
            }
        }
        return null;
    }

    private <T> T injectMembers(T object) {
        injector.injectMembers(object);
        return object;
    }
    
    /**
     * Creates a circuit breaker for the specified service
     * 
     * @param serviceName Name of the service to create circuit breaker for
     * @return CircuitBreaker instance
     */
    protected CircuitBreaker getCircuitBreaker(String serviceName) {
        return circuitBreakerRegistry.circuitBreaker(protocol + "-" + serviceName);
    }
    
    /**
     * Adds instrumentation handlers to the pipeline for observability
     * 
     * @param pipeline The channel pipeline
     * @param channel The channel being initialized
     */
    private void addObservabilityHandlers(ChannelPipeline pipeline, Channel channel) {
        // Add OpenTelemetry tracing handler
        pipeline.addLast(new OpenTelemetryTracingHandler(tracer, protocol));
        
        // Add Micrometer metrics handler
        pipeline.addLast(new MicrometerMetricsHandler(meterRegistry, protocol));
        
        // Add metrics for Netty internals if channel supports it
        if (channel instanceof SocketChannel socketChannel) {
            // Add metrics for event loop
            EventLoop eventLoop = socketChannel.eventLoop();
            new NettyEventExecutorMetrics(eventLoop).bindTo(meterRegistry);
            
            // Add metrics for buffer allocator
            ByteBufAllocator allocator = socketChannel.alloc();
            if (allocator instanceof ByteBufAllocatorMetricProvider) {
                ByteBufAllocatorMetricProvider allocatorMetric = (ByteBufAllocatorMetricProvider) allocator;
                io.micrometer.core.instrument.binder.netty.NettyAllocatorMetrics metrics = 
                        new io.micrometer.core.instrument.binder.netty.NettyAllocatorMetrics(allocatorMetric);
                metrics.bindTo(meterRegistry);
            }
        }
        
        // Add circuit breaker handler for external service calls
        CircuitBreaker circuitBreaker = getCircuitBreaker("external-service");
        pipeline.addLast(new CircuitBreakerHandler(circuitBreaker));
        
        // Add service discovery handler if enabled
        if (config.getBoolean(Keys.SERVICE_DISCOVERY_ENABLED)) {
            pipeline.addLast(injectMembers(new ServiceDiscoveryHandler()));
        }
    }

    @Override
    protected void initChannel(Channel channel) {
        final ChannelPipeline pipeline = channel.pipeline();
        
        // Start a new trace for this channel
        Span span = tracer.spanBuilder("channel.initialize")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("protocol", protocol)
                .setAttribute("remote.address", channel.remoteAddress().toString())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Add transport handlers
            addTransportHandlers(pipeline::addLast);

            // Add timeout handler if configured
            if (timeout > 0 && !connector.isDatagram()) {
                pipeline.addLast(new IdleStateHandler(timeout, 0, 0));
            }
            
            // Add observability handlers
            addObservabilityHandlers(pipeline, channel);
            
            // Add standard pipeline handlers
            pipeline.addLast(new OpenChannelHandler(connector));
            if (config.hasKey(Keys.SERVER_FORWARD)) {
                int port = config.getInteger(Keys.PROTOCOL_PORT.withPrefix(protocol));
                pipeline.addLast(injectMembers(new NetworkForwarderHandler(port)));
            }
            pipeline.addLast(new NetworkMessageHandler());
            pipeline.addLast(injectMembers(new StandardLoggingHandler(protocol)));

            if (!connector.isDatagram() && !config.getBoolean(Keys.SERVER_INSTANT_ACKNOWLEDGEMENT)) {
                pipeline.addLast(new AcknowledgementHandler());
            }

            // Add protocol-specific handlers
            addProtocolHandlers(handler -> {
                if (handler instanceof BaseProtocolDecoder || handler instanceof BaseProtocolEncoder) {
                    injectMembers(handler);
                } else {
                    if (handler instanceof ChannelInboundHandler channelHandler) {
                        handler = new WrapperInboundHandler(channelHandler);
                    } else if (handler instanceof ChannelOutboundHandler channelHandler) {
                        handler = new WrapperOutboundHandler(channelHandler);
                    }
                }
                pipeline.addLast(handler);
            });

            // Add final handlers
            pipeline.addLast(injector.getInstance(RemoteAddressHandler.class));
            pipeline.addLast(injector.getInstance(ProcessingHandler.class));
            pipeline.addLast(injector.getInstance(MainEventHandler.class));
            
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }

}