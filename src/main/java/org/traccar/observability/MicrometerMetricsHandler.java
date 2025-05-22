/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Netty channel handler that collects metrics using Micrometer.
 * Tracks message counts, sizes, and processing times for both inbound and outbound messages.
 */
public class MicrometerMetricsHandler extends ChannelDuplexHandler {

    private final MeterRegistry registry;
    private final String protocol;
    private final ConcurrentMap<String, Timer> timers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> counters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DistributionSummary> summaries = new ConcurrentHashMap<>();

    public MicrometerMetricsHandler(MeterRegistry registry, String protocol) {
        this.registry = registry;
        this.protocol = protocol;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // Increment active connections counter
        getCounter("connections.active").increment();
        getCounter("connections.total").increment();
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // Decrement active connections counter
        getCounter("connections.active").increment(-1);
        ctx.fireChannelInactive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // Start timer for message processing
        Timer.Sample sample = Timer.start(registry);
        
        // Increment message counter
        getCounter("messages.received").increment();
        
        // Record message size if available
        if (msg instanceof io.netty.buffer.ByteBuf byteBuf) {
            getSummary("messages.received.bytes").record(byteBuf.readableBytes());
        }
        
        try {
            ctx.fireChannelRead(msg);
        } finally {
            // Record processing time
            sample.stop(getTimer("messages.received.time"));
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        // Start timer for message processing
        Timer.Sample sample = Timer.start(registry);
        
        // Increment message counter
        getCounter("messages.sent").increment();
        
        // Record message size if available
        if (msg instanceof io.netty.buffer.ByteBuf byteBuf) {
            getSummary("messages.sent.bytes").record(byteBuf.readableBytes());
        }
        
        // Add listener to record completion time
        promise.addListener(future -> {
            sample.stop(getTimer("messages.sent.time"));
            if (!future.isSuccess()) {
                getCounter("messages.sent.errors").increment();
            }
        });
        
        ctx.write(msg, promise);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Increment error counter
        getCounter("errors").increment();
        
        // Record error by type
        getCounter("errors." + cause.getClass().getSimpleName()).increment();
        
        ctx.fireExceptionCaught(cause);
    }

    private Timer getTimer(String name) {
        return timers.computeIfAbsent(name, key -> Timer.builder("traccar." + key)
                .tag("protocol", protocol)
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry));
    }

    private Counter getCounter(String name) {
        return counters.computeIfAbsent(name, key -> Counter.builder("traccar." + key)
                .tag("protocol", protocol)
                .register(registry));
    }

    private DistributionSummary getSummary(String name) {
        return summaries.computeIfAbsent(name, key -> DistributionSummary.builder("traccar." + key)
                .tag("protocol", protocol)
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry));
    }
}