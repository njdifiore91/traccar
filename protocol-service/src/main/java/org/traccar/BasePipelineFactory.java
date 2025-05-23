/*
 * Copyright 2012 - 2023 Anton Tananaev (anton@traccar.org)
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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.timeout.IdleStateHandler;

import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.handler.OpenTelemetryHandler;
import org.traccar.handler.TimeHandler;
import org.traccar.handler.StandardLoggingHandler;
import org.traccar.handler.MessageToPositionHandler;
import org.traccar.messaging.MessageProducer;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;

import java.util.Map;

/**
 * Base pipeline factory for creating channel pipelines for device protocols.
 * This class has been updated to support microservices architecture by:
 * 1. Publishing decoded positions to a message broker instead of direct processing
 * 2. Adding OpenTelemetry instrumentation for distributed tracing
 * 3. Removing dependencies on monolithic components
 */
public abstract class BasePipelineFactory extends ChannelInitializer<Channel> {

    private final Config config;
    private final MessageProducer messageProducer;
    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;
    private final String serviceName;

    /**
     * Construct pipeline factory with provided dependencies.
     *
     * @param config Configuration parameters
     * @param messageProducer Message broker producer for publishing positions
     * @param openTelemetry OpenTelemetry instance for distributed tracing
     */
    public BasePipelineFactory(
            Config config, 
            MessageProducer messageProducer,
            OpenTelemetry openTelemetry) {
        this.config = config;
        this.messageProducer = messageProducer;
        this.openTelemetry = openTelemetry;
        this.tracer = openTelemetry.getTracer("org.traccar.protocol");
        this.serviceName = config.getString(Keys.SERVICE_NAME.getKey(), "protocol-service");
    }

    /**
     * Get timeout value for the idle state handler.
     *
     * @return timeout value in seconds
     */
    protected int getTimeout() {
        return config.getInteger(Keys.PROTOCOL_TIMEOUT.getKey());
    }

    /**
     * Add protocol-specific handlers to the pipeline.
     *
     * @param pipeline channel pipeline
     */
    protected abstract void addProtocolHandlers(ChannelPipeline pipeline);

    /**
     * Add common handlers to the pipeline.
     *
     * @param pipeline channel pipeline
     */
    protected void addCommonHandlers(ChannelPipeline pipeline) {
        int timeout = getTimeout();
        if (timeout > 0) {
            pipeline.addLast(new IdleStateHandler(timeout, 0, 0));
        }
        pipeline.addLast(new StandardLoggingHandler(config));
        pipeline.addLast(new TimeHandler());
    }

    /**
     * Initialize the channel pipeline with all necessary handlers.
     *
     * @param channel the channel for which to initialize the pipeline
     */
    @Override
    protected void initChannel(Channel channel) {
        ChannelPipeline pipeline = channel.pipeline();
        
        // Add OpenTelemetry instrumentation handler for distributed tracing
        pipeline.addLast(new OpenTelemetryHandler(tracer, serviceName));
        
        // Add common handlers
        addCommonHandlers(pipeline);
        
        // Add protocol-specific handlers
        addProtocolHandlers(pipeline);
        
        // Add message broker integration handler
        // This replaces the direct handler chain with message broker integration
        pipeline.addLast(new MessageToPositionHandler(messageProducer));
    }

    /**
     * Create a channel handler for the specified name.
     *
     * @param name handler name
     * @param parameters handler parameters
     * @return channel handler
     */
    public static ChannelHandler createHandler(String name, Map<String, Object> parameters) {
        try {
            Class<?> handlerClass = Class.forName(name);
            if (parameters != null) {
                return (ChannelHandler) handlerClass.getConstructor(Map.class).newInstance(parameters);
            } else {
                return (ChannelHandler) handlerClass.getDeclaredConstructor().newInstance();
            }
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}