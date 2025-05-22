/*
 * Copyright 2015 - 2018 Anton Tananaev (anton@traccar.org)
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
package org.traccar.protocol;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.opentelemetry.api.trace.Tracer;
import org.traccar.BaseProtocol;
import org.traccar.PipelineBuilder;
import org.traccar.TrackerServer;
import org.traccar.config.Config;
import org.traccar.discovery.ServiceRegistry;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.PositionMessageProducer;
import org.traccar.metrics.ProtocolMetrics;
import org.traccar.model.Command;

import jakarta.inject.Inject;
import jakarta.inject.Named;

/**
 * Protocol implementation for Gator GPS devices.
 * Supports both direct and service-based communication.
 */
public class GatorProtocol extends BaseProtocol {

    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final ServiceRegistry serviceRegistry;

    /**
     * Initialize the Gator protocol with required dependencies.
     *
     * @param config Configuration object
     * @param messageProducer Message broker producer for asynchronous position publishing
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry for performance monitoring
     * @param serviceRegistry Service discovery registry for protocol service registration
     */
    @Inject
    public GatorProtocol(
            Config config,
            @Named("positionProducer") MessageProducer messageProducer,
            Tracer tracer,
            MeterRegistry meterRegistry,
            ServiceRegistry serviceRegistry) {
        
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.serviceRegistry = serviceRegistry;
        
        // Register supported commands
        setSupportedDataCommands(
                Command.TYPE_POSITION_SINGLE,
                Command.TYPE_ENGINE_RESUME,
                Command.TYPE_ENGINE_STOP,
                Command.TYPE_SET_SPEED_LIMIT,
                Command.TYPE_SET_ODOMETER);
        
        // Register protocol metrics
        registerMetrics();
        
        // Register with service discovery
        registerWithServiceDiscovery();
        
        // Add TCP server
        addServer(new TrackerServer(config, getName(), false) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                pipeline.addLast(new LengthFieldBasedFrameDecoder(1024, 3, 2));
                pipeline.addLast(new GatorProtocolEncoder(GatorProtocol.this));
                pipeline.addLast(new GatorProtocolDecoder(GatorProtocol.this, messageProducer, tracer));
            }
        });
        
        // Add UDP server
        addServer(new TrackerServer(config, getName(), true) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                pipeline.addLast(new GatorProtocolDecoder(GatorProtocol.this, messageProducer, tracer));
            }
        });
    }

    /**
     * Register protocol metrics with Micrometer.
     */
    private void registerMetrics() {
        Tags tags = Tags.of("protocol", getName());
        
        // Register connection metrics
        meterRegistry.gauge("protocol.connections.active", tags, this, p -> getDeviceCount());
        
        // Register message metrics (these will be incremented by the decoder)
        meterRegistry.counter("protocol.messages.received", tags);
        meterRegistry.counter("protocol.messages.processed", tags);
        meterRegistry.counter("protocol.messages.dropped", tags);
    }

    /**
     * Register the protocol service with the service discovery mechanism.
     */
    private void registerWithServiceDiscovery() {
        if (serviceRegistry != null) {
            serviceRegistry.register("protocol." + getName(), 
                    "Protocol handler for " + getName() + " devices",
                    "protocol");
        }
    }
}