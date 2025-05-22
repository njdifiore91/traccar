/*
 * Copyright 2019 - 2023 Anton Tananaev (anton@traccar.org)
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
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.opentelemetry.api.trace.Tracer;
import org.traccar.BaseProtocol;
import org.traccar.PipelineBuilder;
import org.traccar.TrackerServer;
import org.traccar.config.Config;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.handler.OpenTelemetryHandler;
import org.traccar.handler.MicrometerHandler;
import org.traccar.messaging.MessageProducer;

import jakarta.inject.Inject;

/**
 * Protocol implementation for Radar GPS tracking devices.
 * Supports both direct communication and service-based communication through the protocol service.
 */
public class RadarProtocol extends BaseProtocol {

    private final MessageProducer messageProducer;
    private final ServiceDiscovery serviceDiscovery;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    /**
     * Initialize the protocol with required dependencies.
     *
     * @param config Configuration parameters
     * @param messageProducer Message broker producer for asynchronous position publishing
     * @param serviceDiscovery Service discovery for protocol service location
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry for performance monitoring
     */
    @Inject
    public RadarProtocol(
            Config config,
            MessageProducer messageProducer,
            ServiceDiscovery serviceDiscovery,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        
        super(config);
        this.messageProducer = messageProducer;
        this.serviceDiscovery = serviceDiscovery;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Register protocol name for metrics and tracing
        String protocolName = getName();
        
        // Add server for direct device communication
        if (config.getBoolean("protocol.radar.directCommunication", true)) {
            addServer(new TrackerServer(config, getName(), false) {
                @Override
                protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                    // Add distributed tracing handler
                    pipeline.addLast(new OpenTelemetryHandler(tracer, protocolName));
                    
                    // Add metrics collection handler
                    pipeline.addLast(new MicrometerHandler(meterRegistry, protocolName));
                    
                    // Add protocol-specific handlers
                    pipeline.addLast(new LengthFieldBasedFrameDecoder(1024, 12, 2, -14, 0));
                    pipeline.addLast(new RadarProtocolDecoder(RadarProtocol.this, messageProducer));
                }
            });
        }
        
        // Register with service discovery if enabled
        if (config.getBoolean("protocol.radar.registerWithDiscovery", true)) {
            registerWithServiceDiscovery();
        }
    }
    
    /**
     * Register this protocol instance with the service discovery system.
     * This allows other services to discover and communicate with this protocol handler.
     */
    private void registerWithServiceDiscovery() {
        try {
            // Protocol service registration is handled by the container orchestration
            // This method would contain additional registration logic if needed
            // beyond what's provided by the container platform
        } catch (Exception e) {
            getLogger().warn("Failed to register protocol with service discovery", e);
        }
    }
}