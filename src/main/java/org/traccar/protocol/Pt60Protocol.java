/*
 * Copyright 2018 - 2023 Anton Tananaev (anton@traccar.org)
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

import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.opentelemetry.api.trace.Tracer;

import org.traccar.BaseProtocol;
import org.traccar.CharacterDelimiterFrameDecoder;
import org.traccar.PipelineBuilder;
import org.traccar.TrackerServer;
import org.traccar.config.Config;
import org.traccar.handler.OpenTelemetryHandler;
import org.traccar.handler.MicrometerHandler;
import org.traccar.handler.MessageBrokerHandler;
import org.traccar.discovery.ServiceDiscoveryManager;

import io.micrometer.core.instrument.MeterRegistry;

import jakarta.inject.Inject;

/**
 * Protocol implementation for PT60 GPS tracking devices.
 * This implementation supports both direct and service-based communication.
 */
public class Pt60Protocol extends BaseProtocol {

    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final MessageBrokerHandler messageBrokerHandler;

    /**
     * Initialize the protocol with required dependencies.
     *
     * @param config Configuration parameters
     * @param serviceDiscoveryManager Service discovery for protocol service registration
     * @param meterRegistry Metrics registry for performance monitoring
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param messageBrokerHandler Handler for asynchronous message publishing
     */
    @Inject
    public Pt60Protocol(
            Config config,
            ServiceDiscoveryManager serviceDiscoveryManager,
            MeterRegistry meterRegistry,
            Tracer tracer,
            MessageBrokerHandler messageBrokerHandler) {
        
        super(config);
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        this.messageBrokerHandler = messageBrokerHandler;
        
        // Register this protocol with service discovery
        registerWithServiceDiscovery();
        
        // Add server for direct communication mode
        addDirectCommunicationServer(config);
    }
    
    /**
     * Register this protocol service with the service discovery system.
     */
    private void registerWithServiceDiscovery() {
        if (serviceDiscoveryManager != null) {
            serviceDiscoveryManager.registerService(
                "protocol-pt60",
                "PT60 Protocol Handler",
                config.getInteger("pt60.port", 5055));
        }
    }
    
    /**
     * Add server for direct communication mode (backward compatibility).
     * 
     * @param config Configuration parameters
     */
    private void addDirectCommunicationServer(Config config) {
        addServer(new TrackerServer(config, getName(), false) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                // Frame decoder for PT60 protocol
                pipeline.addLast(new CharacterDelimiterFrameDecoder(1024, "@R#@", "@E#@"));
                
                // String encoder/decoder for text-based protocol
                pipeline.addLast(new StringEncoder());
                pipeline.addLast(new StringDecoder());
                
                // Add distributed tracing if enabled
                if (config.getBoolean("tracing.enabled", false)) {
                    pipeline.addLast(new OpenTelemetryHandler(tracer, "pt60"));
                }
                
                // Add metrics collection if enabled
                if (config.getBoolean("metrics.enabled", false)) {
                    pipeline.addLast(new MicrometerHandler(meterRegistry, "protocol.pt60"));
                }
                
                // Protocol decoder
                pipeline.addLast(new Pt60ProtocolDecoder(Pt60Protocol.this, messageBrokerHandler));
            }
        });
    }
}