/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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

import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.opentelemetry.api.trace.Tracer;
import jakarta.inject.Inject;
import org.traccar.BaseProtocol;
import org.traccar.PipelineBuilder;
import org.traccar.TrackerServer;
import org.traccar.config.Config;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.messaging.MessageProducer;
import org.traccar.metrics.ProtocolMetrics;

/**
 * Protocol implementation for Ramac GPS trackers.
 * This protocol handles HTTP-based communication with Ramac devices.
 */
public class RamacProtocol extends BaseProtocol {

    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final ProtocolMetrics protocolMetrics;

    /**
     * Initialize the protocol with required dependencies.
     *
     * @param config Configuration parameters
     * @param serviceDiscoveryManager Service discovery for protocol service registration
     * @param messageProducer Message broker producer for position publishing
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param protocolMetrics Metrics collector for protocol performance monitoring
     */
    @Inject
    public RamacProtocol(
            Config config,
            ServiceDiscoveryManager serviceDiscoveryManager,
            MessageProducer messageProducer,
            Tracer tracer,
            ProtocolMetrics protocolMetrics) {
        
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.protocolMetrics = protocolMetrics;
        
        // Register protocol with service discovery if available (microservice mode)
        if (serviceDiscoveryManager != null) {
            serviceDiscoveryManager.registerProtocol(getName(), getPort(config));
        }
        
        // Initialize protocol metrics with appropriate labels
        if (protocolMetrics != null) {
            protocolMetrics.initializeMetrics(getName());
        }
        
        // Add server for direct device connections
        addServer(new TrackerServer(config, getName(), false) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                pipeline.addLast(new HttpResponseEncoder());
                pipeline.addLast(new HttpRequestDecoder());
                pipeline.addLast(new HttpObjectAggregator(65535));
                // Create protocol decoder with all required dependencies
                RamacProtocolDecoder decoder = new RamacProtocolDecoder(
                        RamacProtocol.this, messageProducer, tracer, protocolMetrics);
                
                // Add the decoder to the pipeline
                pipeline.addLast(decoder);
            }
        });
    }
    
    /**
     * Returns health status information for this protocol
     * Used by the health check endpoint in containerized environments
     * 
     * @return Health status information
     */
    public boolean isHealthy() {
        // Implement basic health check logic
        return true; // Can be enhanced with more sophisticated checks
    }
    
    /**
     * Get the configured port for this protocol
     * 
     * @param config Configuration parameters
     * @return Port number or default value
     */
    private int getPort(Config config) {
        return config.getInteger("protocol.ramac.port", 8085);
    }
}