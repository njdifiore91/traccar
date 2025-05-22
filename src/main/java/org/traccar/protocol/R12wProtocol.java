/*
 * Copyright 2021 Anton Tananaev (anton@traccar.org)
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

import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.traccar.BaseProtocol;
import org.traccar.PipelineBuilder;
import org.traccar.TrackerServer;
import org.traccar.config.Config;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.messaging.MessageProducer;
import org.traccar.metrics.ProtocolMetrics;

import jakarta.inject.Inject;

/**
 * Protocol implementation for R12w GPS trackers.
 * Supports both direct and service-based communication modes.
 */
public class R12wProtocol extends BaseProtocol {

    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final ProtocolMetrics metrics;
    private final ServiceDiscoveryManager serviceDiscoveryManager;

    /**
     * Constructs the R12w protocol handler with required dependencies.
     *
     * @param config Configuration provider
     * @param messageProducer Message broker producer for position publishing
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param metrics Protocol metrics collector
     * @param serviceDiscoveryManager Service discovery for protocol registration
     */
    @Inject
    public R12wProtocol(
            Config config,
            MessageProducer messageProducer,
            Tracer tracer,
            ProtocolMetrics metrics,
            ServiceDiscoveryManager serviceDiscoveryManager) {
        
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.metrics = metrics;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        
        // Log protocol initialization with metrics
        metrics.getConnectionMetrics().protocolInitialized(getName());
        
        // Register protocol with service discovery if in service mode
        if (!config.getBoolean("protocol.r12w.directMode", true)) {
            registerWithServiceDiscovery();
        }
        
        // Create server based on configuration mode
        addServer(createServer(config));
    }

    /**
     * Registers this protocol implementation with the service discovery system.
     * This allows other services to discover and communicate with this protocol handler.
     */
    private void registerWithServiceDiscovery() {
        Span span = tracer.spanBuilder("R12wProtocol.registerWithServiceDiscovery").startSpan();
        try {
            serviceDiscoveryManager.registerService(
                    "protocol-r12w",
                    "R12w Protocol Handler",
                    "protocol");
            span.addEvent("Protocol registered with service discovery");
        } catch (Exception e) {
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Creates a tracker server instance based on the current configuration.
     * Supports both direct (monolithic) and service-based (microservices) modes.
     *
     * @param config Configuration provider
     * @return Configured tracker server instance
     */
    private TrackerServer createServer(Config config) {
        Span span = tracer.spanBuilder("R12wProtocol.createServer").startSpan();
        try {
            // Determine if we're running in direct mode (monolithic) or service mode (microservices)
            boolean directMode = config.getBoolean("protocol.r12w.directMode", true);
            span.setAttribute("protocol.mode", directMode ? "direct" : "service");
            
            // Get protocol-specific configuration
            int port = config.getInteger("protocol.r12w.port", 5047);
            span.setAttribute("protocol.port", port);
            
            // Configure buffer size based on environment (larger for containerized)
            int bufferSize = config.getInteger("protocol.r12w.bufferSize", 1024);
            if (System.getenv("CONTAINER_ENVIRONMENT") != null) {
                // Use larger buffer in containerized environments
                bufferSize = config.getInteger("protocol.r12w.container.bufferSize", 4096);
                span.setAttribute("environment", "container");
            } else {
                span.setAttribute("environment", "standard");
            }
            span.setAttribute("protocol.bufferSize", bufferSize);
            
            // Create the server with appropriate configuration
            final int finalBufferSize = bufferSize;
            TrackerServer server = new TrackerServer(config, getName(), false) {
                @Override
                protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                    // Add common protocol handlers
                    pipeline.addLast(new LineBasedFrameDecoder(finalBufferSize));
                    pipeline.addLast(new StringEncoder());
                    pipeline.addLast(new StringDecoder());
                    
                    // Create and add protocol decoder with dependencies
                    R12wProtocolDecoder decoder = new R12wProtocolDecoder(
                            R12wProtocol.this, 
                            messageProducer, 
                            tracer, 
                            metrics);
                    pipeline.addLast(decoder);
                }
            };
            
            // Record metrics for server creation
            metrics.getConnectionMetrics().serverCreated(getName());
            span.addEvent("Tracker server created");
            span.setAttribute("server.status", "created");
            
            return server;
        } catch (Exception e) {
            span.recordException(e);
            metrics.getConnectionMetrics().serverCreationFailed(getName());
            span.setAttribute("server.status", "failed");
            throw e;
        } finally {
            span.end();
        }
    }
}