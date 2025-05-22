/*
 * Copyright 2019 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.BaseProtocol;
import org.traccar.PipelineBuilder;
import org.traccar.TrackerServer;
import org.traccar.config.Config;
import org.traccar.messaging.MessageProducer;
import org.traccar.metrics.MetricsManager;
import org.traccar.discovery.ServiceDiscoveryManager;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Protocol implementation for RaceDynamics GPS tracking devices.
 * Updated to support microservices architecture with service discovery,
 * message broker integration, and distributed tracing.
 */
@Singleton
public class RaceDynamicsProtocol extends BaseProtocol {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaceDynamicsProtocol.class);
    
    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final MetricsManager metricsManager;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final Config config;

    /**
     * Constructs the RaceDynamics protocol handler with required dependencies.
     *
     * @param config Configuration provider
     * @param messageProducer Message broker producer for position publishing
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param metricsManager Metrics collection for protocol performance
     * @param serviceDiscoveryManager Service discovery for protocol service registration
     */
    @Inject
    public RaceDynamicsProtocol(
            Config config,
            MessageProducer messageProducer,
            Tracer tracer,
            MetricsManager metricsManager,
            ServiceDiscoveryManager serviceDiscoveryManager) {
        
        this.config = config;
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.metricsManager = metricsManager;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        
        // Create a span for protocol initialization
        Span span = tracer.spanBuilder("RaceDynamicsProtocol.initialize")
                .setAttribute("protocol.name", getName())
                .startSpan();
        
        try {
            // Register protocol metrics
            metricsManager.registerProtocolMetrics(getName());
            
            // Register with service discovery if enabled
            if (config.getBoolean("service.discovery.enabled", false)) {
                try {
                    int port = config.getInteger("protocol.racedynamics.port", 5094);
                    String serviceId = serviceDiscoveryManager.registerProtocolService(getName(), port);
                    span.setAttribute("service.discovery.id", serviceId);
                    span.setAttribute("service.discovery.port", port);
                    LOGGER.info("Registered RaceDynamics protocol with service discovery, serviceId: {}, port: {}", 
                            serviceId, port);
                } catch (Exception e) {
                    LOGGER.warn("Failed to register RaceDynamics protocol with service discovery", e);
                    span.setStatus(StatusCode.ERROR, "Failed to register with service discovery: " + e.getMessage());
                    span.recordException(e);
                }
            }
            
            // Configure message broker
            String brokerType = config.getString("messaging.broker.type", "kafka");
            span.setAttribute("messaging.broker.type", brokerType);
            LOGGER.info("Configured RaceDynamics protocol with message broker type: {}", brokerType);
            
            // Initialize the server
            initializeServer(span);
            
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.error("Error initializing RaceDynamics protocol", e);
            span.setStatus(StatusCode.ERROR, "Protocol initialization failed: " + e.getMessage());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Initializes the tracker server with the appropriate protocol handlers.
     * 
     * @param parentSpan The parent span for tracing
     */
    private void initializeServer(Span parentSpan) {
        addServer(new TrackerServer(config, getName(), false) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                // Create a child span for protocol handler initialization
                Span span = tracer.spanBuilder("RaceDynamicsProtocol.addProtocolHandlers")
                        .setParent(parentSpan.getSpanContext())
                        .setAttribute("protocol.name", getName())
                        .startSpan();
                
                try (var scope = span.makeCurrent()) {
                    // Configure frame decoder with max frame length from config or default
                    int maxFrameLength = config.getInteger("protocol.racedynamics.maxFrameLength", 1500);
                    span.setAttribute("protocol.maxFrameLength", maxFrameLength);
                    
                    pipeline.addLast(new LineBasedFrameDecoder(maxFrameLength));
                    pipeline.addLast(new StringEncoder());
                    pipeline.addLast(new StringDecoder());
                    pipeline.addLast(new RaceDynamicsProtocolDecoder(
                            RaceDynamicsProtocol.this, 
                            messageProducer, 
                            tracer));
                    
                    // Record protocol configuration metrics
                    AttributesBuilder attributesBuilder = Attributes.builder()
                            .put("protocol.name", getName())
                            .put("protocol.maxFrameLength", maxFrameLength);
                    
                    if (config.getBoolean("service.discovery.enabled", false)) {
                        attributesBuilder.put("service.discovery.enabled", true);
                    }
                    
                    metricsManager.recordProtocolInitialization(getName(), attributesBuilder.build());
                    LOGGER.info("Initialized RaceDynamics protocol handlers with maxFrameLength: {}", maxFrameLength);
                    
                    span.setStatus(StatusCode.OK);
                } catch (Exception e) {
                    LOGGER.error("Error initializing RaceDynamics protocol handlers", e);
                    span.setStatus(StatusCode.ERROR, "Protocol handler initialization failed: " + e.getMessage());
                    span.recordException(e);
                    throw e;
                } finally {
                    span.end();
                }
            }
        });
    }
}
}