/*
 * Copyright 2017 - 2018 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.traccar.BaseProtocol;
import org.traccar.PipelineBuilder;
import org.traccar.TrackerServer;
import org.traccar.config.Config;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.messaging.MessageProducer;
import org.traccar.observability.TracingHandler;
import org.traccar.observability.MetricsHandler;

import jakarta.inject.Inject;

/**
 * Protocol implementation for Genx GPS tracking devices.
 * This implementation supports both direct and service-based communication,
 * with integrated service discovery, message broker integration, distributed tracing,
 * and metrics collection.
 *
 * Key features:
 * - Service discovery integration for protocol service registration
 * - Support for both direct and service-based communication
 * - Message broker integration for asynchronous position publishing
 * - Distributed tracing for protocol message tracking
 * - Metrics collection for protocol performance monitoring
 * - Containerized environment configuration support
 */
public class GenxProtocol extends BaseProtocol {

    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    /**
     * Constructs a new GenxProtocol instance with all required dependencies.
     *
     * @param config The configuration object
     * @param serviceDiscoveryManager Service discovery manager for protocol service discovery
     * @param messageProducer Message producer for asynchronous position publishing
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Meter registry for metrics collection
     */
    @Inject
    public GenxProtocol(
            Config config,
            ServiceDiscoveryManager serviceDiscoveryManager,
            MessageProducer messageProducer,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Register protocol with service discovery
        registerWithServiceDiscovery();
        
        // Register metrics for protocol performance monitoring
        registerMetrics();
        
        // Add direct communication server (for backward compatibility)
        if (config.getBoolean("protocol.genx.directCommunication.enabled", true)) {
            addDirectCommunicationServer(config);
        }
        
        // Add service-based communication if enabled
        if (config.getBoolean("protocol.genx.serviceBased.enabled", true)) {
            addServiceBasedCommunication();
        }
    }
    
    /**
     * Registers the protocol service with the service discovery manager.
     */
    private void registerWithServiceDiscovery() {
        serviceDiscoveryManager.registerService(
                "protocol-genx",
                "Protocol handler for Genx devices",
                "/health");
    }
    
    /**
     * Registers metrics for protocol performance monitoring.
     */
    private void registerMetrics() {
        // Register protocol-specific metrics
        Counter messagesReceived = Counter.builder("protocol.messages.received")
                .tag("protocol", "genx")
                .description("Number of messages received from Genx devices")
                .register(meterRegistry);
        
        Counter messagesProcessed = Counter.builder("protocol.messages.processed")
                .tag("protocol", "genx")
                .description("Number of messages successfully processed from Genx devices")
                .register(meterRegistry);
        
        Timer messageProcessingTime = Timer.builder("protocol.message.processing.time")
                .tag("protocol", "genx")
                .description("Time taken to process Genx device messages")
                .register(meterRegistry);
    }
    
    /**
     * Adds a direct communication server for backward compatibility.
     * 
     * @param config The configuration object
     */
    private void addDirectCommunicationServer(Config config) {
        addServer(new TrackerServer(config, getName(), false) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                // Add distributed tracing handler for protocol message tracking
                pipeline.addLast(new TracingHandler(tracer, "genx", SpanKind.SERVER));
                
                // Add original protocol handlers
                pipeline.addLast(new LineBasedFrameDecoder(1024));
                pipeline.addLast(new StringDecoder());
                
                // Add protocol decoder with message broker integration for asynchronous position publishing
                GenxProtocolDecoder decoder = new GenxProtocolDecoder(GenxProtocol.this);
                decoder.setMessageProducer(messageProducer);
                pipeline.addLast(decoder);
                
                // Add metrics handler for protocol performance monitoring
                pipeline.addLast(new MetricsHandler(meterRegistry, "genx"));
            }
        });
    }
    
    /**
     * Adds service-based communication support.
     */
    private void addServiceBasedCommunication() {
        // Service-based communication is handled through the message broker
        // and service discovery. No additional server setup is needed here
        // as the protocol service will receive messages through the broker.
    }
}