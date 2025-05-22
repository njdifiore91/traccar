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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.opentelemetry.api.trace.Tracer;

import org.traccar.BaseProtocol;
import org.traccar.CharacterDelimiterFrameDecoder;
import org.traccar.PipelineBuilder;
import org.traccar.TrackerServer;
import org.traccar.config.Config;
import org.traccar.discovery.ServiceRegistry;
import org.traccar.messaging.MessageProducer;

import jakarta.inject.Inject;

/**
 * Protocol implementation for GL100 GPS trackers.
 * Supports both direct communication and service-based communication through a message broker.
 */
public class Gl100Protocol extends BaseProtocol {

    private final ServiceRegistry serviceRegistry;
    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    /**
     * Metrics for monitoring protocol performance
     */
    private final Counter messagesReceived;
    private final Counter messagesDecoded;
    private final Counter messageErrors;
    private final Timer messageProcessingTime;

    @Inject
    public Gl100Protocol(Config config, 
                        ServiceRegistry serviceRegistry, 
                        MessageProducer messageProducer,
                        Tracer tracer,
                        MeterRegistry meterRegistry) {
        this.serviceRegistry = serviceRegistry;
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.messagesReceived = Counter.builder("protocol.gl100.messages.received")
                .description("Number of messages received from GL100 devices")
                .tag("protocol", "gl100")
                .register(meterRegistry);
        
        this.messagesDecoded = Counter.builder("protocol.gl100.messages.decoded")
                .description("Number of messages successfully decoded from GL100 devices")
                .tag("protocol", "gl100")
                .register(meterRegistry);
        
        this.messageErrors = Counter.builder("protocol.gl100.messages.errors")
                .description("Number of errors encountered while processing GL100 messages")
                .tag("protocol", "gl100")
                .register(meterRegistry);
        
        this.messageProcessingTime = Timer.builder("protocol.gl100.processing.time")
                .description("Time taken to process GL100 messages")
                .tag("protocol", "gl100")
                .register(meterRegistry);

        // Register with service discovery
        registerWithServiceDiscovery();
        
        // Add TCP server (non-SSL)
        addServer(new TrackerServer(config, getName(), false) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                pipeline.addLast(new CharacterDelimiterFrameDecoder(1024, '\0'));
                pipeline.addLast(new StringEncoder());
                pipeline.addLast(new StringDecoder());
                pipeline.addLast(new Gl100ProtocolDecoder(Gl100Protocol.this, 
                                                        messageProducer, 
                                                        tracer, 
                                                        messagesReceived, 
                                                        messagesDecoded, 
                                                        messageErrors, 
                                                        messageProcessingTime));
            }
        });
        
        // Add TCP server (SSL)
        addServer(new TrackerServer(config, getName(), true) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                pipeline.addLast(new StringEncoder());
                pipeline.addLast(new StringDecoder());
                pipeline.addLast(new Gl100ProtocolDecoder(Gl100Protocol.this, 
                                                        messageProducer, 
                                                        tracer, 
                                                        messagesReceived, 
                                                        messagesDecoded, 
                                                        messageErrors, 
                                                        messageProcessingTime));
            }
        });
    }

    /**
     * Registers this protocol service with the service discovery mechanism
     */
    private void registerWithServiceDiscovery() {
        if (serviceRegistry != null) {
            try {
                serviceRegistry.register("protocol-gl100", "tcp", getPort());
            } catch (Exception e) {
                getLogger().warn("Failed to register with service discovery", e);
            }
        }
    }
    
    /**
     * Gets the TCP port used by this protocol
     * @return the port number
     */
    private int getPort() {
        // Return the configured port or a default value
        return 5100; // Default port for GL100 protocol
    }
}