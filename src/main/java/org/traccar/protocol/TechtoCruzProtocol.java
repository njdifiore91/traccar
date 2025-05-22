/*
 * Copyright 2021 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.micrometer.core.instrument.Timer;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.opentelemetry.api.trace.SpanKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.BaseProtocol;
import org.traccar.MicrometerMetricsManager;
import org.traccar.OpenTelemetryManager;
import org.traccar.PipelineBuilder;
import org.traccar.ServiceDiscoveryManager;
import org.traccar.TrackerServer;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.config.ConfigKey;

/**
 * Configuration keys for protocol direct communication mode.
 */
class ProtocolKeys {
    /**
     * Enable direct communication mode for protocols instead of service-based communication.
     */
    public static final ConfigKey<Boolean> PROTOCOL_DIRECT_COMMUNICATION = new ConfigKey<>(
            "protocol.directCommunication", Boolean.class, true);

    /**
     * Enable message broker integration for position publishing.
     */
    public static final ConfigKey<Boolean> BROKER_ENABLED = new ConfigKey<>(
            "broker.enabled", Boolean.class, false);
}
import org.traccar.handler.OpenTelemetryHandler;
import org.traccar.handler.MicrometerMetricsHandler;
import org.traccar.handler.MessageBrokerHandler;

import jakarta.inject.Inject;

/**
 * Protocol implementation for TechtoCruz GPS devices.
 * This implementation supports both direct TCP communication and service-based
 * communication through the Protocol Service in the microservices architecture.
 */
public class TechtoCruzProtocol extends BaseProtocol {

    private static final Logger LOGGER = LoggerFactory.getLogger(TechtoCruzProtocol.class);
    
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final OpenTelemetryManager openTelemetryManager;
    private final MicrometerMetricsManager metricsManager;
    private final Timer messageProcessingTimer;
    private final Counter messagesReceivedCounter;
    
    /**
     * Initialize the protocol with required dependencies.
     *
     * @param config The system configuration
     * @param serviceDiscoveryManager Service discovery for protocol service location
     * @param openTelemetryManager Distributed tracing manager
     * @param metricsManager Metrics collection manager
     */
    @Inject
    public TechtoCruzProtocol(
            Config config,
            ServiceDiscoveryManager serviceDiscoveryManager,
            OpenTelemetryManager openTelemetryManager,
            MicrometerMetricsManager metricsManager) {
        
        super(config);
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.openTelemetryManager = openTelemetryManager;
        this.metricsManager = metricsManager;
        
        // Initialize protocol-specific metrics
        this.messageProcessingTimer = metricsManager.createTimer(
                "traccar.protocol.message.processing",
                "Time taken to process TechtoCruz protocol messages",
                io.micrometer.core.instrument.Tag.of("protocol", "techtocruz"));
        
        this.messagesReceivedCounter = metricsManager.createCounter(
                "traccar.protocol.messages.received",
                "Number of TechtoCruz protocol messages received",
                io.micrometer.core.instrument.Tag.of("protocol", "techtocruz"));
        
        // Check if we should use direct communication or service-based communication
        boolean directCommunication = config.getBoolean(ProtocolKeys.PROTOCOL_DIRECT_COMMUNICATION.getKey(), true);
        
        if (directCommunication) {
            // Direct TCP communication mode
            addServer(createDirectServer(config));
        } else {
            // Service-based communication mode - no servers added here
            // The Protocol Service will handle the connections
            LOGGER.info("TechtoCruz protocol configured for service-based communication");
        }
    }
    
    /**
     * Creates a direct TCP server for the protocol.
     *
     * @param config The system configuration
     * @return The configured tracker server
     */
    private TrackerServer createDirectServer(Config config) {
        return new TrackerServer(config, getName(), false) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                // Add frame decoder
                pipeline.addLast(new TechtoCruzFrameDecoder());
                
                // Add string encoder/decoder
                pipeline.addLast(new StringEncoder());
                pipeline.addLast(new StringDecoder());
                
                // Add distributed tracing handler if enabled
                if (config.getBoolean(OpenTelemetryManager.Keys.TELEMETRY_ENABLE.getKey(), false)) {
                    pipeline.addLast(new OpenTelemetryHandler(
                            openTelemetryManager, 
                            "techtocruz", 
                            SpanKind.SERVER));
                }
                
                // Add metrics collection handler
                pipeline.addLast(new MicrometerMetricsHandler(
                        metricsManager,
                        messagesReceivedCounter,
                        messageProcessingTimer));
                
                // Add protocol decoder
                TechtoCruzProtocolDecoder protocolDecoder = 
                        new TechtoCruzProtocolDecoder(TechtoCruzProtocol.this);
                pipeline.addLast(protocolDecoder);
                
                // Add message broker handler for asynchronous position publishing if enabled
                if (config.getBoolean(ProtocolKeys.BROKER_ENABLED.getKey(), false)) {
                    pipeline.addLast(new MessageBrokerHandler(config, getName()));
                }
            }
        };
    }
}