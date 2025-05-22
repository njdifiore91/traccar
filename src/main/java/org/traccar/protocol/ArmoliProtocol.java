/*
 * Copyright 2022 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.api.metrics.Meter;
import org.traccar.BaseProtocol;
import org.traccar.CharacterDelimiterFrameDecoder;
import org.traccar.PipelineBuilder;
import org.traccar.TrackerServer;
import org.traccar.config.Config;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.messaging.MessageBrokerClient;
import org.traccar.metrics.MetricsCollector;

import jakarta.inject.Inject;

/**
 * Protocol implementation for Armoli GPS tracking devices.
 */
public class ArmoliProtocol extends BaseProtocol {

    private final Tracer tracer;
    private final Meter meter;
    private final MetricsCollector metricsCollector;

    /**
     * Initialize Armoli protocol with required dependencies.
     *
     * @param config Configuration
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meter OpenTelemetry meter for metrics collection
     * @param metricsCollector Metrics collector for performance monitoring
     * @param serviceDiscoveryManager Service discovery for protocol service registration
     * @param messageBrokerClient Message broker for asynchronous position publishing
     */
    @Inject
    public ArmoliProtocol(
            Config config,
            Tracer tracer,
            Meter meter,
            MetricsCollector metricsCollector,
            ServiceDiscoveryManager serviceDiscoveryManager,
            MessageBrokerClient messageBrokerClient) {
        
        this.tracer = tracer;
        this.meter = meter;
        this.metricsCollector = metricsCollector;
        
        // Register protocol with service discovery
        setServiceDiscoveryManager(serviceDiscoveryManager);
        
        // Set message broker for asynchronous position publishing
        setMessageBrokerClient(messageBrokerClient);
        
        // Register protocol metrics
        registerMetrics();
        
        // Add TCP server
        addServer(new TrackerServer(config, getName(), false) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                pipeline.addLast(new CharacterDelimiterFrameDecoder(1024, ";;" , ";\r", ";"));
                pipeline.addLast(new StringEncoder());
                pipeline.addLast(new StringDecoder());
                pipeline.addLast(new ArmoliProtocolDecoder(ArmoliProtocol.this));
                pipeline.addLast(new ArmoliProtocolPoller(ArmoliProtocol.this));
            }
        });
    }
    
    /**
     * Register protocol-specific metrics for performance monitoring.
     */
    private void registerMetrics() {
        if (metricsCollector != null) {
            String metricPrefix = "protocol.armoli.";
            
            // Register counters
            metricsCollector.registerCounter(
                    metricPrefix + "messages.received", 
                    "Number of messages received from Armoli devices");
            
            metricsCollector.registerCounter(
                    metricPrefix + "messages.parsed", 
                    "Number of messages successfully parsed");
            
            metricsCollector.registerCounter(
                    metricPrefix + "messages.dropped", 
                    "Number of messages dropped due to parsing errors");
            
            metricsCollector.registerCounter(
                    metricPrefix + "positions.published", 
                    "Number of positions published to message broker");
            
            // Register gauges
            metricsCollector.registerGauge(
                    metricPrefix + "active.connections", 
                    "Number of active Armoli device connections");
            
            // Register histograms for latency tracking
            metricsCollector.registerHistogram(
                    metricPrefix + "message.parse.time", 
                    "Time taken to parse Armoli messages in milliseconds");
            
            metricsCollector.registerHistogram(
                    metricPrefix + "position.publish.time", 
                    "Time taken to publish positions to message broker in milliseconds");
        }
    }
}