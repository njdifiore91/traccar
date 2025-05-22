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

import io.netty.channel.Channel;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.traccar.BaseProtocolDecoder;
import org.traccar.session.DeviceSession;
import org.traccar.Protocol;
import org.traccar.helper.UnitsConverter;
import org.traccar.model.Position;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.messaging.MessageProducer;
import org.traccar.metrics.MetricsManager;

import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * Protocol decoder for Genx devices.
 * Updated to support microservices architecture with service discovery,
 * message broker integration, distributed tracing, and metrics collection.
 */
public class GenxProtocolDecoder extends BaseProtocolDecoder {

    private int[] reportColumns;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final MetricsManager metricsManager;
    
    // Metrics counters
    private final String METRIC_MESSAGES_RECEIVED = "protocol.genx.messages.received";
    private final String METRIC_MESSAGES_PROCESSED = "protocol.genx.messages.processed";
    private final String METRIC_PROCESSING_TIME = "protocol.genx.processing.time";
    private final String METRIC_MESSAGES_PUBLISHED = "protocol.genx.messages.published";

    /**
     * Constructs the Genx protocol decoder with service discovery and messaging capabilities.
     *
     * @param protocol The protocol instance
     * @param serviceDiscoveryManager Service discovery manager for protocol service discovery
     * @param messageProducer Message producer for asynchronous position publishing
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param metricsManager Metrics manager for performance monitoring
     */
    public GenxProtocolDecoder(Protocol protocol, 
                              ServiceDiscoveryManager serviceDiscoveryManager,
                              MessageProducer messageProducer,
                              Tracer tracer,
                              MetricsManager metricsManager) {
        super(protocol);
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.metricsManager = metricsManager;
    }
    
    /**
     * Legacy constructor for backward compatibility.
     * 
     * @param protocol The protocol instance
     */
    public GenxProtocolDecoder(Protocol protocol) {
        this(protocol, null, null, null, null);
    }

    @Override
    protected void init() {
        // Get configuration from environment variables first, then fall back to config file
        String reportColumnsConfig = System.getenv("GENX_REPORT_COLUMNS");
        if (reportColumnsConfig == null || reportColumnsConfig.isEmpty()) {
            reportColumnsConfig = getConfig().getString(getProtocolName() + ".reportColumns", "1,2,3,4");
        }
        setReportColumns(reportColumnsConfig);
    }

    public void setReportColumns(String format) {
        String[] columns = format.split(",");
        reportColumns = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            reportColumns[i] = Integer.parseInt(columns[i]);
        }
    }
    
    /**
     * Determines if direct processing should be used instead of service-based processing.
     * 
     * @return true if direct processing should be used, false for service-based processing
     */
    private boolean useDirectProcessing() {
        // Use direct processing if service discovery is not available or if configured to do so
        return serviceDiscoveryManager == null || 
               !serviceDiscoveryManager.isServiceAvailable("position-service") ||
               Boolean.parseBoolean(System.getenv().getOrDefault("PROTOCOL_DIRECT_PROCESSING", "false"));
    }
    
    /**
     * Publishes a position to the message broker for asynchronous processing.
     * 
     * @param position The position to publish
     * @param span The current tracing span for context propagation
     * @return true if successfully published, false otherwise
     */
    private boolean publishPosition(Position position, Span span) {
        if (messageProducer == null) {
            return false;
        }
        
        try {
            // Add trace context to the message for distributed tracing
            messageProducer.publishPosition(position, span);
            
            // Record metrics for successful publishing
            if (metricsManager != null) {
                metricsManager.incrementCounter(METRIC_MESSAGES_PUBLISHED);
            }
            return true;
        } catch (Exception e) {
            if (span != null) {
                span.recordException(e);
            }
            return false;
        }
    }

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {
        
        // Start tracing span for this decode operation
        Span span = null;
        Scope scope = null;
        long startTime = System.nanoTime();
        
        try {
            // Initialize tracing if available
            if (tracer != null) {
                span = tracer.spanBuilder("genx.decode").startSpan();
                scope = span.makeCurrent();
                span.setAttribute("protocol", "genx");
                span.setAttribute("remote.address", remoteAddress.toString());
            }
            
            // Record metrics for received message
            if (metricsManager != null) {
                metricsManager.incrementCounter(METRIC_MESSAGES_RECEIVED);
            }

            String[] values = ((String) msg).split(",");
            
            if (span != null) {
                span.setAttribute("message.values.count", values.length);
            }

            Position position = new Position(getProtocolName());
            position.setValid(true);

            for (int i = 0; i < Math.min(values.length, reportColumns.length); i++) {
                switch (reportColumns[i]) {
                    case 1, 28 -> {
                        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, values[i]);
                        if (deviceSession != null) {
                            position.setDeviceId(deviceSession.getDeviceId());
                            if (span != null) {
                                span.setAttribute("device.id", deviceSession.getDeviceId());
                            }
                        }
                    }
                    case 2 -> position.setTime(new SimpleDateFormat("MM/dd/yy HH:mm:ss").parse(values[i]));
                    case 3 -> position.setLatitude(Double.parseDouble(values[i]));
                    case 4 -> position.setLongitude(Double.parseDouble(values[i]));
                    case 11 -> position.set(Position.KEY_IGNITION, values[i].equals("ON"));
                    case 13 -> position.setSpeed(UnitsConverter.knotsFromKph(Integer.parseInt(values[i])));
                    case 17 -> position.setCourse(Integer.parseInt(values[i]));
                    case 23 -> position.set(Position.KEY_ODOMETER, Double.parseDouble(values[i]) * 1000);
                    case 27 -> position.setAltitude(UnitsConverter.metersFromFeet(Integer.parseInt(values[i])));
                    case 46 -> position.set(Position.KEY_SATELLITES, Integer.parseInt(values[i]));
                }
            }

            // Check if we have a valid position
            if (position.getDeviceId() != 0) {
                // Record metrics for successful processing
                if (metricsManager != null) {
                    metricsManager.incrementCounter(METRIC_MESSAGES_PROCESSED);
                }
                
                // Add additional metadata for containerized environments
                position.set("protocol.service.instance", System.getenv().getOrDefault("HOSTNAME", "unknown"));
                
                // Determine whether to use direct processing or service-based processing
                if (!useDirectProcessing()) {
                    // Publish to message broker for asynchronous processing
                    boolean published = publishPosition(position, span);
                    if (span != null) {
                        span.setAttribute("message.published", published);
                    }
                    
                    // In service-based mode, we still return the position for backward compatibility,
                    // but the actual processing will happen asynchronously through the message broker
                }
                
                return position;
            } else {
                return null;
            }
        } finally {
            // Record processing time metric
            if (metricsManager != null) {
                long processingTime = System.nanoTime() - startTime;
                metricsManager.recordTimer(METRIC_PROCESSING_TIME, processingTime, TimeUnit.NANOSECONDS);
            }
            
            // Close tracing span and scope
            if (scope != null) {
                scope.close();
            }
            if (span != null) {
                span.end();
            }
        }
    }
}