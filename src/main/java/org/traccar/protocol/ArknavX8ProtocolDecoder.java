/*
 * Copyright 2016 - 2018 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.traccar.BaseProtocolDecoder;
import org.traccar.config.Config;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.session.DeviceSession;
import org.traccar.Protocol;
import org.traccar.helper.Parser;
import org.traccar.helper.PatternBuilder;
import org.traccar.model.Position;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ArknavX8ProtocolDecoder extends BaseProtocolDecoder {

    private final MessageProducer messageProducer;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final Tracer tracer;
    private final Meter meter;
    private final LongCounter messageCounter;
    private final LongCounter positionCounter;
    private final LongCounter errorCounter;
    private final Config config;
    private final boolean directMode;
    
    public ArknavX8ProtocolDecoder(Protocol protocol) {
        this(protocol, null, null, null, null, null);
    }
    
    public ArknavX8ProtocolDecoder(
            Protocol protocol, 
            MessageProducer messageProducer, 
            ServiceDiscoveryManager serviceDiscoveryManager,
            Tracer tracer,
            Meter meter,
            Config config) {
        super(protocol);
        this.messageProducer = messageProducer;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.tracer = tracer;
        this.meter = meter;
        this.config = config;
        this.directMode = messageProducer == null;
        
        // Initialize metrics if meter is provided
        if (meter != null) {
            this.messageCounter = meter.counterBuilder("arknavx8.messages")
                    .setDescription("Number of ArknavX8 messages received")
                    .build();
            this.positionCounter = meter.counterBuilder("arknavx8.positions")
                    .setDescription("Number of ArknavX8 positions decoded")
                    .build();
            this.errorCounter = meter.counterBuilder("arknavx8.errors")
                    .setDescription("Number of ArknavX8 decoding errors")
                    .build();
        } else {
            this.messageCounter = null;
            this.positionCounter = null;
            this.errorCounter = null;
        }
    }

    private static final Pattern PATTERN_1G = new PatternBuilder()
            .expression("(..),")                 // type
            .number("(dd)(dd)(dd)")              // date (yymmdd)
            .number("(dd)(dd)(dd),")             // time (hhmmss)
            .expression("([AV]),")               // validity
            .number("(d+)(dd.d+)([NS]),")        // latitude
            .number("(d+)(dd.d+)([EW]),")        // longitude
            .number("(d+.d+),")                  // speed
            .number("(d+),")                     // course
            .number("(d+.d+),")                  // hdop
            .number("(d+)")                      // status
            .compile();

    private static final Pattern PATTERN_2G = new PatternBuilder()
            .expression("..,")                   // type
            .number("(dd)(dd)(dd)")              // date (yymmdd)
            .number("(dd)(dd)(dd),")             // time (hhmmss)
            .number("(d+),")                     // satellites
            .number("(d+.d+),")                  // altitude
            .number("(d+.d+),")                  // power
            .number("(d+.d+),")                  // battery
            .number("(d+.d+)")                   // odometer
            .compile();

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {

        String sentence = (String) msg;
        Span span = null;
        
        // Start tracing if tracer is available
        if (tracer != null) {
            span = tracer.spanBuilder("ArknavX8.decode")
                    .setSpanKind(SpanKind.CONSUMER)
                    .setAttribute("protocol.type", "ArknavX8")
                    .setAttribute("message.size", sentence.length())
                    .startSpan();
            Context context = Context.current().with(span);
        }
        
        // Increment message counter if metrics are enabled
        if (messageCounter != null) {
            messageCounter.add(1);
        }
        
        try {
            if (sentence.charAt(2) != ',') {
                getDeviceSession(channel, remoteAddress, sentence.substring(0, 15));
                if (span != null) {
                    span.addEvent("Device identification");
                    span.end();
                }
                return null;
            }
            
            Position position = null;
            String messageType = sentence.substring(0, 2);
            
            // Add message type to span if tracing is enabled
            if (span != null) {
                span.setAttribute("message.type", messageType);
            }
            
            switch (messageType) {
                case "1G", "1R", "1M" -> position = decode1G(channel, remoteAddress, sentence);
                case "2G" -> position = decode2G(channel, remoteAddress, sentence);
                default -> {
                    if (errorCounter != null) {
                        errorCounter.add(1);
                    }
                    if (span != null) {
                        span.setStatus(StatusCode.ERROR, "Unknown message type");
                    }
                    return null;
                }
            }
            
            // Process the position based on mode (direct or service-based)
            if (position != null) {
                if (positionCounter != null) {
                    positionCounter.add(1);
                }
                
                if (span != null) {
                    span.setAttribute("position.valid", position.getValid());
                    span.setAttribute("position.latitude", position.getLatitude());
                    span.setAttribute("position.longitude", position.getLongitude());
                }
                
                // If in service-based mode, publish position to message broker
                if (!directMode && messageProducer != null) {
                    MessageEnvelope envelope = new MessageEnvelope(position, "position");
                    if (span != null) {
                        // Add trace context to message for distributed tracing
                        envelope.getHeaders().put("trace_id", span.getSpanContext().getTraceId());
                        envelope.getHeaders().put("span_id", span.getSpanContext().getSpanId());
                    }
                    messageProducer.send("positions", envelope);
                    
                    if (span != null) {
                        span.addEvent("Position published to broker");
                    }
                }
            } else if (errorCounter != null) {
                errorCounter.add(1);
                if (span != null) {
                    span.setStatus(StatusCode.ERROR, "Failed to decode position");
                }
            }
            
            if (span != null) {
                span.end();
            }
            
            return position;
        } catch (Exception e) {
            if (errorCounter != null) {
                errorCounter.add(1);
            }
            if (span != null) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.end();
            }
            throw e;
        }
    }

    private Position decode1G(Channel channel, SocketAddress remoteAddress, String sentence) {
        Span span = null;
        long startTime = System.nanoTime();
        
        // Create child span for 1G decoding if tracing is enabled
        if (tracer != null) {
            span = tracer.spanBuilder("ArknavX8.decode1G")
                    .setSpanKind(SpanKind.INTERNAL)
                    .startSpan();
        }
        
        try {
            Parser parser = new Parser(PATTERN_1G, sentence);
            if (!parser.matches()) {
                if (span != null) {
                    span.setStatus(StatusCode.ERROR, "Pattern matching failed");
                    span.end();
                }
                return null;
            }

            DeviceSession deviceSession = getDeviceSession(channel, remoteAddress);
            if (deviceSession == null) {
                if (span != null) {
                    span.setStatus(StatusCode.ERROR, "Device session not found");
                    span.end();
                }
                return null;
            }

            Position position = new Position(getProtocolName());
            position.setDeviceId(deviceSession.getDeviceId());

            String type = parser.next();
            position.set(Position.KEY_TYPE, type);
            
            if (span != null) {
                span.setAttribute("message.subtype", type);
            }

            position.setTime(parser.nextDateTime());

            position.setValid(parser.next().equals("A"));
            position.setLatitude(parser.nextCoordinate());
            position.setLongitude(parser.nextCoordinate());
            position.setSpeed(parser.nextDouble(0));
            position.setCourse(parser.nextDouble(0));

            position.set(Position.KEY_HDOP, parser.nextDouble(0));
            position.set(Position.KEY_STATUS, parser.next());
            
            // Add protocol-specific metadata for containerized environments
            if (config != null) {
                String containerId = config.getString("container.id");
                if (containerId != null && !containerId.isEmpty()) {
                    position.set("container.id", containerId);
                }
                String nodeId = config.getString("node.id");
                if (nodeId != null && !nodeId.isEmpty()) {
                    position.set("node.id", nodeId);
                }
            }
            
            if (span != null) {
                span.end();
            }
            
            // Record decode time if metrics are enabled
            if (meter != null) {
                long elapsedTime = System.nanoTime() - startTime;
                meter.gaugeBuilder("arknavx8.decode1G.time")
                        .setDescription("Time taken to decode 1G message")
                        .setUnit("ns")
                        .buildWithCallback(measurement -> measurement.record(elapsedTime));
            }

            return position;
        } catch (Exception e) {
            if (span != null) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.end();
            }
            throw e;
        }
    }

    private Position decode2G(Channel channel, SocketAddress remoteAddress, String sentence) {
        Span span = null;
        long startTime = System.nanoTime();
        
        // Create child span for 2G decoding if tracing is enabled
        if (tracer != null) {
            span = tracer.spanBuilder("ArknavX8.decode2G")
                    .setSpanKind(SpanKind.INTERNAL)
                    .startSpan();
        }
        
        try {
            Parser parser = new Parser(PATTERN_2G, sentence);
            if (!parser.matches()) {
                if (span != null) {
                    span.setStatus(StatusCode.ERROR, "Pattern matching failed");
                    span.end();
                }
                return null;
            }

            DeviceSession deviceSession = getDeviceSession(channel, remoteAddress);
            if (deviceSession == null) {
                if (span != null) {
                    span.setStatus(StatusCode.ERROR, "Device session not found");
                    span.end();
                }
                return null;
            }

            Position position = new Position(getProtocolName());
            position.setDeviceId(deviceSession.getDeviceId());
            
            if (span != null) {
                span.setAttribute("message.subtype", "2G");
                span.setAttribute("device.id", deviceSession.getDeviceId());
            }

            getLastLocation(position, parser.nextDateTime());

            int satellites = parser.nextInt();
            position.set(Position.KEY_SATELLITES, satellites);
            position.setAltitude(parser.nextDouble());
            position.set(Position.KEY_POWER, parser.nextDouble());
            position.set(Position.KEY_BATTERY, parser.nextDouble());
            position.set(Position.KEY_ODOMETER, parser.nextDouble() * 1852 / 3600);
            
            // Add protocol-specific metadata for containerized environments
            if (config != null) {
                String containerId = config.getString("container.id");
                if (containerId != null && !containerId.isEmpty()) {
                    position.set("container.id", containerId);
                }
                String nodeId = config.getString("node.id");
                if (nodeId != null && !nodeId.isEmpty()) {
                    position.set("node.id", nodeId);
                }
            }
            
            if (span != null) {
                span.setAttribute("satellites", satellites);
                span.end();
            }
            
            // Record decode time if metrics are enabled
            if (meter != null) {
                long elapsedTime = System.nanoTime() - startTime;
                meter.gaugeBuilder("arknavx8.decode2G.time")
                        .setDescription("Time taken to decode 2G message")
                        .setUnit("ns")
                        .buildWithCallback(measurement -> measurement.record(elapsedTime));
            }

            return position;
        } catch (Exception e) {
            if (span != null) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.end();
            }
            throw e;
        }
    }

}