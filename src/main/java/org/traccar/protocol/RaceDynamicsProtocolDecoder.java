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

import io.netty.channel.Channel;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.traccar.BaseProtocolDecoder;
import org.traccar.session.DeviceSession;
import org.traccar.NetworkMessage;
import org.traccar.Protocol;
import org.traccar.helper.DateBuilder;
import org.traccar.helper.Parser;
import org.traccar.helper.PatternBuilder;
import org.traccar.model.Position;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.MessagePublisher;
import org.traccar.discovery.ServiceDiscoveryManager;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.net.SocketAddress;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Protocol decoder for RaceDynamics GPS devices.
 * This implementation supports both direct decoding and service-based communication
 * with message broker integration for asynchronous position publishing.
 */
public class RaceDynamicsProtocolDecoder extends BaseProtocolDecoder {

    // Message types
    public static final int MSG_LOGIN = 12;
    public static final int MSG_LOCATION = 15;

    // Pattern for login messages
    private static final Pattern PATTERN_LOGIN = new PatternBuilder()
            .text("$GPRMC,")
            .number("d+,")                       // type
            .number("d{6},")                     // date
            .number("d{6},")                     // time
            .number("(d{15}),")
            .compile();

    // Pattern for location messages
    private static final Pattern PATTERN_LOCATION = new PatternBuilder()
            .number("(dd)(dd)(dd),")             // time (hhmmss)
            .expression("([AV]),")               // validity
            .number("(dd)(dd.d+),")              // latitude
            .expression("([NS]),")
            .number("(ddd)(dd.d+),")             // longitude
            .expression("([EW]),")
            .number("(d+),")                     // speed
            .number("(dd)(dd)(dd),")             // date (ddmmyy)
            .number("(-?d+),")                   // altitude
            .number("(d+),")                     // satellites
            .number("([01]),")                   // ignition
            .number("(d+),")                     // index
            .text("%,")
            .number("([^,]+),")                  // ibutton
            .number("d+,")                       // acceleration
            .number("d+,")                       // deceleration
            .number("[01],")                     // cruise control
            .number("[01],")                     // seat belt
            .number("[01],")                     // wrong ibutton
            .number("(d+),")                     // power
            .number("[01],")                     // power status
            .number("(d+),")                     // battery
            .number("([01]),")                   // panic
            .number("d+,")
            .number("d+,")
            .number("(d),")                      // overspeed
            .number("d+,")                       // speed limit
            .number("d+,")                       // tachometer
            .number("d+,d+,d+,")                 // aux
            .number("d+,")                       // geofence id
            .number("d+,")                       // road speed type
            .number("d+,")                       // ibutton count
            .number("(d),")                      // overdriver alert
            .any()
            .compile();

    private String imei;
    
    // Service discovery manager for protocol service discovery
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    
    // Message publisher for asynchronous position publishing
    private final MessagePublisher messagePublisher;
    
    // OpenTelemetry tracer for distributed tracing
    private final Tracer tracer;
    
    // Metrics for protocol performance monitoring
    private final Counter messageCounter;
    private final Counter positionCounter;
    private final Timer decodeTimer;
    
    // Configuration for containerized environments
    private final boolean asyncPublishing;

    /**
     * Constructor for the RaceDynamics protocol decoder.
     * 
     * @param protocol The protocol instance
     * @param serviceDiscoveryManager Service discovery manager for protocol service discovery
     * @param messagePublisher Message publisher for asynchronous position publishing
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry for protocol performance monitoring
     * @param config Configuration for containerized environments
     */
    public RaceDynamicsProtocolDecoder(Protocol protocol, 
                                      ServiceDiscoveryManager serviceDiscoveryManager,
                                      MessagePublisher messagePublisher,
                                      Tracer tracer,
                                      MeterRegistry meterRegistry,
                                      Config config) {
        super(protocol);
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.messagePublisher = messagePublisher;
        this.tracer = tracer;
        
        // Initialize metrics
        this.messageCounter = meterRegistry.counter("protocol.racedynamics.messages");
        this.positionCounter = meterRegistry.counter("protocol.racedynamics.positions");
        this.decodeTimer = meterRegistry.timer("protocol.racedynamics.decode.time");
        
        // Get configuration for containerized environments
        this.asyncPublishing = config.getBoolean(Keys.PROTOCOL_ASYNC_PUBLISHING.withPrefix("racedynamics"), true);
    }
    
    /**
     * Legacy constructor for backward compatibility.
     * 
     * @param protocol The protocol instance
     */
    public RaceDynamicsProtocolDecoder(Protocol protocol) {
        super(protocol);
        this.serviceDiscoveryManager = null;
        this.messagePublisher = null;
        this.tracer = null;
        this.messageCounter = null;
        this.positionCounter = null;
        this.decodeTimer = null;
        this.asyncPublishing = false;
    }

    /**
     * Sends a response to the device.
     * 
     * @param channel The channel to send the response to
     * @param remoteAddress The remote address to send the response to
     * @param type The message type
     */
    private void sendResponse(Channel channel, SocketAddress remoteAddress, int type) {
        if (channel != null) {
            String response = String.format(
                    "$GPRMC,%1$d,%2$td%2$tm%2$ty,%2$tH%2$tM%2$tS,%3$s,\r\n", type, new Date(), imei);
            channel.writeAndFlush(new NetworkMessage(response, remoteAddress));
        }
    }

    /**
     * Publishes positions to the message broker.
     * 
     * @param positions The positions to publish
     * @param deviceId The device ID
     * @param spanContext The span context for distributed tracing
     */
    private void publishPositions(List<Position> positions, long deviceId, Span span) {
        if (messagePublisher != null && positions != null && !positions.isEmpty()) {
            for (Position position : positions) {
                // Add trace context to position attributes
                if (span != null) {
                    position.set("trace.id", span.getSpanContext().getTraceId());
                    position.set("span.id", span.getSpanContext().getSpanId());
                }
                
                // Publish position to message broker
                messagePublisher.publishPosition(position);
            }
        }
    }

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {
        
        // Start timing the decode operation
        long startTime = System.nanoTime();
        
        // Increment message counter
        if (messageCounter != null) {
            messageCounter.increment();
        }
        
        // Create a span for distributed tracing
        Span span = null;
        Scope scope = null;
        if (tracer != null) {
            span = tracer.spanBuilder("racedynamics.decode")
                    .setAttribute("protocol", getProtocolName())
                    .setAttribute("remote.address", remoteAddress.toString())
                    .startSpan();
            scope = span.makeCurrent();
        }
        
        try {
            String sentence = (String) msg;
            
            if (span != null) {
                span.setAttribute("message.length", sentence.length());
            }

            int type = Integer.parseInt(sentence.substring(7, 9));
            
            if (span != null) {
                span.setAttribute("message.type", type);
            }

            if (type == MSG_LOGIN) {
                // Handle login message
                Parser parser = new Parser(PATTERN_LOGIN, sentence);
                if (parser.matches()) {
                    imei = parser.next();
                    DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, imei);
                    
                    if (span != null && deviceSession != null) {
                        span.setAttribute("device.id", deviceSession.getDeviceId());
                    }
                    
                    sendResponse(channel, remoteAddress, type);
                }

            } else if (type == MSG_LOCATION) {
                // Handle location message
                DeviceSession deviceSession = getDeviceSession(channel, remoteAddress);
                if (deviceSession == null) {
                    return null;
                }
                
                if (span != null) {
                    span.setAttribute("device.id", deviceSession.getDeviceId());
                }

                List<Position> positions = new LinkedList<>();

                for (String data : sentence.substring(17, sentence.length() - 3).split(",#,#,")) {
                    Parser parser = new Parser(PATTERN_LOCATION, data);
                    if (parser.matches()) {

                        Position position = new Position(getProtocolName());
                        position.setDeviceId(deviceSession.getDeviceId());

                        DateBuilder dateBuilder = new DateBuilder()
                                .setTime(parser.nextInt(), parser.nextInt(), parser.nextInt());

                        position.setValid(parser.next().equals("A"));
                        position.setLatitude(parser.nextCoordinate());
                        position.setLongitude(parser.nextCoordinate());
                        position.setSpeed(parser.nextDouble());

                        dateBuilder.setDateReverse(parser.nextInt(), parser.nextInt(), parser.nextInt());
                        position.setTime(dateBuilder.getDate());

                        position.setAltitude(parser.nextInt());
                        position.set(Position.KEY_SATELLITES, parser.nextInt());
                        position.set(Position.KEY_IGNITION, parser.nextInt() == 1);
                        position.set(Position.KEY_INDEX, parser.nextInt());
                        position.set(Position.KEY_DRIVER_UNIQUE_ID, parser.next());
                        position.set(Position.KEY_POWER, parser.nextInt() * 0.01);
                        position.set(Position.KEY_BATTERY, parser.nextInt() * 0.01);
                        position.addAlarm(parser.nextInt() > 0 ? Position.ALARM_SOS : null);
                        position.addAlarm(parser.nextInt() > 0 ? Position.ALARM_OVERSPEED : null);

                        int overDriver = parser.nextInt();
                        if (overDriver > 0) {
                            position.set("overDriver", overDriver);
                        }

                        positions.add(position);
                    }
                }

                sendResponse(channel, remoteAddress, type);
                
                // Increment position counter
                if (positionCounter != null && !positions.isEmpty()) {
                    positionCounter.increment(positions.size());
                }
                
                // Publish positions to message broker if async publishing is enabled
                if (asyncPublishing && !positions.isEmpty()) {
                    publishPositions(positions, deviceSession.getDeviceId(), span);
                    return null; // Return null to indicate async handling
                }
                
                return positions;
            }
        } finally {
            // Record decode time
            if (decodeTimer != null) {
                decodeTimer.record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
            }
            
            // Close the tracing span
            if (scope != null) {
                scope.close();
            }
            if (span != null) {
                span.end();
            }
        }

        return null;
    }
}