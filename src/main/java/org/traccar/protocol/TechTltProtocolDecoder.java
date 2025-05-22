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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

import org.traccar.BaseProtocolDecoder;
import org.traccar.session.DeviceSession;
import org.traccar.Protocol;
import org.traccar.helper.Parser;
import org.traccar.helper.PatternBuilder;
import org.traccar.helper.UnitsConverter;
import org.traccar.model.CellTower;
import org.traccar.model.Network;
import org.traccar.model.Position;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.MessageProducer;
import org.traccar.discovery.ServiceDiscoveryManager;

import java.net.SocketAddress;
import java.util.regex.Pattern;

public class TechTltProtocolDecoder extends BaseProtocolDecoder {

    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final boolean directCommunication;
    
    private final Counter messageCounter;
    private final Counter statusMessageCounter;
    private final Counter locationMessageCounter;
    private final Timer decodeTimer;

    public TechTltProtocolDecoder(Protocol protocol) {
        super(protocol);
        
        Config config = getConfig();
        this.directCommunication = config.getBoolean(Keys.PROTOCOL_DIRECT_COMMUNICATION.withPrefix(protocol.getName()));
        
        // Initialize service discovery manager
        this.serviceDiscoveryManager = ServiceDiscoveryManager.getInstance();
        
        // Initialize message producer for broker communication
        this.messageProducer = serviceDiscoveryManager.getMessageProducer();
        
        // Initialize distributed tracing
        this.tracer = serviceDiscoveryManager.getTracer();
        
        // Initialize metrics collection
        this.meterRegistry = serviceDiscoveryManager.getMeterRegistry();
        this.messageCounter = meterRegistry.counter("protocol.techtlt.messages");
        this.statusMessageCounter = meterRegistry.counter("protocol.techtlt.messages.status");
        this.locationMessageCounter = meterRegistry.counter("protocol.techtlt.messages.location");
        this.decodeTimer = meterRegistry.timer("protocol.techtlt.decode.time");
    }

    private static final Pattern PATTERN_STATUS = new PatternBuilder()
            .number("(d+),")                     // id
            .text("INFOGPRS,")
            .number("V Bat=(d+.d),")             // battery
            .number("TEMP=(d+),")                // temperature
            .expression("[^,]*,")
            .number("(d+)")                      // rssi
            .compile();

    private static final Pattern PATTERN_POSITION = new PatternBuilder()
            .number("(d+)")                      // id
            .text("*POS=Y,")
            .number("(dd):(dd):(dd),")           // time
            .number("(dd)/(dd)/(dd),")           // date
            .number("(dd)(dd.d+)")               // latitude
            .expression("([NS]),")
            .number("(ddd)(dd.d+)")              // longitude
            .expression("([EW]),")
            .number("(d+.d+),")                  // speed
            .number("(d+.d+),")                  // course
            .number("(d+.d+),")                  // altitude
            .number("(d+),")                     // satellites
            .number("(d+),")                     // lac
            .number("(d+)")                      // cid
            .compile();

    private Position decodeStatus(Channel channel, SocketAddress remoteAddress, String sentence) {
        Span span = tracer.spanBuilder("TechTlt.decodeStatus").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("protocol.message", sentence);
            statusMessageCounter.increment();
            
            Parser parser = new Parser(PATTERN_STATUS, sentence);
            if (!parser.matches()) {
                span.setAttribute("protocol.parse.success", false);
                return null;
            }

            DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, parser.next());
            if (deviceSession == null) {
                span.setAttribute("protocol.session.found", false);
                return null;
            }
            span.setAttribute("device.id", deviceSession.getDeviceId());

            Position position = new Position(getProtocolName());
            position.setDeviceId(deviceSession.getDeviceId());

            getLastLocation(position, null);

            position.set(Position.KEY_BATTERY, parser.nextDouble());
            position.set(Position.KEY_DEVICE_TEMP, parser.nextInt());
            position.set(Position.KEY_RSSI, parser.nextInt());
            
            span.setAttribute("protocol.parse.success", true);
            
            // Publish position to message broker if not using direct communication
            if (!directCommunication && messageProducer != null) {
                messageProducer.sendPosition(position);
            }
            
            return position;
        } finally {
            span.end();
        }
    }

    private Position decodeLocation(Channel channel, SocketAddress remoteAddress, String sentence) {
        Span span = tracer.spanBuilder("TechTlt.decodeLocation").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("protocol.message", sentence);
            locationMessageCounter.increment();
            
            Parser parser = new Parser(PATTERN_POSITION, sentence);
            if (!parser.matches()) {
                span.setAttribute("protocol.parse.success", false);
                return null;
            }

            DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, parser.next());
            if (deviceSession == null) {
                span.setAttribute("protocol.session.found", false);
                return null;
            }
            span.setAttribute("device.id", deviceSession.getDeviceId());

            Position position = new Position(getProtocolName());
            position.setDeviceId(deviceSession.getDeviceId());

            position.setValid(true);
            position.setTime(parser.nextDateTime(Parser.DateTimeFormat.HMS_DMY));
            position.setLatitude(parser.nextCoordinate());
            position.setLongitude(parser.nextCoordinate());
            position.setSpeed(UnitsConverter.knotsFromKph(parser.nextDouble()));
            position.setCourse(parser.nextDouble());
            position.setAltitude(parser.nextDouble());

            position.set(Position.KEY_SATELLITES, parser.nextInt());

            position.setNetwork(new Network(CellTower.fromLacCid(getConfig(), parser.nextInt(), parser.nextInt())));
            
            span.setAttribute("protocol.parse.success", true);
            span.setAttribute("position.latitude", position.getLatitude());
            span.setAttribute("position.longitude", position.getLongitude());
            
            // Publish position to message broker if not using direct communication
            if (!directCommunication && messageProducer != null) {
                messageProducer.sendPosition(position);
            }
            
            return position;
        } finally {
            span.end();
        }
    }

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {
        
        messageCounter.increment();
        return decodeTimer.record(() -> {
            String sentence = ((String) msg).trim();
            if (sentence.contains("INFO")) {
                return decodeStatus(channel, remoteAddress, sentence);
            } else if (sentence.contains("POS")) {
                return decodeLocation(channel, remoteAddress, sentence);
            } else {
                return null;
            }
        });
    }

}