/*
 * Copyright 2019 - 2020 Anton Tananaev (anton@traccar.org)
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
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.traccar.BaseProtocolDecoder;
import org.traccar.Protocol;
import org.traccar.config.Config;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.helper.BitUtil;
import org.traccar.helper.UnitsConverter;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHeaders;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Position;
import org.traccar.session.DeviceSession;

import java.net.SocketAddress;
import java.util.BitSet;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RadarProtocolDecoder extends BaseProtocolDecoder {

    private static final AttributeKey<String> ATTR_DEVICE_ID = AttributeKey.stringKey("device.id");
    private static final AttributeKey<String> ATTR_PROTOCOL = AttributeKey.stringKey("protocol.name");
    private static final AttributeKey<Long> ATTR_POSITION_COUNT = AttributeKey.longKey("position.count");
    
    private final ServiceDiscovery serviceDiscovery;
    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Config config;
    
    private final Timer decodeTimer;
    private final Counter messageCounter;
    private final Counter positionCounter;
    private final Counter errorCounter;
    
    private final boolean serviceMode;

    public RadarProtocolDecoder(Protocol protocol, ServiceDiscovery serviceDiscovery, 
                              MessageProducer messageProducer, Tracer tracer, 
                              MeterRegistry meterRegistry, Config config) {
        super(protocol);
        this.serviceDiscovery = serviceDiscovery;
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.config = config;
        
        // Initialize metrics
        this.decodeTimer = Timer.builder("protocol.radar.decode.time")
                .description("Time taken to decode Radar protocol messages")
                .tag("protocol", "radar")
                .register(meterRegistry);
        
        this.messageCounter = Counter.builder("protocol.radar.messages")
                .description("Number of Radar protocol messages received")
                .tag("protocol", "radar")
                .register(meterRegistry);
        
        this.positionCounter = Counter.builder("protocol.radar.positions")
                .description("Number of positions decoded from Radar protocol messages")
                .tag("protocol", "radar")
                .register(meterRegistry);
        
        this.errorCounter = Counter.builder("protocol.radar.errors")
                .description("Number of errors during Radar protocol decoding")
                .tag("protocol", "radar")
                .register(meterRegistry);
        
        // Determine if we're running in service mode (microservices) or direct mode (monolith)
        this.serviceMode = config.getBoolean("protocol.serviceMode", false);
    }

    public static final int MSG_TRACKING = 0x4C;

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {
        
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("radar.decode")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        // Increment message counter
        messageCounter.increment();
        
        // Use the timer to measure decode time
        return decodeTimer.record(() -> {
            try (Scope scope = span.makeCurrent()) {
                // Actual decoding logic
                return decodeMessage(channel, remoteAddress, msg, span);
            } catch (Exception e) {
                // Record error in span and metrics
                span.recordException(e);
                span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR);
                errorCounter.increment();
                throw e;
            } finally {
                span.end();
            }
        });
    }
    
    private Object decodeMessage(
            Channel channel, SocketAddress remoteAddress, Object msg, Span span) throws Exception {

        ByteBuf buf = (ByteBuf) msg;

        buf.readUnsignedByte(); // product id
        buf.readUnsignedByte(); // product version

        String serialNumber = String.valueOf(buf.readUnsignedInt());
        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, serialNumber);
        if (deviceSession == null) {
            return null;
        }
        
        // Add device ID to span
        span.setAttribute(ATTR_DEVICE_ID, String.valueOf(deviceSession.getDeviceId()));
        span.setAttribute(ATTR_PROTOCOL, getProtocolName());

        buf.readUnsignedByte(); // index
        buf.readUnsignedInt(); // timestamp
        int type = buf.readUnsignedByte();
        buf.readUnsignedShort(); // length

        if (type == MSG_TRACKING) {

            buf.readUnsignedShort(); // memory count
            buf.readUnsignedShort(); // memory index
            int count = buf.readUnsignedShort();
            buf.readUnsignedShort(); // first index
            
            // Add position count to span
            span.setAttribute(ATTR_POSITION_COUNT, (long) count);

            List<Position> positions = new LinkedList<>();

            for (int index = 0; index < count; index++) {
                // Create a child span for each position
                Span positionSpan = tracer.spanBuilder("radar.position")
                        .setParent(Context.current())
                        .startSpan();
                
                try (Scope positionScope = positionSpan.makeCurrent()) {
                    Position position = new Position(getProtocolName());
                    position.setDeviceId(deviceSession.getDeviceId());

                    position.set(Position.KEY_EVENT, buf.readUnsignedShort());

                    int maskLength = buf.readUnsignedByte();
                    BitSet mask = BitSet.valueOf(buf.nioBuffer(buf.readerIndex(), maskLength));
                    buf.skipBytes(maskLength);

                    buf.readUnsignedShort(); // length

                    if (mask.get(0)) {
                        position.setDeviceTime(new Date(buf.readUnsignedInt() * 1000));
                    }
                    if (mask.get(1)) {
                        position.setFixTime(new Date(buf.readUnsignedInt() * 1000));
                    }
                    if (mask.get(2)) {
                        position.setLatitude(buf.readInt() / 360000.0);
                    }
                    if (mask.get(3)) {
                        position.setLongitude(buf.readInt() / 360000.0);
                    }
                    if (mask.get(4)) {
                        position.setSpeed(UnitsConverter.knotsFromKph(buf.readUnsignedShort()));
                    }
                    if (mask.get(5)) {
                        position.setCourse(buf.readUnsignedShort() * 0.1);
                    }
                    if (mask.get(6)) {
                        position.setAltitude(buf.readShort());
                    }
                    if (mask.get(7)) {
                        int flags = buf.readUnsignedByte();
                        position.setValid(BitUtil.check(flags, 0));
                        position.set(Position.KEY_SATELLITES, BitUtil.from(flags, 4));
                    }
                    if (mask.get(8)) {
                        long flags = buf.readUnsignedInt();
                        position.set(Position.KEY_IGNITION, BitUtil.check(flags, 0));
                        position.set(Position.KEY_CHARGE, BitUtil.check(flags, 1));
                        position.set(Position.KEY_MOTION, BitUtil.check(flags, 2));
                        for (int i = 0; i < 3; i++) {
                            position.set(Position.PREFIX_IN + i, BitUtil.check(flags, 4 + i));
                        }
                    }
                    if (mask.get(9)) {
                        int flags = buf.readUnsignedShort();
                        position.set(Position.KEY_BLOCKED, BitUtil.check(flags, 0));
                        position.set(Position.PREFIX_IN + 0, BitUtil.check(flags, 4));
                    }
                    for (int i = 10; i <= 14; i++) {
                        if (mask.get(i)) {
                            buf.readUnsignedShort(); // reserved
                        }
                    }
                    if (mask.get(15)) {
                        position.set(Position.KEY_ODOMETER, buf.readUnsignedInt() * 100);
                    }
                    if (mask.get(16)) {
                        buf.readUnsignedInt(); // reserved
                    }
                    for (int i = 17; i <= 27; i++) {
                        if (mask.get(i)) {
                            buf.readUnsignedByte(); // reserved
                        }
                    }
                    for (int i = 28; i <= 37; i += 2) {
                        if (mask.get(i)) {
                            buf.skipBytes(12); // reserved
                        }
                        if (mask.get(i + 1)) {
                            buf.readUnsignedByte(); // reserved
                        }
                    }
                    if (mask.get(38)) {
                        buf.skipBytes(6); // driver id
                    }
                    if (mask.get(39)) {
                        buf.readUnsignedShort(); // hardware problems
                    }
                    if (mask.get(40)) {
                        buf.readShort(); // acceleration x
                    }
                    if (mask.get(41)) {
                        buf.readShort(); // acceleration y
                    }
                    if (mask.get(42)) {
                        buf.readShort(); // acceleration z
                    }
                    if (mask.get(43)) {
                        buf.skipBytes(10); // operator
                    }
                    if (mask.get(44)) {
                        buf.readUnsignedShort(); // power
                    }
                    for (int i = 45; i <= 49; i++) {
                        if (mask.get(i)) {
                            buf.readUnsignedByte(); // reserved
                        }
                    }
                    if (mask.get(50)) {
                        buf.readShort(); // tilt
                    }
                    if (mask.get(51)) {
                        buf.readUnsignedInt(); // partial hours
                    }
                    if (mask.get(52)) {
                        buf.readUnsignedInt(); // accumulated hours
                    }

                    if (position.getDeviceTime() != null && position.getFixTime() != null) {
                        // Add position attributes to span
                        positionSpan.setAttribute("position.latitude", position.getLatitude());
                        positionSpan.setAttribute("position.longitude", position.getLongitude());
                        positionSpan.setAttribute("position.valid", position.getValid());
                        
                        positions.add(position);
                    }
                } finally {
                    positionSpan.end();
                }
            }
            
            // Update position counter
            positionCounter.increment(positions.size());
            
            // If we're in service mode, publish positions to the message broker
            if (serviceMode && !positions.isEmpty()) {
                publishPositions(positions, span);
            }

            // ACK 0x9C

            return positions.isEmpty() ? null : positions;

        }

        return null;
    }
    
    private void publishPositions(List<Position> positions, Span parentSpan) {
        // Create a child span for publishing
        Span publishSpan = tracer.spanBuilder("radar.publish")
                .setParent(Context.current())
                .startSpan();
        
        try (Scope publishScope = publishSpan.makeCurrent()) {
            publishSpan.setAttribute("position.count", positions.size());
            
            for (Position position : positions) {
                // Create message headers with trace context for distributed tracing
                MessageHeaders headers = new MessageHeaders.Builder()
                        .withHeader("device.id", String.valueOf(position.getDeviceId()))
                        .withHeader("protocol.name", getProtocolName())
                        .build();
                
                // Create message envelope
                MessageEnvelope<Position> envelope = new MessageEnvelope<>(position, headers);
                
                // Publish to the message broker
                messageProducer.send("positions", envelope);
            }
        } catch (Exception e) {
            publishSpan.recordException(e);
            publishSpan.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR);
            errorCounter.increment();
        } finally {
            publishSpan.end();
        }
    }
}