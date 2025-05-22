/*
 * Copyright 2013 - 2025 Anton Tananaev (anton@traccar.org)
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.LongCounter;
import org.traccar.BaseProtocolDecoder;
import org.traccar.helper.BitUtil;
import org.traccar.session.DeviceSession;
import org.traccar.NetworkMessage;
import org.traccar.Protocol;
import org.traccar.helper.BcdUtil;
import org.traccar.helper.Checksum;
import org.traccar.helper.DateBuilder;
import org.traccar.helper.UnitsConverter;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHeaders;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.model.Position;

import javax.inject.Inject;
import javax.inject.Named;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

public class GatorProtocolDecoder extends BaseProtocolDecoder {

    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final Meter meter;
    private final LongCounter positionsCounter;
    private final LongCounter messagesCounter;
    private final ServiceDiscovery serviceDiscovery;
    private final boolean directCommunication;

    @Inject
    public GatorProtocolDecoder(
            Protocol protocol,
            @Named("positionProducer") MessageProducer messageProducer,
            Tracer tracer,
            Meter meter,
            ServiceDiscovery serviceDiscovery) {
        super(protocol);
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.meter = meter;
        this.serviceDiscovery = serviceDiscovery;
        
        // Initialize metrics
        this.positionsCounter = meter.counterBuilder("gator.positions.decoded")
                .setDescription("Number of positions decoded by the Gator protocol decoder")
                .build();
        this.messagesCounter = meter.counterBuilder("gator.messages.received")
                .setDescription("Number of messages received by the Gator protocol decoder")
                .build();
        
        // Check if we should use direct communication or service-based
        this.directCommunication = protocol.getConfig().getBoolean("protocol.gator.directCommunication", true);
    }

    public static final int MSG_HEARTBEAT = 0x21;
    public static final int MSG_POSITION_REQUEST = 0x30;
    public static final int MSG_OVERSPEED_ALARM = 0x3F;
    public static final int MSG_RESET_MILEAGE = 0x6B;
    public static final int MSG_RESTORE_OIL_DUCT = 0x38;
    public static final int MSG_CLOSE_OIL_DUCT = 0x39;
    public static final int MSG_POSITION_DATA = 0x80;
    public static final int MSG_ROLLCALL_RESPONSE = 0x81;
    public static final int MSG_ALARM_DATA = 0x82;
    public static final int MSG_TERMINAL_STATUS = 0x83;
    public static final int MSG_MESSAGE = 0x84;
    public static final int MSG_TERMINAL_ANSWER = 0x85;
    public static final int MSG_BLIND_AREA = 0x8E;
    public static final int MSG_PICTURE_FRAME = 0x54;
    public static final int MSG_CAMERA_RESPONSE = 0x56;
    public static final int MSG_PICTURE_DATA = 0x57;

    public static String decodeId(int b1, int b2, int b3, int b4) {

        int d1 = 30 + ((b1 >> 7) << 3) + ((b2 >> 7) << 2) + ((b3 >> 7) << 1) + (b4 >> 7);
        int d2 = b1 & 0x7f;
        int d3 = b2 & 0x7f;
        int d4 = b3 & 0x7f;
        int d5 = b4 & 0x7f;

        return String.format("%02d%02d%02d%02d%02d", d1, d2, d3, d4, d5);
    }

    private void sendResponse(Channel channel, SocketAddress remoteAddress, int type, int checksum) {
        if (channel != null) {
            ByteBuf response = Unpooled.buffer();
            response.writeShort(0x2424); // header
            response.writeByte(MSG_HEARTBEAT);
            response.writeShort(5); // length
            response.writeByte(checksum);
            response.writeByte(type);
            response.writeByte(0); // subtype
            response.writeByte(Checksum.xor(response.nioBuffer(2, response.writerIndex())));
            response.writeByte(0x0D);
            channel.writeAndFlush(new NetworkMessage(response, remoteAddress));
        }
    }

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {

        // Start tracing for this message
        Span span = tracer.spanBuilder("gator.decode")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("protocol", "gator")
                .setAttribute("remoteAddress", remoteAddress.toString())
                .startSpan();
        
        // Increment message counter
        messagesCounter.add(1);
        
        try {
            Context context = Context.current().with(span);
            return context.wrap(() -> decodeMessage(channel, remoteAddress, msg, span));
        } finally {
            span.end();
        }
    }
    
    private Object decodeMessage(
            Channel channel, SocketAddress remoteAddress, Object msg, Span span) throws Exception {

        ByteBuf buf = (ByteBuf) msg;

        buf.skipBytes(2); // header
        int type = buf.readUnsignedByte();
        buf.readUnsignedShort(); // length
        
        span.setAttribute("message.type", type);

        boolean modelM588 = false;
        String imei = null;
        if (buf.readableBytes() > 8) {
            imei = ByteBufUtil.hexDump(buf.slice(buf.readerIndex(), 8)).substring(0, 15);
            if (imei.matches("\\d+")) {
                long number = Long.parseLong(imei.substring(0, 14));
                if (Checksum.luhn(number) == Long.parseLong(imei.substring(14))) {
                    modelM588 = true;
                }
            }
        }

        String[] ids;
        if (modelM588) {
            ids = new String[] {imei};
            buf.skipBytes(8);
        } else {
            String id = decodeId(
                    buf.readUnsignedByte(), buf.readUnsignedByte(),
                    buf.readUnsignedByte(), buf.readUnsignedByte());
            ids = new String[] {"1" + id, id};
        }
        
        span.setAttribute("device.id", ids[0]);

        sendResponse(channel, remoteAddress, type, buf.getByte(buf.writerIndex() - 2));

        if (type == MSG_POSITION_DATA || type == MSG_ROLLCALL_RESPONSE
                || type == MSG_ALARM_DATA || type == MSG_BLIND_AREA) {

            Position position = new Position(getProtocolName());

            DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, ids);
            if (deviceSession == null) {
                return null;
            }
            position.setDeviceId(deviceSession.getDeviceId());

            DateBuilder dateBuilder = new DateBuilder()
                    .setYear(BcdUtil.readInteger(buf, 2))
                    .setMonth(BcdUtil.readInteger(buf, 2))
                    .setDay(BcdUtil.readInteger(buf, 2))
                    .setHour(BcdUtil.readInteger(buf, 2))
                    .setMinute(BcdUtil.readInteger(buf, 2))
                    .setSecond(BcdUtil.readInteger(buf, 2));
            position.setTime(dateBuilder.getDate());

            position.setLatitude(BcdUtil.readCoordinate(buf));
            position.setLongitude(BcdUtil.readCoordinate(buf));
            position.setSpeed(UnitsConverter.knotsFromKph(BcdUtil.readInteger(buf, 4)));
            position.setCourse(BcdUtil.readInteger(buf, 4));

            int flags = buf.readUnsignedByte();
            position.setValid((flags & 0x80) != 0);
            position.set(Position.KEY_SATELLITES, flags & 0x0f);

            position.set(Position.KEY_STATUS, buf.readUnsignedByte());
            position.set("key", buf.readUnsignedByte());

            position.set(Position.PREFIX_ADC + 1, buf.readUnsignedByte() + buf.readUnsignedByte() * 0.01);
            position.set(Position.PREFIX_ADC + 2, buf.readUnsignedByte() + buf.readUnsignedByte() * 0.01);

            position.set(Position.KEY_ODOMETER, buf.readUnsignedInt());

            if (modelM588 && buf.readableBytes() >= 5 + 2) {
                buf.readUnsignedShort();
                buf.readUnsignedShort();
                int alarm = buf.readUnsignedByte();
                position.addAlarm(BitUtil.check(alarm, 0) ? Position.ALARM_ACCELERATION : null);
                position.addAlarm(BitUtil.check(alarm, 1) ? Position.ALARM_BRAKING : null);
                position.addAlarm(BitUtil.check(alarm, 2) ? Position.ALARM_CORNERING : null);
            }

            if (type == MSG_ALARM_DATA) {
                int alarm1 = buf.readUnsignedByte();
                position.addAlarm(BitUtil.check(alarm1, 0) ? Position.ALARM_BRAKING : null);
                position.addAlarm(BitUtil.check(alarm1, 5) ? Position.ALARM_ACCELERATION : null);

                int alarm2 = buf.readUnsignedByte();
                position.addAlarm(BitUtil.check(alarm2, 1) ? Position.ALARM_OVERSPEED : null);
                position.addAlarm(BitUtil.check(alarm2, 4) ? Position.ALARM_CORNERING : null);
            }
            
            // Add tracing attributes for the position
            span.setAttribute("position.valid", position.getValid());
            span.setAttribute("position.latitude", position.getLatitude());
            span.setAttribute("position.longitude", position.getLongitude());
            span.setAttribute("position.speed", position.getSpeed());
            
            // Increment position counter
            positionsCounter.add(1);
            
            // If we're using service-based communication, publish the position to the message broker
            if (!directCommunication) {
                publishPosition(position, span);
                return null; // Return null to prevent the position from being processed by the core
            }

            return position;
        }

        return null;
    }
    
    private void publishPosition(Position position, Span span) {
        try {
            // Create headers with tracing information
            Map<String, String> headers = new HashMap<>();
            headers.put(MessageHeaders.DEVICE_ID, String.valueOf(position.getDeviceId()));
            headers.put(MessageHeaders.PROTOCOL, getProtocolName());
            headers.put(MessageHeaders.TIMESTAMP, String.valueOf(position.getFixTime().getTime()));
            
            // Create message envelope
            MessageEnvelope envelope = new MessageEnvelope(
                    position,
                    headers,
                    "position.raw",
                    span.getSpanContext().getTraceId());
            
            // Publish to message broker
            messageProducer.send(envelope);
            
            span.addEvent("Position published to message broker");
        } catch (Exception e) {
            span.recordException(e);
        }
    }
}