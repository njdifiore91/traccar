/*
 * Copyright 2019 - 2023 Anton Tananaev (anton@traccar.org)
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
import io.netty.channel.Channel;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import org.traccar.BaseProtocolDecoder;
import org.traccar.Protocol;
import org.traccar.helper.BitUtil;
import org.traccar.helper.DateBuilder;
import org.traccar.helper.UnitsConverter;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageHeaders;
import org.traccar.model.Position;

import java.net.SocketAddress;
import java.util.Date;

/**
 * Protocol decoder for Radar GPS tracking devices.
 * Enhanced with message broker integration for asynchronous position publishing
 * and distributed tracing for protocol message tracking.
 */
public class RadarProtocolDecoder extends BaseProtocolDecoder {

    private final MessageProducer messageProducer;

    /**
     * Initialize the decoder with required dependencies.
     *
     * @param protocol The protocol instance
     * @param messageProducer Message broker producer for asynchronous position publishing
     */
    public RadarProtocolDecoder(Protocol protocol, MessageProducer messageProducer) {
        super(protocol);
        this.messageProducer = messageProducer;
    }

    /**
     * Decode the raw message from the device and convert it to a Position object.
     * This implementation adds distributed tracing and message broker integration.
     */
    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {

        ByteBuf buf = (ByteBuf) msg;

        // Create a span for this decoding operation
        Span span = getTracer().spanBuilder("radar.decode").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("protocol.name", getProtocolName());
            span.setAttribute("remote.address", remoteAddress.toString());

            buf.skipBytes(2); // header
            buf.readUnsignedShort(); // length
            buf.readUnsignedShort(); // checksum
            buf.readUnsignedShort(); // type
            buf.readUnsignedShort(); // header checksum

            // Get device by identifier
            String imei = String.valueOf(buf.readLong());
            span.setAttribute("device.id", imei);
            if (!identify(imei, channel, remoteAddress)) {
                span.setAttribute("decode.success", false);
                span.setAttribute("decode.error", "Device not identified");
                return null;
            }

            Position position = new Position(getProtocolName());
            position.setDeviceId(getDeviceId());

            // Record metrics for this device
            getMeterRegistry().counter("protocol.messages", "protocol", getProtocolName(), "device", imei).increment();

            position.set(Position.KEY_INDEX, buf.readUnsignedShort());

            buf.readUnsignedShort(); // product

            position.set(Position.KEY_SATELLITES, BitUtil.to(buf.readUnsignedByte(), 4));

            int status = buf.readUnsignedByte();
            position.setValid(BitUtil.check(status, 0));
            position.set(Position.KEY_IGNITION, BitUtil.check(status, 1));
            position.set(Position.KEY_ALARM, BitUtil.check(status, 2) ? Position.ALARM_GENERAL : null);

            position.setTime(new DateBuilder()
                    .setDate(buf.readUnsignedByte(), buf.readUnsignedByte(), buf.readUnsignedByte())
                    .setTime(buf.readUnsignedByte(), buf.readUnsignedByte(), buf.readUnsignedByte())
                    .getDate());

            position.setLatitude(buf.readUnsignedInt() / 60.0 / 30000.0);
            position.setLongitude(buf.readUnsignedInt() / 60.0 / 30000.0);
            position.setSpeed(UnitsConverter.knotsFromKph(buf.readUnsignedByte()));
            position.setCourse(buf.readUnsignedByte() * 2);

            position.set(Position.KEY_ODOMETER, buf.readUnsignedInt() * 1000);

            // Publish position to message broker if available
            publishPosition(position);

            span.setAttribute("decode.success", true);
            return position;
        } catch (Exception e) {
            span.recordException(e);
            span.setAttribute("decode.success", false);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Publish the position to the message broker for asynchronous processing.
     *
     * @param position The position to publish
     */
    private void publishPosition(Position position) {
        if (messageProducer != null) {
            try {
                // Create message headers with tracing context for correlation
                MessageHeaders headers = new MessageHeaders();
                headers.put(MessageHeaders.DEVICE_ID, String.valueOf(position.getDeviceId()));
                headers.put(MessageHeaders.PROTOCOL, getProtocolName());
                headers.put(MessageHeaders.TIMESTAMP, String.valueOf(new Date().getTime()));
                
                // Publish to the raw-positions topic
                messageProducer.send("raw-positions", position, headers);
                
                // Record metrics for successful publishing
                getMeterRegistry().counter("protocol.positions.published", 
                        "protocol", getProtocolName(), 
                        "device", String.valueOf(position.getDeviceId())).increment();
            } catch (Exception e) {
                getLogger().warn("Failed to publish position to message broker", e);
                
                // Record metrics for failed publishing
                getMeterRegistry().counter("protocol.positions.failed", 
                        "protocol", getProtocolName(), 
                        "device", String.valueOf(position.getDeviceId())).increment();
            }
        }
    }
}