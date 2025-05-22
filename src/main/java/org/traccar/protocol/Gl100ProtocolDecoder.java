/*
 * Copyright 2012 - 2018 Anton Tananaev (anton@traccar.org)
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
import io.netty.channel.Channel;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import org.traccar.BaseProtocolDecoder;
import org.traccar.session.DeviceSession;
import org.traccar.NetworkMessage;
import org.traccar.Protocol;
import org.traccar.helper.Parser;
import org.traccar.helper.PatternBuilder;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Position;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Protocol decoder for GL100 GPS trackers.
 * Supports both direct communication and service-based communication through a message broker.
 */
public class Gl100ProtocolDecoder extends BaseProtocolDecoder {

    private static final Pattern PATTERN = new PatternBuilder()
            .text("+RESP:")
            .expression("GT...,")
            .number("(d{15}),")                  // imei
            .groupBegin()
            .number("d+,")                       // number
            .number("d,")                        // reserved / geofence id
            .number("d+")                        // reserved / geofence alert // battery
            .or()
            .number("[^,]*")                     // calling number
            .groupEnd(",")
            .expression("([01]),")               // gps fix
            .number("(d+.d),")                   // speed
            .number("(d+),")                     // course
            .number("(-?d+.d),")                 // altitude
            .number("d*,")                       // gps accuracy
            .number("(-?d+.d+),")                // longitude
            .number("(-?d+.d+),")                // latitude
            .number("(dddd)(dd)(dd)")            // date (yyyymmdd)
            .number("(dd)(dd)(dd),")             // time (hhmmss)
            .any()
            .compile();

    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final Counter messagesReceived;
    private final Counter messagesDecoded;
    private final Counter messageErrors;
    private final Timer messageProcessingTime;

    /**
     * Constructor for the GL100 protocol decoder with service-based communication support
     * 
     * @param protocol The protocol instance
     * @param messageProducer The message producer for asynchronous position publishing
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param messagesReceived Counter for received messages
     * @param messagesDecoded Counter for successfully decoded messages
     * @param messageErrors Counter for message processing errors
     * @param messageProcessingTime Timer for message processing time
     */
    public Gl100ProtocolDecoder(Protocol protocol, 
                              MessageProducer messageProducer,
                              Tracer tracer,
                              Counter messagesReceived,
                              Counter messagesDecoded,
                              Counter messageErrors,
                              Timer messageProcessingTime) {
        super(protocol);
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.messagesReceived = messagesReceived;
        this.messagesDecoded = messagesDecoded;
        this.messageErrors = messageErrors;
        this.messageProcessingTime = messageProcessingTime;
    }

    /**
     * Legacy constructor for backward compatibility
     * 
     * @param protocol The protocol instance
     */
    public Gl100ProtocolDecoder(Protocol protocol) {
        super(protocol);
        this.messageProducer = null;
        this.tracer = null;
        this.messagesReceived = null;
        this.messagesDecoded = null;
        this.messageErrors = null;
        this.messageProcessingTime = null;
    }

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {

        // Start the timer for message processing
        Timer.Sample sample = null;
        if (messageProcessingTime != null) {
            sample = Timer.start(messageProcessingTime.getId().getRegistry());
        }

        // Create a span for distributed tracing
        Span span = null;
        Scope scope = null;
        if (tracer != null) {
            span = tracer.spanBuilder("gl100.decode")
                    .setAttribute("protocol", "gl100")
                    .startSpan();
            scope = span.makeCurrent();
        }

        try {
            // Increment received messages counter
            if (messagesReceived != null) {
                messagesReceived.increment();
            }

            String sentence = (String) msg;

            // Add the raw message to the span for debugging
            if (span != null) {
                span.setAttribute("message.raw", sentence);
            }

            // Handle heartbeat messages
            if (sentence.contains("AT+GTHBD=") && channel != null) {
                String response = "+RESP:GTHBD,GPRS ACTIVE,";
                response += sentence.substring(9, sentence.lastIndexOf(','));
                response += '\0';
                channel.writeAndFlush(new NetworkMessage(response, remoteAddress)); // heartbeat response
                
                if (span != null) {
                    span.setAttribute("message.type", "heartbeat");
                    span.setAttribute("response.sent", true);
                }
                
                return null;
            }

            Parser parser = new Parser(PATTERN, sentence);
            if (!parser.matches()) {
                if (messageErrors != null) {
                    messageErrors.increment();
                }
                if (span != null) {
                    span.setAttribute("error", "Pattern match failed");
                    span.setAttribute("error.type", "parsing_error");
                }
                return null;
            }

            Position position = new Position(getProtocolName());

            DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, parser.next());
            if (deviceSession == null) {
                if (messageErrors != null) {
                    messageErrors.increment();
                }
                if (span != null) {
                    span.setAttribute("error", "Device session not found");
                    span.setAttribute("error.type", "unknown_device");
                }
                return null;
            }
            position.setDeviceId(deviceSession.getDeviceId());

            // Add device ID to the span for correlation
            if (span != null) {
                span.setAttribute("device.id", String.valueOf(deviceSession.getDeviceId()));
            }

            position.setValid(parser.nextInt(0) == 0);
            position.setSpeed(parser.nextDouble(0));
            position.setCourse(parser.nextDouble(0));
            position.setAltitude(parser.nextDouble(0));
            position.setLongitude(parser.nextDouble(0));
            position.setLatitude(parser.nextDouble(0));

            position.setTime(parser.nextDateTime());

            // Add position attributes to the span
            if (span != null) {
                span.setAttribute("position.valid", position.getValid());
                span.setAttribute("position.latitude", position.getLatitude());
                span.setAttribute("position.longitude", position.getLongitude());
                span.setAttribute("position.speed", position.getSpeed());
            }

            // Increment successfully decoded messages counter
            if (messagesDecoded != null) {
                messagesDecoded.increment();
            }

            // Publish position to message broker if available
            if (messageProducer != null) {
                // Create a new span for the publish operation
                Span publishSpan = null;
                Scope publishScope = null;
                if (tracer != null) {
                    publishSpan = tracer.spanBuilder("gl100.publish")
                            .setAttribute("protocol", "gl100")
                            .setAttribute("device.id", String.valueOf(deviceSession.getDeviceId()))
                            .startSpan();
                    publishScope = publishSpan.makeCurrent();
                }

                try {
                    // Create message headers with tracing context
                    Map<String, String> headers = new HashMap<>();
                    headers.put("deviceId", String.valueOf(deviceSession.getDeviceId()));
                    headers.put("protocol", "gl100");
                    
                    // Publish the position asynchronously
                    messageProducer.send("raw.positions", String.valueOf(deviceSession.getDeviceId()), position, headers);
                    
                    if (publishSpan != null) {
                        publishSpan.setAttribute("publish.success", true);
                    }
                } catch (Exception e) {
                    if (messageErrors != null) {
                        messageErrors.increment();
                    }
                    if (publishSpan != null) {
                        publishSpan.setAttribute("error", e.getMessage());
                        publishSpan.setAttribute("error.type", "publish_error");
                    }
                    getLogger().warn("Failed to publish position to message broker", e);
                } finally {
                    if (publishScope != null) {
                        publishScope.close();
                    }
                    if (publishSpan != null) {
                        publishSpan.end();
                    }
                }
            }

            return position;
        } catch (Exception e) {
            // Record error in metrics and tracing
            if (messageErrors != null) {
                messageErrors.increment();
            }
            if (span != null) {
                span.setAttribute("error", e.getMessage());
                span.setAttribute("error.type", "processing_error");
            }
            throw e;
        } finally {
            // Stop the timer and record the processing time
            if (sample != null && messageProcessingTime != null) {
                sample.stop(messageProcessingTime);
            }
            
            // End the tracing span
            if (scope != null) {
                scope.close();
            }
            if (span != null) {
                span.end();
            }
        }
    }
}