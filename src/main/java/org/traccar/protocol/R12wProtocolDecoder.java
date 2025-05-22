/*
 * Copyright 2021 Anton Tananaev (anton@traccar.org)
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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.traccar.BaseProtocolDecoder;
import org.traccar.session.DeviceSession;
import org.traccar.NetworkMessage;
import org.traccar.Protocol;
import org.traccar.helper.Checksum;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.messaging.MessageProducer;
import org.traccar.config.Config;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

public class R12wProtocolDecoder extends BaseProtocolDecoder {

    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Counter messageCounter;
    private final Timer processingTimer;
    private final boolean directCommunication;

    public R12wProtocolDecoder(Protocol protocol) {
        super(protocol);
        
        // Get dependencies from the context
        Config config = protocol.getConfig();
        this.serviceDiscoveryManager = protocol.getInjector().getInstance(ServiceDiscoveryManager.class);
        this.messageProducer = protocol.getInjector().getInstance(MessageProducer.class);
        this.tracer = protocol.getInjector().getInstance(Tracer.class);
        this.meterRegistry = protocol.getInjector().getInstance(MeterRegistry.class);
        
        // Initialize metrics
        this.messageCounter = meterRegistry.counter("protocol.r12w.messages");
        this.processingTimer = meterRegistry.timer("protocol.r12w.processing");
        
        // Configure communication mode based on configuration
        this.directCommunication = config.getBoolean("protocol.r12w.directCommunication", false);
    }

    private void sendResponse(Channel channel, String type, String id, String data) {
        if (channel != null) {
            String sentence = String.format("$HX,%s,%s,%s,#", type, id, data);
            sentence += String.format(",%02x,\r\n", Checksum.xor(sentence));
            channel.writeAndFlush(new NetworkMessage(sentence, channel.remoteAddress()));
            
            // Record metrics for response
            meterRegistry.counter("protocol.r12w.responses").increment();
        }
    }

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {

        // Start the processing timer
        Timer.Sample sample = Timer.start(meterRegistry);
        
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("r12w.decode").startSpan();
        try (Scope scope = span.makeCurrent()) {
            // Add protocol details to the span
            span.setAttribute("protocol", "r12w");
            span.setAttribute("remoteAddress", remoteAddress.toString());
            
            // Increment message counter
            messageCounter.increment();
            
            String sentence = (String) msg;
            span.setAttribute("message", sentence);
            
            String[] values = sentence.split(",");
            String type = values[1];
            String id = values[2];
            
            span.setAttribute("messageType", type);
            span.setAttribute("deviceId", id);
    
            DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, id);
            if (deviceSession == null) {
                span.setAttribute("deviceFound", false);
                return null;
            }
            
            span.setAttribute("deviceFound", true);
            span.setAttribute("deviceSessionId", deviceSession.getDeviceId());
    
            if (type.equals("0001")) {
                span.setAttribute("messageHandled", true);
                sendResponse(channel, "1001", id, values[3] + ",OK");
                
                // If using service-based communication, publish the message to the broker
                if (!directCommunication) {
                    messageProducer.publishProtocolMessage(deviceSession.getDeviceId(), "r12w", sentence);
                    span.setAttribute("messageBrokerPublished", true);
                }
            } else {
                span.setAttribute("messageHandled", false);
            }
    
            return null;
        } catch (Exception e) {
            span.recordException(e);
            throw e;
        } finally {
            span.end();
            // Record the processing time
            sample.stop(processingTimer);
        }
    }

}