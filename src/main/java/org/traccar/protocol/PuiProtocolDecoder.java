/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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

import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.traccar.BaseMqttProtocolDecoder;
import org.traccar.Protocol;
import org.traccar.helper.UnitsConverter;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Position;
import org.traccar.session.DeviceSession;
import org.traccar.metrics.MetricsCollector;
import org.traccar.discovery.ServiceDiscoveryManager;

import javax.inject.Inject;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class PuiProtocolDecoder extends BaseMqttProtocolDecoder {

    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final MetricsCollector metricsCollector;
    private final ServiceDiscoveryManager serviceDiscoveryManager;

    @Inject
    public PuiProtocolDecoder(
            Protocol protocol,
            MessageProducer messageProducer,
            Tracer tracer,
            MetricsCollector metricsCollector,
            ServiceDiscoveryManager serviceDiscoveryManager) {
        super(protocol);
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.metricsCollector = metricsCollector;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        
        // Register this protocol service with service discovery
        registerService();
    }

    /**
     * Registers this protocol service with the service discovery system
     */
    private void registerService() {
        if (serviceDiscoveryManager != null) {
            serviceDiscoveryManager.register(
                    "protocol-pui",
                    getProtocolName(),
                    "/health",
                    "/ready");
        }
    }

    @Override
    protected Object decode(DeviceSession deviceSession, MqttPublishMessage message) throws Exception {
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("pui.decode")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("protocol.name", getProtocolName())
                .setAttribute("device.id", String.valueOf(deviceSession.getDeviceId()))
                .startSpan();
        
        // Increment message received counter
        metricsCollector.incrementCounter("protocol.pui.messages.received");
        
        try {
            // Attach the span to the current context
            Context context = Context.current().with(span);
            
            // Start timing the message processing
            long startTime = System.currentTimeMillis();
            
            JsonObject json;
            try (ByteBufferInputStream inputStream = new ByteBufferInputStream(message.payload().nioBuffer())) {
                json = Json.createReader(inputStream).readObject();
            }

            String type = json.getString("rpt");
            span.setAttribute("message.type", type);
            
            switch (type) {
                case "hf":
                case "loc":
                    Position position = new Position(getProtocolName());
                    position.setDeviceId(deviceSession.getDeviceId());

                    position.setValid(true);

                    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                    position.setTime(dateFormat.parse(json.getString("ts")));

                    JsonObject location = json.getJsonObject("location");
                    position.setLatitude(location.getJsonNumber("lat").doubleValue());
                    position.setLongitude(location.getJsonNumber("lon").doubleValue());

                    position.setCourse(json.getInt("bear"));
                    position.setSpeed(UnitsConverter.knotsFromCps(json.getInt("spd")));

                    position.set(Position.KEY_IGNITION, json.getString("ign").equals("on"));
                    
                    // Record processing time as a metric
                    long processingTime = System.currentTimeMillis() - startTime;
                    metricsCollector.recordTimer("protocol.pui.processing.time", processingTime);
                    span.setAttribute("processing.time_ms", processingTime);
                    
                    // Publish position to message broker asynchronously
                    publishPosition(position, span);
                    
                    return position;

                default:
                    span.setAttribute("message.unknown", true);
                    metricsCollector.incrementCounter("protocol.pui.messages.unknown");
                    return null;
            }
        } catch (Exception e) {
            // Record error in span and metrics
            span.recordException(e);
            span.setAttribute("error", true);
            metricsCollector.incrementCounter("protocol.pui.messages.error");
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Publishes the position to the message broker asynchronously
     * 
     * @param position The position to publish
     * @param parentSpan The parent span for tracing
     */
    private void publishPosition(Position position, Span parentSpan) {
        if (messageProducer != null) {
            Span span = tracer.spanBuilder("pui.publish.position")
                    .setParent(Context.current().with(parentSpan))
                    .setSpanKind(SpanKind.PRODUCER)
                    .setAttribute("protocol.name", getProtocolName())
                    .setAttribute("device.id", String.valueOf(position.getDeviceId()))
                    .startSpan();
            
            try {
                // Publish to the raw.positions topic with the device ID as the key for partitioning
                messageProducer.publish(
                        "raw.positions",
                        String.valueOf(position.getDeviceId()),
                        position,
                        span);
                
                metricsCollector.incrementCounter("protocol.pui.positions.published");
            } catch (Exception e) {
                span.recordException(e);
                span.setAttribute("error", true);
                metricsCollector.incrementCounter("protocol.pui.positions.publish.error");
            } finally {
                span.end();
            }
        }
    }
}