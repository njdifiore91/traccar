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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.traccar.BaseProtocolDecoder;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.metrics.ProtocolMetrics;
import org.traccar.model.Position;

/**
 * Protocol decoder for R12w GPS trackers.
 * Enhanced with distributed tracing, metrics collection, and message broker integration.
 */
public class R12wProtocolDecoder extends BaseProtocolDecoder {

    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final ProtocolMetrics metrics;

    /**
     * Constructs the R12w protocol decoder with required dependencies.
     *
     * @param protocol The protocol instance
     * @param messageProducer Message broker producer for position publishing
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param metrics Protocol metrics collector
     */
    public R12wProtocolDecoder(BaseProtocol protocol, 
                              MessageProducer messageProducer, 
                              Tracer tracer, 
                              ProtocolMetrics metrics) {
        super(protocol);
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.metrics = metrics;
    }

    /**
     * Decodes a message from the device.
     * This implementation includes distributed tracing, metrics collection,
     * and asynchronous position publishing via message broker.
     *
     * @param channel Communication channel
     * @param remoteAddress Remote device address
     * @param msg Message to decode
     * @return Decoded position or null if decoding failed
     */
    @Override
    protected Object decode(Channel channel, java.net.SocketAddress remoteAddress, Object msg) throws Exception {
        // Create a span for this decode operation
        Span span = tracer.spanBuilder("R12wProtocolDecoder.decode").startSpan();
        
        // Start metrics timer
        long startTime = System.currentTimeMillis();
        
        try (Scope scope = span.makeCurrent()) {
            // Add context information to the span
            span.setAttribute("protocol.name", getProtocolName());
            span.setAttribute("remote.address", remoteAddress.toString());
            
            // Record message received metric
            metrics.getMessageMetrics().messageReceived(getProtocolName());
            
            // Decode the message (original implementation logic would go here)
            // For this stub, we'll just create a placeholder for the actual implementation
            Position position = decodeMessage(channel, remoteAddress, msg.toString());
            
            // If position was successfully decoded, publish it to the message broker
            if (position != null) {
                publishPosition(position, span);
                
                // Record successful decode metric
                metrics.getMessageMetrics().messageDecoded(getProtocolName());
                span.addEvent("Position successfully decoded");
            } else {
                // Record failed decode metric
                metrics.getMessageMetrics().messageDecodeFailed(getProtocolName());
                span.addEvent("Failed to decode position");
            }
            
            return position;
        } catch (Exception e) {
            // Record exception in span and metrics
            span.recordException(e);
            metrics.getMessageMetrics().messageDecodeFailed(getProtocolName());
            throw e;
        } finally {
            // Record processing time metric
            long processingTime = System.currentTimeMillis() - startTime;
            metrics.getMessageMetrics().messageProcessingTime(getProtocolName(), processingTime);
            span.setAttribute("processing.time_ms", processingTime);
            
            // End the span
            span.end();
        }
    }

    /**
     * Actual message decoding implementation.
     * This would contain the original decoding logic for R12w protocol.
     *
     * @param channel Communication channel
     * @param remoteAddress Remote device address
     * @param message Message to decode
     * @return Decoded position or null if decoding failed
     */
    private Position decodeMessage(Channel channel, java.net.SocketAddress remoteAddress, String message) {
        // Original decoding logic would go here
        // This is just a placeholder for the actual implementation
        return null;
    }

    /**
     * Publishes a decoded position to the message broker.
     *
     * @param position The position to publish
     * @param parentSpan The parent span for tracing context
     */
    private void publishPosition(Position position, Span parentSpan) {
        Span span = tracer.spanBuilder("R12wProtocolDecoder.publishPosition")
                .setParent(io.opentelemetry.context.Context.current().with(parentSpan))
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Create message envelope with tracing context
            MessageEnvelope envelope = new MessageEnvelope(position);
            
            // Add device ID as routing key for partitioning
            String routingKey = String.valueOf(position.getDeviceId());
            
            // Publish to the positions topic
            messageProducer.send("positions", routingKey, envelope);
            
            span.addEvent("Position published to message broker");
            metrics.getMessageMetrics().messagePublished(getProtocolName());
        } catch (Exception e) {
            span.recordException(e);
            metrics.getMessageMetrics().messagePublishFailed(getProtocolName());
        } finally {
            span.end();
        }
    }

    /**
     * Gets the protocol name for metrics and tracing.
     *
     * @return Protocol name
     */
    private String getProtocolName() {
        return "r12w";
    }
}