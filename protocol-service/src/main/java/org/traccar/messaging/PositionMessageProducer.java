/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Position;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

/**
 * Specialized MessageProducer for publishing position messages to the 'raw.positions' topic.
 * Handles position-specific serialization, partitioning by device ID, and ensures that
 * position messages are delivered in order for each device.
 */
@Singleton
public class PositionMessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionMessageProducer.class);

    private final MessageProducer messageProducer;
    private final ObjectMapper objectMapper;
    private final MessagingTracer messagingTracer;
    private final MessagingMetrics messagingMetrics;
    private final String positionsTopic;

    /**
     * Container for position-specific metadata that should be included with position messages.
     * This metadata helps downstream services process the position data more effectively.
     */
    public static class PositionMetadata {
        private final String protocol;
        private final long receivedTime;
        private final String originalMessage;
        private final boolean firstPosition;

        /**
         * Constructs a new PositionMetadata object.
         *
         * @param protocol The protocol used to decode the position
         * @param receivedTime The time when the position was received by the server (epoch millis)
         * @param originalMessage The original message from the device, if available
         * @param firstPosition Whether this is the first position received from the device in the current session
         */
        public PositionMetadata(String protocol, long receivedTime, String originalMessage, boolean firstPosition) {
            this.protocol = protocol;
            this.receivedTime = receivedTime;
            this.originalMessage = originalMessage;
            this.firstPosition = firstPosition;
        }

        /**
         * Gets the protocol used to decode the position.
         *
         * @return The protocol name
         */
        public String getProtocol() {
            return protocol;
        }

        /**
         * Gets the time when the position was received by the server.
         *
         * @return The received time in epoch milliseconds
         */
        public long getReceivedTime() {
            return receivedTime;
        }

        /**
         * Gets the original message from the device, if available.
         *
         * @return The original message, or null if not available
         */
        public String getOriginalMessage() {
            return originalMessage;
        }

        /**
         * Checks if this is the first position received from the device in the current session.
         *
         * @return true if this is the first position, false otherwise
         */
        public boolean isFirstPosition() {
            return firstPosition;
        }
    }

    /**
     * Constructs a new PositionMessageProducer with the provided dependencies.
     *
     * @param config Configuration parameters
     * @param messageProducer The underlying message producer implementation (Kafka or RabbitMQ)
     * @param objectMapper Jackson object mapper for JSON serialization
     * @param messagingTracer Tracer for distributed tracing
     * @param messagingMetrics Metrics collector for monitoring
     */
    @Inject
    public PositionMessageProducer(
            Config config,
            MessageProducer messageProducer,
            ObjectMapper objectMapper,
            MessagingTracer messagingTracer,
            MessagingMetrics messagingMetrics) {
        this.messageProducer = messageProducer;
        this.objectMapper = objectMapper;
        this.messagingTracer = messagingTracer;
        this.messagingMetrics = messagingMetrics;
        this.positionsTopic = TopicNames.getRawPositionsTopic();
        
        LOGGER.info("Position message producer initialized with topic: {}", positionsTopic);
    }

    /**
     * Send a position to the message broker.
     * This method uses the default metadata derived from the position object.
     *
     * @param position Position object to send
     * @throws Exception If there is an error sending the message
     */
    public void sendPosition(Position position) throws Exception {
        // Create default metadata with basic information
        PositionMetadata metadata = new PositionMetadata(
                position.getProtocol(),
                System.currentTimeMillis(),
                position.hasAttribute(Position.KEY_ORIGINAL) ? 
                        position.getString(Position.KEY_ORIGINAL) : null,
                false);
        
        sendPosition(position, metadata);
    }

    /**
     * Send a position to the message broker with additional metadata.
     * This method adds position-specific metadata to the message for downstream processing.
     *
     * @param position Position object to send
     * @param metadata Additional metadata to include with the position
     * @throws Exception If there is an error sending the message
     */
    public void sendPosition(Position position, PositionMetadata metadata) throws Exception {
        Timer.Sample timer = Timer.start();
        
        try {
            // Start a new span for position publishing
            Span span = messagingTracer.startProducerSpan("publish_position", positionsTopic);
            span.setAttribute("position.deviceId", position.getDeviceId());
            span.setAttribute("position.protocol", metadata.getProtocol());
            span.setAttribute("position.valid", position.getValid());
            
            try {
                // Enrich the position with metadata before sending
                enrichPositionWithMetadata(position, metadata);
                
                // Send the position using the underlying message producer
                messageProducer.sendPosition(position);
                
                // Record metrics for successful send
                timer.stop(messagingMetrics.getPositionSendTimer());
                messagingMetrics.incrementPositionsSent();
                messagingMetrics.recordPositionSize(objectMapper.writeValueAsString(position).length());
                
                LOGGER.debug("Position sent to broker: deviceId={}, protocol={}, valid={}", 
                        position.getDeviceId(), metadata.getProtocol(), position.getValid());
            } catch (Exception e) {
                // Record error metrics
                messagingMetrics.incrementPositionErrors();
                span.recordException(e);
                throw e;
            } finally {
                span.end();
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to send position to broker", e);
            throw e;
        }
    }

    /**
     * Send a device connection status update to the message broker.
     * This method delegates to the underlying message producer.
     *
     * @param deviceId Device identifier
     * @param connected Connection status
     * @throws Exception If there is an error sending the message
     */
    public void sendDeviceConnectionStatus(long deviceId, boolean connected) throws Exception {
        messageProducer.sendDeviceConnectionStatus(deviceId, connected);
    }

    /**
     * Close the producer and release resources.
     * This method delegates to the underlying message producer.
     */
    public void close() {
        messageProducer.close();
    }

    /**
     * Enriches the position object with metadata before sending.
     * This adds additional fields that are useful for downstream processing.
     *
     * @param position The position object to enrich
     * @param metadata The metadata to add to the position
     */
    private void enrichPositionWithMetadata(Position position, PositionMetadata metadata) {
        // Add metadata as attributes if not already present
        if (!position.hasAttribute("protocol") && metadata.getProtocol() != null) {
            position.set("protocol", metadata.getProtocol());
        }
        
        if (!position.hasAttribute("receivedTime")) {
            position.set("receivedTime", metadata.getReceivedTime());
        }
        
        if (!position.hasAttribute("firstPosition")) {
            position.set("firstPosition", metadata.isFirstPosition());
        }
        
        // Only add original message if it's not already there and it's available
        if (!position.hasAttribute(Position.KEY_ORIGINAL) && metadata.getOriginalMessage() != null) {
            position.set(Position.KEY_ORIGINAL, metadata.getOriginalMessage());
        }
    }
}