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
package org.traccar.metrics;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.concurrent.TimeUnit;

/**
 * Metrics related to message processing in the Protocol Service.
 * This class tracks message throughput, processing times, and success/failure rates using Prometheus metrics.
 * It provides methods to record message events (received, processed, errors) and measure processing times
 * and message sizes, with metrics broken down by protocol and message type.
 */
@Singleton
public class MessageMetrics {

    // Message throughput metrics
    private final Counter messageReceived;
    private final Counter messageProcessed;
    private final Counter messageErrors;
    
    // Message processing time metrics
    private final Histogram messageProcessingTime;
    
    // Message size metrics
    private final Summary messageSize;
    private final Counter totalBytesReceived;
    
    // Message success/failure metrics
    private final Counter messageDecodeSuccess;
    private final Counter messageDecodeFailure;
    
    // Message type metrics
    private final Counter positionMessages;
    private final Counter eventMessages;
    private final Counter commandMessages;
    private final Counter otherMessages;
    
    /**
     * Constructs a new MessageMetrics instance and registers all metrics with the Prometheus registry.
     */
    @Inject
    public MessageMetrics() {
        // Initialize message throughput metrics
        messageReceived = Counter.build()
                .name("traccar_protocol_message_received_total")
                .help("Total number of messages received by protocol and message type")
                .labelNames("protocol", "type")
                .register();
        
        messageProcessed = Counter.build()
                .name("traccar_protocol_message_processed_total")
                .help("Total number of messages successfully processed by protocol and message type")
                .labelNames("protocol", "type")
                .register();
        
        messageErrors = Counter.build()
                .name("traccar_protocol_message_errors_total")
                .help("Total number of message processing errors by protocol, message type, and error type")
                .labelNames("protocol", "type", "error")
                .register();
        
        // Initialize message processing time metrics
        messageProcessingTime = Histogram.build()
                .name("traccar_protocol_message_processing_seconds")
                .help("Message processing time in seconds by protocol and message type")
                .labelNames("protocol", "type")
                .buckets(0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0)
                .register();
        
        // Initialize message size metrics
        messageSize = Summary.build()
                .name("traccar_protocol_message_size_bytes")
                .help("Message size in bytes by protocol and message type")
                .labelNames("protocol", "type")
                .quantile(0.5, 0.05)   // Add median with 5% error
                .quantile(0.9, 0.01)   // Add 90th percentile with 1% error
                .quantile(0.99, 0.001) // Add 99th percentile with 0.1% error
                .register();
        
        totalBytesReceived = Counter.build()
                .name("traccar_protocol_bytes_received_total")
                .help("Total bytes received by protocol")
                .labelNames("protocol")
                .register();
        
        // Initialize message success/failure metrics
        messageDecodeSuccess = Counter.build()
                .name("traccar_protocol_message_decode_success_total")
                .help("Total number of successfully decoded messages by protocol")
                .labelNames("protocol")
                .register();
        
        messageDecodeFailure = Counter.build()
                .name("traccar_protocol_message_decode_failure_total")
                .help("Total number of message decoding failures by protocol and reason")
                .labelNames("protocol", "reason")
                .register();
        
        // Initialize message type metrics
        positionMessages = Counter.build()
                .name("traccar_protocol_position_messages_total")
                .help("Total number of position messages by protocol")
                .labelNames("protocol")
                .register();
        
        eventMessages = Counter.build()
                .name("traccar_protocol_event_messages_total")
                .help("Total number of event messages by protocol")
                .labelNames("protocol")
                .register();
        
        commandMessages = Counter.build()
                .name("traccar_protocol_command_messages_total")
                .help("Total number of command messages by protocol")
                .labelNames("protocol")
                .register();
        
        otherMessages = Counter.build()
                .name("traccar_protocol_other_messages_total")
                .help("Total number of other message types by protocol")
                .labelNames("protocol")
                .register();
    }
    
    /**
     * Records a message received event.
     *
     * @param protocol the protocol name
     * @param type the message type
     */
    public void messageReceived(String protocol, String type) {
        messageReceived.labels(protocol, type).inc();
    }
    
    /**
     * Records a message processed event.
     *
     * @param protocol the protocol name
     * @param type the message type
     */
    public void messageProcessed(String protocol, String type) {
        messageProcessed.labels(protocol, type).inc();
    }
    
    /**
     * Records a message error event.
     *
     * @param protocol the protocol name
     * @param type the message type
     * @param error the error type
     */
    public void messageError(String protocol, String type, String error) {
        messageErrors.labels(protocol, type, error).inc();
    }
    
    /**
     * Records message processing time.
     *
     * @param protocol the protocol name
     * @param type the message type
     * @param seconds the processing time in seconds
     */
    public void recordMessageProcessingTime(String protocol, String type, double seconds) {
        messageProcessingTime.labels(protocol, type).observe(seconds);
    }
    
    /**
     * Records message size.
     *
     * @param protocol the protocol name
     * @param type the message type
     * @param bytes the message size in bytes
     */
    public void recordMessageSize(String protocol, String type, double bytes) {
        messageSize.labels(protocol, type).observe(bytes);
        totalBytesReceived.labels(protocol).inc(bytes);
    }
    
    /**
     * Records a successful message decode.
     *
     * @param protocol the protocol name
     */
    public void messageDecodeSuccess(String protocol) {
        messageDecodeSuccess.labels(protocol).inc();
    }
    
    /**
     * Records a message decode failure.
     *
     * @param protocol the protocol name
     * @param reason the failure reason
     */
    public void messageDecodeFailure(String protocol, String reason) {
        messageDecodeFailure.labels(protocol, reason).inc();
    }
    
    /**
     * Records a position message.
     *
     * @param protocol the protocol name
     */
    public void positionMessage(String protocol) {
        positionMessages.labels(protocol).inc();
    }
    
    /**
     * Records an event message.
     *
     * @param protocol the protocol name
     */
    public void eventMessage(String protocol) {
        eventMessages.labels(protocol).inc();
    }
    
    /**
     * Records a command message.
     *
     * @param protocol the protocol name
     */
    public void commandMessage(String protocol) {
        commandMessages.labels(protocol).inc();
    }
    
    /**
     * Records another type of message.
     *
     * @param protocol the protocol name
     */
    public void otherMessage(String protocol) {
        otherMessages.labels(protocol).inc();
    }
    
    /**
     * Records a message processing event with timing.
     * This is a convenience method that combines multiple metric recordings.
     *
     * @param protocol the protocol name
     * @param type the message type
     * @param bytes the message size in bytes
     * @param processingTimeSeconds the processing time in seconds
     * @param success whether the processing was successful
     */
    public void recordMessageProcessing(
            String protocol, String type, double bytes, double processingTimeSeconds, boolean success) {
        
        // Record basic metrics
        messageReceived.labels(protocol, type).inc();
        recordMessageSize(protocol, type, bytes);
        recordMessageProcessingTime(protocol, type, processingTimeSeconds);
        
        // Record success or failure
        if (success) {
            messageProcessed.labels(protocol, type).inc();
            messageDecodeSuccess.labels(protocol).inc();
        } else {
            messageError(protocol, type, "processing_failed");
        }
        
        // Record by message type
        switch (type.toLowerCase()) {
            case "position":
                positionMessage(protocol);
                break;
            case "event":
                eventMessage(protocol);
                break;
            case "command":
                commandMessage(protocol);
                break;
            default:
                otherMessage(protocol);
                break;
        }
    }
    
    /**
     * Records a message processing event with timing and error details.
     * This is a convenience method that combines multiple metric recordings.
     *
     * @param protocol the protocol name
     * @param type the message type
     * @param bytes the message size in bytes
     * @param processingTimeSeconds the processing time in seconds
     * @param errorType the type of error that occurred, or null if successful
     */
    public void recordMessageProcessing(
            String protocol, String type, double bytes, double processingTimeSeconds, String errorType) {
        
        boolean success = errorType == null;
        recordMessageProcessing(protocol, type, bytes, processingTimeSeconds, success);
        
        if (!success) {
            messageError(protocol, type, errorType);
            messageDecodeFailure.labels(protocol, errorType).inc();
        }
    }
    
    /**
     * Timer class for measuring message processing time.
     * This class provides a convenient way to measure the time taken to process a message.
     */
    public class MessageTimer implements AutoCloseable {
        private final String protocol;
        private final String type;
        private final double bytes;
        private final long startTimeNanos;
        private boolean success = true;
        
        /**
         * Constructs a new MessageTimer and starts timing.
         *
         * @param protocol the protocol name
         * @param type the message type
         * @param bytes the message size in bytes
         */
        public MessageTimer(String protocol, String type, double bytes) {
            this.protocol = protocol;
            this.type = type;
            this.bytes = bytes;
            this.startTimeNanos = System.nanoTime();
            
            // Record message received immediately
            messageReceived.labels(protocol, type).inc();
            recordMessageSize(protocol, type, bytes);
        }
        
        /**
         * Marks the message processing as failed.
         *
         * @param reason the failure reason
         * @return this timer instance for method chaining
         */
        public MessageTimer failed(String reason) {
            this.success = false;
            messageError(protocol, type, reason);
            messageDecodeFailure.labels(protocol, reason).inc();
            return this;
        }
        
        /**
         * Stops timing and records metrics.
         * This method is automatically called when the timer is used in a try-with-resources block.
         */
        @Override
        public void close() {
            double elapsedSeconds = nanosToSeconds(System.nanoTime() - startTimeNanos);
            recordMessageProcessingTime(protocol, type, elapsedSeconds);
            
            if (success) {
                messageProcessed.labels(protocol, type).inc();
                messageDecodeSuccess.labels(protocol).inc();
                
                // Record by message type
                switch (type.toLowerCase()) {
                    case "position":
                        positionMessage(protocol);
                        break;
                    case "event":
                        eventMessage(protocol);
                        break;
                    case "command":
                        commandMessage(protocol);
                        break;
                    default:
                        otherMessage(protocol);
                        break;
                }
            }
        }
    }
    
    /**
     * Creates a new MessageTimer for measuring message processing time.
     * This method is intended to be used with try-with-resources.
     *
     * @param protocol the protocol name
     * @param type the message type
     * @param bytes the message size in bytes
     * @return a new MessageTimer instance
     */
    public MessageTimer time(String protocol, String type, double bytes) {
        return new MessageTimer(protocol, type, bytes);
    }
    
    /**
     * Converts nanoseconds to seconds.
     *
     * @param nanos the time in nanoseconds
     * @return the time in seconds
     */
    public static double nanosToSeconds(long nanos) {
        return nanos / (double) TimeUnit.SECONDS.toNanos(1);
    }
    
    /**
     * Converts milliseconds to seconds.
     *
     * @param millis the time in milliseconds
     * @return the time in seconds
     */
    public static double millisToSeconds(long millis) {
        return millis / (double) TimeUnit.SECONDS.toMillis(1);
    }
}