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

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.concurrent.TimeUnit;

/**
 * Metrics related to device connections in the Protocol Service.
 * This class tracks active connections, connection attempts, and connection durations using Prometheus metrics.
 * It provides methods to record connection events (attempts, successes, failures) and measure connection durations,
 * with metrics broken down by protocol type.
 */
@Singleton
public class ConnectionMetrics {

    // Active connection metrics
    private final Gauge activeConnections;
    
    // Connection attempt metrics
    private final Counter connectionAttempts;
    private final Counter connectionSuccesses;
    private final Counter connectionFailures;
    
    // Connection duration metrics
    private final Histogram connectionDuration;
    
    // Connection type metrics
    private final Counter tcpConnections;
    private final Counter udpConnections;
    private final Counter httpConnections;
    
    /**
     * Constructs a new ConnectionMetrics instance and registers all metrics with the Prometheus registry.
     */
    @Inject
    public ConnectionMetrics() {
        // Initialize active connection metrics
        activeConnections = Gauge.build()
                .name("traccar_protocol_active_connections")
                .help("Number of active device connections by protocol")
                .labelNames("protocol")
                .register();
        
        // Initialize connection attempt metrics
        connectionAttempts = Counter.build()
                .name("traccar_protocol_connection_attempts_total")
                .help("Total number of connection attempts by protocol")
                .labelNames("protocol")
                .register();
        
        connectionSuccesses = Counter.build()
                .name("traccar_protocol_connection_successes_total")
                .help("Total number of successful connections by protocol")
                .labelNames("protocol")
                .register();
        
        connectionFailures = Counter.build()
                .name("traccar_protocol_connection_failures_total")
                .help("Total number of connection failures by protocol and reason")
                .labelNames("protocol", "reason")
                .register();
        
        // Initialize connection duration metrics
        connectionDuration = Histogram.build()
                .name("traccar_protocol_connection_duration_seconds")
                .help("Connection duration in seconds by protocol")
                .labelNames("protocol")
                .buckets(1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0, 14400.0, 28800.0, 86400.0)
                .register();
        
        // Initialize connection type metrics
        tcpConnections = Counter.build()
                .name("traccar_protocol_tcp_connections_total")
                .help("Total number of TCP connections by protocol")
                .labelNames("protocol")
                .register();
        
        udpConnections = Counter.build()
                .name("traccar_protocol_udp_connections_total")
                .help("Total number of UDP connections by protocol")
                .labelNames("protocol")
                .register();
        
        httpConnections = Counter.build()
                .name("traccar_protocol_http_connections_total")
                .help("Total number of HTTP connections by protocol")
                .labelNames("protocol")
                .register();
    }
    
    /**
     * Increments the active connections count for the specified protocol.
     *
     * @param protocol the protocol name
     */
    public void incrementActiveConnections(String protocol) {
        activeConnections.labels(protocol).inc();
    }
    
    /**
     * Decrements the active connections count for the specified protocol.
     *
     * @param protocol the protocol name
     */
    public void decrementActiveConnections(String protocol) {
        activeConnections.labels(protocol).dec();
    }
    
    /**
     * Sets the active connections count for the specified protocol.
     *
     * @param protocol the protocol name
     * @param count the number of active connections
     */
    public void setActiveConnections(String protocol, int count) {
        activeConnections.labels(protocol).set(count);
    }
    
    /**
     * Records a connection attempt for the specified protocol.
     *
     * @param protocol the protocol name
     */
    public void connectionAttempt(String protocol) {
        connectionAttempts.labels(protocol).inc();
    }
    
    /**
     * Records a successful connection for the specified protocol.
     *
     * @param protocol the protocol name
     */
    public void connectionSuccess(String protocol) {
        connectionSuccesses.labels(protocol).inc();
    }
    
    /**
     * Records a connection failure for the specified protocol and reason.
     *
     * @param protocol the protocol name
     * @param reason the failure reason
     */
    public void connectionFailure(String protocol, String reason) {
        connectionFailures.labels(protocol, reason).inc();
    }
    
    /**
     * Records a connection duration for the specified protocol.
     *
     * @param protocol the protocol name
     * @param durationSeconds the connection duration in seconds
     */
    public void recordConnectionDuration(String protocol, double durationSeconds) {
        connectionDuration.labels(protocol).observe(durationSeconds);
    }
    
    /**
     * Records a TCP connection for the specified protocol.
     *
     * @param protocol the protocol name
     */
    public void tcpConnection(String protocol) {
        tcpConnections.labels(protocol).inc();
    }
    
    /**
     * Records a UDP connection for the specified protocol.
     *
     * @param protocol the protocol name
     */
    public void udpConnection(String protocol) {
        udpConnections.labels(protocol).inc();
    }
    
    /**
     * Records an HTTP connection for the specified protocol.
     *
     * @param protocol the protocol name
     */
    public void httpConnection(String protocol) {
        httpConnections.labels(protocol).inc();
    }
    
    /**
     * Records a connection event with the specified transport type.
     * This is a convenience method that combines multiple metric recordings.
     *
     * @param protocol the protocol name
     * @param transportType the transport type ("tcp", "udp", or "http")
     */
    public void recordConnection(String protocol, String transportType) {
        connectionAttempt(protocol);
        connectionSuccess(protocol);
        incrementActiveConnections(protocol);
        
        switch (transportType.toLowerCase()) {
            case "tcp":
                tcpConnection(protocol);
                break;
            case "udp":
                udpConnection(protocol);
                break;
            case "http":
                httpConnection(protocol);
                break;
            default:
                // No specific counter for other transport types
                break;
        }
    }
    
    /**
     * Records a connection event with the specified transport type and failure reason.
     * This is a convenience method that combines multiple metric recordings.
     *
     * @param protocol the protocol name
     * @param transportType the transport type ("tcp", "udp", or "http")
     * @param failureReason the failure reason, or null if the connection was successful
     */
    public void recordConnection(String protocol, String transportType, String failureReason) {
        connectionAttempt(protocol);
        
        if (failureReason == null) {
            connectionSuccess(protocol);
            incrementActiveConnections(protocol);
            
            switch (transportType.toLowerCase()) {
                case "tcp":
                    tcpConnection(protocol);
                    break;
                case "udp":
                    udpConnection(protocol);
                    break;
                case "http":
                    httpConnection(protocol);
                    break;
                default:
                    // No specific counter for other transport types
                    break;
            }
        } else {
            connectionFailure(protocol, failureReason);
        }
    }
    
    /**
     * Timer class for measuring connection duration.
     * This class provides a convenient way to measure the time a connection is active.
     */
    public class ConnectionTimer implements AutoCloseable {
        private final String protocol;
        private final long startTimeNanos;
        private boolean closed = false;
        
        /**
         * Constructs a new ConnectionTimer and starts timing.
         * This also increments the active connections count for the protocol.
         *
         * @param protocol the protocol name
         */
        public ConnectionTimer(String protocol) {
            this.protocol = protocol;
            this.startTimeNanos = System.nanoTime();
            incrementActiveConnections(protocol);
        }
        
        /**
         * Stops timing, records the connection duration, and decrements the active connections count.
         * This method is automatically called when the timer is used in a try-with-resources block.
         */
        @Override
        public void close() {
            if (!closed) {
                double elapsedSeconds = nanosToSeconds(System.nanoTime() - startTimeNanos);
                recordConnectionDuration(protocol, elapsedSeconds);
                decrementActiveConnections(protocol);
                closed = true;
            }
        }
    }
    
    /**
     * Creates a new ConnectionTimer for measuring connection duration.
     * This method is intended to be used with try-with-resources.
     *
     * @param protocol the protocol name
     * @return a new ConnectionTimer instance
     */
    public ConnectionTimer time(String protocol) {
        return new ConnectionTimer(protocol);
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