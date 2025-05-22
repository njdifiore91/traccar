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
package org.traccar.forward;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Collects and reports metrics for forwarding operations, providing visibility into system performance and health.
 * Integrates with OpenTelemetry to record metrics such as message throughput, latency, error rates, and circuit
 * breaker states, enabling monitoring and alerting on key performance indicators.
 */
@Singleton
public class MetricsManager {

    private static final String METRIC_PREFIX = "traccar.forward";
    
    private final Meter meter;
    
    // Throughput metrics
    private final LongCounter positionForwardCounter;
    private final LongCounter eventForwardCounter;
    private final LongCounter networkForwardCounter;
    
    // Error metrics
    private final LongCounter positionErrorCounter;
    private final LongCounter eventErrorCounter;
    private final LongCounter networkErrorCounter;
    
    // Latency metrics
    private final DoubleHistogram positionLatencyHistogram;
    private final DoubleHistogram eventLatencyHistogram;
    private final DoubleHistogram networkLatencyHistogram;
    
    // Connection pool metrics
    private final LongUpDownCounter activeConnectionsCounter;
    private final LongUpDownCounter pendingConnectionsCounter;
    
    // Circuit breaker metrics
    private final LongUpDownCounter circuitBreakerStateCounter;

    /**
     * Initializes the MetricsManager with OpenTelemetry.
     *
     * @param openTelemetry the OpenTelemetry instance
     */
    @Inject
    public MetricsManager(OpenTelemetry openTelemetry) {
        this.meter = openTelemetry.getMeterProvider().get(METRIC_PREFIX);
        
        // Initialize throughput counters
        positionForwardCounter = meter.counterBuilder("position_messages_total")
                .setDescription("Total number of position messages forwarded")
                .setUnit("{messages}")
                .build();
        
        eventForwardCounter = meter.counterBuilder("event_messages_total")
                .setDescription("Total number of event messages forwarded")
                .setUnit("{messages}")
                .build();
        
        networkForwardCounter = meter.counterBuilder("network_messages_total")
                .setDescription("Total number of network messages forwarded")
                .setUnit("{messages}")
                .build();
        
        // Initialize error counters
        positionErrorCounter = meter.counterBuilder("position_errors_total")
                .setDescription("Total number of position forwarding errors")
                .setUnit("{errors}")
                .build();
        
        eventErrorCounter = meter.counterBuilder("event_errors_total")
                .setDescription("Total number of event forwarding errors")
                .setUnit("{errors}")
                .build();
        
        networkErrorCounter = meter.counterBuilder("network_errors_total")
                .setDescription("Total number of network forwarding errors")
                .setUnit("{errors}")
                .build();
        
        // Initialize latency histograms
        positionLatencyHistogram = meter.histogramBuilder("position_latency")
                .setDescription("Latency of position forwarding operations")
                .setUnit("ms")
                .build();
        
        eventLatencyHistogram = meter.histogramBuilder("event_latency")
                .setDescription("Latency of event forwarding operations")
                .setUnit("ms")
                .build();
        
        networkLatencyHistogram = meter.histogramBuilder("network_latency")
                .setDescription("Latency of network forwarding operations")
                .setUnit("ms")
                .build();
        
        // Initialize connection pool metrics
        activeConnectionsCounter = meter.upDownCounterBuilder("active_connections")
                .setDescription("Number of active forwarding connections")
                .setUnit("{connections}")
                .build();
        
        pendingConnectionsCounter = meter.upDownCounterBuilder("pending_connections")
                .setDescription("Number of pending forwarding connections")
                .setUnit("{connections}")
                .build();
        
        // Initialize circuit breaker metrics
        circuitBreakerStateCounter = meter.upDownCounterBuilder("circuit_breaker_state")
                .setDescription("State of circuit breakers (0=closed, 1=open, 2=half-open)")
                .setUnit("{state}")
                .build();
    }
    
    /**
     * Records a successful position forwarding operation.
     *
     * @param destination the destination identifier
     * @param protocol the protocol used
     * @param latencyMs the operation latency in milliseconds
     */
    public void recordPositionForward(String destination, String protocol, double latencyMs) {
        Attributes attributes = Attributes.builder()
                .put("destination", destination)
                .put("protocol", protocol)
                .build();
        
        positionForwardCounter.add(1, attributes);
        positionLatencyHistogram.record(latencyMs, attributes);
    }
    
    /**
     * Records a position forwarding error.
     *
     * @param destination the destination identifier
     * @param protocol the protocol used
     * @param errorType the type of error
     */
    public void recordPositionError(String destination, String protocol, String errorType) {
        Attributes attributes = Attributes.builder()
                .put("destination", destination)
                .put("protocol", protocol)
                .put("error_type", errorType)
                .build();
        
        positionErrorCounter.add(1, attributes);
    }
    
    /**
     * Records a successful event forwarding operation.
     *
     * @param destination the destination identifier
     * @param eventType the type of event
     * @param latencyMs the operation latency in milliseconds
     */
    public void recordEventForward(String destination, String eventType, double latencyMs) {
        Attributes attributes = Attributes.builder()
                .put("destination", destination)
                .put("event_type", eventType)
                .build();
        
        eventForwardCounter.add(1, attributes);
        eventLatencyHistogram.record(latencyMs, attributes);
    }
    
    /**
     * Records an event forwarding error.
     *
     * @param destination the destination identifier
     * @param eventType the type of event
     * @param errorType the type of error
     */
    public void recordEventError(String destination, String eventType, String errorType) {
        Attributes attributes = Attributes.builder()
                .put("destination", destination)
                .put("event_type", eventType)
                .put("error_type", errorType)
                .build();
        
        eventErrorCounter.add(1, attributes);
    }
    
    /**
     * Records a successful network forwarding operation.
     *
     * @param destination the destination identifier
     * @param port the port used
     * @param isDatagram whether the message is a datagram
     * @param latencyMs the operation latency in milliseconds
     */
    public void recordNetworkForward(String destination, int port, boolean isDatagram, double latencyMs) {
        Attributes attributes = Attributes.builder()
                .put("destination", destination)
                .put("port", port)
                .put("transport", isDatagram ? "udp" : "tcp")
                .build();
        
        networkForwardCounter.add(1, attributes);
        networkLatencyHistogram.record(latencyMs, attributes);
    }
    
    /**
     * Records a network forwarding error.
     *
     * @param destination the destination identifier
     * @param port the port used
     * @param isDatagram whether the message is a datagram
     * @param errorType the type of error
     */
    public void recordNetworkError(String destination, int port, boolean isDatagram, String errorType) {
        Attributes attributes = Attributes.builder()
                .put("destination", destination)
                .put("port", port)
                .put("transport", isDatagram ? "udp" : "tcp")
                .put("error_type", errorType)
                .build();
        
        networkErrorCounter.add(1, attributes);
    }
    
    /**
     * Updates the count of active connections.
     *
     * @param destination the destination identifier
     * @param delta the change in active connections (positive or negative)
     */
    public void updateActiveConnections(String destination, long delta) {
        Attributes attributes = Attributes.builder()
                .put("destination", destination)
                .build();
        
        activeConnectionsCounter.add(delta, attributes);
    }
    
    /**
     * Updates the count of pending connections.
     *
     * @param destination the destination identifier
     * @param delta the change in pending connections (positive or negative)
     */
    public void updatePendingConnections(String destination, long delta) {
        Attributes attributes = Attributes.builder()
                .put("destination", destination)
                .build();
        
        pendingConnectionsCounter.add(delta, attributes);
    }
    
    /**
     * Updates the state of a circuit breaker.
     *
     * @param destination the destination identifier
     * @param service the service name
     * @param state the circuit breaker state (0=closed, 1=open, 2=half-open)
     */
    public void updateCircuitBreakerState(String destination, String service, int state) {
        Attributes attributes = Attributes.builder()
                .put("destination", destination)
                .put("service", service)
                .put("state_value", state)
                .build();
        
        // Reset counter first to ensure accurate state reporting
        circuitBreakerStateCounter.add(-1, attributes);
        circuitBreakerStateCounter.add(1, attributes);
    }
    
    /**
     * Gets the OpenTelemetry meter for custom metrics.
     *
     * @return the meter instance
     */
    public Meter getMeter() {
        return meter;
    }
}