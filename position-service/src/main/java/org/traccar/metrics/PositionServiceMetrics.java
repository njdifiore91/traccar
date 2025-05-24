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

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;
import io.prometheus.client.hotspot.DefaultExports;

import java.util.concurrent.Callable;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Central registry for all position-related metrics in the Position Service.
 * This class defines and manages Prometheus metrics for tracking position processing operations,
 * following standardized naming conventions. It serves as the main entry point for accessing
 * all metrics categories (position, enrichment, health) and provides the collector registry
 * for the metrics HTTP endpoint.
 * 
 * This class replaces the functionality previously provided by StatisticsManager for position-related
 * metrics, implementing a distributed, service-specific metrics collection approach as part of the
 * transition to a microservices architecture.
 * 
 * Metrics are exposed in OpenMetrics format via a /metrics endpoint, which is scraped by Prometheus
 * for centralized aggregation and monitoring.
 */
@Singleton
public class PositionServiceMetrics {

    private final CollectorRegistry registry;
    
    // Position metrics
    private final Counter positionsReceived;
    private final Counter positionsProcessed;
    private final Counter positionsRejected;
    private final Summary positionProcessingDuration;
    
    // Enrichment metrics
    private final Counter geocoderRequests;
    private final Counter geolocationRequests;
    private final Counter speedLimitRequests;
    private final Gauge activeGeofences;
    
    // Health metrics
    private final Gauge databaseConnections;
    private final Gauge messageQueueSize;
    private final Counter processingErrors;
    private final Gauge serviceStatus;

    /**
     * Initializes the metrics registry with all position service metrics.
     */
    @Inject
    public PositionServiceMetrics() {
        registry = CollectorRegistry.defaultRegistry;
        
        // Register JVM metrics
        DefaultExports.initialize();
        
        // Position metrics
        positionsReceived = Counter.build()
                .name("position_messages_received_total")
                .help("Total number of position messages received")
                .labelNames("protocol")
                .register(registry);
        
        positionsProcessed = Counter.build()
                .name("position_messages_processed_total")
                .help("Total number of position messages successfully processed")
                .labelNames("protocol")
                .register(registry);
        
        positionsRejected = Counter.build()
                .name("position_messages_rejected_total")
                .help("Total number of position messages rejected")
                .labelNames("protocol", "reason")
                .register(registry);
        
        positionProcessingDuration = Summary.build()
                .name("position_processing_duration_seconds")
                .help("Time taken to process position messages")
                .labelNames("protocol")
                .quantile(0.5, 0.05)   // Add 50th percentile with 5% error margin
                .quantile(0.9, 0.01)   // Add 90th percentile with 1% error margin
                .quantile(0.99, 0.001) // Add 99th percentile with 0.1% error margin
                .register(registry);
        
        // Enrichment metrics
        geocoderRequests = Counter.build()
                .name("geocoder_requests_total")
                .help("Total number of geocoder requests")
                .labelNames("status")
                .register(registry);
        
        geolocationRequests = Counter.build()
                .name("geolocation_requests_total")
                .help("Total number of geolocation requests")
                .labelNames("status")
                .register(registry);
        
        speedLimitRequests = Counter.build()
                .name("speedlimit_requests_total")
                .help("Total number of speed limit requests")
                .labelNames("status")
                .register(registry);
        
        activeGeofences = Gauge.build()
                .name("active_geofences")
                .help("Number of active geofences")
                .register(registry);
        
        // Health metrics
        databaseConnections = Gauge.build()
                .name("database_connections_active")
                .help("Number of active database connections")
                .register(registry);
        
        messageQueueSize = Gauge.build()
                .name("message_queue_size")
                .help("Current size of the message processing queue")
                .register(registry);
        
        processingErrors = Counter.build()
                .name("processing_errors_total")
                .help("Total number of errors during position processing")
                .labelNames("type")
                .register(registry);
                
        serviceStatus = Gauge.build()
                .name("service_status")
                .help("Current status of the position service (1=up, 0=down)")
                .register(registry);
    }

    /**
     * Gets the Prometheus collector registry.
     *
     * @return The collector registry containing all metrics
     */
    public CollectorRegistry getRegistry() {
        return registry;
    }

    /**
     * Records a received position message.
     *
     * @param protocol The protocol used for the position message
     */
    public void recordPositionReceived(String protocol) {
        positionsReceived.labels(protocol).inc();
    }

    /**
     * Records a successfully processed position message.
     *
     * @param protocol The protocol used for the position message
     */
    public void recordPositionProcessed(String protocol) {
        positionsProcessed.labels(protocol).inc();
    }

    /**
     * Records a rejected position message.
     *
     * @param protocol The protocol used for the position message
     * @param reason The reason for rejection
     */
    public void recordPositionRejected(String protocol, String reason) {
        positionsRejected.labels(protocol, reason).inc();
    }

    /**
     * Records the duration of position processing.
     *
     * @param protocol The protocol used for the position message
     * @param durationSeconds The time taken to process the position in seconds
     */
    public void recordPositionProcessingDuration(String protocol, double durationSeconds) {
        positionProcessingDuration.labels(protocol).observe(durationSeconds);
    }
    
    /**
     * Times and records the duration of a position processing operation.
     *
     * @param protocol The protocol used for the position message
     * @param operation The operation to time
     * @param <V> The return type of the operation
     * @return The result of the operation
     * @throws Exception If the operation throws an exception
     */
    public <V> V timePositionProcessing(String protocol, Callable<V> operation) throws Exception {
        long startTime = System.nanoTime();
        try {
            return operation.call();
        } finally {
            double elapsedSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
            recordPositionProcessingDuration(protocol, elapsedSeconds);
        }
    }

    /**
     * Records a geocoder request.
     *
     * @param status The status of the geocoder request ("success" or "error")
     */
    public void recordGeocoderRequest(String status) {
        geocoderRequests.labels(status).inc();
    }

    /**
     * Records a geolocation request.
     *
     * @param status The status of the geolocation request ("success" or "error")
     */
    public void recordGeolocationRequest(String status) {
        geolocationRequests.labels(status).inc();
    }

    /**
     * Records a speed limit request.
     *
     * @param status The status of the speed limit request ("success" or "error")
     */
    public void recordSpeedLimitRequest(String status) {
        speedLimitRequests.labels(status).inc();
    }

    /**
     * Updates the count of active geofences.
     *
     * @param count The current number of active geofences
     */
    public void updateActiveGeofences(int count) {
        activeGeofences.set(count);
    }

    /**
     * Updates the count of active database connections.
     *
     * @param count The current number of active database connections
     */
    public void updateDatabaseConnections(int count) {
        databaseConnections.set(count);
    }

    /**
     * Updates the size of the message processing queue.
     *
     * @param size The current size of the message queue
     */
    public void updateMessageQueueSize(int size) {
        messageQueueSize.set(size);
    }

    /**
     * Records a processing error.
     *
     * @param type The type of error that occurred
     */
    public void recordProcessingError(String type) {
        processingErrors.labels(type).inc();
    }
    
    /**
     * Updates the service status.
     *
     * @param isUp True if the service is up, false otherwise
     */
    public void updateServiceStatus(boolean isUp) {
        serviceStatus.set(isUp ? 1 : 0);
    }
}