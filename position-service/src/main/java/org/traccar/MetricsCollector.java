/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.metrics.EnrichmentMetrics;
import org.traccar.metrics.HealthMetrics;
import org.traccar.metrics.MetricsConfiguration;
import org.traccar.metrics.PositionMetrics;
import org.traccar.metrics.PositionServiceMetrics;

/**
 * Central metrics collector for the Position Processing Service.
 * 
 * This class serves as the main entry point for collecting and exposing metrics
 * related to position processing, including processing latency, throughput, and error rates.
 * It integrates with OpenTelemetry for distributed tracing and Prometheus for metrics exposure.
 */
@Singleton
public class MetricsCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollector.class);

    private final PositionServiceMetrics serviceMetrics;
    private final PositionMetrics positionMetrics;
    private final EnrichmentMetrics enrichmentMetrics;
    private final HealthMetrics healthMetrics;
    private final MetricsConfiguration metricsConfiguration;
    private final OpenTelemetry openTelemetry;
    private final Meter meter;
    private final Tracer tracer;

    /**
     * Histogram buckets for position processing latency in milliseconds.
     * These buckets are designed to capture the expected range of processing times
     * from sub-millisecond to several seconds for outliers.
     */
    private static final double[] LATENCY_BUCKETS = 
            {1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000};

    /**
     * Constructs a new MetricsCollector with the required dependencies.
     *
     * @param serviceMetrics Central registry for all position-related metrics
     * @param positionMetrics Metrics related to position processing
     * @param enrichmentMetrics Metrics related to position enrichment
     * @param healthMetrics Metrics related to service health
     * @param metricsConfiguration Configuration for the metrics subsystem
     * @param openTelemetry OpenTelemetry instance for distributed tracing and metrics
     */
    @Inject
    public MetricsCollector(
            PositionServiceMetrics serviceMetrics,
            PositionMetrics positionMetrics,
            EnrichmentMetrics enrichmentMetrics,
            HealthMetrics healthMetrics,
            MetricsConfiguration metricsConfiguration,
            OpenTelemetry openTelemetry) {
        
        this.serviceMetrics = serviceMetrics;
        this.positionMetrics = positionMetrics;
        this.enrichmentMetrics = enrichmentMetrics;
        this.healthMetrics = healthMetrics;
        this.metricsConfiguration = metricsConfiguration;
        this.openTelemetry = openTelemetry;
        
        this.meter = openTelemetry.getMeter("org.traccar.position");
        this.tracer = openTelemetry.getTracer("org.traccar.position");
        
        LOGGER.info("Metrics collector initialized for Position Service");
    }

    /**
     * Records a position received event.
     * 
     * @param deviceId The device ID associated with the position
     * @param protocol The protocol used to receive the position
     */
    public void recordPositionReceived(long deviceId, String protocol) {
        positionMetrics.incrementPositionsReceived(protocol);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.counterBuilder("position.received")
                .setDescription("Number of positions received")
                .setUnit("positions")
                .build()
                .add(1, Attributes.builder()
                        .put("device_id", String.valueOf(deviceId))
                        .put("protocol", protocol)
                        .build());
    }

    /**
     * Records a position processed event.
     * 
     * @param deviceId The device ID associated with the position
     * @param processingTimeMs The time taken to process the position in milliseconds
     */
    public void recordPositionProcessed(long deviceId, long processingTimeMs) {
        positionMetrics.recordPositionProcessed(processingTimeMs);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.counterBuilder("position.processed")
                .setDescription("Number of positions processed")
                .setUnit("positions")
                .build()
                .add(1, Attributes.builder()
                        .put("device_id", String.valueOf(deviceId))
                        .build());
                
        meter.histogramBuilder("position.processing.duration")
                .setDescription("Position processing duration")
                .setUnit("ms")
                .build()
                .record(processingTimeMs, Attributes.builder()
                        .put("device_id", String.valueOf(deviceId))
                        .build());
    }

    /**
     * Records a position processing error.
     * 
     * @param deviceId The device ID associated with the position
     * @param errorType The type of error that occurred
     */
    public void recordPositionError(long deviceId, String errorType) {
        positionMetrics.incrementPositionErrors(errorType);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.counterBuilder("position.errors")
                .setDescription("Number of position processing errors")
                .setUnit("errors")
                .build()
                .add(1, Attributes.builder()
                        .put("device_id", String.valueOf(deviceId))
                        .put("error_type", errorType)
                        .build());
    }

    /**
     * Records a position enrichment operation.
     * 
     * @param deviceId The device ID associated with the position
     * @param enrichmentType The type of enrichment performed (e.g., "geocoder", "geolocation")
     * @param durationMs The time taken to perform the enrichment in milliseconds
     * @param success Whether the enrichment was successful
     */
    public void recordEnrichment(long deviceId, String enrichmentType, long durationMs, boolean success) {
        if (success) {
            enrichmentMetrics.incrementEnrichmentSuccess(enrichmentType);
        } else {
            enrichmentMetrics.incrementEnrichmentFailure(enrichmentType);
        }
        
        enrichmentMetrics.recordEnrichmentDuration(enrichmentType, durationMs);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.counterBuilder("position.enrichment")
                .setDescription("Number of position enrichment operations")
                .setUnit("operations")
                .build()
                .add(1, Attributes.builder()
                        .put("device_id", String.valueOf(deviceId))
                        .put("enrichment_type", enrichmentType)
                        .put("success", String.valueOf(success))
                        .build());
                
        meter.histogramBuilder("position.enrichment.duration")
                .setDescription("Position enrichment duration")
                .setUnit("ms")
                .build()
                .record(durationMs, Attributes.builder()
                        .put("device_id", String.valueOf(deviceId))
                        .put("enrichment_type", enrichmentType)
                        .build());
    }

    /**
     * Records a message broker interaction.
     * 
     * @param operation The operation performed (e.g., "consume", "produce")
     * @param topic The message topic
     * @param success Whether the operation was successful
     * @param durationMs The time taken to perform the operation in milliseconds
     */
    public void recordBrokerInteraction(String operation, String topic, boolean success, long durationMs) {
        Counter counter = serviceMetrics.getRegistry().counter(
                "position_broker_operations_total",
                "Number of message broker operations",
                "operation", "topic", "success");
        
        counter.labels(operation, topic, String.valueOf(success)).inc();
        
        Histogram histogram = serviceMetrics.getRegistry().histogram(
                "position_broker_operation_duration_seconds",
                "Duration of message broker operations in seconds",
                "operation", "topic");
        
        histogram.labels(operation, topic).observe(durationMs / 1000.0);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.counterBuilder("position.broker.operations")
                .setDescription("Number of message broker operations")
                .setUnit("operations")
                .build()
                .add(1, Attributes.builder()
                        .put("operation", operation)
                        .put("topic", topic)
                        .put("success", String.valueOf(success))
                        .build());
                
        meter.histogramBuilder("position.broker.duration")
                .setDescription("Message broker operation duration")
                .setUnit("ms")
                .build()
                .record(durationMs, Attributes.builder()
                        .put("operation", operation)
                        .put("topic", topic)
                        .build());
    }

    /**
     * Records the current queue size for position processing.
     * 
     * @param queueName The name of the queue
     * @param size The current size of the queue
     */
    public void recordQueueSize(String queueName, long size) {
        Gauge gauge = serviceMetrics.getRegistry().gauge(
                "position_queue_size",
                "Current size of position processing queues",
                "queue");
        
        gauge.labels(queueName).set(size);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.gaugeBuilder("position.queue.size")
                .setDescription("Current size of position processing queues")
                .setUnit("messages")
                .buildWithCallback(
                        measurement -> measurement.record(size, Attributes.builder()
                                .put("queue", queueName)
                                .build()));
    }

    /**
     * Records the current processing rate (positions per second).
     * 
     * @param rate The current processing rate
     */
    public void recordProcessingRate(double rate) {
        Gauge gauge = serviceMetrics.getRegistry().gauge(
                "position_processing_rate",
                "Current rate of position processing in positions per second");
        
        gauge.set(rate);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.gaugeBuilder("position.processing.rate")
                .setDescription("Current rate of position processing")
                .setUnit("positions/s")
                .buildWithCallback(
                        measurement -> measurement.record(rate));
    }

    /**
     * Records the current health status of the service.
     * 
     * @param status The health status (1 for healthy, 0 for unhealthy)
     * @param component The component being checked
     */
    public void recordHealthStatus(int status, String component) {
        healthMetrics.setHealthStatus(component, status);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.gaugeBuilder("position.health.status")
                .setDescription("Health status of the position service components")
                .setUnit("status")
                .buildWithCallback(
                        measurement -> measurement.record(status, Attributes.builder()
                                .put("component", component)
                                .build()));
    }

    /**
     * Records resource utilization metrics.
     * 
     * @param resourceType The type of resource (e.g., "cpu", "memory")
     * @param utilization The utilization percentage (0-100)
     */
    public void recordResourceUtilization(String resourceType, double utilization) {
        Gauge gauge = serviceMetrics.getRegistry().gauge(
                "position_resource_utilization",
                "Resource utilization percentage",
                "resource");
        
        gauge.labels(resourceType).set(utilization);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.gaugeBuilder("position.resource.utilization")
                .setDescription("Resource utilization percentage")
                .setUnit("%")
                .buildWithCallback(
                        measurement -> measurement.record(utilization, Attributes.builder()
                                .put("resource", resourceType)
                                .build()));
    }

    /**
     * Gets the Prometheus collector registry.
     * 
     * @return The collector registry
     */
    public CollectorRegistry getRegistry() {
        return serviceMetrics.getRegistry();
    }

    /**
     * Gets the OpenTelemetry meter for creating custom metrics.
     * 
     * @return The OpenTelemetry meter
     */
    public Meter getMeter() {
        return meter;
    }

    /**
     * Gets the OpenTelemetry tracer for creating spans.
     * 
     * @return The OpenTelemetry tracer
     */
    public Tracer getTracer() {
        return tracer;
    }

    /**
     * Gets the position metrics component.
     * 
     * @return The position metrics component
     */
    public PositionMetrics getPositionMetrics() {
        return positionMetrics;
    }

    /**
     * Gets the enrichment metrics component.
     * 
     * @return The enrichment metrics component
     */
    public EnrichmentMetrics getEnrichmentMetrics() {
        return enrichmentMetrics;
    }

    /**
     * Gets the health metrics component.
     * 
     * @return The health metrics component
     */
    public HealthMetrics getHealthMetrics() {
        return healthMetrics;
    }
    
    /**
     * Records a position forwarding operation.
     * 
     * @param destinationType The type of destination (e.g., "event-service", "external-api")
     * @param success Whether the forwarding was successful
     * @param durationMs The time taken to forward the position in milliseconds
     */
    public void recordPositionForwarding(String destinationType, boolean success, long durationMs) {
        Counter counter = serviceMetrics.getRegistry().counter(
                "position_forwarding_total",
                "Number of position forwarding operations",
                "destination", "success");
        
        counter.labels(destinationType, String.valueOf(success)).inc();
        
        Histogram histogram = serviceMetrics.getRegistry().histogram(
                "position_forwarding_duration_seconds",
                "Duration of position forwarding operations in seconds",
                "destination");
        
        histogram.labels(destinationType).observe(durationMs / 1000.0);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.counterBuilder("position.forwarding")
                .setDescription("Number of position forwarding operations")
                .setUnit("operations")
                .build()
                .add(1, Attributes.builder()
                        .put("destination", destinationType)
                        .put("success", String.valueOf(success))
                        .build());
                
        meter.histogramBuilder("position.forwarding.duration")
                .setDescription("Position forwarding duration")
                .setUnit("ms")
                .build()
                .record(durationMs, Attributes.builder()
                        .put("destination", destinationType)
                        .build());
    }
    
    /**
     * Records a database operation related to position processing.
     * 
     * @param operation The database operation (e.g., "insert", "update", "query")
     * @param success Whether the operation was successful
     * @param durationMs The time taken to perform the operation in milliseconds
     */
    public void recordDatabaseOperation(String operation, boolean success, long durationMs) {
        Counter counter = serviceMetrics.getRegistry().counter(
                "position_database_operations_total",
                "Number of database operations related to position processing",
                "operation", "success");
        
        counter.labels(operation, String.valueOf(success)).inc();
        
        Histogram histogram = serviceMetrics.getRegistry().histogram(
                "position_database_operation_duration_seconds",
                "Duration of database operations in seconds",
                "operation");
        
        histogram.labels(operation).observe(durationMs / 1000.0);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.counterBuilder("position.database.operations")
                .setDescription("Number of database operations")
                .setUnit("operations")
                .build()
                .add(1, Attributes.builder()
                        .put("operation", operation)
                        .put("success", String.valueOf(success))
                        .build());
                
        meter.histogramBuilder("position.database.duration")
                .setDescription("Database operation duration")
                .setUnit("ms")
                .build()
                .record(durationMs, Attributes.builder()
                        .put("operation", operation)
                        .build());
    }
    
    /**
     * Records circuit breaker metrics for external service calls.
     * 
     * @param serviceName The name of the external service
     * @param state The state of the circuit breaker (e.g., "closed", "open", "half-open")
     * @param failureCount The number of failures that triggered the circuit breaker
     */
    public void recordCircuitBreakerMetrics(String serviceName, String state, long failureCount) {
        Gauge stateGauge = serviceMetrics.getRegistry().gauge(
                "position_circuit_breaker_state",
                "Current state of circuit breakers (0=closed, 1=half-open, 2=open)",
                "service");
        
        int stateValue = "closed".equals(state) ? 0 : ("half-open".equals(state) ? 1 : 2);
        stateGauge.labels(serviceName).set(stateValue);
        
        Gauge failureGauge = serviceMetrics.getRegistry().gauge(
                "position_circuit_breaker_failures",
                "Number of failures tracked by circuit breakers",
                "service");
        
        failureGauge.labels(serviceName).set(failureCount);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.gaugeBuilder("position.circuit_breaker.state")
                .setDescription("Current state of circuit breakers")
                .setUnit("state")
                .buildWithCallback(
                        measurement -> measurement.record(stateValue, Attributes.builder()
                                .put("service", serviceName)
                                .build()));
                
        meter.gaugeBuilder("position.circuit_breaker.failures")
                .setDescription("Number of failures tracked by circuit breakers")
                .setUnit("failures")
                .buildWithCallback(
                        measurement -> measurement.record(failureCount, Attributes.builder()
                                .put("service", serviceName)
                                .build()));
    }
    
    /**
     * Records geofence check metrics.
     * 
     * @param deviceId The device ID
     * @param geofenceCount The number of geofences checked
     * @param matchCount The number of geofences matched
     * @param durationMs The time taken to perform the geofence checks in milliseconds
     */
    public void recordGeofenceChecks(long deviceId, int geofenceCount, int matchCount, long durationMs) {
        Counter checksCounter = serviceMetrics.getRegistry().counter(
                "position_geofence_checks_total",
                "Number of geofence checks performed");
        
        checksCounter.inc(geofenceCount);
        
        Counter matchesCounter = serviceMetrics.getRegistry().counter(
                "position_geofence_matches_total",
                "Number of geofence matches found");
        
        matchesCounter.inc(matchCount);
        
        Histogram histogram = serviceMetrics.getRegistry().histogram(
                "position_geofence_check_duration_seconds",
                "Duration of geofence checking operations in seconds");
        
        histogram.observe(durationMs / 1000.0);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.counterBuilder("position.geofence.checks")
                .setDescription("Number of geofence checks performed")
                .setUnit("checks")
                .build()
                .add(geofenceCount, Attributes.builder()
                        .put("device_id", String.valueOf(deviceId))
                        .build());
                
        meter.counterBuilder("position.geofence.matches")
                .setDescription("Number of geofence matches found")
                .setUnit("matches")
                .build()
                .add(matchCount, Attributes.builder()
                        .put("device_id", String.valueOf(deviceId))
                        .build());
                
        meter.histogramBuilder("position.geofence.duration")
                .setDescription("Geofence checking duration")
                .setUnit("ms")
                .build()
                .record(durationMs, Attributes.builder()
                        .put("device_id", String.valueOf(deviceId))
                        .build());
    }
    
    /**
     * Records transaction outbox metrics for reliable message publishing.
     * 
     * @param status The status of the outbox operation (e.g., "queued", "published", "failed")
     * @param count The number of messages in the given status
     */
    public void recordTransactionOutboxMetrics(String status, long count) {
        Gauge gauge = serviceMetrics.getRegistry().gauge(
                "position_transaction_outbox",
                "Number of messages in the transaction outbox by status",
                "status");
        
        gauge.labels(status).set(count);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.gaugeBuilder("position.transaction_outbox")
                .setDescription("Number of messages in the transaction outbox")
                .setUnit("messages")
                .buildWithCallback(
                        measurement -> measurement.record(count, Attributes.builder()
                                .put("status", status)
                                .build()));
    }
    
    /**
     * Records metrics about the position processing pipeline.
     * 
     * @param handlerName The name of the handler in the pipeline
     * @param positionsProcessed The number of positions processed by this handler
     * @param durationMs The time taken by this handler in milliseconds
     */
    public void recordPipelineMetrics(String handlerName, long positionsProcessed, long durationMs) {
        Counter counter = serviceMetrics.getRegistry().counter(
                "position_pipeline_handler_total",
                "Number of positions processed by each pipeline handler",
                "handler");
        
        counter.labels(handlerName).inc(positionsProcessed);
        
        Histogram histogram = serviceMetrics.getRegistry().histogram(
                "position_pipeline_handler_duration_seconds",
                "Duration of each pipeline handler in seconds",
                "handler");
        
        histogram.labels(handlerName).observe(durationMs / 1000.0);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.counterBuilder("position.pipeline.processed")
                .setDescription("Number of positions processed by each pipeline handler")
                .setUnit("positions")
                .build()
                .add(positionsProcessed, Attributes.builder()
                        .put("handler", handlerName)
                        .build());
                
        meter.histogramBuilder("position.pipeline.duration")
                .setDescription("Pipeline handler duration")
                .setUnit("ms")
                .build()
                .record(durationMs, Attributes.builder()
                        .put("handler", handlerName)
                        .build());
    }
    
    /**
     * Records consumer lag metrics for message broker interaction.
     * 
     * @param topic The message topic
     * @param partition The partition within the topic
     * @param consumerGroup The consumer group
     * @param lag The current lag (difference between latest offset and consumer offset)
     */
    public void recordConsumerLag(String topic, int partition, String consumerGroup, long lag) {
        Gauge gauge = serviceMetrics.getRegistry().gauge(
                "position_consumer_lag",
                "Current lag for message consumers in messages",
                "topic", "partition", "consumer_group");
        
        gauge.labels(topic, String.valueOf(partition), consumerGroup).set(lag);
        
        // Also record in OpenTelemetry for correlation with traces
        meter.gaugeBuilder("position.consumer.lag")
                .setDescription("Current lag for message consumers")
                .setUnit("messages")
                .buildWithCallback(
                        measurement -> measurement.record(lag, Attributes.builder()
                                .put("topic", topic)
                                .put("partition", String.valueOf(partition))
                                .put("consumer_group", consumerGroup)
                                .build()));
        
        // Record a summary of lag over time
        Summary summary = serviceMetrics.getRegistry().summary(
                "position_consumer_lag_summary",
                "Summary of consumer lag over time",
                "topic", "consumer_group");
        
        summary.labels(topic, consumerGroup).observe(lag);
        
        // Alert if lag exceeds thresholds
        if (lag > 10000) {
            LOGGER.warn("High consumer lag detected: {} messages for topic {} partition {} consumer group {}", 
                    lag, topic, partition, consumerGroup);
            
            // Increment a counter for high lag events
            Counter counter = serviceMetrics.getRegistry().counter(
                    "position_high_consumer_lag_total",
                    "Number of high consumer lag events",
                    "topic", "consumer_group");
            
            counter.labels(topic, consumerGroup).inc();
        }
    }
}