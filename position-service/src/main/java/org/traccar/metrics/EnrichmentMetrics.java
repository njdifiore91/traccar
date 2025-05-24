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
import io.prometheus.client.Histogram;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Metrics related to position enrichment in the Position Service.
 * This class tracks enrichment operations such as geocoding, geolocation, distance calculation,
 * and attribute computation using Prometheus metrics. It provides methods to record enrichment
 * events and measure enrichment times, with metrics broken down by enrichment type.
 */
@Singleton
public class EnrichmentMetrics {

    private final CollectorRegistry registry;

    // Enrichment operation counters
    private final Counter enrichmentOperations;
    private final Counter enrichmentSuccesses;
    private final Counter enrichmentFailures;
    
    // Enrichment time histograms
    private final Histogram enrichmentDurations;

    /**
     * Initializes the enrichment metrics with the default collector registry.
     */
    @Inject
    public EnrichmentMetrics() {
        registry = CollectorRegistry.defaultRegistry;
        
        // Initialize counters for tracking enrichment operations
        enrichmentOperations = Counter.build()
                .name("position_enrichment_operations_total")
                .help("Total number of position enrichment operations")
                .labelNames("type")
                .register(registry);
        
        enrichmentSuccesses = Counter.build()
                .name("position_enrichment_successes_total")
                .help("Total number of successful position enrichment operations")
                .labelNames("type")
                .register(registry);
        
        enrichmentFailures = Counter.build()
                .name("position_enrichment_failures_total")
                .help("Total number of failed position enrichment operations")
                .labelNames("type")
                .register(registry);
        
        // Initialize histograms for measuring enrichment times
        enrichmentDurations = Histogram.build()
                .name("position_enrichment_duration_seconds")
                .help("Time taken to perform position enrichment operations")
                .labelNames("type")
                .buckets(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0) // From 1ms to 10s
                .register(registry);
    }

    /**
     * Records an enrichment operation.
     *
     * @param type The type of enrichment operation (geocoding, geolocation, distance, attributes)
     */
    public void recordEnrichmentOperation(String type) {
        enrichmentOperations.labels(type).inc();
    }

    /**
     * Records a successful enrichment operation.
     *
     * @param type The type of enrichment operation (geocoding, geolocation, distance, attributes)
     */
    public void recordEnrichmentSuccess(String type) {
        enrichmentSuccesses.labels(type).inc();
    }

    /**
     * Records a failed enrichment operation.
     *
     * @param type The type of enrichment operation (geocoding, geolocation, distance, attributes)
     */
    public void recordEnrichmentFailure(String type) {
        enrichmentFailures.labels(type).inc();
    }

    /**
     * Records the duration of an enrichment operation.
     *
     * @param type The type of enrichment operation (geocoding, geolocation, distance, attributes)
     * @param durationSeconds The time taken to perform the enrichment in seconds
     */
    public void recordEnrichmentDuration(String type, double durationSeconds) {
        enrichmentDurations.labels(type).observe(durationSeconds);
    }

    /**
     * Times and records the duration of an enrichment operation.
     *
     * @param type The type of enrichment operation (geocoding, geolocation, distance, attributes)
     * @param operation The operation to time
     * @param <V> The return type of the operation
     * @return The result of the operation
     * @throws Exception If the operation throws an exception
     */
    public <V> V timeEnrichment(String type, Callable<V> operation) throws Exception {
        recordEnrichmentOperation(type);
        long startTime = System.nanoTime();
        try {
            V result = operation.call();
            recordEnrichmentSuccess(type);
            return result;
        } catch (Exception e) {
            recordEnrichmentFailure(type);
            throw e;
        } finally {
            double elapsedSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
            recordEnrichmentDuration(type, elapsedSeconds);
        }
    }

    /**
     * Times and records the duration of an enrichment operation without a return value.
     *
     * @param type The type of enrichment operation (geocoding, geolocation, distance, attributes)
     * @param operation The operation to time
     * @throws Exception If the operation throws an exception
     */
    public void timeEnrichment(String type, Runnable operation) throws Exception {
        recordEnrichmentOperation(type);
        long startTime = System.nanoTime();
        try {
            operation.run();
            recordEnrichmentSuccess(type);
        } catch (Exception e) {
            recordEnrichmentFailure(type);
            throw e;
        } finally {
            double elapsedSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
            recordEnrichmentDuration(type, elapsedSeconds);
        }
    }
    
    /**
     * Records an asynchronous enrichment operation result.
     * This method should be used when the enrichment operation is asynchronous
     * and the result is received in a callback.
     *
     * @param type The type of enrichment operation (geocoding, geolocation, distance, attributes)
     * @param success Whether the operation was successful
     * @param durationSeconds The time taken to perform the enrichment in seconds
     */
    public void recordAsyncEnrichment(String type, boolean success, double durationSeconds) {
        recordEnrichmentOperation(type);
        if (success) {
            recordEnrichmentSuccess(type);
        } else {
            recordEnrichmentFailure(type);
        }
        recordEnrichmentDuration(type, durationSeconds);
    }

    /**
     * Constants for enrichment types.
     */
    public static final class EnrichmentType {
        public static final String GEOCODING = "geocoding";
        public static final String GEOLOCATION = "geolocation";
        public static final String DISTANCE = "distance";
        public static final String ATTRIBUTES = "attributes";
        public static final String SPEED_LIMIT = "speedlimit";
        public static final String GEOFENCE = "geofence";
        
        private EnrichmentType() {
            // Utility class
        }
    }
    
    /**
     * Starts timing an enrichment operation and returns a timer object that can be used
     * to stop the timer and record the result.
     *
     * @param type The type of enrichment operation
     * @return A timer object that can be used to stop the timer and record the result
     */
    public EnrichmentTimer startTimer(String type) {
        recordEnrichmentOperation(type);
        return new EnrichmentTimer(type);
    }
    
    /**
     * A timer for measuring the duration of enrichment operations.
     */
    public class EnrichmentTimer {
        private final String type;
        private final long startTimeNanos;
        
        private EnrichmentTimer(String type) {
            this.type = type;
            this.startTimeNanos = System.nanoTime();
        }
        
        /**
         * Stops the timer and records a successful enrichment operation.
         */
        public void success() {
            recordEnrichmentSuccess(type);
            double elapsedSeconds = (System.nanoTime() - startTimeNanos) / 1_000_000_000.0;
            recordEnrichmentDuration(type, elapsedSeconds);
        }
        
        /**
         * Stops the timer and records a failed enrichment operation.
         */
        public void failure() {
            recordEnrichmentFailure(type);
            double elapsedSeconds = (System.nanoTime() - startTimeNanos) / 1_000_000_000.0;
            recordEnrichmentDuration(type, elapsedSeconds);
        }
        
        /**
         * Gets the elapsed time in the specified time unit.
         *
         * @param unit The time unit to return the elapsed time in
         * @return The elapsed time in the specified unit
         */
        public long getElapsedTime(TimeUnit unit) {
            return unit.convert(System.nanoTime() - startTimeNanos, TimeUnit.NANOSECONDS);
        }
    }
}