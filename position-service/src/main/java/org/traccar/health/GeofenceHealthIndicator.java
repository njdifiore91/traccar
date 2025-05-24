/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.health;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.model.GeofenceUtil;
import org.traccar.model.Geofence;
import org.traccar.session.cache.CacheManager;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Health indicator for geofence processing.
 * Monitors the health of geofence calculations, memory usage, and processing latencies
 * for the Position Processing Service.
 * <p>
 * This component is used by Kubernetes health probes to determine if the service
 * is functioning correctly with respect to geofence processing capabilities.
 */
@Component
public class GeofenceHealthIndicator implements HealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeofenceHealthIndicator.class);

    private static final String CALCULATION_PERFORMANCE_KEY = "calculationPerformance";
    private static final String MEMORY_USAGE_KEY = "memoryUsage";
    private static final String PROCESSING_LATENCY_KEY = "processingLatency";
    private static final String GEOFENCE_COUNT_KEY = "geofenceCount";
    private static final String CALCULATION_COUNT_KEY = "calculationCount";
    private static final String ERROR_COUNT_KEY = "errorCount";
    private static final String HEAP_MEMORY_KEY = "heapMemory";
    private static final String NON_HEAP_MEMORY_KEY = "nonHeapMemory";
    
    private static final int LATENCY_THRESHOLD_MS = 100; // Default threshold for geofence calculation latency
    private static final double MEMORY_THRESHOLD_PERCENT = 85.0; // Default threshold for memory usage
    private static final int ERROR_THRESHOLD = 10; // Default threshold for error count

    @Autowired
    private Config config;
    
    @Autowired
    private CacheManager cacheManager;
    
    @Autowired
    private MeterRegistry meterRegistry;
    
    // Metrics for tracking geofence processing
    private final AtomicInteger geofenceCount = new AtomicInteger(0);
    private final AtomicLong calculationCount = new AtomicLong(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);
    private final AtomicLong totalProcessingTimeNanos = new AtomicLong(0);
    
    // Timer for measuring geofence calculation performance
    private Timer geofenceCalculationTimer;
    
    @Autowired
    public GeofenceHealthIndicator(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.geofenceCalculationTimer = Timer.builder("geofence.calculation.time")
                .description("Time taken for geofence calculations")
                .register(meterRegistry);
        
        // Register metrics with Micrometer for Prometheus scraping
        meterRegistry.gauge("geofence.count", geofenceCount);
        meterRegistry.gauge("geofence.calculations", calculationCount);
        meterRegistry.gauge("geofence.errors", errorCount);
        meterRegistry.gauge("geofence.latency.ms", this, GeofenceHealthIndicator::calculateAverageLatency);
    }

    /**
     * Provides health information about geofence processing.
     * This method is called by Spring Boot Actuator to determine the health status
     * of geofence processing for Kubernetes health probes.
     *
     * @return Health object containing status and detailed information about geofence processing health
     */
    @Override
    public Health health() {
        Health.Builder builder = new Health.Builder();
        
        try {
            // Get current geofence count from cache manager
            updateGeofenceCount();
            
            // Check memory usage
            Map<String, Object> memoryDetails = checkMemoryUsage();
            
            // Calculate average processing latency
            double avgLatencyMs = calculateAverageLatency();
            
            // Get configured thresholds or use defaults
            int latencyThresholdMs = config.getInteger(Keys.GEOFENCE_LATENCY_THRESHOLD, LATENCY_THRESHOLD_MS);
            double memoryThresholdPercent = config.getDouble(Keys.GEOFENCE_MEMORY_THRESHOLD, MEMORY_THRESHOLD_PERCENT);
            int errorThresholdCount = config.getInteger(Keys.GEOFENCE_ERROR_THRESHOLD, ERROR_THRESHOLD);
            
            // Determine health status based on thresholds
            boolean isHealthy = true;
            Map<String, Object> details = new HashMap<>();
            
            // Check latency threshold
            if (avgLatencyMs > latencyThresholdMs) {
                isHealthy = false;
                details.put("latencyExceeded", true);
                details.put("latencyThreshold", latencyThresholdMs);
                details.put("currentLatency", avgLatencyMs);
            }
            
            // Check memory threshold
            double heapUsedPercent = (double) memoryDetails.get("heapUsedPercent");
            if (heapUsedPercent > memoryThresholdPercent) {
                isHealthy = false;
                details.put("memoryExceeded", true);
                details.put("memoryThreshold", memoryThresholdPercent);
                details.put("currentMemoryUsage", heapUsedPercent);
            }
            
            // Check error threshold
            if (errorCount.get() > errorThresholdCount) {
                isHealthy = false;
                details.put("errorExceeded", true);
                details.put("errorThreshold", errorThresholdCount);
                details.put("currentErrors", errorCount.get());
            }
            
            // Build health response with detailed information
            builder.withDetail(CALCULATION_PERFORMANCE_KEY, Map.of(
                    CALCULATION_COUNT_KEY, calculationCount.get(),
                    ERROR_COUNT_KEY, errorCount.get(),
                    "averageLatencyMs", avgLatencyMs,
                    "p95LatencyMs", geofenceCalculationTimer.takeSnapshot().percentileValues()[0].value(TimeUnit.MILLISECONDS),
                    "p99LatencyMs", geofenceCalculationTimer.takeSnapshot().percentileValues()[1].value(TimeUnit.MILLISECONDS)
            ));
            
            builder.withDetail(MEMORY_USAGE_KEY, memoryDetails);
            builder.withDetail(PROCESSING_LATENCY_KEY, Map.of(
                    "averageMs", avgLatencyMs,
                    "thresholdMs", latencyThresholdMs
            ));
            builder.withDetail(GEOFENCE_COUNT_KEY, geofenceCount.get());
            
            if (isHealthy) {
                builder.up();
            } else {
                builder.down().withDetails(details);
                LOGGER.warn("Geofence processing health check failed: {}", details);
            }
            
        } catch (Exception e) {
            LOGGER.error("Error checking geofence health", e);
            builder.down()
                   .withDetail("error", e.getMessage())
                   .withDetail("errorType", e.getClass().getName());
        }
        
        return builder.build();
    }
    
    /**
     * Updates the count of geofences in the system
     */
    private void updateGeofenceCount() {
        try {
            List<Geofence> geofences = cacheManager.getGeofences();
            geofenceCount.set(geofences != null ? geofences.size() : 0);
        } catch (Exception e) {
            LOGGER.warn("Failed to get geofence count", e);
            // Don't update the count if there's an error
        }
    }
    
    /**
     * Checks memory usage for geofence data structures
     * @return Map containing memory usage details
     */
    private Map<String, Object> checkMemoryUsage() {
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemory = memoryBean.getHeapMemoryUsage();
        MemoryUsage nonHeapMemory = memoryBean.getNonHeapMemoryUsage();
        
        long heapUsed = heapMemory.getUsed();
        long heapMax = heapMemory.getMax();
        double heapUsedPercent = heapMax > 0 ? (double) heapUsed / heapMax * 100.0 : 0.0;
        
        Map<String, Object> heapDetails = Map.of(
                "used", heapUsed,
                "max", heapMax,
                "usedPercent", heapUsedPercent
        );
        
        Map<String, Object> nonHeapDetails = Map.of(
                "used", nonHeapMemory.getUsed(),
                "max", nonHeapMemory.getMax() > 0 ? nonHeapMemory.getMax() : nonHeapMemory.getCommitted()
        );
        
        Map<String, Object> result = new HashMap<>();
        result.put(HEAP_MEMORY_KEY, heapDetails);
        result.put(NON_HEAP_MEMORY_KEY, nonHeapDetails);
        result.put("heapUsedPercent", heapUsedPercent);
        
        return result;
    }
    
    /**
     * Calculates the average latency of geofence processing
     * @return Average latency in milliseconds
     */
    private double calculateAverageLatency() {
        long count = calculationCount.get();
        if (count > 0) {
            return (double) totalProcessingTimeNanos.get() / count / 1_000_000.0; // Convert to ms
        }
        return 0.0;
    }
    
    /**
     * Records a geofence calculation event with its duration.
     * This method should be called by the GeofenceHandler after each calculation.
     * 
     * @param durationNanos Duration of the calculation in nanoseconds
     */
    public void recordCalculation(long durationNanos) {
        calculationCount.incrementAndGet();
        totalProcessingTimeNanos.addAndGet(durationNanos);
        geofenceCalculationTimer.record(durationNanos, TimeUnit.NANOSECONDS);
    }
    
    /**
     * Records an error in geofence processing.
     * This method should be called when a geofence calculation fails.
     */
    public void recordError() {
        errorCount.incrementAndGet();
    }
    
    /**
     * Resets the error count.
     * This can be called periodically to clear transient errors.
     */
    public void resetErrorCount() {
        errorCount.set(0);
    }
}