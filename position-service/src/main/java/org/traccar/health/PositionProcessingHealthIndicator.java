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
package org.traccar.health;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitors the position processing pipeline health by tracking position processing rates,
 * drop ratios, and processing latencies. This component detects issues such as processing backlogs,
 * high error rates, or pipeline stalls, reporting detailed health status to Kubernetes.
 */
@Component
@Singleton
@ConfigurationProperties(prefix = "position.processing.health")
public class PositionProcessingHealthIndicator implements HealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionProcessingHealthIndicator.class);

    private final MeterRegistry meterRegistry;
    
    // Metrics counters
    private final Counter positionsReceivedCounter;
    private final Counter positionsProcessedCounter;
    private final Counter positionsFilteredCounter;
    private final Counter positionsErrorCounter;
    private final Timer processingLatencyTimer;
    
    // Atomic values for rate calculations
    private final AtomicInteger lastProcessedTotal = new AtomicInteger(0);
    private final AtomicInteger lastReceivedTotal = new AtomicInteger(0);
    private final AtomicInteger currentProcessingRate = new AtomicInteger(0);
    private final AtomicInteger currentReceivingRate = new AtomicInteger(0);
    private final AtomicLong lastCalculationTime = new AtomicLong(System.currentTimeMillis());
    
    // Health status thresholds (configurable)
    private double dropRatioThreshold = 0.1; // 10% drop ratio threshold
    private long maxProcessingLatencyMs = 5000; // 5 seconds max processing latency
    private int minProcessingRateThreshold = 1; // Minimum positions per minute
    private long processingStallThresholdMs = 60000; // 1 minute stall detection
    
    // Last processing timestamp to detect stalls
    private final AtomicLong lastProcessingTimestamp = new AtomicLong(System.currentTimeMillis());

    @Inject
    public PositionProcessingHealthIndicator(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        positionsReceivedCounter = Counter.builder("position.messages.received.total")
                .description("Total number of position messages received")
                .register(meterRegistry);
        
        positionsProcessedCounter = Counter.builder("position.messages.processed.total")
                .description("Total number of position messages successfully processed")
                .register(meterRegistry);
        
        positionsFilteredCounter = Counter.builder("position.messages.filtered.total")
                .description("Total number of position messages filtered out")
                .register(meterRegistry);
        
        positionsErrorCounter = Counter.builder("position.messages.error.total")
                .description("Total number of position messages that encountered errors during processing")
                .register(meterRegistry);
        
        processingLatencyTimer = Timer.builder("position.processing.latency")
                .description("Position processing latency in milliseconds")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
        
        // Register gauges for derived metrics
        Gauge.builder("position.processing.drop.ratio", this, PositionProcessingHealthIndicator::calculateDropRatio)
                .description("Ratio of positions dropped or errored to total positions received")
                .register(meterRegistry);
        
        Gauge.builder("position.processing.rate", currentProcessingRate, AtomicInteger::get)
                .description("Current rate of positions being processed per minute")
                .register(meterRegistry);
        
        Gauge.builder("position.receiving.rate", currentReceivingRate, AtomicInteger::get)
                .description("Current rate of positions being received per minute")
                .register(meterRegistry);
        
        Gauge.builder("position.processing.backlog", this, PositionProcessingHealthIndicator::calculateBacklog)
                .description("Current backlog of positions waiting to be processed")
                .register(meterRegistry);
        
        // Start background thread for rate calculations
        Thread rateCalculator = new Thread(this::calculateRates);
        rateCalculator.setDaemon(true);
        rateCalculator.setName("position-rate-calculator");
        rateCalculator.start();
        
        LOGGER.info("Position processing health indicator initialized with drop ratio threshold: {}", dropRatioThreshold);
    }
    
    /**
     * Records a position message being received for processing
     */
    public void recordPositionReceived() {
        positionsReceivedCounter.increment();
    }
    
    /**
     * Records a position message being successfully processed
     */
    public void recordPositionProcessed() {
        positionsProcessedCounter.increment();
        lastProcessingTimestamp.set(System.currentTimeMillis());
    }
    
    /**
     * Records a position message being filtered out
     */
    public void recordPositionFiltered() {
        positionsFilteredCounter.increment();
    }
    
    /**
     * Records a position message encountering an error during processing
     */
    public void recordPositionError() {
        positionsErrorCounter.increment();
    }
    
    /**
     * Records the processing latency for a position message
     * 
     * @param latencyMs Processing latency in milliseconds
     */
    public void recordProcessingLatency(long latencyMs) {
        processingLatencyTimer.record(Duration.ofMillis(latencyMs));
    }
    
    /**
     * Calculates the current drop ratio (positions not successfully processed / total positions)
     * 
     * @return The current drop ratio as a value between 0.0 and 1.0
     */
    private double calculateDropRatio() {
        double received = positionsReceivedCounter.count();
        double processed = positionsProcessedCounter.count();
        
        if (received == 0) {
            return 0.0;
        }
        
        return Math.max(0.0, Math.min(1.0, 1.0 - (processed / received)));
    }
    
    /**
     * Calculates the current backlog of positions waiting to be processed
     * 
     * @return The estimated number of positions in the processing backlog
     */
    private int calculateBacklog() {
        return (int) Math.max(0, positionsReceivedCounter.count() - 
                (positionsProcessedCounter.count() + positionsFilteredCounter.count()));
    }
    
    /**
     * Background thread method that periodically calculates processing rates
     */
    private void calculateRates() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(60000); // Calculate rates every minute
                
                long now = System.currentTimeMillis();
                long elapsed = now - lastCalculationTime.getAndSet(now);
                
                if (elapsed > 0) {
                    // Calculate current processing rate (per minute)
                    int currentProcessed = (int) positionsProcessedCounter.count();
                    int processedDelta = currentProcessed - lastProcessedTotal.getAndSet(currentProcessed);
                    currentProcessingRate.set((int) (processedDelta * 60000 / elapsed));
                    
                    // Calculate current receiving rate (per minute)
                    int currentReceived = (int) positionsReceivedCounter.count();
                    int receivedDelta = currentReceived - lastReceivedTotal.getAndSet(currentReceived);
                    currentReceivingRate.set((int) (receivedDelta * 60000 / elapsed));
                    
                    LOGGER.debug("Position processing rates - Received: {}/min, Processed: {}/min, Drop ratio: {}",
                            currentReceivingRate.get(), currentProcessingRate.get(), calculateDropRatio());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOGGER.warn("Error calculating position processing rates", e);
            }
        }
    }

    @Override
    public Health health() {
        Health.Builder builder = new Health.Builder();
        
        // Calculate current health metrics
        double dropRatio = calculateDropRatio();
        int backlog = calculateBacklog();
        double p95Latency = processingLatencyTimer.takeSnapshot().percentileValues()[1].value(TimeUnit.MILLISECONDS);
        long timeSinceLastProcessing = System.currentTimeMillis() - lastProcessingTimestamp.get();
        
        // Add detailed metrics to health status
        builder.withDetail("dropRatio", dropRatio)
               .withDetail("backlog", backlog)
               .withDetail("processingRate", currentProcessingRate.get())
               .withDetail("receivingRate", currentReceivingRate.get())
               .withDetail("p95LatencyMs", p95Latency)
               .withDetail("timeSinceLastProcessingMs", timeSinceLastProcessing);
        
        // Determine health status based on thresholds
        if (dropRatio > dropRatioThreshold) {
            LOGGER.warn("Position processing health check failed: drop ratio {} exceeds threshold {}", 
                    dropRatio, dropRatioThreshold);
            return builder.status(Status.DOWN)
                    .withDetail("reason", "Drop ratio exceeds threshold")
                    .build();
        }
        
        if (p95Latency > maxProcessingLatencyMs) {
            LOGGER.warn("Position processing health check failed: p95 latency {} ms exceeds threshold {} ms", 
                    p95Latency, maxProcessingLatencyMs);
            return builder.status(Status.DOWN)
                    .withDetail("reason", "Processing latency exceeds threshold")
                    .build();
        }
        
        if (currentReceivingRate.get() > minProcessingRateThreshold && 
                currentProcessingRate.get() < minProcessingRateThreshold) {
            LOGGER.warn("Position processing health check failed: processing rate {} below threshold {} while receiving {} positions/min", 
                    currentProcessingRate.get(), minProcessingRateThreshold, currentReceivingRate.get());
            return builder.status(Status.DOWN)
                    .withDetail("reason", "Processing rate below threshold")
                    .build();
        }
        
        if (currentReceivingRate.get() > 0 && timeSinceLastProcessing > processingStallThresholdMs) {
            LOGGER.warn("Position processing health check failed: no positions processed in {} ms", 
                    timeSinceLastProcessing);
            return builder.status(Status.DOWN)
                    .withDetail("reason", "Processing pipeline stalled")
                    .build();
        }
        
        return builder.up().build();
    }
    
    // Setter methods for configuration properties
    
    public void setDropRatioThreshold(double dropRatioThreshold) {
        this.dropRatioThreshold = dropRatioThreshold;
        LOGGER.info("Position processing drop ratio threshold set to: {}", dropRatioThreshold);
    }
    
    public void setMaxProcessingLatencyMs(long maxProcessingLatencyMs) {
        this.maxProcessingLatencyMs = maxProcessingLatencyMs;
        LOGGER.info("Position processing max latency threshold set to: {} ms", maxProcessingLatencyMs);
    }
    
    public void setMinProcessingRateThreshold(int minProcessingRateThreshold) {
        this.minProcessingRateThreshold = minProcessingRateThreshold;
        LOGGER.info("Position processing minimum rate threshold set to: {} positions/min", minProcessingRateThreshold);
    }
    
    public void setProcessingStallThresholdMs(long processingStallThresholdMs) {
        this.processingStallThresholdMs = processingStallThresholdMs;
        LOGGER.info("Position processing stall threshold set to: {} ms", processingStallThresholdMs);
    }
}