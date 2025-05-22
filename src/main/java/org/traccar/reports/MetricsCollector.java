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
package org.traccar.reports;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableGauge;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Metrics collector for the Reporting Service using OpenTelemetry.
 * 
 * This class defines and collects metrics for report generation performance,
 * resource utilization, and error rates. It provides counters for tracking
 * report generation counts, gauges for monitoring resource usage, and
 * histograms for measuring report generation duration.
 */
@Singleton
public class MetricsCollector {

    private final MeterRegistry registry;
    private final Meter meter;
    
    // Counters for report generation counts by type
    private final Map<String, Counter> reportGenerationCounters = new ConcurrentHashMap<>();
    private final LongCounter reportGenerationCounter;
    
    // Timers for measuring report generation duration
    private final Map<String, Timer> reportGenerationTimers = new ConcurrentHashMap<>();
    
    // Gauges for monitoring resource usage
    private final AtomicLong activeReportGenerations = new AtomicLong(0);
    private final AtomicLong queuedReportGenerations = new AtomicLong(0);
    
    // Counters for tracking error rates
    private final Counter reportGenerationErrorCounter;
    
    /**
     * Constructs a new MetricsCollector with the specified meter registry and OpenTelemetry instance.
     *
     * @param registry The Micrometer registry for metrics collection
     * @param openTelemetry The OpenTelemetry instance for distributed tracing
     */
    @Inject
    public MetricsCollector(MeterRegistry registry, OpenTelemetry openTelemetry) {
        this.registry = registry;
        this.meter = openTelemetry.getMeter("org.traccar.reports");
        
        // Initialize OpenTelemetry counters
        reportGenerationCounter = meter.counterBuilder("report_generation_total")
                .setDescription("Total number of reports generated")
                .setUnit("reports")
                .build();
        
        // Initialize Micrometer gauges
        Gauge.builder("report_generation_active", activeReportGenerations, AtomicLong::get)
                .description("Number of currently active report generations")
                .register(registry);
                
        Gauge.builder("report_generation_queued", queuedReportGenerations, AtomicLong::get)
                .description("Number of queued report generation requests")
                .register(registry);
        
        // Initialize error counter
        reportGenerationErrorCounter = Counter.builder("report_generation_errors_total")
                .description("Total number of report generation errors")
                .register(registry);
    }
    
    /**
     * Records the start of a report generation process.
     *
     * @param reportType The type of report being generated
     * @return A timer.Sample that can be used to record the duration
     */
    public Timer.Sample recordReportGenerationStart(String reportType) {
        activeReportGenerations.incrementAndGet();
        getOrCreateReportCounter(reportType).increment();
        reportGenerationCounter.add(1, Attributes.builder().put("type", reportType).build());
        return Timer.start(registry);
    }
    
    /**
     * Records the completion of a report generation process.
     *
     * @param sample The timer.Sample from recordReportGenerationStart
     * @param reportType The type of report that was generated
     * @param success Whether the report generation was successful
     */
    public void recordReportGenerationEnd(Timer.Sample sample, String reportType, boolean success) {
        activeReportGenerations.decrementAndGet();
        sample.stop(getOrCreateReportTimer(reportType));
        
        if (!success) {
            reportGenerationErrorCounter.increment();
        }
    }
    
    /**
     * Updates the count of queued report generation requests.
     *
     * @param count The current count of queued requests
     */
    public void updateQueuedReportGenerations(long count) {
        queuedReportGenerations.set(count);
    }
    
    /**
     * Records an error during report generation.
     *
     * @param reportType The type of report that encountered an error
     * @param errorType The type of error that occurred
     */
    public void recordReportGenerationError(String reportType, String errorType) {
        Counter errorCounter = Counter.builder("report_generation_error")
                .tags(Tags.of(
                        Tag.of("report_type", reportType),
                        Tag.of("error_type", errorType)))
                .description("Report generation errors by type")
                .register(registry);
        errorCounter.increment();
    }
    
    /**
     * Records the size of a generated report.
     *
     * @param reportType The type of report
     * @param sizeBytes The size of the report in bytes
     */
    public void recordReportSize(String reportType, long sizeBytes) {
        registry.summary("report_size_bytes", 
                Tags.of(Tag.of("report_type", reportType)))
                .record(sizeBytes);
    }
    
    /**
     * Records the number of items included in a report.
     *
     * @param reportType The type of report
     * @param itemCount The number of items in the report
     */
    public void recordReportItemCount(String reportType, long itemCount) {
        registry.summary("report_item_count", 
                Tags.of(Tag.of("report_type", reportType)))
                .record(itemCount);
    }
    
    /**
     * Gets or creates a counter for tracking report generation by type.
     *
     * @param reportType The type of report
     * @return A Counter for the specified report type
     */
    private Counter getOrCreateReportCounter(String reportType) {
        return reportGenerationCounters.computeIfAbsent(reportType, type -> {
            return Counter.builder("report_generation_count")
                    .tags(Tags.of(Tag.of("report_type", type)))
                    .description("Number of reports generated by type")
                    .register(registry);
        });
    }
    
    /**
     * Gets or creates a timer for measuring report generation duration by type.
     *
     * @param reportType The type of report
     * @return A Timer for the specified report type
     */
    private Timer getOrCreateReportTimer(String reportType) {
        return reportGenerationTimers.computeIfAbsent(reportType, type -> {
            return Timer.builder("report_generation_duration")
                    .tags(Tags.of(Tag.of("report_type", type)))
                    .description("Time taken to generate reports by type")
                    .publishPercentiles(0.5, 0.95, 0.99)
                    .publishPercentileHistogram()
                    .register(registry);
        });
    }
}