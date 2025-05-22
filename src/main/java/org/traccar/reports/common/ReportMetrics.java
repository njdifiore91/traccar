/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.reports.common;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility for collecting and reporting metrics related to report generation performance and usage.
 * This component integrates with OpenTelemetry to provide standardized metrics collection for
 * monitoring report generation activities.
 * 
 * The metrics collected include:
 * - Report generation requests, completions, and errors (counters)
 * - Report generation duration (histogram)
 * - Report size distribution (histogram)
 * - Concurrent report generation (gauge)
 * - Report-specific metrics like item counts and database operations
 */
@Singleton
public class ReportMetrics {

    private final Meter meter;
    
    // Counters for tracking report generation requests and completions
    private final LongCounter reportGenerationRequests;
    private final LongCounter reportGenerationCompletions;
    private final LongCounter reportGenerationErrors;
    
    // Histogram for measuring report generation duration
    private final DoubleHistogram reportGenerationDuration;
    
    // Histogram for measuring report size
    private final DoubleHistogram reportSize;
    
    // Gauge for tracking concurrent report generation
    private final ObservableLongGauge concurrentReportGenerations;
    
    // Tracks the number of currently executing report generation tasks
    private final AtomicLong concurrentReportCount = new AtomicLong(0);
    
    // Maps report types to their active generation count for detailed concurrent metrics
    private final Map<String, AtomicLong> reportTypeCounters = new ConcurrentHashMap<>();

    /**
     * Creates a new instance of ReportMetrics with the provided OpenTelemetry meter.
     *
     * @param meter OpenTelemetry meter for creating and registering metrics
     */
    @Inject
    public ReportMetrics(Meter meter) {
        this.meter = meter;
        
        // Initialize counters
        reportGenerationRequests = meter.counterBuilder("report_generation_requests_total")
                .setDescription("Total number of report generation requests")
                .setUnit("{request}")
                .build();
        
        reportGenerationCompletions = meter.counterBuilder("report_generation_completions_total")
                .setDescription("Total number of successfully completed report generations")
                .setUnit("{report}")
                .build();
        
        reportGenerationErrors = meter.counterBuilder("report_generation_errors_total")
                .setDescription("Total number of report generation errors")
                .setUnit("{error}")
                .build();
        
        // Initialize histograms
        reportGenerationDuration = meter.histogramBuilder("report_generation_duration_seconds")
                .setDescription("Duration of report generation in seconds")
                .setUnit("s")
                .build();
        
        reportSize = meter.histogramBuilder("report_size_bytes")
                .setDescription("Size of generated reports in bytes")
                .setUnit("By")
                .build();
        
        // Initialize gauge for concurrent report generations
        concurrentReportGenerations = meter.gaugeBuilder("report_generation_concurrent")
                .setDescription("Number of reports currently being generated")
                .setUnit("{report}")
                .buildWithCallback(measurement -> {
                    // Record overall concurrent count
                    measurement.record(concurrentReportCount.get());
                    
                    // Record per-report-type concurrent counts
                    reportTypeCounters.forEach((reportType, count) -> {
                        measurement.record(
                            count.get(),
                            Attributes.of(AttributeKey.stringKey("report_type"), reportType)
                        );
                    });
                });
    }
    
    /**
     * Records the start of a report generation request.
     *
     * @param reportType Type of report being generated
     * @return Current timestamp in nanoseconds (for duration calculation)
     */
    public long recordReportGenerationStart(String reportType) {
        Attributes attributes = Attributes.of(AttributeKey.stringKey("report_type"), reportType);
        reportGenerationRequests.add(1, attributes);
        concurrentReportCount.incrementAndGet();
        
        // Track per-report-type concurrent count
        reportTypeCounters.computeIfAbsent(reportType, k -> new AtomicLong(0)).incrementAndGet();
        
        return System.nanoTime();
    }
    
    /**
     * Records the successful completion of a report generation.
     *
     * @param reportType Type of report that was generated
     * @param startTimeNanos Start time in nanoseconds (from recordReportGenerationStart)
     * @param sizeBytes Size of the generated report in bytes
     */
    public void recordReportGenerationSuccess(String reportType, long startTimeNanos, long sizeBytes) {
        Attributes attributes = Attributes.of(AttributeKey.stringKey("report_type"), reportType);
        reportGenerationCompletions.add(1, attributes);
        
        // Calculate duration in seconds
        double durationSeconds = (System.nanoTime() - startTimeNanos) / 1_000_000_000.0;
        reportGenerationDuration.record(durationSeconds, attributes);
        
        // Record report size
        reportSize.record(sizeBytes, attributes);
        
        // Decrement concurrent counts
        concurrentReportCount.decrementAndGet();
        decrementReportTypeCounter(reportType);
    }
    
    /**
     * Records a report generation error.
     *
     * @param reportType Type of report that failed
     * @param errorType Type of error that occurred
     * @param startTimeNanos Start time in nanoseconds (from recordReportGenerationStart)
     */
    public void recordReportGenerationError(String reportType, String errorType, long startTimeNanos) {
        Attributes attributes = Attributes.of(
                AttributeKey.stringKey("report_type"), reportType,
                AttributeKey.stringKey("error_type"), errorType);
        reportGenerationErrors.add(1, attributes);
        
        // Calculate duration in seconds even for errors
        double durationSeconds = (System.nanoTime() - startTimeNanos) / 1_000_000_000.0;
        reportGenerationDuration.record(durationSeconds, attributes);
        
        // Decrement concurrent counts
        concurrentReportCount.decrementAndGet();
        decrementReportTypeCounter(reportType);
    }
    
    /**
     * Helper method to safely decrement the per-report-type counter.
     * 
     * @param reportType Type of report
     */
    private void decrementReportTypeCounter(String reportType) {
        AtomicLong counter = reportTypeCounters.get(reportType);
        if (counter != null) {
            long count = counter.decrementAndGet();
            if (count <= 0) {
                // Clean up the map if no reports of this type are being generated
                reportTypeCounters.remove(reportType);
            }
        }
    }
    
    /**
     * Records metrics for a specific report type.
     *
     * @param reportType Type of report
     * @param count Number of items in the report
     */
    public void recordReportItemCount(String reportType, long count) {
        Attributes attributes = Attributes.of(AttributeKey.stringKey("report_type"), reportType);
        meter.counterBuilder("report_items_total")
                .setDescription("Total number of items in generated reports")
                .setUnit("{item}")
                .build()
                .add(count, attributes);
        
        // Also record as a histogram to track distribution of report sizes
        meter.histogramBuilder("report_items_count")
                .setDescription("Distribution of item counts in generated reports")
                .setUnit("{item}")
                .build()
                .record(count, attributes);
    }
    
    /**
     * Records metrics for database operations during report generation.
     *
     * @param reportType Type of report
     * @param operationType Type of database operation (e.g., "query", "fetch")
     * @param count Number of operations performed
     * @param durationSeconds Duration of operations in seconds
     */
    public void recordDatabaseOperations(String reportType, String operationType, long count, double durationSeconds) {
        Attributes attributes = Attributes.of(
                AttributeKey.stringKey("report_type"), reportType,
                AttributeKey.stringKey("operation_type"), operationType);
        
        meter.counterBuilder("report_database_operations_total")
                .setDescription("Total number of database operations during report generation")
                .setUnit("{operation}")
                .build()
                .add(count, attributes);
        
        meter.histogramBuilder("report_database_operation_duration_seconds")
                .setDescription("Duration of database operations during report generation")
                .setUnit("s")
                .build()
                .record(durationSeconds, attributes);
    }
    
    /**
     * Records metrics for template processing during report generation.
     *
     * @param templateType Type of template used (e.g., "excel", "pdf")
     * @param durationSeconds Duration of template processing in seconds
     */
    public void recordTemplateProcessing(String templateType, double durationSeconds) {
        Attributes attributes = Attributes.of(AttributeKey.stringKey("template_type"), templateType);
        
        meter.histogramBuilder("report_template_processing_duration_seconds")
                .setDescription("Duration of template processing during report generation")
                .setUnit("s")
                .build()
                .record(durationSeconds, attributes);
        
        // Also increment a counter for template processing operations
        meter.counterBuilder("report_template_processing_total")
                .setDescription("Total number of template processing operations")
                .setUnit("{operation}")
                .build()
                .add(1, attributes);
    }
    
    /**
     * Records metrics for memory usage during report generation.
     *
     * @param reportType Type of report
     * @param memoryBytes Memory used in bytes
     */
    public void recordMemoryUsage(String reportType, long memoryBytes) {
        Attributes attributes = Attributes.of(AttributeKey.stringKey("report_type"), reportType);
        
        meter.histogramBuilder("report_memory_usage_bytes")
                .setDescription("Memory usage during report generation")
                .setUnit("By")
                .build()
                .record(memoryBytes, attributes);
    }
    
    /**
     * Records metrics for external service calls during report generation (e.g., geocoding).
     *
     * @param reportType Type of report
     * @param serviceType Type of external service
     * @param count Number of service calls
     * @param durationSeconds Duration of service calls in seconds
     */
    public void recordExternalServiceCalls(String reportType, String serviceType, long count, double durationSeconds) {
        Attributes attributes = Attributes.of(
                AttributeKey.stringKey("report_type"), reportType,
                AttributeKey.stringKey("service_type"), serviceType);
        
        meter.counterBuilder("report_external_service_calls_total")
                .setDescription("Total number of external service calls during report generation")
                .setUnit("{call}")
                .build()
                .add(count, attributes);
        
        meter.histogramBuilder("report_external_service_duration_seconds")
                .setDescription("Duration of external service calls during report generation")
                .setUnit("s")
                .build()
                .record(durationSeconds, attributes);
    }
}