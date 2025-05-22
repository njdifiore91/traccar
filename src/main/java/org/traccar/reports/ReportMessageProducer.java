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
package org.traccar.reports;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.traccar.metrics.MetricsCollector;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles publishing report generation requests to the message broker
 * and manages callbacks for asynchronous completion.
 */
@Singleton
public class ReportMessageProducer {

    private final Tracer tracer;
    private final MetricsCollector metricsCollector;
    
    private static final String TOPIC_NAME = "reports.requests";
    private static final String METRIC_PREFIX = "report.broker.";
    
    // Map to store pending report futures by report ID
    private final Map<String, CompletableFuture<String>> pendingReports = new ConcurrentHashMap<>();

    /**
     * Creates a new report message producer with required dependencies.
     *
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param metricsCollector Metrics collector for performance monitoring
     */
    @Inject
    public ReportMessageProducer(Tracer tracer, MetricsCollector metricsCollector) {
        this.tracer = tracer;
        this.metricsCollector = metricsCollector;
        
        // Register metrics for this producer
        this.metricsCollector.registerCounter(METRIC_PREFIX + "sent", "Number of report requests sent");
        this.metricsCollector.registerCounter(METRIC_PREFIX + "errors", "Number of report request send errors");
        this.metricsCollector.registerGauge(METRIC_PREFIX + "pending", "Number of pending report requests");
    }

    /**
     * Sends a report request to the message broker for asynchronous processing.
     *
     * @param request The report request to send
     * @param headers Message headers including trace context
     * @param future CompletableFuture to be completed when the report is generated
     */
    public void sendReportRequest(ReportRequest request, Map<String, String> headers, 
                                 CompletableFuture<String> future) {
        
        // Create a span for tracing this operation
        Span span = tracer.spanBuilder("report.broker.send").startSpan();
        
        try {
            span.setAttribute("reportId", request.getReportId());
            span.setAttribute("reportType", request.getReportType());
            span.setAttribute("topic", TOPIC_NAME);
            
            // Store the future for completion when the report is generated
            pendingReports.put(request.getReportId(), future);
            metricsCollector.incrementGauge(METRIC_PREFIX + "pending");
            
            // In a real implementation, this would publish to Kafka/RabbitMQ
            // For now, we'll simulate the message broker with a direct call to the consumer
            // This would be replaced with actual message broker integration
            
            // Simulate successful message send
            metricsCollector.incrementCounter(METRIC_PREFIX + "sent");
            
            // For testing/development, we can directly process the request
            // In production, this would be handled by a separate consumer service
            // simulateMessageConsumer(request, headers);
            
        } catch (Exception e) {
            // Record error metrics
            metricsCollector.incrementCounter(METRIC_PREFIX + "errors");
            span.recordException(e);
            
            // Remove the pending future and complete it exceptionally
            pendingReports.remove(request.getReportId());
            metricsCollector.decrementGauge(METRIC_PREFIX + "pending");
            future.completeExceptionally(e);
        } finally {
            span.end();
        }
    }

    /**
     * Handles report completion notification from the consumer.
     * This would be called by the message consumer when a report is complete.
     *
     * @param reportId The ID of the completed report
     * @param url The URL of the generated report, or null if there was an error
     * @param error The error that occurred, or null if the report was generated successfully
     */
    public void handleReportCompletion(String reportId, String url, Throwable error) {
        // Create a span for tracing this operation
        Span span = tracer.spanBuilder("report.broker.complete").startSpan();
        
        try {
            span.setAttribute("reportId", reportId);
            
            // Get the pending future for this report
            CompletableFuture<String> future = pendingReports.remove(reportId);
            if (future != null) {
                metricsCollector.decrementGauge(METRIC_PREFIX + "pending");
                
                if (error != null) {
                    // Complete the future exceptionally if there was an error
                    span.recordException(error);
                    future.completeExceptionally(error);
                } else {
                    // Complete the future with the report URL
                    span.setAttribute("reportUrl", url);
                    future.complete(url);
                }
            }
        } finally {
            span.end();
        }
    }

    /**
     * For testing/development only - simulates a message consumer processing the request.
     * This would be replaced with actual message broker integration in production.
     *
     * @param request The report request to process
     * @param headers Message headers including trace context
     */
    /*
    private void simulateMessageConsumer(ReportRequest request, Map<String, String> headers) {
        // This would be in a separate consumer service in production
        CompletableFuture.runAsync(() -> {
            try {
                // Simulate some processing delay
                Thread.sleep(1000);
                
                // Process the report request
                String url = reportProvider.processReportRequest(request, headers);
                
                // Complete the future with the report URL
                handleReportCompletion(request.getReportId(), url, null);
            } catch (Exception e) {
                // Complete the future exceptionally if there was an error
                handleReportCompletion(request.getReportId(), null, e);
            }
        });
    }
    */
}