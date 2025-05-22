/*
 * Copyright 2022 - 2023 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.context.Scope;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.helper.model.PositionUtil;
import org.traccar.messaging.MessageProducer;
import org.traccar.metrics.MetricsService;
import org.traccar.model.Device;
import org.traccar.model.Report;
import org.traccar.storage.ObjectStorageClient;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * KML export provider for generating device track reports in KML format.
 * Supports both synchronous and asynchronous report generation with distributed tracing,
 * metrics collection, and object storage integration.
 */
@Singleton
public class KmlExportProvider {

    private final Storage storage;
    private final ServiceDiscoveryManager discoveryManager;
    private final MessageProducer messageProducer;
    private final ObjectStorageClient objectStorage;
    private final MetricsService metricsService;
    private final Tracer tracer;
    private final String serviceName = "kml-export-provider";
    
    /**
     * Creates a new KML export provider with required dependencies.
     *
     * @param storage Database storage for accessing device and position data
     * @param discoveryManager Service discovery for registration and dependency location
     * @param messageProducer Message broker client for async processing
     * @param objectStorage Object storage client for report persistence
     * @param metricsService Metrics collection service
     * @param tracer OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public KmlExportProvider(
            Storage storage,
            ServiceDiscoveryManager discoveryManager,
            MessageProducer messageProducer,
            ObjectStorageClient objectStorage,
            MetricsService metricsService,
            Tracer tracer) {
        this.storage = storage;
        this.discoveryManager = discoveryManager;
        this.messageProducer = messageProducer;
        this.objectStorage = objectStorage;
        this.metricsService = metricsService;
        this.tracer = tracer;
        
        // Register service with discovery manager
        discoveryManager.register(serviceName, "reporting");
        
        // Initialize metrics
        metricsService.registerCounter("reports.kml.generated", "Number of KML reports generated");
        metricsService.registerTimer("reports.kml.generation_time", "Time taken to generate KML reports");
        metricsService.registerCounter("reports.kml.errors", "Number of errors during KML report generation");
        metricsService.registerGauge("reports.kml.size", "Size of generated KML reports in bytes");
    }
    
    /**
     * Synchronously generates a KML report and writes it to the provided output stream.
     * This method maintains backward compatibility with the original implementation.
     *
     * @param outputStream Output stream to write the KML data to
     * @param deviceId Device ID to generate the report for
     * @param from Start date for the report period
     * @param to End date for the report period
     * @throws StorageException If there's an error accessing the storage
     */
    public void generate(
            OutputStream outputStream, long deviceId, Date from, Date to) throws StorageException {
        
        // Create a span for tracing this operation
        Span span = tracer.spanBuilder("KmlExportProvider.generate")
                .setAttribute("deviceId", deviceId)
                .setAttribute("from", from.getTime())
                .setAttribute("to", to.getTime())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Record metrics - start timer
            long startTime = System.currentTimeMillis();
            
            // Fetch device and positions data
            var device = storage.getObject(Device.class, new Request(
                    new Columns.All(), new Condition.Equals("id", deviceId)));
            var positions = PositionUtil.getPositions(storage, deviceId, from, to);
            
            // Generate KML content
            var dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
            
            try (PrintWriter writer = new PrintWriter(outputStream)) {
                writeKmlContent(writer, device, positions, dateFormat, from, to);
                
                // Record metrics - success
                metricsService.incrementCounter("reports.kml.generated");
                metricsService.recordTimer("reports.kml.generation_time", System.currentTimeMillis() - startTime);
                
                // Add trace attributes for the result
                span.setAttribute("report.positions", positions.size());
                span.setAttribute("report.status", "success");
            }
        } catch (Exception e) {
            // Record metrics - failure
            metricsService.incrementCounter("reports.kml.errors");
            
            // Record error in span
            span.recordException(e);
            span.setAttribute("report.status", "error");
            
            // Re-throw the exception
            if (e instanceof StorageException) {
                throw (StorageException) e;
            } else {
                throw new StorageException(e);
            }
        } finally {
            span.end();
        }
    }
    
    /**
     * Asynchronously generates a KML report and stores it in object storage.
     * Returns a CompletableFuture that will be completed with a signed URL to access the report.
     *
     * @param deviceId Device ID to generate the report for
     * @param from Start date for the report period
     * @param to End date for the report period
     * @param userId User ID requesting the report
     * @return CompletableFuture with a signed URL to access the report
     */
    public CompletableFuture<String> generateAsync(
            long deviceId, Date from, Date to, long userId) {
        
        // Create a parent span for the async operation
        Span parentSpan = tracer.spanBuilder("KmlExportProvider.generateAsync")
                .setAttribute("deviceId", deviceId)
                .setAttribute("userId", userId)
                .setAttribute("from", from.getTime())
                .setAttribute("to", to.getTime())
                .startSpan();
        
        try (Scope scope = parentSpan.makeCurrent()) {
            // Create a unique report ID
            String reportId = UUID.randomUUID().toString();
            parentSpan.setAttribute("reportId", reportId);
            
            // Create a report request message
            Report reportRequest = new Report();
            reportRequest.setId(reportId);
            reportRequest.setDeviceId(deviceId);
            reportRequest.setUserId(userId);
            reportRequest.setType("kml");
            reportRequest.set("from", from.getTime());
            reportRequest.set("to", to.getTime());
            reportRequest.setStatus(Report.STATUS_PENDING);
            
            // Store the report request in the database
            try {
                storage.addObject(reportRequest);
            } catch (StorageException e) {
                parentSpan.recordException(e);
                parentSpan.setAttribute("report.status", "error.storage");
                parentSpan.end();
                CompletableFuture<String> future = new CompletableFuture<>();
                future.completeExceptionally(e);
                return future;
            }
            
            // Create a CompletableFuture to return
            CompletableFuture<String> resultFuture = new CompletableFuture<>();
            
            // Publish the report request to the message broker
            // The message includes the trace context for distributed tracing
            Context context = Context.current();
            messageProducer.publishReportRequest(reportRequest, context)
                    .thenAccept(success -> {
                        if (success) {
                            parentSpan.setAttribute("report.status", "queued");
                            // The report will be processed asynchronously by a consumer
                            // Return a URL that can be used to check the status and download the report
                            String reportUrl = "/api/reports/" + reportId;
                            resultFuture.complete(reportUrl);
                        } else {
                            parentSpan.setAttribute("report.status", "error.queue");
                            resultFuture.completeExceptionally(
                                    new RuntimeException("Failed to queue report generation"));
                        }
                        parentSpan.end();
                    })
                    .exceptionally(e -> {
                        parentSpan.recordException(e);
                        parentSpan.setAttribute("report.status", "error.queue");
                        parentSpan.end();
                        resultFuture.completeExceptionally(e);
                        return null;
                    });
            
            return resultFuture;
        }
    }
    
    /**
     * Processes a report request from the message queue.
     * This method is called by the report consumer service.
     *
     * @param reportId ID of the report to process
     * @param deviceId Device ID to generate the report for
     * @param from Start date for the report period
     * @param to End date for the report period
     * @param context Tracing context from the message
     * @return CompletableFuture that completes when the report is processed
     */
    public CompletableFuture<Void> processReportRequest(
            String reportId, long deviceId, Date from, Date to, Context context) {
        
        // Create a child span from the context
        Span span = tracer.spanBuilder("KmlExportProvider.processReportRequest")
                .setParent(context)
                .setAttribute("reportId", reportId)
                .setAttribute("deviceId", deviceId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Record metrics - start timer
            long startTime = System.currentTimeMillis();
            
            // Update report status to processing
            try {
                Report report = storage.getObject(Report.class, new Request(
                        new Columns.All(), new Condition.Equals("id", reportId)));
                report.setStatus(Report.STATUS_PROCESSING);
                storage.updateObject(report);
            } catch (StorageException e) {
                span.recordException(e);
                span.setAttribute("report.status", "error.storage");
                span.end();
                return CompletableFuture.failedFuture(e);
            }
            
            // Generate the KML report
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try {
                // Fetch device and positions data
                var device = storage.getObject(Device.class, new Request(
                        new Columns.All(), new Condition.Equals("id", deviceId)));
                var positions = PositionUtil.getPositions(storage, deviceId, from, to);
                
                // Generate KML content
                var dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                
                try (PrintWriter writer = new PrintWriter(outputStream)) {
                    writeKmlContent(writer, device, positions, dateFormat, from, to);
                }
                
                // Record metrics
                int reportSize = outputStream.size();
                metricsService.incrementCounter("reports.kml.generated");
                metricsService.recordTimer("reports.kml.generation_time", System.currentTimeMillis() - startTime);
                metricsService.setGauge("reports.kml.size", reportSize);
                
                // Add trace attributes for the result
                span.setAttribute("report.size", reportSize);
                span.setAttribute("report.positions", positions.size());
            } catch (Exception e) {
                // Record metrics - failure
                metricsService.incrementCounter("reports.kml.errors");
                
                // Record error in span
                span.recordException(e);
                span.setAttribute("report.status", "error.generation");
                
                // Update report status to error
                try {
                    Report report = storage.getObject(Report.class, new Request(
                            new Columns.All(), new Condition.Equals("id", reportId)));
                    report.setStatus(Report.STATUS_ERROR);
                    report.set("error", e.getMessage());
                    storage.updateObject(report);
                } catch (StorageException se) {
                    span.recordException(se);
                }
                
                span.end();
                return CompletableFuture.failedFuture(e);
            }
            
            // Store the report in object storage
            String objectKey = "reports/kml/" + reportId + ".kml";
            return objectStorage.putObject(objectKey, outputStream.toByteArray(), "application/vnd.google-earth.kml+xml")
                    .thenCompose(objectUrl -> {
                        // Update report status to complete
                        try {
                            Report report = storage.getObject(Report.class, new Request(
                                    new Columns.All(), new Condition.Equals("id", reportId)));
                            report.setStatus(Report.STATUS_COMPLETE);
                            report.set("url", objectUrl);
                            storage.updateObject(report);
                            
                            // Add trace attributes for success
                            span.setAttribute("report.status", "complete");
                            span.setAttribute("report.url", objectUrl);
                            
                            return CompletableFuture.completedFuture(null);
                        } catch (StorageException e) {
                            span.recordException(e);
                            span.setAttribute("report.status", "error.storage");
                            return CompletableFuture.failedFuture(e);
                        } finally {
                            span.end();
                        }
                    })
                    .exceptionally(e -> {
                        // Record error in span
                        span.recordException(e);
                        span.setAttribute("report.status", "error.storage");
                        
                        // Update report status to error
                        try {
                            Report report = storage.getObject(Report.class, new Request(
                                    new Columns.All(), new Condition.Equals("id", reportId)));
                            report.setStatus(Report.STATUS_ERROR);
                            report.set("error", e.getMessage());
                            storage.updateObject(report);
                        } catch (StorageException se) {
                            span.recordException(se);
                        }
                        
                        span.end();
                        throw new RuntimeException(e);
                    });
        }
    }
    
    /**
     * Writes KML content to the provided writer.
     * Extracted as a separate method to avoid code duplication.
     *
     * @param writer PrintWriter to write the KML content to
     * @param device Device information
     * @param positions List of positions to include in the KML
     * @param dateFormat Date formatter for timestamps
     * @param from Start date for the report period
     * @param to End date for the report period
     */
    private void writeKmlContent(
            PrintWriter writer, Device device, java.util.List<org.traccar.model.Position> positions,
            SimpleDateFormat dateFormat, Date from, Date to) {
        
        writer.print("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        writer.print("<kml xmlns=\"http://www.opengis.net/kml/2.2\">");
        writer.print("<Document>");
        writer.print("<name>");
        writer.print(device.getName());
        writer.print("</name>");
        writer.print("<Placemark>");
        writer.print("<name>");
        writer.print(dateFormat.format(from));
        writer.print(" - ");
        writer.print(dateFormat.format(to));
        writer.print("</name>");
        writer.print("<LineString>");
        writer.print("<extrude>1</extrude>");
        writer.print("<tessellate>1</tessellate>");
        writer.print("<altitudeMode>absolute</altitudeMode>");
        writer.print("<coordinates>");
        writer.print(positions.stream()
                .map((p -> String.format("%f,%f,%f", p.getLongitude(), p.getLatitude(), p.getAltitude())))
                .collect(Collectors.joining(" ")));
        writer.print("</coordinates>");
        writer.print("</LineString>");
        writer.print("</Placemark>");
        writer.print("</Document>");
        writer.print("</kml>");
    }
    
    /**
     * Checks if the service and its dependencies are healthy.
     * Used for Kubernetes health probes.
     *
     * @return true if the service is healthy, false otherwise
     */
    public boolean isHealthy() {
        try {
            // Check if storage is accessible
            storage.getObjects(Device.class, new Request(
                    new Columns.All(), new Condition.Equals("id", 0)));
            
            // Check if object storage is accessible
            objectStorage.checkHealth();
            
            // Check if message broker is accessible
            boolean brokerHealthy = messageProducer.checkHealth();
            
            return brokerHealthy;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Checks if the service is ready to accept requests.
     * Used for Kubernetes readiness probes.
     *
     * @return true if the service is ready, false otherwise
     */
    public boolean isReady() {
        return isHealthy() && discoveryManager.isRegistered(serviceName);
    }
}