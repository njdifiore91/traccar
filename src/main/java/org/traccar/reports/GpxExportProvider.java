/*
 * Copyright 2022 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.helper.DateUtil;
import org.traccar.helper.model.PositionUtil;
import org.traccar.metrics.MetricsCollector;
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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Provider for generating GPX format exports of device positions.
 * Supports both direct synchronous generation and asynchronous generation via message broker.
 */
@Singleton
public class GpxExportProvider {

    private final Storage storage;
    private final ObjectStorageClient objectStorage;
    private final ReportMessageProducer messageProducer;
    private final Tracer tracer;
    private final MetricsCollector metricsCollector;
    private final ServiceDiscoveryManager serviceDiscovery;

    private static final String REPORT_TYPE = "gpx";
    private static final String METRIC_PREFIX = "report.gpx.";
    private static final String HEALTH_CHECK_NAME = "gpx-export";

    /**
     * TextMapSetter for propagating trace context in message headers.
     */
    private static final TextMapSetter<Map<String, String>> SETTER = 
            (carrier, key, value) -> carrier.put(key, value);

    /**
     * TextMapGetter for extracting trace context from message headers.
     */
    private static final TextMapGetter<Map<String, String>> GETTER = 
            new TextMapGetter<>() {
                @Override
                public Iterable<String> keys(Map<String, String> carrier) {
                    return carrier.keySet();
                }

                @Override
                public String get(Map<String, String> carrier, String key) {
                    return carrier.get(key);
                }
            };

    /**
     * Creates a new GPX export provider with required dependencies.
     *
     * @param storage Database storage for retrieving device and position data
     * @param objectStorage Object storage client for storing generated reports
     * @param messageProducer Message producer for asynchronous report generation
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param metricsCollector Metrics collector for performance monitoring
     * @param serviceDiscovery Service discovery manager for health checks
     */
    @Inject
    public GpxExportProvider(
            Storage storage, 
            ObjectStorageClient objectStorage,
            ReportMessageProducer messageProducer,
            Tracer tracer,
            MetricsCollector metricsCollector,
            ServiceDiscoveryManager serviceDiscovery) {
        this.storage = storage;
        this.objectStorage = objectStorage;
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.metricsCollector = metricsCollector;
        this.serviceDiscovery = serviceDiscovery;
        
        // Register health check for this provider
        this.serviceDiscovery.registerHealthCheck(HEALTH_CHECK_NAME, this::checkHealth);
        
        // Register metrics for this provider
        this.metricsCollector.registerGauge(METRIC_PREFIX + "active", "Number of active GPX export operations");
        this.metricsCollector.registerCounter(METRIC_PREFIX + "total", "Total number of GPX exports generated");
        this.metricsCollector.registerCounter(METRIC_PREFIX + "errors", "Number of GPX export errors");
        this.metricsCollector.registerHistogram(METRIC_PREFIX + "duration", "GPX export generation time in milliseconds");
        this.metricsCollector.registerHistogram(METRIC_PREFIX + "size", "GPX export size in bytes");
    }

    /**
     * Synchronously generates a GPX export and writes it to the provided output stream.
     * This is the original direct method for backward compatibility.
     *
     * @param outputStream The output stream to write the GPX data to
     * @param deviceId The ID of the device to generate the report for
     * @param from The start date for positions to include
     * @param to The end date for positions to include
     * @throws StorageException If there is an error retrieving data from storage
     */
    public void generate(
            OutputStream outputStream, long deviceId, Date from, Date to) throws StorageException {
        
        // Create a span for tracing this operation
        Span span = tracer.spanBuilder("gpx.export.generate").startSpan();
        long startTime = System.currentTimeMillis();
        
        try {
            // Increment active exports metric
            metricsCollector.incrementGauge(METRIC_PREFIX + "active");
            
            span.setAttribute("deviceId", deviceId);
            span.setAttribute("fromDate", from.getTime());
            span.setAttribute("toDate", to.getTime());
            
            // Create a child span for device retrieval
            Span deviceSpan = tracer.spanBuilder("gpx.export.getDevice")
                    .setParent(Context.current().with(span))
                    .startSpan();
            
            Device device;
            try {
                device = storage.getObject(Device.class, new Request(
                        new Columns.All(), new Condition.Equals("id", deviceId)));
                deviceSpan.setAttribute("deviceName", device.getName());
            } finally {
                deviceSpan.end();
            }
            
            // Create a child span for position retrieval
            Span positionsSpan = tracer.spanBuilder("gpx.export.getPositions")
                    .setParent(Context.current().with(span))
                    .startSpan();
            
            var positions = PositionUtil.getPositions(storage, deviceId, from, to);
            positionsSpan.setAttribute("positionCount", positions.size());
            positionsSpan.end();
            
            // Create a child span for GPX generation
            Span formatSpan = tracer.spanBuilder("gpx.export.format")
                    .setParent(Context.current().with(span))
                    .startSpan();
            
            try (PrintWriter writer = new PrintWriter(outputStream)) {
                writer.print("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                writer.print("<gpx version=\"1.0\">");
                writer.print("<trk>");
                writer.print("<name>");
                writer.print(device.getName());
                writer.print("</name>");
                writer.print("<trkseg>");
                positions.forEach(position -> {
                    writer.print("<trkpt lat=\"");
                    writer.print(position.getLatitude());
                    writer.print("\" lon=\"");
                    writer.print(position.getLongitude());
                    writer.print("\">");
                    writer.print("<ele>");
                    writer.print(position.getAltitude());
                    writer.print("</ele>");
                    writer.print("<time>");
                    writer.print(DateUtil.formatDate(position.getFixTime()));
                    writer.print("</time>");
                    writer.print("</trkpt>");
                });
                writer.print("</trkseg>");
                writer.print("</trk>");
                writer.print("</gpx>");
            } finally {
                formatSpan.end();
            }
            
            // Record metrics for successful generation
            metricsCollector.incrementCounter(METRIC_PREFIX + "total");
            
        } catch (Exception e) {
            // Record error metrics
            metricsCollector.incrementCounter(METRIC_PREFIX + "errors");
            span.recordException(e);
            throw e;
        } finally {
            // Record duration metric
            long duration = System.currentTimeMillis() - startTime;
            metricsCollector.recordHistogramValue(METRIC_PREFIX + "duration", duration);
            
            // Decrement active exports metric
            metricsCollector.decrementGauge(METRIC_PREFIX + "active");
            
            span.end();
        }
    }

    /**
     * Asynchronously generates a GPX export and stores it in object storage.
     * This method publishes a message to the broker for processing.
     *
     * @param deviceId The ID of the device to generate the report for
     * @param from The start date for positions to include
     * @param to The end date for positions to include
     * @param userId The ID of the user requesting the report
     * @return A CompletableFuture that will be completed with the report URL when generation is complete
     */
    public CompletableFuture<String> generateAsync(long deviceId, Date from, Date to, long userId) {
        // Create a span for tracing this operation
        Span span = tracer.spanBuilder("gpx.export.request").startSpan();
        
        try {
            span.setAttribute("deviceId", deviceId);
            span.setAttribute("userId", userId);
            span.setAttribute("fromDate", from.getTime());
            span.setAttribute("toDate", to.getTime());
            
            // Create a unique ID for this report request
            String reportId = UUID.randomUUID().toString();
            span.setAttribute("reportId", reportId);
            
            // Create message headers with trace context
            Map<String, String> headers = new HashMap<>();
            tracer.getPropagators().getTextMapPropagator().inject(Context.current(), headers, SETTER);
            
            // Create report request message
            ReportRequest request = new ReportRequest();
            request.setReportId(reportId);
            request.setReportType(REPORT_TYPE);
            request.setDeviceId(deviceId);
            request.setUserId(userId);
            request.setFromTime(from.getTime());
            request.setToTime(to.getTime());
            
            // Publish message to broker
            CompletableFuture<String> future = new CompletableFuture<>();
            messageProducer.sendReportRequest(request, headers, future);
            
            return future;
        } catch (Exception e) {
            // Record error metrics
            metricsCollector.incrementCounter(METRIC_PREFIX + "errors");
            span.recordException(e);
            CompletableFuture<String> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        } finally {
            span.end();
        }
    }

    /**
     * Processes an asynchronous report generation request.
     * This method is called by the message consumer when processing a report request.
     *
     * @param request The report request to process
     * @param headers Message headers containing trace context
     * @return The URL of the generated report in object storage
     * @throws StorageException If there is an error retrieving data from storage
     * @throws IOException If there is an error writing to object storage
     */
    public String processReportRequest(ReportRequest request, Map<String, String> headers) 
            throws StorageException, IOException {
        
        // Extract trace context from headers
        Context extractedContext = tracer.getPropagators().getTextMapPropagator()
                .extract(Context.current(), headers, GETTER);
        
        // Create a span for tracing this operation
        Span span = tracer.spanBuilder("gpx.export.process")
                .setParent(extractedContext)
                .startSpan();
        
        long startTime = System.currentTimeMillis();
        
        try {
            // Increment active exports metric
            metricsCollector.incrementGauge(METRIC_PREFIX + "active");
            
            span.setAttribute("reportId", request.getReportId());
            span.setAttribute("deviceId", request.getDeviceId());
            span.setAttribute("userId", request.getUserId());
            
            // Generate the GPX data
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            generate(outputStream, request.getDeviceId(), 
                    new Date(request.getFromTime()), new Date(request.getToTime()));
            
            byte[] reportData = outputStream.toByteArray();
            
            // Record size metric
            metricsCollector.recordHistogramValue(METRIC_PREFIX + "size", reportData.length);
            span.setAttribute("reportSize", reportData.length);
            
            // Create a child span for storing in object storage
            Span storageSpan = tracer.spanBuilder("gpx.export.store")
                    .setParent(Context.current().with(span))
                    .startSpan();
            
            String objectKey = "reports/" + request.getReportId() + ".gpx";
            String contentType = "application/gpx+xml";
            
            try {
                // Store the report in object storage
                String url = objectStorage.storeObject(objectKey, reportData, contentType);
                storageSpan.setAttribute("objectKey", objectKey);
                storageSpan.setAttribute("objectUrl", url);
                
                // Store report metadata in database
                Report report = new Report();
                report.setId(request.getReportId());
                report.setUserId(request.getUserId());
                report.setDeviceId(request.getDeviceId());
                report.setType(REPORT_TYPE);
                report.setCreatedAt(new Date());
                report.setFromTime(new Date(request.getFromTime()));
                report.setToTime(new Date(request.getToTime()));
                report.setUrl(url);
                
                storage.addObject(report);
                
                return url;
            } finally {
                storageSpan.end();
            }
        } catch (Exception e) {
            // Record error metrics
            metricsCollector.incrementCounter(METRIC_PREFIX + "errors");
            span.recordException(e);
            throw e;
        } finally {
            // Record duration metric
            long duration = System.currentTimeMillis() - startTime;
            metricsCollector.recordHistogramValue(METRIC_PREFIX + "duration", duration);
            
            // Decrement active exports metric
            metricsCollector.decrementGauge(METRIC_PREFIX + "active");
            
            span.end();
        }
    }

    /**
     * Health check method for Kubernetes probes.
     * Verifies that the provider can access its dependencies.
     *
     * @return true if the provider is healthy, false otherwise
     */
    private boolean checkHealth() {
        try {
            // Check storage access
            storage.getObjects(Device.class, new Request(
                    new Columns.All(), new Condition.Equals("id", 0)));
            
            // Check object storage access
            objectStorage.checkAccess();
            
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}