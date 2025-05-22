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

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.extension.annotations.WithSpan;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.core.metrics.Timer;

import org.jxls.util.JxlsHelper;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.helper.model.PositionUtil;
import org.traccar.messaging.MessageBroker;
import org.traccar.model.Device;
import org.traccar.model.Message;
import org.traccar.model.User;
import org.traccar.reports.common.ReportUtils;
import org.traccar.reports.model.DeviceReportItem;
import org.traccar.storage.ObjectStorageManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Provider for device reports with support for both direct and service-based processing.
 * Includes integration with service discovery, message broker, distributed tracing,
 * metrics collection, and object storage.
 */
@Singleton
public class DevicesReportProvider {

    private final Config config;
    private final ReportUtils reportUtils;
    private final Storage storage;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final MessageBroker messageBroker;
    private final ObjectStorageManager objectStorageManager;
    private final Tracer tracer;
    
    // Metrics for monitoring report generation performance
    private final Counter reportRequestsCounter;
    private final Counter reportGeneratedCounter;
    private final Counter reportErrorsCounter;
    private final Histogram reportSizeHistogram;
    private final Timer reportGenerationTimer;

    /**
     * Constructs a new DevicesReportProvider with required dependencies.
     *
     * @param config Configuration provider
     * @param reportUtils Report utilities
     * @param storage Storage access
     * @param serviceDiscoveryManager Service discovery manager
     * @param messageBroker Message broker for async processing
     * @param objectStorageManager Object storage for report files
     * @param tracer OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public DevicesReportProvider(
            Config config,
            ReportUtils reportUtils,
            Storage storage,
            ServiceDiscoveryManager serviceDiscoveryManager,
            MessageBroker messageBroker,
            ObjectStorageManager objectStorageManager,
            Tracer tracer) {
        this.config = config;
        this.reportUtils = reportUtils;
        this.storage = storage;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.messageBroker = messageBroker;
        this.objectStorageManager = objectStorageManager;
        this.tracer = tracer;
        
        // Initialize metrics
        this.reportRequestsCounter = Counter.builder()
                .name("reports_devices_requests_total")
                .help("Total number of device report requests")
                .labelNames("type")
                .register();
        
        this.reportGeneratedCounter = Counter.builder()
                .name("reports_devices_generated_total")
                .help("Total number of device reports generated")
                .labelNames("type")
                .register();
        
        this.reportErrorsCounter = Counter.builder()
                .name("reports_devices_errors_total")
                .help("Total number of device report generation errors")
                .labelNames("type")
                .register();
        
        this.reportSizeHistogram = Histogram.builder()
                .name("reports_devices_size_bytes")
                .help("Size of generated device reports in bytes")
                .labelNames("type")
                .register();
        
        this.reportGenerationTimer = Timer.builder()
                .name("reports_devices_generation_seconds")
                .help("Time taken to generate device reports")
                .labelNames("type")
                .register();
        
        // Register with service discovery
        registerService();
    }
    
    /**
     * Registers this service with the service discovery system.
     */
    private void registerService() {
        try {
            serviceDiscoveryManager.register("reporting-service", "devices-report-provider",
                    config.getString(Keys.WEB_PORT));
        } catch (Exception e) {
            // Log but don't fail initialization
            System.err.println("Failed to register with service discovery: " + e.getMessage());
        }
    }

    /**
     * Gets device objects with their latest positions for reporting.
     *
     * @param userId User ID to filter devices by permission
     * @return Collection of device report items
     * @throws StorageException If a storage error occurs
     */
    @WithSpan
    public Collection<DeviceReportItem> getObjects(long userId) throws StorageException {
        Span span = Span.current();
        span.setAttribute("user.id", userId);
        
        var positions = PositionUtil.getLatestPositions(storage, userId).stream()
                .collect(Collectors.toMap(Message::getDeviceId, p -> p));

        span.setAttribute("positions.count", positions.size());
        
        Collection<DeviceReportItem> result = storage.getObjects(Device.class, new Request(
                new Columns.All(),
                new Condition.Permission(User.class, userId, Device.class))).stream()
                .map(device -> new DeviceReportItem(device, positions.get(device.getId())))
                .toList();
        
        span.setAttribute("devices.count", result.size());
        return result;
    }

    /**
     * Generates an Excel report directly to the provided output stream.
     * This is the synchronous/direct processing method.
     *
     * @param outputStream Stream to write the Excel report to
     * @param userId User ID to filter devices by permission
     * @throws StorageException If a storage error occurs
     * @throws IOException If an I/O error occurs
     */
    @WithSpan
    public void getExcel(OutputStream outputStream, long userId) throws StorageException, IOException {
        reportRequestsCounter.labelValues("excel").inc();
        
        try (Timer.Sample sample = Timer.start()) {
            Span span = Span.current();
            span.setAttribute("report.type", "excel");
            span.setAttribute("user.id", userId);
            
            File file = Paths.get(config.getString(Keys.TEMPLATES_ROOT), "export", "devices.xlsx").toFile();
            try (InputStream inputStream = new FileInputStream(file)) {
                var context = reportUtils.initializeContext(userId);
                context.putVar("items", getObjects(userId));
                JxlsHelper.getInstance().setUseFastFormulaProcessor(false)
                        .processTemplate(inputStream, outputStream, context);
            }
            
            reportGeneratedCounter.labelValues("excel").inc();
            sample.stop(reportGenerationTimer.labelValues("excel"));
        } catch (Exception e) {
            reportErrorsCounter.labelValues("excel").inc();
            throw e;
        }
    }
    
    /**
     * Asynchronously generates an Excel report and stores it in object storage.
     * This is the asynchronous/service-based processing method.
     *
     * @param userId User ID to filter devices by permission
     * @return CompletableFuture with the URL to the generated report
     */
    @WithSpan
    public CompletableFuture<String> getExcelAsync(long userId) {
        reportRequestsCounter.labelValues("excel_async").inc();
        
        Span parentSpan = Span.current();
        Context context = Context.current();
        String reportId = UUID.randomUUID().toString();
        
        parentSpan.setAttribute("report.id", reportId);
        parentSpan.setAttribute("report.type", "excel_async");
        parentSpan.setAttribute("user.id", userId);
        
        // Create a message with the report request details
        Map<String, Object> message = new HashMap<>();
        message.put("reportId", reportId);
        message.put("userId", userId);
        message.put("reportType", "devices");
        message.put("format", "excel");
        
        // Publish the message to the broker for async processing
        return messageBroker.publishAsync("reports.generate", message)
                .thenCompose(result -> {
                    // Return a future that will be completed when the report is ready
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            // In a real implementation, we would wait for a notification
                            // that the report is ready, but for simplicity, we'll just
                            // generate it here and store it in object storage
                            
                            // Create a new span for the async processing
                            Tracer tracer = this.tracer;
                            Span span = tracer.spanBuilder("generateExcelReport")
                                    .setParent(context)
                                    .startSpan();
                            
                            try {
                                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                                getExcel(outputStream, userId);
                                byte[] reportData = outputStream.toByteArray();
                                
                                // Record the report size metric
                                reportSizeHistogram.labelValues("excel_async").observe(reportData.length);
                                
                                // Store the report in object storage
                                String objectKey = "reports/devices/" + reportId + ".xlsx";
                                objectStorageManager.putObject(objectKey, new ByteArrayInputStream(reportData), reportData.length, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
                                
                                // Generate a URL for the stored report
                                String reportUrl = objectStorageManager.getObjectUrl(objectKey, 24 * 60 * 60); // 24 hour expiry
                                
                                reportGeneratedCounter.labelValues("excel_async").inc();
                                return reportUrl;
                            } catch (Exception e) {
                                reportErrorsCounter.labelValues("excel_async").inc();
                                throw new RuntimeException("Failed to generate report: " + e.getMessage(), e);
                            } finally {
                                span.end();
                            }
                        } catch (Exception e) {
                            reportErrorsCounter.labelValues("excel_async").inc();
                            throw new RuntimeException("Failed to process report request: " + e.getMessage(), e);
                        }
                    });
                });
    }
    
    /**
     * Health check method for Kubernetes probes.
     * 
     * @return true if the service is healthy, false otherwise
     */
    public boolean isHealthy() {
        try {
            // Check if we can access the storage
            storage.getObjects(Device.class, new Request(
                    new Columns.All(),
                    new Condition.Permission(User.class, 0, Device.class)));
            
            // Check if we can access the template file
            File file = Paths.get(config.getString(Keys.TEMPLATES_ROOT), "export", "devices.xlsx").toFile();
            return file.exists() && file.canRead();
        } catch (Exception e) {
            return false;
        }
    }
}