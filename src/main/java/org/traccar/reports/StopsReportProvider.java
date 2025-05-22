/*
 * Copyright 2017 - 2022 Anton Tananaev (anton@traccar.org)
 * Copyright 2017 Andrey Kunitsyn (andrey@traccar.org)
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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.poi.ss.util.WorkbookUtil;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;
import org.traccar.health.HealthCheckManager;
import org.traccar.helper.model.DeviceUtil;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHeaders;
import org.traccar.model.Device;
import org.traccar.model.Group;
import org.traccar.reports.common.ReportUtils;
import org.traccar.reports.model.DeviceReportSection;
import org.traccar.reports.model.StopReportItem;
import org.traccar.storage.ObjectStorage;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provider for stop reports that supports both direct processing and asynchronous generation via message broker.
 * Includes distributed tracing, metrics collection, and health check reporting.
 */
@Singleton
public class StopsReportProvider {

    private static final Logger LOGGER = Logger.getLogger(StopsReportProvider.class.getName());
    private static final String REPORT_TYPE = "stops";
    private static final String OBJECT_STORAGE_PATH_PREFIX = "reports/stops/";
    
    private final Config config;
    private final ReportUtils reportUtils;
    private final Storage storage;
    private final ServiceDiscovery serviceDiscovery;
    private final MessageProducer messageProducer;
    private final ObjectStorage objectStorage;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final HealthCheckManager healthCheckManager;
    private final Executor reportExecutor;

    private final Timer directProcessingTimer;
    private final Timer asyncProcessingTimer;
    private final Timer excelGenerationTimer;

    /**
     * Constructs a new StopsReportProvider with required dependencies.
     *
     * @param config Configuration provider
     * @param reportUtils Report utilities
     * @param storage Database storage
     * @param serviceDiscovery Service discovery manager
     * @param messageProducer Message broker producer
     * @param objectStorage Object storage for report files
     * @param tracer OpenTelemetry tracer
     * @param meterRegistry Metrics registry
     * @param healthCheckManager Health check manager
     * @param reportExecutor Executor for report processing
     */
    @Inject
    public StopsReportProvider(
            Config config,
            ReportUtils reportUtils,
            Storage storage,
            ServiceDiscovery serviceDiscovery,
            @Named("reportProducer") MessageProducer messageProducer,
            ObjectStorage objectStorage,
            Tracer tracer,
            MeterRegistry meterRegistry,
            HealthCheckManager healthCheckManager,
            @Named("reportExecutor") Executor reportExecutor) {
        this.config = config;
        this.reportUtils = reportUtils;
        this.storage = storage;
        this.serviceDiscovery = serviceDiscovery;
        this.messageProducer = messageProducer;
        this.objectStorage = objectStorage;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.healthCheckManager = healthCheckManager;
        this.reportExecutor = reportExecutor;
        
        // Initialize metrics
        this.directProcessingTimer = meterRegistry.timer("report.stops.processing.direct");
        this.asyncProcessingTimer = meterRegistry.timer("report.stops.processing.async");
        this.excelGenerationTimer = meterRegistry.timer("report.stops.excel.generation");
        
        // Register health check
        this.healthCheckManager.register("report.stops", this::performHealthCheck);
    }

    /**
     * Performs a health check for the stops report provider.
     *
     * @return true if the provider is healthy, false otherwise
     */
    private boolean performHealthCheck() {
        try {
            // Check if template file exists
            File templateFile = Paths.get(config.getString(Keys.TEMPLATES_ROOT), "export", "stops.xlsx").toFile();
            boolean templateExists = templateFile.exists() && templateFile.canRead();
            
            // Check if object storage is accessible
            boolean storageAccessible = objectStorage.isAccessible();
            
            // Check if message broker is available
            Collection<ServiceInstance> brokerInstances = serviceDiscovery.findServiceInstances("message-broker");
            boolean brokerAvailable = !brokerInstances.isEmpty();
            
            return templateExists && storageAccessible && brokerAvailable;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Health check failed", e);
            return false;
        }
    }

    /**
     * Retrieves stop report items for the specified devices and time period.
     * This method supports direct processing for immediate results.
     *
     * @param userId User ID requesting the report
     * @param deviceIds Collection of device IDs to include in the report, or null for all accessible devices
     * @param groupIds Collection of group IDs to include in the report, or null for all accessible groups
     * @param from Start date for the report period
     * @param to End date for the report period
     * @return Collection of stop report items
     * @throws StorageException If a storage error occurs
     */
    public Collection<StopReportItem> getObjects(
            long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Date from, Date to) throws StorageException {
        
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("StopsReportProvider.getObjects")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("report.type", REPORT_TYPE)
                .setAttribute("user.id", userId)
                .setAttribute("from", from.getTime())
                .setAttribute("to", to.getTime())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Record metrics for direct processing
            return directProcessingTimer.record(() -> {
                reportUtils.checkPeriodLimit(from, to);

                ArrayList<StopReportItem> result = new ArrayList<>();
                for (Device device: DeviceUtil.getAccessibleDevices(storage, userId, deviceIds, groupIds)) {
                    span.addEvent("Processing device: " + device.getId());
                    result.addAll(reportUtils.detectTripsAndStops(device, from, to, StopReportItem.class));
                }
                
                // Record result size in span and metrics
                span.setAttribute("result.count", result.size());
                meterRegistry.counter("report.stops.items.count").increment(result.size());
                
                return result;
            });
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Generates an Excel report for stops and writes it to the provided output stream.
     * This method supports direct processing for immediate results.
     *
     * @param outputStream Output stream to write the Excel report to
     * @param userId User ID requesting the report
     * @param deviceIds Collection of device IDs to include in the report, or null for all accessible devices
     * @param groupIds Collection of group IDs to include in the report, or null for all accessible groups
     * @param from Start date for the report period
     * @param to End date for the report period
     * @throws StorageException If a storage error occurs
     * @throws IOException If an I/O error occurs
     */
    public void getExcel(
            OutputStream outputStream, long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Date from, Date to) throws StorageException, IOException {
        
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("StopsReportProvider.getExcel")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("report.type", REPORT_TYPE)
                .setAttribute("report.format", "excel")
                .setAttribute("user.id", userId)
                .setAttribute("from", from.getTime())
                .setAttribute("to", to.getTime())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Record metrics for Excel generation
            excelGenerationTimer.record(() -> {
                try {
                    reportUtils.checkPeriodLimit(from, to);

                    ArrayList<DeviceReportSection> devicesStops = new ArrayList<>();
                    ArrayList<String> sheetNames = new ArrayList<>();
                    for (Device device: DeviceUtil.getAccessibleDevices(storage, userId, deviceIds, groupIds)) {
                        span.addEvent("Processing device: " + device.getId());
                        Collection<StopReportItem> stops = reportUtils.detectTripsAndStops(device, from, to, StopReportItem.class);
                        DeviceReportSection deviceStops = new DeviceReportSection();
                        deviceStops.setDeviceName(device.getName());
                        sheetNames.add(WorkbookUtil.createSafeSheetName(deviceStops.getDeviceName()));
                        if (device.getGroupId() > 0) {
                            Group group = storage.getObject(Group.class, new Request(
                                    new Columns.All(), new Condition.Equals("id", device.getGroupId())));
                            if (group != null) {
                                deviceStops.setGroupName(group.getName());
                            }
                        }
                        deviceStops.setObjects(stops);
                        devicesStops.add(deviceStops);
                    }

                    File file = Paths.get(config.getString(Keys.TEMPLATES_ROOT), "export", "stops.xlsx").toFile();
                    try (InputStream inputStream = new FileInputStream(file)) {
                        var context = reportUtils.initializeContext(userId);
                        context.putVar("devices", devicesStops);
                        context.putVar("sheetNames", sheetNames);
                        context.putVar("from", from);
                        context.putVar("to", to);
                        reportUtils.processTemplateWithSheets(inputStream, outputStream, context);
                    }
                    
                    // Record metrics
                    meterRegistry.counter("report.stops.excel.generated").increment();
                    span.setAttribute("device.count", devicesStops.size());
                } catch (Exception e) {
                    span.recordException(e);
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Requests asynchronous generation of a stops report and returns a future that will be completed
     * when the report is ready. The report will be stored in object storage and can be retrieved
     * using the returned report ID.
     *
     * @param userId User ID requesting the report
     * @param deviceIds Collection of device IDs to include in the report, or null for all accessible devices
     * @param groupIds Collection of group IDs to include in the report, or null for all accessible groups
     * @param from Start date for the report period
     * @param to End date for the report period
     * @param format Report format ("excel", "csv", etc.)
     * @return Future containing the report ID that can be used to retrieve the report from object storage
     */
    public CompletableFuture<String> requestReportAsync(
            long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Date from, Date to, String format) {
        
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("StopsReportProvider.requestReportAsync")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("report.type", REPORT_TYPE)
                .setAttribute("report.format", format)
                .setAttribute("user.id", userId)
                .setAttribute("from", from.getTime())
                .setAttribute("to", to.getTime())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Generate a unique report ID
            String reportId = UUID.randomUUID().toString();
            span.setAttribute("report.id", reportId);
            
            // Create a message with the report request
            MessageEnvelope message = new MessageEnvelope();
            message.setHeaders(new MessageHeaders());
            message.getHeaders().put("report.type", REPORT_TYPE);
            message.getHeaders().put("report.format", format);
            message.getHeaders().put("report.id", reportId);
            message.getHeaders().put("user.id", String.valueOf(userId));
            message.getHeaders().put("from", String.valueOf(from.getTime()));
            message.getHeaders().put("to", String.valueOf(to.getTime()));
            
            // Add trace context to the message headers for distributed tracing
            Context.current().inject(message.getHeaders(), (carrier, key, value) -> carrier.put(key, value));
            
            // Send the message to the report generation topic
            messageProducer.send("report.generation.requests", message);
            
            // Record metrics
            meterRegistry.counter("report.stops.requests.async").increment();
            
            // Create a CompletableFuture that will be completed when the report is ready
            CompletableFuture<String> future = new CompletableFuture<>();
            
            // Start a background task to process the report
            CompletableFuture.runAsync(() -> {
                try {
                    // Record metrics for async processing
                    asyncProcessingTimer.record(() -> {
                        try {
                            // Generate the report
                            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                            getExcel(outputStream, userId, deviceIds, groupIds, from, to);
                            
                            // Store the report in object storage
                            String objectPath = OBJECT_STORAGE_PATH_PREFIX + reportId + ".xlsx";
                            objectStorage.store(objectPath, new ByteArrayInputStream(outputStream.toByteArray()), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
                            
                            // Complete the future with the report ID
                            future.complete(reportId);
                            
                            // Record metrics
                            meterRegistry.counter("report.stops.completed.async").increment();
                        } catch (Exception e) {
                            LOGGER.log(Level.SEVERE, "Error generating async report", e);
                            future.completeExceptionally(e);
                            meterRegistry.counter("report.stops.errors.async").increment();
                        }
                    });
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Error in async report processing", e);
                    future.completeExceptionally(e);
                    meterRegistry.counter("report.stops.errors.async").increment();
                }
            }, reportExecutor);
            
            return future;
        } finally {
            span.end();
        }
    }
    
    /**
     * Retrieves a previously generated report from object storage.
     *
     * @param reportId The ID of the report to retrieve
     * @return InputStream containing the report data
     * @throws IOException If the report cannot be retrieved
     */
    public InputStream getReportFromStorage(String reportId) throws IOException {
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("StopsReportProvider.getReportFromStorage")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("report.type", REPORT_TYPE)
                .setAttribute("report.id", reportId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            String objectPath = OBJECT_STORAGE_PATH_PREFIX + reportId + ".xlsx";
            InputStream inputStream = objectStorage.retrieve(objectPath);
            
            // Record metrics
            meterRegistry.counter("report.stops.retrieved").increment();
            
            return inputStream;
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            meterRegistry.counter("report.stops.retrieval.errors").increment();
            throw new IOException("Failed to retrieve report: " + reportId, e);
        } finally {
            span.end();
        }
    }
}