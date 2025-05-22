/*
 * Copyright 2016 - 2023 Anton Tananaev (anton@traccar.org)
 * Copyright 2016 Andrey Kunitsyn (andrey@traccar.org)
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
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.apache.poi.ss.util.WorkbookUtil;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.helper.model.DeviceUtil;
import org.traccar.helper.model.PositionUtil;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Device;
import org.traccar.model.Group;
import org.traccar.model.Position;
import org.traccar.reports.common.ReportUtils;
import org.traccar.reports.model.DeviceReportSection;
import org.traccar.reports.model.ReportRequest;
import org.traccar.reports.model.ReportResponse;
import org.traccar.reports.model.ReportStatus;
import org.traccar.storage.ObjectStorage;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Route Report Provider for generating route reports.
 * Supports both direct processing and asynchronous report generation via message broker.
 */
public class RouteReportProvider {

    private final Config config;
    private final ReportUtils reportUtils;
    private final Storage storage;
    private final MessageProducer messageProducer;
    private final ObjectStorage objectStorage;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    private final Map<String, Integer> namesCount = new HashMap<>();
    private final Timer reportGenerationTimer;
    private final Timer reportDataFetchTimer;

    /**
     * Constructor for RouteReportProvider with all required dependencies.
     *
     * @param config Configuration object
     * @param reportUtils Report utilities
     * @param storage Database storage
     * @param messageProducer Message broker producer for async processing
     * @param objectStorage Object storage for report files
     * @param serviceDiscoveryManager Service discovery manager
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry for performance monitoring
     */
    @Inject
    public RouteReportProvider(
            Config config,
            ReportUtils reportUtils,
            Storage storage,
            @Named("reportProducer") MessageProducer messageProducer,
            ObjectStorage objectStorage,
            ServiceDiscoveryManager serviceDiscoveryManager,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.config = config;
        this.reportUtils = reportUtils;
        this.storage = storage;
        this.messageProducer = messageProducer;
        this.objectStorage = objectStorage;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.reportGenerationTimer = Timer.builder("report.generation.time")
                .description("Time taken to generate reports")
                .tag("report.type", "route")
                .register(meterRegistry);
        
        this.reportDataFetchTimer = Timer.builder("report.data.fetch.time")
                .description("Time taken to fetch report data")
                .tag("report.type", "route")
                .register(meterRegistry);
        
        // Register with service discovery
        serviceDiscoveryManager.register("reporting-service", "route-report-provider");
    }

    /**
     * Get positions for the report based on user, device, and time criteria.
     *
     * @param userId User ID requesting the data
     * @param deviceIds Collection of device IDs to include
     * @param groupIds Collection of group IDs to include
     * @param from Start date for the report
     * @param to End date for the report
     * @return Collection of positions matching the criteria
     * @throws StorageException If there's an error accessing the storage
     */
    public Collection<Position> getObjects(long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Date from, Date to) throws StorageException {
        Span span = tracer.spanBuilder("RouteReportProvider.getObjects").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("userId", userId);
            span.setAttribute("deviceCount", deviceIds != null ? deviceIds.size() : 0);
            span.setAttribute("groupCount", groupIds != null ? groupIds.size() : 0);
            
            return reportDataFetchTimer.record(() -> {
                reportUtils.checkPeriodLimit(from, to);

                ArrayList<Position> result = new ArrayList<>();
                for (Device device: DeviceUtil.getAccessibleDevices(storage, userId, deviceIds, groupIds)) {
                    result.addAll(PositionUtil.getPositions(storage, device.getId(), from, to));
                }
                span.setAttribute("positionCount", result.size());
                return result;
            });
        } finally {
            span.end();
        }
    }

    /**
     * Generate a unique sheet name for Excel reports.
     *
     * @param key Base name for the sheet
     * @return Unique sheet name
     */
    private String getUniqueSheetName(String key) {
        namesCount.compute(key, (k, value) -> value == null ? 1 : (value + 1));
        return namesCount.get(key) > 1 ? key + '-' + namesCount.get(key) : key;
    }

    /**
     * Generate Excel report directly and write to the provided output stream.
     *
     * @param outputStream Stream to write the report to
     * @param userId User ID requesting the report
     * @param deviceIds Collection of device IDs to include
     * @param groupIds Collection of group IDs to include
     * @param from Start date for the report
     * @param to End date for the report
     * @throws StorageException If there's an error accessing the storage
     * @throws IOException If there's an error writing the report
     */
    public void getExcel(OutputStream outputStream,
            long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Date from, Date to) throws StorageException, IOException {
        Span span = tracer.spanBuilder("RouteReportProvider.getExcel").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("userId", userId);
            span.setAttribute("deviceCount", deviceIds != null ? deviceIds.size() : 0);
            span.setAttribute("groupCount", groupIds != null ? groupIds.size() : 0);
            
            reportGenerationTimer.record(() -> {
                try {
                    reportUtils.checkPeriodLimit(from, to);

                    ArrayList<DeviceReportSection> devicesRoutes = new ArrayList<>();
                    ArrayList<String> sheetNames = new ArrayList<>();
                    for (Device device: DeviceUtil.getAccessibleDevices(storage, userId, deviceIds, groupIds)) {
                        var positions = PositionUtil.getPositions(storage, device.getId(), from, to);
                        DeviceReportSection deviceRoutes = new DeviceReportSection();
                        deviceRoutes.setDeviceName(device.getName());
                        sheetNames.add(WorkbookUtil.createSafeSheetName(getUniqueSheetName(deviceRoutes.getDeviceName())));
                        if (device.getGroupId() > 0) {
                            Group group = storage.getObject(Group.class, new Request(
                                    new Columns.All(), new Condition.Equals("id", device.getGroupId())));
                            if (group != null) {
                                deviceRoutes.setGroupName(group.getName());
                            }
                        }
                        deviceRoutes.setObjects(positions);
                        devicesRoutes.add(deviceRoutes);
                    }

                    File file = Paths.get(config.getString(Keys.TEMPLATES_ROOT), "export", "route.xlsx").toFile();
                    try (InputStream inputStream = new FileInputStream(file)) {
                        var context = reportUtils.initializeContext(userId);
                        context.putVar("devices", devicesRoutes);
                        context.putVar("sheetNames", sheetNames);
                        context.putVar("from", from);
                        context.putVar("to", to);
                        reportUtils.processTemplateWithSheets(inputStream, outputStream, context);
                    }
                } catch (StorageException | IOException e) {
                    span.recordException(e);
                    throw new RuntimeException(e);
                }
            });
        } finally {
            span.end();
        }
    }
    
    /**
     * Request asynchronous report generation via message broker.
     * 
     * @param userId User ID requesting the report
     * @param deviceIds Collection of device IDs to include
     * @param groupIds Collection of group IDs to include
     * @param from Start date for the report
     * @param to End date for the report
     * @return CompletableFuture with the report response containing status and report ID
     */
    public CompletableFuture<ReportResponse> requestReportAsync(
            long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Date from, Date to) {
        Span span = tracer.spanBuilder("RouteReportProvider.requestReportAsync").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("userId", userId);
            span.setAttribute("deviceCount", deviceIds != null ? deviceIds.size() : 0);
            span.setAttribute("groupCount", groupIds != null ? groupIds.size() : 0);
            
            // Create a unique report ID
            String reportId = UUID.randomUUID().toString();
            span.setAttribute("reportId", reportId);
            
            // Create report request
            ReportRequest request = new ReportRequest();
            request.setReportId(reportId);
            request.setUserId(userId);
            request.setDeviceIds(deviceIds);
            request.setGroupIds(groupIds);
            request.setFrom(from);
            request.setTo(to);
            request.setReportType("route");
            request.setFormat("xlsx");
            
            // Send to message broker for async processing
            messageProducer.send("report-requests", reportId, request);
            
            // Return response with status and ID for tracking
            ReportResponse response = new ReportResponse();
            response.setReportId(reportId);
            response.setStatus(ReportStatus.PROCESSING);
            
            return CompletableFuture.completedFuture(response);
        } finally {
            span.end();
        }
    }
    
    /**
     * Get the status of an asynchronous report.
     * 
     * @param reportId ID of the report to check
     * @return Report response with current status
     */
    public ReportResponse getReportStatus(String reportId) {
        Span span = tracer.spanBuilder("RouteReportProvider.getReportStatus").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("reportId", reportId);
            
            // Check if report exists in object storage
            boolean exists = objectStorage.exists("reports", reportId + ".xlsx");
            
            ReportResponse response = new ReportResponse();
            response.setReportId(reportId);
            
            if (exists) {
                response.setStatus(ReportStatus.COMPLETED);
                response.setUrl(objectStorage.getUrl("reports", reportId + ".xlsx"));
            } else {
                // Check if report is still processing or failed
                // This would typically involve checking a database or cache
                // For simplicity, we'll assume it's still processing if not found
                response.setStatus(ReportStatus.PROCESSING);
            }
            
            return response;
        } finally {
            span.end();
        }
    }
    
    /**
     * Process a report request asynchronously and store the result in object storage.
     * This method would be called by a message consumer processing the report-requests topic.
     * 
     * @param request Report request to process
     * @return CompletableFuture that completes when the report is generated
     */
    public CompletableFuture<Void> processReportAsync(ReportRequest request) {
        Span span = tracer.spanBuilder("RouteReportProvider.processReportAsync").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("reportId", request.getReportId());
            span.setAttribute("userId", request.getUserId());
            
            return CompletableFuture.runAsync(() -> {
                try {
                    // Generate the report and store it in object storage
                    var outputStream = objectStorage.createOutput("reports", request.getReportId() + ".xlsx");
                    getExcel(outputStream, 
                            request.getUserId(), 
                            request.getDeviceIds(), 
                            request.getGroupIds(), 
                            request.getFrom(), 
                            request.getTo());
                    outputStream.close();
                    
                    // Update report status to completed
                    // This would typically involve updating a database or cache
                    // For simplicity, we'll rely on the existence of the file in object storage
                } catch (Exception e) {
                    span.recordException(e);
                    // Update report status to failed
                    // This would typically involve updating a database or cache
                    throw new RuntimeException("Failed to generate report: " + e.getMessage(), e);
                }
            });
        } finally {
            span.end();
        }
    }
    
    /**
     * Health check method for Kubernetes probes.
     * 
     * @return true if the service is healthy, false otherwise
     */
    public boolean isHealthy() {
        // Check dependencies health
        boolean storageHealthy = storage != null;
        boolean objectStorageHealthy = objectStorage != null;
        boolean messageProducerHealthy = messageProducer != null;
        
        // Report metrics
        meterRegistry.gauge("report.provider.health", storageHealthy && objectStorageHealthy && messageProducerHealthy ? 1 : 0);
        
        return storageHealthy && objectStorageHealthy && messageProducerHealthy;
    }
}