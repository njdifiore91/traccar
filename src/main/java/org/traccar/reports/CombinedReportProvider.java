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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.traccar.helper.model.DeviceUtil;
import org.traccar.helper.model.PositionUtil;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.reports.common.ReportUtils;
import org.traccar.reports.model.CombinedReportItem;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Order;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Provider for combined reports that include route, events, and positions.
 * Supports both direct processing and asynchronous report generation via message broker.
 */
public class CombinedReportProvider {

    private static final Set<String> EXCLUDE_TYPES = Set.of(Event.TYPE_DEVICE_MOVING);
    private static final String METRIC_NAME = "report.combined.generation";
    private static final String SPAN_NAME = "CombinedReportGeneration";

    private final ReportUtils reportUtils;
    private final Storage storage;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final MessageBrokerIntegration messageBroker;
    private final ObjectStorageClient objectStorage;
    private final DistributedTracingManager tracingManager;
    private final MeterRegistry meterRegistry;
    private final HealthCheckController healthCheckController;

    /**
     * Constructs a new CombinedReportProvider with required dependencies.
     *
     * @param reportUtils Report utilities for common operations
     * @param storage Storage for data access
     * @param serviceDiscoveryManager Service discovery for microservices communication
     * @param messageBroker Message broker for asynchronous processing
     * @param objectStorage Object storage for report persistence
     * @param tracingManager Distributed tracing manager
     * @param meterRegistry Metrics registry
     * @param healthCheckController Health check controller for Kubernetes probes
     */
    @Inject
    public CombinedReportProvider(
            ReportUtils reportUtils, 
            Storage storage,
            ServiceDiscoveryManager serviceDiscoveryManager,
            MessageBrokerIntegration messageBroker,
            ObjectStorageClient objectStorage,
            DistributedTracingManager tracingManager,
            MeterRegistry meterRegistry,
            HealthCheckController healthCheckController) {
        this.reportUtils = reportUtils;
        this.storage = storage;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.messageBroker = messageBroker;
        this.objectStorage = objectStorage;
        this.tracingManager = tracingManager;
        this.meterRegistry = meterRegistry;
        this.healthCheckController = healthCheckController;
        
        // Register health check for this provider
        this.healthCheckController.registerHealthCheck("combinedReportProvider", this::checkHealth);
    }

    /**
     * Synchronously generates combined report objects for the specified devices and time range.
     * This method is used for direct API calls when immediate results are needed.
     *
     * @param userId User ID requesting the report
     * @param deviceIds Collection of device IDs to include in the report, or null for all accessible devices
     * @param groupIds Collection of group IDs to include in the report, or null for all accessible groups
     * @param from Start date for the report period
     * @param to End date for the report period
     * @return Collection of combined report items
     * @throws StorageException If a storage error occurs
     */
    public Collection<CombinedReportItem> getObjects(
            long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Date from, Date to) throws StorageException {
        
        // Create a span for distributed tracing
        Tracer tracer = tracingManager.getTracer();
        Span span = tracer.spanBuilder(SPAN_NAME)
                .setAttribute("userId", userId)
                .setAttribute("deviceCount", deviceIds != null ? deviceIds.size() : 0)
                .setAttribute("groupCount", groupIds != null ? groupIds.size() : 0)
                .setAttribute("timeRangeHours", (to.getTime() - from.getTime()) / 3600000.0)
                .startSpan();
        
        // Create a timer for metrics collection
        Timer.Sample timer = Timer.start(meterRegistry);
        
        try (io.opentelemetry.context.Scope scope = span.makeCurrent()) {
            reportUtils.checkPeriodLimit(from, to);

            ArrayList<CombinedReportItem> result = new ArrayList<>();
            for (Device device: DeviceUtil.getAccessibleDevices(storage, userId, deviceIds, groupIds)) {
                Span deviceSpan = tracer.spanBuilder("ProcessDevice")
                        .setAttribute("deviceId", device.getId())
                        .setAttribute("deviceName", device.getName())
                        .startSpan();
                
                try (io.opentelemetry.context.Scope deviceScope = deviceSpan.makeCurrent()) {
                    CombinedReportItem item = new CombinedReportItem();
                    item.setDeviceId(device.getId());
                    var positions = PositionUtil.getPositions(storage, device.getId(), from, to);
                    item.setRoute(positions.stream()
                            .map(p -> new double[] {p.getLongitude(), p.getLatitude()})
                            .collect(Collectors.toList()));
                    var events = storage.getObjects(Event.class, new Request(
                            new Columns.All(),
                            new Condition.And(
                                    new Condition.Equals("deviceId", device.getId()),
                                    new Condition.Between("eventTime", "from", from, "to", to)),
                            new Order("eventTime")));
                    item.setEvents(events.stream()
                            .filter(e -> e.getPositionId() > 0 && !EXCLUDE_TYPES.contains(e.getType()))
                            .collect(Collectors.toList()));
                    var eventPositions = events.stream()
                            .map(Event::getPositionId)
                            .collect(Collectors.toSet());
                    item.setPositions(positions.stream()
                            .filter(p -> eventPositions.contains(p.getId()))
                            .collect(Collectors.toList()));
                    result.add(item);
                    
                    deviceSpan.setAttribute("positionCount", positions.size());
                    deviceSpan.setAttribute("eventCount", events.size());
                } finally {
                    deviceSpan.end();
                }
            }
            
            // Record metrics
            timer.stop(Timer.builder(METRIC_NAME)
                    .description("Time taken to generate combined reports")
                    .tag("type", "direct")
                    .tag("deviceCount", String.valueOf(result.size()))
                    .register(meterRegistry));
            
            span.setAttribute("resultCount", result.size());
            return result;
        } catch (Exception e) {
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Asynchronously generates a combined report and stores it in object storage.
     * This method is used for scheduled reports or when immediate results are not required.
     *
     * @param userId User ID requesting the report
     * @param deviceIds Collection of device IDs to include in the report, or null for all accessible devices
     * @param groupIds Collection of group IDs to include in the report, or null for all accessible groups
     * @param from Start date for the report period
     * @param to End date for the report period
     * @param format Report format (e.g., "json", "csv")
     * @return CompletableFuture with the URL to the stored report
     */
    public CompletableFuture<String> generateReportAsync(
            long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Date from, Date to, String format) {
        
        String reportId = UUID.randomUUID().toString();
        
        // Create a span for distributed tracing
        Tracer tracer = tracingManager.getTracer();
        Span span = tracer.spanBuilder(SPAN_NAME + "Async")
                .setAttribute("reportId", reportId)
                .setAttribute("userId", userId)
                .setAttribute("deviceCount", deviceIds != null ? deviceIds.size() : 0)
                .setAttribute("groupCount", groupIds != null ? groupIds.size() : 0)
                .setAttribute("timeRangeHours", (to.getTime() - from.getTime()) / 3600000.0)
                .setAttribute("format", format)
                .startSpan();
        
        try (io.opentelemetry.context.Scope scope = span.makeCurrent()) {
            // Create a report generation request
            ReportGenerationRequest request = new ReportGenerationRequest();
            request.setReportId(reportId);
            request.setReportType("combined");
            request.setUserId(userId);
            request.setDeviceIds(deviceIds);
            request.setGroupIds(groupIds);
            request.setFrom(from);
            request.setTo(to);
            request.setFormat(format);
            
            // Extract the trace context for propagation
            String traceContext = tracingManager.extractContextToString(Context.current());
            request.setTraceContext(traceContext);
            
            // Publish the request to the message broker
            return messageBroker.publishReportRequest(request)
                    .thenApplyAsync(success -> {
                        if (success) {
                            // Return the URL where the report will be available
                            return objectStorage.getSignedUrl("reports/" + reportId + "." + format, 24 * 60 * 60); // 24 hours expiry
                        } else {
                            throw new RuntimeException("Failed to publish report request");
                        }
                    });
        } catch (Exception e) {
            span.recordException(e);
            CompletableFuture<String> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        } finally {
            span.end();
        }
    }
    
    /**
     * Processes an asynchronous report generation request received from the message broker.
     * This method is called by the MessageBrokerIntegration when a report request is received.
     *
     * @param request The report generation request
     * @return CompletableFuture that completes when the report is generated and stored
     */
    public CompletableFuture<Void> processReportRequest(ReportGenerationRequest request) {
        // Create a timer for metrics collection
        Timer.Sample timer = Timer.start(meterRegistry);
        
        // Extract and continue the trace context from the request
        Context context = tracingManager.extractContextFromString(request.getTraceContext());
        Span span = tracingManager.getTracer().spanBuilder("ProcessReportRequest")
                .setParent(context)
                .setAttribute("reportId", request.getReportId())
                .setAttribute("reportType", request.getReportType())
                .setAttribute("userId", request.getUserId())
                .startSpan();
        
        try (io.opentelemetry.context.Scope scope = span.makeCurrent()) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    // Generate the report
                    Collection<CombinedReportItem> result = getObjects(
                            request.getUserId(),
                            request.getDeviceIds(),
                            request.getGroupIds(),
                            request.getFrom(),
                            request.getTo());
                    
                    // Convert to the requested format
                    byte[] reportData;
                    String contentType;
                    
                    switch (request.getFormat().toLowerCase()) {
                        case "json":
                            reportData = convertToJson(result);
                            contentType = "application/json";
                            break;
                        case "csv":
                            reportData = convertToCsv(result);
                            contentType = "text/csv";
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported format: " + request.getFormat());
                    }
                    
                    // Store the report in object storage
                    String objectKey = "reports/" + request.getReportId() + "." + request.getFormat();
                    objectStorage.uploadObject(objectKey, reportData, contentType);
                    
                    // Record metrics
                    timer.stop(Timer.builder(METRIC_NAME)
                            .description("Time taken to generate combined reports")
                            .tag("type", "async")
                            .tag("format", request.getFormat())
                            .register(meterRegistry));
                    
                    // Publish completion event
                    ReportCompletionEvent completionEvent = new ReportCompletionEvent();
                    completionEvent.setReportId(request.getReportId());
                    completionEvent.setUserId(request.getUserId());
                    completionEvent.setReportType(request.getReportType());
                    completionEvent.setObjectKey(objectKey);
                    completionEvent.setFormat(request.getFormat());
                    completionEvent.setDeviceCount(result.size());
                    completionEvent.setGeneratedAt(new Date());
                    
                    messageBroker.publishReportCompletion(completionEvent);
                    
                    span.setAttribute("success", true);
                    span.setAttribute("resultCount", result.size());
                    return null;
                } catch (Exception e) {
                    span.recordException(e);
                    span.setAttribute("success", false);
                    throw new RuntimeException("Failed to process report request", e);
                }
            });
        } finally {
            span.end();
        }
    }
    
    /**
     * Converts report items to JSON format.
     *
     * @param items The report items to convert
     * @return Byte array containing the JSON data
     */
    private byte[] convertToJson(Collection<CombinedReportItem> items) {
        // Implementation would use Jackson or similar library
        // This is a placeholder for the actual implementation
        return new byte[0];
    }
    
    /**
     * Converts report items to CSV format.
     *
     * @param items The report items to convert
     * @return Byte array containing the CSV data
     */
    private byte[] convertToCsv(Collection<CombinedReportItem> items) {
        // Implementation would use a CSV library
        // This is a placeholder for the actual implementation
        return new byte[0];
    }
    
    /**
     * Health check implementation for this provider.
     * Used by the HealthCheckController to report health status to Kubernetes.
     *
     * @return true if the provider is healthy, false otherwise
     */
    private boolean checkHealth() {
        try {
            // Check if storage is accessible
            storage.getObjects(Device.class, new Request(
                    new Columns.All(),
                    new Condition.Permission(Device.class, 0, 0),
                    new Order("id"),
                    new Request.Limit(1)));
            
            // Check if service discovery is working
            serviceDiscoveryManager.isHealthy();
            
            // Check if message broker is connected
            messageBroker.isConnected();
            
            // Check if object storage is accessible
            objectStorage.isAccessible();
            
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Request object for asynchronous report generation.
     */
    public static class ReportGenerationRequest {
        private String reportId;
        private String reportType;
        private long userId;
        private Collection<Long> deviceIds;
        private Collection<Long> groupIds;
        private Date from;
        private Date to;
        private String format;
        private String traceContext;
        
        // Getters and setters
        public String getReportId() { return reportId; }
        public void setReportId(String reportId) { this.reportId = reportId; }
        
        public String getReportType() { return reportType; }
        public void setReportType(String reportType) { this.reportType = reportType; }
        
        public long getUserId() { return userId; }
        public void setUserId(long userId) { this.userId = userId; }
        
        public Collection<Long> getDeviceIds() { return deviceIds; }
        public void setDeviceIds(Collection<Long> deviceIds) { this.deviceIds = deviceIds; }
        
        public Collection<Long> getGroupIds() { return groupIds; }
        public void setGroupIds(Collection<Long> groupIds) { this.groupIds = groupIds; }
        
        public Date getFrom() { return from; }
        public void setFrom(Date from) { this.from = from; }
        
        public Date getTo() { return to; }
        public void setTo(Date to) { this.to = to; }
        
        public String getFormat() { return format; }
        public void setFormat(String format) { this.format = format; }
        
        public String getTraceContext() { return traceContext; }
        public void setTraceContext(String traceContext) { this.traceContext = traceContext; }
    }
    
    /**
     * Event object for report completion notification.
     */
    public static class ReportCompletionEvent {
        private String reportId;
        private long userId;
        private String reportType;
        private String objectKey;
        private String format;
        private int deviceCount;
        private Date generatedAt;
        
        // Getters and setters
        public String getReportId() { return reportId; }
        public void setReportId(String reportId) { this.reportId = reportId; }
        
        public long getUserId() { return userId; }
        public void setUserId(long userId) { this.userId = userId; }
        
        public String getReportType() { return reportType; }
        public void setReportType(String reportType) { this.reportType = reportType; }
        
        public String getObjectKey() { return objectKey; }
        public void setObjectKey(String objectKey) { this.objectKey = objectKey; }
        
        public String getFormat() { return format; }
        public void setFormat(String format) { this.format = format; }
        
        public int getDeviceCount() { return deviceCount; }
        public void setDeviceCount(int deviceCount) { this.deviceCount = deviceCount; }
        
        public Date getGeneratedAt() { return generatedAt; }
        public void setGeneratedAt(Date generatedAt) { this.generatedAt = generatedAt; }
    }
}