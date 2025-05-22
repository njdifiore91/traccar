/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
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

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.jxls.util.JxlsHelper;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.traccar.api.security.PermissionsService;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.helper.UnitsConverter;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.helper.model.DeviceUtil;
import org.traccar.helper.model.PositionUtil;
import org.traccar.helper.model.UserUtil;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.reports.common.ReportUtils;
import org.traccar.reports.common.TripsConfig;
import org.traccar.reports.model.SummaryReportItem;
import org.traccar.storage.ObjectStorage;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Order;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Provider for generating summary reports.
 * Supports both direct processing and asynchronous processing via message broker.
 * Includes distributed tracing, metrics collection, and health reporting.
 */
@Service
public class SummaryReportProvider implements HealthIndicator {

    private final Config config;
    private final ReportUtils reportUtils;
    private final PermissionsService permissionsService;
    private final Storage storage;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final KafkaTemplate<String, ReportRequest> kafkaTemplate;
    private final ObjectStorage objectStorage;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final DiscoveryClient discoveryClient;
    
    private final Timer reportGenerationTimer;

    /**
     * Constructs a new SummaryReportProvider with the necessary dependencies.
     *
     * @param config The application configuration
     * @param reportUtils Utilities for report generation
     * @param permissionsService Service for permission checks
     * @param storage Database storage
     * @param serviceDiscoveryManager Service discovery manager for service registration and discovery
     * @param kafkaTemplate Kafka template for asynchronous report generation
     * @param objectStorage Object storage for storing generated reports
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     * @param discoveryClient Spring Cloud discovery client
     */
    @Inject
    public SummaryReportProvider(
            Config config, 
            ReportUtils reportUtils, 
            PermissionsService permissionsService, 
            Storage storage,
            ServiceDiscoveryManager serviceDiscoveryManager,
            KafkaTemplate<String, ReportRequest> kafkaTemplate,
            ObjectStorage objectStorage,
            Tracer tracer,
            MeterRegistry meterRegistry,
            DiscoveryClient discoveryClient) {
        this.config = config;
        this.reportUtils = reportUtils;
        this.permissionsService = permissionsService;
        this.storage = storage;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.kafkaTemplate = kafkaTemplate;
        this.objectStorage = objectStorage;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.discoveryClient = discoveryClient;
        
        // Register metrics
        this.reportGenerationTimer = Timer.builder("report.generation.time")
                .description("Time taken to generate reports")
                .tag("type", "summary")
                .register(meterRegistry);
        
        // Register service with service discovery
        registerService();
    }
    
    /**
     * Registers the reporting service with the service discovery system.
     */
    private void registerService() {
        serviceDiscoveryManager.register("reporting-service", "summary-report-provider");
    }

    /**
     * Gets the edge position (first or last) for a device within a time range.
     *
     * @param deviceId The device ID
     * @param from Start date
     * @param to End date
     * @param end Whether to get the last position (true) or first position (false)
     * @return The edge position
     * @throws StorageException If there's an error accessing storage
     */
    private Position getEdgePosition(long deviceId, Date from, Date to, boolean end) throws StorageException {
        return storage.getObject(Position.class, new Request(
                new Columns.All(),
                new Condition.And(
                        new Condition.Equals("deviceId", deviceId),
                        new Condition.Between("fixTime", "from", from, "to", to)),
                new Order("fixTime", end, 1)));
    }

    /**
     * Calculates the summary report result for a single device.
     *
     * @param device The device
     * @param from Start date
     * @param to End date
     * @param fast Whether to use fast calculation
     * @return Collection of summary report items
     * @throws StorageException If there's an error accessing storage
     */
    @Timed(value = "summary.report.device.calculation", description = "Time taken to calculate device summary report")
    private Collection<SummaryReportItem> calculateDeviceResult(
            Device device, Date from, Date to, boolean fast) throws StorageException {

        Span span = tracer.spanBuilder("calculateDeviceResult").startSpan();
        try (var scope = span.makeCurrent()) {
            span.setAttribute("device.id", device.getId());
            span.setAttribute("device.name", device.getName());
            span.setAttribute("calculation.mode", fast ? "fast" : "detailed");
            
            SummaryReportItem result = new SummaryReportItem();
            result.setDeviceId(device.getId());
            result.setDeviceName(device.getName());

            Position first = null;
            Position last = null;
            if (fast) {
                first = getEdgePosition(device.getId(), from, to, false);
                last = getEdgePosition(device.getId(), from, to, true);
            } else {
                var positions = PositionUtil.getPositions(storage, device.getId(), from, to);
                for (Position position : positions) {
                    if (first == null) {
                        first = position;
                    }
                    if (position.getSpeed() > result.getMaxSpeed()) {
                        result.setMaxSpeed(position.getSpeed());
                    }
                    last = position;
                }
            }

            if (first != null && last != null) {
                TripsConfig tripsConfig = new TripsConfig(
                        new AttributeUtil.StorageProvider(config, storage, permissionsService, device));
                boolean ignoreOdometer = tripsConfig.getIgnoreOdometer();
                result.setDistance(PositionUtil.calculateDistance(first, last, !ignoreOdometer));
                result.setSpentFuel(reportUtils.calculateFuel(first, last));

                if (first.hasAttribute(Position.KEY_HOURS) && last.hasAttribute(Position.KEY_HOURS)) {
                    result.setStartHours(first.getLong(Position.KEY_HOURS));
                    result.setEndHours(last.getLong(Position.KEY_HOURS));
                    long engineHours = result.getEngineHours();
                    if (engineHours > 0) {
                        result.setAverageSpeed(UnitsConverter.knotsFromMps(result.getDistance() * 1000 / engineHours));
                    }
                }

                if (!ignoreOdometer
                        && first.getDouble(Position.KEY_ODOMETER) != 0 && last.getDouble(Position.KEY_ODOMETER) != 0) {
                    result.setStartOdometer(first.getDouble(Position.KEY_ODOMETER));
                    result.setEndOdometer(last.getDouble(Position.KEY_ODOMETER));
                } else {
                    result.setStartOdometer(first.getDouble(Position.KEY_TOTAL_DISTANCE));
                    result.setEndOdometer(last.getDouble(Position.KEY_TOTAL_DISTANCE));
                }

                result.setStartTime(first.getFixTime());
                result.setEndTime(last.getFixTime());
                
                span.setAttribute("result.distance", result.getDistance());
                span.setAttribute("result.maxSpeed", result.getMaxSpeed());
                span.setAttribute("result.spentFuel", result.getSpentFuel());
                
                return List.of(result);
            }

            return List.of();
        } finally {
            span.end();
        }
    }

    /**
     * Calculates summary report results for a device, optionally broken down by day.
     *
     * @param device The device
     * @param from Start date/time
     * @param to End date/time
     * @param daily Whether to break down by day
     * @return Collection of summary report items
     * @throws StorageException If there's an error accessing storage
     */
    private Collection<SummaryReportItem> calculateDeviceResults(
            Device device, ZonedDateTime from, ZonedDateTime to, boolean daily) throws StorageException {

        boolean fast = Duration.between(from, to).toSeconds() > config.getLong(Keys.REPORT_FAST_THRESHOLD);
        var results = new ArrayList<SummaryReportItem>();
        if (daily) {
            while (from.truncatedTo(ChronoUnit.DAYS).isBefore(to.truncatedTo(ChronoUnit.DAYS))) {
                ZonedDateTime fromDay = from.truncatedTo(ChronoUnit.DAYS);
                ZonedDateTime nextDay = fromDay.plusDays(1);
                results.addAll(calculateDeviceResult(
                        device, Date.from(from.toInstant()), Date.from(nextDay.toInstant()), fast));
                from = nextDay;
            }
        }
        results.addAll(calculateDeviceResult(device, Date.from(from.toInstant()), Date.from(to.toInstant()), fast));
        return results;
    }

    /**
     * Gets summary report objects for the specified devices and time range.
     * This method supports direct processing.
     *
     * @param userId User ID
     * @param deviceIds Device IDs to include
     * @param groupIds Group IDs to include
     * @param from Start date
     * @param to End date
     * @param daily Whether to break down by day
     * @return Collection of summary report items
     * @throws StorageException If there's an error accessing storage
     */
    @Timed(value = "summary.report.objects", description = "Time taken to get summary report objects")
    public Collection<SummaryReportItem> getObjects(
            long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Date from, Date to, boolean daily) throws StorageException {
        
        return reportGenerationTimer.record(() -> {
            Span span = tracer.spanBuilder("getObjects").startSpan();
            try (var scope = span.makeCurrent()) {
                span.setAttribute("user.id", userId);
                span.setAttribute("report.type", "summary");
                span.setAttribute("report.daily", daily);
                
                reportUtils.checkPeriodLimit(from, to);

                var tz = UserUtil.getTimezone(permissionsService.getServer(), permissionsService.getUser(userId)).toZoneId();

                ArrayList<SummaryReportItem> result = new ArrayList<>();
                for (Device device: DeviceUtil.getAccessibleDevices(storage, userId, deviceIds, groupIds)) {
                    var deviceResults = calculateDeviceResults(
                            device, from.toInstant().atZone(tz), to.toInstant().atZone(tz), daily);
                    for (SummaryReportItem summaryReport : deviceResults) {
                        if (summaryReport.getStartTime() != null && summaryReport.getEndTime() != null) {
                            result.add(summaryReport);
                        }
                    }
                }
                
                span.setAttribute("result.count", result.size());
                return result;
            } finally {
                span.end();
            }
        });
    }

    /**
     * Generates an Excel report for the specified devices and time range.
     * This method supports direct processing.
     *
     * @param outputStream Output stream to write the Excel report to
     * @param userId User ID
     * @param deviceIds Device IDs to include
     * @param groupIds Group IDs to include
     * @param from Start date
     * @param to End date
     * @param daily Whether to break down by day
     * @throws StorageException If there's an error accessing storage
     * @throws IOException If there's an error writing to the output stream
     */
    @Timed(value = "summary.report.excel", description = "Time taken to generate Excel summary report")
    public void getExcel(OutputStream outputStream,
            long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Date from, Date to, boolean daily) throws StorageException, IOException {
        
        Span span = tracer.spanBuilder("getExcel").startSpan();
        try (var scope = span.makeCurrent()) {
            span.setAttribute("user.id", userId);
            span.setAttribute("report.type", "summary");
            span.setAttribute("report.format", "excel");
            span.setAttribute("report.daily", daily);
            
            Collection<SummaryReportItem> summaries = getObjects(userId, deviceIds, groupIds, from, to, daily);

            File file = Paths.get(config.getString(Keys.TEMPLATES_ROOT), "export", "summary.xlsx").toFile();
            try (InputStream inputStream = new FileInputStream(file)) {
                var context = reportUtils.initializeContext(userId);
                context.putVar("summaries", summaries);
                context.putVar("from", from);
                context.putVar("to", to);
                JxlsHelper.getInstance().setUseFastFormulaProcessor(false)
                        .processTemplate(inputStream, outputStream, context);
            }
            
            span.setAttribute("result.count", summaries.size());
        } finally {
            span.end();
        }
    }
    
    /**
     * Asynchronously requests a summary report generation.
     * This method uses the message broker for asynchronous processing.
     *
     * @param userId User ID
     * @param deviceIds Device IDs to include
     * @param groupIds Group IDs to include
     * @param from Start date
     * @param to End date
     * @param daily Whether to break down by day
     * @return CompletableFuture with the report ID
     */
    @Async
    @Timed(value = "summary.report.async.request", description = "Time taken to request async summary report")
    public CompletableFuture<String> requestReportAsync(
            long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Date from, Date to, boolean daily) {
        
        Span span = tracer.spanBuilder("requestReportAsync").startSpan();
        try (var scope = span.makeCurrent()) {
            span.setAttribute("user.id", userId);
            span.setAttribute("report.type", "summary");
            span.setAttribute("report.daily", daily);
            
            String reportId = UUID.randomUUID().toString();
            
            ReportRequest request = new ReportRequest();
            request.setReportId(reportId);
            request.setUserId(userId);
            request.setDeviceIds(deviceIds);
            request.setGroupIds(groupIds);
            request.setFrom(from);
            request.setTo(to);
            request.setDaily(daily);
            request.setReportType("summary");
            
            // Inject trace context into the message
            Context context = Context.current();
            
            // Send the request to Kafka
            kafkaTemplate.send("report-requests", reportId, request)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        span.recordException(ex);
                    } else {
                        span.setAttribute("kafka.topic", "report-requests");
                        span.setAttribute("kafka.partition", result.getRecordMetadata().partition());
                        span.setAttribute("kafka.offset", result.getRecordMetadata().offset());
                    }
                });
            
            span.setAttribute("report.id", reportId);
            return CompletableFuture.completedFuture(reportId);
        } finally {
            span.end();
        }
    }
    
    /**
     * Processes a report request from the message broker.
     * This method is called when a report request message is consumed from Kafka.
     *
     * @param request The report request
     * @throws StorageException If there's an error accessing storage
     * @throws IOException If there's an error generating the report
     */
    @Timed(value = "summary.report.process.request", description = "Time taken to process a report request")
    public void processReportRequest(ReportRequest request) throws StorageException, IOException {
        Span span = tracer.spanBuilder("processReportRequest").startSpan();
        try (var scope = span.makeCurrent()) {
            span.setAttribute("report.id", request.getReportId());
            span.setAttribute("user.id", request.getUserId());
            span.setAttribute("report.type", request.getReportType());
            
            if ("summary".equals(request.getReportType())) {
                // Generate the report
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                getExcel(outputStream, request.getUserId(), request.getDeviceIds(), request.getGroupIds(),
                        request.getFrom(), request.getTo(), request.isDaily());
                
                // Store the report in object storage
                String objectKey = "reports/" + request.getReportId() + ".xlsx";
                objectStorage.store(objectKey, new ByteArrayInputStream(outputStream.toByteArray()), 
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
                
                span.setAttribute("storage.object.key", objectKey);
                span.setAttribute("report.size", outputStream.size());
                
                // Notify completion via Kafka
                ReportCompletionEvent completionEvent = new ReportCompletionEvent();
                completionEvent.setReportId(request.getReportId());
                completionEvent.setUserId(request.getUserId());
                completionEvent.setObjectKey(objectKey);
                completionEvent.setStatus("COMPLETED");
                
                kafkaTemplate.send("report-completions", request.getReportId(), completionEvent)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            span.recordException(ex);
                        }
                    });
            }
        } finally {
            span.end();
        }
    }
    
    /**
     * Gets a generated report from object storage.
     *
     * @param reportId The report ID
     * @return InputStream for the report
     * @throws IOException If there's an error retrieving the report
     */
    @Timed(value = "summary.report.retrieve", description = "Time taken to retrieve a report")
    public InputStream getReport(String reportId) throws IOException {
        Span span = tracer.spanBuilder("getReport").startSpan();
        try (var scope = span.makeCurrent()) {
            span.setAttribute("report.id", reportId);
            
            String objectKey = "reports/" + reportId + ".xlsx";
            InputStream inputStream = objectStorage.retrieve(objectKey);
            
            span.setAttribute("storage.object.key", objectKey);
            return inputStream;
        } finally {
            span.end();
        }
    }
    
    /**
     * Implements the health check for the reporting service.
     * This is used by Kubernetes probes to determine service health.
     *
     * @return Health status
     */
    @Override
    public Health health() {
        try {
            // Check if we can access the database
            storage.getObjects(Device.class, new Request(new Columns.All(), null, new Order("id", true, 1)));
            
            // Check if we can discover other services
            List<String> services = discoveryClient.getServices();
            
            // Check if object storage is accessible
            objectStorage.checkHealth();
            
            return Health.up()
                    .withDetail("discoveredServices", services.size())
                    .withDetail("objectStorage", "available")
                    .build();
        } catch (Exception e) {
            return Health.down()
                    .withException(e)
                    .build();
        }
    }
    
    /**
     * Request object for asynchronous report generation.
     */
    public static class ReportRequest {
        private String reportId;
        private long userId;
        private Collection<Long> deviceIds;
        private Collection<Long> groupIds;
        private Date from;
        private Date to;
        private boolean daily;
        private String reportType;
        
        public String getReportId() {
            return reportId;
        }
        
        public void setReportId(String reportId) {
            this.reportId = reportId;
        }
        
        public long getUserId() {
            return userId;
        }
        
        public void setUserId(long userId) {
            this.userId = userId;
        }
        
        public Collection<Long> getDeviceIds() {
            return deviceIds;
        }
        
        public void setDeviceIds(Collection<Long> deviceIds) {
            this.deviceIds = deviceIds;
        }
        
        public Collection<Long> getGroupIds() {
            return groupIds;
        }
        
        public void setGroupIds(Collection<Long> groupIds) {
            this.groupIds = groupIds;
        }
        
        public Date getFrom() {
            return from;
        }
        
        public void setFrom(Date from) {
            this.from = from;
        }
        
        public Date getTo() {
            return to;
        }
        
        public void setTo(Date to) {
            this.to = to;
        }
        
        public boolean isDaily() {
            return daily;
        }
        
        public void setDaily(boolean daily) {
            this.daily = daily;
        }
        
        public String getReportType() {
            return reportType;
        }
        
        public void setReportType(String reportType) {
            this.reportType = reportType;
        }
    }
    
    /**
     * Event object for report completion notification.
     */
    public static class ReportCompletionEvent {
        private String reportId;
        private long userId;
        private String objectKey;
        private String status;
        
        public String getReportId() {
            return reportId;
        }
        
        public void setReportId(String reportId) {
            this.reportId = reportId;
        }
        
        public long getUserId() {
            return userId;
        }
        
        public void setUserId(long userId) {
            this.userId = userId;
        }
        
        public String getObjectKey() {
            return objectKey;
        }
        
        public void setObjectKey(String objectKey) {
            this.objectKey = objectKey;
        }
        
        public String getStatus() {
            return status;
        }
        
        public void setStatus(String status) {
            this.status = status;
        }
    }
}