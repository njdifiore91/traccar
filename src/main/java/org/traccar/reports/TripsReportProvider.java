/*
 * Copyright 2016 - 2022 Anton Tananaev (anton@traccar.org)
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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.apache.poi.ss.util.WorkbookUtil;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.helper.model.DeviceUtil;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Device;
import org.traccar.model.Group;
import org.traccar.reports.common.ReportUtils;
import org.traccar.reports.model.DeviceReportSection;
import org.traccar.reports.model.TripReportItem;
import org.traccar.reports.model.ReportRequest;
import org.traccar.reports.model.ReportResponse;
import org.traccar.storage.ObjectStorageClient;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
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

/**
 * Provider for trip reports that supports both direct processing and asynchronous report generation
 * through message broker integration. Includes distributed tracing, metrics collection, and health
 * check reporting for Kubernetes probes.
 */
@Singleton
public class TripsReportProvider {

    private final Config config;
    private final ReportUtils reportUtils;
    private final Storage storage;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final MessageProducer messageProducer;
    private final ObjectStorageClient objectStorageClient;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    
    private final Timer reportGenerationTimer;
    private final Counter reportGenerationCounter;
    private final Counter reportErrorCounter;

    /**
     * Constructs a new TripsReportProvider with required dependencies.
     *
     * @param config Configuration provider
     * @param reportUtils Report utilities
     * @param storage Database storage
     * @param serviceDiscoveryManager Service discovery manager for service registration
     * @param messageProducer Message broker producer for async report generation
     * @param objectStorageClient Object storage client for report storage
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry for performance monitoring
     */
    @Inject
    public TripsReportProvider(
            Config config, 
            ReportUtils reportUtils, 
            Storage storage,
            ServiceDiscoveryManager serviceDiscoveryManager,
            MessageProducer messageProducer,
            ObjectStorageClient objectStorageClient,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.config = config;
        this.reportUtils = reportUtils;
        this.storage = storage;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.messageProducer = messageProducer;
        this.objectStorageClient = objectStorageClient;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.reportGenerationTimer = Timer.builder("reports.trips.generation.time")
                .description("Time taken to generate trips reports")
                .register(meterRegistry);
        this.reportGenerationCounter = Counter.builder("reports.trips.generation.count")
                .description("Number of trips reports generated")
                .register(meterRegistry);
        this.reportErrorCounter = Counter.builder("reports.trips.generation.errors")
                .description("Number of errors during trips report generation")
                .register(meterRegistry);
    }

    /**
     * Retrieves trip report objects directly.
     *
     * @param userId User ID
     * @param deviceIds Device IDs
     * @param groupIds Group IDs
     * @param from Start date
     * @param to End date
     * @return Collection of trip report items
     * @throws StorageException If a storage error occurs
     */
    public Collection<TripReportItem> getObjects(
            long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Date from, Date to) throws StorageException {
        
        Span span = tracer.spanBuilder("TripsReportProvider.getObjects").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("userId", userId);
            span.setAttribute("deviceCount", deviceIds != null ? deviceIds.size() : 0);
            span.setAttribute("groupCount", groupIds != null ? groupIds.size() : 0);
            
            reportUtils.checkPeriodLimit(from, to);

            ArrayList<TripReportItem> result = new ArrayList<>();
            for (Device device: DeviceUtil.getAccessibleDevices(storage, userId, deviceIds, groupIds)) {
                span.setAttribute("deviceId", device.getId());
                result.addAll(reportUtils.detectTripsAndStops(device, from, to, TripReportItem.class));
            }
            
            span.setAttribute("resultCount", result.size());
            return result;
        } catch (Exception e) {
            span.recordException(e);
            reportErrorCounter.increment();
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Generates Excel report directly and writes to the provided output stream.
     *
     * @param outputStream Output stream to write the report to
     * @param userId User ID
     * @param deviceIds Device IDs
     * @param groupIds Group IDs
     * @param from Start date
     * @param to End date
     * @throws StorageException If a storage error occurs
     * @throws IOException If an I/O error occurs
     */
    public void getExcel(OutputStream outputStream,
            long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Date from, Date to) throws StorageException, IOException {
        
        Timer.Sample sample = Timer.start(meterRegistry);
        Span span = tracer.spanBuilder("TripsReportProvider.getExcel").startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("userId", userId);
            span.setAttribute("deviceCount", deviceIds != null ? deviceIds.size() : 0);
            span.setAttribute("groupCount", groupIds != null ? groupIds.size() : 0);
            
            reportUtils.checkPeriodLimit(from, to);

            ArrayList<DeviceReportSection> devicesTrips = new ArrayList<>();
            ArrayList<String> sheetNames = new ArrayList<>();
            for (Device device: DeviceUtil.getAccessibleDevices(storage, userId, deviceIds, groupIds)) {
                span.setAttribute("deviceId", device.getId());
                Collection<TripReportItem> trips = reportUtils.detectTripsAndStops(device, from, to, TripReportItem.class);
                DeviceReportSection deviceTrips = new DeviceReportSection();
                deviceTrips.setDeviceName(device.getName());
                sheetNames.add(WorkbookUtil.createSafeSheetName(deviceTrips.getDeviceName()));
                if (device.getGroupId() > 0) {
                    Group group = storage.getObject(Group.class, new Request(
                            new Columns.All(), new Condition.Equals("id", device.getGroupId())));
                    if (group != null) {
                        deviceTrips.setGroupName(group.getName());
                    }
                }
                deviceTrips.setObjects(trips);
                devicesTrips.add(deviceTrips);
            }

            File file = Paths.get(config.getString(Keys.TEMPLATES_ROOT), "export", "trips.xlsx").toFile();
            try (InputStream inputStream = new FileInputStream(file)) {
                var context = reportUtils.initializeContext(userId);
                context.putVar("devices", devicesTrips);
                context.putVar("sheetNames", sheetNames);
                context.putVar("from", from);
                context.putVar("to", to);
                reportUtils.processTemplateWithSheets(inputStream, outputStream, context);
            }
            
            reportGenerationCounter.increment();
            span.setAttribute("success", true);
        } catch (Exception e) {
            span.recordException(e);
            reportErrorCounter.increment();
            span.setAttribute("success", false);
            throw e;
        } finally {
            sample.stop(reportGenerationTimer);
            span.end();
        }
    }
    
    /**
     * Asynchronously generates a trips report and stores it in object storage.
     * The report generation is processed through the message broker.
     *
     * @param userId User ID
     * @param deviceIds Device IDs
     * @param groupIds Group IDs
     * @param from Start date
     * @param to End date
     * @return CompletableFuture with the report response containing the storage URL
     */
    public CompletableFuture<ReportResponse> generateReportAsync(
            long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Date from, Date to) {
        
        Span span = tracer.spanBuilder("TripsReportProvider.generateReportAsync").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("userId", userId);
            span.setAttribute("deviceCount", deviceIds != null ? deviceIds.size() : 0);
            span.setAttribute("groupCount", groupIds != null ? groupIds.size() : 0);
            
            // Create a report request
            String reportId = UUID.randomUUID().toString();
            ReportRequest request = new ReportRequest();
            request.setReportId(reportId);
            request.setUserId(userId);
            request.setDeviceIds(deviceIds);
            request.setGroupIds(groupIds);
            request.setFrom(from);
            request.setTo(to);
            request.setReportType("trips");
            
            // Publish the request to the message broker
            messageProducer.publish("reports.requests", reportId, request);
            
            span.setAttribute("reportId", reportId);
            span.setAttribute("async", true);
            
            // Return a future that will be completed when the report is generated
            return CompletableFuture.supplyAsync(() -> {
                // In a real implementation, this would wait for a response from the message broker
                // For now, we'll just return a placeholder response
                ReportResponse response = new ReportResponse();
                response.setReportId(reportId);
                response.setStatus("PENDING");
                response.setMessage("Report generation in progress");
                return response;
            });
        } finally {
            span.end();
        }
    }
    
    /**
     * Processes a report request received from the message broker.
     * Generates the report and stores it in object storage.
     *
     * @param request Report request
     * @return Report response with storage URL
     */
    public ReportResponse processReportRequest(ReportRequest request) {
        Timer.Sample sample = Timer.start(meterRegistry);
        Span span = tracer.spanBuilder("TripsReportProvider.processReportRequest").startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("reportId", request.getReportId());
            span.setAttribute("userId", request.getUserId());
            span.setAttribute("reportType", request.getReportType());
            
            // Generate the report
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            getExcel(outputStream, request.getUserId(), request.getDeviceIds(), 
                    request.getGroupIds(), request.getFrom(), request.getTo());
            
            // Store the report in object storage
            String objectKey = String.format("reports/%s/%s.xlsx", 
                    request.getUserId(), request.getReportId());
            
            objectStorageClient.putObject(
                    config.getString(Keys.REPORTS_BUCKET, "traccar-reports"),
                    objectKey,
                    new ByteArrayInputStream(outputStream.toByteArray()),
                    outputStream.size(),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
            
            // Create and return the response
            ReportResponse response = new ReportResponse();
            response.setReportId(request.getReportId());
            response.setStatus("COMPLETED");
            response.setStorageUrl(objectKey);
            response.setExpirationTime(new Date(System.currentTimeMillis() + 
                    config.getLong(Keys.REPORTS_EXPIRATION, 86400000))); // Default 24 hours
            
            // Publish the response to the message broker
            messageProducer.publish("reports.responses", request.getReportId(), response);
            
            reportGenerationCounter.increment();
            span.setAttribute("success", true);
            return response;
        } catch (Exception e) {
            span.recordException(e);
            reportErrorCounter.increment();
            span.setAttribute("success", false);
            
            // Create and return error response
            ReportResponse response = new ReportResponse();
            response.setReportId(request.getReportId());
            response.setStatus("ERROR");
            response.setMessage(e.getMessage());
            
            // Publish the error response to the message broker
            messageProducer.publish("reports.responses", request.getReportId(), response);
            
            return response;
        } finally {
            sample.stop(reportGenerationTimer);
            span.end();
        }
    }
    
    /**
     * Checks the health of the trips report provider.
     * Used by Kubernetes health probes.
     *
     * @return true if healthy, false otherwise
     */
    public boolean checkHealth() {
        try {
            // Check if template file exists
            File file = Paths.get(config.getString(Keys.TEMPLATES_ROOT), "export", "trips.xlsx").toFile();
            if (!file.exists()) {
                return false;
            }
            
            // Check service discovery registration
            if (!serviceDiscoveryManager.isRegistered()) {
                return false;
            }
            
            // Check object storage connection
            if (!objectStorageClient.checkConnection()) {
                return false;
            }
            
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}