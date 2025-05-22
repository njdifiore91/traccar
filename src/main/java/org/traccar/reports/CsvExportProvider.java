/*
 * Copyright 2022 - 2025 Anton Tananaev (anton@traccar.org)
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
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.helper.DateUtil;
import org.traccar.helper.model.PositionUtil;
import org.traccar.model.Position;
import org.traccar.reports.common.ReportException;
import org.traccar.reports.model.ReportStatus;
import org.traccar.reports.storage.ObjectStorageManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;

import jakarta.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Provides CSV export functionality for position data with support for both synchronous and asynchronous processing.
 * Includes integration with service discovery, message broker, distributed tracing, metrics collection,
 * and object storage for report persistence.
 */
public class CsvExportProvider {

    private final Storage storage;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final ObjectStorageManager objectStorageManager;
    private final ReportMessageBroker messageBroker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Timer generateReportTimer;
    private final Timer asyncReportTimer;
    private final HealthCheckReporter healthCheckReporter;

    /**
     * Constructs a new CsvExportProvider with required dependencies.
     *
     * @param storage The storage interface for accessing position data
     * @param serviceDiscoveryManager Service discovery for microservice registration
     * @param objectStorageManager Object storage for report persistence
     * @param messageBroker Message broker for asynchronous report generation
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry for performance monitoring
     * @param healthCheckReporter Health check reporter for Kubernetes probes
     */
    @Inject
    public CsvExportProvider(
            Storage storage,
            ServiceDiscoveryManager serviceDiscoveryManager,
            ObjectStorageManager objectStorageManager,
            ReportMessageBroker messageBroker,
            Tracer tracer,
            MeterRegistry meterRegistry,
            HealthCheckReporter healthCheckReporter) {
        this.storage = storage;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.objectStorageManager = objectStorageManager;
        this.messageBroker = messageBroker;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.healthCheckReporter = healthCheckReporter;
        
        // Register metrics
        this.generateReportTimer = Timer.builder("report.csv.generate")
                .description("Time taken to generate CSV reports")
                .register(meterRegistry);
        this.asyncReportTimer = Timer.builder("report.csv.async")
                .description("Time taken for asynchronous CSV report processing")
                .register(meterRegistry);
        
        // Register health check
        this.healthCheckReporter.registerCheck("csv-export-provider", this::performHealthCheck);
        
        // Register with service discovery
        this.serviceDiscoveryManager.register("csv-export-provider", "reports");
    }

    /**
     * Synchronously generates a CSV report for the specified device and time range.
     * The report is written directly to the provided output stream.
     *
     * @param outputStream The output stream to write the CSV data to
     * @param deviceId The ID of the device to generate the report for
     * @param from The start date for the report period
     * @param to The end date for the report period
     * @throws StorageException If there is an error accessing the storage
     */
    public void generate(
            OutputStream outputStream, long deviceId, Date from, Date to) throws StorageException {
        
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("csv.export.generate")
                .setAttribute("deviceId", deviceId)
                .setAttribute("from", from.getTime())
                .setAttribute("to", to.getTime())
                .startSpan();
        
        try {
            // Record metrics for the operation
            generateReportTimer.record(() -> {
                try {
                    doGenerate(outputStream, deviceId, from, to);
                } catch (StorageException e) {
                    span.recordException(e);
                    throw new RuntimeException(e);
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Asynchronously generates a CSV report for the specified device and time range.
     * The report is stored in object storage and a message is published to the message broker.
     *
     * @param deviceId The ID of the device to generate the report for
     * @param from The start date for the report period
     * @param to The end date for the report period
     * @param userId The ID of the user requesting the report
     * @return A CompletableFuture that resolves to the report ID when the report is queued
     */
    public CompletableFuture<String> generateAsync(
            long deviceId, Date from, Date to, long userId) {
        
        // Generate a unique report ID
        String reportId = UUID.randomUUID().toString();
        
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("csv.export.queue")
                .setAttribute("reportId", reportId)
                .setAttribute("deviceId", deviceId)
                .setAttribute("userId", userId)
                .setAttribute("from", from.getTime())
                .setAttribute("to", to.getTime())
                .startSpan();
        
        try {
            // Create a report status object
            ReportStatus status = new ReportStatus(
                    reportId, "csv", deviceId, userId, from, to, ReportStatus.Status.QUEUED);
            
            // Publish the report request to the message broker
            messageBroker.publishReportRequest(status);
            
            // Return the report ID
            return CompletableFuture.completedFuture(reportId);
        } finally {
            span.end();
        }
    }

    /**
     * Processes an asynchronous report request from the message broker.
     * This method is called by the message consumer when a report request is received.
     *
     * @param status The report status object containing request details
     * @throws ReportException If there is an error generating the report
     */
    public void processAsyncReport(ReportStatus status) throws ReportException {
        // Extract context from the message for distributed tracing
        Context context = messageBroker.extractContext(status.getReportId());
        Span span = tracer.spanBuilder("csv.export.process")
                .setParent(context)
                .setAttribute("reportId", status.getReportId())
                .setAttribute("deviceId", status.getDeviceId())
                .setAttribute("userId", status.getUserId())
                .startSpan();
        
        try {
            // Update status to PROCESSING
            status.setStatus(ReportStatus.Status.PROCESSING);
            messageBroker.publishReportStatus(status);
            
            // Record metrics for the operation
            asyncReportTimer.record(() -> {
                try {
                    // Generate the report to a byte array
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    doGenerate(outputStream, status.getDeviceId(), status.getFrom(), status.getTo());
                    
                    // Store the report in object storage
                    String objectKey = "reports/csv/" + status.getReportId() + ".csv";
                    objectStorageManager.storeObject(objectKey, outputStream.toByteArray(), "text/csv");
                    
                    // Update the report status with the object key
                    status.setStatus(ReportStatus.Status.COMPLETED);
                    status.setObjectKey(objectKey);
                    messageBroker.publishReportStatus(status);
                    
                } catch (Exception e) {
                    span.recordException(e);
                    status.setStatus(ReportStatus.Status.FAILED);
                    status.setErrorMessage(e.getMessage());
                    messageBroker.publishReportStatus(status);
                    throw new RuntimeException(e);
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Core implementation of CSV report generation.
     * This method is used by both synchronous and asynchronous processing paths.
     *
     * @param outputStream The output stream to write the CSV data to
     * @param deviceId The ID of the device to generate the report for
     * @param from The start date for the report period
     * @param to The end date for the report period
     * @throws StorageException If there is an error accessing the storage
     */
    private void doGenerate(
            OutputStream outputStream, long deviceId, Date from, Date to) throws StorageException {

        var positions = PositionUtil.getPositions(storage, deviceId, from, to);

        var attributes = positions.stream()
                .flatMap((position -> position.getAttributes().keySet().stream()))
                .collect(Collectors.toUnmodifiableSet());

        var properties = new LinkedHashMap<String, Function<Position, Object>>();
        properties.put("id", Position::getId);
        properties.put("deviceId", Position::getDeviceId);
        properties.put("protocol", Position::getProtocol);
        properties.put("serverTime", position -> DateUtil.formatDate(position.getServerTime()));
        properties.put("deviceTime", position -> DateUtil.formatDate(position.getDeviceTime()));
        properties.put("fixTime", position -> DateUtil.formatDate(position.getFixTime()));
        properties.put("valid", Position::getValid);
        properties.put("latitude", Position::getLatitude);
        properties.put("longitude", Position::getLongitude);
        properties.put("altitude", Position::getAltitude);
        properties.put("speed", Position::getSpeed);
        properties.put("course", Position::getCourse);
        properties.put("address", Position::getAddress);
        properties.put("accuracy", Position::getAccuracy);
        attributes.forEach(key -> properties.put(key, position -> position.getAttributes().get(key)));

        try (PrintWriter writer = new PrintWriter(outputStream)) {
            writer.println(String.join(",", properties.keySet()));
            positions.forEach(position -> writer.println(properties.values().stream()
                    .map(f -> Objects.toString(f.apply(position), ""))
                    .collect(Collectors.joining(","))));
        }
    }

    /**
     * Performs a health check for this component.
     * Used by the health check reporter for Kubernetes liveness and readiness probes.
     *
     * @return true if the component is healthy, false otherwise
     */
    private boolean performHealthCheck() {
        try {
            // Verify storage connection
            storage.getObjects(Position.class, true);
            
            // Verify service discovery registration
            boolean serviceRegistered = serviceDiscoveryManager.isRegistered("csv-export-provider");
            
            // Verify message broker connection
            boolean brokerConnected = messageBroker.isConnected();
            
            // Verify object storage connection
            boolean storageConnected = objectStorageManager.isConnected();
            
            return serviceRegistered && brokerConnected && storageConnected;
        } catch (Exception e) {
            return false;
        }
    }
}