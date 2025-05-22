/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2016 - 2018 Andrey Kunitsyn (andrey@traccar.org)
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
import io.opentelemetry.context.Context;
import org.apache.poi.ss.util.WorkbookUtil;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.helper.model.DeviceUtil;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Geofence;
import org.traccar.model.Group;
import org.traccar.model.Maintenance;
import org.traccar.model.Position;
import org.traccar.reports.common.DistributedTracingUtils;
import org.traccar.reports.common.MessageBrokerClient;
import org.traccar.reports.common.ObjectStorageClient;
import org.traccar.reports.common.ReportMetrics;
import org.traccar.reports.common.ReportUtils;
import org.traccar.reports.model.DeviceReportSection;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class EventsReportProvider {

    private final Config config;
    private final ReportUtils reportUtils;
    private final Storage storage;
    private final ServiceDiscovery serviceDiscovery;
    private final MessageBrokerClient messageBrokerClient;
    private final ObjectStorageClient objectStorageClient;
    private final ReportMetrics reportMetrics;
    private final DistributedTracingUtils tracingUtils;

    @Inject
    public EventsReportProvider(Config config, ReportUtils reportUtils, Storage storage,
                               ServiceDiscovery serviceDiscovery, MessageBrokerClient messageBrokerClient,
                               ObjectStorageClient objectStorageClient, ReportMetrics reportMetrics,
                               DistributedTracingUtils tracingUtils) {
        this.config = config;
        this.reportUtils = reportUtils;
        this.storage = storage;
        this.serviceDiscovery = serviceDiscovery;
        this.messageBrokerClient = messageBrokerClient;
        this.objectStorageClient = objectStorageClient;
        this.reportMetrics = reportMetrics;
        this.tracingUtils = tracingUtils;
    }

    private List<Event> getEvents(long deviceId, Date from, Date to) throws StorageException {
        return storage.getObjects(Event.class, new Request(
                new Columns.All(),
                new Condition.And(
                        new Condition.Equals("deviceId", deviceId),
                        new Condition.Between("eventTime", "from", from, "to", to)),
                new Order("eventTime")));
    }

    private boolean filterType(Collection<String> types, Collection<String> alarms, Event event) {
        if (!types.contains(event.getType())) {
            return false;
        }
        return !event.getType().equals(Event.TYPE_ALARM) || alarms.isEmpty()
                || alarms.contains(event.getString(Position.KEY_ALARM));
    }

    public Collection<Event> getObjects(
            long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Collection<String> types, Collection<String> alarms, Date from, Date to) throws StorageException {
        Span span = tracingUtils.startSpan("EventsReportProvider.getObjects");
        try {
            reportUtils.checkPeriodLimit(from, to);
            reportMetrics.recordReportRequest("events", deviceIds.size());

            ArrayList<Event> result = new ArrayList<>();
            for (Device device: DeviceUtil.getAccessibleDevices(storage, userId, deviceIds, groupIds)) {
                Collection<Event> events = getEvents(device.getId(), from, to);
                boolean all = types.isEmpty() || types.contains(Event.ALL_EVENTS);
                for (Event event : events) {
                    if (all || filterType(types, alarms, event)) {
                        long geofenceId = event.getGeofenceId();
                        long maintenanceId = event.getMaintenanceId();
                        if ((geofenceId == 0 || reportUtils.getObject(userId, Geofence.class, geofenceId) != null)
                                && (maintenanceId == 0
                                || reportUtils.getObject(userId, Maintenance.class, maintenanceId) != null)) {
                           result.add(event);
                        }
                    }
                }
            }
            reportMetrics.recordReportSize("events", result.size());
            return result;
        } finally {
            span.end();
        }
    }

    public void getExcel(
            OutputStream outputStream, long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Collection<String> types, Collection<String> alarms,
            Date from, Date to) throws StorageException, IOException {
        Span span = tracingUtils.startSpan("EventsReportProvider.getExcel");
        try {
            reportUtils.checkPeriodLimit(from, to);
            reportMetrics.recordReportRequest("events_excel", deviceIds.size());

            ArrayList<DeviceReportSection> devicesEvents = new ArrayList<>();
            ArrayList<String> sheetNames = new ArrayList<>();
            HashMap<Long, String> geofenceNames = new HashMap<>();
            HashMap<Long, String> maintenanceNames = new HashMap<>();
            HashMap<Long, Position> positions = new HashMap<>();
            for (Device device: DeviceUtil.getAccessibleDevices(storage, userId, deviceIds, groupIds)) {
                Collection<Event> events = getEvents(device.getId(), from, to);
                boolean all = types.isEmpty() || types.contains(Event.ALL_EVENTS);
                for (Iterator<Event> iterator = events.iterator(); iterator.hasNext();) {
                    Event event = iterator.next();
                    if (all || filterType(types, alarms, event)) {
                        long geofenceId = event.getGeofenceId();
                        long maintenanceId = event.getMaintenanceId();
                        if (geofenceId != 0) {
                            Geofence geofence = reportUtils.getObject(userId, Geofence.class, geofenceId);
                            if (geofence != null) {
                                geofenceNames.put(geofenceId, geofence.getName());
                            } else {
                                iterator.remove();
                            }
                        } else if (maintenanceId != 0) {
                            Maintenance maintenance = reportUtils.getObject(userId, Maintenance.class, maintenanceId);
                            if (maintenance != null) {
                                maintenanceNames.put(maintenanceId, maintenance.getName());
                            } else {
                                iterator.remove();
                            }
                        }
                    } else {
                        iterator.remove();
                    }
                }
                for (Event event : events) {
                    long positionId = event.getPositionId();
                    if (positionId > 0) {
                        Position position = storage.getObject(Position.class, new Request(
                                new Columns.All(), new Condition.Equals("id", positionId)));
                        positions.put(positionId, position);
                    }
                }
                DeviceReportSection deviceEvents = new DeviceReportSection();
                deviceEvents.setDeviceName(device.getName());
                sheetNames.add(WorkbookUtil.createSafeSheetName(deviceEvents.getDeviceName()));
                if (device.getGroupId() > 0) {
                    Group group = storage.getObject(Group.class, new Request(
                            new Columns.All(), new Condition.Equals("id", device.getGroupId())));
                    if (group != null) {
                        deviceEvents.setGroupName(group.getName());
                    }
                }
                deviceEvents.setObjects(events);
                devicesEvents.add(deviceEvents);
            }

            File file = Paths.get(config.getString(Keys.TEMPLATES_ROOT), "export", "events.xlsx").toFile();
            try (InputStream inputStream = new FileInputStream(file)) {
                var context = reportUtils.initializeContext(userId);
                context.putVar("devices", devicesEvents);
                context.putVar("sheetNames", sheetNames);
                context.putVar("geofenceNames", geofenceNames);
                context.putVar("maintenanceNames", maintenanceNames);
                context.putVar("positions", positions);
                context.putVar("from", from);
                context.putVar("to", to);
                reportUtils.processTemplateWithSheets(inputStream, outputStream, context);
            }
            reportMetrics.recordReportCompletion("events_excel");
        } finally {
            span.end();
        }
    }

    /**
     * Asynchronously generate an Excel report and store it in object storage.
     * 
     * @param userId User ID requesting the report
     * @param deviceIds Collection of device IDs to include in the report
     * @param groupIds Collection of group IDs to include in the report
     * @param types Collection of event types to include in the report
     * @param alarms Collection of alarm types to include in the report
     * @param from Start date for the report period
     * @param to End date for the report period
     * @return CompletableFuture with the URL to the stored report
     */
    public CompletableFuture<String> getExcelAsync(
            long userId, Collection<Long> deviceIds, Collection<Long> groupIds,
            Collection<String> types, Collection<String> alarms,
            Date from, Date to) {
        Context context = tracingUtils.getContext();
        String reportId = UUID.randomUUID().toString();
        
        return CompletableFuture.supplyAsync(() -> {
            Span span = tracingUtils.startSpanFromContext(context, "EventsReportProvider.getExcelAsync");
            try {
                reportMetrics.recordReportRequest("events_excel_async", deviceIds.size());
                
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                try {
                    getExcel(outputStream, userId, deviceIds, groupIds, types, alarms, from, to);
                    
                    // Store the report in object storage
                    String objectKey = String.format("reports/%d/events_%s.xlsx", userId, reportId);
                    String contentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
                    String url = objectStorageClient.uploadFile(objectKey, inputStream, contentType);
                    
                    // Publish report completion event to message broker
                    messageBrokerClient.publishReportCompletion(userId, "events_excel", url);
                    
                    reportMetrics.recordReportCompletion("events_excel_async");
                    return url;
                } catch (StorageException | IOException e) {
                    reportMetrics.recordReportError("events_excel_async");
                    throw new RuntimeException("Failed to generate events report", e);
                }
            } finally {
                span.end();
            }
        });
    }
}