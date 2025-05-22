/*
 * Copyright 2020 - 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.schedule;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Group;
import org.traccar.model.Position;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TaskDeviceInactivityCheck extends SingleScheduleTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskDeviceInactivityCheck.class);

    public static final String ATTRIBUTE_DEVICE_INACTIVITY_START = "deviceInactivityStart";
    public static final String ATTRIBUTE_DEVICE_INACTIVITY_PERIOD = "deviceInactivityPeriod";
    public static final String ATTRIBUTE_LAST_UPDATE = "lastUpdate";

    private static final long CHECK_PERIOD_MINUTES = 15;
    private static final String TOPIC_DEVICE_INACTIVITY = "device.inactivity";
    private static final String LEADER_KEY = "inactivity-check-leader";
    private static final int DEFAULT_PARTITION_SIZE = 100;

    private final Storage storage;
    private final MessageProducer messageProducer;
    private final ServiceDiscovery serviceDiscovery;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Config config;

    private final Timer checkTimer;
    private final Counter eventsCounter;
    private final Counter devicesCheckedCounter;

    @Inject
    public TaskDeviceInactivityCheck(
            Storage storage,
            MessageProducer messageProducer,
            ServiceDiscovery serviceDiscovery,
            Tracer tracer,
            MeterRegistry meterRegistry,
            Config config) {
        this.storage = storage;
        this.messageProducer = messageProducer;
        this.serviceDiscovery = serviceDiscovery;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.config = config;

        // Initialize metrics
        this.checkTimer = Timer.builder("traccar.inactivity.check.duration")
                .description("Time taken to perform device inactivity check")
                .register(meterRegistry);
        this.eventsCounter = Counter.builder("traccar.inactivity.events")
                .description("Number of inactivity events generated")
                .register(meterRegistry);
        this.devicesCheckedCounter = Counter.builder("traccar.inactivity.devices.checked")
                .description("Number of devices checked for inactivity")
                .register(meterRegistry);
    }

    @Override
    public void schedule(ScheduledExecutorService executor) {
        executor.scheduleAtFixedRate(this, CHECK_PERIOD_MINUTES, CHECK_PERIOD_MINUTES, TimeUnit.MINUTES);
    }

    @Override
    public void run() {
        // Create a span for the entire operation
        Span span = tracer.spanBuilder("DeviceInactivityCheck")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("schedule.period.minutes", CHECK_PERIOD_MINUTES);

            // Check if this instance is the leader
            if (!isLeader()) {
                LOGGER.debug("Not the leader, skipping inactivity check");
                span.addEvent("Skipped - Not Leader");
                return;
            }

            // Measure execution time
            checkTimer.record(() -> {
                processDeviceInactivity(span);
            });

            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.warn("Error during device inactivity check", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }

    private boolean isLeader() {
        try {
            // Use service discovery to determine if this instance should be the leader
            return serviceDiscovery.acquireLeadership(LEADER_KEY);
        } catch (Exception e) {
            LOGGER.warn("Error during leader election, assuming leadership to ensure task execution", e);
            return true; // Default to true to ensure the task runs if service discovery fails
        }
    }

    private void processDeviceInactivity(Span parentSpan) {
        long currentTime = System.currentTimeMillis();
        long checkPeriod = TimeUnit.MINUTES.toMillis(CHECK_PERIOD_MINUTES);
        int partitionSize = config.getInteger("inactivity.partition.size", DEFAULT_PARTITION_SIZE);

        parentSpan.setAttribute("partition.size", partitionSize);

        try {
            // Load all groups for attribute inheritance
            Span groupsSpan = tracer.spanBuilder("LoadGroups")
                    .setParent(parentSpan.getSpanContext())
                    .startSpan();
            
            Map<Long, Group> groups;
            try {
                groups = storage.getObjects(Group.class, new Request(new Columns.All()))
                        .stream().collect(Collectors.toMap(Group::getId, group -> group));
                groupsSpan.setAttribute("groups.count", groups.size());
                groupsSpan.setStatus(StatusCode.OK);
            } catch (StorageException e) {
                LOGGER.warn("Error loading groups", e);
                groupsSpan.recordException(e);
                groupsSpan.setStatus(StatusCode.ERROR, e.getMessage());
                throw e;
            } finally {
                groupsSpan.end();
            }

            // Get total device count for partitioning
            Span countSpan = tracer.spanBuilder("CountDevices")
                    .setParent(parentSpan.getSpanContext())
                    .startSpan();
            
            long deviceCount;
            try {
                deviceCount = storage.getObjects(Device.class, new Request(new Columns.Count()));
                countSpan.setAttribute("devices.total", deviceCount);
                countSpan.setStatus(StatusCode.OK);
            } catch (StorageException e) {
                LOGGER.warn("Error counting devices", e);
                countSpan.recordException(e);
                countSpan.setStatus(StatusCode.ERROR, e.getMessage());
                throw e;
            } finally {
                countSpan.end();
            }

            // Process devices in partitions
            int totalPartitions = (int) Math.ceil((double) deviceCount / partitionSize);
            parentSpan.setAttribute("partitions.total", totalPartitions);

            for (int partition = 0; partition < totalPartitions; partition++) {
                Span partitionSpan = tracer.spanBuilder("ProcessPartition")
                        .setParent(parentSpan.getSpanContext())
                        .startSpan();
                
                partitionSpan.setAttribute("partition.number", partition);
                partitionSpan.setAttribute("partition.offset", partition * partitionSize);
                partitionSpan.setAttribute("partition.limit", partitionSize);

                try {
                    processDevicePartition(partition, partitionSize, groups, currentTime, checkPeriod, partitionSpan);
                    partitionSpan.setStatus(StatusCode.OK);
                } catch (Exception e) {
                    LOGGER.warn("Error processing device partition " + partition, e);
                    partitionSpan.recordException(e);
                    partitionSpan.setStatus(StatusCode.ERROR, e.getMessage());
                } finally {
                    partitionSpan.end();
                }
            }

        } catch (StorageException e) {
            LOGGER.warn("Database error", e);
        }
    }

    private void processDevicePartition(int partition, int partitionSize, Map<Long, Group> groups, 
                                       long currentTime, long checkPeriod, Span parentSpan) throws StorageException {
        int offset = partition * partitionSize;
        
        // Load devices for this partition
        Span loadSpan = tracer.spanBuilder("LoadDevicesPartition")
                .setParent(parentSpan.getSpanContext())
                .startSpan();
        
        List<Device> devices;
        try {
            Request request = new Request(new Columns.All())
                    .setOffset(offset)
                    .setLimit(partitionSize);
            
            devices = storage.getObjects(Device.class, request);
            loadSpan.setAttribute("devices.loaded", devices.size());
            loadSpan.setStatus(StatusCode.OK);
        } finally {
            loadSpan.end();
        }

        // Process devices in this partition
        Map<Event, Position> events = new HashMap<>();
        int inactiveCount = 0;

        for (Device device : devices) {
            devicesCheckedCounter.increment();
            
            if (device.getLastUpdate() != null && checkDevice(device, groups, currentTime, checkPeriod)) {
                Event event = new Event(Event.TYPE_DEVICE_INACTIVE, device.getId());
                event.set(ATTRIBUTE_LAST_UPDATE, device.getLastUpdate().getTime());
                events.put(event, null);
                inactiveCount++;
            }
        }

        // Publish events to message broker
        if (!events.isEmpty()) {
            Span publishSpan = tracer.spanBuilder("PublishInactivityEvents")
                    .setParent(parentSpan.getSpanContext())
                    .startSpan();
            
            try {
                publishSpan.setAttribute("events.count", events.size());
                
                for (Map.Entry<Event, Position> entry : events.entrySet()) {
                    messageProducer.publish(TOPIC_DEVICE_INACTIVITY, entry.getKey());
                }
                
                eventsCounter.increment(events.size());
                publishSpan.setStatus(StatusCode.OK);
            } catch (Exception e) {
                LOGGER.warn("Error publishing inactivity events", e);
                publishSpan.recordException(e);
                publishSpan.setStatus(StatusCode.ERROR, e.getMessage());
            } finally {
                publishSpan.end();
            }
        }

        parentSpan.setAttribute("devices.inactive", inactiveCount);
    }

    private long getAttribute(Device device, Map<Long, Group> groups, String key) {
        long deviceValue = device.getLong(key);
        if (deviceValue > 0) {
            return deviceValue;
        } else {
            long groupId = device.getGroupId();
            while (groupId > 0) {
                Group group = groups.get(groupId);
                if (group == null) {
                    return 0;
                }
                long groupValue = group.getLong(key);
                if (groupValue > 0) {
                    return groupValue;
                }
                groupId = group.getGroupId();
            }
            return 0;
        }
    }

    private boolean checkDevice(Device device, Map<Long, Group> groups, long currentTime, long checkPeriod) {
        long deviceInactivityStart = getAttribute(device, groups, ATTRIBUTE_DEVICE_INACTIVITY_START);
        if (deviceInactivityStart > 0) {
            long timeThreshold = device.getLastUpdate().getTime() + deviceInactivityStart;
            if (currentTime >= timeThreshold) {

                if (currentTime - checkPeriod < timeThreshold) {
                    return true;
                }

                long deviceInactivityPeriod = getAttribute(device, groups, ATTRIBUTE_DEVICE_INACTIVITY_PERIOD);
                if (deviceInactivityPeriod > 0) {
                    long count = (currentTime - timeThreshold - 1) / deviceInactivityPeriod;
                    timeThreshold += count * deviceInactivityPeriod;
                    return currentTime - checkPeriod < timeThreshold;
                }

            }
        }
        return false;
    }

}