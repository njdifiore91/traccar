/*
 * Copyright 2015 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.session;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.Channel;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.traccar.Protocol;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.database.DeviceLookupService;
import org.traccar.model.BaseModel;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.LogRecord;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.session.cache.CacheManager;
import org.traccar.session.store.DistributedSessionStore;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;
import org.traccar.session.event.SessionEventPublisher;
import org.traccar.session.discovery.ServiceDiscoveryManager;
import org.traccar.scheduler.ScheduleManager;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Singleton
public class ConnectionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

    private final long deviceTimeout;
    private final boolean showUnknownDevices;

    private final DistributedSessionStore sessionStore;
    private final Map<ConnectionKey, String> unknownByEndpoint = new HashMap<>();

    private final Config config;
    private final CacheManager cacheManager;
    private final Storage storage;
    private final Timer timer;
    private final DeviceLookupService deviceLookupService;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final SessionEventPublisher eventPublisher;
    private final ScheduleManager scheduleManager;
    private final MeterRegistry meterRegistry;
    private final CircuitBreaker circuitBreaker;

    private final Map<Long, Set<UpdateListener>> listeners = new HashMap<>();
    private final Map<Long, Set<Long>> userDevices = new HashMap<>();
    private final Map<Long, Set<Long>> deviceUsers = new HashMap<>();

    private final Map<Long, Timeout> timeouts = new HashMap<>();

    // Metrics
    private final Counter deviceConnectionCounter;
    private final Counter deviceDisconnectionCounter;
    private final Timer deviceLookupTimer;
    private final Timer sessionOperationTimer;

    @Inject
    public ConnectionManager(
            Config config, CacheManager cacheManager, Storage storage,
            Timer timer, DeviceLookupService deviceLookupService,
            ServiceDiscoveryManager serviceDiscoveryManager,
            SessionEventPublisher eventPublisher,
            ScheduleManager scheduleManager,
            MeterRegistry meterRegistry) {
        this.config = config;
        this.cacheManager = cacheManager;
        this.storage = storage;
        this.timer = timer;
        this.deviceLookupService = deviceLookupService;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.eventPublisher = eventPublisher;
        this.scheduleManager = scheduleManager;
        this.meterRegistry = meterRegistry;
        this.deviceTimeout = config.getLong(Keys.STATUS_TIMEOUT);
        this.showUnknownDevices = config.getBoolean(Keys.WEB_SHOW_UNKNOWN_DEVICES);
        
        // Initialize distributed session store
        this.sessionStore = new DistributedSessionStore(serviceDiscoveryManager);
        
        // Initialize metrics
        this.deviceConnectionCounter = meterRegistry.counter("device.connections");
        this.deviceDisconnectionCounter = meterRegistry.counter("device.disconnections");
        this.deviceLookupTimer = meterRegistry.timer("device.lookup.time");
        this.sessionOperationTimer = meterRegistry.timer("session.operation.time");
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(3)
                .slidingWindowSize(10)
                .build();
        this.circuitBreaker = CircuitBreaker.of("connectionManager", circuitBreakerConfig);
        
        // Register with service discovery
        serviceDiscoveryManager.register("connection-manager", "session");
        
        // Setup graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    public DeviceSession getDeviceSession(long deviceId) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("getDeviceSession").start();
        span.setTag("deviceId", deviceId);
        
        try {
            return sessionOperationTimer.record(() -> {
                return circuitBreaker.executeSupplier(() -> sessionStore.getSession(deviceId));
            });
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.warn("Failed to get device session for device {}: {}", deviceId, e.getMessage());
            return null;
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    public DeviceSession getDeviceSession(
            Protocol protocol, Channel channel, SocketAddress remoteAddress,
            String... uniqueIds) throws Exception {
        
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("getDeviceSession").start();
        span.setTag("protocol", protocol.getName());
        span.setTag("remoteAddress", remoteAddress.toString());
        
        try {
            ConnectionKey connectionKey = new ConnectionKey(channel, remoteAddress);
            Map<String, DeviceSession> endpointSessions = sessionStore.getSessionsByEndpoint(connectionKey);

            uniqueIds = Arrays.stream(uniqueIds).filter(Objects::nonNull).toArray(String[]::new);
            if (uniqueIds.length > 0) {
                for (String uniqueId : uniqueIds) {
                    span.setTag("uniqueId", uniqueId);
                    DeviceSession deviceSession = endpointSessions.get(uniqueId);
                    if (deviceSession != null) {
                        return deviceSession;
                    }
                }
            } else {
                return endpointSessions.values().stream().findAny().orElse(null);
            }

            Device device = deviceLookupTimer.record((Supplier<Device>) () -> {
                try {
                    return deviceLookupService.lookup(uniqueIds);
                } catch (Exception e) {
                    LOGGER.warn("Device lookup failed", e);
                    return null;
                }
            });

            String firstUniqueId = uniqueIds[0];
            if (device == null && config.getBoolean(Keys.DATABASE_REGISTER_UNKNOWN)) {
                if (firstUniqueId.matches(config.getString(Keys.DATABASE_REGISTER_UNKNOWN_REGEX))) {
                    device = addUnknownDevice(firstUniqueId);
                }
            }

            if (device != null) {
                unknownByEndpoint.remove(connectionKey);
                device.checkDisabled();

                DeviceSession oldSession = sessionStore.removeSession(device.getId());
                if (oldSession != null) {
                    Map<String, DeviceSession> oldEndpointSessions = sessionStore.getSessionsByEndpoint(oldSession.getConnectionKey());
                    if (oldEndpointSessions != null && oldEndpointSessions.size() > 1) {
                        oldEndpointSessions.remove(device.getUniqueId());
                        sessionStore.updateSessionsByEndpoint(oldSession.getConnectionKey(), oldEndpointSessions);
                    } else {
                        sessionStore.removeSessionsByEndpoint(oldSession.getConnectionKey());
                    }
                }

                DeviceSession deviceSession = new DeviceSession(
                        device.getId(), device.getUniqueId(), device.getModel(), protocol, channel, remoteAddress);
                endpointSessions.put(device.getUniqueId(), deviceSession);
                sessionStore.updateSessionsByEndpoint(connectionKey, endpointSessions);
                sessionStore.putSession(device.getId(), deviceSession);

                if (oldSession == null) {
                    cacheManager.addDevice(device.getId(), connectionKey);
                    deviceConnectionCounter.increment();
                    eventPublisher.publishDeviceConnected(device.getId());
                }

                return deviceSession;
            } else {
                unknownByEndpoint.put(connectionKey, firstUniqueId);
                LOGGER.warn("Unknown device - " + String.join(" ", uniqueIds)
                        + " (" + ((InetSocketAddress) remoteAddress).getHostString() + ")");
                return null;
            }
        } catch (Exception e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            throw e;
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    private Device addUnknownDevice(String uniqueId) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("addUnknownDevice").start();
        span.setTag("uniqueId", uniqueId);
        
        try {
            Device device = new Device();
            device.setName(uniqueId);
            device.setUniqueId(uniqueId);
            device.setCategory(config.getString(Keys.DATABASE_REGISTER_UNKNOWN_DEFAULT_CATEGORY));

            long defaultGroupId = config.getLong(Keys.DATABASE_REGISTER_UNKNOWN_DEFAULT_GROUP_ID);
            if (defaultGroupId != 0) {
                device.setGroupId(defaultGroupId);
            }

            try {
                device.setId(storage.addObject(device, new Request(new Columns.Exclude("id"))));
                LOGGER.info("Automatically registered " + uniqueId);
                return device;
            } catch (StorageException e) {
                Tags.ERROR.set(span, true);
                span.log(Map.of("error.message", e.getMessage()));
                LOGGER.warn("Automatic registration failed", e);
                return null;
            }
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    public void deviceDisconnected(Channel channel, boolean supportsOffline) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("deviceDisconnected").start();
        
        try {
            SocketAddress remoteAddress = channel.remoteAddress();
            if (remoteAddress != null) {
                ConnectionKey connectionKey = new ConnectionKey(channel, remoteAddress);
                Map<String, DeviceSession> endpointSessions = sessionStore.removeSessionsByEndpoint(connectionKey);
                if (endpointSessions != null) {
                    for (DeviceSession deviceSession : endpointSessions.values()) {
                        if (supportsOffline) {
                            updateDevice(deviceSession.getDeviceId(), Device.STATUS_OFFLINE, null);
                        }
                        sessionStore.removeSession(deviceSession.getDeviceId());
                        cacheManager.removeDevice(deviceSession.getDeviceId(), connectionKey);
                        deviceDisconnectionCounter.increment();
                        eventPublisher.publishDeviceDisconnected(deviceSession.getDeviceId());
                    }
                }
                unknownByEndpoint.remove(connectionKey);
            }
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    public void deviceUnknown(long deviceId) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("deviceUnknown").start();
        span.setTag("deviceId", deviceId);
        
        try {
            updateDevice(deviceId, Device.STATUS_UNKNOWN, null);
            removeDeviceSession(deviceId);
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    private void removeDeviceSession(long deviceId) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("removeDeviceSession").start();
        span.setTag("deviceId", deviceId);
        
        try {
            DeviceSession deviceSession = sessionStore.removeSession(deviceId);
            if (deviceSession != null) {
                ConnectionKey connectionKey = deviceSession.getConnectionKey();
                cacheManager.removeDevice(deviceId, connectionKey);
                Map<String, DeviceSession> sessions = sessionStore.getSessionsByEndpoint(connectionKey);
                if (sessions != null) {
                    sessions.remove(deviceSession.getUniqueId());
                    if (sessions.isEmpty()) {
                        sessionStore.removeSessionsByEndpoint(connectionKey);
                    } else {
                        sessionStore.updateSessionsByEndpoint(connectionKey, sessions);
                    }
                }
                eventPublisher.publishDeviceSessionRemoved(deviceId);
            }
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    public void updateDevice(long deviceId, String status, Date time) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("updateDevice").start();
        span.setTag("deviceId", deviceId);
        span.setTag("status", status);
        if (time != null) {
            span.setTag("time", time.toString());
        }
        
        try {
            Device device = cacheManager.getObject(Device.class, deviceId);
            if (device == null) {
                try {
                    device = storage.getObject(Device.class, new Request(
                            new Columns.All(), new Condition.Equals("id", deviceId)));
                } catch (StorageException e) {
                    Tags.ERROR.set(span, true);
                    span.log(Map.of("error.message", e.getMessage()));
                    LOGGER.warn("Failed to get device", e);
                }
                if (device == null) {
                    return;
                }
            }

            String oldStatus = device.getStatus();
            device.setStatus(status);

            if (!status.equals(oldStatus)) {
                String eventType;
                Map<Event, Position> events = new HashMap<>();
                eventType = switch (status) {
                    case Device.STATUS_ONLINE -> Event.TYPE_DEVICE_ONLINE;
                    case Device.STATUS_UNKNOWN -> Event.TYPE_DEVICE_UNKNOWN;
                    default -> Event.TYPE_DEVICE_OFFLINE;
                };
                events.put(new Event(eventType, deviceId), null);
                eventPublisher.publishEvents(events);
            }

            if (time != null) {
                device.setLastUpdate(time);
            }

            Timeout timeout = timeouts.remove(deviceId);
            if (timeout != null) {
                timeout.cancel();
            }

            if (status.equals(Device.STATUS_ONLINE)) {
                scheduleManager.scheduleDeviceTimeout(deviceId, deviceTimeout, TimeUnit.SECONDS, 
                        () -> deviceUnknown(deviceId));
            }

            try {
                storage.updateObject(device, new Request(
                        new Columns.Include("status", "lastUpdate"),
                        new Condition.Equals("id", deviceId)));
            } catch (StorageException e) {
                Tags.ERROR.set(span, true);
                span.log(Map.of("error.message", e.getMessage()));
                LOGGER.warn("Update device status error", e);
            }

            updateDevice(true, device);
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    public synchronized void sendKeepalive() {
        for (Set<UpdateListener> userListeners : listeners.values()) {
            for (UpdateListener listener : userListeners) {
                listener.onKeepalive();
            }
        }
    }

    public synchronized void updateDevice(boolean local, Device device) {
        if (local) {
            eventPublisher.publishDeviceUpdate(device);
        } else if (Device.STATUS_ONLINE.equals(device.getStatus())) {
            scheduleManager.cancelDeviceTimeout(device.getId());
            removeDeviceSession(device.getId());
        }
        for (long userId : deviceUsers.getOrDefault(device.getId(), Collections.emptySet())) {
            if (listeners.containsKey(userId)) {
                for (UpdateListener listener : listeners.get(userId)) {
                    listener.onUpdateDevice(device);
                }
            }
        }
    }

    public synchronized void updatePosition(boolean local, Position position) {
        if (local) {
            eventPublisher.publishPositionUpdate(position);
        }
        for (long userId : deviceUsers.getOrDefault(position.getDeviceId(), Collections.emptySet())) {
            if (listeners.containsKey(userId)) {
                for (UpdateListener listener : listeners.get(userId)) {
                    listener.onUpdatePosition(position);
                }
            }
        }
    }

    public synchronized void updateEvent(boolean local, long userId, Event event) {
        if (local) {
            eventPublisher.publishEventUpdate(userId, event);
        }
        if (listeners.containsKey(userId)) {
            for (UpdateListener listener : listeners.get(userId)) {
                listener.onUpdateEvent(event);
            }
        }
    }

    public synchronized <T1 extends BaseModel, T2 extends BaseModel> void invalidatePermission(
            boolean local, Class<T1> clazz1, long id1, Class<T2> clazz2, long id2, boolean link) {
        if (link && clazz1.equals(User.class) && clazz2.equals(Device.class)) {
            if (listeners.containsKey(id1)) {
                userDevices.get(id1).add(id2);
                deviceUsers.put(id2, new HashSet<>(List.of(id1)));
            }
        }
        if (local) {
            eventPublisher.publishPermissionInvalidation(clazz1, id1, clazz2, id2, link);
        }
    }

    public synchronized void updateLog(LogRecord record) {
        var sessions = sessionStore.getSessionsByEndpoint(record.getConnectionKey());
        if (sessions == null || sessions.isEmpty()) {
            String unknownUniqueId = unknownByEndpoint.get(record.getConnectionKey());
            if (unknownUniqueId != null && showUnknownDevices) {
                record.setUniqueId(unknownUniqueId);
                listeners.values().stream()
                        .flatMap(Set::stream)
                        .forEach((listener) -> listener.onUpdateLog(record));
            }
        } else {
            var firstEntry = sessions.entrySet().iterator().next();
            record.setUniqueId(firstEntry.getKey());
            record.setDeviceId(firstEntry.getValue().getDeviceId());
            for (long userId : deviceUsers.getOrDefault(record.getDeviceId(), Set.of())) {
                for (UpdateListener listener : listeners.getOrDefault(userId, Set.of())) {
                    listener.onUpdateLog(record);
                }
            }
        }
    }

    public interface UpdateListener {
        void onKeepalive();
        void onUpdateDevice(Device device);
        void onUpdatePosition(Position position);
        void onUpdateEvent(Event event);
        void onUpdateLog(LogRecord record);
    }

    public synchronized void addListener(long userId, UpdateListener listener) throws StorageException {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("addListener").start();
        span.setTag("userId", userId);
        
        try {
            var set = listeners.get(userId);
            if (set == null) {
                set = new HashSet<>();
                listeners.put(userId, set);

                var devices = storage.getObjects(Device.class, new Request(
                        new Columns.Include("id"), new Condition.Permission(User.class, userId, Device.class)));
                userDevices.put(userId, devices.stream().map(BaseModel::getId).collect(Collectors.toSet()));
                devices.forEach(device -> deviceUsers.computeIfAbsent(device.getId(), id -> new HashSet<>()).add(userId));
            }
            set.add(listener);
            eventPublisher.publishListenerAdded(userId);
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    public synchronized void removeListener(long userId, UpdateListener listener) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("removeListener").start();
        span.setTag("userId", userId);
        
        try {
            var set = listeners.get(userId);
            if (set != null) {
                set.remove(listener);
                if (set.isEmpty()) {
                    listeners.remove(userId);

                    userDevices.remove(userId).forEach(deviceId -> deviceUsers.computeIfPresent(deviceId, (x, userIds) -> {
                        userIds.remove(userId);
                        return userIds.isEmpty() ? null : userIds;
                    }));
                }
                eventPublisher.publishListenerRemoved(userId);
            }
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }
    
    private void shutdown() {
        LOGGER.info("Shutting down ConnectionManager...");
        try {
            // Cancel all timeouts
            for (Timeout timeout : timeouts.values()) {
                timeout.cancel();
            }
            timeouts.clear();
            
            // Unregister from service discovery
            serviceDiscoveryManager.unregister("connection-manager");
            
            // Publish shutdown event
            eventPublisher.publishServiceShutdown("connection-manager");
            
            LOGGER.info("ConnectionManager shutdown complete");
        } catch (Exception e) {
            LOGGER.error("Error during ConnectionManager shutdown", e);
        }
    }
}