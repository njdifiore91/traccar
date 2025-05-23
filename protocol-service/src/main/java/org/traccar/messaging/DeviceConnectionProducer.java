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
package org.traccar.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Device;
import org.traccar.session.DeviceSession;
import org.traccar.session.cache.CacheManager;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Specializes the MessageProducer interface for publishing device connection events
 * to the 'device.connections' topic. It handles connection event serialization,
 * ensures reliable delivery of critical connection state changes, and includes device metadata.
 */
@Singleton
public class DeviceConnectionProducer implements MessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceConnectionProducer.class);

    private final org.traccar.messaging.MessageProducer messageProducer;
    private final CacheManager cacheManager;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final MessagingTracer messagingTracer;

    // Metrics
    private final Counter connectionEventCounter;
    private final Counter disconnectionEventCounter;
    private final Counter idleEventCounter;
    private final Counter errorCounter;
    private final Timer publishTimer;

    /**
     * Connection event types
     */
    public enum ConnectionEventType {
        CONNECTED,
        DISCONNECTED,
        IDLE
    }

    /**
     * Connection event data structure
     */
    public static class ConnectionEvent {
        private long deviceId;
        private String uniqueId;
        private String type;
        private String protocol;
        private String status;
        private Instant timestamp;
        private Map<String, Object> attributes;

        public ConnectionEvent() {
            // Jackson deserialization
        }

        public ConnectionEvent(long deviceId, String uniqueId, String type, String protocol, String status) {
            this.deviceId = deviceId;
            this.uniqueId = uniqueId;
            this.type = type;
            this.protocol = protocol;
            this.status = status;
            this.timestamp = Instant.now();
            this.attributes = new HashMap<>();
        }

        public long getDeviceId() {
            return deviceId;
        }

        public void setDeviceId(long deviceId) {
            this.deviceId = deviceId;
        }

        public String getUniqueId() {
            return uniqueId;
        }

        public void setUniqueId(String uniqueId) {
            this.uniqueId = uniqueId;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getProtocol() {
            return protocol;
        }

        public void setProtocol(String protocol) {
            this.protocol = protocol;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public Instant getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Instant timestamp) {
            this.timestamp = timestamp;
        }

        public Map<String, Object> getAttributes() {
            return attributes;
        }

        public void setAttributes(Map<String, Object> attributes) {
            this.attributes = attributes;
        }

        public void addAttribute(String key, Object value) {
            if (this.attributes == null) {
                this.attributes = new HashMap<>();
            }
            this.attributes.put(key, value);
        }
    }

    @Inject
    public DeviceConnectionProducer(
            org.traccar.messaging.MessageProducer messageProducer,
            CacheManager cacheManager,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            Tracer tracer,
            MessagingTracer messagingTracer) {
        this.messageProducer = messageProducer;
        this.cacheManager = cacheManager;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        this.messagingTracer = messagingTracer;

        // Initialize metrics
        this.connectionEventCounter = meterRegistry.counter("device.connection.events", "type", "connected");
        this.disconnectionEventCounter = meterRegistry.counter("device.connection.events", "type", "disconnected");
        this.idleEventCounter = meterRegistry.counter("device.connection.events", "type", "idle");
        this.errorCounter = meterRegistry.counter("device.connection.errors");
        this.publishTimer = meterRegistry.timer("device.connection.publish.time");
    }

    /**
     * Publish a device connection event
     *
     * @param deviceId The device ID
     * @param eventType The connection event type
     * @return CompletableFuture that completes when the message is acknowledged
     */
    public CompletableFuture<Void> publishConnectionEvent(long deviceId, ConnectionEventType eventType) {
        Span span = tracer.spanBuilder("publishConnectionEvent").startSpan();
        Context context = Context.current().with(span);
        
        try {
            Device device = cacheManager.getObject(Device.class, deviceId);
            if (device == null) {
                LOGGER.warn("Cannot publish connection event for unknown device: {}", deviceId);
                errorCounter.increment();
                return CompletableFuture.completedFuture(null);
            }

            String status;
            switch (eventType) {
                case CONNECTED:
                    status = Device.STATUS_ONLINE;
                    connectionEventCounter.increment();
                    break;
                case DISCONNECTED:
                    status = Device.STATUS_OFFLINE;
                    disconnectionEventCounter.increment();
                    break;
                case IDLE:
                    status = Device.STATUS_UNKNOWN;
                    idleEventCounter.increment();
                    break;
                default:
                    LOGGER.warn("Unknown connection event type: {}", eventType);
                    errorCounter.increment();
                    return CompletableFuture.completedFuture(null);
            }

            ConnectionEvent event = new ConnectionEvent(
                    deviceId,
                    device.getUniqueId(),
                    eventType.name(),
                    device.getProtocol(),
                    status);

            // Add device metadata
            event.addAttribute("deviceId", deviceId);
            event.addAttribute("uniqueId", device.getUniqueId());
            if (device.getName() != null) {
                event.addAttribute("name", device.getName());
            }
            if (device.getModel() != null) {
                event.addAttribute("model", device.getModel());
            }
            if (device.getContact() != null) {
                event.addAttribute("contact", device.getContact());
            }
            if (device.getCategory() != null) {
                event.addAttribute("category", device.getCategory());
            }

            // Create message headers with tracing information
            Map<String, Object> headers = messagingTracer.getHeadersFromCurrentContext();
            headers.put("deviceId", String.valueOf(deviceId));
            headers.put("eventType", eventType.name());
            headers.put("timestamp", String.valueOf(System.currentTimeMillis()));

            // Publish the event with device ID as routing key for partitioning
            return publishTimer.recordCallable(() -> {
                try {
                    return messageProducer.publish(
                            TopicNames.getDeviceConnectionsTopic(),
                            String.valueOf(deviceId),
                            event,
                            headers);
                } catch (Exception e) {
                    LOGGER.error("Failed to publish connection event for device {}: {}", deviceId, e.getMessage(), e);
                    errorCounter.increment();
                    throw e;
                }
            });
        } catch (Exception e) {
            LOGGER.error("Error preparing connection event for device {}: {}", deviceId, e.getMessage(), e);
            errorCounter.increment();
            return CompletableFuture.failedFuture(e);
        } finally {
            span.end();
        }
    }

    /**
     * Publish a device connection event with session information
     *
     * @param session The device session
     * @param eventType The connection event type
     * @return CompletableFuture that completes when the message is acknowledged
     */
    public CompletableFuture<Void> publishConnectionEvent(DeviceSession session, ConnectionEventType eventType) {
        Span span = tracer.spanBuilder("publishConnectionEventWithSession").startSpan();
        Context context = Context.current().with(span);
        
        try {
            if (session == null) {
                LOGGER.warn("Cannot publish connection event for null session");
                errorCounter.increment();
                return CompletableFuture.completedFuture(null);
            }

            Device device = cacheManager.getObject(Device.class, session.getDeviceId());
            if (device == null) {
                LOGGER.warn("Cannot publish connection event for unknown device: {}", session.getDeviceId());
                errorCounter.increment();
                return CompletableFuture.completedFuture(null);
            }

            String status;
            switch (eventType) {
                case CONNECTED:
                    status = Device.STATUS_ONLINE;
                    connectionEventCounter.increment();
                    break;
                case DISCONNECTED:
                    status = Device.STATUS_OFFLINE;
                    disconnectionEventCounter.increment();
                    break;
                case IDLE:
                    status = Device.STATUS_UNKNOWN;
                    idleEventCounter.increment();
                    break;
                default:
                    LOGGER.warn("Unknown connection event type: {}", eventType);
                    errorCounter.increment();
                    return CompletableFuture.completedFuture(null);
            }

            ConnectionEvent event = new ConnectionEvent(
                    session.getDeviceId(),
                    session.getUniqueId(),
                    eventType.name(),
                    device.getProtocol(),
                    status);

            // Add device metadata
            event.addAttribute("deviceId", session.getDeviceId());
            event.addAttribute("uniqueId", session.getUniqueId());
            if (device.getName() != null) {
                event.addAttribute("name", device.getName());
            }
            if (session.getModel() != null) {
                event.addAttribute("model", session.getModel());
            } else if (device.getModel() != null) {
                event.addAttribute("model", device.getModel());
            }
            if (device.getContact() != null) {
                event.addAttribute("contact", device.getContact());
            }
            if (device.getCategory() != null) {
                event.addAttribute("category", device.getCategory());
            }

            // Add session metadata
            event.addAttribute("sessionId", session.getCorrelationId());
            event.addAttribute("creationTime", session.getCreationTime().toString());
            event.addAttribute("lastAccessTime", session.getLastAccessTime().toString());
            event.addAttribute("version", session.getVersion());

            // Create message headers with tracing information
            Map<String, Object> headers = messagingTracer.getHeadersFromCurrentContext();
            headers.put("deviceId", String.valueOf(session.getDeviceId()));
            headers.put("eventType", eventType.name());
            headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
            headers.put("sessionId", session.getCorrelationId());

            // Publish the event with device ID as routing key for partitioning
            return publishTimer.recordCallable(() -> {
                try {
                    return messageProducer.publish(
                            TopicNames.getDeviceConnectionsTopic(),
                            String.valueOf(session.getDeviceId()),
                            event,
                            headers);
                } catch (Exception e) {
                    LOGGER.error("Failed to publish connection event for device {}: {}", 
                            session.getDeviceId(), e.getMessage(), e);
                    errorCounter.increment();
                    throw e;
                }
            });
        } catch (Exception e) {
            LOGGER.error("Error preparing connection event for device {}: {}", 
                    session != null ? session.getDeviceId() : "unknown", e.getMessage(), e);
            errorCounter.increment();
            return CompletableFuture.failedFuture(e);
        } finally {
            span.end();
        }
    }

    @Override
    public void sendPosition(org.traccar.model.Position position) throws Exception {
        // Not implemented - this producer specializes in device connection events
        throw new UnsupportedOperationException("DeviceConnectionProducer does not support sending positions");
    }

    @Override
    public void sendDeviceConnectionStatus(long deviceId, boolean connected) throws Exception {
        ConnectionEventType eventType = connected ? ConnectionEventType.CONNECTED : ConnectionEventType.DISCONNECTED;
        publishConnectionEvent(deviceId, eventType).join(); // Block until complete for backward compatibility
    }

    @Override
    public void close() {
        // No resources to close in this specialized producer
    }
}