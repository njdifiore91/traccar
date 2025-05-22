/*
 * Copyright 2016 - 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.api.trace.TraceState;
import org.traccar.storage.StorageName;

import java.util.Date;

/**
 * Event entity class for tracking events across distributed services.
 * Includes support for message broker serialization and distributed tracing.
 */
@StorageName("tc_events")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Event extends Message {

    public Event(String type, Position position) {
        setType(type);
        setPositionId(position.getId());
        setDeviceId(position.getDeviceId());
        eventTime = position.getDeviceTime();
    }

    public Event(String type, long deviceId) {
        setType(type);
        setDeviceId(deviceId);
        eventTime = new Date();
    }

    public Event() {
    }

    // Event type constants for cross-service compatibility
    public static final String ALL_EVENTS = "allEvents";

    public static final String TYPE_COMMAND_RESULT = "commandResult";

    public static final String TYPE_DEVICE_ONLINE = "deviceOnline";
    public static final String TYPE_DEVICE_UNKNOWN = "deviceUnknown";
    public static final String TYPE_DEVICE_OFFLINE = "deviceOffline";
    public static final String TYPE_DEVICE_INACTIVE = "deviceInactive";
    public static final String TYPE_QUEUED_COMMAND_SENT = "queuedCommandSent";

    public static final String TYPE_DEVICE_MOVING = "deviceMoving";
    public static final String TYPE_DEVICE_STOPPED = "deviceStopped";

    public static final String TYPE_DEVICE_OVERSPEED = "deviceOverspeed";
    public static final String TYPE_DEVICE_FUEL_DROP = "deviceFuelDrop";
    public static final String TYPE_DEVICE_FUEL_INCREASE = "deviceFuelIncrease";

    public static final String TYPE_GEOFENCE_ENTER = "geofenceEnter";
    public static final String TYPE_GEOFENCE_EXIT = "geofenceExit";

    public static final String TYPE_ALARM = "alarm";

    public static final String TYPE_IGNITION_ON = "ignitionOn";
    public static final String TYPE_IGNITION_OFF = "ignitionOff";

    public static final String TYPE_MAINTENANCE = "maintenance";
    public static final String TYPE_TEXT_MESSAGE = "textMessage";
    public static final String TYPE_DRIVER_CHANGED = "driverChanged";
    public static final String TYPE_MEDIA = "media";

    // Distributed tracing context fields
    private String traceId;
    private String spanId;
    private byte traceFlags;
    private String traceState;

    // Event correlation fields
    private String correlationId;
    private String parentEventId;
    private String serviceOrigin;

    @JsonProperty("eventTime")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    private Date eventTime;

    public Date getEventTime() {
        return eventTime;
    }

    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }

    @JsonProperty("positionId")
    private long positionId;

    public long getPositionId() {
        return positionId;
    }

    public void setPositionId(long positionId) {
        this.positionId = positionId;
    }

    @JsonProperty("geofenceId")
    private long geofenceId = 0;

    public long getGeofenceId() {
        return geofenceId;
    }

    public void setGeofenceId(long geofenceId) {
        this.geofenceId = geofenceId;
    }

    @JsonProperty("maintenanceId")
    private long maintenanceId = 0;

    public long getMaintenanceId() {
        return maintenanceId;
    }

    public void setMaintenanceId(long maintenanceId) {
        this.maintenanceId = maintenanceId;
    }

    /**
     * Gets the OpenTelemetry trace ID for distributed tracing.
     * @return the trace ID as a string
     */
    @JsonProperty("traceId")
    public String getTraceId() {
        return traceId;
    }

    /**
     * Sets the OpenTelemetry trace ID for distributed tracing.
     * @param traceId the trace ID as a string
     */
    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    /**
     * Gets the OpenTelemetry span ID for distributed tracing.
     * @return the span ID as a string
     */
    @JsonProperty("spanId")
    public String getSpanId() {
        return spanId;
    }

    /**
     * Sets the OpenTelemetry span ID for distributed tracing.
     * @param spanId the span ID as a string
     */
    public void setSpanId(String spanId) {
        this.spanId = spanId;
    }

    /**
     * Gets the OpenTelemetry trace flags for distributed tracing.
     * @return the trace flags as a byte
     */
    @JsonProperty("traceFlags")
    public byte getTraceFlags() {
        return traceFlags;
    }

    /**
     * Sets the OpenTelemetry trace flags for distributed tracing.
     * @param traceFlags the trace flags as a byte
     */
    public void setTraceFlags(byte traceFlags) {
        this.traceFlags = traceFlags;
    }

    /**
     * Gets the OpenTelemetry trace state for distributed tracing.
     * @return the trace state as a string
     */
    @JsonProperty("traceState")
    public String getTraceState() {
        return traceState;
    }

    /**
     * Sets the OpenTelemetry trace state for distributed tracing.
     * @param traceState the trace state as a string
     */
    public void setTraceState(String traceState) {
        this.traceState = traceState;
    }

    /**
     * Gets the correlation ID for linking related events across services.
     * @return the correlation ID as a string
     */
    @JsonProperty("correlationId")
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * Sets the correlation ID for linking related events across services.
     * @param correlationId the correlation ID as a string
     */
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    /**
     * Gets the parent event ID for establishing event hierarchies.
     * @return the parent event ID as a string
     */
    @JsonProperty("parentEventId")
    public String getParentEventId() {
        return parentEventId;
    }

    /**
     * Sets the parent event ID for establishing event hierarchies.
     * @param parentEventId the parent event ID as a string
     */
    public void setParentEventId(String parentEventId) {
        this.parentEventId = parentEventId;
    }

    /**
     * Gets the service origin identifier.
     * @return the service origin as a string
     */
    @JsonProperty("serviceOrigin")
    public String getServiceOrigin() {
        return serviceOrigin;
    }

    /**
     * Sets the service origin identifier.
     * @param serviceOrigin the service origin as a string
     */
    public void setServiceOrigin(String serviceOrigin) {
        this.serviceOrigin = serviceOrigin;
    }

    /**
     * Creates a SpanContext from the trace information in this event.
     * @return a SpanContext object for OpenTelemetry tracing
     */
    @JsonIgnore
    public SpanContext getSpanContext() {
        if (traceId == null || spanId == null) {
            return null;
        }
        return SpanContext.create(
                traceId,
                spanId,
                TraceFlags.fromByte(traceFlags),
                traceState != null ? TraceState.fromHexString(traceState) : TraceState.getDefault());
    }

    /**
     * Sets the trace information from a SpanContext.
     * @param spanContext the SpanContext to extract trace information from
     */
    public void setSpanContext(SpanContext spanContext) {
        if (spanContext != null) {
            this.traceId = spanContext.getTraceId();
            this.spanId = spanContext.getSpanId();
            this.traceFlags = spanContext.getTraceFlags().asByte();
            this.traceState = spanContext.getTraceState().asHexString();
        }
    }
}