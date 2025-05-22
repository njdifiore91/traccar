/*
 * Copyright 2012 - 2023 Anton Tananaev (anton@traccar.org)
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import org.traccar.storage.QueryIgnore;
import org.traccar.storage.StorageName;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Device entity class for the Traccar platform.
 * Enhanced with message broker serialization support and distributed tracing context.
 */
@StorageName("tc_devices")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Device extends GroupedModel implements Disableable, Schedulable, Serializable {

    private static final long serialVersionUID = 1L;

    private long calendarId;

    @Override
    @JsonProperty
    public long getCalendarId() {
        return calendarId;
    }

    @Override
    public void setCalendarId(long calendarId) {
        this.calendarId = calendarId;
    }

    private String name;

    @JsonProperty
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private String uniqueId;

    @JsonProperty
    public String getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(String uniqueId) {
        if (uniqueId != null && uniqueId.contains("..")) {
            throw new IllegalArgumentException("Invalid unique id");
        }
        this.uniqueId = uniqueId != null ? uniqueId.trim() : null;
    }

    public static final String STATUS_UNKNOWN = "unknown";
    public static final String STATUS_ONLINE = "online";
    public static final String STATUS_OFFLINE = "offline";

    private String status;
    private Date statusUpdateTime;

    @QueryIgnore
    @JsonProperty
    public String getStatus() {
        return status != null ? status : STATUS_OFFLINE;
    }

    public void setStatus(String status) {
        this.status = status != null ? status.trim() : null;
        this.statusUpdateTime = new Date();
    }
    
    @QueryIgnore
    @JsonProperty
    public Date getStatusUpdateTime() {
        return statusUpdateTime;
    }

    public void setStatusUpdateTime(Date statusUpdateTime) {
        this.statusUpdateTime = statusUpdateTime;
    }

    private Date lastUpdate;

    @QueryIgnore
    @JsonProperty
    public Date getLastUpdate() {
        return this.lastUpdate;
    }

    public void setLastUpdate(Date lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    private long positionId;

    @QueryIgnore
    @JsonProperty
    public long getPositionId() {
        return positionId;
    }

    public void setPositionId(long positionId) {
        this.positionId = positionId;
    }

    private String phone;

    @JsonProperty
    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone != null ? phone.trim() : null;
    }

    private String model;

    @JsonProperty
    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    private String contact;

    @JsonProperty
    public String getContact() {
        return contact;
    }

    public void setContact(String contact) {
        this.contact = contact;
    }

    private String category;

    @JsonProperty
    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    private boolean disabled;

    @Override
    @JsonProperty
    public boolean getDisabled() {
        return disabled;
    }

    @Override
    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    private Date expirationTime;

    @Override
    @JsonProperty
    public Date getExpirationTime() {
        return expirationTime;
    }

    @Override
    public void setExpirationTime(Date expirationTime) {
        this.expirationTime = expirationTime;
    }

    private boolean motionStreak;

    @QueryIgnore
    @JsonIgnore
    public boolean getMotionStreak() {
        return motionStreak;
    }

    @JsonIgnore
    public void setMotionStreak(boolean motionStreak) {
        this.motionStreak = motionStreak;
    }

    private boolean motionState;

    @QueryIgnore
    @JsonIgnore
    public boolean getMotionState() {
        return motionState;
    }

    @JsonIgnore
    public void setMotionState(boolean motionState) {
        this.motionState = motionState;
    }

    private Date motionTime;

    @QueryIgnore
    @JsonIgnore
    public Date getMotionTime() {
        return motionTime;
    }

    @JsonIgnore
    public void setMotionTime(Date motionTime) {
        this.motionTime = motionTime;
    }

    private double motionDistance;

    @QueryIgnore
    @JsonIgnore
    public double getMotionDistance() {
        return motionDistance;
    }

    @JsonIgnore
    public void setMotionDistance(double motionDistance) {
        this.motionDistance = motionDistance;
    }

    private boolean overspeedState;

    @QueryIgnore
    @JsonIgnore
    public boolean getOverspeedState() {
        return overspeedState;
    }

    @JsonIgnore
    public void setOverspeedState(boolean overspeedState) {
        this.overspeedState = overspeedState;
    }

    private Date overspeedTime;

    @QueryIgnore
    @JsonIgnore
    public Date getOverspeedTime() {
        return overspeedTime;
    }

    @JsonIgnore
    public void setOverspeedTime(Date overspeedTime) {
        this.overspeedTime = overspeedTime;
    }

    private long overspeedGeofenceId;

    @QueryIgnore
    @JsonIgnore
    public long getOverspeedGeofenceId() {
        return overspeedGeofenceId;
    }

    @JsonIgnore
    public void setOverspeedGeofenceId(long overspeedGeofenceId) {
        this.overspeedGeofenceId = overspeedGeofenceId;
    }

    // Distributed tracing context support
    private String traceId;
    private String spanId;
    private byte traceFlags;
    private String traceState;

    @QueryIgnore
    @JsonProperty
    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    @QueryIgnore
    @JsonProperty
    public String getSpanId() {
        return spanId;
    }

    public void setSpanId(String spanId) {
        this.spanId = spanId;
    }

    @QueryIgnore
    @JsonProperty
    public byte getTraceFlags() {
        return traceFlags;
    }

    public void setTraceFlags(byte traceFlags) {
        this.traceFlags = traceFlags;
    }

    @QueryIgnore
    @JsonProperty
    public String getTraceState() {
        return traceState;
    }

    public void setTraceState(String traceState) {
        this.traceState = traceState;
    }

    /**
     * Sets the OpenTelemetry span context for distributed tracing.
     * 
     * @param spanContext The OpenTelemetry span context
     */
    public void setSpanContext(SpanContext spanContext) {
        if (spanContext != null) {
            this.traceId = spanContext.getTraceId();
            this.spanId = spanContext.getSpanId();
            this.traceFlags = spanContext.getTraceFlags().asByte();
            this.traceState = spanContext.getTraceState().asString();
        }
    }

    /**
     * Gets the OpenTelemetry span context for distributed tracing.
     * 
     * @return The OpenTelemetry span context or null if not set
     */
    @QueryIgnore
    @JsonIgnore
    public SpanContext getSpanContext() {
        if (traceId == null || spanId == null) {
            return null;
        }
        return SpanContext.create(
                traceId,
                spanId,
                TraceFlags.fromByte(traceFlags),
                TraceState.builder().build(traceState));
    }

    // Support for partial device updates across services
    private Map<String, Boolean> updatedFields;

    @QueryIgnore
    @JsonProperty
    public Map<String, Boolean> getUpdatedFields() {
        if (updatedFields == null) {
            updatedFields = new HashMap<>();
        }
        return updatedFields;
    }

    public void setUpdatedFields(Map<String, Boolean> updatedFields) {
        this.updatedFields = updatedFields;
    }

    /**
     * Marks a field as updated.
     * 
     * @param fieldName The name of the field that was updated
     */
    public void markFieldUpdated(String fieldName) {
        getUpdatedFields().put(fieldName, true);
    }

    /**
     * Checks if a field has been updated.
     * 
     * @param fieldName The name of the field to check
     * @return True if the field has been updated, false otherwise
     */
    public boolean isFieldUpdated(String fieldName) {
        return getUpdatedFields().containsKey(fieldName) && getUpdatedFields().get(fieldName);
    }

    /**
     * Merges partial updates from another device instance.
     * Only fields that have been marked as updated in the source device will be copied.
     * 
     * @param source The source device with partial updates
     */
    public void mergeUpdates(Device source) {
        if (source == null || source.updatedFields == null || source.updatedFields.isEmpty()) {
            return;
        }

        for (String field : source.updatedFields.keySet()) {
            if (!source.isFieldUpdated(field)) {
                continue;
            }

            switch (field) {
                case "name":
                    this.setName(source.getName());
                    break;
                case "uniqueId":
                    this.setUniqueId(source.getUniqueId());
                    break;
                case "status":
                    this.setStatus(source.getStatus());
                    break;
                case "lastUpdate":
                    this.setLastUpdate(source.getLastUpdate());
                    break;
                case "positionId":
                    this.setPositionId(source.getPositionId());
                    break;
                case "phone":
                    this.setPhone(source.getPhone());
                    break;
                case "model":
                    this.setModel(source.getModel());
                    break;
                case "contact":
                    this.setContact(source.getContact());
                    break;
                case "category":
                    this.setCategory(source.getCategory());
                    break;
                case "disabled":
                    this.setDisabled(source.getDisabled());
                    break;
                case "expirationTime":
                    this.setExpirationTime(source.getExpirationTime());
                    break;
                case "calendarId":
                    this.setCalendarId(source.getCalendarId());
                    break;
                default:
                    // Unknown field, ignore
                    break;
            }

            // Mark the field as updated in this device as well
            this.markFieldUpdated(field);
        }
    }
}