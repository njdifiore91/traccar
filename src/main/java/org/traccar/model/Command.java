/*
 * Copyright 2015 - 2022 Anton Tananaev (anton@traccar.org)
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

import org.traccar.storage.QueryIgnore;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.traccar.storage.StorageName;

import java.time.Instant;

/**
 * Command model representing device commands that can be executed across services.
 * This class has been enhanced to support message broker serialization and distributed tracing.
 */
@StorageName("tc_commands")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Command extends BaseCommand {

    // Command types
    public static final String TYPE_CUSTOM = "custom";
    public static final String TYPE_IDENTIFICATION = "deviceIdentification";
    public static final String TYPE_POSITION_SINGLE = "positionSingle";
    public static final String TYPE_POSITION_PERIODIC = "positionPeriodic";
    public static final String TYPE_POSITION_STOP = "positionStop";
    public static final String TYPE_ENGINE_STOP = "engineStop";
    public static final String TYPE_ENGINE_RESUME = "engineResume";
    public static final String TYPE_ALARM_ARM = "alarmArm";
    public static final String TYPE_ALARM_DISARM = "alarmDisarm";
    public static final String TYPE_ALARM_DISMISS = "alarmDismiss";
    public static final String TYPE_SET_TIMEZONE = "setTimezone";
    public static final String TYPE_REQUEST_PHOTO = "requestPhoto";
    public static final String TYPE_POWER_OFF = "powerOff";
    public static final String TYPE_REBOOT_DEVICE = "rebootDevice";
    public static final String TYPE_FACTORY_RESET = "factoryReset";
    public static final String TYPE_SEND_SMS = "sendSms";
    public static final String TYPE_SEND_USSD = "sendUssd";
    public static final String TYPE_SOS_NUMBER = "sosNumber";
    public static final String TYPE_SILENCE_TIME = "silenceTime";
    public static final String TYPE_SET_PHONEBOOK = "setPhonebook";
    public static final String TYPE_MESSAGE = "message";
    public static final String TYPE_VOICE_MESSAGE = "voiceMessage";
    public static final String TYPE_OUTPUT_CONTROL = "outputControl";
    public static final String TYPE_VOICE_MONITORING = "voiceMonitoring";
    public static final String TYPE_SET_AGPS = "setAgps";
    public static final String TYPE_SET_INDICATOR = "setIndicator";
    public static final String TYPE_CONFIGURATION = "configuration";
    public static final String TYPE_GET_VERSION = "getVersion";
    public static final String TYPE_FIRMWARE_UPDATE = "firmwareUpdate";
    public static final String TYPE_SET_CONNECTION = "setConnection";
    public static final String TYPE_SET_ODOMETER = "setOdometer";
    public static final String TYPE_GET_MODEM_STATUS = "getModemStatus";
    public static final String TYPE_GET_DEVICE_STATUS = "getDeviceStatus";
    public static final String TYPE_SET_SPEED_LIMIT = "setSpeedLimit";
    public static final String TYPE_MODE_POWER_SAVING = "modePowerSaving";
    public static final String TYPE_MODE_DEEP_SLEEP = "modeDeepSleep";

    public static final String TYPE_ALARM_GEOFENCE = "alarmGeofence";
    public static final String TYPE_ALARM_BATTERY = "alarmBattery";
    public static final String TYPE_ALARM_SOS = "alarmSos";
    public static final String TYPE_ALARM_REMOVE = "alarmRemove";
    public static final String TYPE_ALARM_CLOCK = "alarmClock";
    public static final String TYPE_ALARM_SPEED = "alarmSpeed";
    public static final String TYPE_ALARM_FALL = "alarmFall";
    public static final String TYPE_ALARM_VIBRATION = "alarmVibration";

    // Command attribute keys
    public static final String KEY_UNIQUE_ID = "uniqueId";
    public static final String KEY_FREQUENCY = "frequency";
    public static final String KEY_LANGUAGE = "language";
    public static final String KEY_TIMEZONE = "timezone";
    public static final String KEY_DEVICE_PASSWORD = "devicePassword";
    public static final String KEY_RADIUS = "radius";
    public static final String KEY_MESSAGE = "message";
    public static final String KEY_ENABLE = "enable";
    public static final String KEY_DATA = "data";
    public static final String KEY_INDEX = "index";
    public static final String KEY_PHONE = "phone";
    public static final String KEY_SERVER = "server";
    public static final String KEY_PORT = "port";
    public static final String KEY_NO_QUEUE = "noQueue";

    // Command status values
    public static final String STATUS_NEW = "new";
    public static final String STATUS_PENDING = "pending";
    public static final String STATUS_DELIVERED = "delivered";
    public static final String STATUS_SUCCESSFUL = "successful";
    public static final String STATUS_FAILED = "failed";
    public static final String STATUS_TIMEOUT = "timeout";

    @QueryIgnore
    @Override
    public long getDeviceId() {
        return super.getDeviceId();
    }

    @QueryIgnore
    @Override
    public void setDeviceId(long deviceId) {
        super.setDeviceId(deviceId);
    }

    private String description;

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    // Fields for distributed tracing and cross-service execution

    private String correlationId;

    /**
     * Gets the correlation ID for distributed tracing.
     * This ID is used to track the command across different services.
     *
     * @return The correlation ID
     */
    @JsonProperty
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * Sets the correlation ID for distributed tracing.
     *
     * @param correlationId The correlation ID to set
     */
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    private String parentSpanId;

    /**
     * Gets the parent span ID for OpenTelemetry tracing.
     *
     * @return The parent span ID
     */
    @JsonProperty
    public String getParentSpanId() {
        return parentSpanId;
    }

    /**
     * Sets the parent span ID for OpenTelemetry tracing.
     *
     * @param parentSpanId The parent span ID to set
     */
    public void setParentSpanId(String parentSpanId) {
        this.parentSpanId = parentSpanId;
    }

    private String sourceService;

    /**
     * Gets the source service that initiated the command.
     *
     * @return The source service name
     */
    @JsonProperty
    public String getSourceService() {
        return sourceService;
    }

    /**
     * Sets the source service that initiated the command.
     *
     * @param sourceService The source service name to set
     */
    public void setSourceService(String sourceService) {
        this.sourceService = sourceService;
    }

    private String targetService;

    /**
     * Gets the target service that should execute the command.
     *
     * @return The target service name
     */
    @JsonProperty
    public String getTargetService() {
        return targetService;
    }

    /**
     * Sets the target service that should execute the command.
     *
     * @param targetService The target service name to set
     */
    public void setTargetService(String targetService) {
        this.targetService = targetService;
    }

    // Command status tracking fields

    private String status;

    /**
     * Gets the current status of the command.
     *
     * @return The command status
     */
    @JsonProperty
    public String getStatus() {
        return status;
    }

    /**
     * Sets the current status of the command.
     *
     * @param status The command status to set
     */
    public void setStatus(String status) {
        this.status = status;
    }

    private Instant createdAt;

    /**
     * Gets the timestamp when the command was created.
     *
     * @return The creation timestamp
     */
    @JsonProperty
    public Instant getCreatedAt() {
        return createdAt;
    }

    /**
     * Sets the timestamp when the command was created.
     *
     * @param createdAt The creation timestamp to set
     */
    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    private Instant updatedAt;

    /**
     * Gets the timestamp when the command was last updated.
     *
     * @return The last update timestamp
     */
    @JsonProperty
    public Instant getUpdatedAt() {
        return updatedAt;
    }

    /**
     * Sets the timestamp when the command was last updated.
     *
     * @param updatedAt The last update timestamp to set
     */
    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    private Instant executedAt;

    /**
     * Gets the timestamp when the command was executed.
     *
     * @return The execution timestamp
     */
    @JsonProperty
    public Instant getExecutedAt() {
        return executedAt;
    }

    /**
     * Sets the timestamp when the command was executed.
     *
     * @param executedAt The execution timestamp to set
     */
    public void setExecutedAt(Instant executedAt) {
        this.executedAt = executedAt;
    }

    private String statusMessage;

    /**
     * Gets the status message with additional details about command execution.
     *
     * @return The status message
     */
    @JsonProperty
    public String getStatusMessage() {
        return statusMessage;
    }

    /**
     * Sets the status message with additional details about command execution.
     *
     * @param statusMessage The status message to set
     */
    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }
}