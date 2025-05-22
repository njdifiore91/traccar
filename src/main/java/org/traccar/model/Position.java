/*
 * Copyright 2012 - 2024 Anton Tananaev (anton@traccar.org)
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

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.traccar.storage.QueryIgnore;
import org.traccar.storage.StorageName;

// Message broker serialization annotations
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

// Bean validation annotations
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;

// OpenTelemetry annotations for distributed tracing
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.trace.SpanContext;

/**
 * Position entity representing a device location at a specific point in time.
 * Enhanced for microservices architecture with message broker serialization,
 * distributed tracing support, and validation annotations.
 */
@StorageName("tc_positions")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "_type")
@JsonTypeName("position")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Position extends Message {

    public static final String KEY_ORIGINAL = "raw";
    public static final String KEY_INDEX = "index";
    public static final String KEY_HDOP = "hdop";
    public static final String KEY_VDOP = "vdop";
    public static final String KEY_PDOP = "pdop";
    public static final String KEY_SATELLITES = "sat"; // in use
    public static final String KEY_SATELLITES_VISIBLE = "satVisible";
    public static final String KEY_RSSI = "rssi";
    public static final String KEY_GPS = "gps";
    public static final String KEY_ROAMING = "roaming";
    public static final String KEY_EVENT = "event";
    public static final String KEY_ALARM = "alarm";
    public static final String KEY_STATUS = "status";
    public static final String KEY_ODOMETER = "odometer"; // meters
    public static final String KEY_ODOMETER_SERVICE = "serviceOdometer"; // meters
    public static final String KEY_ODOMETER_TRIP = "tripOdometer"; // meters
    public static final String KEY_HOURS = "hours"; // milliseconds
    public static final String KEY_STEPS = "steps";
    public static final String KEY_HEART_RATE = "heartRate";
    public static final String KEY_INPUT = "input";
    public static final String KEY_OUTPUT = "output";
    public static final String KEY_IMAGE = "image";
    public static final String KEY_VIDEO = "video";
    public static final String KEY_AUDIO = "audio";

    // The units for the below four KEYs currently vary.
    // The preferred units of measure are specified in the comment for each.
    public static final String KEY_POWER = "power"; // volts
    public static final String KEY_BATTERY = "battery"; // volts
    public static final String KEY_BATTERY_LEVEL = "batteryLevel"; // percentage
    public static final String KEY_FUEL_LEVEL = "fuel"; // liters
    public static final String KEY_FUEL_USED = "fuelUsed"; // liters
    public static final String KEY_FUEL_CONSUMPTION = "fuelConsumption"; // liters/hour

    public static final String KEY_VERSION_FW = "versionFw";
    public static final String KEY_VERSION_HW = "versionHw";
    public static final String KEY_TYPE = "type";
    public static final String KEY_IGNITION = "ignition";
    public static final String KEY_FLAGS = "flags";
    public static final String KEY_ANTENNA = "antenna";
    public static final String KEY_CHARGE = "charge";
    public static final String KEY_IP = "ip";
    public static final String KEY_ARCHIVE = "archive";
    public static final String KEY_DISTANCE = "distance"; // meters
    public static final String KEY_TOTAL_DISTANCE = "totalDistance"; // meters
    public static final String KEY_RPM = "rpm";
    public static final String KEY_VIN = "vin";
    public static final String KEY_APPROXIMATE = "approximate";
    public static final String KEY_THROTTLE = "throttle";
    public static final String KEY_MOTION = "motion";
    public static final String KEY_ARMED = "armed";
    public static final String KEY_GEOFENCE = "geofence";
    public static final String KEY_ACCELERATION = "acceleration";
    public static final String KEY_HUMIDITY = "humidity";
    public static final String KEY_DEVICE_TEMP = "deviceTemp"; // celsius
    public static final String KEY_COOLANT_TEMP = "coolantTemp"; // celsius
    public static final String KEY_ENGINE_LOAD = "engineLoad";
    public static final String KEY_ENGINE_TEMP = "engineTemp";
    public static final String KEY_OPERATOR = "operator";
    public static final String KEY_COMMAND = "command";
    public static final String KEY_BLOCKED = "blocked";
    public static final String KEY_LOCK = "lock";
    public static final String KEY_DOOR = "door";
    public static final String KEY_AXLE_WEIGHT = "axleWeight";
    public static final String KEY_G_SENSOR = "gSensor";
    public static final String KEY_ICCID = "iccid";
    public static final String KEY_PHONE = "phone";
    public static final String KEY_SPEED_LIMIT = "speedLimit";
    public static final String KEY_DRIVING_TIME = "drivingTime";

    public static final String KEY_DTCS = "dtcs";
    public static final String KEY_OBD_SPEED = "obdSpeed"; // km/h
    public static final String KEY_OBD_ODOMETER = "obdOdometer"; // meters

    public static final String KEY_RESULT = "result";

    public static final String KEY_DRIVER_UNIQUE_ID = "driverUniqueId";
    public static final String KEY_CARD = "card";

    // Start with 1 not 0
    public static final String PREFIX_TEMP = "temp";
    public static final String PREFIX_ADC = "adc";
    public static final String PREFIX_IO = "io";
    public static final String PREFIX_COUNT = "count";
    public static final String PREFIX_IN = "in";
    public static final String PREFIX_OUT = "out";

    public static final String ALARM_GENERAL = "general";
    public static final String ALARM_SOS = "sos";
    public static final String ALARM_VIBRATION = "vibration";
    public static final String ALARM_MOVEMENT = "movement";
    public static final String ALARM_LOW_SPEED = "lowspeed";
    public static final String ALARM_OVERSPEED = "overspeed";
    public static final String ALARM_FALL_DOWN = "fallDown";
    public static final String ALARM_LOW_POWER = "lowPower";
    public static final String ALARM_LOW_BATTERY = "lowBattery";
    public static final String ALARM_FAULT = "fault";
    public static final String ALARM_POWER_OFF = "powerOff";
    public static final String ALARM_POWER_ON = "powerOn";
    public static final String ALARM_DOOR = "door";
    public static final String ALARM_LOCK = "lock";
    public static final String ALARM_UNLOCK = "unlock";
    public static final String ALARM_GEOFENCE = "geofence";
    public static final String ALARM_GEOFENCE_ENTER = "geofenceEnter";
    public static final String ALARM_GEOFENCE_EXIT = "geofenceExit";
    public static final String ALARM_GPS_ANTENNA_CUT = "gpsAntennaCut";
    public static final String ALARM_ACCIDENT = "accident";
    public static final String ALARM_TOW = "tow";
    public static final String ALARM_IDLE = "idle";
    public static final String ALARM_HIGH_RPM = "highRpm";
    public static final String ALARM_ACCELERATION = "hardAcceleration";
    public static final String ALARM_BRAKING = "hardBraking";
    public static final String ALARM_CORNERING = "hardCornering";
    public static final String ALARM_LANE_CHANGE = "laneChange";
    public static final String ALARM_FATIGUE_DRIVING = "fatigueDriving";
    public static final String ALARM_POWER_CUT = "powerCut";
    public static final String ALARM_POWER_RESTORED = "powerRestored";
    public static final String ALARM_JAMMING = "jamming";
    public static final String ALARM_TEMPERATURE = "temperature";
    public static final String ALARM_PARKING = "parking";
    public static final String ALARM_BONNET = "bonnet";
    public static final String ALARM_FOOT_BRAKE = "footBrake";
    public static final String ALARM_FUEL_LEAK = "fuelLeak";
    public static final String ALARM_TAMPERING = "tampering";
    public static final String ALARM_REMOVING = "removing";

    // Fields for distributed tracing context
    private String traceId;
    private String spanId;
    private String traceState;
    private String baggageItems;
    
    // Field for optimistic locking in distributed systems
    private long version;
    
    // Field to track modified attributes for partial updates
    private Set<String> modifiedFields = new HashSet<>();

    public Position() {
    }

    public Position(String protocol) {
        this.protocol = protocol;
    }

    private String protocol;

    @NotNull(message = "Protocol cannot be null")
    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
        modifiedFields.add("protocol");
    }

    private Date serverTime = new Date();

    @NotNull(message = "Server time cannot be null")
    @PastOrPresent(message = "Server time must be in the past or present")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    public Date getServerTime() {
        return serverTime;
    }

    public void setServerTime(Date serverTime) {
        this.serverTime = serverTime;
        modifiedFields.add("serverTime");
    }

    private Date deviceTime;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    public Date getDeviceTime() {
        return deviceTime;
    }

    public void setDeviceTime(Date deviceTime) {
        this.deviceTime = deviceTime;
        modifiedFields.add("deviceTime");
    }

    private Date fixTime;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    public Date getFixTime() {
        return fixTime;
    }

    public void setFixTime(Date fixTime) {
        this.fixTime = fixTime;
        modifiedFields.add("fixTime");
    }

    @QueryIgnore
    public void setTime(Date time) {
        setDeviceTime(time);
        setFixTime(time);
    }

    private boolean outdated;

    @QueryIgnore
    public boolean getOutdated() {
        return outdated;
    }

    @QueryIgnore
    public void setOutdated(boolean outdated) {
        this.outdated = outdated;
        modifiedFields.add("outdated");
    }

    private boolean valid;

    public boolean getValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
        modifiedFields.add("valid");
    }

    private double latitude;

    @Min(value = -90, message = "Latitude must be greater than or equal to -90")
    @Max(value = 90, message = "Latitude must be less than or equal to 90")
    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        if (latitude < -90 || latitude > 90) {
            throw new IllegalArgumentException("Latitude out of range");
        }
        this.latitude = latitude;
        modifiedFields.add("latitude");
    }

    private double longitude;

    @Min(value = -180, message = "Longitude must be greater than or equal to -180")
    @Max(value = 180, message = "Longitude must be less than or equal to 180")
    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        if (longitude < -180 || longitude > 180) {
            throw new IllegalArgumentException("Longitude out of range");
        }
        this.longitude = longitude;
        modifiedFields.add("longitude");
    }

    private double altitude; // value in meters

    public double getAltitude() {
        return altitude;
    }

    public void setAltitude(double altitude) {
        this.altitude = altitude;
        modifiedFields.add("altitude");
    }

    private double speed; // value in knots

    @Min(value = 0, message = "Speed cannot be negative")
    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
        modifiedFields.add("speed");
    }

    private double course;

    @Min(value = 0, message = "Course must be greater than or equal to 0")
    @Max(value = 360, message = "Course must be less than or equal to 360")
    public double getCourse() {
        return course;
    }

    public void setCourse(double course) {
        this.course = course;
        modifiedFields.add("course");
    }

    private String address;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
        modifiedFields.add("address");
    }

    private double accuracy;

    @Min(value = 0, message = "Accuracy cannot be negative")
    public double getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(double accuracy) {
        this.accuracy = accuracy;
        modifiedFields.add("accuracy");
    }

    private Network network;

    public Network getNetwork() {
        return network;
    }

    public void setNetwork(Network network) {
        this.network = network;
        modifiedFields.add("network");
    }

    private List<Long> geofenceIds;

    public List<Long> getGeofenceIds() {
        return geofenceIds;
    }

    public void setGeofenceIds(List<? extends Number> geofenceIds) {
        if (geofenceIds != null) {
            this.geofenceIds = geofenceIds.stream().map(Number::longValue).collect(Collectors.toList());
        } else {
            this.geofenceIds = null;
        }
        modifiedFields.add("geofenceIds");
    }

    public void addAlarm(String alarm) {
        if (alarm != null) {
            if (hasAttribute(KEY_ALARM)) {
                set(KEY_ALARM, getAttributes().get(KEY_ALARM) + "," + alarm);
            } else {
                set(KEY_ALARM, alarm);
            }
            modifiedFields.add("attributes");
        }
    }

    @JsonIgnore
    @QueryIgnore
    @Override
    public String getType() {
        return super.getType();
    }

    @JsonIgnore
    @QueryIgnore
    @Override
    public void setType(String type) {
        super.setType(type);
        modifiedFields.add("type");
    }
    
    // Distributed tracing context methods
    
    /**
     * Gets the trace ID for distributed tracing.
     * @return the trace ID
     */
    public String getTraceId() {
        return traceId;
    }

    /**
     * Sets the trace ID for distributed tracing.
     * @param traceId the trace ID to set
     */
    public void setTraceId(String traceId) {
        this.traceId = traceId;
        modifiedFields.add("traceId");
    }

    /**
     * Gets the span ID for distributed tracing.
     * @return the span ID
     */
    public String getSpanId() {
        return spanId;
    }

    /**
     * Sets the span ID for distributed tracing.
     * @param spanId the span ID to set
     */
    public void setSpanId(String spanId) {
        this.spanId = spanId;
        modifiedFields.add("spanId");
    }

    /**
     * Gets the trace state for distributed tracing.
     * @return the trace state
     */
    public String getTraceState() {
        return traceState;
    }

    /**
     * Sets the trace state for distributed tracing.
     * @param traceState the trace state to set
     */
    public void setTraceState(String traceState) {
        this.traceState = traceState;
        modifiedFields.add("traceState");
    }

    /**
     * Gets the baggage items for distributed tracing.
     * @return the baggage items as a string
     */
    public String getBaggageItems() {
        return baggageItems;
    }

    /**
     * Sets the baggage items for distributed tracing.
     * @param baggageItems the baggage items to set
     */
    public void setBaggageItems(String baggageItems) {
        this.baggageItems = baggageItems;
        modifiedFields.add("baggageItems");
    }
    
    /**
     * Sets the OpenTelemetry span context for distributed tracing.
     * @param spanContext the span context to set
     */
    @JsonIgnore
    @QueryIgnore
    public void setSpanContext(SpanContext spanContext) {
        if (spanContext != null) {
            this.traceId = spanContext.getTraceId();
            this.spanId = spanContext.getSpanId();
            this.traceState = spanContext.getTraceState().asString();
            modifiedFields.add("traceId");
            modifiedFields.add("spanId");
            modifiedFields.add("traceState");
        }
    }
    
    /**
     * Sets the OpenTelemetry baggage for distributed tracing.
     * @param baggage the baggage to set
     */
    @JsonIgnore
    @QueryIgnore
    public void setBaggage(Baggage baggage) {
        if (baggage != null) {
            this.baggageItems = baggage.asString();
            modifiedFields.add("baggageItems");
        }
    }
    
    // Version for optimistic locking
    
    /**
     * Gets the version for optimistic locking.
     * @return the version
     */
    public long getVersion() {
        return version;
    }

    /**
     * Sets the version for optimistic locking.
     * @param version the version to set
     */
    public void setVersion(long version) {
        this.version = version;
        modifiedFields.add("version");
    }
    
    // Methods for partial updates
    
    /**
     * Gets the set of modified fields for partial updates.
     * @return the set of modified field names
     */
    @JsonIgnore
    @QueryIgnore
    public Set<String> getModifiedFields() {
        return modifiedFields;
    }

    /**
     * Clears the set of modified fields.
     */
    @JsonIgnore
    @QueryIgnore
    public void clearModifiedFields() {
        modifiedFields.clear();
    }
    
    /**
     * Merges partial updates from another Position object.
     * Only fields that have been modified in the source object will be updated.
     * @param source the source Position object with partial updates
     */
    @JsonIgnore
    @QueryIgnore
    public void mergeFrom(Position source) {
        if (source == null) {
            return;
        }
        
        if (source.getModifiedFields().contains("protocol")) {
            setProtocol(source.getProtocol());
        }
        
        if (source.getModifiedFields().contains("serverTime")) {
            setServerTime(source.getServerTime());
        }
        
        if (source.getModifiedFields().contains("deviceTime")) {
            setDeviceTime(source.getDeviceTime());
        }
        
        if (source.getModifiedFields().contains("fixTime")) {
            setFixTime(source.getFixTime());
        }
        
        if (source.getModifiedFields().contains("outdated")) {
            setOutdated(source.getOutdated());
        }
        
        if (source.getModifiedFields().contains("valid")) {
            setValid(source.getValid());
        }
        
        if (source.getModifiedFields().contains("latitude")) {
            setLatitude(source.getLatitude());
        }
        
        if (source.getModifiedFields().contains("longitude")) {
            setLongitude(source.getLongitude());
        }
        
        if (source.getModifiedFields().contains("altitude")) {
            setAltitude(source.getAltitude());
        }
        
        if (source.getModifiedFields().contains("speed")) {
            setSpeed(source.getSpeed());
        }
        
        if (source.getModifiedFields().contains("course")) {
            setCourse(source.getCourse());
        }
        
        if (source.getModifiedFields().contains("address")) {
            setAddress(source.getAddress());
        }
        
        if (source.getModifiedFields().contains("accuracy")) {
            setAccuracy(source.getAccuracy());
        }
        
        if (source.getModifiedFields().contains("network")) {
            setNetwork(source.getNetwork());
        }
        
        if (source.getModifiedFields().contains("geofenceIds")) {
            setGeofenceIds(source.getGeofenceIds());
        }
        
        if (source.getModifiedFields().contains("attributes")) {
            setAttributes(source.getAttributes());
        }
        
        if (source.getModifiedFields().contains("traceId")) {
            setTraceId(source.getTraceId());
        }
        
        if (source.getModifiedFields().contains("spanId")) {
            setSpanId(source.getSpanId());
        }
        
        if (source.getModifiedFields().contains("traceState")) {
            setTraceState(source.getTraceState());
        }
        
        if (source.getModifiedFields().contains("baggageItems")) {
            setBaggageItems(source.getBaggageItems());
        }
        
        // Increment version when merging
        setVersion(getVersion() + 1);
    }
}