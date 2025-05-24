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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Message schema for position data exchanged between microservices in the Traccar system.
 * This class defines the structure for position information, device metadata, and processing context
 * to be serialized and deserialized across service boundaries.
 */
public class PositionMessage {

    // Position identification
    private long id;
    private String protocol;
    
    // Device identification
    private long deviceId;
    private String deviceIdentifier; // IMEI, serial number, etc.
    
    // Timestamps
    private Date serverTime;
    private Date deviceTime;
    private Date fixTime;
    
    // Position data
    private boolean valid;
    private double latitude;
    private double longitude;
    private double altitude;
    private double speed;
    private double course;
    private String address;
    private double accuracy;
    
    // Network information
    private NetworkInfo network;
    
    // Geofence information
    private List<Long> geofenceIds;
    
    // Additional attributes
    private Map<String, Object> attributes = new HashMap<>();
    
    // Processing context for distributed tracing and correlation
    private String correlationId;
    private String messageId;
    private long timestamp;
    private String sourceService;
    private int schemaVersion = 1;
    
    /**
     * Default constructor for serialization frameworks
     */
    public PositionMessage() {
    }
    
    /**
     * Constructor with protocol information
     * 
     * @param protocol The protocol used to receive this position
     */
    public PositionMessage(String protocol) {
        this.protocol = protocol;
        this.serverTime = new Date();
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Get position ID
     * 
     * @return Position ID
     */
    public long getId() {
        return id;
    }

    /**
     * Set position ID
     * 
     * @param id Position ID
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * Get protocol name
     * 
     * @return Protocol name
     */
    public String getProtocol() {
        return protocol;
    }

    /**
     * Set protocol name
     * 
     * @param protocol Protocol name
     */
    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    /**
     * Get device ID
     * 
     * @return Device ID
     */
    public long getDeviceId() {
        return deviceId;
    }

    /**
     * Set device ID
     * 
     * @param deviceId Device ID
     */
    public void setDeviceId(long deviceId) {
        this.deviceId = deviceId;
    }

    /**
     * Get device identifier (IMEI, serial number, etc.)
     * 
     * @return Device identifier
     */
    public String getDeviceIdentifier() {
        return deviceIdentifier;
    }

    /**
     * Set device identifier
     * 
     * @param deviceIdentifier Device identifier
     */
    public void setDeviceIdentifier(String deviceIdentifier) {
        this.deviceIdentifier = deviceIdentifier;
    }

    /**
     * Get server time
     * 
     * @return Server time
     */
    public Date getServerTime() {
        return serverTime;
    }

    /**
     * Set server time
     * 
     * @param serverTime Server time
     */
    public void setServerTime(Date serverTime) {
        this.serverTime = serverTime;
    }

    /**
     * Get device time
     * 
     * @return Device time
     */
    public Date getDeviceTime() {
        return deviceTime;
    }

    /**
     * Set device time
     * 
     * @param deviceTime Device time
     */
    public void setDeviceTime(Date deviceTime) {
        this.deviceTime = deviceTime;
    }

    /**
     * Get fix time
     * 
     * @return Fix time
     */
    public Date getFixTime() {
        return fixTime;
    }

    /**
     * Set fix time
     * 
     * @param fixTime Fix time
     */
    public void setFixTime(Date fixTime) {
        this.fixTime = fixTime;
    }

    /**
     * Set time for both device time and fix time
     * 
     * @param time Time to set
     */
    public void setTime(Date time) {
        setDeviceTime(time);
        setFixTime(time);
    }

    /**
     * Get position validity status
     * 
     * @return Position validity status
     */
    public boolean isValid() {
        return valid;
    }

    /**
     * Set position validity status
     * 
     * @param valid Position validity status
     */
    public void setValid(boolean valid) {
        this.valid = valid;
    }

    /**
     * Get latitude
     * 
     * @return Latitude in degrees
     */
    public double getLatitude() {
        return latitude;
    }

    /**
     * Set latitude
     * 
     * @param latitude Latitude in degrees
     * @throws IllegalArgumentException If latitude is out of range (-90 to 90)
     */
    public void setLatitude(double latitude) {
        if (latitude < -90 || latitude > 90) {
            throw new IllegalArgumentException("Latitude out of range");
        }
        this.latitude = latitude;
    }

    /**
     * Get longitude
     * 
     * @return Longitude in degrees
     */
    public double getLongitude() {
        return longitude;
    }

    /**
     * Set longitude
     * 
     * @param longitude Longitude in degrees
     * @throws IllegalArgumentException If longitude is out of range (-180 to 180)
     */
    public void setLongitude(double longitude) {
        if (longitude < -180 || longitude > 180) {
            throw new IllegalArgumentException("Longitude out of range");
        }
        this.longitude = longitude;
    }

    /**
     * Get altitude
     * 
     * @return Altitude in meters
     */
    public double getAltitude() {
        return altitude;
    }

    /**
     * Set altitude
     * 
     * @param altitude Altitude in meters
     */
    public void setAltitude(double altitude) {
        this.altitude = altitude;
    }

    /**
     * Get speed
     * 
     * @return Speed in knots
     */
    public double getSpeed() {
        return speed;
    }

    /**
     * Set speed
     * 
     * @param speed Speed in knots
     */
    public void setSpeed(double speed) {
        this.speed = speed;
    }

    /**
     * Get course
     * 
     * @return Course in degrees
     */
    public double getCourse() {
        return course;
    }

    /**
     * Set course
     * 
     * @param course Course in degrees
     */
    public void setCourse(double course) {
        this.course = course;
    }

    /**
     * Get address
     * 
     * @return Address as string
     */
    public String getAddress() {
        return address;
    }

    /**
     * Set address
     * 
     * @param address Address as string
     */
    public void setAddress(String address) {
        this.address = address;
    }

    /**
     * Get accuracy
     * 
     * @return Accuracy in meters
     */
    public double getAccuracy() {
        return accuracy;
    }

    /**
     * Set accuracy
     * 
     * @param accuracy Accuracy in meters
     */
    public void setAccuracy(double accuracy) {
        this.accuracy = accuracy;
    }

    /**
     * Get network information
     * 
     * @return Network information
     */
    public NetworkInfo getNetwork() {
        return network;
    }

    /**
     * Set network information
     * 
     * @param network Network information
     */
    public void setNetwork(NetworkInfo network) {
        this.network = network;
    }

    /**
     * Get geofence IDs
     * 
     * @return List of geofence IDs
     */
    public List<Long> getGeofenceIds() {
        return geofenceIds;
    }

    /**
     * Set geofence IDs
     * 
     * @param geofenceIds List of geofence IDs
     */
    public void setGeofenceIds(List<Long> geofenceIds) {
        this.geofenceIds = geofenceIds;
    }

    /**
     * Get attributes
     * 
     * @return Map of attributes
     */
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    /**
     * Set attributes
     * 
     * @param attributes Map of attributes
     */
    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    /**
     * Get attribute by key
     * 
     * @param key Attribute key
     * @return Attribute value or null if not found
     */
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    /**
     * Set attribute
     * 
     * @param key Attribute key
     * @param value Attribute value
     */
    public void setAttribute(String key, Object value) {
        if (value != null) {
            attributes.put(key, value);
        } else {
            attributes.remove(key);
        }
    }

    /**
     * Check if attribute exists
     * 
     * @param key Attribute key
     * @return True if attribute exists
     */
    public boolean hasAttribute(String key) {
        return attributes.containsKey(key);
    }

    /**
     * Get correlation ID for distributed tracing
     * 
     * @return Correlation ID
     */
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * Set correlation ID for distributed tracing
     * 
     * @param correlationId Correlation ID
     */
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    /**
     * Get message ID
     * 
     * @return Message ID
     */
    public String getMessageId() {
        return messageId;
    }

    /**
     * Set message ID
     * 
     * @param messageId Message ID
     */
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    /**
     * Get message timestamp
     * 
     * @return Message timestamp in milliseconds
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Set message timestamp
     * 
     * @param timestamp Message timestamp in milliseconds
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Get source service name
     * 
     * @return Source service name
     */
    public String getSourceService() {
        return sourceService;
    }

    /**
     * Set source service name
     * 
     * @param sourceService Source service name
     */
    public void setSourceService(String sourceService) {
        this.sourceService = sourceService;
    }

    /**
     * Get schema version
     * 
     * @return Schema version
     */
    public int getSchemaVersion() {
        return schemaVersion;
    }

    /**
     * Set schema version
     * 
     * @param schemaVersion Schema version
     */
    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    /**
     * Network information class for storing cellular and WiFi network details
     */
    public static class NetworkInfo {
        private List<CellTower> cellTowers;
        private List<WifiAccessPoint> wifiAccessPoints;

        /**
         * Get cell towers
         * 
         * @return List of cell towers
         */
        public List<CellTower> getCellTowers() {
            return cellTowers;
        }

        /**
         * Set cell towers
         * 
         * @param cellTowers List of cell towers
         */
        public void setCellTowers(List<CellTower> cellTowers) {
            this.cellTowers = cellTowers;
        }

        /**
         * Get WiFi access points
         * 
         * @return List of WiFi access points
         */
        public List<WifiAccessPoint> getWifiAccessPoints() {
            return wifiAccessPoints;
        }

        /**
         * Set WiFi access points
         * 
         * @param wifiAccessPoints List of WiFi access points
         */
        public void setWifiAccessPoints(List<WifiAccessPoint> wifiAccessPoints) {
            this.wifiAccessPoints = wifiAccessPoints;
        }
    }

    /**
     * Cell tower information for network-based positioning
     */
    public static class CellTower {
        private int mobileCountryCode;
        private int mobileNetworkCode;
        private int locationAreaCode;
        private int cellId;
        private int signalStrength;

        /**
         * Get mobile country code (MCC)
         * 
         * @return Mobile country code
         */
        public int getMobileCountryCode() {
            return mobileCountryCode;
        }

        /**
         * Set mobile country code (MCC)
         * 
         * @param mobileCountryCode Mobile country code
         */
        public void setMobileCountryCode(int mobileCountryCode) {
            this.mobileCountryCode = mobileCountryCode;
        }

        /**
         * Get mobile network code (MNC)
         * 
         * @return Mobile network code
         */
        public int getMobileNetworkCode() {
            return mobileNetworkCode;
        }

        /**
         * Set mobile network code (MNC)
         * 
         * @param mobileNetworkCode Mobile network code
         */
        public void setMobileNetworkCode(int mobileNetworkCode) {
            this.mobileNetworkCode = mobileNetworkCode;
        }

        /**
         * Get location area code (LAC)
         * 
         * @return Location area code
         */
        public int getLocationAreaCode() {
            return locationAreaCode;
        }

        /**
         * Set location area code (LAC)
         * 
         * @param locationAreaCode Location area code
         */
        public void setLocationAreaCode(int locationAreaCode) {
            this.locationAreaCode = locationAreaCode;
        }

        /**
         * Get cell ID
         * 
         * @return Cell ID
         */
        public int getCellId() {
            return cellId;
        }

        /**
         * Set cell ID
         * 
         * @param cellId Cell ID
         */
        public void setCellId(int cellId) {
            this.cellId = cellId;
        }

        /**
         * Get signal strength
         * 
         * @return Signal strength in dBm
         */
        public int getSignalStrength() {
            return signalStrength;
        }

        /**
         * Set signal strength
         * 
         * @param signalStrength Signal strength in dBm
         */
        public void setSignalStrength(int signalStrength) {
            this.signalStrength = signalStrength;
        }
    }

    /**
     * WiFi access point information for network-based positioning
     */
    public static class WifiAccessPoint {
        private String macAddress;
        private int signalStrength;

        /**
         * Get MAC address
         * 
         * @return MAC address
         */
        public String getMacAddress() {
            return macAddress;
        }

        /**
         * Set MAC address
         * 
         * @param macAddress MAC address
         */
        public void setMacAddress(String macAddress) {
            this.macAddress = macAddress;
        }

        /**
         * Get signal strength
         * 
         * @return Signal strength in dBm
         */
        public int getSignalStrength() {
            return signalStrength;
        }

        /**
         * Set signal strength
         * 
         * @param signalStrength Signal strength in dBm
         */
        public void setSignalStrength(int signalStrength) {
            this.signalStrength = signalStrength;
        }
    }
}