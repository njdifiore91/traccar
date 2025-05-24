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

import java.util.HashMap;
import java.util.Map;

/**
 * Defines the message schema for position data exchanged between microservices in the Traccar system.
 * Includes fields for position information, device metadata, and processing context.
 */
public class PositionMessage {

    private long id;
    private long deviceId;
    private String protocol;
    private String deviceTime;
    private String serverTime;
    private String fixTime;
    private boolean valid;
    private double latitude;
    private double longitude;
    private Double altitude;
    private Double speed;
    private Double course;
    private String address;
    private Double accuracy;
    private String network;
    private Map<String, Object> attributes = new HashMap<>();
    private String correlationId;

    /**
     * Gets the position ID.
     *
     * @return The position ID
     */
    public long getId() {
        return id;
    }

    /**
     * Sets the position ID.
     *
     * @param id The position ID
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * Gets the device ID.
     *
     * @return The device ID
     */
    public long getDeviceId() {
        return deviceId;
    }

    /**
     * Sets the device ID.
     *
     * @param deviceId The device ID
     */
    public void setDeviceId(long deviceId) {
        this.deviceId = deviceId;
    }

    /**
     * Gets the protocol name.
     *
     * @return The protocol name
     */
    public String getProtocol() {
        return protocol;
    }

    /**
     * Sets the protocol name.
     *
     * @param protocol The protocol name
     */
    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    /**
     * Gets the device time.
     *
     * @return The device time
     */
    public String getDeviceTime() {
        return deviceTime;
    }

    /**
     * Sets the device time.
     *
     * @param deviceTime The device time
     */
    public void setDeviceTime(String deviceTime) {
        this.deviceTime = deviceTime;
    }

    /**
     * Gets the server time.
     *
     * @return The server time
     */
    public String getServerTime() {
        return serverTime;
    }

    /**
     * Sets the server time.
     *
     * @param serverTime The server time
     */
    public void setServerTime(String serverTime) {
        this.serverTime = serverTime;
    }

    /**
     * Gets the fix time.
     *
     * @return The fix time
     */
    public String getFixTime() {
        return fixTime;
    }

    /**
     * Sets the fix time.
     *
     * @param fixTime The fix time
     */
    public void setFixTime(String fixTime) {
        this.fixTime = fixTime;
    }

    /**
     * Checks if the position is valid.
     *
     * @return true if the position is valid, false otherwise
     */
    public boolean isValid() {
        return valid;
    }

    /**
     * Sets whether the position is valid.
     *
     * @param valid true if the position is valid, false otherwise
     */
    public void setValid(boolean valid) {
        this.valid = valid;
    }

    /**
     * Gets the latitude.
     *
     * @return The latitude
     */
    public double getLatitude() {
        return latitude;
    }

    /**
     * Sets the latitude.
     *
     * @param latitude The latitude
     */
    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    /**
     * Gets the longitude.
     *
     * @return The longitude
     */
    public double getLongitude() {
        return longitude;
    }

    /**
     * Sets the longitude.
     *
     * @param longitude The longitude
     */
    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    /**
     * Gets the altitude.
     *
     * @return The altitude
     */
    public Double getAltitude() {
        return altitude;
    }

    /**
     * Sets the altitude.
     *
     * @param altitude The altitude
     */
    public void setAltitude(Double altitude) {
        this.altitude = altitude;
    }

    /**
     * Gets the speed.
     *
     * @return The speed
     */
    public Double getSpeed() {
        return speed;
    }

    /**
     * Sets the speed.
     *
     * @param speed The speed
     */
    public void setSpeed(Double speed) {
        this.speed = speed;
    }

    /**
     * Gets the course.
     *
     * @return The course
     */
    public Double getCourse() {
        return course;
    }

    /**
     * Sets the course.
     *
     * @param course The course
     */
    public void setCourse(Double course) {
        this.course = course;
    }

    /**
     * Gets the address.
     *
     * @return The address
     */
    public String getAddress() {
        return address;
    }

    /**
     * Sets the address.
     *
     * @param address The address
     */
    public void setAddress(String address) {
        this.address = address;
    }

    /**
     * Gets the accuracy.
     *
     * @return The accuracy
     */
    public Double getAccuracy() {
        return accuracy;
    }

    /**
     * Sets the accuracy.
     *
     * @param accuracy The accuracy
     */
    public void setAccuracy(Double accuracy) {
        this.accuracy = accuracy;
    }

    /**
     * Gets the network information.
     *
     * @return The network information
     */
    public String getNetwork() {
        return network;
    }

    /**
     * Sets the network information.
     *
     * @param network The network information
     */
    public void setNetwork(String network) {
        this.network = network;
    }

    /**
     * Gets the attributes map.
     *
     * @return The attributes map
     */
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    /**
     * Sets the attributes map.
     *
     * @param attributes The attributes map
     */
    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    /**
     * Gets an attribute value.
     *
     * @param key The attribute key
     * @return The attribute value
     */
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    /**
     * Sets an attribute value.
     *
     * @param key The attribute key
     * @param value The attribute value
     */
    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    /**
     * Gets the correlation ID for distributed tracing.
     *
     * @return The correlation ID
     */
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * Sets the correlation ID for distributed tracing.
     *
     * @param correlationId The correlation ID
     */
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }
}