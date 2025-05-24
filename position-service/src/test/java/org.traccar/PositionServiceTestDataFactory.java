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
package org.traccar;

import org.traccar.model.CellTower;
import org.traccar.model.Device;
import org.traccar.model.Network;
import org.traccar.model.Position;
import org.traccar.model.WifiAccessPoint;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Test data factory for Position Service tests.
 * Provides methods to create test position data with various attributes and configurations.
 */
public class PositionServiceTestDataFactory {

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    
    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    /**
     * Creates a basic position with the specified parameters.
     *
     * @param time   ISO-8601 formatted time string ("yyyy-MM-dd HH:mm:ss.SSS")
     * @param valid  whether the position is valid
     * @param lat    latitude
     * @param lon    longitude
     * @return Position object
     * @throws ParseException if time string cannot be parsed
     */
    public Position createPosition(String time, boolean valid, double lat, double lon) throws ParseException {
        Position position = new Position();
        position.setTime(DATE_FORMAT.parse(time));
        position.setValid(valid);
        position.setLatitude(lat);
        position.setLongitude(lon);
        return position;
    }

    /**
     * Creates a position with device ID and protocol.
     *
     * @param deviceId device identifier
     * @param protocol protocol name
     * @param time     ISO-8601 formatted time string ("yyyy-MM-dd HH:mm:ss.SSS")
     * @param valid    whether the position is valid
     * @param lat      latitude
     * @param lon      longitude
     * @return Position object
     * @throws ParseException if time string cannot be parsed
     */
    public Position createPosition(long deviceId, String protocol, String time, boolean valid, double lat, double lon) throws ParseException {
        Position position = createPosition(time, valid, lat, lon);
        position.setDeviceId(deviceId);
        position.setProtocol(protocol);
        return position;
    }

    /**
     * Creates a fully enriched position with all common attributes.
     *
     * @param deviceId device identifier
     * @param protocol protocol name
     * @param time     ISO-8601 formatted time string ("yyyy-MM-dd HH:mm:ss.SSS")
     * @param valid    whether the position is valid
     * @param lat      latitude
     * @param lon      longitude
     * @return Position object with enriched attributes
     * @throws ParseException if time string cannot be parsed
     */
    public Position createEnrichedPosition(long deviceId, String protocol, String time, boolean valid, double lat, double lon) throws ParseException {
        Position position = createPosition(deviceId, protocol, time, valid, lat, lon);
        
        // Add common attributes
        position.setAltitude(150.0);
        position.setSpeed(50.0);
        position.setCourse(90.0);
        position.setAccuracy(10.0);
        position.setAddress("123 Test Street, Test City");
        
        // Add extended attributes
        position.set(Position.KEY_SATELLITES, 8);
        position.set(Position.KEY_HDOP, 1.1);
        position.set(Position.KEY_ODOMETER, 12345.0);
        position.set(Position.KEY_FUEL_LEVEL, 75.5);
        position.set(Position.KEY_BATTERY, 12.4);
        position.set(Position.KEY_BATTERY_LEVEL, 85);
        position.set(Position.KEY_IGNITION, true);
        position.set(Position.KEY_MOTION, true);
        position.set(Position.KEY_DISTANCE, 150.0);
        position.set(Position.KEY_TOTAL_DISTANCE, 15000.0);
        position.set(Position.KEY_RPM, 1800);
        
        return position;
    }

    /**
     * Creates a position with network information (cell towers and WiFi access points).
     *
     * @param deviceId device identifier
     * @param protocol protocol name
     * @param time     ISO-8601 formatted time string ("yyyy-MM-dd HH:mm:ss.SSS")
     * @param valid    whether the position is valid
     * @param lat      latitude
     * @param lon      longitude
     * @return Position object with network information
     * @throws ParseException if time string cannot be parsed
     */
    public Position createPositionWithNetwork(long deviceId, String protocol, String time, boolean valid, double lat, double lon) throws ParseException {
        Position position = createPosition(deviceId, protocol, time, valid, lat, lon);
        
        Network network = new Network();
        
        // Add cell towers
        network.addCellTower(CellTower.from(310, 410, 1001, 12345, -85));
        network.addCellTower(CellTower.from(310, 410, 1002, 23456, -90));
        
        // Add WiFi access points
        network.addWifiAccessPoint(WifiAccessPoint.from("00:11:22:33:44:55", -65, 6));
        network.addWifiAccessPoint(WifiAccessPoint.from("AA:BB:CC:DD:EE:FF", -75, 11));
        
        position.setNetwork(network);
        
        return position;
    }

    /**
     * Creates a position with alarm information.
     *
     * @param deviceId device identifier
     * @param protocol protocol name
     * @param time     ISO-8601 formatted time string ("yyyy-MM-dd HH:mm:ss.SSS")
     * @param valid    whether the position is valid
     * @param lat      latitude
     * @param lon      longitude
     * @param alarmType alarm type from Position.ALARM_* constants
     * @return Position object with alarm
     * @throws ParseException if time string cannot be parsed
     */
    public Position createPositionWithAlarm(long deviceId, String protocol, String time, boolean valid, double lat, double lon, String alarmType) throws ParseException {
        Position position = createPosition(deviceId, protocol, time, valid, lat, lon);
        position.set(Position.KEY_ALARM, alarmType);
        return position;
    }

    /**
     * Creates a sequence of positions for the same device with time progression.
     *
     * @param deviceId device identifier
     * @param protocol protocol name
     * @param startTime ISO-8601 formatted start time string ("yyyy-MM-dd HH:mm:ss.SSS")
     * @param count number of positions to generate
     * @param timeIntervalSeconds time interval between positions in seconds
     * @param startLat starting latitude
     * @param startLon starting longitude
     * @param latIncrement latitude increment per position
     * @param lonIncrement longitude increment per position
     * @return List of Position objects
     * @throws ParseException if time string cannot be parsed
     */
    public List<Position> createPositionSequence(
            long deviceId, String protocol, String startTime, int count,
            int timeIntervalSeconds, double startLat, double startLon,
            double latIncrement, double lonIncrement) throws ParseException {
        
        List<Position> positions = new ArrayList<>();
        Date time = DATE_FORMAT.parse(startTime);
        double lat = startLat;
        double lon = startLon;
        
        for (int i = 0; i < count; i++) {
            Position position = new Position();
            position.setDeviceId(deviceId);
            position.setProtocol(protocol);
            position.setTime(time);
            position.setValid(true);
            position.setLatitude(lat);
            position.setLongitude(lon);
            
            // Add some basic attributes
            position.setSpeed(50.0 + (i * 2.0)); // Gradually increasing speed
            position.setCourse(90.0);
            position.set(Position.KEY_SATELLITES, 8);
            position.set(Position.KEY_ODOMETER, 10000.0 + (i * 100.0)); // Increasing odometer
            position.set(Position.KEY_DISTANCE, i * 100.0); // Distance since last position
            position.set(Position.KEY_TOTAL_DISTANCE, 10000.0 + (i * 100.0)); // Total distance
            
            positions.add(position);
            
            // Increment time and coordinates for next position
            time = new Date(time.getTime() + (timeIntervalSeconds * 1000L));
            lat += latIncrement;
            lon += lonIncrement;
        }
        
        return positions;
    }

    /**
     * Creates test data for geofence checks with positions inside and outside a circular area.
     *
     * @param deviceId device identifier
     * @param protocol protocol name
     * @param time ISO-8601 formatted time string ("yyyy-MM-dd HH:mm:ss.SSS")
     * @param centerLat center latitude of the geofence
     * @param centerLon center longitude of the geofence
     * @param radiusDegrees radius of the geofence in degrees
     * @return List containing positions inside and outside the geofence
     * @throws ParseException if time string cannot be parsed
     */
    public List<Position> createGeofenceTestData(long deviceId, String protocol, String time, 
                                               double centerLat, double centerLon, double radiusDegrees) throws ParseException {
        List<Position> positions = new ArrayList<>();
        Date baseTime = DATE_FORMAT.parse(time);
        
        // Position at the center of the geofence
        Position center = createPosition(deviceId, protocol, DATE_FORMAT.format(baseTime), true, centerLat, centerLon);
        positions.add(center);
        
        // Position inside the geofence (half radius)
        Position inside = createPosition(deviceId, protocol, 
                DATE_FORMAT.format(new Date(baseTime.getTime() + 60000)), // 1 minute later
                true, centerLat + (radiusDegrees / 2), centerLon + (radiusDegrees / 2));
        positions.add(inside);
        
        // Position at the edge of the geofence
        Position edge = createPosition(deviceId, protocol, 
                DATE_FORMAT.format(new Date(baseTime.getTime() + 120000)), // 2 minutes later
                true, centerLat + radiusDegrees, centerLon);
        positions.add(edge);
        
        // Position outside the geofence
        Position outside = createPosition(deviceId, protocol, 
                DATE_FORMAT.format(new Date(baseTime.getTime() + 180000)), // 3 minutes later
                true, centerLat + (radiusDegrees * 2), centerLon + (radiusDegrees * 2));
        positions.add(outside);
        
        return positions;
    }

    /**
     * Creates test data for distance calculation tests.
     *
     * @param deviceId device identifier
     * @param protocol protocol name
     * @param startTime ISO-8601 formatted start time string ("yyyy-MM-dd HH:mm:ss.SSS")
     * @return List of positions with known distances between them
     * @throws ParseException if time string cannot be parsed
     */
    public List<Position> createDistanceCalculationTestData(long deviceId, String protocol, String startTime) throws ParseException {
        List<Position> positions = new ArrayList<>();
        Date time = DATE_FORMAT.parse(startTime);
        
        // Starting position
        Position start = createPosition(deviceId, protocol, DATE_FORMAT.format(time), true, 0.0, 0.0);
        start.set(Position.KEY_TOTAL_DISTANCE, 0.0);
        positions.add(start);
        
        // Move 100 meters east (approximately 0.001 degrees longitude at equator)
        time = new Date(time.getTime() + 60000); // 1 minute later
        Position pos1 = createPosition(deviceId, protocol, DATE_FORMAT.format(time), true, 0.0, 0.001);
        pos1.set(Position.KEY_DISTANCE, 100.0);
        pos1.set(Position.KEY_TOTAL_DISTANCE, 100.0);
        positions.add(pos1);
        
        // Move 100 meters north (approximately 0.001 degrees latitude)
        time = new Date(time.getTime() + 60000); // 1 minute later
        Position pos2 = createPosition(deviceId, protocol, DATE_FORMAT.format(time), true, 0.001, 0.001);
        pos2.set(Position.KEY_DISTANCE, 100.0);
        pos2.set(Position.KEY_TOTAL_DISTANCE, 200.0);
        positions.add(pos2);
        
        // Move 141.4 meters diagonally (100m east, 100m north - approximately 0.001 degrees in each direction)
        time = new Date(time.getTime() + 60000); // 1 minute later
        Position pos3 = createPosition(deviceId, protocol, DATE_FORMAT.format(time), true, 0.002, 0.002);
        pos3.set(Position.KEY_DISTANCE, 141.4);
        pos3.set(Position.KEY_TOTAL_DISTANCE, 341.4);
        positions.add(pos3);
        
        return positions;
    }

    /**
     * Creates test data for position forwarding tests.
     *
     * @param deviceId device identifier
     * @param protocol protocol name
     * @param time ISO-8601 formatted time string ("yyyy-MM-dd HH:mm:ss.SSS")
     * @return Position with forwarding-specific attributes
     * @throws ParseException if time string cannot be parsed
     */
    public Position createForwardingTestData(long deviceId, String protocol, String time) throws ParseException {
        Position position = createEnrichedPosition(deviceId, protocol, time, true, 40.7128, -74.0060); // NYC coordinates
        
        // Add device-specific information useful for forwarding
        position.set(Position.KEY_DEVICE_TEMP, 35.5);
        position.set(Position.KEY_DRIVER_UNIQUE_ID, "driver123");
        position.set(Position.KEY_VIN, "1HGCM82633A123456");
        position.set(Position.KEY_ICCID, "89014103211118510720");
        position.set(Position.KEY_PHONE, "+12125551234");
        
        // Add custom attributes that might be used in forwarding templates
        position.set("customField1", "customValue1");
        position.set("customField2", "customValue2");
        
        return position;
    }

    /**
     * Creates a position with device information for testing device-position relationships.
     *
     * @param device Device object
     * @param time ISO-8601 formatted time string ("yyyy-MM-dd HH:mm:ss.SSS")
     * @param valid whether the position is valid
     * @param lat latitude
     * @param lon longitude
     * @return Position linked to the specified device
     * @throws ParseException if time string cannot be parsed
     */
    public Position createPositionForDevice(Device device, String time, boolean valid, double lat, double lon) throws ParseException {
        Position position = createPosition(device.getId(), "test", time, valid, lat, lon);
        
        // Add device-specific information
        if (device.getUniqueId() != null) {
            position.set("deviceUniqueId", device.getUniqueId());
        }
        if (device.getName() != null) {
            position.set("deviceName", device.getName());
        }
        if (device.getPhone() != null) {
            position.set("devicePhone", device.getPhone());
        }
        
        return position;
    }
}