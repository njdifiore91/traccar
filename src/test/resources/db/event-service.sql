-- Event Service Database Initialization Script for Testing
-- This script creates the necessary tables and test data for the Event Processing Service

-- Drop tables if they exist to ensure clean state
DROP TABLE IF EXISTS tc_user_event;
DROP TABLE IF EXISTS tc_group_event;
DROP TABLE IF EXISTS tc_device_event;
DROP TABLE IF EXISTS tc_events;
DROP TABLE IF EXISTS tc_user_geofence;
DROP TABLE IF EXISTS tc_group_geofence;
DROP TABLE IF EXISTS tc_device_geofence;
DROP TABLE IF EXISTS tc_geofences;

-- Create geofences table
CREATE TABLE tc_geofences (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(512),
    area TEXT NOT NULL,
    calendarid INT,
    attributes VARCHAR(4000),
    createdtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updatedtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create events table
CREATE TABLE tc_events (
    id INT PRIMARY KEY AUTO_INCREMENT,
    type VARCHAR(128) NOT NULL,
    eventtime TIMESTAMP NOT NULL,
    deviceid INT,
    positionid INT,
    geofenceid INT,
    attributes VARCHAR(4000),
    maintenanceid INT,
    servertime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX event_deviceid_servertime (deviceid, servertime)
);

-- Create permission tables for geofences
CREATE TABLE tc_user_geofence (
    userid INT NOT NULL,
    geofenceid INT NOT NULL,
    PRIMARY KEY (userid, geofenceid)
);

CREATE TABLE tc_group_geofence (
    groupid INT NOT NULL,
    geofenceid INT NOT NULL,
    PRIMARY KEY (groupid, geofenceid)
);

CREATE TABLE tc_device_geofence (
    deviceid INT NOT NULL,
    geofenceid INT NOT NULL,
    PRIMARY KEY (deviceid, geofenceid)
);

-- Create permission tables for events
CREATE TABLE tc_user_event (
    userid INT NOT NULL,
    eventid INT NOT NULL,
    PRIMARY KEY (userid, eventid)
);

CREATE TABLE tc_group_event (
    groupid INT NOT NULL,
    eventid INT NOT NULL,
    PRIMARY KEY (groupid, eventid)
);

CREATE TABLE tc_device_event (
    deviceid INT NOT NULL,
    eventid INT NOT NULL,
    PRIMARY KEY (deviceid, eventid)
);

-- Insert test geofence data
INSERT INTO tc_geofences (id, name, description, area) VALUES 
(1, 'Office', 'Main office location', 'CIRCLE (30.274105 -97.740692, 150)'),
(2, 'Warehouse', 'Warehouse area', 'POLYGON ((30.286275 -97.735714, 30.285885 -97.731551, 30.282870 -97.731894, 30.282907 -97.736035, 30.286275 -97.735714))'),
(3, 'Restricted Zone', 'No entry area', 'CIRCLE (30.270308 -97.743159, 80)'),
(4, 'Customer Site', 'Customer location', 'CIRCLE (30.252109 -97.745920, 200)'),
(5, 'Delivery Route', 'Standard delivery path', 'LINESTRING (30.274105 -97.740692, 30.252109 -97.745920, 30.286275 -97.735714)'),
(6, 'Parking Area', 'Vehicle parking', 'POLYGON ((30.270308 -97.743159, 30.270308 -97.741159, 30.272308 -97.741159, 30.272308 -97.743159, 30.270308 -97.743159))'),
(7, 'Construction Zone', 'Temporary restricted area', 'CIRCLE (30.267541 -97.749812, 120)');

-- Insert test event data
-- Device IDs 1-5 for testing
-- Position IDs 100-150 for testing
-- Various event types to test event detection

-- Geofence events
INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, geofenceid, attributes) VALUES
(1, 'geofenceEnter', '2023-06-01 08:00:00', 1, 100, 1, '{"alarm": false}'),
(2, 'geofenceExit', '2023-06-01 17:30:00', 1, 110, 1, '{"alarm": false}'),
(3, 'geofenceEnter', '2023-06-01 09:15:00', 2, 105, 2, '{"alarm": false}'),
(4, 'geofenceExit', '2023-06-01 16:45:00', 2, 115, 2, '{"alarm": false}'),
(5, 'geofenceEnter', '2023-06-01 12:30:00', 3, 120, 3, '{"alarm": true}'),
(6, 'geofenceExit', '2023-06-01 12:35:00', 3, 121, 3, '{"alarm": false}');

-- Speed events
INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, attributes) VALUES
(7, 'deviceOverspeed', '2023-06-01 10:15:00', 1, 125, '{"speed": 75.5, "speedLimit": 60.0, "alarm": true}'),
(8, 'deviceOverspeed', '2023-06-01 14:20:00', 2, 130, '{"speed": 92.3, "speedLimit": 80.0, "alarm": true}'),
(9, 'deviceOverspeed', '2023-06-01 11:45:00', 4, 135, '{"speed": 68.7, "speedLimit": 60.0, "alarm": true}');

-- Status events
INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, attributes) VALUES
(10, 'deviceOffline', '2023-06-01 18:30:00', 1, NULL, '{"alarm": true}'),
(11, 'deviceOnline', '2023-06-01 18:45:00', 1, 140, '{"alarm": false}'),
(12, 'deviceUnknown', '2023-06-01 23:50:00', 3, NULL, '{"alarm": true}'),
(13, 'deviceMoving', '2023-06-01 08:15:00', 2, 142, '{"alarm": false}'),
(14, 'deviceStopped', '2023-06-01 12:00:00', 2, 143, '{"alarm": false}');

-- Alarm events
INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, attributes) VALUES
(15, 'alarm', '2023-06-01 02:30:00', 5, 144, '{"alarm": "sos"}'),
(16, 'alarm', '2023-06-01 13:20:00', 4, 145, '{"alarm": "power"}'),
(17, 'alarm', '2023-06-01 22:15:00', 3, 146, '{"alarm": "vibration"}'),
(18, 'alarm', '2023-06-01 16:40:00', 1, 147, '{"alarm": "lowBattery", "batteryLevel": 10}');

-- Driver behavior events
INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, attributes) VALUES
(19, 'driverBehavior', '2023-06-01 09:45:00', 2, 148, '{"behavior": "hardAcceleration", "value": 3.5, "alarm": true}'),
(20, 'driverBehavior', '2023-06-01 15:30:00', 1, 149, '{"behavior": "hardBraking", "value": 4.2, "alarm": true}'),
(21, 'driverBehavior', '2023-06-01 11:20:00', 4, 150, '{"behavior": "hardCornering", "value": 2.8, "alarm": true}');

-- Maintenance events
INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, maintenanceid, attributes) VALUES
(22, 'maintenance', '2023-06-01 07:00:00', 1, NULL, 1, '{"maintenance": "oil", "alarm": true}'),
(23, 'maintenance', '2023-06-01 14:00:00', 3, NULL, 2, '{"maintenance": "service", "alarm": true}'),
(24, 'maintenance', '2023-06-01 10:00:00', 5, NULL, 3, '{"maintenance": "tireRotation", "alarm": true}');

-- Ignition events
INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, attributes) VALUES
(25, 'ignitionOn', '2023-06-01 08:05:00', 1, 151, '{"alarm": false}'),
(26, 'ignitionOff', '2023-06-01 17:35:00', 1, 152, '{"alarm": false}'),
(27, 'ignitionOn', '2023-06-01 09:10:00', 2, 153, '{"alarm": false}'),
(28, 'ignitionOff', '2023-06-01 16:50:00', 2, 154, '{"alarm": false}');

-- Command result events
INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, attributes) VALUES
(29, 'commandResult', '2023-06-01 13:00:00', 1, NULL, '{"result": "success", "commandId": 101}'),
(30, 'commandResult', '2023-06-01 15:00:00', 3, NULL, '{"result": "failed", "commandId": 102, "reason": "Device offline"}');

-- Test permission assignments
-- Assign geofences to test users, groups, and devices
INSERT INTO tc_user_geofence (userid, geofenceid) VALUES
(1, 1), (1, 2), (1, 3),
(2, 1), (2, 4),
(3, 2), (3, 5), (3, 6);

INSERT INTO tc_group_geofence (groupid, geofenceid) VALUES
(1, 1), (1, 2),
(2, 3), (2, 4),
(3, 5), (3, 6), (3, 7);

INSERT INTO tc_device_geofence (deviceid, geofenceid) VALUES
(1, 1), (1, 3),
(2, 2), (2, 5),
(3, 3), (3, 6),
(4, 4), (4, 7),
(5, 1), (5, 7);

-- Assign events to test users, groups, and devices for permission testing
INSERT INTO tc_user_event (userid, eventid) VALUES
(1, 1), (1, 2), (1, 7), (1, 10), (1, 11),
(2, 3), (2, 4), (2, 8), (2, 13), (2, 14),
(3, 5), (3, 6), (3, 12), (3, 17), (3, 23);

INSERT INTO tc_group_event (groupid, eventid) VALUES
(1, 1), (1, 2), (1, 7), (1, 18), (1, 20),
(2, 3), (2, 4), (2, 9), (2, 16), (2, 21),
(3, 5), (3, 6), (3, 15), (3, 17), (3, 24);

INSERT INTO tc_device_event (deviceid, eventid) VALUES
(1, 1), (1, 2), (1, 7), (1, 10), (1, 11), (1, 18), (1, 20), (1, 22), (1, 25), (1, 26), (1, 29),
(2, 3), (2, 4), (2, 8), (2, 13), (2, 14), (2, 19), (2, 27), (2, 28),
(3, 5), (3, 6), (3, 12), (3, 17), (3, 23), (3, 30),
(4, 9), (4, 16), (4, 21),
(5, 15), (5, 24);