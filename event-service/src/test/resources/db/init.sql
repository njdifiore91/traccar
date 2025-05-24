-- Event Service Test Database Initialization Script
-- This script initializes the database schema and test data for Event Service testing
-- It is designed to be atomic and provide a consistent database state for integration tests

-- Start transaction to ensure atomic execution
BEGIN;

-- Error handling
\set ON_ERROR_STOP true

-- Schema creation

-- Events table
CREATE TABLE IF NOT EXISTS tc_events (
    id SERIAL PRIMARY KEY,
    type VARCHAR(128) NOT NULL,
    servertime TIMESTAMP WITH TIME ZONE NOT NULL,
    deviceid BIGINT NOT NULL,
    positionid BIGINT,
    geofenceid BIGINT,
    maintenanceid BIGINT,
    attributes VARCHAR(4000)
);

CREATE INDEX IF NOT EXISTS event_deviceid_servertime ON tc_events(deviceid, servertime);

-- Geofences table
CREATE TABLE IF NOT EXISTS tc_geofences (
    id SERIAL PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(128),
    area VARCHAR(4000) NOT NULL,
    calendarid BIGINT,
    attributes VARCHAR(4000)
);

-- Device-Geofence relationship table
CREATE TABLE IF NOT EXISTS tc_device_geofence (
    deviceid BIGINT NOT NULL,
    geofenceid BIGINT NOT NULL,
    PRIMARY KEY (deviceid, geofenceid)
);

-- Group-Geofence relationship table
CREATE TABLE IF NOT EXISTS tc_group_geofence (
    groupid BIGINT NOT NULL,
    geofenceid BIGINT NOT NULL,
    PRIMARY KEY (groupid, geofenceid)
);

-- User-Geofence relationship table
CREATE TABLE IF NOT EXISTS tc_user_geofence (
    userid BIGINT NOT NULL,
    geofenceid BIGINT NOT NULL,
    PRIMARY KEY (userid, geofenceid)
);

-- Maintenance table
CREATE TABLE IF NOT EXISTS tc_maintenance (
    id SERIAL PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    type VARCHAR(128) NOT NULL,
    start DOUBLE PRECISION NOT NULL,
    period DOUBLE PRECISION NOT NULL,
    attributes VARCHAR(4000)
);

-- Device-Maintenance relationship table
CREATE TABLE IF NOT EXISTS tc_device_maintenance (
    deviceid BIGINT NOT NULL,
    maintenanceid BIGINT NOT NULL,
    PRIMARY KEY (deviceid, maintenanceid)
);

-- Calendars table for time-based event rules
CREATE TABLE IF NOT EXISTS tc_calendars (
    id SERIAL PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    data VARCHAR(4000) NOT NULL,
    attributes VARCHAR(4000)
);

-- Event rules table
CREATE TABLE IF NOT EXISTS tc_event_rules (
    id SERIAL PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    type VARCHAR(128) NOT NULL,
    deviceid BIGINT,
    groupid BIGINT,
    calendarid BIGINT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    attributes VARCHAR(4000)
);

-- Insert test data

-- Test calendars
INSERT INTO tc_calendars (name, data) VALUES
('Business Hours', '{"type":"weekly","days":["monday","tuesday","wednesday","thursday","friday"],"ranges":[{"from":"09:00:00","to":"17:00:00"}]}'),
('Weekend', '{"type":"weekly","days":["saturday","sunday"]}'),
('24/7', '{"type":"weekly","days":["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]}');

-- Test geofences
INSERT INTO tc_geofences (name, description, area) VALUES
('Office', 'Main office location', '{"type":"Circle","coordinates":[[30.0, 60.0]],"radius":100.0}'),
('Warehouse', 'Warehouse location', '{"type":"Polygon","coordinates":[[[30.1, 60.1],[30.2, 60.1],[30.2, 60.2],[30.1, 60.2],[30.1, 60.1]]]}'),
('Customer Site', 'Customer location', '{"type":"Circle","coordinates":[[30.3, 60.3]],"radius":50.0}'),
('Restricted Area', 'No entry zone', '{"type":"Polygon","coordinates":[[[30.4, 60.4],[30.5, 60.4],[30.5, 60.5],[30.4, 60.5],[30.4, 60.4]]]}'),
('Parking', 'Parking area', '{"type":"Circle","coordinates":[[30.05, 60.05]],"radius":25.0}');

-- Test maintenance items
INSERT INTO tc_maintenance (name, type, start, period) VALUES
('Oil Change', 'distance', 5000, 10000),
('Engine Check', 'hours', 100, 200),
('Tire Rotation', 'distance', 10000, 20000),
('General Service', 'hours', 500, 1000),
('Battery Check', 'days', 90, 180);

-- Test event rules
INSERT INTO tc_event_rules (name, type, enabled, attributes) VALUES
('Speeding', 'speedLimit', TRUE, '{"speedLimit":80}'),
('Harsh Braking', 'acceleration', TRUE, '{"threshold":0.3}'),
('Geofence Entry', 'geofenceEnter', TRUE, NULL),
('Geofence Exit', 'geofenceExit', TRUE, NULL),
('Device Offline', 'deviceOffline', TRUE, '{"timeThreshold":300}'),
('Fuel Drop', 'fuelDrop', TRUE, '{"threshold":10}'),
('Ignition On', 'ignitionOn', TRUE, NULL),
('Ignition Off', 'ignitionOff', TRUE, NULL),
('Motion', 'deviceMoving', TRUE, NULL),
('Stopped', 'deviceStopped', TRUE, '{"minStopDuration":300}');

-- Test events (simulating various event types for different devices)
INSERT INTO tc_events (type, servertime, deviceid, positionid, geofenceid, attributes) VALUES
('geofenceEnter', NOW() - INTERVAL '1 hour', 1, 1001, 1, '{"geofenceName":"Office"}'),
('deviceStopped', NOW() - INTERVAL '50 minutes', 1, 1002, NULL, '{"duration":600}'),
('ignitionOff', NOW() - INTERVAL '49 minutes', 1, 1003, NULL, NULL),
('ignitionOn', NOW() - INTERVAL '30 minutes', 1, 1004, NULL, NULL),
('deviceMoving', NOW() - INTERVAL '29 minutes', 1, 1005, NULL, NULL),
('geofenceExit', NOW() - INTERVAL '20 minutes', 1, 1006, 1, '{"geofenceName":"Office"}'),
('speedLimit', NOW() - INTERVAL '15 minutes', 1, 1007, NULL, '{"speedLimit":80,"speed":95}'),
('geofenceEnter', NOW() - INTERVAL '10 minutes', 1, 1008, 3, '{"geofenceName":"Customer Site"}'),
('deviceStopped', NOW() - INTERVAL '5 minutes', 1, 1009, NULL, '{"duration":300}'),
('ignitionOff', NOW() - INTERVAL '4 minutes', 1, 1010, NULL, NULL),

('geofenceEnter', NOW() - INTERVAL '2 hours', 2, 2001, 2, '{"geofenceName":"Warehouse"}'),
('deviceStopped', NOW() - INTERVAL '110 minutes', 2, 2002, NULL, '{"duration":1200}'),
('ignitionOff', NOW() - INTERVAL '109 minutes', 2, 2003, NULL, NULL),
('ignitionOn', NOW() - INTERVAL '60 minutes', 2, 2004, NULL, NULL),
('deviceMoving', NOW() - INTERVAL '59 minutes', 2, 2005, NULL, NULL),
('geofenceExit', NOW() - INTERVAL '50 minutes', 2, 2006, 2, '{"geofenceName":"Warehouse"}'),
('acceleration', NOW() - INTERVAL '40 minutes', 2, 2007, NULL, '{"acceleration":0.4}'),
('geofenceEnter', NOW() - INTERVAL '30 minutes', 2, 2008, 5, '{"geofenceName":"Parking"}'),
('deviceStopped', NOW() - INTERVAL '29 minutes', 2, 2009, NULL, '{"duration":600}'),
('ignitionOff', NOW() - INTERVAL '28 minutes', 2, 2010, NULL, NULL),

('geofenceEnter', NOW() - INTERVAL '3 hours', 3, 3001, 1, '{"geofenceName":"Office"}'),
('deviceStopped', NOW() - INTERVAL '170 minutes', 3, 3002, NULL, '{"duration":900}'),
('ignitionOff', NOW() - INTERVAL '169 minutes', 3, 3003, NULL, NULL),
('ignitionOn', NOW() - INTERVAL '100 minutes', 3, 3004, NULL, NULL),
('deviceMoving', NOW() - INTERVAL '99 minutes', 3, 3005, NULL, NULL),
('geofenceExit', NOW() - INTERVAL '90 minutes', 3, 3006, 1, '{"geofenceName":"Office"}'),
('fuelDrop', NOW() - INTERVAL '70 minutes', 3, 3007, NULL, '{"drop":15}'),
('deviceOffline', NOW() - INTERVAL '40 minutes', 3, NULL, NULL, '{"duration":600}'),
('deviceOnline', NOW() - INTERVAL '30 minutes', 3, 3008, NULL, NULL),
('maintenance', NOW() - INTERVAL '20 minutes', 3, 3009, NULL, '{"maintenance":"Oil Change"}');

-- Device-Geofence relationships
INSERT INTO tc_device_geofence (deviceid, geofenceid) VALUES
(1, 1), (1, 3), (1, 5),
(2, 2), (2, 5),
(3, 1), (3, 4);

-- Device-Maintenance relationships
INSERT INTO tc_device_maintenance (deviceid, maintenanceid) VALUES
(1, 1), (1, 3),
(2, 2), (2, 4),
(3, 1), (3, 5);

-- Commit transaction
COMMIT;