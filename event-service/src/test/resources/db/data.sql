-- Event Service Test Data Population Script
-- This script populates the test database with sample event records, geofence definitions,
-- and event-device associations for testing event detection, processing, and notification
-- triggering functionality.

-- Clear existing test data to ensure clean state
DELETE FROM tc_events WHERE 1=1;
DELETE FROM tc_geofences WHERE 1=1;
DELETE FROM tc_user_geofence WHERE 1=1;
DELETE FROM tc_device_geofence WHERE 1=1;
DELETE FROM tc_group_geofence WHERE 1=1;

-- Reset sequences (with database compatibility handling)
-- PostgreSQL
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'tc_events_id_seq') THEN
        ALTER SEQUENCE tc_events_id_seq RESTART WITH 1;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'tc_geofences_id_seq') THEN
        ALTER SEQUENCE tc_geofences_id_seq RESTART WITH 1;
    END IF;
EXCEPTION WHEN OTHERS THEN
    -- Ignore errors for other database types
END
$$;

-- H2 Database (for in-memory testing)
ALTER TABLE tc_events ALTER COLUMN id RESTART WITH 1;
ALTER TABLE tc_geofences ALTER COLUMN id RESTART WITH 1;

-- Insert test geofences
INSERT INTO tc_geofences (id, name, description, area, attributes, calendarid) VALUES 
(1, 'Office Zone', 'Main office perimeter', 'POLYGON((30.0 10.0, 40.0 40.0, 20.0 40.0, 10.0 20.0, 30.0 10.0))', '{"color":"#FF0000","radius":500}', NULL),
(2, 'Warehouse', 'Warehouse loading area', 'POLYGON((35.0 15.0, 45.0 45.0, 25.0 45.0, 15.0 25.0, 35.0 15.0))', '{"color":"#00FF00","radius":300}', NULL),
(3, 'Customer Site', 'Customer location', 'POLYGON((38.0 18.0, 48.0 48.0, 28.0 48.0, 18.0 28.0, 38.0 18.0))', '{"color":"#0000FF","radius":200}', NULL),
(4, 'Restricted Area', 'No entry zone', 'POLYGON((40.0 20.0, 50.0 50.0, 30.0 50.0, 20.0 30.0, 40.0 20.0))', '{"color":"#FF00FF","radius":150}', NULL),
(5, 'Parking Lot', 'Vehicle parking area', 'POLYGON((42.0 22.0, 52.0 52.0, 32.0 52.0, 22.0 32.0, 42.0 22.0))', '{"color":"#FFFF00","radius":100}', NULL);

-- Insert test events with various types and timestamps
-- Note: The Event Service is responsible for detecting and processing these events
-- based on position data received from the Position Service
INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, geofenceid, maintenanceid, attributes) VALUES
-- Geofence events
(1, 'geofenceEnter', '2023-01-01 08:00:00', 1, 1001, 1, NULL, '{"priority":0}'),
(2, 'geofenceExit', '2023-01-01 17:00:00', 1, 1002, 1, NULL, '{"priority":0}'),
(3, 'geofenceEnter', '2023-01-02 08:30:00', 1, 1003, 1, NULL, '{"priority":0}'),
(4, 'geofenceExit', '2023-01-02 16:45:00', 1, 1004, 1, NULL, '{"priority":0}'),
(5, 'geofenceEnter', '2023-01-01 09:15:00', 2, 2001, 2, NULL, '{"priority":0}'),
(6, 'geofenceExit', '2023-01-01 15:30:00', 2, 2002, 2, NULL, '{"priority":0}'),

-- Speed events
(7, 'deviceOverspeed', '2023-01-03 12:30:00', 1, 1005, NULL, NULL, '{"priority":1,"speed":135.0}'),
(8, 'deviceOverspeed', '2023-01-04 14:45:00', 2, 2003, NULL, NULL, '{"priority":1,"speed":142.0}'),
(9, 'deviceOverspeed', '2023-01-05 10:15:00', 3, 3001, NULL, NULL, '{"priority":1,"speed":128.0}'),

-- Motion events
(10, 'deviceMoving', '2023-01-06 08:00:00', 1, 1006, NULL, NULL, '{"priority":0}'),
(11, 'deviceStopped', '2023-01-06 08:45:00', 1, 1007, NULL, NULL, '{"priority":0}'),
(12, 'deviceMoving', '2023-01-06 09:30:00', 1, 1008, NULL, NULL, '{"priority":0}'),
(13, 'deviceStopped', '2023-01-06 10:15:00', 1, 1009, NULL, NULL, '{"priority":0}'),

-- Status events
(14, 'deviceOnline', '2023-01-07 08:00:00', 2, NULL, NULL, NULL, '{"priority":0}'),
(15, 'deviceOffline', '2023-01-07 18:00:00', 2, NULL, NULL, NULL, '{"priority":0}'),
(16, 'deviceUnknown', '2023-01-07 23:00:00', 3, NULL, NULL, NULL, '{"priority":0}'),

-- Alarm events
(17, 'alarm', '2023-01-08 02:30:00', 1, 1010, NULL, NULL, '{"priority":2,"alarm":"sos"}'),
(18, 'alarm', '2023-01-08 14:15:00', 2, 2004, NULL, NULL, '{"priority":2,"alarm":"power"}'),
(19, 'alarm', '2023-01-09 07:45:00', 3, 3002, NULL, NULL, '{"priority":2,"alarm":"vibration"}'),

-- Ignition events
(20, 'ignitionOn', '2023-01-10 07:30:00', 1, 1011, NULL, NULL, '{"priority":0}'),
(21, 'ignitionOff', '2023-01-10 17:45:00', 1, 1012, NULL, NULL, '{"priority":0}'),

-- Maintenance events
(22, 'maintenance', '2023-01-11 10:00:00', 1, 1013, NULL, 1, '{"priority":1,"maintenance":"oil"}'),
(23, 'maintenance', '2023-01-12 11:30:00', 2, 2005, NULL, 2, '{"priority":1,"maintenance":"service"}'),

-- Driver events
(24, 'driverChanged', '2023-01-13 08:00:00', 1, 1014, NULL, NULL, '{"priority":0,"driverId":101}'),
(25, 'driverChanged', '2023-01-13 17:00:00', 1, 1015, NULL, NULL, '{"priority":0,"driverId":102}'),

-- Command result events
(26, 'commandResult', '2023-01-14 09:30:00', 1, NULL, NULL, NULL, '{"priority":0,"commandId":201,"success":true}'),
(27, 'commandResult', '2023-01-14 14:45:00', 2, NULL, NULL, NULL, '{"priority":0,"commandId":202,"success":false,"result":"Timeout"}'),

-- Device events with different priorities
(28, 'deviceFuelDrop', '2023-01-15 11:30:00', 1, 1016, NULL, NULL, '{"priority":1,"fuelDrop":15.5}'),
(29, 'deviceFuelIncrease', '2023-01-16 10:15:00', 1, 1017, NULL, NULL, '{"priority":0,"fuelIncrease":25.0}'),
(30, 'deviceCrash', '2023-01-17 13:45:00', 2, 2006, NULL, NULL, '{"priority":3,"acceleration":12.5}'),

-- Additional events for testing time-based queries
(31, 'geofenceEnter', '2023-02-01 08:00:00', 1, 1018, 1, NULL, '{"priority":0}'),
(32, 'geofenceExit', '2023-02-01 17:00:00', 1, 1019, 1, NULL, '{"priority":0}'),
(33, 'deviceOverspeed', '2023-02-02 12:30:00', 1, 1020, NULL, NULL, '{"priority":1,"speed":138.0}'),
(34, 'alarm', '2023-02-03 02:30:00', 1, 1021, NULL, NULL, '{"priority":2,"alarm":"tampering"}'),

-- Events with custom attributes for testing attribute filtering
(35, 'custom', '2023-02-04 10:00:00', 1, 1022, NULL, NULL, '{"priority":1,"customAttribute":"test1","testValue":123}'),
(36, 'custom', '2023-02-04 11:00:00', 2, 2007, NULL, NULL, '{"priority":1,"customAttribute":"test2","testValue":456}'),
(37, 'custom', '2023-02-04 12:00:00', 3, 3003, NULL, NULL, '{"priority":1,"customAttribute":"test3","testValue":789}'),

-- Events for testing multi-condition filtering
(38, 'multiCondition', '2023-02-05 09:00:00', 1, 1023, 1, NULL, '{"priority":2,"condition1":true,"condition2":false,"value":100}'),
(39, 'multiCondition', '2023-02-05 10:00:00', 1, 1024, 2, NULL, '{"priority":2,"condition1":true,"condition2":true,"value":200}'),
(40, 'multiCondition', '2023-02-05 11:00:00', 2, 2008, 3, NULL, '{"priority":2,"condition1":false,"condition2":true,"value":300}');

-- Insert test device-geofence associations
INSERT INTO tc_device_geofence (deviceid, geofenceid) VALUES
(1, 1),
(1, 3),
(1, 5),
(2, 2),
(2, 4),
(3, 1),
(3, 2),
(3, 3);

-- Insert test user-geofence associations (assuming user IDs exist)
INSERT INTO tc_user_geofence (userid, geofenceid) VALUES
(1, 1),
(1, 2),
(1, 3),
(1, 4),
(1, 5),
(2, 1),
(2, 2),
(3, 3),
(3, 4);

-- Insert test group-geofence associations (assuming group IDs exist)
INSERT INTO tc_group_geofence (groupid, geofenceid) VALUES
(1, 1),
(1, 2),
(2, 3),
(2, 4),
(3, 5);

-- Insert test event rules (for event processing testing)
-- Note: These are simplified examples. In production, rules would be more complex
INSERT INTO tc_event_rules (id, deviceid, type, always, attributes) VALUES
(1, 1, 'geofenceEnter', true, '{"priority":0,"description":"Office entry notification","notificationTypes":["web","mail"]}'),
(2, 1, 'geofenceExit', true, '{"priority":0,"description":"Office exit notification","notificationTypes":["web"]}'),
(3, 1, 'deviceOverspeed', true, '{"priority":1,"description":"Speeding alert","notificationTypes":["web","mail","sms"],"speedThreshold":120}'),
(4, 2, 'deviceOverspeed', true, '{"priority":1,"description":"Speeding alert","notificationTypes":["web","mail"],"speedThreshold":100}'),
(5, NULL, 'alarm', true, '{"priority":2,"description":"SOS alarm","notificationTypes":["web","mail","sms","push"]}'),
(6, 1, 'deviceFuelDrop', true, '{"priority":1,"description":"Fuel theft alert","notificationTypes":["web","mail"],"threshold":10}'),
(7, 2, 'deviceCrash', true, '{"priority":3,"description":"Crash alert","notificationTypes":["web","mail","sms","push"]}');

-- Insert test event forwarding configurations (for integration testing)
INSERT INTO tc_event_forwarding (id, description, type, url, attributes) VALUES
(1, 'Webhook Endpoint 1', 'webhook', 'https://example.com/webhook1', '{"headers":{"Authorization":"Bearer test-token"},"retryCount":3}'),
(2, 'Webhook Endpoint 2', 'webhook', 'https://example.com/webhook2', '{"headers":{"X-API-Key":"test-api-key"},"retryCount":5}'),
(3, 'External System', 'json', 'https://external-system.example.com/events', '{"headers":{"Content-Type":"application/json"},"retryCount":3,"batchSize":10}');

-- Insert test event-forwarding associations
INSERT INTO tc_event_forwarding_events (forwardingid, eventid) VALUES
(1, 1),
(1, 2),
(1, 7),
(2, 17),
(2, 18),
(2, 19),
(3, 28),
(3, 29),
(3, 30);

-- -----------------------------------------------------------------------
-- Test Data Summary:
-- -----------------------------------------------------------------------
-- 1. Geofences: 5 different geofences with various shapes and attributes
-- 2. Events: 40 events of different types:
--    - Geofence events (enter/exit)
--    - Speed events (overspeed)
--    - Motion events (moving/stopped)
--    - Status events (online/offline/unknown)
--    - Alarm events (various types)
--    - Ignition events (on/off)
--    - Maintenance events
--    - Driver events
--    - Command result events
--    - Device-specific events (fuel, crash)
--    - Custom events with various attributes
-- 3. Event Rules: 7 rules for different event types and devices
-- 4. Event Forwarding: 3 forwarding configurations with associations
-- 5. Associations:
--    - Device-Geofence associations
--    - User-Geofence associations
--    - Group-Geofence associations
--    - Event-Forwarding associations
-- -----------------------------------------------------------------------
-- This test data is designed to support comprehensive testing of the
-- Event Service's event detection, processing, and notification
-- triggering functionality in isolation from other services.
-- -----------------------------------------------------------------------