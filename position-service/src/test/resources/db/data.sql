-- Position Service Test Data
-- This file contains test data for Position Service testing

-- Clear existing test data
DELETE FROM tc_positions;

-- Insert test devices (if not already present in the test database)
INSERT INTO tc_devices (id, name, uniqueid, status, lastupdate, positionid) 
VALUES 
(1, 'Test Device 1', '123456789012345', 'online', NOW(), NULL),
(2, 'Test Device 2', '123456789012346', 'offline', NOW(), NULL),
(3, 'Test Device 3', '123456789012347', 'unknown', NOW(), NULL),
(4, 'Test Device 4', '123456789012348', 'online', NOW(), NULL),
(5, 'Test Device 5', '123456789012349', 'online', NOW(), NULL)
ON CONFLICT (id) DO NOTHING;

-- Insert test positions with various attributes
-- Device 1: Regular movement pattern
INSERT INTO tc_positions (id, deviceid, protocol, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes) VALUES
(101, 1, 'test-protocol', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', true, 40.7128, -74.0060, 10.0, 0.0, 0.0, '123 Test Street, New York, NY', '{"battery": 100, "ignition": true, "sat": 8, "hdop": 1.1, "event": 1, "odometer": 1000.0, "fuel": 90.5, "motion": true}'),
(102, 1, 'test-protocol', NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '50 minutes', true, 40.7130, -74.0065, 12.0, 15.5, 45.0, '124 Test Street, New York, NY', '{"battery": 98, "ignition": true, "sat": 9, "hdop": 1.0, "event": 0, "odometer": 1005.5, "fuel": 90.0, "motion": true}'),
(103, 1, 'test-protocol', NOW() - INTERVAL '40 minutes', NOW() - INTERVAL '40 minutes', NOW() - INTERVAL '40 minutes', true, 40.7135, -74.0070, 15.0, 25.0, 45.0, '125 Test Street, New York, NY', '{"battery": 97, "ignition": true, "sat": 10, "hdop": 0.9, "event": 0, "odometer": 1010.0, "fuel": 89.5, "motion": true}'),
(104, 1, 'test-protocol', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '30 minutes', true, 40.7140, -74.0075, 14.0, 30.0, 45.0, '126 Test Street, New York, NY', '{"battery": 96, "ignition": true, "sat": 9, "hdop": 1.0, "event": 0, "odometer": 1015.0, "fuel": 89.0, "motion": true}'),
(105, 1, 'test-protocol', NOW() - INTERVAL '20 minutes', NOW() - INTERVAL '20 minutes', NOW() - INTERVAL '20 minutes', true, 40.7145, -74.0080, 13.0, 28.5, 45.0, '127 Test Street, New York, NY', '{"battery": 95, "ignition": true, "sat": 8, "hdop": 1.1, "event": 0, "odometer": 1020.0, "fuel": 88.5, "motion": true}'),
(106, 1, 'test-protocol', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes', true, 40.7150, -74.0085, 12.0, 0.0, 45.0, '128 Test Street, New York, NY', '{"battery": 94, "ignition": false, "sat": 8, "hdop": 1.2, "event": 2, "odometer": 1025.0, "fuel": 88.0, "motion": false}');

-- Device 2: Stationary with GPS issues
INSERT INTO tc_positions (id, deviceid, protocol, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes) VALUES
(201, 2, 'test-protocol', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', true, 34.0522, -118.2437, 50.0, 0.0, 0.0, '456 Test Blvd, Los Angeles, CA', '{"battery": 75, "ignition": false, "sat": 8, "hdop": 1.1, "event": 0, "odometer": 5000.0, "fuel": 45.5, "motion": false}'),
(202, 2, 'test-protocol', NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '50 minutes', true, 34.0522, -118.2437, 50.0, 0.0, 0.0, '456 Test Blvd, Los Angeles, CA', '{"battery": 74, "ignition": false, "sat": 7, "hdop": 1.2, "event": 0, "odometer": 5000.0, "fuel": 45.5, "motion": false}'),
(203, 2, 'test-protocol', NOW() - INTERVAL '40 minutes', NOW() - INTERVAL '40 minutes', NOW() - INTERVAL '40 minutes', false, 34.0523, -118.2438, 51.0, 0.0, 0.0, '456 Test Blvd, Los Angeles, CA', '{"battery": 73, "ignition": false, "sat": 3, "hdop": 5.5, "event": 0, "odometer": 5000.0, "fuel": 45.5, "motion": false}'),
(204, 2, 'test-protocol', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '30 minutes', false, 34.0525, -118.2440, 52.0, 0.0, 0.0, '456 Test Blvd, Los Angeles, CA', '{"battery": 72, "ignition": false, "sat": 2, "hdop": 6.0, "event": 0, "odometer": 5000.0, "fuel": 45.5, "motion": false}'),
(205, 2, 'test-protocol', NOW() - INTERVAL '20 minutes', NOW() - INTERVAL '20 minutes', NOW() - INTERVAL '20 minutes', true, 34.0522, -118.2437, 50.0, 0.0, 0.0, '456 Test Blvd, Los Angeles, CA', '{"battery": 71, "ignition": false, "sat": 9, "hdop": 0.9, "event": 0, "odometer": 5000.0, "fuel": 45.5, "motion": false}'),
(206, 2, 'test-protocol', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes', true, 34.0522, -118.2437, 50.0, 0.0, 0.0, '456 Test Blvd, Los Angeles, CA', '{"battery": 70, "ignition": false, "sat": 8, "hdop": 1.0, "event": 0, "odometer": 5000.0, "fuel": 45.5, "motion": false}');

-- Device 3: Geofence testing scenario
INSERT INTO tc_positions (id, deviceid, protocol, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes) VALUES
(301, 3, 'test-protocol', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', true, 37.7749, -122.4194, 20.0, 0.0, 0.0, '789 Test Ave, San Francisco, CA', '{"battery": 85, "ignition": true, "sat": 10, "hdop": 0.8, "event": 0, "odometer": 3000.0, "fuel": 70.0, "motion": false}'),
(302, 3, 'test-protocol', NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '50 minutes', true, 37.7740, -122.4180, 22.0, 15.0, 135.0, '790 Test Ave, San Francisco, CA', '{"battery": 84, "ignition": true, "sat": 10, "hdop": 0.8, "event": 0, "odometer": 3005.0, "fuel": 69.5, "motion": true}'),
(303, 3, 'test-protocol', NOW() - INTERVAL '40 minutes', NOW() - INTERVAL '40 minutes', NOW() - INTERVAL '40 minutes', true, 37.7730, -122.4170, 23.0, 18.0, 135.0, '791 Test Ave, San Francisco, CA', '{"battery": 83, "ignition": true, "sat": 9, "hdop": 0.9, "event": 0, "odometer": 3010.0, "fuel": 69.0, "motion": true}'),
(304, 3, 'test-protocol', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '30 minutes', true, 37.7720, -122.4160, 24.0, 20.0, 135.0, '792 Test Ave, San Francisco, CA', '{"battery": 82, "ignition": true, "sat": 9, "hdop": 0.9, "event": 0, "odometer": 3015.0, "fuel": 68.5, "motion": true}'),
(305, 3, 'test-protocol', NOW() - INTERVAL '20 minutes', NOW() - INTERVAL '20 minutes', NOW() - INTERVAL '20 minutes', true, 37.7710, -122.4150, 25.0, 22.0, 135.0, '793 Test Ave, San Francisco, CA', '{"battery": 81, "ignition": true, "sat": 8, "hdop": 1.0, "event": 0, "odometer": 3020.0, "fuel": 68.0, "motion": true}'),
(306, 3, 'test-protocol', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes', true, 37.7700, -122.4140, 26.0, 25.0, 135.0, '794 Test Ave, San Francisco, CA', '{"battery": 80, "ignition": true, "sat": 8, "hdop": 1.0, "event": 0, "odometer": 3025.0, "fuel": 67.5, "motion": true}');

-- Device 4: Speed limit testing scenario
INSERT INTO tc_positions (id, deviceid, protocol, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes) VALUES
(401, 4, 'test-protocol', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', true, 41.8781, -87.6298, 30.0, 0.0, 0.0, '101 Test Road, Chicago, IL', '{"battery": 90, "ignition": true, "sat": 9, "hdop": 0.9, "event": 0, "odometer": 7000.0, "fuel": 80.0, "motion": false}'),
(402, 4, 'test-protocol', NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '50 minutes', true, 41.8785, -87.6290, 30.0, 20.0, 90.0, '102 Test Road, Chicago, IL', '{"battery": 89, "ignition": true, "sat": 9, "hdop": 0.9, "event": 0, "odometer": 7005.0, "fuel": 79.5, "motion": true}'),
(403, 4, 'test-protocol', NOW() - INTERVAL '40 minutes', NOW() - INTERVAL '40 minutes', NOW() - INTERVAL '40 minutes', true, 41.8790, -87.6280, 30.0, 40.0, 90.0, '103 Test Road, Chicago, IL', '{"battery": 88, "ignition": true, "sat": 10, "hdop": 0.8, "event": 0, "odometer": 7010.0, "fuel": 79.0, "motion": true}'),
(404, 4, 'test-protocol', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '30 minutes', true, 41.8795, -87.6270, 30.0, 60.0, 90.0, '104 Test Road, Chicago, IL', '{"battery": 87, "ignition": true, "sat": 10, "hdop": 0.8, "event": 0, "odometer": 7015.0, "fuel": 78.5, "motion": true}'),
(405, 4, 'test-protocol', NOW() - INTERVAL '20 minutes', NOW() - INTERVAL '20 minutes', NOW() - INTERVAL '20 minutes', true, 41.8800, -87.6260, 30.0, 80.0, 90.0, '105 Test Road, Chicago, IL', '{"battery": 86, "ignition": true, "sat": 9, "hdop": 0.9, "event": 0, "odometer": 7020.0, "fuel": 78.0, "motion": true}'),
(406, 4, 'test-protocol', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes', true, 41.8805, -87.6250, 30.0, 100.0, 90.0, '106 Test Road, Chicago, IL', '{"battery": 85, "ignition": true, "sat": 9, "hdop": 0.9, "event": 0, "odometer": 7025.0, "fuel": 77.5, "motion": true}');

-- Device 5: Various attribute testing
INSERT INTO tc_positions (id, deviceid, protocol, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes) VALUES
(501, 5, 'test-protocol', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', true, 47.6062, -122.3321, 40.0, 0.0, 0.0, '555 Test Place, Seattle, WA', '{"battery": 100, "ignition": false, "sat": 8, "hdop": 1.0, "event": 0, "odometer": 9000.0, "fuel": 95.0, "motion": false, "temp": 25.5, "humidity": 60, "door": false, "alarm": false, "acceleration": 0.0, "rpm": 0, "vin": "TEST12345678901", "driverUniqueId": "driver123"}'),
(502, 5, 'test-protocol', NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '50 minutes', true, 47.6062, -122.3321, 40.0, 0.0, 0.0, '555 Test Place, Seattle, WA', '{"battery": 99, "ignition": true, "sat": 9, "hdop": 0.9, "event": 0, "odometer": 9000.0, "fuel": 95.0, "motion": false, "temp": 26.0, "humidity": 58, "door": true, "alarm": false, "acceleration": 0.0, "rpm": 800, "vin": "TEST12345678901", "driverUniqueId": "driver123"}'),
(503, 5, 'test-protocol', NOW() - INTERVAL '40 minutes', NOW() - INTERVAL '40 minutes', NOW() - INTERVAL '40 minutes', true, 47.6065, -122.3325, 40.0, 15.0, 45.0, '556 Test Place, Seattle, WA', '{"battery": 98, "ignition": true, "sat": 10, "hdop": 0.8, "event": 0, "odometer": 9005.0, "fuel": 94.5, "motion": true, "temp": 27.0, "humidity": 55, "door": true, "alarm": false, "acceleration": 1.2, "rpm": 1500, "vin": "TEST12345678901", "driverUniqueId": "driver123"}'),
(504, 5, 'test-protocol', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '30 minutes', true, 47.6070, -122.3330, 40.0, 25.0, 45.0, '557 Test Place, Seattle, WA', '{"battery": 97, "ignition": true, "sat": 10, "hdop": 0.8, "event": 0, "odometer": 9010.0, "fuel": 94.0, "motion": true, "temp": 28.0, "humidity": 52, "door": true, "alarm": false, "acceleration": 0.5, "rpm": 2000, "vin": "TEST12345678901", "driverUniqueId": "driver123"}'),
(505, 5, 'test-protocol', NOW() - INTERVAL '20 minutes', NOW() - INTERVAL '20 minutes', NOW() - INTERVAL '20 minutes', true, 47.6075, -122.3335, 40.0, 30.0, 45.0, '558 Test Place, Seattle, WA', '{"battery": 96, "ignition": true, "sat": 9, "hdop": 0.9, "event": 0, "odometer": 9015.0, "fuel": 93.5, "motion": true, "temp": 29.0, "humidity": 50, "door": true, "alarm": false, "acceleration": 0.3, "rpm": 2200, "vin": "TEST12345678901", "driverUniqueId": "driver123"}'),
(506, 5, 'test-protocol', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes', true, 47.6080, -122.3340, 40.0, 0.0, 45.0, '559 Test Place, Seattle, WA', '{"battery": 95, "ignition": false, "sat": 8, "hdop": 1.0, "event": 0, "odometer": 9020.0, "fuel": 93.0, "motion": false, "temp": 25.0, "humidity": 55, "door": false, "alarm": false, "acceleration": 0.0, "rpm": 0, "vin": "TEST12345678901", "driverUniqueId": "driver123"}');

-- Insert test position attributes for specific testing scenarios
INSERT INTO tc_positions (id, deviceid, protocol, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes) VALUES
-- Low battery alert testing
(601, 1, 'test-protocol', NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '5 minutes', true, 40.7155, -74.0090, 12.0, 0.0, 45.0, '129 Test Street, New York, NY', '{"battery": 10, "ignition": false, "sat": 8, "hdop": 1.2, "event": 0, "odometer": 1030.0, "fuel": 87.5, "motion": false}'),

-- Fuel level drop testing
(602, 3, 'test-protocol', NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '5 minutes', true, 37.7690, -122.4130, 26.0, 0.0, 135.0, '795 Test Ave, San Francisco, CA', '{"battery": 79, "ignition": true, "sat": 8, "hdop": 1.0, "event": 0, "odometer": 3030.0, "fuel": 40.0, "motion": false}'),

-- SOS/Panic button testing
(603, 4, 'test-protocol', NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '5 minutes', true, 41.8810, -87.6240, 30.0, 0.0, 90.0, '107 Test Road, Chicago, IL', '{"battery": 84, "ignition": true, "sat": 9, "hdop": 0.9, "event": 99, "odometer": 7030.0, "fuel": 77.0, "motion": false, "alarm": true, "sos": true}'),

-- Device status change testing
(604, 2, 'test-protocol', NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '5 minutes', true, 34.0522, -118.2437, 50.0, 0.0, 0.0, '456 Test Blvd, Los Angeles, CA', '{"battery": 69, "ignition": true, "sat": 8, "hdop": 1.0, "event": 0, "odometer": 5000.0, "fuel": 45.5, "motion": false, "power": "on", "status": "charging"}'),

-- Maintenance alert testing
(605, 5, 'test-protocol', NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '5 minutes', true, 47.6085, -122.3345, 40.0, 15.0, 45.0, '560 Test Place, Seattle, WA', '{"battery": 94, "ignition": true, "sat": 9, "hdop": 0.9, "event": 0, "odometer": 10000.0, "fuel": 92.5, "motion": true, "temp": 26.0, "humidity": 54, "door": true, "alarm": false, "acceleration": 0.5, "rpm": 1800, "vin": "TEST12345678901", "driverUniqueId": "driver123", "serviceOdometer": 10000.0, "hours": 500}');

-- Update device lastPositionId to point to the latest position
UPDATE tc_devices SET positionid = 106 WHERE id = 1;
UPDATE tc_devices SET positionid = 206 WHERE id = 2;
UPDATE tc_devices SET positionid = 306 WHERE id = 3;
UPDATE tc_devices SET positionid = 406 WHERE id = 4;
UPDATE tc_devices SET positionid = 506 WHERE id = 5;

-- Insert test geofences for position testing
INSERT INTO tc_geofences (id, name, description, area) VALUES
(1, 'New York Office', 'Test geofence for New York office', 'CIRCLE (40.7150 -74.0085, 100)'),
(2, 'LA Warehouse', 'Test geofence for LA warehouse', 'CIRCLE (34.0522 -118.2437, 150)'),
(3, 'SF Zone', 'Test geofence for San Francisco area', 'POLYGON ((37.7700 -122.4140, 37.7749 -122.4194, 37.7800 -122.4100, 37.7700 -122.4050, 37.7700 -122.4140))'),
(4, 'Chicago Route', 'Test geofence for Chicago route', 'LINESTRING (41.8781 -87.6298, 41.8805 -87.6250)'),
(5, 'Seattle Office', 'Test geofence for Seattle office', 'CIRCLE (47.6080 -122.3340, 200)')
ON CONFLICT (id) DO NOTHING;

-- Link devices to geofences for testing
INSERT INTO tc_user_geofence (userid, geofenceid) VALUES
(1, 1), (1, 2), (1, 3), (1, 4), (1, 5)
ON CONFLICT DO NOTHING;

INSERT INTO tc_device_geofence (deviceid, geofenceid) VALUES
(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)
ON CONFLICT DO NOTHING;