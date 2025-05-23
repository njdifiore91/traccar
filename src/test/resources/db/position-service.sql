-- Position Service Database Test Schema
-- This script creates the necessary tables and test data for position service testing

-- Drop tables if they exist to ensure clean state
DROP TABLE IF EXISTS tc_positions;

-- Create position table
CREATE TABLE tc_positions (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  deviceId BIGINT NOT NULL,
  protocol VARCHAR(128),
  serverTime TIMESTAMP NOT NULL,
  deviceTime TIMESTAMP NULL DEFAULT NULL,
  fixTime TIMESTAMP NULL DEFAULT NULL,
  valid BOOLEAN NOT NULL DEFAULT FALSE,
  latitude DOUBLE NOT NULL,
  longitude DOUBLE NOT NULL,
  altitude DOUBLE DEFAULT 0,
  speed DOUBLE DEFAULT 0,
  course DOUBLE DEFAULT 0,
  address VARCHAR(512),
  attributes VARCHAR(4000),
  accuracy DOUBLE DEFAULT 0,
  network VARCHAR(4000)
);

-- Create indexes for better query performance
CREATE INDEX position_deviceid_fixtime ON tc_positions(deviceId, fixTime);

-- Insert test position data for various scenarios

-- Basic position data with minimal attributes
INSERT INTO tc_positions (deviceId, protocol, serverTime, deviceTime, fixTime, valid, latitude, longitude, altitude, speed, course, address, attributes)
VALUES (1, 'tcp', '2023-01-01 10:00:00', '2023-01-01 10:00:00', '2023-01-01 10:00:00', true, 40.7128, -74.0060, 10.0, 0.0, 0.0, '350 5th Ave, New York, NY 10118', '{"sat":8,"hdop":1.1,"battery":95.0}');

-- Moving vehicle with speed and course
INSERT INTO tc_positions (deviceId, protocol, serverTime, deviceTime, fixTime, valid, latitude, longitude, altitude, speed, course, address, attributes)
VALUES (1, 'tcp', '2023-01-01 10:05:00', '2023-01-01 10:05:00', '2023-01-01 10:05:00', true, 40.7138, -74.0070, 10.0, 35.0, 90.0, '340 5th Ave, New York, NY 10118', '{"sat":10,"hdop":0.9,"battery":94.0,"ignition":true,"motion":true}');

-- Position with low accuracy
INSERT INTO tc_positions (deviceId, protocol, serverTime, deviceTime, fixTime, valid, latitude, longitude, altitude, speed, course, address, attributes, accuracy)
VALUES (1, 'tcp', '2023-01-01 10:10:00', '2023-01-01 10:10:00', '2023-01-01 10:10:00', true, 40.7148, -74.0080, 12.0, 40.0, 90.0, '330 5th Ave, New York, NY 10118', '{"sat":6,"hdop":2.5,"battery":93.0,"ignition":true,"motion":true}', 25.0);

-- Invalid position (GPS fix lost)
INSERT INTO tc_positions (deviceId, protocol, serverTime, deviceTime, fixTime, valid, latitude, longitude, altitude, speed, course, attributes)
VALUES (1, 'tcp', '2023-01-01 10:15:00', '2023-01-01 10:15:00', '2023-01-01 10:15:00', false, 0.0, 0.0, 0.0, 0.0, 0.0, '{"sat":0,"hdop":99.9,"battery":92.0,"ignition":true,"motion":false}');

-- Position with network information (cell towers)
INSERT INTO tc_positions (deviceId, protocol, serverTime, deviceTime, fixTime, valid, latitude, longitude, altitude, speed, course, address, attributes, network)
VALUES (1, 'tcp', '2023-01-01 10:20:00', '2023-01-01 10:20:00', '2023-01-01 10:20:00', true, 40.7158, -74.0090, 10.0, 30.0, 90.0, '320 5th Ave, New York, NY 10118', '{"sat":9,"hdop":1.0,"battery":91.0,"ignition":true,"motion":true}', '{"cellTowers":[{"mobileCountryCode":310,"mobileNetworkCode":410,"locationAreaCode":11365,"cellId":773}]}');

-- Position with complex attributes (full device status)
INSERT INTO tc_positions (deviceId, protocol, serverTime, deviceTime, fixTime, valid, latitude, longitude, altitude, speed, course, address, attributes)
VALUES (1, 'tcp', '2023-01-01 10:25:00', '2023-01-01 10:25:00', '2023-01-01 10:25:00', true, 40.7168, -74.0100, 11.0, 25.0, 90.0, '310 5th Ave, New York, NY 10118', 
'{"sat":11,"hdop":0.8,"pdop":1.5,"battery":90.0,"ignition":true,"motion":true,"rssi":75,"io200":0,"gpsStatus":1,"power":12.5,"io24":0,"distance":1250.63,"totalDistance":15250.78,"hours":3600,"temp":25.5,"fuel":85.3,"alarm":false,"odometer":12500.5,"event":0}');

-- Different device with basic position
INSERT INTO tc_positions (deviceId, protocol, serverTime, deviceTime, fixTime, valid, latitude, longitude, altitude, speed, course, address, attributes)
VALUES (2, 'tcp', '2023-01-01 10:00:00', '2023-01-01 10:00:00', '2023-01-01 10:00:00', true, 34.0522, -118.2437, 100.0, 0.0, 0.0, '200 E Broadway, Los Angeles, CA 90012', '{"sat":8,"hdop":1.1,"battery":100.0}');

-- Different device with movement
INSERT INTO tc_positions (deviceId, protocol, serverTime, deviceTime, fixTime, valid, latitude, longitude, altitude, speed, course, address, attributes)
VALUES (2, 'tcp', '2023-01-01 10:05:00', '2023-01-01 10:05:00', '2023-01-01 10:05:00', true, 34.0532, -118.2447, 100.0, 45.0, 180.0, '210 E Broadway, Los Angeles, CA 90012', '{"sat":10,"hdop":0.9,"battery":99.0,"ignition":true,"motion":true}');

-- Position with WiFi networks for location
INSERT INTO tc_positions (deviceId, protocol, serverTime, deviceTime, fixTime, valid, latitude, longitude, altitude, speed, course, address, attributes, network)
VALUES (3, 'tcp', '2023-01-01 10:00:00', '2023-01-01 10:00:00', '2023-01-01 10:00:00', true, 51.5074, -0.1278, 20.0, 0.0, 0.0, '10 Downing St, London SW1A 2AA, UK', '{"battery":85.0,"motion":false}', 
'{"wifiAccessPoints":[{"macAddress":"00:11:22:33:44:55","signalStrength":-65},{"macAddress":"66:77:88:99:AA:BB","signalStrength":-85}]}');

-- Position with both GPS and network location
INSERT INTO tc_positions (deviceId, protocol, serverTime, deviceTime, fixTime, valid, latitude, longitude, altitude, speed, course, address, attributes, accuracy, network)
VALUES (3, 'tcp', '2023-01-01 10:05:00', '2023-01-01 10:05:00', '2023-01-01 10:05:00', true, 51.5084, -0.1288, 22.0, 5.0, 45.0, '11 Downing St, London SW1A 2AB, UK', '{"sat":4,"hdop":2.0,"battery":84.0,"motion":true}', 15.0, 
'{"wifiAccessPoints":[{"macAddress":"CC:DD:EE:FF:00:11","signalStrength":-70},{"macAddress":"22:33:44:55:66:77","signalStrength":-90}],"cellTowers":[{"mobileCountryCode":234,"mobileNetworkCode":15,"locationAreaCode":12345,"cellId":6789}]}');

-- Position with extended device-specific attributes
INSERT INTO tc_positions (deviceId, protocol, serverTime, deviceTime, fixTime, valid, latitude, longitude, altitude, speed, course, address, attributes)
VALUES (4, 'tcp', '2023-01-01 10:00:00', '2023-01-01 10:00:00', '2023-01-01 10:00:00', true, 48.8566, 2.3522, 35.0, 0.0, 0.0, '1 Rue de Rivoli, 75001 Paris, France', 
'{"sat":12,"hdop":0.7,"battery":100.0,"ignition":false,"motion":false,"door":false,"lock":true,"arm":true,"alarm":false,"power":13.8,"rssi":90,"temperature":[{"id":1,"value":22.5},{"id":2,"value":18.0}],"humidity":45.0,"pressure":1013.25,"accelerometer":{"x":0.01,"y":0.02,"z":0.98},"obdData":{"rpm":0,"speed":0,"engineLoad":0,"coolantTemp":85,"fuelLevel":75,"vin":"1HGCM82633A123456"}}');

-- Position with geofence events
INSERT INTO tc_positions (deviceId, protocol, serverTime, deviceTime, fixTime, valid, latitude, longitude, altitude, speed, course, address, attributes)
VALUES (4, 'tcp', '2023-01-01 10:05:00', '2023-01-01 10:05:00', '2023-01-01 10:05:00', true, 48.8576, 2.3532, 35.0, 25.0, 90.0, '10 Rue de Rivoli, 75001 Paris, France', 
'{"sat":11,"hdop":0.8,"battery":99.0,"ignition":true,"motion":true,"door":false,"lock":true,"arm":true,"alarm":false,"power":13.6,"rssi":85,"geofenceExit":1,"geofenceEnter":2,"temperature":[{"id":1,"value":23.0},{"id":2,"value":18.5}],"humidity":44.0,"pressure":1013.0,"accelerometer":{"x":0.05,"y":0.02,"z":0.97},"obdData":{"rpm":1500,"speed":25,"engineLoad":35,"coolantTemp":90,"fuelLevel":74,"vin":"1HGCM82633A123456"}}');