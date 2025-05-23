-- Traccar Test Data SQL Script
-- This file contains test data for integration testing of the Traccar GPS tracking system
-- It populates the database with sample records for all core entities with realistic test scenarios

-- Clear existing test data if any exists
DELETE FROM tc_user_command;
DELETE FROM tc_device_command;
DELETE FROM tc_group_command;
DELETE FROM tc_user_notification;
DELETE FROM tc_device_notification;
DELETE FROM tc_group_notification;
DELETE FROM tc_user_geofence;
DELETE FROM tc_device_geofence;
DELETE FROM tc_group_geofence;
DELETE FROM tc_user_device;
DELETE FROM tc_group_device;
DELETE FROM tc_user_group;
DELETE FROM tc_events;
DELETE FROM tc_positions;
DELETE FROM tc_commands;
DELETE FROM tc_notifications;
DELETE FROM tc_geofences;
DELETE FROM tc_devices;
DELETE FROM tc_groups;
DELETE FROM tc_users;

-- Insert test users
-- Admin user
INSERT INTO tc_users (id, name, email, hashedpassword, salt, administrator, coordinateformat, latitude, longitude, zoom, twelvehourampm, disabled, expirationtime, devicelimit, token, attributes) 
VALUES (1, 'admin', 'admin@example.com', '4F6A6D832BE0CC3C22D58D9EFD9C45E4', '123', true, 'dd', 0, 0, 1, false, false, NULL, -1, 'admin_token', '{"notificationTokens":[],"poiLayer":"","mapLiveRoutes":false,"mapFollow":true,"mapLayers":"osm"}');

-- Manager user
INSERT INTO tc_users (id, name, email, hashedpassword, salt, administrator, coordinateformat, latitude, longitude, zoom, twelvehourampm, disabled, expirationtime, devicelimit, token, attributes) 
VALUES (2, 'manager', 'manager@example.com', '4F6A6D832BE0CC3C22D58D9EFD9C45E4', '123', false, 'dd', 0, 0, 1, false, false, NULL, 10, 'manager_token', '{"notificationTokens":[],"poiLayer":"","mapLiveRoutes":true,"mapFollow":true,"mapLayers":"osm"}');

-- Regular user
INSERT INTO tc_users (id, name, email, hashedpassword, salt, administrator, coordinateformat, latitude, longitude, zoom, twelvehourampm, disabled, expirationtime, devicelimit, token, attributes) 
VALUES (3, 'user', 'user@example.com', '4F6A6D832BE0CC3C22D58D9EFD9C45E4', '123', false, 'dd', 0, 0, 1, true, false, NULL, 3, 'user_token', '{"notificationTokens":[],"poiLayer":"","mapLiveRoutes":false,"mapFollow":true,"mapLayers":"osm"}');

-- Disabled user for testing access control
INSERT INTO tc_users (id, name, email, hashedpassword, salt, administrator, coordinateformat, latitude, longitude, zoom, twelvehourampm, disabled, expirationtime, devicelimit, token, attributes) 
VALUES (4, 'disabled', 'disabled@example.com', '4F6A6D832BE0CC3C22D58D9EFD9C45E4', '123', false, 'dd', 0, 0, 1, false, true, NULL, 0, 'disabled_token', '{"notificationTokens":[],"poiLayer":"","mapLiveRoutes":false,"mapFollow":true,"mapLayers":"osm"}');

-- Insert test groups
-- Main group
INSERT INTO tc_groups (id, name, groupid, attributes) 
VALUES (1, 'Main Group', NULL, '{"description":"Main test group"}');

-- Subgroups
INSERT INTO tc_groups (id, name, groupid, attributes) 
VALUES (2, 'Delivery Vehicles', 1, '{"description":"Delivery fleet vehicles"}');

INSERT INTO tc_groups (id, name, groupid, attributes) 
VALUES (3, 'Service Vehicles', 1, '{"description":"Service fleet vehicles"}');

INSERT INTO tc_groups (id, name, groupid, attributes) 
VALUES (4, 'Executive Vehicles', NULL, '{"description":"Executive fleet vehicles"}');

-- Insert test devices
-- Active devices with different protocols
INSERT INTO tc_devices (id, name, uniqueid, protocol, phone, lastupdate, positionid, groupid, status, disabled, attributes) 
VALUES (1, 'Test Device 1', '123456789012345', 'tk103', '+1234567890', NOW(), NULL, 2, 'online', false, '{"speedLimit":90,"maintenance.start":"2023-01-01T00:00:00.000Z","maintenance.interval":"P3M"}');

INSERT INTO tc_devices (id, name, uniqueid, protocol, phone, lastupdate, positionid, groupid, status, disabled, attributes) 
VALUES (2, 'Test Device 2', '123456789012346', 'teltonika', '+1234567891', NOW(), NULL, 2, 'online', false, '{"speedLimit":80,"fuel.consumption":8.5}');

INSERT INTO tc_devices (id, name, uniqueid, protocol, phone, lastupdate, positionid, groupid, status, disabled, attributes) 
VALUES (3, 'Test Device 3', '123456789012347', 'meitrack', '+1234567892', NOW(), NULL, 3, 'offline', false, '{"speedLimit":100,"fuel.capacity":65}');

INSERT INTO tc_devices (id, name, uniqueid, protocol, phone, lastupdate, positionid, groupid, status, disabled, attributes) 
VALUES (4, 'Test Device 4', '123456789012348', 'atrack', '+1234567893', NOW(), NULL, 3, 'unknown', false, '{"speedLimit":110,"driver.name":"John Smith"}');

INSERT INTO tc_devices (id, name, uniqueid, protocol, phone, lastupdate, positionid, groupid, status, disabled, attributes) 
VALUES (5, 'Executive Car', '123456789012349', 'totem', '+1234567894', NOW(), NULL, 4, 'online', false, '{"speedLimit":120,"driver.name":"Jane Doe","vip":true}');

-- Disabled device for testing access control
INSERT INTO tc_devices (id, name, uniqueid, protocol, phone, lastupdate, positionid, groupid, status, disabled, attributes) 
VALUES (6, 'Disabled Device', '123456789012350', 'tk103', '+1234567895', NOW(), NULL, NULL, 'offline', true, '{"speedLimit":90}');

-- Insert test positions
-- Recent positions for active devices
INSERT INTO tc_positions (id, protocol, deviceid, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes, accuracy, network) 
VALUES (1, 'tk103', 1, NOW() - INTERVAL '5 minute', NOW() - INTERVAL '5 minute', NOW() - INTERVAL '5 minute', true, 40.7128, -74.0060, 10, 0, 0, '123 Broadway, New York, NY, USA', '{"batteryLevel":95,"fuel":75,"ignition":true,"motion":false}', 5, NULL);

INSERT INTO tc_positions (id, protocol, deviceid, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes, accuracy, network) 
VALUES (2, 'tk103', 1, NOW() - INTERVAL '4 minute', NOW() - INTERVAL '4 minute', NOW() - INTERVAL '4 minute', true, 40.7130, -74.0065, 10, 15, 45, '124 Broadway, New York, NY, USA', '{"batteryLevel":94,"fuel":74,"ignition":true,"motion":true}', 4, NULL);

INSERT INTO tc_positions (id, protocol, deviceid, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes, accuracy, network) 
VALUES (3, 'tk103', 1, NOW() - INTERVAL '3 minute', NOW() - INTERVAL '3 minute', NOW() - INTERVAL '3 minute', true, 40.7135, -74.0070, 10, 30, 45, '125 Broadway, New York, NY, USA', '{"batteryLevel":93,"fuel":73,"ignition":true,"motion":true}', 3, NULL);

INSERT INTO tc_positions (id, protocol, deviceid, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes, accuracy, network) 
VALUES (4, 'teltonika', 2, NOW() - INTERVAL '5 minute', NOW() - INTERVAL '5 minute', NOW() - INTERVAL '5 minute', true, 34.0522, -118.2437, 50, 0, 0, '123 Hollywood Blvd, Los Angeles, CA, USA', '{"batteryLevel":85,"fuel":65,"ignition":false,"motion":false}', 8, NULL);

INSERT INTO tc_positions (id, protocol, deviceid, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes, accuracy, network) 
VALUES (5, 'teltonika', 2, NOW() - INTERVAL '3 minute', NOW() - INTERVAL '3 minute', NOW() - INTERVAL '3 minute', true, 34.0525, -118.2440, 50, 25, 90, '125 Hollywood Blvd, Los Angeles, CA, USA', '{"batteryLevel":84,"fuel":64,"ignition":true,"motion":true}', 6, NULL);

INSERT INTO tc_positions (id, protocol, deviceid, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes, accuracy, network) 
VALUES (6, 'meitrack', 3, NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day', true, 41.8781, -87.6298, 20, 0, 0, '123 Michigan Ave, Chicago, IL, USA', '{"batteryLevel":75,"fuel":55,"ignition":false,"motion":false}', 10, NULL);

INSERT INTO tc_positions (id, protocol, deviceid, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes, accuracy, network) 
VALUES (7, 'atrack', 4, NOW() - INTERVAL '2 hour', NOW() - INTERVAL '2 hour', NOW() - INTERVAL '2 hour', true, 29.7604, -95.3698, 15, 0, 0, '123 Main St, Houston, TX, USA', '{"batteryLevel":65,"fuel":45,"ignition":false,"motion":false}', 12, NULL);

INSERT INTO tc_positions (id, protocol, deviceid, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes, accuracy, network) 
VALUES (8, 'atrack', 4, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', true, 29.7605, -95.3700, 15, 10, 180, '124 Main St, Houston, TX, USA', '{"batteryLevel":64,"fuel":44,"ignition":true,"motion":true}', 11, NULL);

INSERT INTO tc_positions (id, protocol, deviceid, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes, accuracy, network) 
VALUES (9, 'totem', 5, NOW() - INTERVAL '30 minute', NOW() - INTERVAL '30 minute', NOW() - INTERVAL '30 minute', true, 37.7749, -122.4194, 30, 0, 0, '123 Market St, San Francisco, CA, USA', '{"batteryLevel":90,"fuel":85,"ignition":false,"motion":false}', 5, NULL);

INSERT INTO tc_positions (id, protocol, deviceid, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes, accuracy, network) 
VALUES (10, 'totem', 5, NOW() - INTERVAL '15 minute', NOW() - INTERVAL '15 minute', NOW() - INTERVAL '15 minute', true, 37.7750, -122.4196, 30, 15, 270, '124 Market St, San Francisco, CA, USA', '{"batteryLevel":89,"fuel":84,"ignition":true,"motion":true}', 4, NULL);

-- Invalid position for testing
INSERT INTO tc_positions (id, protocol, deviceid, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, address, attributes, accuracy, network) 
VALUES (11, 'tk103', 1, NOW() - INTERVAL '2 minute', NOW() - INTERVAL '2 minute', NOW() - INTERVAL '2 minute', false, 0, 0, 0, 0, 0, NULL, '{"batteryLevel":92,"fuel":72,"ignition":true,"motion":false}', 0, NULL);

-- Update devices with latest position IDs
UPDATE tc_devices SET positionid = 3 WHERE id = 1;
UPDATE tc_devices SET positionid = 5 WHERE id = 2;
UPDATE tc_devices SET positionid = 6 WHERE id = 3;
UPDATE tc_devices SET positionid = 8 WHERE id = 4;
UPDATE tc_devices SET positionid = 10 WHERE id = 5;

-- Insert test geofences
INSERT INTO tc_geofences (id, name, description, area, attributes) 
VALUES (1, 'New York Office', 'New York City office location', 'CIRCLE (40.7128 -74.0060, 500)', '{"color":"#FF0000"}');

INSERT INTO tc_geofences (id, name, description, area, attributes) 
VALUES (2, 'Los Angeles Office', 'Los Angeles office location', 'CIRCLE (34.0522 -118.2437, 500)', '{"color":"#00FF00"}');

INSERT INTO tc_geofences (id, name, description, area, attributes) 
VALUES (3, 'Chicago Office', 'Chicago office location', 'CIRCLE (41.8781 -87.6298, 500)', '{"color":"#0000FF"}');

INSERT INTO tc_geofences (id, name, description, area, attributes) 
VALUES (4, 'Houston Office', 'Houston office location', 'CIRCLE (29.7604 -95.3698, 500)', '{"color":"#FFFF00"}');

INSERT INTO tc_geofences (id, name, description, area, attributes) 
VALUES (5, 'San Francisco Office', 'San Francisco office location', 'CIRCLE (37.7749 -122.4194, 500)', '{"color":"#FF00FF"}');

INSERT INTO tc_geofences (id, name, description, area, attributes) 
VALUES (6, 'Delivery Zone NYC', 'New York City delivery zone', 'POLYGON ((40.7000 -74.0200, 40.7300 -74.0200, 40.7300 -73.9800, 40.7000 -73.9800, 40.7000 -74.0200))', '{"color":"#00FFFF"}');

-- Insert test events
INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, geofenceid, attributes) 
VALUES (1, 'deviceOnline', NOW() - INTERVAL '1 hour', 1, 1, NULL, '{"alarm":false}');

INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, geofenceid, attributes) 
VALUES (2, 'deviceMoving', NOW() - INTERVAL '55 minute', 1, 2, NULL, '{"alarm":false}');

INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, geofenceid, attributes) 
VALUES (3, 'geofenceEnter', NOW() - INTERVAL '50 minute', 1, 3, 1, '{"alarm":true}');

INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, geofenceid, attributes) 
VALUES (4, 'deviceOnline', NOW() - INTERVAL '2 hour', 2, 4, NULL, '{"alarm":false}');

INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, geofenceid, attributes) 
VALUES (5, 'geofenceEnter', NOW() - INTERVAL '1 hour 55 minute', 2, 5, 2, '{"alarm":true}');

INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, geofenceid, attributes) 
VALUES (6, 'deviceOffline', NOW() - INTERVAL '23 hour', 3, 6, NULL, '{"alarm":false}');

INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, geofenceid, attributes) 
VALUES (7, 'deviceStopped', NOW() - INTERVAL '2 hour', 4, 7, NULL, '{"alarm":false}');

INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, geofenceid, attributes) 
VALUES (8, 'deviceMoving', NOW() - INTERVAL '1 hour', 4, 8, NULL, '{"alarm":false}');

INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, geofenceid, attributes) 
VALUES (9, 'deviceStopped', NOW() - INTERVAL '30 minute', 5, 9, NULL, '{"alarm":false}');

INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, geofenceid, attributes) 
VALUES (10, 'geofenceEnter', NOW() - INTERVAL '15 minute', 5, 10, 5, '{"alarm":true}');

INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, geofenceid, attributes) 
VALUES (11, 'ignitionOn', NOW() - INTERVAL '14 minute', 5, 10, NULL, '{"alarm":false}');

INSERT INTO tc_events (id, type, eventtime, deviceid, positionid, geofenceid, attributes) 
VALUES (12, 'alarm', NOW() - INTERVAL '10 minute', 1, 3, NULL, '{"alarm":true,"alarmType":"sos"}');

-- Insert test notifications
INSERT INTO tc_notifications (id, type, always, notificators, attributes) 
VALUES (1, 'deviceOnline', false, '["web","mail"]', '{"description":"Device came online"}');

INSERT INTO tc_notifications (id, type, always, notificators, attributes) 
VALUES (2, 'deviceOffline', false, '["web","mail","sms"]', '{"description":"Device went offline"}');

INSERT INTO tc_notifications (id, type, always, notificators, attributes) 
VALUES (3, 'geofenceEnter', false, '["web","mail"]', '{"description":"Device entered geofence"}');

INSERT INTO tc_notifications (id, type, always, notificators, attributes) 
VALUES (4, 'geofenceExit', false, '["web","mail"]', '{"description":"Device exited geofence"}');

INSERT INTO tc_notifications (id, type, always, notificators, attributes) 
VALUES (5, 'alarm', true, '["web","mail","sms"]', '{"description":"Device triggered alarm"}');

-- Insert test commands
INSERT INTO tc_commands (id, description, type, textchannel, attributes) 
VALUES (1, 'Position Single', 'positionSingle', false, '{"description":"Request single position"}');

INSERT INTO tc_commands (id, description, type, textchannel, attributes) 
VALUES (2, 'Position Periodic', 'positionPeriodic', false, '{"description":"Request periodic position updates","data":{"frequency":15}}');

INSERT INTO tc_commands (id, description, type, textchannel, attributes) 
VALUES (3, 'Engine Stop', 'engineStop', false, '{"description":"Stop engine"}');

INSERT INTO tc_commands (id, description, type, textchannel, attributes) 
VALUES (4, 'Engine Resume', 'engineResume', false, '{"description":"Resume engine"}');

INSERT INTO tc_commands (id, description, type, textchannel, attributes) 
VALUES (5, 'Custom Command', 'custom', true, '{"description":"Custom device command","data":{"command":"AT+COMMAND"}}');

-- Insert user-device relationships
-- Admin has access to all devices
INSERT INTO tc_user_device (userid, deviceid) VALUES (1, 1);
INSERT INTO tc_user_device (userid, deviceid) VALUES (1, 2);
INSERT INTO tc_user_device (userid, deviceid) VALUES (1, 3);
INSERT INTO tc_user_device (userid, deviceid) VALUES (1, 4);
INSERT INTO tc_user_device (userid, deviceid) VALUES (1, 5);
INSERT INTO tc_user_device (userid, deviceid) VALUES (1, 6);

-- Manager has access to delivery and service vehicles
INSERT INTO tc_user_device (userid, deviceid) VALUES (2, 1);
INSERT INTO tc_user_device (userid, deviceid) VALUES (2, 2);
INSERT INTO tc_user_device (userid, deviceid) VALUES (2, 3);
INSERT INTO tc_user_device (userid, deviceid) VALUES (2, 4);

-- Regular user has access to delivery vehicles only
INSERT INTO tc_user_device (userid, deviceid) VALUES (3, 1);
INSERT INTO tc_user_device (userid, deviceid) VALUES (3, 2);

-- Insert user-group relationships
INSERT INTO tc_user_group (userid, groupid) VALUES (1, 1);
INSERT INTO tc_user_group (userid, groupid) VALUES (1, 2);
INSERT INTO tc_user_group (userid, groupid) VALUES (1, 3);
INSERT INTO tc_user_group (userid, groupid) VALUES (1, 4);
INSERT INTO tc_user_group (userid, groupid) VALUES (2, 2);
INSERT INTO tc_user_group (userid, groupid) VALUES (2, 3);
INSERT INTO tc_user_group (userid, groupid) VALUES (3, 2);

-- Insert group-device relationships
-- These are redundant with the device.groupid column but included for testing
INSERT INTO tc_group_device (groupid, deviceid) VALUES (2, 1);
INSERT INTO tc_group_device (groupid, deviceid) VALUES (2, 2);
INSERT INTO tc_group_device (groupid, deviceid) VALUES (3, 3);
INSERT INTO tc_group_device (groupid, deviceid) VALUES (3, 4);
INSERT INTO tc_group_device (groupid, deviceid) VALUES (4, 5);

-- Insert user-geofence relationships
INSERT INTO tc_user_geofence (userid, geofenceid) VALUES (1, 1);
INSERT INTO tc_user_geofence (userid, geofenceid) VALUES (1, 2);
INSERT INTO tc_user_geofence (userid, geofenceid) VALUES (1, 3);
INSERT INTO tc_user_geofence (userid, geofenceid) VALUES (1, 4);
INSERT INTO tc_user_geofence (userid, geofenceid) VALUES (1, 5);
INSERT INTO tc_user_geofence (userid, geofenceid) VALUES (1, 6);
INSERT INTO tc_user_geofence (userid, geofenceid) VALUES (2, 1);
INSERT INTO tc_user_geofence (userid, geofenceid) VALUES (2, 2);
INSERT INTO tc_user_geofence (userid, geofenceid) VALUES (2, 3);
INSERT INTO tc_user_geofence (userid, geofenceid) VALUES (2, 4);
INSERT INTO tc_user_geofence (userid, geofenceid) VALUES (3, 1);
INSERT INTO tc_user_geofence (userid, geofenceid) VALUES (3, 2);

-- Insert device-geofence relationships
INSERT INTO tc_device_geofence (deviceid, geofenceid) VALUES (1, 1);
INSERT INTO tc_device_geofence (deviceid, geofenceid) VALUES (1, 6);
INSERT INTO tc_device_geofence (deviceid, geofenceid) VALUES (2, 2);
INSERT INTO tc_device_geofence (deviceid, geofenceid) VALUES (3, 3);
INSERT INTO tc_device_geofence (deviceid, geofenceid) VALUES (4, 4);
INSERT INTO tc_device_geofence (deviceid, geofenceid) VALUES (5, 5);

-- Insert group-geofence relationships
INSERT INTO tc_group_geofence (groupid, geofenceid) VALUES (2, 1);
INSERT INTO tc_group_geofence (groupid, geofenceid) VALUES (2, 2);
INSERT INTO tc_group_geofence (groupid, geofenceid) VALUES (3, 3);
INSERT INTO tc_group_geofence (groupid, geofenceid) VALUES (3, 4);
INSERT INTO tc_group_geofence (groupid, geofenceid) VALUES (4, 5);

-- Insert user-notification relationships
INSERT INTO tc_user_notification (userid, notificationid) VALUES (1, 1);
INSERT INTO tc_user_notification (userid, notificationid) VALUES (1, 2);
INSERT INTO tc_user_notification (userid, notificationid) VALUES (1, 3);
INSERT INTO tc_user_notification (userid, notificationid) VALUES (1, 4);
INSERT INTO tc_user_notification (userid, notificationid) VALUES (1, 5);
INSERT INTO tc_user_notification (userid, notificationid) VALUES (2, 1);
INSERT INTO tc_user_notification (userid, notificationid) VALUES (2, 2);
INSERT INTO tc_user_notification (userid, notificationid) VALUES (2, 3);
INSERT INTO tc_user_notification (userid, notificationid) VALUES (3, 1);
INSERT INTO tc_user_notification (userid, notificationid) VALUES (3, 5);

-- Insert device-notification relationships
INSERT INTO tc_device_notification (deviceid, notificationid) VALUES (1, 1);
INSERT INTO tc_device_notification (deviceid, notificationid) VALUES (1, 2);
INSERT INTO tc_device_notification (deviceid, notificationid) VALUES (1, 3);
INSERT INTO tc_device_notification (deviceid, notificationid) VALUES (1, 5);
INSERT INTO tc_device_notification (deviceid, notificationid) VALUES (2, 1);
INSERT INTO tc_device_notification (deviceid, notificationid) VALUES (2, 2);
INSERT INTO tc_device_notification (deviceid, notificationid) VALUES (3, 2);
INSERT INTO tc_device_notification (deviceid, notificationid) VALUES (4, 2);
INSERT INTO tc_device_notification (deviceid, notificationid) VALUES (5, 1);
INSERT INTO tc_device_notification (deviceid, notificationid) VALUES (5, 5);

-- Insert group-notification relationships
INSERT INTO tc_group_notification (groupid, notificationid) VALUES (2, 1);
INSERT INTO tc_group_notification (groupid, notificationid) VALUES (2, 2);
INSERT INTO tc_group_notification (groupid, notificationid) VALUES (3, 2);
INSERT INTO tc_group_notification (groupid, notificationid) VALUES (4, 1);
INSERT INTO tc_group_notification (groupid, notificationid) VALUES (4, 5);

-- Insert user-command relationships
INSERT INTO tc_user_command (userid, commandid) VALUES (1, 1);
INSERT INTO tc_user_command (userid, commandid) VALUES (1, 2);
INSERT INTO tc_user_command (userid, commandid) VALUES (1, 3);
INSERT INTO tc_user_command (userid, commandid) VALUES (1, 4);
INSERT INTO tc_user_command (userid, commandid) VALUES (1, 5);
INSERT INTO tc_user_command (userid, commandid) VALUES (2, 1);
INSERT INTO tc_user_command (userid, commandid) VALUES (2, 2);
INSERT INTO tc_user_command (userid, commandid) VALUES (3, 1);

-- Insert device-command relationships
INSERT INTO tc_device_command (deviceid, commandid) VALUES (1, 1);
INSERT INTO tc_device_command (deviceid, commandid) VALUES (1, 2);
INSERT INTO tc_device_command (deviceid, commandid) VALUES (1, 3);
INSERT INTO tc_device_command (deviceid, commandid) VALUES (1, 4);
INSERT INTO tc_device_command (deviceid, commandid) VALUES (2, 1);
INSERT INTO tc_device_command (deviceid, commandid) VALUES (2, 2);
INSERT INTO tc_device_command (deviceid, commandid) VALUES (3, 1);
INSERT INTO tc_device_command (deviceid, commandid) VALUES (4, 1);
INSERT INTO tc_device_command (deviceid, commandid) VALUES (5, 1);
INSERT INTO tc_device_command (deviceid, commandid) VALUES (5, 5);

-- Insert group-command relationships
INSERT INTO tc_group_command (groupid, commandid) VALUES (2, 1);
INSERT INTO tc_group_command (groupid, commandid) VALUES (2, 2);
INSERT INTO tc_group_command (groupid, commandid) VALUES (3, 1);
INSERT INTO tc_group_command (groupid, commandid) VALUES (4, 1);
INSERT INTO tc_group_command (groupid, commandid) VALUES (4, 5);