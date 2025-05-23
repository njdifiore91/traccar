-- Notification Service Test Database Initialization Script
-- This script sets up the schema and test data for notification service integration tests

-- Clear existing test data if present
DELETE FROM tc_notifications WHERE id >= 1000;
DELETE FROM tc_notification_templates WHERE id >= 1000;
DELETE FROM tc_notification_deliveries WHERE id >= 1000;

-- Create notification tables if they don't exist

-- Notification configuration table
CREATE TABLE IF NOT EXISTS tc_notifications (
    id INT PRIMARY KEY AUTO_INCREMENT,
    type VARCHAR(128) NOT NULL,
    always BOOLEAN NOT NULL DEFAULT false,
    notificators VARCHAR(128),
    calendarid INT,
    attributes VARCHAR(4000),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Notification templates table
CREATE TABLE IF NOT EXISTS tc_notification_templates (
    id INT PRIMARY KEY AUTO_INCREMENT,
    type VARCHAR(128) NOT NULL,
    channel VARCHAR(64) NOT NULL,
    subject VARCHAR(512),
    body TEXT NOT NULL,
    language VARCHAR(10) DEFAULT 'en',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Notification delivery tracking table
CREATE TABLE IF NOT EXISTS tc_notification_deliveries (
    id INT PRIMARY KEY AUTO_INCREMENT,
    notification_id INT NOT NULL,
    user_id INT NOT NULL,
    device_id INT,
    event_id INT,
    channel VARCHAR(64) NOT NULL,
    destination VARCHAR(512) NOT NULL,
    status VARCHAR(32) NOT NULL,
    status_message VARCHAR(1024),
    sent_at TIMESTAMP,
    delivered_at TIMESTAMP,
    correlation_id VARCHAR(128),
    retry_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (notification_id) REFERENCES tc_notifications(id) ON DELETE CASCADE
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_notification_type ON tc_notifications(type);
CREATE INDEX IF NOT EXISTS idx_notification_template_type_channel ON tc_notification_templates(type, channel);
CREATE INDEX IF NOT EXISTS idx_notification_delivery_status ON tc_notification_deliveries(status);
CREATE INDEX IF NOT EXISTS idx_notification_delivery_user ON tc_notification_deliveries(user_id);
CREATE INDEX IF NOT EXISTS idx_notification_delivery_device ON tc_notification_deliveries(device_id);

-- Insert test data for notification configurations

-- Device online/offline notifications
INSERT INTO tc_notifications (id, type, always, notificators, attributes)
VALUES 
(1000, 'deviceOnline', true, 'web,mail', '{"webhookUrl":"","customMessage":"Device is now online"}'),
(1001, 'deviceOffline', true, 'web,mail,sms', '{"webhookUrl":"","customMessage":"Device has gone offline"}');

-- Geofence notifications
INSERT INTO tc_notifications (id, type, always, notificators, attributes)
VALUES 
(1002, 'geofenceEnter', false, 'web,mail,push', '{"webhookUrl":"","customMessage":"Device has entered geofence"}'),
(1003, 'geofenceExit', false, 'web,mail,push', '{"webhookUrl":"","customMessage":"Device has exited geofence"}');

-- Alarm notifications
INSERT INTO tc_notifications (id, type, always, notificators, attributes)
VALUES 
(1004, 'alarm', true, 'web,mail,sms,push,telegram', '{"webhookUrl":"","customMessage":"Alarm triggered on device"}');

-- Overspeed notifications
INSERT INTO tc_notifications (id, type, always, notificators, attributes)
VALUES 
(1005, 'deviceOverspeed', false, 'web,mail,sms', '{"webhookUrl":"","customMessage":"Device exceeded speed limit"}');

-- Fuel drop notifications
INSERT INTO tc_notifications (id, type, always, notificators, attributes)
VALUES 
(1006, 'fuelDrop', false, 'web,mail,push', '{"webhookUrl":"","customMessage":"Fuel level has dropped"}');

-- Command result notifications
INSERT INTO tc_notifications (id, type, always, notificators, attributes)
VALUES 
(1007, 'commandResult', true, 'web,mail', '{"webhookUrl":"","customMessage":"Command execution result"}');

-- Insert test data for notification templates

-- Email templates
INSERT INTO tc_notification_templates (id, type, channel, subject, body, language)
VALUES 
(1000, 'deviceOnline', 'mail', 'Device {device.name} is now online', 'Your device {device.name} (ID: {device.uniqueId}) is now online at {event.serverTime}.\n\nCurrent position: {position.latitude}, {position.longitude}\nCurrent address: {position.address}', 'en'),
(1001, 'deviceOffline', 'mail', 'Device {device.name} has gone offline', 'Your device {device.name} (ID: {device.uniqueId}) has gone offline at {event.serverTime}.\n\nLast known position: {position.latitude}, {position.longitude}\nLast known address: {position.address}', 'en'),
(1002, 'geofenceEnter', 'mail', 'Device {device.name} has entered geofence {geofence.name}', 'Your device {device.name} has entered geofence {geofence.name} at {event.serverTime}.\n\nPosition: {position.latitude}, {position.longitude}\nAddress: {position.address}', 'en'),
(1003, 'geofenceExit', 'mail', 'Device {device.name} has exited geofence {geofence.name}', 'Your device {device.name} has exited geofence {geofence.name} at {event.serverTime}.\n\nPosition: {position.latitude}, {position.longitude}\nAddress: {position.address}', 'en'),
(1004, 'alarm', 'mail', 'ALARM: {device.name} - {event.type}', 'ALARM NOTIFICATION\n\nDevice: {device.name}\nTime: {event.serverTime}\nType: {event.type}\nPosition: {position.latitude}, {position.longitude}\nAddress: {position.address}', 'en'),
(1005, 'deviceOverspeed', 'mail', 'Overspeed: {device.name}', 'Your device {device.name} has exceeded the speed limit.\n\nSpeed: {position.speed} km/h\nLimit: {speedLimit} km/h\nTime: {event.serverTime}\nPosition: {position.latitude}, {position.longitude}\nAddress: {position.address}', 'en');

-- SMS templates
INSERT INTO tc_notification_templates (id, type, channel, subject, body, language)
VALUES 
(1006, 'deviceOffline', 'sms', NULL, 'Device {device.name} offline since {event.serverTime}', 'en'),
(1007, 'alarm', 'sms', NULL, 'ALARM: {device.name} - {event.type} at {event.serverTime}', 'en'),
(1008, 'deviceOverspeed', 'sms', NULL, 'Overspeed: {device.name} at {position.speed} km/h (limit: {speedLimit})', 'en');

-- Push notification templates
INSERT INTO tc_notification_templates (id, type, channel, subject, body, language)
VALUES 
(1009, 'geofenceEnter', 'push', 'Geofence entered', '{device.name} entered {geofence.name}', 'en'),
(1010, 'geofenceExit', 'push', 'Geofence exited', '{device.name} exited {geofence.name}', 'en'),
(1011, 'alarm', 'push', 'Alarm', '{device.name}: {event.type}', 'en'),
(1012, 'fuelDrop', 'push', 'Fuel drop detected', '{device.name}: Fuel level dropped by {event.attributes.drop}%', 'en');

-- Web notification templates
INSERT INTO tc_notification_templates (id, type, channel, subject, body, language)
VALUES 
(1013, 'deviceOnline', 'web', 'Device online', '{device.name} is now online', 'en'),
(1014, 'deviceOffline', 'web', 'Device offline', '{device.name} has gone offline', 'en'),
(1015, 'geofenceEnter', 'web', 'Geofence entered', '{device.name} entered {geofence.name}', 'en'),
(1016, 'geofenceExit', 'web', 'Geofence exited', '{device.name} exited {geofence.name}', 'en'),
(1017, 'alarm', 'web', 'Alarm', '{device.name}: {event.type}', 'en'),
(1018, 'deviceOverspeed', 'web', 'Overspeed', '{device.name} exceeded speed limit ({position.speed} km/h)', 'en'),
(1019, 'fuelDrop', 'web', 'Fuel drop', '{device.name}: Fuel level dropped by {event.attributes.drop}%', 'en'),
(1020, 'commandResult', 'web', 'Command result', '{device.name}: Command {command.type} - {command.result}', 'en');

-- Telegram notification templates
INSERT INTO tc_notification_templates (id, type, channel, subject, body, language)
VALUES 
(1021, 'alarm', 'telegram', NULL, '🚨 *ALARM*\n*Device:* {device.name}\n*Type:* {event.type}\n*Time:* {event.serverTime}\n*Location:* {position.address}', 'en');

-- Insert test data for notification deliveries

-- Successful deliveries
INSERT INTO tc_notification_deliveries (id, notification_id, user_id, device_id, event_id, channel, destination, status, sent_at, delivered_at, correlation_id, retry_count)
VALUES 
(1000, 1000, 1, 1, 1, 'mail', 'test@example.com', 'delivered', '2023-01-01 10:00:00', '2023-01-01 10:00:05', 'corr-id-1000', 0),
(1001, 1001, 1, 2, 2, 'mail', 'test@example.com', 'delivered', '2023-01-01 11:00:00', '2023-01-01 11:00:03', 'corr-id-1001', 0),
(1002, 1002, 2, 3, 3, 'push', 'device-token-123', 'delivered', '2023-01-01 12:00:00', '2023-01-01 12:00:01', 'corr-id-1002', 0),
(1003, 1004, 1, 1, 4, 'sms', '+1234567890', 'delivered', '2023-01-01 13:00:00', '2023-01-01 13:00:10', 'corr-id-1003', 0),
(1004, 1004, 2, 1, 4, 'telegram', '123456789', 'delivered', '2023-01-01 13:00:00', '2023-01-01 13:00:02', 'corr-id-1004', 0);

-- Failed deliveries
INSERT INTO tc_notification_deliveries (id, notification_id, user_id, device_id, event_id, channel, destination, status, status_message, sent_at, correlation_id, retry_count)
VALUES 
(1005, 1001, 3, 4, 5, 'mail', 'invalid@example', 'failed', 'Invalid email address', '2023-01-01 14:00:00', 'corr-id-1005', 3),
(1006, 1001, 3, 4, 5, 'sms', '+9876543210', 'failed', 'SMS gateway error: Rate limit exceeded', '2023-01-01 14:00:00', 'corr-id-1006', 5),
(1007, 1005, 1, 5, 6, 'push', 'device-token-456', 'failed', 'Invalid device token', '2023-01-01 15:00:00', 'corr-id-1007', 1);

-- Pending deliveries
INSERT INTO tc_notification_deliveries (id, notification_id, user_id, device_id, event_id, channel, destination, status, sent_at, correlation_id, retry_count)
VALUES 
(1008, 1006, 2, 6, 7, 'mail', 'pending@example.com', 'pending', '2023-01-01 16:00:00', 'corr-id-1008', 0),
(1009, 1007, 1, 1, 8, 'web', 'session-id-123', 'pending', '2023-01-01 17:00:00', 'corr-id-1009', 0);

-- Retrying deliveries
INSERT INTO tc_notification_deliveries (id, notification_id, user_id, device_id, event_id, channel, destination, status, status_message, sent_at, correlation_id, retry_count)
VALUES 
(1010, 1004, 3, 7, 9, 'mail', 'retry@example.com', 'retrying', 'Temporary server failure', '2023-01-01 18:00:00', 'corr-id-1010', 2);

-- Create user-notification links for testing permission checks
CREATE TABLE IF NOT EXISTS tc_user_notification (
    userid INT NOT NULL,
    notificationid INT NOT NULL,
    PRIMARY KEY (userid, notificationid),
    FOREIGN KEY (notificationid) REFERENCES tc_notifications(id) ON DELETE CASCADE
);

INSERT INTO tc_user_notification (userid, notificationid)
VALUES 
(1, 1000),
(1, 1001),
(1, 1004),
(1, 1005),
(1, 1007),
(2, 1002),
(2, 1003),
(2, 1004),
(2, 1006),
(3, 1001),
(3, 1004);

-- Create notification queue table for testing message processing
CREATE TABLE IF NOT EXISTS tc_notification_queue (
    id INT PRIMARY KEY AUTO_INCREMENT,
    event_id INT NOT NULL,
    notification_id INT NOT NULL,
    user_id INT NOT NULL,
    device_id INT,
    status VARCHAR(32) NOT NULL DEFAULT 'queued',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    FOREIGN KEY (notification_id) REFERENCES tc_notifications(id) ON DELETE CASCADE
);

INSERT INTO tc_notification_queue (id, event_id, notification_id, user_id, device_id, status)
VALUES 
(1000, 10, 1000, 1, 1, 'queued'),
(1001, 11, 1001, 1, 2, 'queued'),
(1002, 12, 1002, 2, 3, 'processing'),
(1003, 13, 1004, 1, 1, 'processed'),
(1004, 14, 1005, 3, 5, 'failed');