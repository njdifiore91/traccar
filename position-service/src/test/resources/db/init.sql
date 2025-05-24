-- Position Service Database Initialization Script for Testing
-- This script provides a combined schema and data initialization for Position Service testing
-- It ensures atomic database setup through transaction handling

-- Start transaction for atomic operations
START TRANSACTION;

-- Note: This script is designed to be compatible with MySQL/MariaDB
-- For PostgreSQL or SQL Server, syntax adjustments may be needed

-- ----------------------------------------
-- Schema Creation Section
-- ----------------------------------------

-- Create positions table (primary table owned by Position Service)
CREATE TABLE IF NOT EXISTS tc_positions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    protocol VARCHAR(128),
    deviceid BIGINT NOT NULL,
    servertime TIMESTAMP NOT NULL,
    devicetime TIMESTAMP NOT NULL,
    fixtime TIMESTAMP NOT NULL,
    valid BOOLEAN NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    altitude DOUBLE DEFAULT 0,
    speed DOUBLE DEFAULT 0,
    course DOUBLE DEFAULT 0,
    address VARCHAR(512),
    accuracy DOUBLE DEFAULT 0,
    network VARCHAR(4096),
    attributes VARCHAR(4096),
    FOREIGN KEY (deviceid) REFERENCES tc_devices(id) ON DELETE CASCADE
);

-- Create position_attributes table for extended position data
CREATE TABLE IF NOT EXISTS tc_position_attributes (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    positionid BIGINT NOT NULL,
    attribute VARCHAR(128) NOT NULL,
    value VARCHAR(512),
    FOREIGN KEY (positionid) REFERENCES tc_positions(id) ON DELETE CASCADE
);

-- Create devices table reference (owned by API Gateway but referenced by Position Service)
-- Note: In production, this would be accessed via API or read replica
CREATE TABLE IF NOT EXISTS tc_devices (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    uniqueid VARCHAR(128) NOT NULL,
    lastupdate TIMESTAMP,
    positionid BIGINT,
    groupid BIGINT,
    attributes VARCHAR(4096),
    phone VARCHAR(128),
    model VARCHAR(128),
    contact VARCHAR(512),
    category VARCHAR(128),
    disabled BOOLEAN DEFAULT FALSE NOT NULL,
    UNIQUE (uniqueid)
);

-- Create device_attributes table for extended device data
CREATE TABLE IF NOT EXISTS tc_device_attributes (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    deviceid BIGINT NOT NULL,
    attribute VARCHAR(128) NOT NULL,
    value VARCHAR(512),
    FOREIGN KEY (deviceid) REFERENCES tc_devices(id) ON DELETE CASCADE
);

-- Create groups table reference (owned by API Gateway but referenced by Position Service)
CREATE TABLE IF NOT EXISTS tc_groups (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    groupid BIGINT,
    attributes VARCHAR(4096),
    FOREIGN KEY (groupid) REFERENCES tc_groups(id) ON DELETE CASCADE
);

-- Create users table reference (owned by API Gateway but referenced by Position Service)
CREATE TABLE IF NOT EXISTS tc_users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    email VARCHAR(128) NOT NULL UNIQUE,
    hashedpassword VARCHAR(128),
    salt VARCHAR(128),
    readonly BOOLEAN DEFAULT FALSE NOT NULL,
    administrator BOOLEAN DEFAULT FALSE NOT NULL,
    map VARCHAR(128),
    latitude DOUBLE DEFAULT 0,
    longitude DOUBLE DEFAULT 0,
    zoom INT DEFAULT 0,
    twelvehourformat BOOLEAN DEFAULT FALSE NOT NULL,
    attributes VARCHAR(4096),
    coordinateformat VARCHAR(128),
    disabled BOOLEAN DEFAULT FALSE NOT NULL,
    expirationtime TIMESTAMP,
    devicelimit INT DEFAULT -1 NOT NULL,
    userlimit INT DEFAULT 0 NOT NULL,
    devicereadonly BOOLEAN DEFAULT FALSE NOT NULL,
    phone VARCHAR(128),
    limitcommands BOOLEAN DEFAULT FALSE NOT NULL,
    login VARCHAR(128),
    poilayer VARCHAR(512)
);

-- Create user_device table for user-device associations
CREATE TABLE IF NOT EXISTS tc_user_device (
    userid BIGINT NOT NULL,
    deviceid BIGINT NOT NULL,
    PRIMARY KEY (userid, deviceid),
    FOREIGN KEY (userid) REFERENCES tc_users(id) ON DELETE CASCADE,
    FOREIGN KEY (deviceid) REFERENCES tc_devices(id) ON DELETE CASCADE
);

-- Create user_group table for user-group associations
CREATE TABLE IF NOT EXISTS tc_user_group (
    userid BIGINT NOT NULL,
    groupid BIGINT NOT NULL,
    PRIMARY KEY (userid, groupid),
    FOREIGN KEY (userid) REFERENCES tc_users(id) ON DELETE CASCADE,
    FOREIGN KEY (groupid) REFERENCES tc_groups(id) ON DELETE CASCADE
);

-- Create group_device table for group-device associations
CREATE TABLE IF NOT EXISTS tc_group_device (
    groupid BIGINT NOT NULL,
    deviceid BIGINT NOT NULL,
    PRIMARY KEY (groupid, deviceid),
    FOREIGN KEY (groupid) REFERENCES tc_groups(id) ON DELETE CASCADE,
    FOREIGN KEY (deviceid) REFERENCES tc_devices(id) ON DELETE CASCADE
);

-- ----------------------------------------
-- Index Creation Section
-- ----------------------------------------

-- Create indexes for optimized queries
CREATE INDEX IF NOT EXISTS position_deviceid_fixtime ON tc_positions(deviceid, fixtime);
CREATE INDEX IF NOT EXISTS position_deviceid_servertime ON tc_positions(deviceid, servertime);
CREATE INDEX IF NOT EXISTS position_attributes_positionid ON tc_position_attributes(positionid);
CREATE INDEX IF NOT EXISTS device_attributes_deviceid ON tc_device_attributes(deviceid);
CREATE INDEX IF NOT EXISTS idx_devices_uniqueid ON tc_devices(uniqueid);
CREATE INDEX IF NOT EXISTS user_device_user_id ON tc_user_device(userid);
CREATE INDEX IF NOT EXISTS user_device_device_id ON tc_user_device(deviceid);
CREATE INDEX IF NOT EXISTS user_group_user_id ON tc_user_group(userid);
CREATE INDEX IF NOT EXISTS user_group_group_id ON tc_user_group(groupid);
CREATE INDEX IF NOT EXISTS group_device_group_id ON tc_group_device(groupid);
CREATE INDEX IF NOT EXISTS group_device_device_id ON tc_group_device(deviceid);

-- ----------------------------------------
-- Test Data Insertion Section
-- ----------------------------------------

-- Insert test users
INSERT INTO tc_users (id, name, email, hashedpassword, salt, administrator)
VALUES 
    (1, 'admin', 'admin@test.org', 'D2D8C1D1646B8642A7F93BEF9F989D6E', 'A6DAB80C', TRUE),
    (2, 'test', 'test@test.org', 'D2D8C1D1646B8642A7F93BEF9F989D6E', 'A6DAB80C', FALSE);

-- Insert test groups
INSERT INTO tc_groups (id, name)
VALUES 
    (1, 'Test Group 1'),
    (2, 'Test Group 2');

-- Insert test devices
INSERT INTO tc_devices (id, name, uniqueid, lastupdate)
VALUES 
    (1, 'Test Device 1', '123456789012345', NOW()),
    (2, 'Test Device 2', '123456789012346', NOW()),
    (3, 'Test Device 3', '123456789012347', NOW());

-- Associate users with devices
INSERT INTO tc_user_device (userid, deviceid)
VALUES 
    (1, 1),
    (1, 2),
    (1, 3),
    (2, 2);

-- Associate users with groups
INSERT INTO tc_user_group (userid, groupid)
VALUES 
    (1, 1),
    (1, 2),
    (2, 1);

-- Associate groups with devices
INSERT INTO tc_group_device (groupid, deviceid)
VALUES 
    (1, 1),
    (1, 2),
    (2, 3);

-- Insert test positions for Device 1
INSERT INTO tc_positions (id, protocol, deviceid, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, attributes)
VALUES 
    (1, 'tcp', 1, DATE_SUB(NOW(), INTERVAL 1 HOUR), DATE_SUB(NOW(), INTERVAL 1 HOUR), DATE_SUB(NOW(), INTERVAL 1 HOUR), TRUE, 40.7128, -74.0060, 10.0, 50.0, 90.0, '{"batteryLevel":95,"fuel":75,"ignition":true}'),
    (2, 'tcp', 1, DATE_SUB(NOW(), INTERVAL 50 MINUTE), DATE_SUB(NOW(), INTERVAL 50 MINUTE), DATE_SUB(NOW(), INTERVAL 50 MINUTE), TRUE, 40.7130, -74.0065, 12.0, 45.0, 92.0, '{"batteryLevel":94,"fuel":73,"ignition":true}'),
    (3, 'tcp', 1, DATE_SUB(NOW(), INTERVAL 40 MINUTE), DATE_SUB(NOW(), INTERVAL 40 MINUTE), DATE_SUB(NOW(), INTERVAL 40 MINUTE), TRUE, 40.7135, -74.0070, 11.0, 40.0, 95.0, '{"batteryLevel":93,"fuel":72,"ignition":true}');

-- Insert test positions for Device 2
INSERT INTO tc_positions (id, protocol, deviceid, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, attributes)
VALUES 
    (4, 'tcp', 2, DATE_SUB(NOW(), INTERVAL 1 HOUR), DATE_SUB(NOW(), INTERVAL 1 HOUR), DATE_SUB(NOW(), INTERVAL 1 HOUR), TRUE, 34.0522, -118.2437, 15.0, 30.0, 180.0, '{"batteryLevel":85,"fuel":65,"ignition":true}'),
    (5, 'tcp', 2, DATE_SUB(NOW(), INTERVAL 45 MINUTE), DATE_SUB(NOW(), INTERVAL 45 MINUTE), DATE_SUB(NOW(), INTERVAL 45 MINUTE), TRUE, 34.0525, -118.2440, 16.0, 32.0, 182.0, '{"batteryLevel":84,"fuel":64,"ignition":true}'),
    (6, 'tcp', 2, DATE_SUB(NOW(), INTERVAL 30 MINUTE), DATE_SUB(NOW(), INTERVAL 30 MINUTE), DATE_SUB(NOW(), INTERVAL 30 MINUTE), TRUE, 34.0530, -118.2445, 14.0, 0.0, 182.0, '{"batteryLevel":83,"fuel":63,"ignition":false}');

-- Insert test positions for Device 3
INSERT INTO tc_positions (id, protocol, deviceid, servertime, devicetime, fixtime, valid, latitude, longitude, altitude, speed, course, attributes)
VALUES 
    (7, 'tcp', 3, DATE_SUB(NOW(), INTERVAL 2 HOUR), DATE_SUB(NOW(), INTERVAL 2 HOUR), DATE_SUB(NOW(), INTERVAL 2 HOUR), TRUE, 51.5074, -0.1278, 20.0, 25.0, 270.0, '{"batteryLevel":75,"fuel":55,"ignition":true}'),
    (8, 'tcp', 3, DATE_SUB(NOW(), INTERVAL 1 HOUR), DATE_SUB(NOW(), INTERVAL 1 HOUR), DATE_SUB(NOW(), INTERVAL 1 HOUR), TRUE, 51.5080, -0.1280, 22.0, 27.0, 272.0, '{"batteryLevel":73,"fuel":53,"ignition":true}'),
    (9, 'tcp', 3, DATE_SUB(NOW(), INTERVAL 30 MINUTE), DATE_SUB(NOW(), INTERVAL 30 MINUTE), DATE_SUB(NOW(), INTERVAL 30 MINUTE), FALSE, 0.0, 0.0, 0.0, 0.0, 0.0, '{"batteryLevel":70,"fuel":52,"ignition":false,"reason":"No GPS signal"}');

-- Insert position attributes for extended testing
INSERT INTO tc_position_attributes (positionid, attribute, value)
VALUES 
    (1, 'temperature', '25.5'),
    (1, 'odometer', '12500'),
    (2, 'temperature', '26.0'),
    (2, 'odometer', '12510'),
    (3, 'temperature', '26.2'),
    (3, 'odometer', '12525'),
    (4, 'temperature', '30.0'),
    (4, 'odometer', '8700'),
    (5, 'temperature', '30.5'),
    (5, 'odometer', '8715'),
    (6, 'temperature', '29.0'),
    (6, 'odometer', '8720'),
    (7, 'temperature', '15.0'),
    (7, 'odometer', '5200'),
    (8, 'temperature', '15.5'),
    (8, 'odometer', '5225'),
    (9, 'temperature', '14.0'),
    (9, 'odometer', '5225');

-- Update devices with latest position IDs
UPDATE tc_devices SET positionid = 3 WHERE id = 1;
UPDATE tc_devices SET positionid = 6 WHERE id = 2;
UPDATE tc_devices SET positionid = 9 WHERE id = 3;

-- Insert device attributes for extended testing
INSERT INTO tc_device_attributes (deviceid, attribute, value)
VALUES 
    (1, 'maintenance-interval', '10000'),
    (1, 'fuel-capacity', '100'),
    (2, 'maintenance-interval', '15000'),
    (2, 'fuel-capacity', '80'),
    (3, 'maintenance-interval', '12000'),
    (3, 'fuel-capacity', '90');

-- Commit transaction if no errors
COMMIT;

-- Note on error handling:
-- In a real implementation, database-specific error handling would be added here
-- For MySQL/MariaDB, you might use:
--   DELIMITER //
--   CREATE PROCEDURE init_with_error_handling()
--   BEGIN
--     DECLARE EXIT HANDLER FOR SQLEXCEPTION
--     BEGIN
--       ROLLBACK;
--       SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Database initialization failed';
--     END;
--     
--     START TRANSACTION;
--     -- All the SQL statements would go here
--     COMMIT;
--   END //
--   DELIMITER ;
--   CALL init_with_error_handling();
--
-- For PostgreSQL, you might use:
--   DO $$
--   BEGIN
--     -- All the SQL statements would go here
--   EXCEPTION WHEN OTHERS THEN
--     ROLLBACK;
--     RAISE EXCEPTION 'Database initialization failed: %', SQLERRM;
--   END $$;