-- Protocol Service Test Database Initialization Script
-- This script provides a combined schema and data initialization for Protocol Service testing
-- It ensures atomic database setup with transaction handling and error handling

-- Start transaction to ensure atomic database setup
BEGIN;

-- Error handling setup
\set ON_ERROR_STOP true

-- Schema creation for command-related tables

-- Commands table - stores command definitions
CREATE TABLE IF NOT EXISTS tc_commands (
    id SERIAL PRIMARY KEY,
    type VARCHAR(128) NOT NULL,
    description VARCHAR(4000) NOT NULL,
    attributes VARCHAR(4000),
    textchannel BOOLEAN DEFAULT false NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create index for command lookup by type
CREATE INDEX IF NOT EXISTS idx_commands_type ON tc_commands(type);

-- Device-Command association table
CREATE TABLE IF NOT EXISTS tc_device_command (
    deviceid INTEGER NOT NULL,
    commandid INTEGER NOT NULL,
    PRIMARY KEY (deviceid, commandid)
);

-- User-Command association table
CREATE TABLE IF NOT EXISTS tc_user_command (
    userid INTEGER NOT NULL,
    commandid INTEGER NOT NULL,
    PRIMARY KEY (userid, commandid)
);

-- Group-Command association table
CREATE TABLE IF NOT EXISTS tc_group_command (
    groupid INTEGER NOT NULL,
    commandid INTEGER NOT NULL,
    PRIMARY KEY (groupid, commandid)
);

-- Command queue table - stores pending commands
CREATE TABLE IF NOT EXISTS tc_command_queue (
    id SERIAL PRIMARY KEY,
    deviceid INTEGER NOT NULL,
    commandid INTEGER NOT NULL,
    parameters VARCHAR(4000),
    status VARCHAR(128) NOT NULL DEFAULT 'QUEUED',
    sent_at TIMESTAMP WITH TIME ZONE,
    executed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create index for command queue lookup by device and status
CREATE INDEX IF NOT EXISTS idx_command_queue_device ON tc_command_queue(deviceid);
CREATE INDEX IF NOT EXISTS idx_command_queue_status ON tc_command_queue(status);

-- Command templates table - stores protocol-specific command templates
CREATE TABLE IF NOT EXISTS tc_command_templates (
    id SERIAL PRIMARY KEY,
    protocol VARCHAR(128) NOT NULL,
    command_type VARCHAR(128) NOT NULL,
    template VARCHAR(4000) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create unique index for command templates by protocol and command type
CREATE UNIQUE INDEX IF NOT EXISTS idx_command_templates_protocol_type ON tc_command_templates(protocol, command_type);

-- Insert test data for command definitions
INSERT INTO tc_commands (id, type, description, attributes, textchannel)
VALUES 
(1, 'positionSingle', 'Request single position', NULL, false),
(2, 'engineStop', 'Stop engine', '{"duration":0}', false),
(3, 'engineResume', 'Resume engine', NULL, false),
(4, 'alarmArm', 'Arm alarm', NULL, false),
(5, 'alarmDisarm', 'Disarm alarm', NULL, false),
(6, 'setTimezone', 'Set timezone', '{"timezone":""}', false),
(7, 'requestPhoto', 'Request photo', NULL, false),
(8, 'powerOff', 'Power off device', NULL, false),
(9, 'rebootDevice', 'Reboot device', NULL, false),
(10, 'sendSms', 'Send SMS', '{"phone":"","message":""}', true),
(11, 'setSpeedLimit', 'Set speed limit', '{"speedLimit":0}', false),
(12, 'getVersion', 'Get device version', NULL, false),
(13, 'firmwareUpdate', 'Firmware update', '{"url":""}', false),
(14, 'setConnection', 'Set connection', '{"server":"","port":0}', false),
(15, 'setOdometer', 'Set odometer', '{"odometer":0}', false)
ON CONFLICT (id) DO UPDATE
SET type = EXCLUDED.type,
    description = EXCLUDED.description,
    attributes = EXCLUDED.attributes,
    textchannel = EXCLUDED.textchannel;

-- Insert test data for command templates
INSERT INTO tc_command_templates (protocol, command_type, template)
VALUES
('teltonika', 'positionSingle', 'getgps'),
('teltonika', 'engineStop', 'engine,0'),
('teltonika', 'engineResume', 'engine,1'),
('teltonika', 'rebootDevice', 'reboot'),
('coban', 'positionSingle', '**,123456789012345,100'),
('coban', 'engineStop', '**,123456789012345,A10'),
('coban', 'engineResume', '**,123456789012345,A11'),
('coban', 'rebootDevice', '**,123456789012345,991'),
('meitrack', 'positionSingle', 'W01,123456789012345,0'),
('meitrack', 'engineStop', 'S20,123456789012345,0,1234,0'),
('meitrack', 'engineResume', 'S20,123456789012345,0,1234,1'),
('meitrack', 'rebootDevice', 'F02,123456789012345,0'),
('tk103', 'positionSingle', '(123456789012345,A10)'),
('tk103', 'engineStop', '(123456789012345,DYD,1)'),
('tk103', 'engineResume', '(123456789012345,DYD,0)'),
('tk103', 'rebootDevice', '(123456789012345,A01)')
ON CONFLICT (protocol, command_type) DO UPDATE
SET template = EXCLUDED.template;

-- Insert test data for device-command associations
INSERT INTO tc_device_command (deviceid, commandid)
VALUES
(1, 1), (1, 2), (1, 3), (1, 9),
(2, 1), (2, 2), (2, 3), (2, 4), (2, 5), (2, 9),
(3, 1), (3, 7), (3, 9), (3, 12),
(4, 1), (4, 2), (4, 3), (4, 10), (4, 11),
(5, 1), (5, 6), (5, 9), (5, 13), (5, 14)
ON CONFLICT (deviceid, commandid) DO NOTHING;

-- Insert test data for user-command associations
INSERT INTO tc_user_command (userid, commandid)
VALUES
(1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (1, 7), (1, 8), (1, 9), (1, 10), (1, 11), (1, 12), (1, 13), (1, 14), (1, 15),
(2, 1), (2, 2), (2, 3), (2, 9),
(3, 1), (3, 10), (3, 12)
ON CONFLICT (userid, commandid) DO NOTHING;

-- Insert test data for group-command associations
INSERT INTO tc_group_command (groupid, commandid)
VALUES
(1, 1), (1, 2), (1, 3), (1, 9),
(2, 1), (2, 10), (2, 12)
ON CONFLICT (groupid, commandid) DO NOTHING;

-- Insert test data for command queue
INSERT INTO tc_command_queue (deviceid, commandid, parameters, status, sent_at, executed_at)
VALUES
(1, 1, NULL, 'COMPLETED', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '59 minutes'),
(1, 2, '{"duration":3600}', 'COMPLETED', NOW() - INTERVAL '2 hours', NOW() - INTERVAL '1 hour 59 minutes'),
(2, 1, NULL, 'QUEUED', NULL, NULL),
(3, 7, NULL, 'SENT', NOW() - INTERVAL '5 minutes', NULL),
(4, 2, '{"duration":1800}', 'FAILED', NOW() - INTERVAL '30 minutes', NULL),
(5, 9, NULL, 'QUEUED', NULL, NULL)
ON CONFLICT (id) DO NOTHING;

-- Commit transaction if all operations succeed
COMMIT;