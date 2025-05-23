-- H2-specific database schema for in-memory testing of the Protocol Service
-- Contains H2-compatible DDL statements for creating command-related tables
-- This schema is optimized for H2 dialect and in-memory testing

-- Drop existing tables if they exist to ensure clean setup
-- Drop in reverse order of dependencies to avoid constraint violations
DROP TABLE IF EXISTS tc_command_parameters;
DROP TABLE IF EXISTS tc_command_queue;
DROP TABLE IF EXISTS tc_device_command;
DROP TABLE IF EXISTS tc_group_command;
DROP TABLE IF EXISTS tc_user_command;
DROP TABLE IF EXISTS tc_protocol_command_templates;
DROP TABLE IF EXISTS tc_commands;

-- Create commands table
-- Stores command definitions that can be sent to devices
CREATE TABLE tc_commands (
    id INT AUTO_INCREMENT PRIMARY KEY,
    description VARCHAR(4000),                  -- Human-readable description of the command
    type VARCHAR(128) NOT NULL,                 -- Command type identifier (e.g., positionPeriodic, engineStop)
    textchannel BOOLEAN DEFAULT FALSE NOT NULL, -- Whether command uses text channel for transmission
    attributes VARCHAR(4000),                   -- JSON-formatted command parameters and attributes
    uniquehash VARCHAR(128),                    -- Optional hash for command deduplication
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Command creation timestamp
);

-- Create protocol-specific command templates
-- Stores protocol-specific formatting templates for commands
CREATE TABLE tc_protocol_command_templates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    protocol VARCHAR(128) NOT NULL,             -- Protocol identifier (e.g., teltonika, meitrack)
    commandtype VARCHAR(128) NOT NULL,          -- Command type this template applies to
    template VARCHAR(4000) NOT NULL,            -- Template string with parameter placeholders
    binary BOOLEAN DEFAULT FALSE NOT NULL,      -- Whether this is a binary command format
    UNIQUE KEY protocol_command_unique (protocol, commandtype)
);

-- Create user-command association table
-- Links users to commands they are allowed to execute
CREATE TABLE tc_user_command (
    userid INT NOT NULL,                        -- Reference to user ID
    commandid INT NOT NULL,                     -- Reference to command ID
    PRIMARY KEY (userid, commandid),            -- Composite primary key
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

-- Create group-command association table
-- Links groups to commands that can be executed on all devices in the group
CREATE TABLE tc_group_command (
    groupid INT NOT NULL,                       -- Reference to group ID
    commandid INT NOT NULL,                     -- Reference to command ID
    PRIMARY KEY (groupid, commandid),           -- Composite primary key
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

-- Create device-command association table
-- Links devices to commands that can be executed on them
CREATE TABLE tc_device_command (
    deviceid INT NOT NULL,                      -- Reference to device ID
    commandid INT NOT NULL,                     -- Reference to command ID
    PRIMARY KEY (deviceid, commandid),          -- Composite primary key
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

-- Create command execution queue table
-- Tracks command execution attempts and their results
CREATE TABLE tc_command_queue (
    id INT AUTO_INCREMENT PRIMARY KEY,
    deviceid INT NOT NULL,                      -- Target device ID
    commandid INT NOT NULL,                     -- Command being executed
    userid INT,                                 -- User who initiated the command (if any)
    status VARCHAR(20) NOT NULL,                -- Status: QUEUED, SENT, DELIVERED, FAILED
    sent TIMESTAMP,                             -- When the command was sent
    delivered TIMESTAMP,                        -- When the command was confirmed delivered
    result VARCHAR(4000),                       -- Command execution result or error message
    protocol VARCHAR(128),                      -- Protocol used to send the command
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When the command was queued
    FOREIGN KEY (commandid) REFERENCES tc_commands(id)
);

-- Create command parameters table
-- Stores parameter values for command execution
CREATE TABLE tc_command_parameters (
    id INT AUTO_INCREMENT PRIMARY KEY,
    queueid INT NOT NULL,                       -- Reference to command queue entry
    name VARCHAR(128) NOT NULL,                 -- Parameter name
    value VARCHAR(4000),                        -- Parameter value
    FOREIGN KEY (queueid) REFERENCES tc_command_queue(id) ON DELETE CASCADE
);

-- Create indexes for faster lookups
CREATE INDEX idx_command_type ON tc_commands(type);
CREATE INDEX idx_command_queue_device ON tc_command_queue(deviceid);
CREATE INDEX idx_command_queue_status ON tc_command_queue(status);
CREATE INDEX idx_command_queue_created ON tc_command_queue(created);
CREATE INDEX idx_user_command_user ON tc_user_command(userid);
CREATE INDEX idx_group_command_group ON tc_group_command(groupid);
CREATE INDEX idx_device_command_device ON tc_device_command(deviceid);
CREATE INDEX idx_protocol_template ON tc_protocol_command_templates(protocol, commandtype);
CREATE INDEX idx_command_parameters_queue ON tc_command_parameters(queueid);

-- Insert sample command definitions for testing
-- These are common command types supported across multiple protocols
INSERT INTO tc_commands (description, type, textchannel, attributes) VALUES
('Request single position update', 'positionSingle', false, '{"timeout": 60}'),
('Configure position update interval', 'positionPeriodic', false, '{"frequency": 60}'),
('Engine stop command', 'engineStop', false, '{"type": "engineControl", "action": "stop"}'),
('Engine resume command', 'engineResume', false, '{"type": "engineControl", "action": "resume"}'),
('Set device timezone', 'setTimezone', true, '{"timezone": "UTC"}'),
('Reboot device', 'deviceReboot', false, '{"delaySeconds": 5}'),
('Custom command', 'custom', true, '{"allowUserInput": true}');

-- Insert protocol-specific command templates
-- These templates show how commands are formatted for different protocols
INSERT INTO tc_protocol_command_templates (protocol, commandtype, template, binary) VALUES
('teltonika', 'positionSingle', 'getgps', false),
('teltonika', 'positionPeriodic', 'setgprs:${frequency}', false),
('teltonika', 'engineStop', 'setdigout:1:1', false),
('teltonika', 'engineResume', 'setdigout:1:0', false),
('meitrack', 'positionSingle', 'W01,', false),
('meitrack', 'positionPeriodic', 'W71,${frequency}', false),
('meitrack', 'engineStop', 'W30,1,1', false),
('meitrack', 'engineResume', 'W30,1,0', false),
('coban', 'positionSingle', '**,123456789012345,100', false),
('coban', 'engineStop', '**,123456789012345,J01,1#', false),
('coban', 'engineResume', '**,123456789012345,J01,0#', false),
('coban', 'setTimezone', '**,123456789012345,B00,${timezone}#', false),
('h02', 'positionSingle', '*HQ,123456789012345,LOC#', false),
('h02', 'positionPeriodic', '*HQ,123456789012345,S71,${frequency}#', false);