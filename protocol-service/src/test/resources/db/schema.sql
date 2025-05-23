-- Protocol Service Test Database Schema
-- Contains command-related tables owned by the Protocol Service

-- Drop tables if they exist to ensure clean initialization
DROP TABLE IF EXISTS tc_command_history;
DROP TABLE IF EXISTS tc_command_queue;
DROP TABLE IF EXISTS tc_device_command;
DROP TABLE IF EXISTS tc_user_command;
DROP TABLE IF EXISTS tc_group_command;
DROP TABLE IF EXISTS tc_commands;

-- Commands table - stores device command definitions
CREATE TABLE tc_commands (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    description VARCHAR(4000) NOT NULL,
    type VARCHAR(128) NOT NULL,
    textchannel BOOLEAN NOT NULL DEFAULT false,
    attributes VARCHAR(4000) NOT NULL
);

-- Device command mapping - links commands to specific devices
CREATE TABLE tc_device_command (
    deviceid BIGINT NOT NULL,
    commandid BIGINT NOT NULL,
    PRIMARY KEY (deviceid, commandid),
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

-- User command mapping - links commands to specific users
CREATE TABLE tc_user_command (
    userid BIGINT NOT NULL,
    commandid BIGINT NOT NULL,
    PRIMARY KEY (userid, commandid),
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

-- Group command mapping - links commands to device groups
CREATE TABLE tc_group_command (
    groupid BIGINT NOT NULL,
    commandid BIGINT NOT NULL,
    PRIMARY KEY (groupid, commandid),
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

-- Command queue - tracks command execution status
CREATE TABLE tc_command_queue (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    deviceid BIGINT NOT NULL,
    commandid BIGINT NOT NULL,
    queue_time TIMESTAMP NOT NULL,
    sent_time TIMESTAMP NULL DEFAULT NULL,
    executed_time TIMESTAMP NULL DEFAULT NULL,
    status VARCHAR(128) NULL DEFAULT NULL,
    attributes VARCHAR(4000) NOT NULL,
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

-- Command history - stores historical record of executed commands
CREATE TABLE tc_command_history (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    deviceid BIGINT NOT NULL,
    commandid BIGINT NOT NULL,
    execution_time TIMESTAMP NOT NULL,
    success BOOLEAN NOT NULL DEFAULT false,
    response VARCHAR(4000) NULL,
    attributes VARCHAR(4000) NOT NULL,
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

-- Create indexes for optimized command lookup
CREATE INDEX idx_command_type ON tc_commands(type);
CREATE INDEX idx_device_command_deviceid ON tc_device_command(deviceid);
CREATE INDEX idx_user_command_userid ON tc_user_command(userid);
CREATE INDEX idx_group_command_groupid ON tc_group_command(groupid);
CREATE INDEX idx_command_queue_deviceid ON tc_command_queue(deviceid);
CREATE INDEX idx_command_queue_status ON tc_command_queue(status);
CREATE INDEX idx_command_queue_times ON tc_command_queue(queue_time, sent_time, executed_time);
CREATE INDEX idx_command_history_deviceid ON tc_command_history(deviceid);
CREATE INDEX idx_command_history_execution_time ON tc_command_history(execution_time);
CREATE INDEX idx_command_history_success ON tc_command_history(success);