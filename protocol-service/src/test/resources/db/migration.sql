-- Protocol Service Test Migration Script
-- This script simulates database schema migrations for testing purposes
-- It contains sample migration statements that allow tests to verify the Protocol Service's
-- ability to handle database schema evolution correctly

-- Version 1.0.0: Initial Schema Creation
-- Simulates the initial creation of command-related tables owned by the Protocol Service

CREATE TABLE IF NOT EXISTS tc_commands (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    description VARCHAR(4000) NOT NULL,
    type VARCHAR(128) NOT NULL,
    textchannel BOOLEAN NOT NULL DEFAULT false,
    attributes VARCHAR(4000) NOT NULL
);

CREATE TABLE IF NOT EXISTS tc_device_command (
    deviceid BIGINT NOT NULL,
    commandid BIGINT NOT NULL,
    PRIMARY KEY (deviceid, commandid),
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tc_group_command (
    groupid BIGINT NOT NULL,
    commandid BIGINT NOT NULL,
    PRIMARY KEY (groupid, commandid),
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tc_user_command (
    userid BIGINT NOT NULL,
    commandid BIGINT NOT NULL,
    PRIMARY KEY (userid, commandid),
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_command_type ON tc_commands(type);

-- Version 1.1.0: Add Command Templates
-- Simulates adding a new table for command templates

CREATE TABLE IF NOT EXISTS tc_command_templates (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(4000),
    protocol VARCHAR(128) NOT NULL,
    template TEXT NOT NULL,
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_command_template_protocol ON tc_command_templates(protocol);

-- Version 1.2.0: Add Command Execution History
-- Simulates adding command execution tracking

CREATE TABLE IF NOT EXISTS tc_command_executions (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    deviceid BIGINT NOT NULL,
    commandid BIGINT NOT NULL,
    userid BIGINT,
    status VARCHAR(128) NOT NULL,
    executed TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    response TEXT,
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_command_executions_device ON tc_command_executions(deviceid);
CREATE INDEX IF NOT EXISTS idx_command_executions_status ON tc_command_executions(status);

-- Version 1.3.0: Schema Modification
-- Simulates altering existing tables to add new columns

ALTER TABLE tc_commands ADD COLUMN IF NOT EXISTS uniqueid VARCHAR(128);
ALTER TABLE tc_commands ADD COLUMN IF NOT EXISTS protocol VARCHAR(128);
ALTER TABLE tc_commands ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT true;

CREATE INDEX IF NOT EXISTS idx_command_uniqueid ON tc_commands(uniqueid);
CREATE INDEX IF NOT EXISTS idx_command_protocol ON tc_commands(protocol);

-- Version 1.4.0: Add Command Categories
-- Simulates adding a new table and relationships

CREATE TABLE IF NOT EXISTS tc_command_categories (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(4000),
    color VARCHAR(7) DEFAULT '#3388ff'
);

ALTER TABLE tc_commands ADD COLUMN IF NOT EXISTS categoryid BIGINT;
ALTER TABLE tc_commands ADD CONSTRAINT IF NOT EXISTS fk_command_category 
    FOREIGN KEY (categoryid) REFERENCES tc_command_categories(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_command_category ON tc_commands(categoryid);

-- Version 1.5.0: Command Queue Management
-- Simulates adding a queue for pending commands

CREATE TABLE IF NOT EXISTS tc_command_queue (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    deviceid BIGINT NOT NULL,
    commandid BIGINT NOT NULL,
    userid BIGINT,
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    scheduled TIMESTAMP,
    priority INT NOT NULL DEFAULT 0,
    status VARCHAR(128) NOT NULL DEFAULT 'PENDING',
    attempts INT NOT NULL DEFAULT 0,
    lastAttempt TIMESTAMP,
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_command_queue_device ON tc_command_queue(deviceid);
CREATE INDEX IF NOT EXISTS idx_command_queue_status ON tc_command_queue(status);
CREATE INDEX IF NOT EXISTS idx_command_queue_scheduled ON tc_command_queue(scheduled);

-- Version 1.6.0: Data Type Modifications
-- Simulates changing column types and constraints

ALTER TABLE tc_commands ALTER COLUMN attributes TYPE TEXT;
ALTER TABLE tc_command_templates ALTER COLUMN template TYPE TEXT;

-- Version 1.7.0: Add Command Parameters
-- Simulates adding a new table for parameterized commands

CREATE TABLE IF NOT EXISTS tc_command_parameters (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    commandid BIGINT NOT NULL,
    name VARCHAR(128) NOT NULL,
    datatype VARCHAR(32) NOT NULL DEFAULT 'STRING',
    required BOOLEAN NOT NULL DEFAULT false,
    defaultvalue VARCHAR(255),
    description VARCHAR(4000),
    displayorder INT NOT NULL DEFAULT 0,
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_command_parameters_command ON tc_command_parameters(commandid);

-- Version 1.8.0: Add Command Validation Rules
-- Simulates adding validation for command parameters

CREATE TABLE IF NOT EXISTS tc_command_validation (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    parameterid BIGINT NOT NULL,
    ruletype VARCHAR(32) NOT NULL,
    rulevalue VARCHAR(255),
    errormessage VARCHAR(255),
    FOREIGN KEY (parameterid) REFERENCES tc_command_parameters(id) ON DELETE CASCADE
);

-- Version 1.9.0: Add Command Throttling
-- Simulates adding rate limiting for commands

ALTER TABLE tc_commands ADD COLUMN IF NOT EXISTS throttleperiod INT;
ALTER TABLE tc_commands ADD COLUMN IF NOT EXISTS throttlelimit INT;

-- Version 2.0.0: Command Permissions Refactoring
-- Simulates a major schema refactoring for permissions

-- Create new consolidated permissions table
CREATE TABLE IF NOT EXISTS tc_command_permissions (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    commandid BIGINT NOT NULL,
    entitytype VARCHAR(32) NOT NULL, -- 'USER', 'GROUP', or 'DEVICE'
    entityid BIGINT NOT NULL,
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_command_permissions_unique 
    ON tc_command_permissions(commandid, entitytype, entityid);

-- Simulate data migration (in a real migration, this would copy data from old tables)
INSERT INTO tc_command_permissions (commandid, entitytype, entityid)
SELECT commandid, 'USER', userid FROM tc_user_command
ON CONFLICT DO NOTHING;

INSERT INTO tc_command_permissions (commandid, entitytype, entityid)
SELECT commandid, 'GROUP', groupid FROM tc_group_command
ON CONFLICT DO NOTHING;

INSERT INTO tc_command_permissions (commandid, entitytype, entityid)
SELECT commandid, 'DEVICE', deviceid FROM tc_device_command
ON CONFLICT DO NOTHING;

-- In a real migration, these would be dropped after successful data migration
-- DROP TABLE tc_user_command;
-- DROP TABLE tc_group_command;
-- DROP TABLE tc_device_command;

-- Version 2.1.0: Add Command Auditing
-- Simulates adding audit logging for commands

CREATE TABLE IF NOT EXISTS tc_command_audit (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    commandid BIGINT,
    deviceid BIGINT,
    userid BIGINT,
    action VARCHAR(32) NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    details TEXT,
    ipaddress VARCHAR(45)
);

CREATE INDEX IF NOT EXISTS idx_command_audit_command ON tc_command_audit(commandid);
CREATE INDEX IF NOT EXISTS idx_command_audit_device ON tc_command_audit(deviceid);
CREATE INDEX IF NOT EXISTS idx_command_audit_user ON tc_command_audit(userid);
CREATE INDEX IF NOT EXISTS idx_command_audit_timestamp ON tc_command_audit(timestamp);

-- Version 2.2.0: Add Binary Command Support
-- Simulates adding support for binary commands

ALTER TABLE tc_commands ADD COLUMN IF NOT EXISTS binarydata BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE tc_commands ADD COLUMN IF NOT EXISTS binaryencoding VARCHAR(32);

-- Version 2.3.0: Add Command Response Templates
-- Simulates adding expected response patterns for commands

CREATE TABLE IF NOT EXISTS tc_command_responses (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    commandid BIGINT NOT NULL,
    pattern VARCHAR(255) NOT NULL,
    responsetype VARCHAR(32) NOT NULL DEFAULT 'SUCCESS',
    timeout INT NOT NULL DEFAULT 10000,
    description VARCHAR(4000),
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_command_responses_command ON tc_command_responses(commandid);

-- Version 2.4.0: Add Command Scheduling
-- Simulates adding advanced scheduling capabilities

CREATE TABLE IF NOT EXISTS tc_command_schedules (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    commandid BIGINT NOT NULL,
    deviceid BIGINT NOT NULL,
    userid BIGINT,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(4000),
    cronexpression VARCHAR(128) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    lastrun TIMESTAMP,
    nextrun TIMESTAMP,
    parameters TEXT,
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_command_schedules_command ON tc_command_schedules(commandid);
CREATE INDEX IF NOT EXISTS idx_command_schedules_device ON tc_command_schedules(deviceid);
CREATE INDEX IF NOT EXISTS idx_command_schedules_nextrun ON tc_command_schedules(nextrun);

-- Version 2.5.0: Add Command Batching
-- Simulates adding support for command batches

CREATE TABLE IF NOT EXISTS tc_command_batches (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(4000),
    userid BIGINT,
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    executed TIMESTAMP,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING'
);

CREATE TABLE IF NOT EXISTS tc_command_batch_items (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    batchid BIGINT NOT NULL,
    commandid BIGINT NOT NULL,
    deviceid BIGINT NOT NULL,
    parameters TEXT,
    sequence INT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    executed TIMESTAMP,
    response TEXT,
    FOREIGN KEY (batchid) REFERENCES tc_command_batches(id) ON DELETE CASCADE,
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_command_batch_items_batch ON tc_command_batch_items(batchid);
CREATE INDEX IF NOT EXISTS idx_command_batch_items_device ON tc_command_batch_items(deviceid);
CREATE INDEX IF NOT EXISTS idx_command_batch_items_sequence ON tc_command_batch_items(sequence);

-- Version 2.6.0: Add Command Macros
-- Simulates adding support for command macros (sequences of commands)

CREATE TABLE IF NOT EXISTS tc_command_macros (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(4000),
    userid BIGINT,
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tc_command_macro_items (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    macroid BIGINT NOT NULL,
    commandid BIGINT NOT NULL,
    parameters TEXT,
    sequence INT NOT NULL DEFAULT 0,
    delaybefore INT NOT NULL DEFAULT 0,
    delayafter INT NOT NULL DEFAULT 0,
    FOREIGN KEY (macroid) REFERENCES tc_command_macros(id) ON DELETE CASCADE,
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_command_macro_items_macro ON tc_command_macro_items(macroid);
CREATE INDEX IF NOT EXISTS idx_command_macro_items_sequence ON tc_command_macro_items(sequence);

-- Version 2.7.0: Add Command Result Processing
-- Simulates adding support for processing command responses

CREATE TABLE IF NOT EXISTS tc_command_processors (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    commandid BIGINT NOT NULL,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(4000),
    processortype VARCHAR(32) NOT NULL,
    configuration TEXT,
    enabled BOOLEAN NOT NULL DEFAULT true,
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_command_processors_command ON tc_command_processors(commandid);

-- Version 2.8.0: Add Command Access Control Lists
-- Simulates adding fine-grained access control for commands

CREATE TABLE IF NOT EXISTS tc_command_acl (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    commandid BIGINT NOT NULL,
    entitytype VARCHAR(32) NOT NULL, -- 'USER', 'GROUP', 'ROLE'
    entityid BIGINT NOT NULL,
    permission VARCHAR(32) NOT NULL, -- 'EXECUTE', 'VIEW', 'EDIT', 'DELETE'
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_command_acl_unique 
    ON tc_command_acl(commandid, entitytype, entityid, permission);

-- Version 2.9.0: Add Command Versioning
-- Simulates adding versioning support for commands

ALTER TABLE tc_commands ADD COLUMN IF NOT EXISTS version INT NOT NULL DEFAULT 1;
ALTER TABLE tc_commands ADD COLUMN IF NOT EXISTS versioncomment TEXT;
ALTER TABLE tc_commands ADD COLUMN IF NOT EXISTS lastmodified TIMESTAMP;

-- Version 3.0.0: Add Command Tags
-- Simulates adding tagging support for commands

CREATE TABLE IF NOT EXISTS tc_command_tags (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(128) NOT NULL UNIQUE,
    color VARCHAR(7) DEFAULT '#3388ff'
);

CREATE TABLE IF NOT EXISTS tc_command_tag_map (
    commandid BIGINT NOT NULL,
    tagid BIGINT NOT NULL,
    PRIMARY KEY (commandid, tagid),
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE,
    FOREIGN KEY (tagid) REFERENCES tc_command_tags(id) ON DELETE CASCADE
);

-- This migration script includes a comprehensive set of schema changes
-- that simulate the evolution of the command-related tables owned by the Protocol Service.
-- It allows tests to verify that the service can handle database migrations correctly.