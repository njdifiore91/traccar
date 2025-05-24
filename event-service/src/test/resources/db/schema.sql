-- Event Service Database Schema for Testing Environment
-- This schema defines the tables owned by the Event Service for testing purposes

-- Drop tables if they exist to ensure clean state for tests
DROP TABLE IF EXISTS tc_event_data CASCADE;
DROP TABLE IF EXISTS tc_event_forwarding CASCADE;
DROP TABLE IF EXISTS tc_event_rule_device CASCADE;
DROP TABLE IF EXISTS tc_event_rule_group CASCADE;
DROP TABLE IF EXISTS tc_event_rule CASCADE;
DROP TABLE IF EXISTS tc_user_geofence CASCADE;
DROP TABLE IF EXISTS tc_group_geofence CASCADE;
DROP TABLE IF EXISTS tc_device_geofence CASCADE;
DROP TABLE IF EXISTS tc_events CASCADE;
DROP TABLE IF EXISTS tc_geofences CASCADE;

-- Create geofences table
CREATE TABLE tc_geofences (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(512),
    area VARCHAR(4096) NOT NULL,
    calendarid BIGINT,
    attributes VARCHAR(4096)
);

-- Create events table
CREATE TABLE tc_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    type VARCHAR(128) NOT NULL,
    eventtime TIMESTAMP NOT NULL,
    deviceid BIGINT NOT NULL,
    positionid BIGINT,
    geofenceid BIGINT,
    maintenanceid BIGINT,
    attributes VARCHAR(4096),
    processed BOOLEAN DEFAULT FALSE
);

-- Create device-geofence mapping table
CREATE TABLE tc_device_geofence (
    deviceid BIGINT NOT NULL,
    geofenceid BIGINT NOT NULL,
    PRIMARY KEY (deviceid, geofenceid),
    FOREIGN KEY (geofenceid) REFERENCES tc_geofences(id) ON DELETE CASCADE
);

-- Create group-geofence mapping table
CREATE TABLE tc_group_geofence (
    groupid BIGINT NOT NULL,
    geofenceid BIGINT NOT NULL,
    PRIMARY KEY (groupid, geofenceid),
    FOREIGN KEY (geofenceid) REFERENCES tc_geofences(id) ON DELETE CASCADE
);

-- Create user-geofence mapping table
CREATE TABLE tc_user_geofence (
    userid BIGINT NOT NULL,
    geofenceid BIGINT NOT NULL,
    PRIMARY KEY (userid, geofenceid),
    FOREIGN KEY (geofenceid) REFERENCES tc_geofences(id) ON DELETE CASCADE
);

-- Create event rule table for configuring event detection rules
CREATE TABLE tc_event_rule (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(512),
    type VARCHAR(128) NOT NULL,
    eventtype VARCHAR(128) NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    priority VARCHAR(128) DEFAULT 'NORMAL',
    parameters VARCHAR(4096),
    notificationenabled BOOLEAN DEFAULT FALSE,
    calendarid BIGINT,
    attributes VARCHAR(4096)
);

-- Create event rule to device mapping
CREATE TABLE tc_event_rule_device (
    ruleid BIGINT NOT NULL,
    deviceid BIGINT NOT NULL,
    PRIMARY KEY (ruleid, deviceid),
    FOREIGN KEY (ruleid) REFERENCES tc_event_rule(id) ON DELETE CASCADE
);

-- Create event rule to group mapping
CREATE TABLE tc_event_rule_group (
    ruleid BIGINT NOT NULL,
    groupid BIGINT NOT NULL,
    PRIMARY KEY (ruleid, groupid),
    FOREIGN KEY (ruleid) REFERENCES tc_event_rule(id) ON DELETE CASCADE
);

-- Create event forwarding table to track event delivery status
CREATE TABLE tc_event_forwarding (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    eventid BIGINT NOT NULL,
    destination VARCHAR(512) NOT NULL,
    status VARCHAR(128) NOT NULL,
    senttime TIMESTAMP,
    attempts INT DEFAULT 0,
    FOREIGN KEY (eventid) REFERENCES tc_events(id) ON DELETE CASCADE
);

-- Create event data table for additional event information
CREATE TABLE tc_event_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    eventid BIGINT NOT NULL,
    attribute VARCHAR(128) NOT NULL,
    value VARCHAR(4096) NOT NULL,
    FOREIGN KEY (eventid) REFERENCES tc_events(id) ON DELETE CASCADE
);

-- Create indexes for optimized queries
-- Index for querying events by device and time
CREATE INDEX event_deviceid_eventtime ON tc_events(deviceid, eventtime);

-- Index for querying events by type
CREATE INDEX event_type ON tc_events(type);

-- Index for querying events by position
CREATE INDEX event_positionid ON tc_events(positionid);

-- Index for querying events by geofence
CREATE INDEX event_geofenceid ON tc_events(geofenceid);

-- Index for geofence lookups
CREATE INDEX geofence_name ON tc_geofences(name);

-- Index for event rule lookups
CREATE INDEX event_rule_type ON tc_event_rule(type);
CREATE INDEX event_rule_eventtype ON tc_event_rule(eventtype);

-- Index for event data lookups
CREATE INDEX event_data_eventid ON tc_event_data(eventid);
CREATE INDEX event_data_lookup ON tc_event_data(attribute, value);

-- Index for event forwarding status
CREATE INDEX event_forwarding_eventid ON tc_event_forwarding(eventid);
CREATE INDEX event_forwarding_status ON tc_event_forwarding(status);

-- Create mock tables for testing purposes
-- This is a simplified version of the actual device table owned by the API Gateway service
CREATE TABLE tc_devices (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    uniqueid VARCHAR(128) NOT NULL,
    status VARCHAR(128),
    lastupdate TIMESTAMP,
    positionid BIGINT,
    UNIQUE (uniqueid)
);

-- Create mock positions table for testing event-position relationships
CREATE TABLE tc_positions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    protocol VARCHAR(128),
    deviceid BIGINT NOT NULL,
    servertime TIMESTAMP NOT NULL,
    devicetime TIMESTAMP NOT NULL,
    fixtime TIMESTAMP NOT NULL,
    valid BOOLEAN NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    altitude DOUBLE,
    speed DOUBLE,
    course DOUBLE,
    address VARCHAR(512),
    attributes VARCHAR(4096)
);

-- Create mock groups table for testing group-related functionality
CREATE TABLE tc_groups (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    groupid BIGINT,
    attributes VARCHAR(4096)
);

-- Create mock users table for testing user-related functionality
CREATE TABLE tc_users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    email VARCHAR(128) NOT NULL,
    administrator BOOLEAN DEFAULT FALSE,
    attributes VARCHAR(4096),
    UNIQUE (email)
);

-- Create mock calendars table for testing calendar-based event rules
CREATE TABLE tc_calendars (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    data VARCHAR(4096) NOT NULL,
    attributes VARCHAR(4096)
);

-- Commit the transaction
COMMIT;