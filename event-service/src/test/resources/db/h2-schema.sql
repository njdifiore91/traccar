-- H2-specific database schema for in-memory testing of the Event Service
-- This file contains H2-compatible DDL statements for creating event-related tables
-- Used for fast, in-memory testing without requiring a full database server

-- Create events table
CREATE TABLE IF NOT EXISTS tc_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    type VARCHAR(128) NOT NULL,
    eventtime TIMESTAMP NOT NULL,
    deviceid BIGINT,
    positionid BIGINT,
    geofenceid BIGINT,
    maintenanceid BIGINT,
    attributes VARCHAR(4000),
    servertime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create geofences table
CREATE TABLE IF NOT EXISTS tc_geofences (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(128),
    area VARCHAR(4000) NOT NULL,
    calendarid BIGINT,
    attributes VARCHAR(4000)
);

-- Create permission tables for geofences
CREATE TABLE IF NOT EXISTS tc_user_geofence (
    userid BIGINT NOT NULL,
    geofenceid BIGINT NOT NULL,
    PRIMARY KEY (userid, geofenceid)
);

CREATE TABLE IF NOT EXISTS tc_group_geofence (
    groupid BIGINT NOT NULL,
    geofenceid BIGINT NOT NULL,
    PRIMARY KEY (groupid, geofenceid)
);

CREATE TABLE IF NOT EXISTS tc_device_geofence (
    deviceid BIGINT NOT NULL,
    geofenceid BIGINT NOT NULL,
    PRIMARY KEY (deviceid, geofenceid)
);

-- Create maintenance table
CREATE TABLE IF NOT EXISTS tc_maintenance (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    type VARCHAR(128) NOT NULL,
    start DOUBLE DEFAULT 0 NOT NULL,
    period DOUBLE DEFAULT 0 NOT NULL,
    attributes VARCHAR(4000)
);

-- Create permission tables for maintenance
CREATE TABLE IF NOT EXISTS tc_user_maintenance (
    userid BIGINT NOT NULL,
    maintenanceid BIGINT NOT NULL,
    PRIMARY KEY (userid, maintenanceid)
);

CREATE TABLE IF NOT EXISTS tc_group_maintenance (
    groupid BIGINT NOT NULL,
    maintenanceid BIGINT NOT NULL,
    PRIMARY KEY (groupid, maintenanceid)
);

CREATE TABLE IF NOT EXISTS tc_device_maintenance (
    deviceid BIGINT NOT NULL,
    maintenanceid BIGINT NOT NULL,
    PRIMARY KEY (deviceid, maintenanceid)
);

-- Create event rules table
CREATE TABLE IF NOT EXISTS tc_event_rules (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(256),
    type VARCHAR(128) NOT NULL,
    deviceid BIGINT,
    groupid BIGINT,
    calendarid BIGINT,
    always BOOLEAN DEFAULT FALSE NOT NULL,
    expression VARCHAR(4000),
    attributes VARCHAR(4000),
    enabled BOOLEAN DEFAULT TRUE NOT NULL
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_events_deviceid ON tc_events(deviceid);
CREATE INDEX IF NOT EXISTS idx_events_geofenceid ON tc_events(geofenceid);
CREATE INDEX IF NOT EXISTS idx_events_maintenanceid ON tc_events(maintenanceid);
CREATE INDEX IF NOT EXISTS idx_events_positionid ON tc_events(positionid);
CREATE INDEX IF NOT EXISTS idx_events_type ON tc_events(type);
CREATE INDEX IF NOT EXISTS idx_events_eventtime ON tc_events(eventtime);

-- H2-specific optimized index for event timeline queries
CREATE INDEX IF NOT EXISTS idx_events_deviceid_eventtime ON tc_events(deviceid, eventtime);

-- Create version table for schema tracking
CREATE TABLE IF NOT EXISTS tc_version (
    version VARCHAR(128) NOT NULL
);

-- Insert current schema version
MERGE INTO tc_version KEY(version) VALUES ('1.0');