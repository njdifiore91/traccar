-- Position Service Database Schema for Testing Environment
-- This schema defines the tables owned by the Position Service for testing purposes

-- Drop tables if they exist to ensure clean state for tests
DROP TABLE IF EXISTS tc_positions CASCADE;
DROP TABLE IF EXISTS tc_position_attributes CASCADE;
DROP TABLE IF EXISTS tc_device_positions CASCADE;

-- Create positions table
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
    accuracy DOUBLE,
    network VARCHAR(4096),
    attributes VARCHAR(4096),
    processed BOOLEAN DEFAULT FALSE
);

-- Create position attributes table for extended attributes
CREATE TABLE tc_position_attributes (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    positionid BIGINT NOT NULL,
    attribute VARCHAR(128) NOT NULL,
    value VARCHAR(4096) NOT NULL,
    FOREIGN KEY (positionid) REFERENCES tc_positions(id) ON DELETE CASCADE
);

-- Create device positions table to track latest position per device
CREATE TABLE tc_device_positions (
    deviceid BIGINT PRIMARY KEY,
    positionid BIGINT NOT NULL,
    FOREIGN KEY (positionid) REFERENCES tc_positions(id) ON DELETE CASCADE
);

-- Create indexes for optimized queries
-- Index for querying positions by device and time
CREATE INDEX position_deviceid_fixtime ON tc_positions(deviceid, fixtime);

-- Index for querying positions by device and server time
CREATE INDEX position_deviceid_servertime ON tc_positions(deviceid, servertime);

-- Index for position attributes lookup
CREATE INDEX position_attribute_positionid ON tc_position_attributes(positionid);
CREATE INDEX position_attribute_lookup ON tc_position_attributes(attribute, value);

-- Create mock device table for testing purposes
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

-- Create mock geofence table for testing position-geofence interactions
CREATE TABLE tc_geofences (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(512),
    area VARCHAR(4096) NOT NULL
);

-- Create mock device-geofence mapping table for testing
CREATE TABLE tc_device_geofence (
    deviceid BIGINT NOT NULL,
    geofenceid BIGINT NOT NULL,
    PRIMARY KEY (deviceid, geofenceid)
);

-- Create mock events table for testing position-event relationships
CREATE TABLE tc_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    type VARCHAR(128) NOT NULL,
    eventtime TIMESTAMP NOT NULL,
    deviceid BIGINT NOT NULL,
    positionid BIGINT,
    geofenceid BIGINT,
    attributes VARCHAR(4096),
    FOREIGN KEY (positionid) REFERENCES tc_positions(id) ON DELETE CASCADE
);

-- Create index for event queries
CREATE INDEX event_deviceid_eventtime ON tc_events(deviceid, eventtime);

-- Create position processing status table to track processing state
CREATE TABLE tc_position_processing (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    positionid BIGINT NOT NULL,
    status VARCHAR(128) NOT NULL,
    processingtime TIMESTAMP NOT NULL,
    attributes VARCHAR(4096),
    FOREIGN KEY (positionid) REFERENCES tc_positions(id) ON DELETE CASCADE
);

-- Create index for position processing status
CREATE INDEX position_processing_positionid ON tc_position_processing(positionid);

-- Create table for position forwarding status
CREATE TABLE tc_position_forwarding (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    positionid BIGINT NOT NULL,
    destination VARCHAR(512) NOT NULL,
    status VARCHAR(128) NOT NULL,
    senttime TIMESTAMP,
    attempts INT DEFAULT 0,
    FOREIGN KEY (positionid) REFERENCES tc_positions(id) ON DELETE CASCADE
);

-- Create index for position forwarding status
CREATE INDEX position_forwarding_positionid ON tc_position_forwarding(positionid);
CREATE INDEX position_forwarding_status ON tc_position_forwarding(status);

-- Create table for position calculations (distance, motion, etc.)
CREATE TABLE tc_position_calculations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    positionid BIGINT NOT NULL,
    distance DOUBLE,
    totaldistance DOUBLE,
    motion BOOLEAN,
    motionduration BIGINT,
    idleduration BIGINT,
    attributes VARCHAR(4096),
    FOREIGN KEY (positionid) REFERENCES tc_positions(id) ON DELETE CASCADE
);

-- Create index for position calculations
CREATE INDEX position_calculations_positionid ON tc_position_calculations(positionid);

-- Create table for position geocoding results
CREATE TABLE tc_position_geocoding (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    positionid BIGINT NOT NULL,
    provider VARCHAR(128) NOT NULL,
    address VARCHAR(512),
    country VARCHAR(128),
    state VARCHAR(128),
    city VARCHAR(128),
    street VARCHAR(128),
    housenumber VARCHAR(128),
    postalcode VARCHAR(128),
    attributes VARCHAR(4096),
    FOREIGN KEY (positionid) REFERENCES tc_positions(id) ON DELETE CASCADE
);

-- Create index for position geocoding
CREATE INDEX position_geocoding_positionid ON tc_position_geocoding(positionid);

-- Create table for position speed limit information
CREATE TABLE tc_position_speedlimit (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    positionid BIGINT NOT NULL,
    speedlimit DOUBLE,
    provider VARCHAR(128) NOT NULL,
    attributes VARCHAR(4096),
    FOREIGN KEY (positionid) REFERENCES tc_positions(id) ON DELETE CASCADE
);

-- Create index for position speed limit
CREATE INDEX position_speedlimit_positionid ON tc_position_speedlimit(positionid);

-- Insert test data for unit tests if needed
-- This can be extended or moved to separate data.sql file if preferred

-- Commit the transaction
COMMIT;