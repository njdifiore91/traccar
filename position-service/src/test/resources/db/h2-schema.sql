-- H2-specific database schema for in-memory testing of the Position Service
-- Contains H2-compatible DDL statements for creating position-related tables

-- Drop existing tables if they exist to ensure clean setup
-- Drop in reverse order of dependencies to avoid constraint violations
DROP TABLE IF EXISTS tc_position_attributes;
DROP TABLE IF EXISTS tc_device_position;
DROP TABLE IF EXISTS tc_positions;

-- Create positions table
-- This is the main table for storing GPS position data
CREATE TABLE tc_positions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    protocol VARCHAR(128),
    deviceid BIGINT NOT NULL,
    servertime TIMESTAMP NOT NULL,
    devicetime TIMESTAMP,
    fixtime TIMESTAMP,
    valid BOOLEAN,
    latitude DOUBLE,
    longitude DOUBLE,
    altitude DOUBLE,
    speed DOUBLE,
    course DOUBLE,
    address VARCHAR(512),
    accuracy DOUBLE,
    network VARCHAR(4000),
    attributes VARCHAR(4000), -- JSON format for additional attributes
    processed BOOLEAN DEFAULT FALSE
);

-- Create indexes for optimized queries
-- Index for efficient time-based position queries
CREATE INDEX position_deviceid_fixtime ON tc_positions(deviceid, fixtime);
-- Index for device-specific position queries
CREATE INDEX position_deviceid ON tc_positions(deviceid);
-- Index for time-based queries
CREATE INDEX position_fixtime ON tc_positions(fixtime);
-- Index for processed status queries
CREATE INDEX position_processed ON tc_positions(processed);

-- Create device position mapping table
-- This table maps devices to their latest positions
CREATE TABLE tc_device_position (
    deviceid BIGINT NOT NULL,
    positionid BIGINT NOT NULL,
    PRIMARY KEY (deviceid),
    FOREIGN KEY (positionid) REFERENCES tc_positions(id) ON DELETE CASCADE
);

-- Create position attributes table
-- This table stores additional attributes for positions that need to be queried directly
CREATE TABLE tc_position_attributes (
    positionid BIGINT NOT NULL,
    attribute VARCHAR(128) NOT NULL,
    value VARCHAR(4000) NOT NULL,
    PRIMARY KEY (positionid, attribute),
    FOREIGN KEY (positionid) REFERENCES tc_positions(id) ON DELETE CASCADE
);

-- Create index for attribute-based queries
CREATE INDEX position_attribute_name ON tc_position_attributes(attribute);

-- H2-specific optimizations for in-memory testing
-- Enable in-memory mode with no database closing on VM exit
SET DB_CLOSE_DELAY -1;
-- Optimize for in-memory operations
SET OPTIMIZE_REUSE_RESULTS 1;
-- Enable multi-version concurrency for better test parallelism
SET MVCC TRUE;
-- Set larger cache size for better performance
SET CACHE_SIZE 65536;