-- Event Service Test Migration Script
-- This script contains test migration statements for testing database schema migrations in the Event Service.
-- It simulates schema evolution to verify that the Event Service can handle database changes correctly during service updates.

-- ==========================================
-- Version 1.0: Initial Schema Setup
-- ==========================================

-- Create test events table (simplified version of tc_events)
CREATE TABLE IF NOT EXISTS tc_events_test (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    type VARCHAR(128) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    device_id BIGINT NOT NULL,
    position_id BIGINT,
    geofence_id BIGINT,
    maintenance_id BIGINT,
    attributes VARCHAR(4000),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create test geofences table (simplified version of tc_geofences)
CREATE TABLE IF NOT EXISTS tc_geofences_test (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(128),
    area VARCHAR(4000) NOT NULL,
    attributes VARCHAR(4000),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create test event rules table
CREATE TABLE IF NOT EXISTS tc_event_rules_test (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    device_id BIGINT,
    group_id BIGINT,
    type VARCHAR(128) NOT NULL,
    always BOOLEAN DEFAULT false NOT NULL,
    attributes VARCHAR(4000),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create basic indexes
CREATE INDEX IF NOT EXISTS event_deviceid_eventtime ON tc_events_test(device_id, event_time);
CREATE INDEX IF NOT EXISTS event_type_idx ON tc_events_test(type);
CREATE INDEX IF NOT EXISTS geofence_name_idx ON tc_geofences_test(name);

-- ==========================================
-- Version 1.1: Add Event Severity
-- ==========================================

-- Add severity column to events table
ALTER TABLE tc_events_test ADD COLUMN severity VARCHAR(10) DEFAULT 'INFO';

-- Update existing events to set severity based on type
UPDATE tc_events_test SET severity = 'WARNING' WHERE type IN ('deviceOverspeed', 'geofenceEnter', 'geofenceExit');
UPDATE tc_events_test SET severity = 'CRITICAL' WHERE type IN ('deviceOffline', 'deviceUnknown');

-- Create index on severity for filtering
CREATE INDEX IF NOT EXISTS event_severity_idx ON tc_events_test(severity);

-- ==========================================
-- Version 1.2: Add Event Categories
-- ==========================================

-- Create event categories table
CREATE TABLE IF NOT EXISTS tc_event_categories_test (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(256),
    color VARCHAR(7),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add category_id to events table
ALTER TABLE tc_events_test ADD COLUMN category_id BIGINT;

-- Create foreign key constraint
ALTER TABLE tc_events_test ADD CONSTRAINT fk_event_category 
    FOREIGN KEY (category_id) REFERENCES tc_event_categories_test(id) ON DELETE SET NULL;

-- Insert default categories
INSERT INTO tc_event_categories_test (name, description, color) VALUES
    ('System', 'System-related events', '#FF0000'),
    ('Movement', 'Movement-related events', '#00FF00'),
    ('Geofence', 'Geofence-related events', '#0000FF'),
    ('Maintenance', 'Maintenance-related events', '#FFFF00');

-- Update existing events with categories
UPDATE tc_events_test SET category_id = 
    (SELECT id FROM tc_event_categories_test WHERE name = 'System') 
    WHERE type IN ('deviceOnline', 'deviceOffline', 'deviceUnknown');

UPDATE tc_events_test SET category_id = 
    (SELECT id FROM tc_event_categories_test WHERE name = 'Movement') 
    WHERE type IN ('deviceMoving', 'deviceStopped', 'deviceOverspeed');

UPDATE tc_events_test SET category_id = 
    (SELECT id FROM tc_event_categories_test WHERE name = 'Geofence') 
    WHERE type IN ('geofenceEnter', 'geofenceExit');

-- ==========================================
-- Version 1.3: Modify Column Types and Add Constraints
-- ==========================================

-- Modify attributes column to JSON type (database-specific syntax)
-- For PostgreSQL
-- ALTER TABLE tc_events_test ALTER COLUMN attributes TYPE jsonb USING attributes::jsonb;
-- For MySQL/MariaDB
ALTER TABLE tc_events_test MODIFY COLUMN attributes JSON;

-- Add not null constraint to device_id
ALTER TABLE tc_event_rules_test MODIFY COLUMN device_id BIGINT NOT NULL;

-- Add unique constraint to geofence name
ALTER TABLE tc_geofences_test ADD CONSTRAINT unique_geofence_name UNIQUE (name);

-- ==========================================
-- Version 1.4: Rename Columns and Add New Table
-- ==========================================

-- Rename column in events table
ALTER TABLE tc_events_test CHANGE event_time occurred_at TIMESTAMP NOT NULL;

-- Create event acknowledgment table
CREATE TABLE IF NOT EXISTS tc_event_acknowledgments_test (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    comment VARCHAR(512),
    acknowledged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_event_ack FOREIGN KEY (event_id) REFERENCES tc_events_test(id) ON DELETE CASCADE
);

-- Add acknowledged flag to events table
ALTER TABLE tc_events_test ADD COLUMN acknowledged BOOLEAN DEFAULT FALSE;

-- ==========================================
-- Version 1.5: Add Partitioning Support (Example)
-- ==========================================

-- Note: This is a simplified example of partitioning logic
-- Actual implementation would depend on the specific database being used

-- Create partition function (for SQL Server)
-- CREATE PARTITION FUNCTION EventTimeRangePF (TIMESTAMP) AS RANGE RIGHT FOR VALUES
--    ('2023-01-01', '2023-04-01', '2023-07-01', '2023-10-01', '2024-01-01');

-- Create partition scheme (for SQL Server)
-- CREATE PARTITION SCHEME EventTimeRangePS AS PARTITION EventTimeRangePF
--    TO (event_data_2022, event_data_q1_2023, event_data_q2_2023, 
--        event_data_q3_2023, event_data_q4_2023, event_data_2024);

-- For MySQL example (requires MySQL 8.0+)
-- ALTER TABLE tc_events_test PARTITION BY RANGE (UNIX_TIMESTAMP(occurred_at)) (
--    PARTITION p_2022 VALUES LESS THAN (UNIX_TIMESTAMP('2023-01-01')),
--    PARTITION p_q1_2023 VALUES LESS THAN (UNIX_TIMESTAMP('2023-04-01')),
--    PARTITION p_q2_2023 VALUES LESS THAN (UNIX_TIMESTAMP('2023-07-01')),
--    PARTITION p_q3_2023 VALUES LESS THAN (UNIX_TIMESTAMP('2023-10-01')),
--    PARTITION p_q4_2023 VALUES LESS THAN (UNIX_TIMESTAMP('2024-01-01')),
--    PARTITION p_future VALUES LESS THAN MAXVALUE
-- );

-- ==========================================
-- Version 1.6: Add Event Correlation Support
-- ==========================================

-- Add correlation ID column to events table
ALTER TABLE tc_events_test ADD COLUMN correlation_id VARCHAR(36);

-- Create index on correlation_id
CREATE INDEX IF NOT EXISTS event_correlation_idx ON tc_events_test(correlation_id);

-- Create event correlation rules table
CREATE TABLE IF NOT EXISTS tc_event_correlation_rules_test (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(256),
    condition_type VARCHAR(20) NOT NULL, -- 'sequence', 'pattern', 'threshold'
    time_window INT NOT NULL, -- in seconds
    event_types VARCHAR(512) NOT NULL, -- comma-separated list of event types
    attributes VARCHAR(4000),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- Version 1.7: Data Migration Example
-- ==========================================

-- Create temporary table for data migration
CREATE TABLE IF NOT EXISTS tc_events_temp_test (
    id BIGINT PRIMARY KEY,
    type VARCHAR(128) NOT NULL,
    occurred_at TIMESTAMP NOT NULL,
    device_id BIGINT NOT NULL,
    position_id BIGINT,
    geofence_id BIGINT,
    maintenance_id BIGINT,
    attributes JSON,
    created_at TIMESTAMP,
    severity VARCHAR(10),
    category_id BIGINT,
    acknowledged BOOLEAN,
    correlation_id VARCHAR(36),
    processed BOOLEAN DEFAULT FALSE
);

-- Copy data to temporary table with transformation
INSERT INTO tc_events_temp_test
SELECT 
    id, 
    type, 
    occurred_at, 
    device_id, 
    position_id, 
    geofence_id, 
    maintenance_id, 
    attributes, 
    created_at, 
    severity, 
    category_id, 
    acknowledged, 
    correlation_id,
    FALSE as processed
FROM tc_events_test;

-- Update processed flag based on business logic
UPDATE tc_events_temp_test SET processed = TRUE WHERE acknowledged = TRUE;

-- Drop original table (commented out for safety in test script)
-- DROP TABLE tc_events_test;

-- Rename temporary table to original name (commented out for safety in test script)
-- ALTER TABLE tc_events_temp_test RENAME TO tc_events_test;

-- ==========================================
-- Version 1.8: Add Computed Columns and Triggers
-- ==========================================

-- Add computed column for event age (database-specific syntax)
-- For MySQL
ALTER TABLE tc_events_test ADD COLUMN event_age_hours DECIMAL(10,2) 
    GENERATED ALWAYS AS (TIMESTAMPDIFF(HOUR, occurred_at, CURRENT_TIMESTAMP)) STORED;

-- Create trigger for automatic category assignment (database-specific syntax)
-- For MySQL
DELIMITER //
CREATE TRIGGER IF NOT EXISTS trg_event_category_assignment
BEFORE INSERT ON tc_events_test
FOR EACH ROW
BEGIN
    IF NEW.type IN ('deviceOnline', 'deviceOffline', 'deviceUnknown') THEN
        SET NEW.category_id = (SELECT id FROM tc_event_categories_test WHERE name = 'System');
    ELSEIF NEW.type IN ('deviceMoving', 'deviceStopped', 'deviceOverspeed') THEN
        SET NEW.category_id = (SELECT id FROM tc_event_categories_test WHERE name = 'Movement');
    ELSEIF NEW.type IN ('geofenceEnter', 'geofenceExit') THEN
        SET NEW.category_id = (SELECT id FROM tc_event_categories_test WHERE name = 'Geofence');
    ELSEIF NEW.type LIKE 'maintenance%' THEN
        SET NEW.category_id = (SELECT id FROM tc_event_categories_test WHERE name = 'Maintenance');
    END IF;
    
    IF NEW.type IN ('deviceOffline', 'deviceUnknown') THEN
        SET NEW.severity = 'CRITICAL';
    ELSEIF NEW.type IN ('deviceOverspeed', 'geofenceEnter', 'geofenceExit') THEN
        SET NEW.severity = 'WARNING';
    ELSE
        SET NEW.severity = 'INFO';
    END IF;
END //
DELIMITER ;

-- ==========================================
-- Version 1.9: Add Event Archiving Support
-- ==========================================

-- Create archive table with same structure as events table
CREATE TABLE IF NOT EXISTS tc_events_archive_test (
    id BIGINT PRIMARY KEY,
    type VARCHAR(128) NOT NULL,
    occurred_at TIMESTAMP NOT NULL,
    device_id BIGINT NOT NULL,
    position_id BIGINT,
    geofence_id BIGINT,
    maintenance_id BIGINT,
    attributes JSON,
    created_at TIMESTAMP,
    severity VARCHAR(10),
    category_id BIGINT,
    acknowledged BOOLEAN,
    correlation_id VARCHAR(36),
    processed BOOLEAN,
    event_age_hours DECIMAL(10,2),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on archive table
CREATE INDEX IF NOT EXISTS archive_deviceid_occurredat ON tc_events_archive_test(device_id, occurred_at);

-- Create stored procedure for archiving old events (database-specific syntax)
-- For MySQL
DELIMITER //
CREATE PROCEDURE IF NOT EXISTS sp_archive_old_events(IN days_old INT)
BEGIN
    -- Insert old events into archive table
    INSERT INTO tc_events_archive_test
    SELECT e.*, CURRENT_TIMESTAMP as archived_at
    FROM tc_events_test e
    WHERE e.occurred_at < DATE_SUB(CURRENT_TIMESTAMP, INTERVAL days_old DAY);
    
    -- Delete archived events from main table
    DELETE FROM tc_events_test
    WHERE occurred_at < DATE_SUB(CURRENT_TIMESTAMP, INTERVAL days_old DAY);
END //
DELIMITER ;

-- ==========================================
-- Version 2.0: Add Event Aggregation Support
-- ==========================================

-- Create event aggregation table
CREATE TABLE IF NOT EXISTS tc_event_aggregations_test (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    device_id BIGINT NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    count INT NOT NULL DEFAULT 0,
    attributes JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for aggregation table
CREATE INDEX IF NOT EXISTS agg_deviceid_type ON tc_event_aggregations_test(device_id, event_type);
CREATE INDEX IF NOT EXISTS agg_timerange ON tc_event_aggregations_test(start_time, end_time);

-- Create view for event statistics
CREATE OR REPLACE VIEW vw_event_statistics AS
SELECT 
    device_id,
    type as event_type,
    DATE(occurred_at) as event_date,
    COUNT(*) as event_count,
    MIN(occurred_at) as first_occurrence,
    MAX(occurred_at) as last_occurrence
FROM tc_events_test
GROUP BY device_id, type, DATE(occurred_at);

-- ==========================================
-- Version 2.1: Add Support for Event Forwarding
-- ==========================================

-- Create event forwarding configuration table
CREATE TABLE IF NOT EXISTS tc_event_forward_config_test (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(256),
    url VARCHAR(512) NOT NULL,
    header_authorization VARCHAR(512),
    event_types VARCHAR(512), -- comma-separated list of event types, NULL means all
    device_ids VARCHAR(512), -- comma-separated list of device IDs, NULL means all
    retry_count INT DEFAULT 3,
    retry_delay INT DEFAULT 60, -- in seconds
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create event forwarding history table
CREATE TABLE IF NOT EXISTS tc_event_forward_history_test (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    config_id BIGINT NOT NULL,
    event_id BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'SUCCESS', 'FAILED', 'PENDING', 'RETRYING'
    attempt_count INT DEFAULT 0,
    last_attempt TIMESTAMP,
    next_attempt TIMESTAMP,
    response_code INT,
    response_message VARCHAR(512),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_forward_config FOREIGN KEY (config_id) REFERENCES tc_event_forward_config_test(id) ON DELETE CASCADE,
    CONSTRAINT fk_forward_event FOREIGN KEY (event_id) REFERENCES tc_events_test(id) ON DELETE CASCADE
);

-- Create indexes for forwarding tables
CREATE INDEX IF NOT EXISTS forward_config_enabled ON tc_event_forward_config_test(enabled);
CREATE INDEX IF NOT EXISTS forward_history_status ON tc_event_forward_history_test(status);
CREATE INDEX IF NOT EXISTS forward_history_next_attempt ON tc_event_forward_history_test(next_attempt);

-- ==========================================
-- Version 2.2: Database Schema Compatibility Testing
-- ==========================================

-- Test case for handling NULL values in new columns
ALTER TABLE tc_events_test ADD COLUMN test_nullable VARCHAR(50);

-- Test case for handling default values
ALTER TABLE tc_events_test ADD COLUMN test_default VARCHAR(50) DEFAULT 'DEFAULT_VALUE';

-- Test case for handling column type changes
ALTER TABLE tc_events_test MODIFY COLUMN severity VARCHAR(20);

-- Test case for handling column drops
ALTER TABLE tc_events_test DROP COLUMN test_nullable;

-- Test case for handling index changes
DROP INDEX event_type_idx ON tc_events_test;
CREATE INDEX event_type_severity_idx ON tc_events_test(type, severity);

-- ==========================================
-- Version 2.3: Clean Up Test Tables (for test reset)
-- ==========================================

-- These statements are commented out but can be used to clean up test tables
-- when running tests multiple times

/*
DROP TABLE IF EXISTS tc_event_forward_history_test;
DROP TABLE IF EXISTS tc_event_forward_config_test;
DROP VIEW IF EXISTS vw_event_statistics;
DROP TABLE IF EXISTS tc_event_aggregations_test;
DROP TABLE IF EXISTS tc_events_archive_test;
DROP PROCEDURE IF EXISTS sp_archive_old_events;
DROP TRIGGER IF EXISTS trg_event_category_assignment;
DROP TABLE IF EXISTS tc_event_acknowledgments_test;
DROP TABLE IF EXISTS tc_event_correlation_rules_test;
DROP TABLE IF EXISTS tc_events_temp_test;
DROP TABLE IF EXISTS tc_events_test;
DROP TABLE IF EXISTS tc_event_rules_test;
DROP TABLE IF EXISTS tc_event_categories_test;
DROP TABLE IF EXISTS tc_geofences_test;
*/