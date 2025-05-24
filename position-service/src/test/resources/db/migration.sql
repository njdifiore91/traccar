-- Test Migration Script for Position Service Database Schema Evolution Testing
-- This script contains sample migration statements to test the Position Service's ability
-- to handle database schema changes correctly during service updates.

-- Version 1.0: Initial schema setup for testing
-- =============================================

-- Create a test positions table (simplified version of tc_positions)
CREATE TABLE IF NOT EXISTS test_positions (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    device_id BIGINT NOT NULL,
    protocol VARCHAR(128),
    server_time TIMESTAMP NOT NULL,
    device_time TIMESTAMP NOT NULL,
    fix_time TIMESTAMP NOT NULL,
    valid BOOLEAN NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    altitude DOUBLE,
    speed DOUBLE,
    course DOUBLE,
    address VARCHAR(512),
    attributes VARCHAR(4096),
    accuracy DOUBLE,
    network VARCHAR(4096),
    FOREIGN KEY (device_id) REFERENCES tc_devices(id) ON DELETE CASCADE
);

-- Create index on device_id and fix_time for efficient queries
CREATE INDEX test_positions_device_id_fix_time ON test_positions(device_id, fix_time);

-- Create a test table for position attributes
CREATE TABLE IF NOT EXISTS test_position_attributes (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    position_id BIGINT NOT NULL,
    attribute VARCHAR(128) NOT NULL,
    value VARCHAR(4096),
    FOREIGN KEY (position_id) REFERENCES test_positions(id) ON DELETE CASCADE
);

-- Create index on position_id for efficient lookups
CREATE INDEX test_position_attributes_position_id ON test_position_attributes(position_id);

-- Insert some test data
INSERT INTO test_positions (device_id, protocol, server_time, device_time, fix_time, valid, latitude, longitude, altitude, speed, course, address, attributes, accuracy, network)
VALUES 
(1, 'test-protocol', NOW(), NOW(), NOW(), true, 37.7749, -122.4194, 10.0, 0.0, 0.0, '123 Test St, San Francisco, CA', '{"fuel": 100, "ignition": true}', 5.0, NULL),
(1, 'test-protocol', NOW() - INTERVAL 1 HOUR, NOW() - INTERVAL 1 HOUR, NOW() - INTERVAL 1 HOUR, true, 37.7749, -122.4294, 15.0, 30.0, 90.0, '456 Test Ave, San Francisco, CA', '{"fuel": 95, "ignition": true}', 4.0, NULL);

-- Version 2.0: Schema evolution - Add new columns
-- =============================================

-- Add a new column for battery level
ALTER TABLE test_positions ADD COLUMN battery_level INT;

-- Add a new column for position status
ALTER TABLE test_positions ADD COLUMN status VARCHAR(128);

-- Update existing records with default values
UPDATE test_positions SET battery_level = 100, status = 'ACTIVE' WHERE battery_level IS NULL;

-- Version 3.0: Schema evolution - Modify column types and constraints
-- =============================================

-- Change attributes column to JSON type (if supported by database)
-- For MySQL/MariaDB
ALTER TABLE test_positions MODIFY COLUMN attributes JSON;

-- For PostgreSQL (commented out, will be conditionally executed based on database type)
-- ALTER TABLE test_positions ALTER COLUMN attributes TYPE JSONB USING attributes::JSONB;

-- Make address column nullable
ALTER TABLE test_positions MODIFY COLUMN address VARCHAR(512) NULL;

-- Version 4.0: Schema evolution - Add and drop indexes
-- =============================================

-- Add a new index for server_time for time-based queries
CREATE INDEX test_positions_server_time ON test_positions(server_time);

-- Add a composite index for device_id and status
CREATE INDEX test_positions_device_id_status ON test_positions(device_id, status);

-- Drop an obsolete index (if it exists)
-- This is wrapped in a procedure to handle different database dialects
DELIMITER //
CREATE PROCEDURE drop_index_if_exists()
BEGIN
    DECLARE index_exists INT;
    SELECT COUNT(1) INTO index_exists FROM information_schema.statistics 
    WHERE table_schema = DATABASE() AND table_name = 'test_positions' AND index_name = 'test_positions_device_id_fix_time';
    
    IF index_exists > 0 THEN
        DROP INDEX test_positions_device_id_fix_time ON test_positions;
    END IF;
END //
DELIMITER ;

CALL drop_index_if_exists();
DROP PROCEDURE IF EXISTS drop_index_if_exists;

-- Version 5.0: Schema evolution - Add new table for position processing
-- =============================================

-- Create a new table for position processing status
CREATE TABLE IF NOT EXISTS test_position_processing (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    position_id BIGINT NOT NULL,
    processed BOOLEAN DEFAULT FALSE,
    process_time TIMESTAMP,
    status VARCHAR(128),
    error_message VARCHAR(512),
    FOREIGN KEY (position_id) REFERENCES test_positions(id) ON DELETE CASCADE
);

-- Create index on position_id for efficient lookups
CREATE INDEX test_position_processing_position_id ON test_position_processing(position_id);

-- Create index on processed status for filtering
CREATE INDEX test_position_processing_processed ON test_position_processing(processed);

-- Version 6.0: Schema evolution - Table renaming and column dropping
-- =============================================

-- Rename the attributes table to a more specific name
ALTER TABLE test_position_attributes RENAME TO test_position_extended_attributes;

-- Drop obsolete columns from positions table
ALTER TABLE test_positions DROP COLUMN network;

-- Version 7.0: Schema evolution - Add partitioning support (MySQL 8.0+ example)
-- =============================================

-- Create a partitioned table for historical positions
CREATE TABLE IF NOT EXISTS test_position_history (
    id BIGINT PRIMARY KEY,
    device_id BIGINT NOT NULL,
    fix_time TIMESTAMP NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    attributes JSON,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (UNIX_TIMESTAMP(created_at)) (
    PARTITION p_oldest VALUES LESS THAN (UNIX_TIMESTAMP('2023-01-01 00:00:00')),
    PARTITION p_2023_q1 VALUES LESS THAN (UNIX_TIMESTAMP('2023-04-01 00:00:00')),
    PARTITION p_2023_q2 VALUES LESS THAN (UNIX_TIMESTAMP('2023-07-01 00:00:00')),
    PARTITION p_2023_q3 VALUES LESS THAN (UNIX_TIMESTAMP('2023-10-01 00:00:00')),
    PARTITION p_2023_q4 VALUES LESS THAN (UNIX_TIMESTAMP('2024-01-01 00:00:00')),
    PARTITION p_2024_q1 VALUES LESS THAN (UNIX_TIMESTAMP('2024-04-01 00:00:00')),
    PARTITION p_2024_q2 VALUES LESS THAN (UNIX_TIMESTAMP('2024-07-01 00:00:00')),
    PARTITION p_current VALUES LESS THAN MAXVALUE
);

-- Create necessary indexes on the partitioned table
CREATE INDEX test_position_history_device_id_fix_time ON test_position_history(device_id, fix_time);

-- Version 8.0: Schema evolution - Add stored procedures for position processing
-- =============================================

-- Create a stored procedure for cleaning up old positions
DELIMITER //
CREATE PROCEDURE test_cleanup_old_positions(IN retention_days INT)
BEGIN
    DECLARE cutoff_date TIMESTAMP;
    SET cutoff_date = DATE_SUB(NOW(), INTERVAL retention_days DAY);
    
    -- Move positions to history table before deletion
    INSERT INTO test_position_history (id, device_id, fix_time, latitude, longitude, attributes, created_at)
    SELECT id, device_id, fix_time, latitude, longitude, attributes, NOW()
    FROM test_positions
    WHERE fix_time < cutoff_date;
    
    -- Delete old positions
    DELETE FROM test_positions WHERE fix_time < cutoff_date;
    
    -- Return number of moved records
    SELECT ROW_COUNT() AS moved_records;
END //
DELIMITER ;

-- Create a stored procedure for position statistics
DELIMITER //
CREATE PROCEDURE test_get_position_stats(IN device_id_param BIGINT, IN from_date TIMESTAMP, IN to_date TIMESTAMP)
BEGIN
    SELECT 
        COUNT(*) as total_positions,
        MIN(fix_time) as first_position,
        MAX(fix_time) as last_position,
        AVG(speed) as avg_speed,
        MAX(speed) as max_speed,
        AVG(altitude) as avg_altitude
    FROM test_positions
    WHERE device_id = device_id_param
    AND fix_time BETWEEN from_date AND to_date;
END //
DELIMITER ;

-- Version 9.0: Schema evolution - Add triggers for data integrity
-- =============================================

-- Create a trigger to update the last_position timestamp in devices table
DELIMITER //
CREATE TRIGGER test_after_position_insert
AFTER INSERT ON test_positions
FOR EACH ROW
BEGIN
    -- This would update a last_position field in the devices table
    -- In a real scenario, we would update the actual tc_devices table
    -- For testing purposes, we'll just insert into a log table
    
    -- Create the log table if it doesn't exist
    CREATE TABLE IF NOT EXISTS test_position_log (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        device_id BIGINT NOT NULL,
        event_type VARCHAR(64) NOT NULL,
        position_id BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Log the position insert
    INSERT INTO test_position_log (device_id, event_type, position_id)
    VALUES (NEW.device_id, 'INSERT', NEW.id);
END //
DELIMITER ;

-- Version 10.0: Schema evolution - Add views for reporting
-- =============================================

-- Create a view for the latest positions of all devices
CREATE OR REPLACE VIEW test_latest_positions AS
SELECT p.*
FROM test_positions p
INNER JOIN (
    SELECT device_id, MAX(fix_time) as max_fix_time
    FROM test_positions
    GROUP BY device_id
) latest ON p.device_id = latest.device_id AND p.fix_time = latest.max_fix_time;

-- Create a view for daily distance summary
CREATE OR REPLACE VIEW test_daily_distance_summary AS
SELECT 
    device_id,
    DATE(fix_time) as day,
    COUNT(*) as position_count,
    MIN(fix_time) as first_position,
    MAX(fix_time) as last_position,
    SUM(speed) / COUNT(*) as avg_speed,
    MAX(speed) as max_speed
FROM test_positions
GROUP BY device_id, DATE(fix_time);

-- Final cleanup for test completion
-- =============================================

-- Add a test completion marker table
CREATE TABLE IF NOT EXISTS test_migration_completed (
    id INT PRIMARY KEY,
    version VARCHAR(32) NOT NULL,
    completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN DEFAULT TRUE
);

-- Insert a record to indicate successful migration test
INSERT INTO test_migration_completed (id, version, success)
VALUES (1, '10.0', TRUE);