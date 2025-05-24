-- Database cleanup script for Position Service tests
-- This script ensures that each test starts with a clean database state
-- by removing all test data from position-related tables

-- Start transaction for atomic cleanup
BEGIN;

-- Disable foreign key checks temporarily to allow truncating tables with foreign key relationships
-- Note: Syntax may vary by database vendor (MySQL/MariaDB shown here)
SET FOREIGN_KEY_CHECKS = 0;

-- Clean up position-related events first (if they exist in this service's database)
TRUNCATE TABLE tc_events;

-- Clean up position forwarding status (if exists)
TRUNCATE TABLE tc_position_forward_status;

-- Clean up any position-related computed data
TRUNCATE TABLE tc_computed_attributes;

-- Clean up position attributes table (if exists)
TRUNCATE TABLE tc_position_attributes;

-- Clean up device positions (latest position references)
UPDATE tc_devices SET positionid = NULL WHERE positionid IS NOT NULL;

-- Clean up geofence positions (if exists)
TRUNCATE TABLE tc_geofence_positions;

-- Clean up trip positions (if exists)
TRUNCATE TABLE tc_trip_positions;

-- Clean up route positions (if exists)
TRUNCATE TABLE tc_route_positions;

-- Clean up the main positions table
TRUNCATE TABLE tc_positions;

-- Re-enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;

-- Commit the transaction
COMMIT;

-- Add database-specific commands (commented out by default)
-- MySQL/MariaDB: Optimize tables after truncation
-- OPTIMIZE TABLE tc_positions, tc_events, tc_position_attributes;

-- PostgreSQL: Vacuum tables after truncation
-- VACUUM ANALYZE tc_positions, tc_events, tc_position_attributes;