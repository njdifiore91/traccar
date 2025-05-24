-- Database cleanup script for Event Service tests
-- This script ensures that each test starts with a clean database state
-- by removing all test data from event-related tables

-- Start transaction for atomic cleanup
BEGIN;

-- Disable foreign key checks temporarily to allow truncating tables with foreign key relationships
-- Note: Syntax may vary by database vendor (this works for MySQL/MariaDB)
SET FOREIGN_KEY_CHECKS = 0;

-- Clean up association tables first (many-to-many relationships)
TRUNCATE TABLE tc_user_event;
TRUNCATE TABLE tc_device_event;
TRUNCATE TABLE tc_group_event;

TRUNCATE TABLE tc_user_geofence;
TRUNCATE TABLE tc_device_geofence;
TRUNCATE TABLE tc_group_geofence;

-- Clean up main event-related tables
TRUNCATE TABLE tc_events;
TRUNCATE TABLE tc_geofences;

-- Re-enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;

-- Commit transaction
COMMIT;

-- Add PostgreSQL-specific version (commented out, uncomment if using PostgreSQL)
/*
BEGIN;

-- Disable triggers temporarily
SET session_replication_role = 'replica';

-- Clean up association tables first (many-to-many relationships)
TRUNCATE TABLE tc_user_event CASCADE;
TRUNCATE TABLE tc_device_event CASCADE;
TRUNCATE TABLE tc_group_event CASCADE;

TRUNCATE TABLE tc_user_geofence CASCADE;
TRUNCATE TABLE tc_device_geofence CASCADE;
TRUNCATE TABLE tc_group_geofence CASCADE;

-- Clean up main event-related tables
TRUNCATE TABLE tc_events CASCADE;
TRUNCATE TABLE tc_geofences CASCADE;

-- Re-enable triggers
SET session_replication_role = 'origin';

COMMIT;
*/

-- Add SQL Server-specific version (commented out, uncomment if using SQL Server)
/*
BEGIN TRANSACTION;

-- Disable constraints
EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL';

-- Clean up association tables first (many-to-many relationships)
TRUNCATE TABLE tc_user_event;
TRUNCATE TABLE tc_device_event;
TRUNCATE TABLE tc_group_event;

TRUNCATE TABLE tc_user_geofence;
TRUNCATE TABLE tc_device_geofence;
TRUNCATE TABLE tc_group_geofence;

-- Clean up main event-related tables
TRUNCATE TABLE tc_events;
TRUNCATE TABLE tc_geofences;

-- Re-enable constraints
EXEC sp_MSforeachtable 'ALTER TABLE ? CHECK CONSTRAINT ALL';

COMMIT TRANSACTION;
*/