-- Database cleanup script for test isolation
-- This script removes all test data and resets sequences between test runs
-- Ensures each test starts with a known database state
--
-- Usage:
-- 1. For PostgreSQL: psql -U username -d database_name -f cleanup.sql
-- 2. For MySQL/MariaDB: mysql -u username -p database_name < cleanup.sql (uncomment MySQL sections first)
-- 3. In Java with JPA/Hibernate: entityManager.createNativeQuery("path/to/cleanup.sql").executeUpdate();
-- 4. In Java with JDBC: statement.execute(FileUtils.readFileToString(new File("path/to/cleanup.sql")));
-- 5. With Testcontainers: Exec into container and run appropriate command or mount as volume

-- Disable foreign key constraints temporarily to allow truncating tables with dependencies
-- PostgreSQL syntax
SET CONSTRAINTS ALL DEFERRED;

-- MySQL/MariaDB syntax (uncomment if using MySQL/MariaDB)
-- SET FOREIGN_KEY_CHECKS = 0;

-- Truncate notification-related tables (with IF EXISTS for compatibility)
TRUNCATE TABLE IF EXISTS tc_notification_queue CASCADE;
TRUNCATE TABLE IF EXISTS tc_notification_deliveries CASCADE;
TRUNCATE TABLE IF EXISTS tc_notification_templates CASCADE;
TRUNCATE TABLE IF EXISTS tc_user_notification CASCADE;
TRUNCATE TABLE IF EXISTS tc_group_notification CASCADE;
TRUNCATE TABLE IF EXISTS tc_device_notification CASCADE;

-- Truncate event-related tables
TRUNCATE TABLE IF EXISTS tc_user_event CASCADE;
TRUNCATE TABLE IF EXISTS tc_group_event CASCADE;
TRUNCATE TABLE IF EXISTS tc_device_event CASCADE;

-- Truncate association tables (many-to-many relationships)
TRUNCATE TABLE IF EXISTS tc_user_device CASCADE;
TRUNCATE TABLE IF EXISTS tc_user_group CASCADE;
TRUNCATE TABLE IF EXISTS tc_group_device CASCADE;
TRUNCATE TABLE IF EXISTS tc_user_geofence CASCADE;
TRUNCATE TABLE IF EXISTS tc_group_geofence CASCADE;
TRUNCATE TABLE IF EXISTS tc_device_geofence CASCADE;
TRUNCATE TABLE IF EXISTS tc_user_command CASCADE;
TRUNCATE TABLE IF EXISTS tc_group_command CASCADE;
TRUNCATE TABLE IF EXISTS tc_device_command CASCADE;
TRUNCATE TABLE IF EXISTS tc_user_calendar CASCADE;
TRUNCATE TABLE IF EXISTS tc_group_calendar CASCADE;
TRUNCATE TABLE IF EXISTS tc_device_calendar CASCADE;
TRUNCATE TABLE IF EXISTS tc_user_attribute CASCADE;
TRUNCATE TABLE IF EXISTS tc_group_attribute CASCADE;
TRUNCATE TABLE IF EXISTS tc_device_attribute CASCADE;
TRUNCATE TABLE IF EXISTS tc_user_driver CASCADE;
TRUNCATE TABLE IF EXISTS tc_group_driver CASCADE;
TRUNCATE TABLE IF EXISTS tc_device_driver CASCADE;
TRUNCATE TABLE IF EXISTS tc_user_maintenance CASCADE;
TRUNCATE TABLE IF EXISTS tc_group_maintenance CASCADE;

-- Truncate dependent entity tables
TRUNCATE TABLE IF EXISTS tc_positions CASCADE;
TRUNCATE TABLE IF EXISTS tc_events CASCADE;
TRUNCATE TABLE IF EXISTS tc_reports CASCADE;
TRUNCATE TABLE IF EXISTS tc_statistics CASCADE;
TRUNCATE TABLE IF EXISTS tc_maintenance CASCADE;
TRUNCATE TABLE IF EXISTS tc_drivers CASCADE;
TRUNCATE TABLE IF EXISTS tc_attributes CASCADE;

-- Truncate core entity tables
TRUNCATE TABLE IF EXISTS tc_notifications CASCADE;
TRUNCATE TABLE IF EXISTS tc_devices CASCADE;
TRUNCATE TABLE IF EXISTS tc_groups CASCADE;
TRUNCATE TABLE IF EXISTS tc_geofences CASCADE;
TRUNCATE TABLE IF EXISTS tc_calendars CASCADE;
TRUNCATE TABLE IF EXISTS tc_commands CASCADE;
TRUNCATE TABLE IF EXISTS tc_users CASCADE;

-- Reset sequences to ensure consistent ID generation
-- PostgreSQL sequence reset syntax
-- Core entity sequences
ALTER SEQUENCE IF EXISTS tc_users_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS tc_devices_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS tc_positions_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS tc_events_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS tc_groups_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS tc_geofences_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS tc_notifications_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS tc_calendars_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS tc_commands_id_seq RESTART WITH 1;

-- Additional entity sequences
ALTER SEQUENCE IF EXISTS tc_attributes_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS tc_drivers_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS tc_maintenance_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS tc_reports_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS tc_statistics_id_seq RESTART WITH 1;

-- Notification-specific sequences
ALTER SEQUENCE IF EXISTS tc_notification_templates_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS tc_notification_deliveries_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS tc_notification_queue_id_seq RESTART WITH 1;

-- MySQL/MariaDB auto-increment reset syntax (uncomment if using MySQL/MariaDB)
-- ALTER TABLE tc_users AUTO_INCREMENT = 1;
-- ALTER TABLE tc_devices AUTO_INCREMENT = 1;
-- ALTER TABLE tc_positions AUTO_INCREMENT = 1;
-- ALTER TABLE tc_events AUTO_INCREMENT = 1;
-- ALTER TABLE tc_groups AUTO_INCREMENT = 1;
-- ALTER TABLE tc_geofences AUTO_INCREMENT = 1;
-- ALTER TABLE tc_notifications AUTO_INCREMENT = 1;
-- ALTER TABLE tc_calendars AUTO_INCREMENT = 1;
-- ALTER TABLE tc_commands AUTO_INCREMENT = 1;
-- ALTER TABLE tc_attributes AUTO_INCREMENT = 1;
-- ALTER TABLE tc_drivers AUTO_INCREMENT = 1;
-- ALTER TABLE tc_maintenance AUTO_INCREMENT = 1;
-- ALTER TABLE tc_reports AUTO_INCREMENT = 1;
-- ALTER TABLE tc_statistics AUTO_INCREMENT = 1;
-- ALTER TABLE tc_notification_templates AUTO_INCREMENT = 1;
-- ALTER TABLE tc_notification_deliveries AUTO_INCREMENT = 1;
-- ALTER TABLE tc_notification_queue AUTO_INCREMENT = 1;

-- Re-enable foreign key constraints
-- PostgreSQL syntax
SET CONSTRAINTS ALL IMMEDIATE;

-- MySQL/MariaDB syntax (uncomment if using MySQL/MariaDB)
-- SET FOREIGN_KEY_CHECKS = 1;

-- Add compatibility for different database engines
-- For MySQL/MariaDB, use the following instead of VACUUM ANALYZE:
-- ANALYZE TABLE tc_users, tc_devices, tc_positions, tc_events, tc_groups, tc_geofences, tc_notifications;

-- For PostgreSQL, reclaim storage and update statistics
VACUUM ANALYZE;