-- Database schema initialization script for integration testing
-- This file defines the schema for test databases with tables that mirror the production database structure

-- Core entities

-- Users table
CREATE TABLE IF NOT EXISTS tc_users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    email VARCHAR(128) NOT NULL UNIQUE,
    hashedpassword VARCHAR(128),
    salt VARCHAR(128),
    readonly BOOLEAN DEFAULT false NOT NULL,
    administrator BOOLEAN DEFAULT false NOT NULL,
    map VARCHAR(128),
    latitude DOUBLE PRECISION DEFAULT 0,
    longitude DOUBLE PRECISION DEFAULT 0,
    zoom INT DEFAULT 0,
    twelvehourampm BOOLEAN DEFAULT false NOT NULL,
    coordinateformat VARCHAR(128),
    disabled BOOLEAN DEFAULT false NOT NULL,
    expirationtime TIMESTAMP,
    devicelimit INT DEFAULT -1 NOT NULL,
    userlimit INT DEFAULT 0 NOT NULL,
    devicereadonly BOOLEAN DEFAULT false NOT NULL,
    phone VARCHAR(128),
    limitcommands BOOLEAN DEFAULT false NOT NULL,
    login VARCHAR(128),
    poilayer VARCHAR(512),
    timezone VARCHAR(128),
    language VARCHAR(128),
    token VARCHAR(128),
    attributes VARCHAR(4000)
);

-- Devices table
CREATE TABLE IF NOT EXISTS tc_devices (
    id SERIAL PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    uniqueid VARCHAR(128) NOT NULL UNIQUE,
    lastupdate TIMESTAMP,
    positionid INT,
    groupid INT,
    attributes VARCHAR(4000),
    phone VARCHAR(128),
    model VARCHAR(128),
    contact VARCHAR(512),
    category VARCHAR(128),
    disabled BOOLEAN DEFAULT false NOT NULL,
    status VARCHAR(128),
    lastpositionupdate TIMESTAMP,
    geofenceids VARCHAR(4000)
);

-- Positions table
CREATE TABLE IF NOT EXISTS tc_positions (
    id SERIAL PRIMARY KEY,
    protocol VARCHAR(128),
    deviceid INT NOT NULL,
    servertime TIMESTAMP NOT NULL,
    devicetime TIMESTAMP NOT NULL,
    fixtime TIMESTAMP NOT NULL,
    valid BOOLEAN NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    altitude DOUBLE PRECISION NOT NULL,
    speed DOUBLE PRECISION NOT NULL,
    course DOUBLE PRECISION NOT NULL,
    address VARCHAR(512),
    attributes VARCHAR(4000),
    accuracy DOUBLE PRECISION DEFAULT 0 NOT NULL,
    network VARCHAR(4000),
    FOREIGN KEY (deviceid) REFERENCES tc_devices(id) ON DELETE CASCADE
);

-- Events table
CREATE TABLE IF NOT EXISTS tc_events (
    id SERIAL PRIMARY KEY,
    type VARCHAR(128) NOT NULL,
    servertime TIMESTAMP NOT NULL,
    deviceid INT,
    positionid INT,
    geofenceid INT,
    attributes VARCHAR(4000),
    maintenanceid INT,
    FOREIGN KEY (deviceid) REFERENCES tc_devices(id) ON DELETE CASCADE,
    FOREIGN KEY (positionid) REFERENCES tc_positions(id) ON DELETE CASCADE
);

-- Groups table
CREATE TABLE IF NOT EXISTS tc_groups (
    id SERIAL PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    groupid INT,
    attributes VARCHAR(4000),
    FOREIGN KEY (groupid) REFERENCES tc_groups(id) ON DELETE CASCADE
);

-- Geofences table
CREATE TABLE IF NOT EXISTS tc_geofences (
    id SERIAL PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(512),
    area VARCHAR(4000) NOT NULL,
    calendarid INT,
    attributes VARCHAR(4000)
);

-- Notifications table
CREATE TABLE IF NOT EXISTS tc_notifications (
    id SERIAL PRIMARY KEY,
    type VARCHAR(128) NOT NULL,
    attributes VARCHAR(4000),
    always BOOLEAN DEFAULT false NOT NULL,
    calendarid INT,
    notificators VARCHAR(128)
);

-- Commands table
CREATE TABLE IF NOT EXISTS tc_commands (
    id SERIAL PRIMARY KEY,
    description VARCHAR(512),
    type VARCHAR(128) NOT NULL,
    textchannel BOOLEAN NOT NULL DEFAULT false,
    attributes VARCHAR(4000)
);

-- Calendars table
CREATE TABLE IF NOT EXISTS tc_calendars (
    id SERIAL PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    data VARCHAR(4000) NOT NULL,
    attributes VARCHAR(4000)
);

-- Maintenances table
CREATE TABLE IF NOT EXISTS tc_maintenances (
    id SERIAL PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    type VARCHAR(128) NOT NULL,
    start DOUBLE PRECISION NOT NULL DEFAULT 0,
    period DOUBLE PRECISION NOT NULL DEFAULT 0,
    attributes VARCHAR(4000)
);

-- Servers table
CREATE TABLE IF NOT EXISTS tc_servers (
    id SERIAL PRIMARY KEY,
    registration BOOLEAN NOT NULL DEFAULT false,
    readonly BOOLEAN NOT NULL DEFAULT false,
    devicereadonly BOOLEAN NOT NULL DEFAULT false,
    map VARCHAR(128),
    bingkey VARCHAR(128),
    mapurl VARCHAR(512),
    latitude DOUBLE PRECISION DEFAULT 0,
    longitude DOUBLE PRECISION DEFAULT 0,
    zoom INT DEFAULT 0,
    twelvehourampm BOOLEAN NOT NULL DEFAULT false,
    attributes VARCHAR(4000),
    coordinateformat VARCHAR(128),
    devicelimit INT DEFAULT -1 NOT NULL,
    userlimit INT DEFAULT 0 NOT NULL,
    forcesettings BOOLEAN NOT NULL DEFAULT false,
    poilayer VARCHAR(512),
    announcement VARCHAR(4000),
    timezone VARCHAR(128),
    language VARCHAR(128)
);

-- Statistics table
CREATE TABLE IF NOT EXISTS tc_statistics (
    id SERIAL PRIMARY KEY,
    capturetime TIMESTAMP NOT NULL,
    activeusers INT DEFAULT 0 NOT NULL,
    activedevices INT DEFAULT 0 NOT NULL,
    requests INT DEFAULT 0 NOT NULL,
    messagesreceived INT DEFAULT 0 NOT NULL,
    messagesstored INT DEFAULT 0 NOT NULL,
    attributes VARCHAR(4000),
    mailsent INT DEFAULT 0 NOT NULL,
    smssent INT DEFAULT 0 NOT NULL,
    geocoderrequests INT DEFAULT 0 NOT NULL,
    geolocationrequests INT DEFAULT 0 NOT NULL
);

-- User-Device relationship table
CREATE TABLE IF NOT EXISTS tc_user_device (
    userid INT NOT NULL,
    deviceid INT NOT NULL,
    PRIMARY KEY (userid, deviceid),
    FOREIGN KEY (userid) REFERENCES tc_users(id) ON DELETE CASCADE,
    FOREIGN KEY (deviceid) REFERENCES tc_devices(id) ON DELETE CASCADE
);

-- Group-Device relationship table
CREATE TABLE IF NOT EXISTS tc_group_device (
    groupid INT NOT NULL,
    deviceid INT NOT NULL,
    PRIMARY KEY (groupid, deviceid),
    FOREIGN KEY (groupid) REFERENCES tc_groups(id) ON DELETE CASCADE,
    FOREIGN KEY (deviceid) REFERENCES tc_devices(id) ON DELETE CASCADE
);

-- User-Group relationship table
CREATE TABLE IF NOT EXISTS tc_user_group (
    userid INT NOT NULL,
    groupid INT NOT NULL,
    PRIMARY KEY (userid, groupid),
    FOREIGN KEY (userid) REFERENCES tc_users(id) ON DELETE CASCADE,
    FOREIGN KEY (groupid) REFERENCES tc_groups(id) ON DELETE CASCADE
);

-- Geofence permission tables
CREATE TABLE IF NOT EXISTS tc_user_geofence (
    userid INT NOT NULL,
    geofenceid INT NOT NULL,
    PRIMARY KEY (userid, geofenceid),
    FOREIGN KEY (userid) REFERENCES tc_users(id) ON DELETE CASCADE,
    FOREIGN KEY (geofenceid) REFERENCES tc_geofences(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tc_group_geofence (
    groupid INT NOT NULL,
    geofenceid INT NOT NULL,
    PRIMARY KEY (groupid, geofenceid),
    FOREIGN KEY (groupid) REFERENCES tc_groups(id) ON DELETE CASCADE,
    FOREIGN KEY (geofenceid) REFERENCES tc_geofences(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tc_device_geofence (
    deviceid INT NOT NULL,
    geofenceid INT NOT NULL,
    PRIMARY KEY (deviceid, geofenceid),
    FOREIGN KEY (deviceid) REFERENCES tc_devices(id) ON DELETE CASCADE,
    FOREIGN KEY (geofenceid) REFERENCES tc_geofences(id) ON DELETE CASCADE
);

-- Notification permission tables
CREATE TABLE IF NOT EXISTS tc_user_notification (
    userid INT NOT NULL,
    notificationid INT NOT NULL,
    PRIMARY KEY (userid, notificationid),
    FOREIGN KEY (userid) REFERENCES tc_users(id) ON DELETE CASCADE,
    FOREIGN KEY (notificationid) REFERENCES tc_notifications(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tc_group_notification (
    groupid INT NOT NULL,
    notificationid INT NOT NULL,
    PRIMARY KEY (groupid, notificationid),
    FOREIGN KEY (groupid) REFERENCES tc_groups(id) ON DELETE CASCADE,
    FOREIGN KEY (notificationid) REFERENCES tc_notifications(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tc_device_notification (
    deviceid INT NOT NULL,
    notificationid INT NOT NULL,
    PRIMARY KEY (deviceid, notificationid),
    FOREIGN KEY (deviceid) REFERENCES tc_devices(id) ON DELETE CASCADE,
    FOREIGN KEY (notificationid) REFERENCES tc_notifications(id) ON DELETE CASCADE
);

-- Calendar permission tables
CREATE TABLE IF NOT EXISTS tc_user_calendar (
    userid INT NOT NULL,
    calendarid INT NOT NULL,
    PRIMARY KEY (userid, calendarid),
    FOREIGN KEY (userid) REFERENCES tc_users(id) ON DELETE CASCADE,
    FOREIGN KEY (calendarid) REFERENCES tc_calendars(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tc_group_calendar (
    groupid INT NOT NULL,
    calendarid INT NOT NULL,
    PRIMARY KEY (groupid, calendarid),
    FOREIGN KEY (groupid) REFERENCES tc_groups(id) ON DELETE CASCADE,
    FOREIGN KEY (calendarid) REFERENCES tc_calendars(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tc_device_calendar (
    deviceid INT NOT NULL,
    calendarid INT NOT NULL,
    PRIMARY KEY (deviceid, calendarid),
    FOREIGN KEY (deviceid) REFERENCES tc_devices(id) ON DELETE CASCADE,
    FOREIGN KEY (calendarid) REFERENCES tc_calendars(id) ON DELETE CASCADE
);

-- User-Command relationship table
CREATE TABLE IF NOT EXISTS tc_user_command (
    userid INT NOT NULL,
    commandid INT NOT NULL,
    PRIMARY KEY (userid, commandid),
    FOREIGN KEY (userid) REFERENCES tc_users(id) ON DELETE CASCADE,
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

-- Group-Command relationship table
CREATE TABLE IF NOT EXISTS tc_group_command (
    groupid INT NOT NULL,
    commandid INT NOT NULL,
    PRIMARY KEY (groupid, commandid),
    FOREIGN KEY (groupid) REFERENCES tc_groups(id) ON DELETE CASCADE,
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

-- Device-Command relationship table
CREATE TABLE IF NOT EXISTS tc_device_command (
    deviceid INT NOT NULL,
    commandid INT NOT NULL,
    PRIMARY KEY (deviceid, commandid),
    FOREIGN KEY (deviceid) REFERENCES tc_devices(id) ON DELETE CASCADE,
    FOREIGN KEY (commandid) REFERENCES tc_commands(id) ON DELETE CASCADE
);

-- Device-Maintenance relationship table
CREATE TABLE IF NOT EXISTS tc_device_maintenance (
    deviceid INT NOT NULL,
    maintenanceid INT NOT NULL,
    PRIMARY KEY (deviceid, maintenanceid),
    FOREIGN KEY (deviceid) REFERENCES tc_devices(id) ON DELETE CASCADE,
    FOREIGN KEY (maintenanceid) REFERENCES tc_maintenances(id) ON DELETE CASCADE
);

-- User-Maintenance relationship table
CREATE TABLE IF NOT EXISTS tc_user_maintenance (
    userid INT NOT NULL,
    maintenanceid INT NOT NULL,
    PRIMARY KEY (userid, maintenanceid),
    FOREIGN KEY (userid) REFERENCES tc_users(id) ON DELETE CASCADE,
    FOREIGN KEY (maintenanceid) REFERENCES tc_maintenances(id) ON DELETE CASCADE
);

-- Group-Maintenance relationship table
CREATE TABLE IF NOT EXISTS tc_group_maintenance (
    groupid INT NOT NULL,
    maintenanceid INT NOT NULL,
    PRIMARY KEY (groupid, maintenanceid),
    FOREIGN KEY (groupid) REFERENCES tc_groups(id) ON DELETE CASCADE,
    FOREIGN KEY (maintenanceid) REFERENCES tc_maintenances(id) ON DELETE CASCADE
);

-- User-Driver relationship table
CREATE TABLE IF NOT EXISTS tc_user_driver (
    userid INT NOT NULL,
    driverid INT NOT NULL,
    PRIMARY KEY (userid, driverid),
    FOREIGN KEY (userid) REFERENCES tc_users(id) ON DELETE CASCADE
);

-- Group-Driver relationship table
CREATE TABLE IF NOT EXISTS tc_group_driver (
    groupid INT NOT NULL,
    driverid INT NOT NULL,
    PRIMARY KEY (groupid, driverid),
    FOREIGN KEY (groupid) REFERENCES tc_groups(id) ON DELETE CASCADE
);

-- Drivers table
CREATE TABLE IF NOT EXISTS tc_drivers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    uniqueid VARCHAR(128) NOT NULL UNIQUE,
    attributes VARCHAR(4000)
);

-- Device-Driver relationship table
CREATE TABLE IF NOT EXISTS tc_device_driver (
    deviceid INT NOT NULL,
    driverid INT NOT NULL,
    PRIMARY KEY (deviceid, driverid),
    FOREIGN KEY (deviceid) REFERENCES tc_devices(id) ON DELETE CASCADE,
    FOREIGN KEY (driverid) REFERENCES tc_drivers(id) ON DELETE CASCADE
);

-- Attributes table
CREATE TABLE IF NOT EXISTS tc_attributes (
    id SERIAL PRIMARY KEY,
    description VARCHAR(4000),
    type VARCHAR(128),
    attribute VARCHAR(128),
    expression VARCHAR(4000)
);

-- User-Attribute relationship table
CREATE TABLE IF NOT EXISTS tc_user_attribute (
    userid INT NOT NULL,
    attributeid INT NOT NULL,
    PRIMARY KEY (userid, attributeid),
    FOREIGN KEY (userid) REFERENCES tc_users(id) ON DELETE CASCADE,
    FOREIGN KEY (attributeid) REFERENCES tc_attributes(id) ON DELETE CASCADE
);

-- Group-Attribute relationship table
CREATE TABLE IF NOT EXISTS tc_group_attribute (
    groupid INT NOT NULL,
    attributeid INT NOT NULL,
    PRIMARY KEY (groupid, attributeid),
    FOREIGN KEY (groupid) REFERENCES tc_groups(id) ON DELETE CASCADE,
    FOREIGN KEY (attributeid) REFERENCES tc_attributes(id) ON DELETE CASCADE
);

-- Device-Attribute relationship table
CREATE TABLE IF NOT EXISTS tc_device_attribute (
    deviceid INT NOT NULL,
    attributeid INT NOT NULL,
    PRIMARY KEY (deviceid, attributeid),
    FOREIGN KEY (deviceid) REFERENCES tc_devices(id) ON DELETE CASCADE,
    FOREIGN KEY (attributeid) REFERENCES tc_attributes(id) ON DELETE CASCADE
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_tc_positions_deviceid_fixtime ON tc_positions(deviceid, fixtime);
CREATE INDEX IF NOT EXISTS idx_tc_events_deviceid_servertime ON tc_events(deviceid, servertime);
CREATE INDEX IF NOT EXISTS idx_tc_devices_uniqueid ON tc_devices(uniqueid);
CREATE INDEX IF NOT EXISTS idx_tc_users_email ON tc_users(email);
CREATE INDEX IF NOT EXISTS idx_tc_users_login ON tc_users(login);
CREATE INDEX IF NOT EXISTS idx_tc_user_device_userid ON tc_user_device(userid);
CREATE INDEX IF NOT EXISTS idx_tc_user_device_deviceid ON tc_user_device(deviceid);
CREATE INDEX IF NOT EXISTS idx_tc_group_device_groupid ON tc_group_device(groupid);