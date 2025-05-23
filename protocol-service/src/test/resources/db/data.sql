-- Protocol Service Test Data
-- This file contains test data for Protocol Service testing, including command definitions,
-- device-command associations, and command templates for various protocols.

-- Clear existing test data to ensure clean state
DELETE FROM tc_device_command;
DELETE FROM tc_user_command;
DELETE FROM tc_group_command;
DELETE FROM tc_commands;

-- Command Types
-- Insert command definitions for different protocols
INSERT INTO tc_commands (id, type, description) VALUES 
(1, 'custom', 'Custom command'),
(2, 'positionSingle', 'Request single position'),
(3, 'engineStop', 'Engine stop command'),
(4, 'engineResume', 'Engine resume command'),
(5, 'alarmArm', 'Arm alarm'),
(6, 'alarmDisarm', 'Disarm alarm'),
(7, 'setTimezone', 'Set timezone'),
(8, 'requestPhoto', 'Request photo'),
(9, 'powerOff', 'Power off device'),
(10, 'rebootDevice', 'Reboot device');

-- Protocol-specific command templates
-- These templates are used to format commands for specific protocols

-- H02 Protocol Commands
INSERT INTO tc_commands (id, type, description, textchannel, attributes) VALUES
(101, 'custom', 'H02 Custom Command', true, '{"data":"*HQ,{uniqueId},S71,0,5678#"}'),
(102, 'positionSingle', 'H02 Position Request', true, '{"data":"*HQ,{uniqueId},S71,0,0001#"}'),
(103, 'engineStop', 'H02 Engine Stop', true, '{"data":"*HQ,{uniqueId},S20,0,0,0,0,0,0#"}'),
(104, 'engineResume', 'H02 Engine Resume', true, '{"data":"*HQ,{uniqueId},S20,1,0,0,0,0,0#"}');

-- GT06 Protocol Commands
INSERT INTO tc_commands (id, type, description, textchannel, attributes) VALUES
(201, 'custom', 'GT06 Custom Command', true, '{"data":"({uniqueId},A70,{data})"}'),
(202, 'positionSingle', 'GT06 Position Request', true, '{"data":"({uniqueId},A00)"}'),
(203, 'engineStop', 'GT06 Engine Stop', true, '{"data":"({uniqueId},A12,0)"}'),
(204, 'engineResume', 'GT06 Engine Resume', true, '{"data":"({uniqueId},A12,1)"}'),
(205, 'setTimezone', 'GT06 Set Timezone', true, '{"data":"({uniqueId},A63,{timezone})"}');

-- Teltonika Protocol Commands
INSERT INTO tc_commands (id, type, description, textchannel, attributes) VALUES
(301, 'custom', 'Teltonika Custom Command', false, '{"data":"setparam {key} {value}"}'),
(302, 'positionSingle', 'Teltonika Position Request', false, '{"data":"getgps"}'),
(303, 'engineStop', 'Teltonika Engine Stop', false, '{"data":"setdigout 0 1"}'),
(304, 'engineResume', 'Teltonika Engine Resume', false, '{"data":"setdigout 0 0"}'),
(305, 'rebootDevice', 'Teltonika Reboot', false, '{"data":"reboot"}');

-- Meitrack Protocol Commands
INSERT INTO tc_commands (id, type, description, textchannel, attributes) VALUES
(401, 'custom', 'Meitrack Custom Command', true, '{"data":"@@{uniqueId}@MT90,{data}*"}'),
(402, 'positionSingle', 'Meitrack Position Request', true, '{"data":"@@{uniqueId}@MT90,A10*"}'),
(403, 'engineStop', 'Meitrack Engine Stop', true, '{"data":"@@{uniqueId}@MT90,C01,1*"}'),
(404, 'engineResume', 'Meitrack Engine Resume', true, '{"data":"@@{uniqueId}@MT90,C01,0*"}'),
(405, 'alarmArm', 'Meitrack Alarm Arm', true, '{"data":"@@{uniqueId}@MT90,B21,1*"}'),
(406, 'alarmDisarm', 'Meitrack Alarm Disarm', true, '{"data":"@@{uniqueId}@MT90,B21,0*"}');

-- TK103 Protocol Commands
INSERT INTO tc_commands (id, type, description, textchannel, attributes) VALUES
(501, 'custom', 'TK103 Custom Command', true, '{"data":"{data}"}'),
(502, 'positionSingle', 'TK103 Position Request', true, '{"data":"({uniqueId}AP00)"}'),
(503, 'engineStop', 'TK103 Engine Stop', true, '{"data":"({uniqueId}AS10)"}'),
(504, 'engineResume', 'TK103 Engine Resume', true, '{"data":"({uniqueId}AS11)"}'),
(505, 'requestPhoto', 'TK103 Request Photo', true, '{"data":"({uniqueId}D03)"}');

-- Coban Protocol Commands
INSERT INTO tc_commands (id, type, description, textchannel, attributes) VALUES
(601, 'custom', 'Coban Custom Command', true, '{"data":"{data}"}'),
(602, 'positionSingle', 'Coban Position Request', true, '{"data":"*{uniqueId},000#"}'),
(603, 'engineStop', 'Coban Engine Stop', true, '{"data":"*{uniqueId},125#"}'),
(604, 'engineResume', 'Coban Engine Resume', true, '{"data":"*{uniqueId},126#"}'),
(605, 'powerOff', 'Coban Power Off', true, '{"data":"*{uniqueId},127#"}');

-- Totem Protocol Commands
INSERT INTO tc_commands (id, type, description, textchannel, attributes) VALUES
(701, 'custom', 'Totem Custom Command', true, '{"data":"$$0{uniqueId}AAA{data}"}'),
(702, 'positionSingle', 'Totem Position Request', true, '{"data":"$$0{uniqueId}AAA27"}'),
(703, 'engineStop', 'Totem Engine Stop', true, '{"data":"$$0{uniqueId}AAA28,0"}'),
(704, 'engineResume', 'Totem Engine Resume', true, '{"data":"$$0{uniqueId}AAA28,1"}'),
(705, 'rebootDevice', 'Totem Reboot Device', true, '{"data":"$$0{uniqueId}AAA99"}');

-- ATrack Protocol Commands
INSERT INTO tc_commands (id, type, description, textchannel, attributes) VALUES
(801, 'custom', 'ATrack Custom Command', false, '{"data":"AT$ATRK,{uniqueId},{data}"}'),
(802, 'positionSingle', 'ATrack Position Request', false, '{"data":"AT$ATRK,{uniqueId},QP"}'),
(803, 'engineStop', 'ATrack Engine Stop', false, '{"data":"AT$ATRK,{uniqueId},DO1,1"}'),
(804, 'engineResume', 'ATrack Engine Resume', false, '{"data":"AT$ATRK,{uniqueId},DO1,0"}');

-- Device-Command Associations
-- These associations link specific commands to test devices

-- Test Device 1 - H02 Protocol
INSERT INTO tc_device_command (deviceid, commandid) VALUES
(1, 101), (1, 102), (1, 103), (1, 104);

-- Test Device 2 - GT06 Protocol
INSERT INTO tc_device_command (deviceid, commandid) VALUES
(2, 201), (2, 202), (2, 203), (2, 204), (2, 205);

-- Test Device 3 - Teltonika Protocol
INSERT INTO tc_device_command (deviceid, commandid) VALUES
(3, 301), (3, 302), (3, 303), (3, 304), (3, 305);

-- Test Device 4 - Meitrack Protocol
INSERT INTO tc_device_command (deviceid, commandid) VALUES
(4, 401), (4, 402), (4, 403), (4, 404), (4, 405), (4, 406);

-- Test Device 5 - TK103 Protocol
INSERT INTO tc_device_command (deviceid, commandid) VALUES
(5, 501), (5, 502), (5, 503), (5, 504), (5, 505);

-- Test Device 6 - Coban Protocol
INSERT INTO tc_device_command (deviceid, commandid) VALUES
(6, 601), (6, 602), (6, 603), (6, 604), (6, 605);

-- Test Device 7 - Totem Protocol
INSERT INTO tc_device_command (deviceid, commandid) VALUES
(7, 701), (7, 702), (7, 703), (7, 704), (7, 705);

-- Test Device 8 - ATrack Protocol
INSERT INTO tc_device_command (deviceid, commandid) VALUES
(8, 801), (8, 802), (8, 803), (8, 804);

-- User-Command Associations for testing permission-based command access
INSERT INTO tc_user_command (userid, commandid) VALUES
(1, 1), (1, 2), (1, 3), (1, 4), (1, 5),  -- Admin user has access to generic commands
(2, 1), (2, 2),                          -- Regular user has limited access
(3, 1), (3, 2), (3, 3);                  -- Manager has moderate access

-- Group-Command Associations for testing group-based command access
INSERT INTO tc_group_command (groupid, commandid) VALUES
(1, 1), (1, 2), (1, 3), (1, 4),          -- Admin group has extended access
(2, 1), (2, 2);                          -- User group has basic access