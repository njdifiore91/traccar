# Apel Protocol Documentation

## Overview

The Apel protocol is a binary protocol used by Apel GPS tracking devices for transmitting location and telemetry data. This protocol uses a length-field based frame structure with little-endian byte ordering and supports various message types for device identification, position reporting, sensor data, and event logging.

## Protocol Specification

### Communication Parameters

- **Transport Protocol**: TCP
- **Default Port**: Not specified (configured in Traccar setup)
- **Byte Order**: Little-endian
- **Character Encoding**: ASCII (for text fields)

### Frame Structure

The Apel protocol uses a length-field based frame structure with the following format:

| Field | Size (bytes) | Description |
|-------|--------------|-------------|
| Message Type | 2 | Identifies the type of message |
| Length | 2 | Length of the payload in bytes |
| Payload | Variable | Message-specific data |
| Checksum | 4 | CRC32 checksum of the message |

The most significant bit of the message type field (0x8000) is used to indicate an alarm condition.

### Message Types

The protocol supports various message types, including:

| Type ID | Name | Description |
|---------|------|-------------|
| 0 | MSG_NULL | Null message |
| 10 | MSG_REQUEST_TRACKER_ID | Request for tracker identification |
| 11 | MSG_TRACKER_ID | Tracker identification response |
| 12 | MSG_TRACKER_ID_EXT | Extended tracker identification response |
| 20 | MSG_DISCONNECT | Disconnect notification |
| 30 | MSG_REQUEST_PASSWORD | Password request |
| 31 | MSG_PASSWORD | Password response |
| 90 | MSG_REQUEST_STATE_FULL_INFO | Request for full state information |
| 92 | MSG_STATE_FULL_INFO_T104 | Full state information for T104 model |
| 100 | MSG_REQUEST_CURRENT_GPS_DATA | Request for current GPS data |
| 101 | MSG_CURRENT_GPS_DATA | Current GPS data response |
| 110 | MSG_REQUEST_SENSORS_STATE | Request for sensor state |
| 111 | MSG_SENSORS_STATE | Sensor state response |
| 112 | MSG_SENSORS_STATE_T100 | Sensor state for T100 model |
| 113 | MSG_SENSORS_STATE_T100_4 | Sensor state for T100_4 model |
| 120 | MSG_REQUEST_LAST_LOG_INDEX | Request for last log index |
| 121 | MSG_LAST_LOG_INDEX | Last log index response |
| 130 | MSG_REQUEST_LOG_RECORDS | Request for log records |
| 131 | MSG_LOG_RECORDS | Log records response |
| 141 | MSG_EVENT | Event notification |
| 150 | MSG_TEXT | Text message |
| 160 | MSG_ACK_ALARM | Alarm acknowledgment |
| 170 | MSG_SET_TRACKER_MODE | Set tracker mode command |
| 180 | MSG_GPRS_COMMAND | GPRS command |

### Key Message Formats

#### MSG_TRACKER_ID_EXT (12)

Extended tracker identification message:

| Field | Size (bytes) | Description |
|-------|--------------|-------------|
| ID | 4 | Device ID |
| Length1 | 2 | Length of first data block |
| Data1 | Variable | First data block |
| Length2 | 2 | Length of second data block (device identifier) |
| Data2 | Variable | Device identifier (ASCII) |

#### MSG_CURRENT_GPS_DATA (101)

Current GPS data message:

| Field | Size (bytes) | Description |
|-------|--------------|-------------|
| Timestamp | 4 | Unix timestamp (seconds) |
| Latitude | 4 | Latitude (scaled by 0x7FFFFFFF/180) |
| Longitude | 4 | Longitude (scaled by 0x7FFFFFFF/180) |
| Speed | 2 | Speed in km/h * 100 (-1 for invalid) |
| Course | 2 | Course in degrees * 100 |
| Altitude | 2 | Altitude in meters |

#### MSG_STATE_FULL_INFO_T104 (92)

Full state information message for T104 model:

| Field | Size (bytes) | Description |
|-------|--------------|-------------|
| Timestamp | 4 | Unix timestamp (seconds) |
| Latitude | 4 | Latitude (scaled by 0x7FFFFFFF/180) |
| Longitude | 4 | Longitude (scaled by 0x7FFFFFFF/180) |
| Speed | 1 | Speed in km/h (255 for invalid) |
| HDOP | 1 | Horizontal dilution of precision |
| Course | 2 | Course in degrees * 100 |
| Altitude | 2 | Altitude in meters |
| Satellites | 1 | Number of satellites |
| RSSI | 1 | Signal strength indicator |
| Event | 2 | Event code |
| Odometer | 4 | Odometer value |
| Input | 1 | Input state |
| Output | 1 | Output state |
| ADC1-8 | 16 | 8 ADC values (2 bytes each) |
| Counter1 | 4 | Counter 1 value |
| Counter2 | 4 | Counter 2 value |
| Counter3 | 4 | Counter 3 value |

#### MSG_LOG_RECORDS (131)

Log records message:

| Field | Size (bytes) | Description |
|-------|--------------|-------------|
| Record Count | 2 | Number of records |
| Records | Variable | Multiple position records |

Each record contains:

| Field | Size (bytes) | Description |
|-------|--------------|-------------|
| Index | 4 | Record index |
| Subtype | 2 | Record type (101 or 92) |
| Length | 2 | Record length |
| Data | Variable | Record data (format depends on subtype) |

## Traccar Implementation

### Protocol Handler

The Apel protocol is implemented in Traccar through the following classes:

- **ApelProtocol**: Extends `BaseProtocol` and sets up the protocol decoder with appropriate frame decoder
- **ApelProtocolDecoder**: Extends `BaseProtocolDecoder` and handles the decoding of Apel protocol messages

### Frame Decoding

The protocol uses a `LengthFieldBasedFrameDecoder` with the following parameters:

- Byte order: Little-endian
- Max frame length: 1024 bytes
- Length field offset: 2 bytes
- Length field length: 2 bytes
- Length adjustment: 4 bytes
- Initial bytes to strip: 0
- Strip length field: true

### Message Processing

The decoder processes the following message types:

1. **MSG_TRACKER_ID_EXT (12)**: Extracts the device identifier for session management
2. **MSG_LAST_LOG_INDEX (121)**: Processes the last log index and requests archived data if needed
3. **MSG_CURRENT_GPS_DATA (101)**: Decodes current position information
4. **MSG_STATE_FULL_INFO_T104 (92)**: Decodes extended position and status information
5. **MSG_LOG_RECORDS (131)**: Processes historical position records

### Position Decoding

The decoder extracts the following information from position messages:

- Device identifier
- Timestamp
- Latitude and longitude
- Speed (converted from km/h to knots)
- Course
- Altitude
- Validity flag

For extended messages (MSG_STATE_FULL_INFO_T104), additional information is extracted:

- Number of satellites
- Signal strength (RSSI)
- Event code
- Odometer value
- Input and output states
- ADC values (analog inputs 1-8)
- Counter values (1-3)

### Communication Flow

The typical communication flow with an Apel device includes:

1. Device connects and sends identification (MSG_TRACKER_ID_EXT)
2. Server acknowledges and establishes session
3. Device sends current position data (MSG_CURRENT_GPS_DATA or MSG_STATE_FULL_INFO_T104)
4. Server requests last log index (MSG_REQUEST_LAST_LOG_INDEX)
5. Device responds with last log index (MSG_LAST_LOG_INDEX)
6. If needed, server requests historical records (MSG_REQUEST_LOG_RECORDS)
7. Device sends historical records (MSG_LOG_RECORDS)
8. Process repeats for ongoing tracking

## Usage Guidelines

### Server Configuration

To enable the Apel protocol in Traccar server:

1. Add the following to the `traccar.xml` configuration file:
   ```xml
   <entry key='apel.enable'>true</entry>
   <entry key='apel.port'>YOUR_PORT</entry>
   ```

2. Replace `YOUR_PORT` with the desired port number for the Apel protocol.

### Device Configuration

To configure an Apel tracking device to connect to Traccar:

1. Set the server IP address to your Traccar server's IP address
2. Set the server port to the port configured in Traccar
3. Configure the device ID according to your requirements
4. Set the data reporting interval as needed

### Troubleshooting

Common issues with Apel devices include:

1. **Connection Problems**: Verify network connectivity and firewall settings
2. **Authentication Failures**: Ensure the device ID is correctly configured
3. **No Position Updates**: Check the reporting interval settings on the device
4. **Incorrect Data**: Verify the device configuration and time settings

## References

- Traccar source code: [ApelProtocol.java](https://github.com/traccar/traccar/blob/master/src/main/java/org/traccar/protocol/ApelProtocol.java)
- Traccar source code: [ApelProtocolDecoder.java](https://github.com/traccar/traccar/blob/master/src/main/java/org/traccar/protocol/ApelProtocolDecoder.java)