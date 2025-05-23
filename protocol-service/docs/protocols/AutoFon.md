# AutoFon Protocol

## Overview

AutoFon is a protocol used by GPS tracking devices manufactured by AutoFon, a Russian company that produces innovative GSM, GPS, GPRS, LBS, and GLONASS tracking devices. The protocol supports various device models including AutoFon D, AutoFon SE, and AutoFon SE+.

The AutoFon protocol uses a binary message format with specific message types for different operations such as authentication, position reporting, and history data transmission. The protocol is designed for efficient communication over cellular networks, typically using TCP/IP connections.

## Supported Models

- AutoFon D
- AutoFon SE
- AutoFon SE+
- Other compatible AutoFon devices

## Connection Settings

| Setting | Value |
| --- | --- |
| Transport | TCP |
| Default Port | 6077 |

## Message Format

The AutoFon protocol uses a binary message format with a message type identifier at the beginning of each message. The message length varies depending on the message type.

### Message Types

| Type ID (Hex) | Description | Length (bytes) |
| --- | --- | --- |
| 0x10 | Login/Authentication | 12 |
| 0x11 | Location Data | 78 |
| 0x12 | History Data | 257 |
| 0x41 | Login (v4.5) | 19 |
| 0x02 | Location Data (v4.5) | 34 |

### Message Structure

#### Login Message (0x10)

Used for device authentication:

| Offset | Length | Description |
| --- | --- | --- |
| 0 | 1 | Message type (0x10) |
| 1 | 1 | Hardware version |
| 2 | 1 | Software version |
| 3 | 8 | Device IMEI (in BCD format) |
| 11 | 1 | Checksum |

#### Location Message (0x11)

Contains position information:

| Offset | Length | Description |
| --- | --- | --- |
| 0 | 1 | Message type (0x11) |
| 1 | 1 | Interval |
| 2 | 8 | Settings |
| 10 | 1 | Status |
| 11 | 2 | Reserved |
| 13 | 1 | Battery level |
| 14 | 6 | Time |
| 20 | 20 | Additional time and interval settings |
| 40 | 1 | Temperature |
| 41 | 1 | RSSI (signal strength) |
| 42 | 8 | Cell information (MCC, MNC, LAC, CID) |
| 50 | 1 | Validity and satellite count |
| 51 | 6 | Date and time (YY-MM-DD HH:MM:SS) |
| 57 | 4 | Latitude (integer format) |
| 61 | 4 | Longitude (integer format) |
| 65 | 2 | Altitude |
| 67 | 1 | Speed |
| 68 | 1 | Course |
| 69 | 2 | HDOP |
| 71 | 2 | Reserved |
| 73 | 1 | Checksum |

#### History Message (0x12)

Contains historical position data:

| Offset | Length | Description |
| --- | --- | --- |
| 0 | 1 | Message type (0x12) |
| 1 | 1 | Record count (lower 4 bits) |
| 2 | 2 | Total record count |
| 4 | n*73 | Position records (same format as location message) |

#### Login Message v4.5 (0x41)

Used for device authentication in newer devices:

| Offset | Length | Description |
| --- | --- | --- |
| 0 | 1 | Message type (0x41) |
| 1 | 8 | Device IMEI (in BCD format) |
| 9 | 10 | Additional data |

#### Location Message v4.5 (0x02)

Contains position information for newer devices:

| Offset | Length | Description |
| --- | --- | --- |
| 0 | 1 | Message type (0x02) |
| 1 | 1 | Status |
| 2 | 2 | Remaining time |
| 4 | 1 | Temperature |
| 5 | 2 | Timer settings |
| 7 | 1 | Mode |
| 8 | 1 | GPRS sending interval |
| 9 | 6 | Cell information |
| 15 | 1 | Validity and satellite count |
| 16 | 3 | Time (HHMMSS) |
| 19 | 3 | Date (DDMMYY) |
| 22 | 5 | Latitude |
| 27 | 5 | Longitude |
| 32 | 1 | Speed |
| 33 | 1 | Course |

## Data Format

### Coordinate Format

The AutoFon protocol uses two different coordinate formats:

1. For standard messages (0x11, 0x12):
   - Coordinates are stored as integers (raw value)
   - Conversion: degrees = raw / 1000000, minutes = (raw % 1000000) / 10000.0
   - Final coordinate = degrees + minutes / 60

2. For v4.5 messages (0x02):
   - Latitude: 1 byte for degrees, 3 bytes for minutes
   - Longitude: 1 byte for degrees, 3 bytes for minutes
   - Bit 0 of minutes indicates sign (0 = negative, 1 = positive)
   - Conversion: value = degrees + (minutes >> 4) / 600000.0

### Status Flags

The status byte contains various device status flags:

| Bit | Description |
| --- | --- |
| 7 | Alarm flag (1 = alarm active) |
| 0-6 | Battery level (percentage) |

## Implementation Details

### AutoFonFrameDecoder

The `AutoFonFrameDecoder` class extends `BaseFrameDecoder` and is responsible for identifying and extracting complete AutoFon messages from the incoming data stream. It determines the message length based on the message type and ensures that only complete messages are passed to the protocol decoder.

### AutoFonProtocol

The `AutoFonProtocol` class extends `BaseProtocol` and sets up the TCP server for handling AutoFon device connections. It configures the pipeline with the `AutoFonFrameDecoder` and `AutoFonProtocolDecoder` to process incoming messages.

### AutoFonProtocolDecoder

The `AutoFonProtocolDecoder` class extends `BaseProtocolDecoder` and is responsible for parsing the binary messages from AutoFon devices and converting them into the standard Traccar position model. It handles different message types and formats, extracts position data, device status, and other information.

## Authentication

When an AutoFon device connects to the server, it sends a login message (type 0x10 or 0x41) containing its IMEI number. The server responds with a confirmation message that includes a CRC checksum from the received message.

Login sequence:
1. Device sends login message with IMEI
2. Server extracts IMEI and validates device
3. Server responds with confirmation message
4. Device begins sending position updates

## Usage Guidelines

### Device Configuration

To configure an AutoFon device to connect to your Traccar server:

1. Send an SMS command to the device with the server IP address and port:
   ```
   1234,IP1=server_ip.port,ALARMCLOCK1=240M,FGESE
   ```
   
   Example:
   ```
   1234,IP1=192.168.1.10.6077,ALARMCLOCK1=240M,FGESE
   ```

2. For periodic reporting, configure the reporting interval:
   ```
   ALARMCLOCK2=HHMMSS,interval,FSE
   ```
   
   Example (report every day at 8:30):
   ```
   ALARMCLOCK2=083000,1D,FSE
   ```

### Server Configuration

In the Traccar configuration file (`traccar.xml`), ensure the AutoFon protocol is enabled:

```xml
<entry key='autofon.port'>6077</entry>
```

## Notes

- AutoFon devices typically use pre-paid SIM cards, so APN information is usually configured by the operator.
- The protocol supports both regular position updates and historical data transmission.
- Temperature data is available in most message types.
- The protocol includes cell tower information for additional location context.

## References

1. AutoFon official website: http://www.autofon.ru/
2. Traccar protocol implementation: https://github.com/traccar/traccar/tree/master/src/main/java/org/traccar/protocol