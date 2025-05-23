# Ardi01 Protocol Documentation

## Overview

The Ardi01 protocol is used by GPS tracking devices manufactured by Ardi Technology. This protocol supports both text and binary message formats with configurable position report formats, making it suitable for various tracking applications. Ardi01 devices are commonly used in fleet management, asset tracking, and vehicle telematics applications.

## Protocol Specifications

### Communication

- **Transport Protocol**: TCP/UDP over cellular networks (GPRS/3G/4G/LTE)
- **Default Ports**: 
  - TCP: 5027 (configurable in Traccar)
  - UDP: 5027 (configurable in Traccar)
- **Message Formats**: Text (ASCII) and Binary
- **Encryption**: Optional AES-128 data encryption
- **Acknowledgment**: Server acknowledges messages with a response containing the device ID and message index

### Message Structure

#### Text Format

Text messages follow this general structure:
```
$<header>,<device_id>,<index>,<timestamp>,<data...>*<checksum>
```

Where:
- `<header>`: Message type identifier (e.g., "$ARDI" for position data)
- `<device_id>`: Unique device identifier (typically IMEI number)
- `<index>`: Message sequence number
- `<timestamp>`: Device timestamp in format YYYYMMDDHHMMSS
- `<data...>`: Position data and additional parameters
- `<checksum>`: Two-byte hexadecimal checksum (XOR of all bytes between $ and *)

Example of a text position message:
```
$ARDI,123456789012345,1,20230101123456,3755.7158,N,12226.5272,W,0.8,145.2,11,452,12.5,92,0,0,0,0,1,1,0,0*3F
```

#### Binary Format

Binary messages have the following structure:
```
<start_marker><length><device_id><message_type><data...><checksum><end_marker>
```

Where:
- `<start_marker>`: 2-byte message start marker (0xAA55)
- `<length>`: 2-byte message length (excluding start/end markers)
- `<device_id>`: 8-byte device identifier
- `<message_type>`: 1-byte message type identifier
- `<data...>`: Binary position data and additional parameters
- `<checksum>`: 2-byte message checksum (CRC-16)
- `<end_marker>`: 2-byte message end marker (0x55AA)

The binary format is more efficient for data transmission but requires proper configuration on both the device and server sides to ensure correct parsing.

### Position Data Format

Position data format is configurable using the AT$FORM command. The format can include various fields such as:

- Date and time
- Coordinates (latitude, longitude)
- Speed, course
- Altitude
- HDOP (Horizontal Dilution of Precision)
- Input/output states
- Analog inputs
- Temperature sensors
- Custom data fields

## Traccar Implementation

The Ardi01 protocol is implemented in Traccar through several key components:

### Ardi01Protocol

The main protocol class that defines supported commands and server configurations. It sets up TCP and UDP servers and configures the protocol pipeline.

```java
public class Ardi01Protocol extends BaseProtocol {

    @Inject
    public Ardi01Protocol(Config config) {
        setSupportedDataCommands(
                Command.TYPE_CUSTOM,
                Command.TYPE_POSITION_SINGLE,
                Command.TYPE_ENGINE_STOP,
                Command.TYPE_ENGINE_RESUME);
        addServer(new TrackerServer(config, getName(), false) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                pipeline.addLast(new Ardi01ProtocolDecoder(Ardi01Protocol.this));
            }
        });
        addServer(new TrackerServer(config, getName(), true) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                pipeline.addLast(new Ardi01ProtocolDecoder(Ardi01Protocol.this));
            }
        });
    }
}
```

### Ardi01ProtocolDecoder

Decodes both binary and text messages into Position objects. It supports:
- Text position messages
- Binary position messages
- Status messages
- Alert messages
- Custom data formats

The protocol decoder is configurable through several parameters:
- `ardi01.longDate`: Controls date format interpretation
- `ardi01.custom`: Enables custom data parsing
- `ardi01.form`: Defines the custom data format string

Key methods in the decoder include:
- `decode`: Main entry point for message decoding
- `decodeText`: Parses text format position messages
- `decodeBinary`: Parses binary format position messages
- `decodeStatus`: Parses device status messages
- `decodeAlert`: Handles alert messages

## Message Types

### Position Messages

Position messages contain GPS location data and various device status information. They can be in text or binary format with configurable fields.

Position messages typically include:
- Timestamp
- Coordinates (latitude, longitude)
- Speed and course
- Altitude
- HDOP (Horizontal Dilution of Precision)
- Input/output states
- Analog inputs
- Temperature readings
- Custom data fields

Example text position message:
```
$ARDI,123456789012345,1,20230101123456,3755.7158,N,12226.5272,W,0.8,145.2,11,452,12.5,92,0,0,0,0,1,1,0,0*3F
```

Example binary position message (hexadecimal representation):
```
AA55002A123456789012345010123456789ABCDEF0123456789ABCDEF0123456789ABCDEF01234567FFFF55AA
```

### Status Messages

Status messages provide information about the device's current state, including:
- Power status
- Battery level
- Signal strength
- Memory usage
- Firmware version

Example status message:
```
$STAT,123456789012345,2,20230101123456,12.5,92,4,1,0,0,1.0.5*3F
```

### Alert Messages

Alert messages are sent when specific events occur, such as:
- Power disconnection
- Low battery
- Geofence violations
- Excessive speed
- Harsh acceleration/braking
- SOS button press

Example alert message:
```
$ALRT,123456789012345,3,20230101123456,3755.7158,N,12226.5272,W,0.8,145.2,1,0*3F
```

### Command Response Messages

Command response messages are sent in response to commands from the server. They include:
- Command acknowledgment
- Command execution status
- Command result data

Example command response message:
```
$RESP,123456789012345,4,20230101123456,CMD,OK*3F
```

## Custom Data

The Ardi01 protocol supports custom data fields through configurable formats. This allows for device-specific data to be included in position messages.

### Text Custom Data

Text custom data is formatted as a comma-separated list of values with a format identifier.

Common format codes include:
- `%SA`: Satellites count
- `%MV`: Main power voltage (0.1V)
- `%BV`: Backup battery voltage (0.1V)
- `%GQ`: GSM signal quality
- `%CE`: Cell ID
- `%LC`: Location Area Code
- `%CN`: Combined Mobile Country Code and Mobile Network Code
- `%PC`: Position count
- `%AT`: Altitude
- `%RP`: RPM value
- `%GS`: GSM signal strength
- `%DT`: Archive flag
- `%VN`: VIN (Vehicle Identification Number)
- `%TR`: Throttle position
- `%ET`: Engine coolant temperature
- `%FL`: Fuel level
- `%FC`: Fuel consumption

### Binary Custom Data

Binary custom data follows a format string that defines the data structure. Each field in the format string corresponds to a specific data type and size in the binary message.

The binary format is more efficient but requires careful configuration to ensure proper parsing. The format string defines both the field identifiers and their binary representation (data type and size).

## Special Features

### CAN Bus Data

Ardi01 devices support reading data from vehicle CAN bus systems, including:
- OBDII: Standard diagnostic system for passenger vehicles
- J1939: Heavy-duty vehicle standard
- J1708: Legacy standard for commercial vehicles
- FMS: Fleet Management System standard

This allows for detailed vehicle telemetry such as:
- Engine parameters (RPM, load, temperature)
- Fuel data (level, consumption, economy)
- Vehicle speed and odometer
- Diagnostic trouble codes
- Driver behavior indicators
- Emissions-related data

CAN bus data is typically included in the custom data section of position messages, with specific format codes for different parameters.

### Driver Identification

Ardi01 devices support driver identification through various methods:
- RFID card readers
- iButton keys
- Dallas 1-Wire keys
- Bluetooth identification

Driver identification data is included in position messages, allowing for driver-specific tracking and reporting.

### Remote Configuration

Ardi01 devices can be configured remotely through commands sent from the server. Configuration options include:
- Reporting intervals
- Geofence settings
- Alert thresholds
- Communication parameters
- Power management settings

## Commands

The Ardi01 protocol supports various commands for device configuration and control. In Traccar, commands can be sent to devices using the Ardi01ProtocolEncoder.

Common commands include:

### AT$FORM: Configure Position Report Format

This command configures the format of position reports sent by the device.

```
AT$FORM="%DT%GD%GT%GS%GL%BD%BT%BI%BP%GQ%CE%LC%CN%RL%PC%AT%RP%GS%DT%VN%MF%EL%TR%ET%FL%ML%FC%CI%AV1%NC%SM%GL%MA%PD%CD%CM%GN%GV%ME%IA%MP%EO%EH"
```

The format string defines which fields are included in position reports and their order.

### AT$TRAX: Configure GPS Tracking Properties

This command configures when and how the device sends position reports.

```
AT$TRAX=1,1,3600,500,30,0,0,0
```

Parameters include reporting mode, interval, distance, and other triggers.

### AT$STRA: Configure Scheduled Tracking Reports

This command sets up scheduled position reporting at specific times.

```
AT$STRA=1,210000,003000,034500
```

This would schedule reports at 21:00, 00:30, and 03:45 daily.

### AT$SLOG: Configure GPS Logging Properties

This command configures the device's internal logging of GPS data.

```
AT$SLOG=1,1,60,100,30,0,0,0
```

Parameters control logging frequency, conditions, and storage behavior.

### AT$DLOG: Download GPS Logging Data

This command initiates the download of logged GPS data from the device.

```
AT$DLOG=090101000000,990101000000,0
```

Parameters specify the time range for data retrieval.

## Configuration Options

The Ardi01 protocol in Traccar supports several configuration options that can be set in the Traccar configuration file:

### protocol.ardi01.longDate

Controls whether to use the long date format for parsing timestamps.

```
protocol.ardi01.longDate = true
```

When enabled, the decoder expects date fields in a longer format with explicit year, month, day, hour, minute, and second components.

### protocol.ardi01.custom

Enables or disables parsing of custom data fields in position messages.

```
protocol.ardi01.custom = true
```

When enabled, the decoder will attempt to parse custom data fields according to the format string.

### protocol.ardi01.form

Defines the custom data format string for parsing position messages.

```
protocol.ardi01.form = "%SA%MV%BV%GQ%CE%LC%CN"
```

This string tells the decoder which fields to expect in the custom data section and in what order.

### protocol.ardi01.alarmMap

Maps device-specific alarm codes to standard Traccar alarm types.

```
protocol.ardi01.alarmMap = 1=powerCut,2=lowBattery,3=sos
```

This allows for translating device-specific alarm codes into standardized alarm types used throughout the Traccar system.

## Examples

### Text Position Message

```
$ARDI,123456789012345,1,20230101123456,3755.7158,N,12226.5272,W,0.8,145.2,11,452,12.5,92,0,0,0,0,1,1,0,0*3F
```

### Binary Position Message

```
AA55002A123456789012345010123456789ABCDEF0123456789ABCDEF0123456789ABCDEF01234567FFFF55AA
```

### Status Message

```
$STAT,123456789012345,2,20230101123456,12.5,92,4,1,0,0,1.0.5*3F
```

### Alert Message

```
$ALRT,123456789012345,3,20230101123456,3755.7158,N,12226.5272,W,0.8,145.2,1,0*3F
```

## Troubleshooting

### Common Issues

1. **Message Format Mismatch**: Ensure the device is configured with the correct position format that matches the server configuration.
   - Use the AT$FORM command to check the current format on the device
   - Configure the `protocol.ardi01.form` parameter to match the device's format
   - Verify that all expected fields are being parsed correctly

2. **Binary vs. Text Format**: Some servers may have difficulty automatically detecting binary vs. text format. Consider using separate ports for each format.
   - Configure TCP port 5027 for text format
   - Configure TCP port 5028 for binary format
   - Ensure the device is configured to use the correct port for its format

3. **Custom Data Parsing**: Custom data fields require proper configuration of the `protocol.ardi01.form` parameter.
   - Check the device's custom data format using AT$FORM=?
   - Update the server configuration to match
   - Test with a single position report to verify parsing

4. **Multiple Position Messages**: When sending multiple positions in a single message, ensure the server can properly parse the message boundaries.
   - Check the Ardi01ProtocolDecoder implementation for correct boundary detection
   - Consider using single position reports for testing
   - Verify that all positions in a batch are being correctly parsed

5. **Date/Time Format Issues**: Timestamp parsing can be problematic if the format doesn't match expectations.
   - Configure the `protocol.ardi01.longDate` parameter appropriately
   - Check for timezone differences between device and server
   - Verify that the parsed timestamps are correct

## References

- Ardi Technology Inc. official documentation
  - Protocol Document (available from manufacturer)
  - Device User Manuals
  - AT Command Reference

- Traccar Implementation
  - [Ardi01Protocol.java](https://github.com/traccar/traccar/blob/master/src/main/java/org/traccar/protocol/Ardi01Protocol.java)
  - [Ardi01ProtocolDecoder.java](https://github.com/traccar/traccar/blob/master/src/main/java/org/traccar/protocol/Ardi01ProtocolDecoder.java)

- Test Resources
  - Binary and text message samples
  - Expected position data
  - Configuration examples