# Atrack Protocol Documentation

## Overview

The Atrack protocol is used by GPS tracking devices manufactured by ATrack Technology Inc., headquartered in Taiwan. This protocol supports both text (ASCII) and binary message formats with configurable position report formats, making it highly flexible for various tracking applications. Atrack devices are used in fleet management, asset tracking, and vehicle telematics applications across various industries.

## Protocol Specifications

### Communication

- **Transport Protocol**: TCP/UDP over cellular networks (GPRS/3G/4G/LTE)
- **Default Ports**: 
  - TCP: 5044 (configurable in Traccar)
  - UDP: 5044 (configurable in Traccar)
- **Message Formats**: Text (ASCII) and Binary
- **Encryption**: Optional AES-128 data encryption
- **Acknowledgment**: Server acknowledges messages with a response containing the device ID and message index

### Message Structure

#### Text Format

Text messages follow this general structure:
```
<header>,<device_id>,<index>,<device_time>,<data...>

```

Where:
- `<header>`: Message type identifier (e.g., "@P" for position data)
- `<device_id>`: Unique device identifier
- `<index>`: Message sequence number
- `<device_time>`: Device timestamp
- `<data...>`: Position data in configurable format

Example of a text position message:
```
@P,123456,1,20230101123456,20230101123456,20230101123456,12345678,87654321,0,1,100,10,1,60,1,3800,Driver1,25,26,Hello,%CISA%MV%BV%GQ%CE%LC%CN,10,3800,3900,15,12345,1234,310410

```

#### Binary Format

Binary messages have the following structure:
```
<prefix><checksum><length><index><device_id><data...>
```

Where:
- `<prefix>`: 2-byte message prefix (e.g., "@P")
- `<checksum>`: 2-byte message checksum
- `<length>`: 2-byte message length
- `<index>`: 2-byte message sequence number
- `<device_id>`: 8-byte device identifier
- `<data...>`: Binary position data in configurable format

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

The Atrack protocol is implemented in Traccar through several key components:

### AtrackProtocol

The main protocol class that defines supported commands and server configurations. It sets up TCP and UDP servers and configures the protocol pipeline.

```java
public class AtrackProtocol extends BaseProtocol {

    @Inject
    public AtrackProtocol(Config config) {
        setSupportedDataCommands(
                Command.TYPE_CUSTOM);
        addServer(new TrackerServer(config, getName(), false) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                pipeline.addLast(new AtrackFrameDecoder());
                pipeline.addLast(new AtrackProtocolEncoder(AtrackProtocol.this));
                pipeline.addLast(new AtrackProtocolDecoder(AtrackProtocol.this));
            }
        });
        addServer(new TrackerServer(config, getName(), true) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                pipeline.addLast(new AtrackProtocolEncoder(AtrackProtocol.this));
                pipeline.addLast(new AtrackProtocolDecoder(AtrackProtocol.this));
            }
        });
    }
}
```

### AtrackFrameDecoder

Handles message framing and packet identification. It detects different message types:
- Keepalive messages (0xfe02 prefix)
- Binary messages (0x40 prefix without comma)
- Text messages (with comma separator)

The frame decoder is responsible for identifying complete messages in the incoming data stream and extracting them for further processing. It handles three distinct message formats:

1. **Keepalive Messages**: Fixed 12-byte messages starting with 0xfe02
2. **Binary Messages**: Messages starting with 0x40 without a comma at position 2
3. **Text Messages**: Messages with comma separators and length indicators

```java
public class AtrackFrameDecoder extends BaseFrameDecoder {

    private static final int KEEPALIVE_LENGTH = 12;

    @Override
    protected Object decode(
            ChannelHandlerContext ctx, Channel channel, ByteBuf buf) throws Exception {

        if (buf.readableBytes() >= 2) {

            if (buf.getUnsignedShort(buf.readerIndex()) == 0xfe02) {

                if (buf.readableBytes() >= KEEPALIVE_LENGTH) {
                    return buf.readRetainedSlice(KEEPALIVE_LENGTH);
                }

            } else if (buf.getUnsignedByte(buf.readerIndex()) == 0x40 && buf.getByte(buf.readerIndex() + 2) != ',') {

                if (buf.readableBytes() > 6) {
                    int length = buf.getUnsignedShort(buf.readerIndex() + 4) + 4 + 2;
                    if (buf.readableBytes() >= length) {
                        return buf.readRetainedSlice(length);
                    }
                }

            } else {
                // Text message handling logic
            }
        }
        return null;
    }
}
```

### AtrackProtocolDecoder

Decodes both binary and text messages into Position objects. It supports:
- Text position messages
- Binary position messages
- Information messages
- Photo data messages
- Custom data formats

The protocol decoder is highly configurable through several parameters:
- `longDate`: Controls date format interpretation
- `decimalFuel`: Controls fuel value interpretation
- `custom`: Enables custom data parsing
- `frameMask`: Sets the frame mask for custom data
- `form`: Defines the custom data format string

The decoder handles multiple message formats and extracts position data, device status, and custom fields according to the configured format. It also supports special features like beacon data parsing and G-sensor data handling.

Key methods in the decoder include:
- `decodeInfo`: Parses device information messages
- `decodeText`: Parses text format position messages
- `decodeBinary`: Parses binary format position messages
- `decodePhoto`: Handles photo data messages
- `readTextCustomData`: Parses custom data in text format
- `readBinaryCustomData`: Parses custom data in binary format

### AtrackProtocolEncoder

Encodes commands to be sent to devices. Currently supports custom commands through the `Command.TYPE_CUSTOM` command type.

```java
public class AtrackProtocolEncoder extends BaseProtocolEncoder {

    public AtrackProtocolEncoder(Protocol protocol) {
        super(protocol);
    }

    @Override
    protected Object encodeCommand(Command command) {

        if (command.getType().equals(Command.TYPE_CUSTOM)) {
            return Unpooled.copiedBuffer(
                    command.getString(Command.KEY_DATA) + "\r\n", StandardCharsets.US_ASCII);
        }
        return null;
    }
}
```

The encoder is relatively simple as it currently only supports sending custom commands. The command data is sent directly to the device with CRLF line endings. Future enhancements could include support for specific command types like position request, engine stop/start, etc.

## Message Types

### Position Messages

Position messages contain GPS location data and various device status information. They can be in text or binary format with configurable fields.

Position messages typically include:
- Timestamp (device time, GPS time, server time)
- Coordinates (latitude, longitude)
- Speed and course
- Altitude
- HDOP (Horizontal Dilution of Precision)
- Input/output states
- Analog inputs
- Temperature readings
- Custom data fields

### Information Messages

Information messages start with "$INFO" and contain device status information such as:
- Device model
- Firmware version
- Power status
- Battery level
- Satellite count
- Signal strength

Example information message:
```
$INFO=123456,AT5,1.0.0,123456789012345,123456789012345,1234567890,3800,3900,10,1,15,1,1

```

### Photo Data Messages

Photo data messages contain image data captured by the device. They are sent in chunks and reassembled by the server. These messages use the "@R" prefix and include:
- Timestamp
- Chunk index
- Total chunk count
- Image data

The server acknowledges each chunk and reassembles the complete image once all chunks are received.

### Keepalive Messages

Keepalive messages maintain the connection between the device and server. They have a fixed format with the prefix 0xfe02 and are 12 bytes long. The server should respond with the same message to acknowledge receipt.

## Custom Data

The Atrack protocol supports custom data fields through configurable formats. This allows for device-specific data to be included in position messages.

### Text Custom Data

Text custom data is formatted as a comma-separated list of values with a format identifier (e.g., "%CI%SA%MV%BV%GQ%CE%LC%CN").

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

### Beacon Data

Some Atrack devices support beacon data for proximity detection. This data includes beacon identifiers and signal strength information. The beacon data is typically included in the custom data section of position messages.

Beacon data includes:
- Beacon ID (MAC address)
- Signal strength (RSSI)
- Additional sensor data (temperature, humidity, etc.)

The AtrackProtocolDecoder includes a specialized method `decodeBeaconData()` to parse this information from both text and binary messages.

### G-Sensor Data

Devices equipped with accelerometers can send G-sensor data for impact detection. This data includes acceleration values on X, Y, and Z axes.

G-sensor data is particularly useful for:
- Crash detection
- Harsh driving behavior monitoring (acceleration, braking, cornering)
- Vehicle orientation tracking

The G-sensor data can be sent in real-time or stored for later retrieval. Some devices can send detailed impact data covering periods before and after an impact event.

### CAN Bus Data

Atrack devices support reading data from vehicle CAN bus systems, including:
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

## Commands

The Atrack protocol supports various commands for device configuration and control. In Traccar, custom commands can be sent to devices using the AtrackProtocolEncoder.

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

The Atrack protocol in Traccar supports several configuration options that can be set in the Traccar configuration file:

### protocol.atrack.longDate

Controls whether to use the long date format for parsing timestamps.

```
protocol.atrack.longDate = true
```

When enabled, the decoder expects date fields in a longer format with explicit year, month, day, hour, minute, and second components.

### protocol.atrack.custom

Enables or disables parsing of custom data fields in position messages.

```
protocol.atrack.custom = true
```

When enabled, the decoder will attempt to parse custom data fields according to the format string.

### protocol.atrack.frameMask

Sets the frame mask value for custom data parsing.

```
protocol.atrack.frameMask = 1
```

This value is used when decoding beacon data and other custom data formats.

### protocol.atrack.form

Defines the custom data format string for parsing position messages.

```
protocol.atrack.form = "%SA%MV%BV%GQ%CE%LC%CN"
```

This string tells the decoder which fields to expect in the custom data section and in what order.

### protocol.atrack.alarmMap

Maps device-specific alarm codes to standard Traccar alarm types.

```
protocol.atrack.alarmMap = 1=powerCut,2=lowBattery,3=sos
```

This allows for translating device-specific alarm codes into standardized alarm types used throughout the Traccar system.

## Examples

### Text Position Message

```
@P,123456,1,20230101123456,20230101123456,20230101123456,12345678,87654321,0,1,100,10,1,60,1,3800,Driver1,25,26,Hello,%CISA%MV%BV%GQ%CE%LC%CN,10,3800,3900,15,12345,1234,310410

```

### Binary Position Message

```
@P<checksum><length><index><device_id><binary_data>
```

### Information Message

```
$INFO=123456,AT5,1.0.0,123456789012345,123456789012345,1234567890,3800,3900,10,1,15,1,1

```

## Troubleshooting

### Common Issues

1. **Message Format Mismatch**: Ensure the device is configured with the correct position format that matches the server configuration.
   - Use the AT$FORM command to check the current format on the device
   - Configure the `protocol.atrack.form` parameter to match the device's format
   - Verify that all expected fields are being parsed correctly

2. **Binary vs. Text Format**: Some servers may have difficulty automatically detecting binary vs. text format. Consider using separate ports for each format.
   - Configure TCP port 5044 for text format
   - Configure TCP port 5045 for binary format
   - Ensure the device is configured to use the correct port for its format

3. **Custom Data Parsing**: Custom data fields require proper configuration of the `protocol.atrack.form` parameter.
   - Check the device's custom data format using AT$FORM=?
   - Update the server configuration to match
   - Test with a single position report to verify parsing

4. **Multiple Position Messages**: When sending multiple positions in a single message, ensure the server can properly parse the message boundaries.
   - Check the AtrackFrameDecoder implementation for correct boundary detection
   - Consider using single position reports for testing
   - Verify that all positions in a batch are being correctly parsed

5. **Date/Time Format Issues**: Timestamp parsing can be problematic if the format doesn't match expectations.
   - Configure the `protocol.atrack.longDate` parameter appropriately
   - Check for timezone differences between device and server
   - Verify that the parsed timestamps are correct

## References

- ATrack Technology Inc. official documentation
  - Protocol Document (available from manufacturer)
  - Device User Manuals
  - AT Command Reference

- Traccar Implementation
  - [AtrackProtocol.java](https://github.com/traccar/traccar/blob/master/src/main/java/org/traccar/protocol/AtrackProtocol.java)
  - [AtrackFrameDecoder.java](https://github.com/traccar/traccar/blob/master/src/main/java/org/traccar/protocol/AtrackFrameDecoder.java)
  - [AtrackProtocolDecoder.java](https://github.com/traccar/traccar/blob/master/src/main/java/org/traccar/protocol/AtrackProtocolDecoder.java)
  - [AtrackProtocolEncoder.java](https://github.com/traccar/traccar/blob/master/src/main/java/org/traccar/protocol/AtrackProtocolEncoder.java)

- Flespi Protocol Documentation
  - [Atrack Protocol Overview](https://flespi.com/protocols/atrack)
  - [Atrack Protocol Parameters](https://flespi.com/protocols/atrack#parameters)

- Test Resources
  - Binary and text message samples
  - Expected position data
  - Configuration examples