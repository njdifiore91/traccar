# ADM Protocol Documentation

## Overview

The ADM protocol is used by GPS tracking devices manufactured by ADM Technology. This protocol supports both text and binary message formats for position reporting, device status, and command handling. ADM devices are commonly used in fleet management, asset tracking, and vehicle monitoring applications.

## Protocol Specifications

### Communication

- **Transport Protocol**: TCP over cellular networks (GPRS/3G/4G)
- **Default Port**: Configurable in Traccar (typically 5007)
- **Message Formats**: Binary with specific message structures
- **Acknowledgment**: Server acknowledges commands with response messages

### Message Structure

ADM protocol messages have different structures depending on the message type:

#### Data Messages

Data messages contain position information and device status. The structure varies based on the message type flags:

```
<device_id><size><type><data...>
```

Where:
- `<device_id>`: 2-byte device identifier
- `<size>`: 1-byte message size
- `<type>`: 1-byte message type
- `<data...>`: Variable-length data fields depending on the message type

#### IMEI Messages

IMEI messages are used for device identification:

```
<device_id><size><type=0x03><imei>
```

Where:
- `<device_id>`: 2-byte device identifier
- `<size>`: 1-byte message size
- `<type>`: 1-byte message type (0x03 for IMEI)
- `<imei>`: 15-byte IMEI string

#### Command Response Messages

Command response messages are sent by the device in response to commands:

```
<device_id><size=0x84><response_text>
```

Where:
- `<device_id>`: 2-byte device identifier
- `<size>`: 1-byte message size (0x84 for command responses)
- `<response_text>`: Variable-length response text

## Position Data Format

Position data in ADM messages includes:

- Firmware version
- Message index
- Status flags
- Coordinates (latitude, longitude)
- Course
- Speed
- Acceleration
- Altitude
- HDOP
- Satellite count
- Timestamp
- Power and battery levels
- Digital inputs/outputs
- Analog inputs
- Counters
- Fuel levels
- Temperature readings
- Additional sensor data

The exact fields included depend on the message type flags.

## Traccar Implementation

The ADM protocol is implemented in Traccar through several key components:

### AdmProtocol

The main protocol class that defines supported commands and server configurations:

```java
public class AdmProtocol extends BaseProtocol {

    @Inject
    public AdmProtocol(Config config) {
        setSupportedDataCommands(
                Command.TYPE_GET_DEVICE_STATUS,
                Command.TYPE_CUSTOM);
        addServer(new TrackerServer(config, getName(), false) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                pipeline.addLast(new AdmFrameDecoder());
                pipeline.addLast(new StringEncoder());
                pipeline.addLast(new AdmProtocolEncoder(AdmProtocol.this));
                pipeline.addLast(new AdmProtocolDecoder(AdmProtocol.this));
            }
        });
    }
}
```

The AdmProtocol class:
- Supports GET_DEVICE_STATUS and CUSTOM command types
- Sets up a TCP server with the appropriate protocol handlers
- Configures the pipeline with the frame decoder, string encoder, protocol encoder, and protocol decoder

### AdmFrameDecoder

Handles message framing and packet identification:

```java
public class AdmFrameDecoder extends BaseFrameDecoder {

    @Override
    protected Object decode(
            ChannelHandlerContext ctx, Channel channel, ByteBuf buf) throws Exception {

        if (buf.readableBytes() < 15 + 3) {
            return null;
        }

        int length;
        if (Character.isDigit(buf.getUnsignedByte(buf.readerIndex()))) {
            length = 15 + buf.getUnsignedByte(buf.readerIndex() + 15 + 2);
        } else {
            length = buf.getUnsignedByte(buf.readerIndex() + 2);
        }

        if (buf.readableBytes() >= length) {
            return buf.readRetainedSlice(length);
        }

        return null;
    }
}
```

The AdmFrameDecoder:
- Determines message length based on the message format
- Handles two different message formats:
  - Messages starting with a digit (IMEI messages)
  - Messages with a size byte at position 2
- Returns complete messages for further processing

### AdmProtocolDecoder

Decodes messages into Position objects:

```java
public class AdmProtocolDecoder extends BaseProtocolDecoder {

    public AdmProtocolDecoder(Protocol protocol) {
        super(protocol);
    }

    public static final int CMD_RESPONSE_SIZE = 0x84;
    public static final int MSG_IMEI = 0x03;
    public static final int MSG_PHOTO = 0x0A;
    public static final int MSG_ADM5 = 0x01;

    // Implementation details for decoding different message types
    // ...
}
```

The AdmProtocolDecoder:
- Defines constants for command response size and message types
- Handles different message formats:
  - Data messages with position information
  - IMEI messages for device identification
  - Command response messages
- Extracts position data, device status, and various sensor readings
- Processes different data fields based on message type flags

Key methods in the decoder include:
- `decodeData`: Parses position data messages
- `parseCommandResponse`: Handles command response messages
- `decode`: Main method that identifies message type and routes to appropriate handler

### AdmProtocolEncoder

Encodes commands to be sent to devices:

```java
public class AdmProtocolEncoder extends StringProtocolEncoder {

    public AdmProtocolEncoder(Protocol protocol) {
        super(protocol);
    }

    @Override
    protected Object encodeCommand(Command command) {

        return switch (command.getType()) {
            case Command.TYPE_GET_DEVICE_STATUS -> formatCommand(command, "STATUS\r\n");
            case Command.TYPE_CUSTOM -> formatCommand(command, "%s\r\n", Command.KEY_DATA);
            default -> null;
        };
    }
}
```

The AdmProtocolEncoder:
- Extends StringProtocolEncoder for text-based command encoding
- Supports two command types:
  - GET_DEVICE_STATUS: Sends "STATUS\r\n" to the device
  - CUSTOM: Sends custom command data with CRLF line endings

## Message Types

### Position Messages

Position messages (type 0x01) contain GPS location data and various device status information. The data fields included depend on the type flags:

- Base fields (always included):
  - Firmware version
  - Message index
  - Status flags
  - Coordinates (latitude, longitude)
  - Course
  - Speed
  - Acceleration
  - Altitude
  - HDOP
  - Satellite count
  - Timestamp
  - Power and battery levels

- Additional fields (based on type flags):
  - Digital inputs/outputs
  - Analog inputs
  - Counters
  - Fuel levels
  - Temperature readings
  - CAN bus data
  - Odometer

### IMEI Messages

IMEI messages (type 0x03) are used for device identification. They contain the device's 15-digit IMEI number, which is used to identify the device in the Traccar system.

### Photo Messages

Photo messages (type 0x0A) contain image data captured by the device. The implementation includes a constant for this message type, but the detailed handling is not shown in the provided code.

### Command Response Messages

Command response messages have a fixed size (0x84) and contain text responses to commands sent to the device. The response text is extracted and stored in the Position object's RESULT attribute.

## Data Fields

The ADM protocol supports various data fields in position messages:

### Status Flags

Status flags indicate the device's current state, including:
- GPS validity
- Alarm conditions
- Power status
- Input/output states

### Digital Inputs/Outputs

Digital inputs and outputs are included when the type flag bit 2 is set. The decoder extracts:
- Output states (up to 4 channels)
- Vibration data
- Alarm inputs

### Analog Inputs

Analog input values are included when the type flag bit 3 is set. The decoder extracts up to 6 analog input channels, with values scaled to volts (multiplied by 0.001).

### Counters

Counter values are included when the type flag bit 4 is set. The decoder extracts up to 2 counter channels.

### Fuel and Temperature

Fuel levels and temperature readings are included when the type flag bit 5 is set. The decoder extracts:
- Up to 3 fuel level channels
- Up to 3 temperature channels

### CAN Bus Data

CAN bus data is included when the type flag bit 6 is set. The decoder extracts various CAN bus parameters based on their identifiers, including:
- Temperature
- Humidity
- Illumination
- Battery level
- Other custom CAN parameters

### Odometer

Odometer value is included when the type flag bit 7 is set. The decoder extracts the vehicle's total distance traveled.

## Commands

The ADM protocol supports several commands for device control and status retrieval:

### STATUS Command

The STATUS command requests the current status of the device. It is sent as a simple text string:

```
STATUS

```

The device responds with a command response message containing status information.

### Custom Commands

Custom commands allow sending arbitrary text commands to the device. The command data is sent directly to the device with CRLF line endings.

Example custom command:

```
GET_PARAMS

```

## Configuration Options

The ADM protocol in Traccar does not have specific configuration options in the provided code. However, standard Traccar configuration options apply, such as:

- Server port configuration
- Connection timeout settings
- Protocol-specific behavior flags

## Examples

### Position Message

A typical position message might include:

```
<device_id><size><type=0x01><firmware_version><index><status><latitude><longitude><course><speed><acceleration><altitude><hdop><satellites><timestamp><power><battery>...
```

The exact format depends on the type flags and included data fields.

### IMEI Message

An IMEI message for device identification:

```
<device_id><size><type=0x03><imei>
```

Where `<imei>` is a 15-digit string like "123456789012345".

### Command Response

A command response message:

```
<device_id><size=0x84><response_text>
```

Where `<response_text>` might be "OK" or detailed status information.

## Troubleshooting

### Common Issues

1. **Device Identification**: Ensure the device is sending a valid IMEI message for identification before sending position data.

2. **Message Format**: Verify that the message format matches what the decoder expects. The ADM protocol has specific message structures that must be followed.

3. **Type Flags**: Check that the type flags in position messages correctly indicate which data fields are included.

4. **Command Responses**: If commands are not being acknowledged, verify that the device is properly processing the command format and sending response messages.

5. **Data Field Scaling**: Some data fields (like analog inputs) are scaled in the decoder. Ensure the scaling factors match the device's actual output.

## References

- Traccar Implementation
  - [AdmProtocol.java](https://github.com/traccar/traccar/blob/master/src/main/java/org/traccar/protocol/AdmProtocol.java)
  - [AdmFrameDecoder.java](https://github.com/traccar/traccar/blob/master/src/main/java/org/traccar/protocol/AdmFrameDecoder.java)
  - [AdmProtocolDecoder.java](https://github.com/traccar/traccar/blob/master/src/main/java/org/traccar/protocol/AdmProtocolDecoder.java)
  - [AdmProtocolEncoder.java](https://github.com/traccar/traccar/blob/master/src/main/java/org/traccar/protocol/AdmProtocolEncoder.java)

- ADM Technology Documentation
  - Device User Manuals
  - Protocol Specifications (available from manufacturer)

- Test Resources
  - Binary message samples
  - Expected position data
  - Command examples