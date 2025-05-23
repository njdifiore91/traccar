# Arnavi Protocol Documentation

## Overview

The Arnavi protocol is used by GPS tracking devices manufactured by Arusnavi. It supports both text and binary message formats, allowing for flexible communication between devices and the tracking server. Traccar implements comprehensive support for both formats through specialized decoders.

## Device Models

The Arnavi protocol is used by various GPS tracking devices, including:

- **Arnavi 4**: An affordable yet feature-rich vehicle GPS tracker
- **Arnavi 5**: A feature-rich GPS tracker capable of reporting to two monitoring servers, with RS232, RS485, and 1-WIRE interfaces, CANbus support, dual SIM capability, and more
- **Arnavi Integral 3**: A compact, economical, and easy-to-install GPS tracker with RS-485 or RS-232 interfaces
- **Arnavi Beacon**: A GPS tracking device for vehicle theft protection and location detection with energy-efficient sleep mode

## Protocol Variants

The Arnavi protocol supports two main variants:

### Text Protocol

The text protocol uses ASCII messages that start with the `$AV` prefix. This format is human-readable and easier to debug but less efficient for data transmission.

### Binary Protocol

The binary protocol offers a more compact and efficient data format. It supports two versions:
- Version 1 (0x22): Original binary format
- Version 2 (0x23): Enhanced binary format with additional features

## Message Structure

### Text Protocol Format

Text messages follow this general format:
```
$AV,Vd,<device_id>,<index>,<power>,<battery>,<value>,<movement>,<ignition>,<input>,<input1>,<input1_value>,<input2>,<input2_value>,<fix_type>,<satellites>,<altitude>,<geoid_height>,<time>,<latitude>,<longitude>,<speed>,<course>,<date>

```

Example:
```
$AV,Vd,123456,1,1245,3789,-345,0,1,3,2,0,3,1,1,8,300.8,14.3,102134,5533.7107N,03733.1796E,0.000,131.8,021214
```

### Binary Protocol Format

Binary messages have a more complex structure:

1. **Header**: 
   - Start sign (0xFF)
   - Version byte (0x22 or 0x23)
   - Device identifier (IMEI as long integer)

2. **Message Body**:
   - Index byte
   - Record type byte (PING=0x00, DATA=0x01, TEXT=0x03, FILE=0x04, BINARY=0x06)
   - Length (unsigned short, little-endian)
   - Timestamp (unsigned int, little-endian, Unix time in seconds)
   - Data payload (varies by record type)
   - Checksum byte (modulo 256)

3. **Position Data Tags** (for DATA records):
   - TAG_LATITUDE (3): 4-byte float, little-endian
   - TAG_LONGITUDE (4): 4-byte float, little-endian
   - TAG_COORD_PARAMS (5): Course, altitude, satellites, speed

## Implementation Details

Traccar implements the Arnavi protocol through several specialized classes:

### ArnaviProtocol

The main protocol class that sets up the server and pipeline. It adds the necessary decoders to the pipeline for processing incoming messages.

```java
public class ArnaviProtocol extends BaseProtocol {
    @Inject
    public ArnaviProtocol(Config config) {
        addServer(new TrackerServer(config, getName(), false) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                pipeline.addLast(new ArnaviFrameDecoder());
                pipeline.addLast(new ArnaviProtocolDecoder(ArnaviProtocol.this));
            }
        });
    }
}
```

### ArnaviFrameDecoder

Handles the initial frame decoding for both text and binary formats. It identifies message boundaries and extracts complete frames from the incoming data stream.

- For text messages, it looks for the `$` prefix and `\r\n` suffix
- For binary messages, it handles different frame lengths based on message type

### ArnaviProtocolDecoder

Determines whether to use the text or binary decoder based on the message format. It examines the first byte of the message:

```java
@Override
protected Object decode(
        Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {

    ByteBuf buf = (ByteBuf) msg;

    if (buf.getByte(buf.readerIndex()) == '$') {
        return textProtocolDecoder.decode(channel, remoteAddress, msg);
    } else {
        return binaryProtocolDecoder.decode(channel, remoteAddress, msg);
    }
}
```

### ArnaviTextProtocolDecoder

Handles text protocol messages. It uses a regular expression pattern to parse the message fields and extract position information.

Key fields extracted:
- Device identifier
- Index
- Power and battery levels
- Ignition status
- Input states
- Satellite count
- Altitude
- Time, latitude, longitude
- Speed and course
- Date

### ArnaviBinaryProtocolDecoder

Handles binary protocol messages. It processes different record types and extracts position data from DATA records.

Key features:
- Handles device registration (messages with 0xFF start sign)
- Processes different record types (PING, DATA, TEXT, FILE, BINARY)
- Extracts position data using tag-based parsing
- Sends appropriate responses to the device

## Communication Flow

1. **Device Registration**:
   - Device sends a registration message with its IMEI
   - Server responds with an acknowledgment

2. **Position Reporting**:
   - Device sends position data in text or binary format
   - Server processes the data and responds with an acknowledgment
   - For binary messages, the server includes the message index in the response

3. **Server Response Format**:
   - For Version 1 (0x22): `0x7b 0x00 <index> 0x7d`
   - For Version 2 (0x23): `0x7b 0x04 0x00 <checksum> <timestamp> 0x7d`

## Supported Features

The Arnavi protocol in Traccar supports the following features:

- Real-time position tracking
- Device status monitoring (power, battery, ignition)
- Input/output state reporting
- Satellite information and GPS quality data
- Speed and course reporting
- Altitude reporting

## Configuration

No special configuration is required for the Arnavi protocol in Traccar. The system automatically detects the message format (text or binary) and processes it accordingly.

## Troubleshooting

1. **Connection Issues**:
   - Verify that the device is configured with the correct server IP and port
   - Check that the device has an active SIM card with data connectivity

2. **No Position Updates**:
   - Ensure the device has a clear view of the sky for GPS reception
   - Verify that the device is properly powered
   - Check the device configuration for correct reporting intervals

3. **Invalid Data**:
   - Verify that the device firmware is up to date
   - Check for any custom device configurations that might affect data format

## References

- [Arnavi Official Website](https://arusnavi.ru/) (in Russian)
- [Traccar Arnavi Protocol Implementation](https://github.com/traccar/traccar/tree/master/src/main/java/org/traccar/protocol)