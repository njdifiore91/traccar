# Anytrek Protocol Documentation

## Overview

The Anytrek protocol is used by GPS tracking devices manufactured by Anytrek Corporation, a company specializing in GPS tracking solutions for the transportation industry. Anytrek devices are primarily designed for fleet management, trailer tracking, and asset monitoring applications.

This protocol documentation covers the communication specifications, message formats, and implementation details for integrating Anytrek devices with the Traccar GPS tracking platform.

## Supported Devices

The Anytrek protocol supports various device models manufactured by Anytrek, including:

- TrackLight Series (VT1711, VT2011, VT2211) - GPS trackers embedded in LED trailer lights
- In-Dash Tracker (VT2208) - Covert vehicle tracker installed in the dashboard
- OBD Tracker - Plug-and-play device for OBD-II port
- ThermoTrack - Temperature monitoring device with GPS tracking
- Asset Tracker (AT2217) - Tracker for non-powered assets

## Protocol Specifications

### Connection Type

- **Transport Protocol**: TCP
- **Default Port**: 5100

### Authentication

Anytrek devices authenticate using a unique device identifier (IMEI or device ID) included in each message. This identifier is used to associate the device with a specific account in the tracking system.

### Message Format

The Anytrek protocol uses a binary message format with the following general structure:

```
[Header][Length][Message Type][Device ID][Payload][Checksum][Footer]
```

Where:
- **Header**: Fixed start bytes (0x7878 or 0x7979) indicating the beginning of a message
- **Length**: Length of the message excluding header and footer
- **Message Type**: Identifier for the type of message (login, heartbeat, location, etc.)
- **Device ID**: Unique identifier for the device (IMEI number)
- **Payload**: Variable-length data specific to the message type
- **Checksum**: CRC or checksum for data verification
- **Footer**: Fixed end bytes (0x0D0A) indicating the end of a message

## Message Types

### Login Message (0x01)

Sent by the device when establishing a connection with the server.

```
[Header][Length][0x01][Device ID][Information Content][Checksum][Footer]
```

The server should respond with an acknowledgment message.

### Heartbeat Message (0x13)

Periodically sent by the device to maintain the connection.

```
[Header][Length][0x13][Device ID][Status Information][Checksum][Footer]
```

The server should respond with an acknowledgment message.

### Location Message (0x22)

Contains GPS position data and device status information.

```
[Header][Length][0x22][Device ID][Date Time][GPS Information][Status][Checksum][Footer]
```

Where:
- **Date Time**: UTC timestamp in YY-MM-DD-HH-MM-SS format
- **GPS Information**: Latitude, longitude, speed, course
- **Status**: Device status flags (ignition, power, etc.)

The server should respond with an acknowledgment message.

### Alarm Message (0x26)

Sent when an alarm condition is detected (geofence violation, low battery, etc.).

```
[Header][Length][0x26][Device ID][Date Time][GPS Information][Alarm Type][Checksum][Footer]
```

The server should respond with an acknowledgment message.

### Acknowledgment Message (0x15)

Sent by the server in response to device messages.

```
[Header][Length][0x15][Device ID][Message Type][Checksum][Footer]
```

Where **Message Type** is the type of the message being acknowledged.

## Command Protocol

Commands can be sent to Anytrek devices using the following format:

```
[Header][Length][0x80][Device ID][Command Type][Command Parameters][Checksum][Footer]
```

Supported commands include:

- **Position Request (0x01)**: Request current position
- **Configuration (0x02)**: Configure device parameters
- **Reboot (0x03)**: Restart the device
- **Reset (0x04)**: Reset to factory settings
- **Power Mode (0x05)**: Change power saving mode

The device will respond with a command acknowledgment message.

## Traccar Implementation

### Protocol Class

The Anytrek protocol is implemented in Traccar through the `AnytrekProtocol` class which extends `BaseProtocol`. This class defines the protocol characteristics, supported commands, and configures the pipeline for message processing.

```java
public class AnytrekProtocol extends BaseProtocol {

    public AnytrekProtocol() {
        super("anytrek");
        setTextCommandEncoder(null);
        setSupportedDataCommands(
                Command.TYPE_CUSTOM,
                Command.TYPE_REQUEST_PHOTO,
                Command.TYPE_REBOOT_DEVICE,
                Command.TYPE_POSITION_SINGLE,
                Command.TYPE_ENGINE_STOP,
                Command.TYPE_ENGINE_RESUME);
    }

    @Override
    public void initTrackerServers(List<TrackerServer> serverList) {
        serverList.add(new TrackerServer(false, getName()) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline) {
                pipeline.addLast(new AnytrekFrameDecoder());
                pipeline.addLast(new AnytrekProtocolDecoder(AnytrekProtocol.this));
            }
        });
    }
}
```

### Protocol Decoder

The `AnytrekProtocolDecoder` class is responsible for parsing the binary messages received from Anytrek devices and converting them into the Traccar position model.

```java
public class AnytrekProtocolDecoder extends BaseProtocolDecoder {

    public AnytrekProtocolDecoder(AnytrekProtocol protocol) {
        super(protocol);
    }

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {
        
        ByteBuf buf = (ByteBuf) msg;
        
        // Parse message header
        int header = buf.readUnsignedShort();
        int length = buf.readUnsignedByte();
        int messageType = buf.readUnsignedByte();
        
        // Parse device identifier
        String imei = String.valueOf(buf.readLong());
        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, imei);
        if (deviceSession == null) {
            return null;
        }
        
        // Handle different message types
        switch (messageType) {
            case 0x01: // Login message
                // Send acknowledgment
                if (channel != null) {
                    ByteBuf response = Unpooled.buffer();
                    response.writeShort(0x7878);
                    response.writeByte(0x01); // Length
                    response.writeByte(0x15); // Acknowledgment
                    response.writeByte(0x01); // Login message type
                    response.writeByte(calculateChecksum(response));
                    response.writeBytes(new byte[] {0x0D, 0x0A});
                    channel.writeAndFlush(new NetworkMessage(response, remoteAddress));
                }
                return null;
                
            case 0x22: // Location message
                Position position = new Position(getProtocolName());
                position.setDeviceId(deviceSession.getDeviceId());
                
                // Parse date and time
                int year = buf.readUnsignedByte();
                int month = buf.readUnsignedByte();
                int day = buf.readUnsignedByte();
                int hour = buf.readUnsignedByte();
                int minute = buf.readUnsignedByte();
                int second = buf.readUnsignedByte();
                
                DateBuilder dateBuilder = new DateBuilder()
                        .setDate(2000 + year, month, day)
                        .setTime(hour, minute, second);
                position.setTime(dateBuilder.getDate());
                
                // Parse GPS data
                double latitude = buf.readUnsignedInt() / 1800000.0;
                double longitude = buf.readUnsignedInt() / 1800000.0;
                position.setLatitude(latitude);
                position.setLongitude(longitude);
                
                int speed = buf.readUnsignedByte();
                position.setSpeed(UnitsConverter.knotsFromKph(speed));
                
                int course = buf.readUnsignedShort();
                position.setCourse(course);
                
                // Parse status flags
                int status = buf.readUnsignedByte();
                position.set(Position.KEY_IGNITION, BitUtil.check(status, 0));
                position.set(Position.KEY_STATUS, status);
                
                // Send acknowledgment
                if (channel != null) {
                    ByteBuf response = Unpooled.buffer();
                    response.writeShort(0x7878);
                    response.writeByte(0x01); // Length
                    response.writeByte(0x15); // Acknowledgment
                    response.writeByte(0x22); // Location message type
                    response.writeByte(calculateChecksum(response));
                    response.writeBytes(new byte[] {0x0D, 0x0A});
                    channel.writeAndFlush(new NetworkMessage(response, remoteAddress));
                }
                
                return position;
                
            // Handle other message types...
            
            default:
                return null;
        }
    }
    
    private byte calculateChecksum(ByteBuf buffer) {
        byte checksum = 0;
        for (int i = 2; i < buffer.writerIndex(); i++) {
            checksum ^= buffer.getByte(i);
        }
        return checksum;
    }
}
```

### Frame Decoder

The `AnytrekFrameDecoder` class is responsible for extracting complete messages from the incoming data stream.

```java
public class AnytrekFrameDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 5) {
            return;
        }

        int headerIndex = in.readerIndex();
        int header = in.getUnsignedShort(headerIndex);

        if (header == 0x7878 || header == 0x7979) {
            int length = in.getUnsignedByte(headerIndex + 2);
            int endIndex = headerIndex + length + 5; // header (2) + length (1) + payload (length) + footer (2)

            if (in.writerIndex() >= endIndex) {
                ByteBuf frame = in.retainedSlice(headerIndex, endIndex - headerIndex);
                in.readerIndex(endIndex);
                out.add(frame);
            }
        } else {
            in.skipBytes(1); // Skip one byte and try again
        }
    }
}
```

## Configuration

To enable the Anytrek protocol in Traccar, add the following lines to the `traccar.xml` configuration file:

```xml
<entry key='anytrek.port'>5100</entry>
```

Additional configuration options:

```xml
<entry key='anytrek.timeout'>60</entry> <!-- Connection timeout in seconds -->
<entry key='anytrek.extended'>true</entry> <!-- Enable extended functionality -->
```

## Troubleshooting

### Common Issues

1. **Device not connecting**: Verify that the correct port is configured in both the device and server.

2. **No position updates**: Check that the device has a valid GPS fix and that the reporting interval is configured correctly.

3. **Invalid data**: Ensure that the device firmware is up to date and compatible with the protocol implementation.

### Debugging

Enable debug logging in Traccar to see detailed information about the communication with Anytrek devices:

```xml
<entry key='logger.anytrek'>DEBUG</entry>
```

## References

- [Anytrek Corporation](https://www.anytrek.com/) - Official website
- [AnyFleet Platform](https://www.anytrek.com/products/anyfleet-platform/) - Anytrek's tracking platform
- [Traccar Protocol Implementation Guide](https://www.traccar.org/implement-protocol/) - Guide for implementing new protocols in Traccar