# Arknav Protocol

## Overview

Arknav is a Taiwan-based GPS tracker manufacturer specializing in high-quality GPS tracking products for vehicle security, fleet management, and personal tracking applications. The Arknav protocol is used by various Arknav GPS tracking devices including the RX-8W, R-9PRO, R-9W, RX-9, RV-8, IR-7, CT-X8, AT-5000, AT-04, AT-9000, and DX-3 models.

This protocol documentation covers the communication format, message structure, and implementation details for integrating Arknav devices with the Traccar GPS tracking system.

## Transport

The Arknav protocol uses TCP as its primary transport layer. Devices connect to the server using a persistent TCP connection and send data packets containing position information, status updates, and other telemetry data.

**Default Port:** 6107

## Message Format

Arknav devices transmit data in a binary format with a specific structure that includes headers, payload data, and checksums. Each message follows this general structure:

### Message Structure

| Field | Size (bytes) | Description |
|-------|--------------|-------------|
| Header | 2 | Message start marker (0x7878) |
| Length | 1 | Length of the payload (excluding header, length, and checksum) |
| Protocol ID | 1 | Identifies the type of message |
| Payload | Variable | Message data (depends on protocol ID) |
| Checksum | 2 | CRC-16 checksum for error detection |
| Footer | 2 | Message end marker (0x0D0A) |

### Protocol IDs

The protocol ID field identifies the type of message being transmitted:

| Protocol ID | Description |
|-------------|-------------|
| 0x01 | Login message |
| 0x12 | Position report |
| 0x13 | Heartbeat |
| 0x16 | Alarm report |
| 0x15 | Status information |
| 0x80 | Command response |

### Login Message (0x01)

The login message is sent when the device first connects to the server. It contains the device's unique identifier (IMEI).

**Payload format:**

| Field | Size (bytes) | Description |
|-------|--------------|-------------|
| IMEI | 8 | Device IMEI number (BCD encoded) |
| Serial Number | 2 | Device serial number |

### Position Report (0x12)

The position report message contains GPS location data and various status information.

**Payload format:**

| Field | Size (bytes) | Description |
|-------|--------------|-------------|
| Date Time | 6 | UTC date and time (YY MM DD HH MM SS) |
| Satellites | 1 | Number of GPS satellites |
| Latitude | 4 | Latitude in degrees (multiplied by 1,000,000) |
| Longitude | 4 | Longitude in degrees (multiplied by 1,000,000) |
| Speed | 1 | Speed in knots |
| Course | 2 | Direction in degrees (0-359) |
| Status | 1 | Device status flags |
| MCC | 2 | Mobile Country Code |
| MNC | 1 | Mobile Network Code |
| LAC | 2 | Location Area Code |
| Cell ID | 2 | Cell tower ID |

### Heartbeat Message (0x13)

The heartbeat message is sent periodically to maintain the connection with the server.

**Payload format:**

| Field | Size (bytes) | Description |
|-------|--------------|-------------|
| Status | 1 | Device status flags |

### Alarm Report (0x16)

The alarm report message is sent when an alarm condition is triggered on the device.

**Payload format:**

| Field | Size (bytes) | Description |
|-------|--------------|-------------|
| Date Time | 6 | UTC date and time (YY MM DD HH MM SS) |
| Satellites | 1 | Number of GPS satellites |
| Latitude | 4 | Latitude in degrees (multiplied by 1,000,000) |
| Longitude | 4 | Longitude in degrees (multiplied by 1,000,000) |
| Speed | 1 | Speed in knots |
| Course | 2 | Direction in degrees (0-359) |
| Status | 1 | Device status flags |
| Alarm Type | 1 | Type of alarm triggered |
| MCC | 2 | Mobile Country Code |
| MNC | 1 | Mobile Network Code |
| LAC | 2 | Location Area Code |
| Cell ID | 2 | Cell tower ID |

### Status Information (0x15)

The status information message contains various device status data.

**Payload format:**

| Field | Size (bytes) | Description |
|-------|--------------|-------------|
| Battery Level | 1 | Battery level percentage |
| External Power | 1 | External power status |
| GSM Signal | 1 | GSM signal strength |
| Status Flags | 2 | Various status flags |

### Command Response (0x80)

The command response message is sent in response to a command from the server.

**Payload format:**

| Field | Size (bytes) | Description |
|-------|--------------|-------------|
| Command Type | 1 | Type of command being responded to |
| Response Status | 1 | Status of command execution (0 = success) |

## Supported Commands

The Arknav protocol supports the following commands that can be sent from the server to the device:

| Command | Description |
|---------|-------------|
| Position Request | Request current position from device |
| Configuration | Configure device parameters |
| Reboot | Restart the device |
| Engine Stop | Remotely disable vehicle engine |
| Engine Resume | Remotely enable vehicle engine |
| Set Geofence | Configure geofence parameters |
| Set Reporting Interval | Change position reporting frequency |

### Command Format

Commands sent to the device follow this structure:

| Field | Size (bytes) | Description |
|-------|--------------|-------------|
| Header | 2 | Command start marker (0x7878) |
| Length | 1 | Length of the payload |
| Protocol ID | 1 | Command type identifier |
| Command Data | Variable | Command-specific data |
| Checksum | 2 | CRC-16 checksum |
| Footer | 2 | Command end marker (0x0D0A) |

## Implementation in Traccar

The Arknav protocol is implemented in Traccar through the following classes:

### ArknavProtocol

The `ArknavProtocol` class extends `BaseProtocol` and defines the protocol characteristics, including supported commands and server-side configuration.

```java
public class ArknavProtocol extends BaseProtocol {

    public ArknavProtocol() {
        super("arknav");
        setSupportedDataCommands(
                Command.TYPE_CUSTOM,
                Command.TYPE_POSITION_SINGLE,
                Command.TYPE_ENGINE_STOP,
                Command.TYPE_ENGINE_RESUME);
        addServer(new TraccarServer(6107));
    }

    @Override
    public void initTrackerServers(List<TrackerServer> serverList) {
        serverList.add(new TrackerServer(false, getName()) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline) {
                pipeline.addLast(new ArknavFrameDecoder());
                pipeline.addLast(new ArknavProtocolDecoder(ArknavProtocol.this));
                pipeline.addLast(new ArknavProtocolEncoder(ArknavProtocol.this));
            }
        });
    }
}
```

### ArknavFrameDecoder

The `ArknavFrameDecoder` class extends `ByteToMessageDecoder` and is responsible for extracting complete frames from the incoming data stream.

```java
public class ArknavFrameDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
        if (buf.readableBytes() < 5) {
            return;
        }

        int beginIndex = buf.readerIndex();
        for (int i = beginIndex; i < buf.writerIndex() - 1; i++) {
            if (buf.getByte(i) == 0x78 && buf.getByte(i + 1) == 0x78) {
                buf.readerIndex(i);
                int length = buf.getByte(i + 2) & 0xFF;
                if (buf.readableBytes() >= length + 5) {
                    ByteBuf frame = buf.readRetainedSlice(length + 5);
                    out.add(frame);
                }
                return;
            }
        }

        buf.readerIndex(buf.writerIndex() - 1);
    }
}
```

### ArknavProtocolDecoder

The `ArknavProtocolDecoder` class extends `BaseProtocolDecoder` and is responsible for parsing the device messages and converting them into the Traccar position model.

```java
public class ArknavProtocolDecoder extends BaseProtocolDecoder {

    public ArknavProtocolDecoder(Protocol protocol) {
        super(protocol);
    }

    private String decodeImei(ByteBuf buf, int length) {
        StringBuilder imei = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int b = buf.readUnsignedByte();
            imei.append((char) ((b & 0xf0) >> 4) + '0');
            imei.append((char) (b & 0x0f) + '0');
        }
        return imei.toString();
    }

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {

        ByteBuf buf = (ByteBuf) msg;

        buf.skipBytes(2); // header
        int length = buf.readUnsignedByte();
        int protocolId = buf.readUnsignedByte();

        if (protocolId == 0x01) { // login message
            String imei = decodeImei(buf, 8);
            DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, imei);
            if (deviceSession != null && channel != null) {
                ByteBuf response = Unpooled.buffer();
                response.writeByte(0x78);
                response.writeByte(0x78);
                response.writeByte(0x01); // length
                response.writeByte(0x01); // protocol id
                response.writeByte(0x00); // response
                response.writeByte(0x00); // checksum
                response.writeByte(0x00); // checksum
                response.writeByte(0x0D);
                response.writeByte(0x0A);
                channel.writeAndFlush(new NetworkMessage(response, remoteAddress));
            }
            return null;
        }

        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress);
        if (deviceSession == null) {
            return null;
        }

        if (protocolId == 0x12 || protocolId == 0x16) { // position report or alarm

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

            // Parse location data
            int satellites = buf.readUnsignedByte();
            position.set(Position.KEY_SATELLITES, satellites);

            double latitude = buf.readInt() / 1000000.0;
            double longitude = buf.readInt() / 1000000.0;
            position.setLatitude(latitude);
            position.setLongitude(longitude);

            int speed = buf.readUnsignedByte();
            position.setSpeed(UnitsConverter.knotsFromKph(speed));

            int course = buf.readUnsignedShort();
            position.setCourse(course);

            int status = buf.readUnsignedByte();
            position.setValid((status & 0x01) != 0);
            position.set(Position.KEY_STATUS, status);

            if (protocolId == 0x16) { // alarm report
                int alarmType = buf.readUnsignedByte();
                position.set(Position.KEY_ALARM, decodeAlarm(alarmType));
            }

            // Parse network information
            if (buf.readableBytes() >= 7) {
                position.set(Position.KEY_MCC, buf.readUnsignedShort());
                position.set(Position.KEY_MNC, buf.readUnsignedByte());
                position.set(Position.KEY_LAC, buf.readUnsignedShort());
                position.set(Position.KEY_CID, buf.readUnsignedShort());
            }

            return position;
        }

        return null;
    }

    private String decodeAlarm(int alarmType) {
        switch (alarmType) {
            case 0x01:
                return Position.ALARM_SOS;
            case 0x02:
                return Position.ALARM_POWER_CUT;
            case 0x03:
                return Position.ALARM_LOW_BATTERY;
            case 0x04:
                return Position.ALARM_OVERSPEED;
            case 0x05:
                return Position.ALARM_GEOFENCE_ENTER;
            case 0x06:
                return Position.ALARM_GEOFENCE_EXIT;
            case 0x07:
                return Position.ALARM_TOW;
            case 0x08:
                return Position.ALARM_VIBRATION;
            default:
                return null;
        }
    }
}
```

### ArknavProtocolEncoder

The `ArknavProtocolEncoder` class extends `BaseProtocolEncoder` and is responsible for encoding commands sent from the server to the device.

```java
public class ArknavProtocolEncoder extends BaseProtocolEncoder {

    public ArknavProtocolEncoder(Protocol protocol) {
        super(protocol);
    }

    @Override
    protected Object encodeCommand(Command command) {

        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(0x78);
        buf.writeByte(0x78);

        switch (command.getType()) {
            case Command.TYPE_POSITION_SINGLE:
                buf.writeByte(0x01); // length
                buf.writeByte(0x41); // command type
                break;
            case Command.TYPE_ENGINE_STOP:
                buf.writeByte(0x01); // length
                buf.writeByte(0x44); // command type
                break;
            case Command.TYPE_ENGINE_RESUME:
                buf.writeByte(0x01); // length
                buf.writeByte(0x45); // command type
                break;
            case Command.TYPE_CUSTOM:
                buf.writeByte(0x01); // length
                buf.writeByte(Integer.parseInt(command.getString(Command.KEY_DATA)));
                break;
            default:
                return null;
        }

        // Placeholder for checksum
        buf.writeByte(0x00);
        buf.writeByte(0x00);

        buf.writeByte(0x0D);
        buf.writeByte(0x0A);

        return buf;
    }
}
```

## Configuration

To configure Traccar to work with Arknav devices, add the following to your `traccar.xml` configuration file:

```xml
<entry key='arknav.port'>6107</entry>
```

Additional configuration options:

```xml
<entry key='arknav.timeout'>60</entry> <!-- Connection timeout in seconds -->
```

## Examples

### Login Message

**Hex Format:**
```
78780D01012345678901234500018CDD0D0A
```

**Breakdown:**
- `7878`: Header
- `0D`: Length (13 bytes)
- `01`: Protocol ID (login)
- `0123456789012345`: IMEI (BCD encoded)
- `0001`: Serial number
- `8CDD`: Checksum
- `0D0A`: Footer

### Position Report

**Hex Format:**
```
78781210051E0C2A1D0C8E1D023305BD027AC8800014240D0A
```

**Breakdown:**
- `7878`: Header
- `12`: Length (18 bytes)
- `10`: Protocol ID (position report)
- `051E0C2A1D0C`: Date/time (05/30/12 10:29:12)
- `8E`: Satellites (14)
- `1D023305`: Latitude (22.123456°)
- `BD027AC8`: Longitude (114.123456°)
- `80`: Speed (128 km/h)
- `0014`: Course (20°)
- `24`: Status
- `0D0A`: Footer

### Heartbeat Message

**Hex Format:**
```
787801130824450D0A
```

**Breakdown:**
- `7878`: Header
- `01`: Length (1 byte)
- `13`: Protocol ID (heartbeat)
- `08`: Status
- `2445`: Checksum
- `0D0A`: Footer

## References

1. Arknav GPS Tracker Manufacturer - [Official Website](http://www.arknavgps.com.tw/)
2. Traccar Protocol Implementation Guide - [Traccar Documentation](https://www.traccar.org/implement-protocol/)