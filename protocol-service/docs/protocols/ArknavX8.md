# ArknavX8 Protocol Documentation

## Overview

The ArknavX8 protocol is used by Arknav GPS tracking devices, specifically the CT-X8 container lock GPS asset tracker. This protocol enables communication between the tracking device and the Traccar server for real-time position tracking, status monitoring, and command execution.

Arknav devices are known for their rugged design, waterproof capabilities, and reliable performance in harsh environments, making them ideal for container tracking and asset monitoring applications.

## Device Specifications

- **Manufacturer**: Arknav (Taiwan)
- **Model**: CT-X8 Container Lock GPS Asset Tracker
- **Key Features**:
  - Highly sensitive internal GPS and GSM antennas
  - Rugged waterproof locking mechanism
  - Long battery life
  - Tamper detection
  - Motion sensing
  - Geofence capabilities

## Protocol Specification

### Connection Details

- **Transport Protocol**: TCP
- **Default Port**: 5047
- **Connection Mode**: Client-initiated (device connects to server)

### Message Format

The ArknavX8 protocol uses a binary message format with the following structure:

```
[Header][Length][Message ID][Device ID][Data][Checksum][Footer]
```

Where:
- **Header**: Fixed 2-byte value (0x7878) marking the start of a message
- **Length**: 1 byte indicating the length of the message (excluding header and footer)
- **Message ID**: 1 byte identifying the message type
- **Device ID**: 8 bytes containing the device's unique identifier (IMEI)
- **Data**: Variable length field containing the message payload
- **Checksum**: 2 bytes CRC-16 checksum of all bytes between Header and Checksum
- **Footer**: Fixed 2-byte value (0x0D0A) marking the end of a message

### Message Types

#### Login Message (0x01)

Sent by the device when establishing a connection with the server.

```
[0x7878][Length][0x01][Device ID][Information Content][Checksum][0x0D0A]
```

#### Heartbeat Message (0x08)

Periodically sent by the device to maintain the connection.

```
[0x7878][Length][0x08][Device ID][Status][Checksum][0x0D0A]
```

#### Location Report (0x10)

Contains GPS position and status information.

```
[0x7878][Length][0x10][Device ID][Date Time][GPS Info][LBS Info][Status][Checksum][0x0D0A]
```

Where:
- **Date Time**: 6 bytes (YY MM DD HH MM SS)
- **GPS Info**: 12 bytes
  - Latitude: 4 bytes (decimal degrees × 1,000,000)
  - Longitude: 4 bytes (decimal degrees × 1,000,000)
  - Speed: 2 bytes (knots)
  - Course: 2 bytes (degrees from North)
- **LBS Info**: Cell tower information (variable length)
- **Status**: Device status flags (1 byte)

#### Alarm Report (0x12)

Sent when an alarm condition is detected.

```
[0x7878][Length][0x12][Device ID][Date Time][GPS Info][LBS Info][Alarm Type][Status][Checksum][0x0D0A]
```

Where:
- **Alarm Type**: 1 byte indicating the type of alarm
  - 0x01: SOS Button Pressed
  - 0x02: Power Cut
  - 0x03: Vibration
  - 0x04: Geofence Entry
  - 0x05: Geofence Exit
  - 0x06: Overspeed
  - 0x07: Low Battery

#### Command Response (0x15)

Sent by the device in response to a command from the server.

```
[0x7878][Length][0x15][Device ID][Command ID][Response Status][Checksum][0x0D0A]
```

### Server to Device Commands

#### Position Request (0x30)

Requests the current position from the device.

```
[0x7878][Length][0x30][Device ID][Checksum][0x0D0A]
```

#### Configuration Command (0x31)

Sends configuration parameters to the device.

```
[0x7878][Length][0x31][Device ID][Parameter ID][Parameter Value][Checksum][0x0D0A]
```

Where:
- **Parameter ID**: 1 byte identifying the parameter to configure
- **Parameter Value**: Variable length field containing the parameter value

#### Lock/Unlock Command (0x32)

Sends a command to lock or unlock the container lock mechanism.

```
[0x7878][Length][0x32][Device ID][Lock Command][Checksum][0x0D0A]
```

Where:
- **Lock Command**: 1 byte (0x01 for lock, 0x00 for unlock)

## Traccar Implementation

### Protocol Class

The ArknavX8 protocol is implemented in Traccar through the `ArknavX8Protocol` class which extends `BaseProtocol`. This class defines the protocol characteristics and configures the pipeline for processing messages.

```java
public class ArknavX8Protocol extends BaseProtocol {

    public ArknavX8Protocol() {
        super("arknavx8");
        addServer(new TrackerServer(false, getName()) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline) {
                pipeline.addLast(new ArknavX8ProtocolDecoder(ArknavX8Protocol.this));
                pipeline.addLast(new ArknavX8ProtocolEncoder(ArknavX8Protocol.this));
            }
        });
    }

    @Override
    public boolean getSupportedDataCommands() {
        return true;
    }

    @Override
    public String getConfigCommandType() {
        return "configuration";
    }

    @Override
    public Collection<String> getSupportedTextCommands() {
        return Arrays.asList(
                Command.TYPE_CUSTOM,
                Command.TYPE_REQUEST_PHOTO,
                Command.TYPE_POSITION_SINGLE,
                Command.TYPE_ENGINE_STOP,
                Command.TYPE_ENGINE_RESUME,
                Command.TYPE_LOCK,
                Command.TYPE_UNLOCK);
    }
}
```

### Protocol Decoder

The `ArknavX8ProtocolDecoder` class extends `BaseProtocolDecoder` and is responsible for decoding incoming messages from the device.

```java
public class ArknavX8ProtocolDecoder extends BaseProtocolDecoder {

    public ArknavX8ProtocolDecoder(Protocol protocol) {
        super(protocol);
    }

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {
        
        ByteBuf buf = (ByteBuf) msg;
        
        // Verify message header
        if (buf.readUnsignedShort() != 0x7878) {
            return null;
        }
        
        int length = buf.readUnsignedByte();
        int messageType = buf.readUnsignedByte();
        
        // Read device ID (IMEI)
        String imei = buf.readSlice(8).toString(StandardCharsets.US_ASCII);
        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, imei);
        if (deviceSession == null) {
            return null;
        }
        
        // Process different message types
        switch (messageType) {
            case 0x01: // Login message
                if (channel != null) {
                    ByteBuf response = Unpooled.buffer();
                    response.writeShort(0x7878);
                    response.writeByte(0x05);
                    response.writeByte(0x01);
                    response.writeByte(0x00); // Login success
                    response.writeShort(calcCrc(response));
                    response.writeShort(0x0D0A);
                    channel.writeAndFlush(new NetworkMessage(response, remoteAddress));
                }
                return null;
                
            case 0x08: // Heartbeat message
                if (channel != null) {
                    ByteBuf response = Unpooled.buffer();
                    response.writeShort(0x7878);
                    response.writeByte(0x05);
                    response.writeByte(0x08);
                    response.writeByte(0x00); // Heartbeat acknowledge
                    response.writeShort(calcCrc(response));
                    response.writeShort(0x0D0A);
                    channel.writeAndFlush(new NetworkMessage(response, remoteAddress));
                }
                return null;
                
            case 0x10: // Location report
            case 0x12: // Alarm report
                Position position = new Position(getProtocolName());
                position.setDeviceId(deviceSession.getDeviceId());
                
                // Date and time
                int year = buf.readUnsignedByte();
                int month = buf.readUnsignedByte();
                int day = buf.readUnsignedByte();
                int hour = buf.readUnsignedByte();
                int minute = buf.readUnsignedByte();
                int second = buf.readUnsignedByte();
                
                position.setTime(new Date(Date.UTC(year + 2000, month - 1, day, hour, minute, second)));
                
                // GPS data
                int latitude = buf.readInt();
                int longitude = buf.readInt();
                position.setLatitude(latitude / 1000000.0);
                position.setLongitude(longitude / 1000000.0);
                
                position.setSpeed(UnitsConverter.knotsFromKph(buf.readUnsignedShort()));
                position.setCourse(buf.readUnsignedShort());
                
                // Status
                int status = buf.readUnsignedByte();
                position.setValid((status & 0x01) > 0);
                position.set(Position.KEY_STATUS, status);
                
                // Handle alarm type for alarm reports
                if (messageType == 0x12) {
                    int alarmType = buf.readUnsignedByte();
                    switch (alarmType) {
                        case 0x01:
                            position.set(Position.KEY_ALARM, Position.ALARM_SOS);
                            break;
                        case 0x02:
                            position.set(Position.KEY_ALARM, Position.ALARM_POWER_CUT);
                            break;
                        case 0x03:
                            position.set(Position.KEY_ALARM, Position.ALARM_VIBRATION);
                            break;
                        case 0x04:
                            position.set(Position.KEY_ALARM, Position.ALARM_GEOFENCE_ENTER);
                            break;
                        case 0x05:
                            position.set(Position.KEY_ALARM, Position.ALARM_GEOFENCE_EXIT);
                            break;
                        case 0x06:
                            position.set(Position.KEY_ALARM, Position.ALARM_OVERSPEED);
                            break;
                        case 0x07:
                            position.set(Position.KEY_ALARM, Position.ALARM_LOW_BATTERY);
                            break;
                        default:
                            position.set(Position.KEY_ALARM, Position.ALARM_GENERAL);
                            break;
                    }
                }
                
                return position;
                
            case 0x15: // Command response
                // Process command response if needed
                return null;
                
            default:
                return null;
        }
    }
    
    private int calcCrc(ByteBuf buf) {
        // CRC calculation logic
        return 0; // Placeholder for actual implementation
    }
}
```

### Protocol Encoder

The `ArknavX8ProtocolEncoder` class extends `BaseProtocolEncoder` and is responsible for encoding outgoing commands to the device.

```java
public class ArknavX8ProtocolEncoder extends BaseProtocolEncoder {

    public ArknavX8ProtocolEncoder(Protocol protocol) {
        super(protocol);
    }

    @Override
    protected Object encodeCommand(Command command) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(0x7878); // Header
        
        switch (command.getType()) {
            case Command.TYPE_POSITION_SINGLE:
                buf.writeByte(0x05); // Length
                buf.writeByte(0x30); // Position request command
                buf.writeBytes(command.getDeviceId().getBytes(StandardCharsets.US_ASCII));
                buf.writeShort(calcCrc(buf));
                buf.writeShort(0x0D0A); // Footer
                return buf;
                
            case Command.TYPE_ENGINE_STOP:
                buf.writeByte(0x06); // Length
                buf.writeByte(0x31); // Configuration command
                buf.writeBytes(command.getDeviceId().getBytes(StandardCharsets.US_ASCII));
                buf.writeByte(0x01); // Engine control parameter
                buf.writeByte(0x01); // Stop engine
                buf.writeShort(calcCrc(buf));
                buf.writeShort(0x0D0A); // Footer
                return buf;
                
            case Command.TYPE_ENGINE_RESUME:
                buf.writeByte(0x06); // Length
                buf.writeByte(0x31); // Configuration command
                buf.writeBytes(command.getDeviceId().getBytes(StandardCharsets.US_ASCII));
                buf.writeByte(0x01); // Engine control parameter
                buf.writeByte(0x00); // Resume engine
                buf.writeShort(calcCrc(buf));
                buf.writeShort(0x0D0A); // Footer
                return buf;
                
            case Command.TYPE_LOCK:
                buf.writeByte(0x06); // Length
                buf.writeByte(0x32); // Lock/Unlock command
                buf.writeBytes(command.getDeviceId().getBytes(StandardCharsets.US_ASCII));
                buf.writeByte(0x01); // Lock
                buf.writeShort(calcCrc(buf));
                buf.writeShort(0x0D0A); // Footer
                return buf;
                
            case Command.TYPE_UNLOCK:
                buf.writeByte(0x06); // Length
                buf.writeByte(0x32); // Lock/Unlock command
                buf.writeBytes(command.getDeviceId().getBytes(StandardCharsets.US_ASCII));
                buf.writeByte(0x00); // Unlock
                buf.writeShort(calcCrc(buf));
                buf.writeShort(0x0D0A); // Footer
                return buf;
                
            default:
                return null;
        }
    }
    
    private int calcCrc(ByteBuf buf) {
        // CRC calculation logic
        return 0; // Placeholder for actual implementation
    }
}
```

## Configuration Parameters

The ArknavX8 protocol supports the following configuration parameters in the Traccar server:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `arknavx8.port` | 5047 | TCP port for the ArknavX8 protocol |
| `arknavx8.timeout` | 60 | Connection timeout in seconds |
| `arknavx8.heartbeat` | 300 | Heartbeat interval in seconds |
| `arknavx8.extended` | false | Enable extended functionality |

These parameters can be set in the Traccar configuration file (`conf/traccar.xml`):

```xml
<entry key='arknavx8.port'>5047</entry>
<entry key='arknavx8.timeout'>60</entry>
<entry key='arknavx8.heartbeat'>300</entry>
<entry key='arknavx8.extended'>false</entry>
```

## Usage Examples

### Device Setup

1. Configure the ArknavX8 device with the following settings:
   - Server IP: Your Traccar server IP address
   - Server Port: 5047 (or your custom port)
   - APN: Your cellular provider's APN
   - Reporting Interval: As needed (e.g., 60 seconds)

2. Add the device to Traccar:
   - Go to Traccar web interface
   - Navigate to Devices > Add
   - Enter a name for the device
   - Enter the device identifier (IMEI)
   - Select "ArknavX8" as the protocol
   - Click Save

### Sending Commands

To send a command to the device:

1. Go to Traccar web interface
2. Navigate to Devices
3. Select the device
4. Click on Commands
5. Select the desired command (e.g., "Request Position", "Lock", "Unlock")
6. Click Send

## Troubleshooting

### Common Issues

1. **Device not connecting**:
   - Verify the server IP and port are correctly configured on the device
   - Check that the port is open in your firewall
   - Ensure the device has an active SIM card with data connectivity

2. **Device connected but not reporting positions**:
   - Check that the device has a valid GPS fix
   - Verify the reporting interval is correctly configured
   - Ensure the device is properly powered

3. **Commands not being received by the device**:
   - Verify the device is online in Traccar
   - Check that the command is supported by the protocol
   - Ensure the device has a stable connection

### Debugging

To enable debug logging for the ArknavX8 protocol, add the following to your `conf/traccar.xml` file:

```xml
<entry key='logger.arknavx8'>DEBUG</entry>
```

This will provide detailed logs of all communication with ArknavX8 devices, which can be helpful for troubleshooting.

## References

- [Arknav Official Website](https://www.arknavgps.com.tw/)
- [Traccar Protocol Implementation Guide](https://www.traccar.org/implement-protocol/)
- [Traccar Protocol Identification](https://www.traccar.org/identify-protocol/)