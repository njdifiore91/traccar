# Astra Protocol

## Overview

The Astra protocol is a proprietary communication protocol developed for GPS tracking devices manufactured by Astra Telematics. It is designed for vehicle tracking, fleet management, and asset monitoring applications. The protocol supports a wide range of features including real-time tracking, historical data retrieval, event reporting, and remote device configuration.

Astra devices typically communicate over TCP/IP networks using cellular (2G/3G/4G) connectivity, providing reliable data transmission even in challenging network conditions. The protocol is optimized for low bandwidth usage while maintaining comprehensive telemetry capabilities.

### Key Features

- Real-time GPS position reporting
- Vehicle telemetry data (speed, heading, altitude)
- Engine diagnostics via CAN bus integration
- Digital and analog input monitoring
- Configurable event-based reporting
- Remote device configuration
- Firmware update capability
- Low power modes for battery-operated deployments

### Typical Applications

- Fleet management and vehicle tracking
- Heavy equipment monitoring
- Stolen vehicle recovery
- Driver behavior analysis
- Fuel consumption monitoring
- Temperature monitoring for refrigerated transport
- Asset security and geofence compliance

## Protocol Specification

### Communication Details

- **Transport Protocol**: TCP
- **Default Port**: 5013
- **Connection Mode**: Client-initiated persistent connection
- **Reconnection Strategy**: Exponential backoff with configurable parameters
- **Data Format**: Binary
- **Encryption**: Optional AES-128 encryption for sensitive data

### Protocol Structure

The Astra protocol uses a binary message format with the following general structure:

```
+----------------+----------------+----------------+----------------+
| Start Marker   | Message Length | Message Type   | Device ID      |
| (1 byte)       | (2 bytes)      | (1 byte)       | (4 bytes)      |
+----------------+----------------+----------------+----------------+
| Timestamp      | Payload        | Checksum       | End Marker     |
| (4 bytes)      | (variable)     | (2 bytes)      | (1 byte)       |
+----------------+----------------+----------------+----------------+
```

- **Start Marker**: Fixed value `0x7E` indicating the beginning of a message
- **Message Length**: Total length of the message in bytes (excluding start/end markers)
- **Message Type**: Identifies the type of message (position report, status update, etc.)
- **Device ID**: Unique identifier of the device (IMEI or custom ID)
- **Timestamp**: Unix timestamp in seconds since epoch
- **Payload**: Variable-length data specific to the message type
- **Checksum**: CRC-16 checksum for data integrity verification
- **End Marker**: Fixed value `0x7E` indicating the end of a message

### Message Types

| Type ID | Description | Direction |
|---------|-------------|----------|
| 0x01 | Login/Authentication | Device → Server |
| 0x02 | Authentication Response | Server → Device |
| 0x10 | Standard Position Report | Device → Server |
| 0x11 | Extended Position Report | Device → Server |
| 0x12 | Alarm/Event Report | Device → Server |
| 0x13 | Status Report | Device → Server |
| 0x20 | Command Request | Server → Device |
| 0x21 | Command Response | Device → Server |
| 0x30 | Configuration Request | Server → Device |
| 0x31 | Configuration Response | Device → Server |
| 0x40 | Heartbeat | Device ↔ Server |
| 0x41 | Acknowledgment | Device ↔ Server |

### Authentication Mechanism

Devices authenticate with the server using a login message (Type 0x01) containing:

1. Device IMEI or unique identifier
2. Authentication token (configurable, typically based on device serial number)
3. Firmware version information
4. Device capabilities bitmap

The server responds with an authentication response (Type 0x02) containing a status code indicating success or failure. Upon successful authentication, the device begins normal operation, sending position and status reports according to its configuration.

## Message Format Details

### Header Structure

All messages begin with a standard header:

```
+----------------+----------------+----------------+----------------+
| Start Marker   | Message Length | Message Type   | Device ID      |
| (1 byte)       | (2 bytes)      | (1 byte)       | (4 bytes)      |
+----------------+----------------+----------------+----------------+
```

- **Start Marker**: Always `0x7E`
- **Message Length**: 16-bit unsigned integer in big-endian format
- **Message Type**: 8-bit identifier as per the message types table
- **Device ID**: 32-bit device identifier in big-endian format

### Standard Position Report (Type 0x10)

The standard position report contains basic location information:

```
+----------------+----------------+----------------+----------------+
| Timestamp      | Latitude       | Longitude      | Speed          |
| (4 bytes)      | (4 bytes)      | (4 bytes)      | (2 bytes)      |
+----------------+----------------+----------------+----------------+
| Heading        | Altitude       | Satellites     | Status Flags   |
| (2 bytes)      | (2 bytes)      | (1 byte)       | (2 bytes)      |
+----------------+----------------+----------------+----------------+
```

- **Timestamp**: Unix timestamp (seconds since epoch)
- **Latitude**: Signed 32-bit integer representing latitude * 1,000,000
- **Longitude**: Signed 32-bit integer representing longitude * 1,000,000
- **Speed**: 16-bit unsigned integer representing speed in km/h * 10
- **Heading**: 16-bit unsigned integer representing heading in degrees (0-359)
- **Altitude**: 16-bit signed integer representing altitude in meters
- **Satellites**: 8-bit unsigned integer representing the number of satellites used
- **Status Flags**: 16-bit bitmap containing device status information

### Extended Position Report (Type 0x11)

The extended position report includes additional telemetry data:

```
+----------------+----------------+----------------+----------------+
| Standard Position Report Format (as above)                      |
+----------------+----------------+----------------+----------------+
| Battery Level  | External Power | Temperature    | Accelerometer |
| (1 byte)       | (2 bytes)      | (2 bytes)      | (6 bytes)      |
+----------------+----------------+----------------+----------------+
| Input States   | Output States  | Analog Inputs  | Odometer      |
| (1 byte)       | (1 byte)       | (variable)     | (4 bytes)      |
+----------------+----------------+----------------+----------------+
```

- **Battery Level**: 8-bit unsigned integer representing battery percentage (0-100)
- **External Power**: 16-bit unsigned integer representing external voltage in mV
- **Temperature**: 16-bit signed integer representing temperature in °C * 10
- **Accelerometer**: Three 16-bit signed integers (X, Y, Z) representing acceleration in mg
- **Input States**: 8-bit bitmap representing digital input states
- **Output States**: 8-bit bitmap representing digital output states
- **Analog Inputs**: Variable length field containing analog input readings
- **Odometer**: 32-bit unsigned integer representing odometer in meters

### Alarm/Event Report (Type 0x12)

The alarm/event report is sent when specific events occur:

```
+----------------+----------------+----------------+----------------+
| Standard Position Report Format (as above)                      |
+----------------+----------------+----------------+----------------+
| Event Type     | Event Data     | Event Duration | Reserved      |
| (1 byte)       | (4 bytes)      | (2 bytes)      | (1 byte)       |
+----------------+----------------+----------------+----------------+
```

- **Event Type**: 8-bit identifier indicating the type of event
- **Event Data**: 32-bit value with event-specific information
- **Event Duration**: 16-bit unsigned integer representing duration in seconds
- **Reserved**: Reserved for future use

#### Event Types

| Event ID | Description |
|----------|-------------|
| 0x01 | Power On/Reset |
| 0x02 | Power Off |
| 0x03 | Motion Start |
| 0x04 | Motion Stop |
| 0x05 | Geofence Enter |
| 0x06 | Geofence Exit |
| 0x07 | Overspeed |
| 0x08 | Impact/Crash Detection |
| 0x09 | Harsh Acceleration |
| 0x0A | Harsh Braking |
| 0x0B | Harsh Cornering |
| 0x0C | Tow Detection |
| 0x0D | External Power Connected |
| 0x0E | External Power Disconnected |
| 0x0F | Low Battery |
| 0x10 | SOS Button Pressed |
| 0x11 | Tampering Detected |

### Checksum Calculation

The checksum is calculated using the CRC-16 (CCITT) algorithm with polynomial 0x1021, initial value 0xFFFF. The checksum is calculated over all bytes between the start and end markers, excluding the markers themselves and the checksum field.

Pseudo-code for the checksum calculation:

```
function calculateCRC16(data, length):
    crc = 0xFFFF
    for i = 0 to length-1:
        crc ^= (data[i] << 8)
        for j = 0 to 7:
            if (crc & 0x8000) != 0:
                crc = ((crc << 1) ^ 0x1021)
            else:
                crc = (crc << 1)
    return crc & 0xFFFF
```

### Example Messages

#### Authentication Message (Type 0x01)

```
7E 00 0F 01 01 23 45 67 60 0A BC DE 01 02 03 04 05 06 07 08 12 34 7E
```

Breakdown:
- Start Marker: `7E`
- Message Length: `00 0F` (15 bytes)
- Message Type: `01` (Authentication)
- Device ID: `01 23 45 67` (19,088,743)
- Timestamp: `60 0A BC DE` (1,611,308,254 - Jan 22, 2021)
- Authentication Token: `01 02 03 04 05 06 07 08` (example token)
- Checksum: `12 34`
- End Marker: `7E`

#### Standard Position Report (Type 0x10)

```
7E 00 15 10 01 23 45 67 60 0A BC DE 02 7A C1 F8 06 8B 1A 00 00 32 01 5A 00 0A AB CD 7E
```

Breakdown:
- Start Marker: `7E`
- Message Length: `00 15` (21 bytes)
- Message Type: `10` (Standard Position Report)
- Device ID: `01 23 45 67` (19,088,743)
- Timestamp: `60 0A BC DE` (1,611,308,254 - Jan 22, 2021)
- Latitude: `02 7A C1 F8` (41,500,152 → 41.500152°)
- Longitude: `06 8B 1A 00` (109,240,832 → 109.240832°)
- Speed: `00 32` (50 → 5.0 km/h)
- Heading: `01 5A` (346°)
- Altitude: `00 0A` (10 meters)
- Satellites: `0A` (10 satellites)
- Status Flags: `AB CD` (example status)
- Checksum: `AB CD`
- End Marker: `7E`

## Traccar Implementation

### AstraProtocol Class

The `AstraProtocol` class extends the `BaseProtocol` class and is responsible for initializing the protocol handler and defining the connection parameters. It registers the protocol decoder and configures the appropriate pipeline for handling Astra protocol messages.

```java
public class AstraProtocol extends BaseProtocol {

    public AstraProtocol() {
        super("astra");
        addServer(new TrackerServer(false, getName()) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline) {
                pipeline.addLast(new AstraProtocolDecoder(AstraProtocol.this));
            }
        });
    }
}
```

Key aspects of the implementation:

1. The protocol is registered with the name "astra"
2. A TCP server is configured to handle incoming connections
3. The pipeline is set up with the `AstraProtocolDecoder` for message processing

### AstraProtocolDecoder Class

The `AstraProtocolDecoder` class extends `BaseProtocolDecoder` and is responsible for parsing and decoding messages from Astra devices. It implements the `decode` method to process incoming messages and convert them into Traccar's internal position objects.

The decoder handles the following tasks:

1. Message validation and integrity checking
2. Parsing of different message types
3. Extraction of position and attribute data
4. Conversion to Traccar's standardized position format

```java
public class AstraProtocolDecoder extends BaseProtocolDecoder {

    public AstraProtocolDecoder(Protocol protocol) {
        super(protocol);
    }

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {
        
        ByteBuf buf = (ByteBuf) msg;
        
        // Verify start marker
        if (buf.readByte() != 0x7E) {
            return null;
        }
        
        // Read message length
        int length = buf.readUnsignedShort();
        
        // Read message type
        int type = buf.readUnsignedByte();
        
        // Read device ID and get device
        long deviceId = buf.readUnsignedInt();
        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, String.valueOf(deviceId));
        if (deviceSession == null) {
            return null;
        }
        
        // Process different message types
        switch (type) {
            case 0x01: // Authentication
                if (channel != null) {
                    ByteBuf response = Unpooled.buffer();
                    response.writeByte(0x7E);
                    response.writeShort(5); // Length
                    response.writeByte(0x02); // Authentication response
                    response.writeInt((int) deviceId);
                    response.writeByte(0x01); // Success
                    response.writeShort(calculateChecksum(response, 1, response.writerIndex()));
                    response.writeByte(0x7E);
                    channel.writeAndFlush(new NetworkMessage(response, remoteAddress));
                }
                return null;
                
            case 0x10: // Standard position
            case 0x11: // Extended position
            case 0x12: // Alarm/Event
                return decodePosition(deviceSession, buf, type);
                
            default:
                return null;
        }
    }
    
    private Position decodePosition(DeviceSession deviceSession, ByteBuf buf, int type) {
        Position position = new Position(getProtocolName());
        position.setDeviceId(deviceSession.getDeviceId());
        
        // Read timestamp
        position.setTime(new Date(buf.readUnsignedInt() * 1000L));
        
        // Read coordinates
        position.setLatitude(buf.readInt() / 1000000.0);
        position.setLongitude(buf.readInt() / 1000000.0);
        
        // Read other position data
        position.setSpeed(buf.readUnsignedShort() / 10.0);
        position.setCourse(buf.readUnsignedShort());
        position.setAltitude(buf.readShort());
        
        position.set(Position.KEY_SATELLITES, buf.readUnsignedByte());
        position.set(Position.KEY_STATUS, buf.readUnsignedShort());
        
        // Process extended data for types 0x11 and 0x12
        if (type == 0x11) { // Extended position
            position.set(Position.KEY_BATTERY_LEVEL, buf.readUnsignedByte());
            position.set(Position.KEY_POWER, buf.readUnsignedShort() / 1000.0);
            position.set(Position.KEY_DEVICE_TEMP, buf.readShort() / 10.0);
            
            // Read accelerometer data
            position.set(Position.KEY_ACCELERATION_X, buf.readShort() / 1000.0);
            position.set(Position.KEY_ACCELERATION_Y, buf.readShort() / 1000.0);
            position.set(Position.KEY_ACCELERATION_Z, buf.readShort() / 1000.0);
            
            position.set(Position.KEY_INPUT, buf.readUnsignedByte());
            position.set(Position.KEY_OUTPUT, buf.readUnsignedByte());
            
            // Read analog inputs if available
            if (buf.readableBytes() >= 4) {
                position.set(Position.PREFIX_ADC + 1, buf.readUnsignedShort());
                position.set(Position.PREFIX_ADC + 2, buf.readUnsignedShort());
            }
            
            // Read odometer if available
            if (buf.readableBytes() >= 4) {
                position.set(Position.KEY_ODOMETER, buf.readUnsignedInt());
            }
        } else if (type == 0x12) { // Alarm/Event
            int eventType = buf.readUnsignedByte();
            position.set(Position.KEY_EVENT, eventType);
            position.set(Position.KEY_ALARM, getAlarmType(eventType));
            position.set("eventData", buf.readUnsignedInt());
            position.set("eventDuration", buf.readUnsignedShort());
        }
        
        return position;
    }
    
    private String getAlarmType(int eventType) {
        switch (eventType) {
            case 0x08: return Position.ALARM_CRASH;
            case 0x09: return Position.ALARM_HARD_ACCELERATION;
            case 0x0A: return Position.ALARM_HARD_BRAKING;
            case 0x0B: return Position.ALARM_CORNERING;
            case 0x0C: return Position.ALARM_TOW;
            case 0x0F: return Position.ALARM_LOW_BATTERY;
            case 0x10: return Position.ALARM_SOS;
            case 0x11: return Position.ALARM_TAMPERING;
            default: return null;
        }
    }
    
    private int calculateChecksum(ByteBuf buf, int from, int to) {
        int crc = 0xFFFF;
        for (int i = from; i < to; i++) {
            crc ^= (buf.getByte(i) & 0xFF) << 8;
            for (int j = 0; j < 8; j++) {
                if ((crc & 0x8000) != 0) {
                    crc = ((crc << 1) ^ 0x1021) & 0xFFFF;
                } else {
                    crc = (crc << 1) & 0xFFFF;
                }
            }
        }
        return crc;
    }
}
```

### Special Considerations

1. **Message Validation**: The decoder verifies the start marker and calculates the checksum to ensure message integrity.

2. **Device Authentication**: When an authentication message is received, the decoder responds with an authentication acknowledgment.

3. **Position Decoding**: The decoder extracts position information and converts it to the Traccar position format, handling different message types appropriately.

4. **Alarm Mapping**: Event types from the Astra protocol are mapped to standardized Traccar alarm types for consistent handling across different protocols.

5. **Extended Data**: The decoder extracts additional telemetry data from extended position reports and alarm/event reports, storing them as position attributes.

## Configuration

### Server Configuration

To enable the Astra protocol in Traccar, add the following to the `traccar.xml` configuration file:

```xml
<entry key='astra.port'>5013</entry>
```

Additional configuration options:

```xml
<!-- Enable or disable the protocol -->
<entry key='astra.enable'>true</entry>

<!-- Connection timeout in milliseconds -->
<entry key='astra.timeout'>60000</entry>

<!-- Custom device identification attribute -->
<entry key='astra.deviceId'>imei</entry>
```

### Device Setup

To configure an Astra device to communicate with Traccar:

1. Set the server IP address or hostname in the device configuration
2. Configure the port to match the Traccar server configuration (default: 5013)
3. Set the device to use TCP communication mode
4. Configure the reporting interval as required by your application
5. If using authentication, ensure the device is configured with the correct authentication parameters

### Troubleshooting

#### Common Issues

1. **Connection Problems**:
   - Verify network connectivity between the device and server
   - Check firewall settings to ensure the configured port is open
   - Verify the device is configured with the correct server address and port

2. **Authentication Failures**:
   - Ensure the device ID is correctly registered in Traccar
   - Check that the authentication token is correctly configured

3. **Missing Position Data**:
   - Verify the device has a valid GPS fix
   - Check the reporting interval configuration
   - Ensure the device has sufficient power

4. **Incorrect Position Information**:
   - Verify the device has good satellite visibility
   - Check for GPS antenna issues
   - Ensure the device firmware is up to date

#### Debugging

Enable debug logging in Traccar to troubleshoot protocol-specific issues:

```xml
<entry key='logger.astra'>debug</entry>
```

This will provide detailed logs of all communication with Astra devices, including:

- Raw message data
- Parsing results
- Authentication attempts
- Position updates
- Error conditions

## References

- Astra Telematics Device Documentation
- Traccar Protocol Implementation Guide
- CRC-16 CCITT Standard (X.25)
- GPS NMEA Protocol Reference