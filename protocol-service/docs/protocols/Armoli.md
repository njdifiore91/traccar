# Armoli Protocol Documentation

## 1. Overview

The Armoli protocol is a binary communication protocol used by Armoli GPS tracking devices manufactured by Armoli Technology A.Ş., a company established in 2014 and headquartered in Gebze, Turkey. Armoli specializes in designing and producing state-of-the-art GPS tracking solutions for various sectors, particularly the automotive industry.

This protocol enables bidirectional communication between Armoli tracking devices and the Traccar server, allowing for real-time position tracking, telemetry data collection, and remote device management.

## 2. Protocol Specification

### 2.1 Connection

Armoli devices connect to the server using TCP/IP. The default port for Armoli devices in the Traccar server is 5150. The connection is persistent, with devices maintaining a continuous connection to the server and sending periodic heartbeat messages to keep the connection alive.

### 2.2 Message Format

Armoli protocol messages follow a binary format with the following structure:

```
+----------------+----------------+----------------+----------------+
| Header (2 bytes)| Length (2 bytes)| Message ID (1B) | Payload (var) |
+----------------+----------------+----------------+----------------+
|    Checksum    |    End Marker  |
+----------------+----------------+
```

#### 2.2.1 Header

All Armoli messages start with a fixed 2-byte header: `0x2424` (ASCII "$$").

#### 2.2.2 Length

A 2-byte field indicating the total length of the message in bytes, including the header, length field, and end marker.

#### 2.2.3 Message ID

A 1-byte field identifying the type of message. Common message IDs include:

| ID (hex) | Description |
|----------|-------------|
| 0x01     | Login/Authentication |
| 0x10     | Position Report |
| 0x13     | Status Report |
| 0x16     | Alarm Report |
| 0x80     | Command Response |
| 0x90     | Heartbeat |

#### 2.2.4 Payload

Variable-length field containing the message data. The structure of the payload depends on the message type as identified by the Message ID.

#### 2.2.5 Checksum

A 2-byte field containing the XOR checksum of all bytes from the header to the end of the payload.

#### 2.2.6 End Marker

All Armoli messages end with a fixed 2-byte marker: `0x0D0A` (ASCII CR+LF).

### 2.3 Authentication

When an Armoli device connects to the server, it sends an authentication message (ID 0x01) containing the device's unique identifier (IMEI). The server validates this identifier against its database and responds with an acknowledgment message.

Authentication message payload format:

```
+----------------+----------------+----------------+
| IMEI (15 bytes) | Device Type(1B)| Protocol Ver(1B)|
+----------------+----------------+----------------+
```

### 2.4 Position Report

Position reports (ID 0x10) contain GPS location data and basic telemetry information. The payload format is as follows:

```
+----------------+----------------+----------------+----------------+
| Timestamp (6B) | Latitude (4B)  | Longitude (4B) | Speed (1B)     |
+----------------+----------------+----------------+----------------+
| Course (2B)    | Satellites (1B)| HDOP (1B)      | Status (2B)    |
+----------------+----------------+----------------+----------------+
| IO Status (var)| Mileage (4B)   |
+----------------+----------------+
```

- **Timestamp**: UTC time in format YYMMDDHHMMSS (BCD encoded)
- **Latitude**: Signed 32-bit integer, in degrees * 1,000,000
- **Longitude**: Signed 32-bit integer, in degrees * 1,000,000
- **Speed**: Unsigned 8-bit integer, in knots
- **Course**: Unsigned 16-bit integer, in degrees (0-359)
- **Satellites**: Number of satellites used for positioning
- **HDOP**: Horizontal dilution of precision * 10
- **Status**: Device status flags
- **IO Status**: Variable-length field containing digital/analog input/output states
- **Mileage**: Unsigned 32-bit integer, in meters

### 2.5 Status Report

Status reports (ID 0x13) provide information about the device's current state, including battery level, external power, and signal strength.

### 2.6 Alarm Report

Alarm reports (ID 0x16) are sent when the device detects specific events such as:

- Power disconnection
- Low battery
- SOS button press
- Geofence entry/exit
- Excessive speed
- Unauthorized movement (towing)

### 2.7 Command Response

Command responses (ID 0x80) are sent by the device in response to commands received from the server.

### 2.8 Heartbeat

Heartbeat messages (ID 0x90) are sent periodically to maintain the connection with the server. These messages typically contain minimal information, often just the device identifier.

## 3. Traccar Implementation

The Armoli protocol is implemented in Traccar through three main classes:

### 3.1 ArmoliProtocol

This class extends `BaseProtocol` and serves as the entry point for the protocol implementation. It registers the protocol with the server and defines the supported message types and configuration parameters.

```java
public class ArmoliProtocol extends BaseProtocol {

    public ArmoliProtocol() {
        super("armoli");
        addServer(new TraccarServer(this, false) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline) {
                pipeline.addLast(new ArmoliFrameDecoder());
                pipeline.addLast(new ArmoliProtocolDecoder(ArmoliProtocol.this));
            }
        });
        addServer(new TraccarServer(this, true) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline) {
                pipeline.addLast(new ArmoliFrameDecoder());
                pipeline.addLast(new ArmoliProtocolDecoder(ArmoliProtocol.this));
                pipeline.addLast(new ArmoliProtocolEncoder(ArmoliProtocol.this));
            }
        });
    }
}
```

### 3.2 ArmoliProtocolDecoder

This class extends `BaseProtocolDecoder` and is responsible for decoding incoming messages from Armoli devices. It parses the binary data according to the protocol specification and converts it into Traccar's internal position and event objects.

```java
public class ArmoliProtocolDecoder extends BaseProtocolDecoder {

    public ArmoliProtocolDecoder(ArmoliProtocol protocol) {
        super(protocol);
    }

    @Override
    protected Object decode(Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {
        ByteBuf buf = (ByteBuf) msg;
        
        // Skip header bytes (0x2424)
        buf.skipBytes(2);
        
        // Read message length
        int length = buf.readUnsignedShort();
        
        // Read message type
        int type = buf.readUnsignedByte();
        
        // Process message based on type
        switch (type) {
            case 0x01: // Authentication
                return decodeAuthentication(channel, remoteAddress, buf);
            case 0x10: // Position
                return decodePosition(channel, remoteAddress, buf);
            case 0x13: // Status
                return decodeStatus(channel, remoteAddress, buf);
            case 0x16: // Alarm
                return decodeAlarm(channel, remoteAddress, buf);
            case 0x80: // Command Response
                return decodeCommandResponse(channel, remoteAddress, buf);
            case 0x90: // Heartbeat
                return decodeHeartbeat(channel, remoteAddress, buf);
            default:
                return null;
        }
    }
    
    // Implementation of specific message decoders...
}
```

### 3.3 ArmoliProtocolPoller

This class implements the `DevicePoller` interface and is responsible for sending periodic polling commands to Armoli devices to request updates or maintain the connection.

```java
public class ArmoliProtocolPoller implements DevicePoller {

    @Override
    public void sendCommands(DeviceSession deviceSession, Collection<Command> commands) {
        for (Command command : commands) {
            // Implement command sending logic
        }
    }

    @Override
    public void sendPositionRequest(DeviceSession deviceSession) {
        // Implement position request logic
    }
}
```

## 4. Configuration Parameters

The Armoli protocol implementation in Traccar supports the following configuration parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| armoli.port | 5150 | TCP port for Armoli devices |
| armoli.heartbeat | 60 | Heartbeat interval in seconds |
| armoli.ack | true | Enable/disable server acknowledgments |
| armoli.extended | false | Enable extended data parsing |

These parameters can be configured in the Traccar configuration file (`traccar.xml` or `conf/traccar.xml`).

## 5. Example Messages

### 5.1 Authentication Message

```
24 24 00 1F 01 35 36 37 38 39 30 31 32 33 34 35 36 37 38 39 01 01 7E 0D 0A
```

Decoded:
- Header: `0x2424`
- Length: `0x001F` (31 bytes)
- Type: `0x01` (Authentication)
- IMEI: "567890123456789"
- Device Type: `0x01`
- Protocol Version: `0x01`
- Checksum: `0x7E`
- End Marker: `0x0D0A`

### 5.2 Position Report

```
24 24 00 2A 10 23 05 15 10 30 45 05 9D 5B 38 0A 6C 1D 12 25 06 14 00 03 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 5A 0D 0A
```

Decoded:
- Header: `0x2424`
- Length: `0x002A` (42 bytes)
- Type: `0x10` (Position)
- Timestamp: 2023-05-15 10:30:45
- Latitude: 37.123456 degrees
- Longitude: 127.123456 degrees
- Speed: 25 knots
- Course: 70 degrees
- Satellites: 6
- HDOP: 1.4
- Status: 0x0003 (Ignition on, GPS fixed)
- IO Status: All zeros (no active inputs/outputs)
- Mileage: 0 meters
- Checksum: `0x5A`
- End Marker: `0x0D0A`

## 6. Troubleshooting

### 6.1 Common Issues

1. **Connection Problems**
   - Verify that the device is configured with the correct server IP and port
   - Check firewall settings to ensure the port is open
   - Verify that the device has an active internet connection

2. **Authentication Failures**
   - Ensure the device IMEI is correctly registered in the Traccar server
   - Check for any special characters or formatting issues in the IMEI

3. **Missing Position Data**
   - Verify that the device has a clear view of the sky for GPS reception
   - Check the device's GPS antenna connection
   - Ensure the device is configured to send position updates at the expected frequency

### 6.2 Debugging

To enable debug logging for the Armoli protocol in Traccar, add the following to the `conf/default.xml` file:

```xml
<entry key="logger.org.traccar.protocol.ArmoliProtocol">DEBUG</entry>
<entry key="logger.org.traccar.protocol.ArmoliProtocolDecoder">DEBUG</entry>
<entry key="logger.org.traccar.protocol.ArmoliProtocolEncoder">DEBUG</entry>
```

## 7. References

1. Armoli Technology A.Ş. - Official Website: [https://www.armoli.com](https://www.armoli.com)
2. Traccar - Open Source GPS Tracking System: [https://www.traccar.org](https://www.traccar.org)
3. NMEA 0183 Standard for GPS Communication: [https://gpsd.gitlab.io/gpsd/NMEA.html](https://gpsd.gitlab.io/gpsd/NMEA.html)