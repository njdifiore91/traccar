# Appello GPS Protocol Documentation

## Overview

The Appello protocol is a text-based communication protocol used by Appello GPS tracking devices. This protocol enables the transmission of location data, device status, and other telemetry information from Appello trackers to a tracking server. Appello devices are commonly used for vehicle, asset, and pet tracking applications, including the Appello 4P pet tracker.

## Protocol Specifications

### Connection Details

- **Transport Protocol**: TCP/IP
- **Default Port**: 6109
- **Character Encoding**: ASCII
- **Message Termination**: Line-based (messages end with CR+LF)

### Message Format

The Appello protocol uses a text-based format with messages terminated by a carriage return and line feed (CR+LF) sequence. Messages follow a structured format with fields separated by delimiters.

#### General Message Structure

Messages in the Appello protocol typically follow this structure:

```
<Header>,<Device ID>,<Message Type>,<Parameters>,...<Checksum>
```

Where:
- **Header**: Identifies the start of a message
- **Device ID**: Unique identifier for the device (IMEI or serial number)
- **Message Type**: Indicates the type of message (position update, status, alarm, etc.)
- **Parameters**: Various parameters depending on the message type
- **Checksum**: Optional validation value to ensure message integrity

## Message Types

### Login/Authentication Message

Sent by the device when establishing a connection with the server to authenticate itself.

```
LOGIN,<Device ID>,<Authentication Token>
```

### Position Update Message

Contains GPS location data and basic status information.

```
POS,<Device ID>,<Timestamp>,<Latitude>,<Longitude>,<Speed>,<Course>,<Altitude>,<Satellites>,<Status>
```

Where:
- **Timestamp**: UTC timestamp in format YYYYMMDDHHMMSS
- **Latitude**: Decimal degrees (positive for North, negative for South)
- **Longitude**: Decimal degrees (positive for East, negative for West)
- **Speed**: Speed in km/h
- **Course**: Direction in degrees (0-359, where 0 is North)
- **Altitude**: Altitude in meters
- **Satellites**: Number of satellites used for fix
- **Status**: Device status flags

#### Example Position Message

```
POS,123456789012345,20230615123456,37.7749,-122.4194,35.2,270,25,8,0
```

This example shows a device with ID 123456789012345 reporting its position at 12:34:56 UTC on June 15, 2023, at latitude 37.7749° N, longitude 122.4194° W, traveling at 35.2 km/h in a westerly direction (270°), at an altitude of 25 meters, with 8 satellites used for the fix, and a status value of 0 (normal operation).

### Status Message

Contains device status information without position data.

```
STATUS,<Device ID>,<Timestamp>,<Battery Level>,<External Power>,<GSM Signal>,<Status Flags>
```

#### Example Status Message

```
STATUS,123456789012345,20230615123456,85,1,4,0
```

This example shows a device reporting its status with 85% battery level, external power connected (1), GSM signal strength of 4 (out of 5), and no special status flags (0).

### Alarm Message

Sent when the device detects an alarm condition.

```
ALARM,<Device ID>,<Timestamp>,<Alarm Type>,<Latitude>,<Longitude>,<Additional Data>
```

Common alarm types include:
- **SOS**: Emergency button pressed
- **POWER**: External power disconnected
- **GEOFENCE**: Geofence boundary crossed
- **MOTION**: Unexpected movement detected
- **LOWBATT**: Low battery warning

#### Example Alarm Message

```
ALARM,123456789012345,20230615123456,SOS,37.7749,-122.4194,0
```

This example shows an SOS alarm triggered by the device, including its current location.

## Implementation in Traccar

The Appello protocol is implemented in Traccar through two main classes:

### AppelloProtocol.java

This class extends `BaseProtocol` and sets up the server infrastructure to handle Appello device connections. It configures the appropriate decoders and handlers for processing incoming messages.

```java
public class AppelloProtocol extends BaseProtocol {
    public AppelloProtocol() {
        super("appello");
    }

    @Override
    public void initTrackerServers(List<TrackerServer> serverList) {
        serverList.add(new TrackerServer(new ServerBootstrap(), getName()) {
            @Override
            protected void addSpecificHandlers(ChannelPipeline pipeline) {
                pipeline.addLast("frameDecoder", new LineBasedFrameDecoder(1024));
                pipeline.addLast("stringDecoder", new StringDecoder());
                pipeline.addLast("stringEncoder", new StringEncoder());
                pipeline.addLast("objectDecoder", new AppelloProtocolDecoder(AppelloProtocol.this));
            }
        });
    }
}
```

Key components:
- Uses `LineBasedFrameDecoder` to handle the line-based message format
- Configures `StringDecoder` and `StringEncoder` for text processing
- Registers the `AppelloProtocolDecoder` for message parsing

### AppelloProtocolDecoder.java

This class extends `BaseProtocolDecoder` and contains the logic for parsing and interpreting Appello protocol messages. It extracts relevant information from the messages and converts them into standardized Position objects that Traccar can process.

The decoder handles:
- Device identification and registration
- Position data extraction and validation
- Status information parsing
- Alarm condition detection

The decoder uses regular expression patterns to match different message formats and extract the relevant data fields. It then populates a Position object with the extracted information, which is passed to the Traccar core for further processing and storage.

## Configuration

### Server Configuration

To enable the Appello protocol in Traccar, ensure the following configuration is present in the `traccar.xml` configuration file:

```xml
<entry key='appello.port'>6109</entry>
```

### Device Configuration

Appello devices typically require the following configuration:

1. Server IP address or hostname
2. Server port (default: 6109)
3. APN settings for the cellular network
4. Reporting interval
5. Any device-specific settings (alarms, geofences, etc.)

Refer to the specific Appello device model's manual for detailed configuration instructions.

## Device-Specific Features

### Appello 4P Pet Tracker

The Appello 4P is a specialized GPS tracker designed for pets with the following features:

- Compact size suitable for medium and small-sized pets
- Long battery life with large-capacity battery
- Waterproof design for outdoor use
- Real-time tracking capabilities
- Geofence support for safe zone monitoring
- SOS button for emergency situations
- Low battery alerts

### Command Set

Appello devices support various commands that can be sent from the server to control device behavior:

```
CMD,<Device ID>,<Command Type>,<Parameters>
```

Common commands include:

- **INTERVAL**: Set position reporting interval (in seconds)
- **POWERMODE**: Configure power saving mode
- **GEOFENCE**: Set up geofence parameters
- **REBOOT**: Restart the device
- **FACTORY**: Reset to factory settings

## Troubleshooting

### Common Issues

1. **Connection Problems**:
   - Verify the server IP and port are correctly configured on the device
   - Check that the server firewall allows connections on the configured port
   - Ensure the device has an active cellular data connection

2. **Authentication Failures**:
   - Verify the device ID is correctly entered in the Traccar device list
   - Check that the device is sending the correct authentication information

3. **Incorrect Position Data**:
   - Ensure the device has a clear view of the sky for GPS reception
   - Check that the device has acquired a valid GPS fix
   - Verify the device's time settings are correct

### Debugging

To enable debug logging for the Appello protocol in Traccar, add the following to the `traccar.xml` configuration file:

```xml
<entry key='logger.appello'>DEBUG</entry>
```

This will provide detailed logs of all communication with Appello devices, which can be helpful for troubleshooting.

## Integration with Traccar Microservices

In the microservices architecture, the Appello protocol handling is implemented in the Protocol Service component. This service is responsible for:

1. Accepting connections from Appello devices on port 6109
2. Decoding the messages according to the Appello protocol specification
3. Converting the decoded data into standardized message formats
4. Publishing the processed data to the message broker for consumption by other services

The Protocol Service uses the following components for Appello protocol support:

- **Connection Management**: Netty-based TCP server that listens on the configured port
- **Message Decoding**: AppelloProtocolDecoder for parsing the text-based messages
- **Device Authentication**: Verification of device identifiers against the registered devices
- **Message Publishing**: Conversion of protocol-specific messages to standardized message formats for the message broker

## References

- Traccar Official Documentation: [https://www.traccar.org/](https://www.traccar.org/)
- Traccar Supported Protocols: [https://www.traccar.org/protocols/](https://www.traccar.org/protocols/)
- Appello Device Manuals (refer to specific device model documentation)
- GPSWOX Supported Devices: [https://www.gpswox.com/en/supported-gps-trackers](https://www.gpswox.com/en/supported-gps-trackers)