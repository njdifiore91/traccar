# AutoTrack Protocol Documentation

## Overview

The AutoTrack protocol is a GPS tracking protocol developed by AutoTrack, a GPS services company based in Bulgaria. This protocol was integrated into the Traccar GPS tracking system in version 4.0 (released in 2018) to expand the platform's compatibility with a wider range of tracking devices.

AutoTrack protocol uses a text-based format with comma-separated values, making it relatively straightforward to implement and debug. The protocol supports standard GPS tracking functionality including position reporting, status updates, and event notifications.

This document provides comprehensive information about the AutoTrack protocol specification, message formats, and implementation details within the Traccar system.

## Protocol Specification

### General Information

- **Protocol Name**: AutoTrack
- **Origin**: Bulgaria (AutoTrack GPS services company)
- **Default Port**: 5013 (TCP)
- **Type**: Text-based protocol with comma-separated values
- **Added to Traccar**: Version 4.0 (2018)

### Message Format

The AutoTrack protocol uses a structured text-based message format for communication between tracking devices and the server. Each message begins with a '#' character followed by the device identifier, with subsequent data fields separated by commas.

Messages typically include:

- Device identification information (IMEI or custom ID)
- Timestamp in YYYYMMDDHHMMSS format (UTC)
- GPS position data (latitude, longitude, altitude)
- Speed in km/h
- Direction/course in degrees
- Number of GPS satellites used for the fix
- Event code indicating the reason for the message
- Digital input/output status
- Battery level percentage
- Optional additional sensor readings (depending on device capabilities)

### Message Structure

AutoTrack messages follow a text-based format with fields separated by commas. The general structure is:

```
#<Device ID>,<Data Fields>
```

Where:
- **#**: The message prefix that marks the beginning of an AutoTrack message
- **Device ID**: Unique identifier for the tracking device (IMEI or custom ID)
- **Data Fields**: Comma-separated values containing position and status information

The protocol supports various message types, including:

1. **Position Update**: Regular GPS position reports
2. **Status Update**: Device status information
3. **Alert Message**: Emergency or event-triggered messages

## Traccar Implementation

### Classes

The AutoTrack protocol is implemented in Traccar through the following main classes:

1. **AutoTrackProtocol.java**: Extends the BaseProtocol class and defines the protocol characteristics, including port configuration and pipeline setup. This class registers the protocol with the Traccar system and configures the communication channel.

```java
public class AutoTrackProtocol extends BaseProtocol {
    public AutoTrackProtocol() {
        super("autotrack");
    }

    @Inject
    public AutoTrackProtocol(Config config) {
        this();
        addServer(new TrackerServer(config, getName(), false) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline) {
                pipeline.addLast(new LineBasedFrameDecoder(1024));
                pipeline.addLast(new StringDecoder());
                pipeline.addLast(new StringEncoder());
                pipeline.addLast(new AutoTrackProtocolDecoder(AutoTrackProtocol.this));
            }
        });
    }
}
```

2. **AutoTrackProtocolDecoder.java**: Extends the BaseProtocolDecoder class and handles the decoding of messages received from AutoTrack devices. This class contains the pattern matching logic and field extraction for AutoTrack messages.

```java
public class AutoTrackProtocolDecoder extends BaseProtocolDecoder {
    // Pattern for matching and extracting fields from AutoTrack messages
    private static final Pattern PATTERN = Pattern.compile(
            "#([^,]+)," +                  // Device ID
            "(\d{14})," +                 // Timestamp (YYYYMMDDHHMMSS)
            "(-?\d+\.\d+)," +             // Latitude
            "(-?\d+\.\d+)," +             // Longitude
            "(\d+\.?\d*)," +              // Speed
            "(\d+\.?\d*)," +              // Course
            "(\d+\.?\d*)," +              // Altitude
            "(\d+)," +                    // Satellites
            "(\d+)," +                    // Event Code
            "(\d+)," +                    // Input Status
            "(\d+)," +                    // Output Status
            "(\d+)" +                     // Battery Level
            ".*");

    public AutoTrackProtocolDecoder(AutoTrackProtocol protocol) {
        super(protocol);
    }

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {
        
        String sentence = (String) msg;
        
        // Parse message using the defined pattern
        Parser parser = new Parser(PATTERN, sentence);
        if (!parser.matches()) {
            return null;
        }
        
        // Extract device identifier and create session
        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, parser.next());
        if (deviceSession == null) {
            return null;
        }
        
        // Create position object and set protocol
        Position position = new Position(getProtocolName());
        position.setDeviceId(deviceSession.getDeviceId());
        
        // Parse timestamp
        DateBuilder dateBuilder = new DateBuilder()
                .setDate(parser.nextInt(0, 4), parser.nextInt(4, 6), parser.nextInt(6, 8))
                .setTime(parser.nextInt(8, 10), parser.nextInt(10, 12), parser.nextInt(12, 14));
        position.setTime(dateBuilder.getDate());
        
        // Set position coordinates
        position.setLatitude(parser.nextDouble());
        position.setLongitude(parser.nextDouble());
        position.setSpeed(parser.nextDouble());
        position.setCourse(parser.nextDouble());
        position.setAltitude(parser.nextDouble());
        
        // Set additional attributes
        position.set(Position.KEY_SATELLITES, parser.nextInt());
        position.set(Position.KEY_EVENT, parser.nextInt());
        position.set(Position.KEY_INPUT, parser.nextInt());
        position.set(Position.KEY_OUTPUT, parser.nextInt());
        position.set(Position.KEY_BATTERY_LEVEL, parser.nextInt());
        
        return position;
    }
}
```

### Decoding Process

The decoding process for AutoTrack messages in Traccar follows these general steps:

1. The message is received by the server on the configured port (default 5013)
2. The `AutoTrackProtocolDecoder` parses the message using a regular expression pattern to extract fields
3. The device identifier is extracted from the message (after the '#' prefix)
4. A device session is established using the identifier through the DeviceSession mechanism
5. Timestamp, position coordinates, and other data fields are parsed and validated
6. The data is converted to a standardized Position object with appropriate attribute mapping
7. Additional attributes (satellites, event code, I/O status, battery level) are added to the Position object
8. The Position object is returned for further processing by the Traccar system

### Configuration

To enable the AutoTrack protocol in Traccar, add the following line to the `traccar.xml` configuration file:

```xml
<entry key='autotrack.port'>5013</entry>
```

You can customize the port number as needed for your environment.

## Device Setup

To configure an AutoTrack device to communicate with a Traccar server:

1. Access the device configuration interface (typically through manufacturer software or SMS commands)
2. Set the server IP address or hostname in the device configuration
3. Configure the port to match the port specified in the Traccar configuration (default: 5013)
4. Ensure the device ID is properly configured (usually the device IMEI number)
5. Set the data upload interval according to your tracking requirements
6. Verify that the device is set to use the AutoTrack protocol format
7. Save the configuration and restart the device if necessary

Specific configuration commands may vary depending on the exact model of the AutoTrack device being used.

## Troubleshooting

### Common Issues

1. **Device Not Connecting**: 
   - Verify that the correct port (default 5013) is open on the server and not blocked by firewalls
   - Confirm that the device is configured with the correct server address and port
   - Check network connectivity between the device and server
   - Verify that the AutoTrack protocol is enabled in the Traccar configuration

2. **No Position Updates**: 
   - Check that the device has a valid GPS fix (sufficient satellites)
   - Verify that the message format matches the expected pattern
   - Ensure the device ID in the message matches the device ID registered in Traccar
   - Check the device's data transmission interval settings

3. **Incorrect Data**: 
   - Ensure that the device is properly configured and using the correct protocol format
   - Verify timestamp settings and potential timezone issues
   - Check for any message format variations specific to the device model
   - Validate coordinate format and conversion in the protocol decoder

4. **Parser Errors**:
   - Review debug logs for parsing exceptions
   - Verify that the message structure matches the expected pattern
   - Check for any special characters or formatting issues in the messages

### Debugging

To enable debug logging for the AutoTrack protocol, add the following line to the `traccar.xml` configuration file:

```xml
<entry key='logger.autotrack'>debug</entry>
```

This will provide detailed logs of the communication between the device and the server, which can be helpful for troubleshooting issues.

## Examples

### Sample Message

Below is an example of a typical AutoTrack position message format:

```
#<ID>,<Timestamp>,<Latitude>,<Longitude>,<Speed>,<Course>,<Altitude>,<Satellites>,<Event Code>,<Input Status>,<Output Status>,<Battery Level>
```

Example:
```
#123456789,20180510123456,42.123456,24.654321,60.5,270,850,8,1,1,0,95
```

### Decoded Position Data

When properly decoded, the sample message above would result in a Position object with the following information:

- Device ID: 123456789
- Timestamp: 2018-05-10 12:34:56 UTC
- Latitude: 42.123456°
- Longitude: 24.654321°
- Altitude: 850 meters
- Speed: 60.5 km/h
- Course: 270° (West)
- Satellites: 8
- Event Code: 1 (may represent a specific event type)
- Input Status: 1 (digital input active)
- Output Status: 0 (digital output inactive)
- Battery Level: 95%

These values are mapped to standard Traccar Position attributes, allowing for consistent handling across different protocols.

## References

- [Traccar Official Website](https://www.traccar.org/)
- [Traccar Protocol Implementation Guide](https://www.traccar.org/implement-protocol/)
- [Traccar Protocol Documentation](https://www.traccar.org/protocols/)
- [AutoTrack Protocol Implementation in Traccar](https://github.com/traccar/traccar/tree/master/src/main/java/org/traccar/protocol)
- [AutoTrack Protocol Test Cases](https://github.com/traccar/traccar/tree/master/src/test/java/org/traccar/protocol)
- [AutoTrack Company Website](https://www.autotrack.bg/) (GPS services company from Bulgaria)

## Support

For additional support with the AutoTrack protocol implementation in Traccar:

- Refer to the [Traccar Forums](https://www.traccar.org/forums/) for community assistance
- Check the [Traccar Documentation](https://www.traccar.org/documentation/) for general protocol handling information
- Review the source code in the [Traccar GitHub repository](https://github.com/traccar/traccar) for implementation details
- Contact the Traccar support team for professional assistance

When reporting issues with the AutoTrack protocol, always include debug logs, sample messages, and device information to facilitate troubleshooting.