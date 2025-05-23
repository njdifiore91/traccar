# AustinNb Protocol Documentation

## Overview

The AustinNb protocol is a text-based GPS tracking protocol used for transmitting location and status information from GPS tracking devices to the Traccar server. It operates primarily over UDP connections (with TCP support) and uses a simple string-based message format with semicolon-delimited fields.

## Protocol Specifications

### Transport Layer

- **Protocol**: TCP/UDP (primarily UDP based on community reports)
- **Default Port**: 5158

### Message Format

The AustinNb protocol uses a text-based message format with fields separated by semicolons (`;`). The general format is as follows:

```
<IMEI>;<DATE> <TIME>;<LATITUDE>;<LONGITUDE>;<AZIMUTH>;<ANGLE>;<RANGE>;<OUT_OF_RANGE>;<OPERATOR>
```

Example:
```
123456789012345;2022-08-23 10:15:30;-33,12345;-70,98765;180;45;100;0;Operator Name
```

### Field Descriptions

| Field | Description | Format | Example |
|-------|-------------|--------|--------|
| IMEI | Device unique identifier | Numeric string | `123456789012345` |
| DATE | Date of the position | YYYY-MM-DD | `2022-08-23` |
| TIME | Time of the position | HH:MM:SS | `10:15:30` |
| LATITUDE | Latitude coordinate | Decimal degrees with comma as separator | `-33,12345` |
| LONGITUDE | Longitude coordinate | Decimal degrees with comma as separator | `-70,98765` |
| AZIMUTH | Direction of movement in degrees | 0-359 | `180` |
| ANGLE | Angle value | Degrees | `45` |
| RANGE | Range value | Numeric | `100` |
| OUT_OF_RANGE | Out of range indicator | 0 = in range, 1 = out of range | `0` |
| OPERATOR | Cellular network operator or carrier name | Text | `Operator Name` |

## Traccar Implementation

The AustinNb protocol is implemented in Traccar through two main classes:

### AustinNbProtocol

This class extends `BaseProtocol` and sets up the server configuration for handling AustinNb protocol messages. It configures the pipeline with string encoding/decoding and adds the protocol decoder.

```java
public class AustinNbProtocol extends BaseProtocol {

    @Inject
    public AustinNbProtocol(Config config) {
        addServer(new TrackerServer(config, getName(), true) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                pipeline.addLast(new StringEncoder());
                pipeline.addLast(new StringDecoder());
                pipeline.addLast(new AustinNbProtocolDecoder(AustinNbProtocol.this));
            }
        });
    }
}
```

### AustinNbProtocolDecoder

This class extends `BaseProtocolDecoder` and is responsible for parsing the incoming messages according to the AustinNb protocol format. It uses a regular expression pattern to extract the fields from the message string and creates a Position object with the extracted data.

The decoder handles the following tasks:

1. Parsing the message using a regular expression pattern
2. Extracting device identification (IMEI)
3. Creating a device session
4. Parsing date and time information
5. Extracting position coordinates (latitude, longitude)
6. Setting additional attributes (azimuth, angle, range, out of range, carrier)

## Configuration

To enable the AustinNb protocol in Traccar, add the following to your `traccar.xml` configuration file:

```xml
<entry key='austinnb.port'>5158</entry>
```

For UDP support, use:

```xml
<entry key='austinnb.port'>5158</entry>
<entry key='austinnb.protocol'>udp</entry>
```

You can customize the port number as needed for your environment.

## Device Setup

To configure a device to work with the AustinNb protocol:

1. Set the device to connect to your Traccar server's IP address or hostname
2. Configure the device to use the port specified in your Traccar configuration (default: 5158)
3. Set the correct protocol (TCP or UDP) on the device to match your server configuration
4. Ensure the device is sending data in the correct format as described in the Message Format section

## Troubleshooting

### Common Issues

1. **No data received from device**:
   - Verify the device is configured with the correct server address and port
   - Check if the port is open in your firewall
   - Ensure the device is sending data in the correct format
   - Verify that the correct protocol (TCP or UDP) is configured in both the device and server

2. **Device shows connected but no positions**:
   - Verify the IMEI is correctly registered in Traccar
   - Check if the device has a valid GPS fix
   - Ensure the message format matches the expected pattern

3. **Incorrect position data**:
   - Verify the device is sending coordinates with comma as decimal separator
   - Check if the latitude and longitude values are in the correct format

### Debugging

To enable debug logging for the AustinNb protocol, add the following to your `traccar.xml` configuration file:

```xml
<entry key='logger.org.traccar.protocol.AustinNbProtocol'>DEBUG</entry>
<entry key='logger.org.traccar.protocol.AustinNbProtocolDecoder'>DEBUG</entry>
```

## References

- [Traccar Protocol Documentation](https://www.traccar.org/protocols/)
- [Traccar Source Code Repository](https://github.com/traccar/traccar)