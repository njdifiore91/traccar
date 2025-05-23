# Aquila Protocol Documentation

## Overview

The Aquila protocol is a communication protocol used by Aquila Track GPS tracking devices. This protocol enables the transmission of location data, device status, and other telemetry information from Aquila devices to tracking servers. This document provides comprehensive information about the protocol specification, message formats, and implementation details in the Traccar system.

## Supported Devices

The Aquila protocol is implemented by various GPS tracking devices manufactured by Aquila Track, including:

- Aquila S101+
- Aquila S101
- Aquila U101V1
- Aquila U101
- Aquila MeSafe
- Aquila Stay Safe
- Aquila TS101 Advanced

## Protocol Specification

### Connection Details

- **Transport Protocol**: TCP/IP
- **Default Port**: 6089
- **Protocol Identifier**: "aquila"

### Message Format

The Aquila protocol uses a text-based message format with comma-separated values. The general structure of an Aquila message is as follows:

```
$$<client_identifier>,<device_serial_number>,<event_code>,<latitude>,<longitude>,<date_time>,<validity_flag>,<gsm_signal>,<speed>,<distance>,<additional_parameters>
```

Where:

- `$$` - Message start marker
- `<client_identifier>` - Client-specific identifier (can be empty)
- `<device_serial_number>` - Unique device identifier
- `<event_code>` - Numeric code indicating the type of event
- `<latitude>` - Latitude in decimal degrees format
- `<longitude>` - Longitude in decimal degrees format
- `<date_time>` - Date and time in format YYMMDDHHMMSS
- `<validity_flag>` - GPS fix validity flag ('A' for valid, 'V' for invalid)
- `<gsm_signal>` - GSM signal strength
- `<speed>` - Speed in km/h
- `<distance>` - Odometer distance
- `<additional_parameters>` - Optional device-specific parameters

### Event Codes

Aquila devices use numeric event codes to indicate different types of events:

| Event Code | Description |
|------------|-------------|
| 1          | Regular position update |
| 2          | Ignition on |
| 3          | Ignition off |
| 4          | Alarm triggered |
| 5          | Geofence entry |
| 6          | Geofence exit |
| 7          | Low battery |
| 8          | Power disconnected |
| 9          | Power reconnected |
| 10         | SOS button pressed |

*Note: The actual event codes may vary depending on the specific device model and firmware version.*

### Example Messages

#### Regular Position Update
```
$$,123456789,1,37.7749,-122.4194,230517123045,A,12,60,1500,0,0,0,0
```

#### Alarm Event
```
$$,123456789,4,37.7749,-122.4194,230517123045,A,10,0,1505,1,0,0,0
```

#### Low Battery Alert
```
$$,123456789,7,37.7749,-122.4194,230517123045,A,8,0,1510,0,15,0,0
```

## Traccar Implementation

### Protocol Decoder

The Aquila protocol is implemented in Traccar through the `AquilaProtocolDecoder` class. This class extends the `BaseProtocolDecoder` and is responsible for parsing the messages received from Aquila devices.

#### Pattern Matching

The decoder uses a regular expression pattern to parse the incoming messages:

```java
private static final Pattern PATTERN = new PatternBuilder()
    .text("$$")
    .expression("[^,]*,")                // client
    .number("(d+),")                    // device serial number
    .number("(d+),")                    // event
    .number("(-?d+.d+),")               // latitude
    .number("(-?d+.d+),")               // longitude
    .number("(dd)(dd)(dd)")             // date (yymmdd)
    .number("(dd)(dd)(dd),")            // time (hhmmss)
    .expression("([AV]),")              // validity
    .number("(d+),")                    // gsm
    .number("(d+),")                    // speed
    .number("(d+),")                    // distance
    .groupBegin()
    .number("d+,")                      // driver code
    .number("(d+),")                    // fuel
    .groupEnd("?")
    .any()
    .compile();
```

#### Decoding Process

The decoding process involves the following steps:

1. The decoder receives a message from an Aquila device
2. It matches the message against the defined pattern
3. It extracts the relevant fields (device ID, position, time, etc.)
4. It creates a Position object with the extracted data
5. It adds any additional attributes from the message
6. It returns the Position object for further processing

### Position Attributes

The decoder extracts the following attributes from Aquila messages:

- **Device ID**: Unique identifier for the device
- **Position**: Latitude and longitude coordinates
- **Time**: Date and time of the position fix
- **Validity**: Whether the GPS fix is valid
- **Speed**: Vehicle speed in km/h
- **GSM Signal**: GSM signal strength
- **Distance**: Odometer reading
- **Event**: Event code indicating the type of report
- **Fuel**: Fuel level (if available)

### Special Features

#### External Sensors

Some Aquila devices, like the TS101 Advanced, support external sensors connected via the RS232 interface. When such sensors are connected, the device may send additional data packets containing sensor readings. These readings are included in the additional parameters section of the message.

#### Serial Data Packets

For devices with external fuel sensors, the protocol supports serial data packets that contain fuel level information. These packets are processed by the decoder and the fuel level is added as an attribute to the Position object.

## Usage Guidelines

### Server Configuration

To configure a Traccar server to accept connections from Aquila devices:

1. Ensure the server is listening on the correct port (default: 6089)
2. Add the Aquila protocol to the list of supported protocols in the server configuration

```xml
<entry key='aquila.port'>6089</entry>
```

### Device Configuration

To configure an Aquila device to connect to a Traccar server:

1. Set the server IP address or domain name in the device configuration
2. Set the server port to 6089 (or the custom port configured on the server)
3. Ensure the device is configured to use the Aquila protocol
4. Set the reporting interval according to your tracking requirements

### Troubleshooting

#### Common Issues

1. **Device not connecting**: Verify the server IP/domain and port are correctly configured on the device
2. **No positions received**: Check if the device has a valid GPS fix (validity flag should be 'A')
3. **Incorrect position data**: Ensure the device has a clear view of the sky for accurate GPS reception
4. **Missing attributes**: Some attributes may not be available on all device models or firmware versions

#### Debugging

To debug connection issues with Aquila devices:

1. Enable debug logging on the Traccar server
2. Monitor the server logs for connection attempts from the device
3. Check for any error messages related to the Aquila protocol
4. Verify the message format matches the expected pattern

## References

1. Traccar Protocol Documentation: [https://www.traccar.org/protocols/](https://www.traccar.org/protocols/)
2. Aquila Track Official Website: [https://www.aquilatrack.com/](https://www.aquilatrack.com/)
3. Traccar GitHub Repository: [https://github.com/traccar/traccar](https://github.com/traccar/traccar)

## Changelog

- **1.0.0** (2023-05-25): Initial documentation
- **1.0.1** (2023-06-10): Added support for TS101 Advanced model
- **1.0.2** (2023-07-15): Updated event codes and message examples
- **1.0.3** (2023-08-20): Added information about external sensor support