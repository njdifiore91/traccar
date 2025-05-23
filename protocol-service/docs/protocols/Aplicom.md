# Aplicom Protocol Documentation

## Overview

The Aplicom protocol is used by GPS tracking devices manufactured by Aplicom, a Finnish company specializing in telematics and IoT solutions. This protocol supports a wide range of telematics data including position tracking, event reporting, engine diagnostics, CAN bus data, and more. The protocol is designed for professional fleet management and vehicle tracking applications.

## Protocol Specifications

### Communication Method

- **Transport Protocol**: TCP
- **Default Port**: Configurable (typically 5011-5020 range)
- **Connection Mode**: Persistent connection with keep-alive messages

### Message Structure

Aplicom protocol messages follow this general structure:

1. **Protocol Identifier**: Single character indicating the message type ('D', 'E', 'F', or 'H')
2. **Version**: Byte containing version information and flags
3. **Unit ID/IMEI**: Device identifier (3-7 bytes depending on version)
4. **Length**: Message length (2 bytes)
5. **Selector**: Bit field indicating which data fields are present (3 bytes, optional)
6. **Event Code**: Event type identifier (1 byte)
7. **Event Info**: Additional event information (1 byte)
8. **Data Fields**: Variable data fields as specified by the selector

### Device Identification

Aplicom devices are identified by their IMEI number. The protocol supports several IMEI encoding methods:

- For newer devices, the IMEI is directly encoded in 7 bytes
- For older devices, the IMEI is derived from a 3-byte unit ID using one of several base values:
  - TC65 v2.0: Base IMEI 0x1437207000000
  - TC65 v2.8: Base IMEI 358244010000000
  - TC65i v1.1: Base IMEI 0x14143B4000000

The protocol includes a Luhn algorithm check to validate the derived IMEI.

## Message Types

The Aplicom protocol supports several message types, each identified by a single character:

### 'D' Messages

Standard position and status messages with the following data fields (controlled by selector bits):

- Device and fix time
- GPS coordinates and status
- Speed, course, and maximum speed
- Digital inputs and analog inputs
- Power and battery levels
- Trip meters
- Digital outputs
- Driver ID
- Altitude
- Event data
- CAN bus data (when enabled)

### 'E' Messages

Extended messages with tachograph and additional vehicle data:

- Tachograph events
- Driver states and working states
- Vehicle speed from OBD
- Odometer values
- Engine RPM
- Vehicle identification number (VIN)
- Driver card information

### 'F' Messages

Fuel and engine-related messages:

- Engine RPM and temperature
- Engine hours
- Throttle position
- Fuel level and consumption
- Vehicle speed
- Tachograph data
- Ambient temperature
- Axle weights

### 'H' Messages

Histogram data messages containing statistical information in matrix format.

## Data Selectors

The Aplicom protocol uses bit selectors to determine which data fields are included in a message. This allows for efficient bandwidth usage by only transmitting relevant data. Default selectors are defined for each message type:

- 'D' Messages: Default selector 0x0002fC
- 'E' Messages: Default selector 0x007ffc
- 'F' Messages: Default selector 0x0007fd

If the version byte has the 0x40 bit set, a custom 3-byte selector follows the length field.

## Special Data Types

### CAN Bus Data

The protocol supports detailed CAN bus data decoding, including:

- Engine parameters (RPM, temperature)
- Battery voltage
- Temperature readings from various sensors
- Alarm states
- Pressure readings
- Service indicators
- Software versions

### Event Data

Event data is encoded based on the event code. The protocol supports numerous event types, each with specific data formats. Event code 119 provides raw event data in hexadecimal format.

## Traccar Implementation

The Traccar implementation of the Aplicom protocol consists of three main components:

### AplicomFrameDecoder

Responsible for identifying and extracting complete Aplicom messages from the incoming data stream. Key features:

- Skips numeric "keep-alive" messages
- Determines message length based on version flags and header size
- Handles variable-length messages

### AplicomProtocol

Registers the protocol with the Traccar server and sets up the protocol pipeline with the frame decoder and protocol decoder.

### AplicomProtocolDecoder

Decodes Aplicom protocol messages into Traccar Position objects. Key features:

- IMEI validation and conversion
- Message type detection and appropriate decoding
- Selector-based field decoding
- Support for all message types ('D', 'E', 'F', 'H')
- Special handling for CAN bus data and event data

## Configuration

### Server Configuration

To enable the Aplicom protocol in Traccar, add the following to the configuration file:

```properties
protocol.aplicom.port = 5011
```

Optional parameters:

```properties
protocol.aplicom.can = true  # Enable CAN bus data decoding
```

### Device Configuration

Aplicom devices typically require the following configuration:

1. Server IP address and port
2. APN settings for the cellular network
3. Reporting interval
4. Event triggers
5. Keep-alive interval (typically 60 seconds)

## Protocol Examples

### Keep-Alive Message

Aplicom devices send numeric keep-alive messages to maintain the connection. These are simple numeric strings and are filtered out by the frame decoder.

### Position Message (Type 'D')

A typical 'D' message with GPS position, device status, and basic vehicle information.

### Extended Message (Type 'E')

An 'E' message containing tachograph data, driver information, and additional vehicle parameters.

### Fuel Message (Type 'F')

An 'F' message with detailed engine parameters, fuel consumption, and vehicle status information.

## Troubleshooting

### Common Issues

1. **Connection Problems**: Ensure the device is configured with the correct server address and port.
2. **Data Not Appearing**: Check that the appropriate selectors are enabled for the data you wish to receive.
3. **IMEI Not Recognized**: Verify the device IMEI is correctly registered in the Traccar system.
4. **Incomplete Messages**: If messages appear truncated, check for network issues or buffer size limitations.

### Debugging

Enable debug logging in Traccar to see detailed protocol information:

```properties
logger.org.traccar.protocol.AplicomProtocolDecoder.level = ALL
```

## References

1. Traccar Aplicom Protocol Implementation
   - AplicomFrameDecoder.java
   - AplicomProtocol.java
   - AplicomProtocolDecoder.java

2. Aplicom Company Information
   - Website: [www.aplicom.com](https://www.aplicom.com)
   - Detailed protocol specifications are available to Aplicom customers through their extranet