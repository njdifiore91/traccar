# AIS Protocol Documentation

*This document provides comprehensive information about the Automatic Identification System (AIS) protocol and its implementation in Traccar.*

## Overview

The Automatic Identification System (AIS) is a maritime navigation safety communications system standardized by the International Telecommunication Union (ITU) and adopted by the International Maritime Organization (IMO). AIS provides vessel information, including identity, type, position, course, speed, navigational status, and other safety-related information automatically to appropriately equipped shore stations, other ships, and aircraft.

AIS operates autonomously and continuously in a self-organizing manner, requiring no central control station. The system is designed to handle thousands of reports per minute and update as frequently as every two seconds.

## Purpose and Applications

AIS was originally developed as a collision avoidance tool, but has expanded to serve multiple purposes:

- **Collision Avoidance**: Provides real-time information about nearby vessels to help prevent collisions
- **Vessel Traffic Services (VTS)**: Enables shore authorities to monitor and manage vessel traffic
- **Maritime Security**: Helps identify and track vessels for security purposes
- **Search and Rescue**: Assists in locating vessels in distress
- **Navigation Aids**: Marks navigation hazards and provides information about waterways
- **Fleet Management**: Allows companies to track and manage their vessels

## Regulatory Requirements

The International Maritime Organization's (IMO) International Convention for the Safety of Life at Sea (SOLAS) requires AIS transponders on:

- All international voyaging ships with 300 or more gross tonnage (GT)
- All cargo vessels of more than 500 GT
- All passenger vessels, regardless of size

## Technical Specifications

### Communication Channels

AIS uses two dedicated VHF maritime channels:
- AIS Channel A: 161.975 MHz (87B)
- AIS Channel B: 162.025 MHz (88B)

The system uses 9.6 kbit/s Gaussian minimum shift keying (GMSK) modulation over 25 kHz channels using the high-level data link control (HDLC) packet protocol.

### Message Format

AIS messages are transmitted in NMEA 0183 format, specifically using AIVDM/AIVDO sentences:
- AIVDM: Messages received from other vessels
- AIVDO: Messages from the vessel's own transponder

Example AIVDM sentence:
```
!AIVDM,1,1,,A,14eG;o@034o8sd<L9i:a;WF>062D,0*7D
```

The AIVDM/AIVDO sentence structure is as follows:
1. `!AIVDM` - Sentence identifier
2. Number of fragments for the message
3. Fragment number
4. Sequential message ID (for multi-sentence messages)
5. Radio channel code (A or B)
6. Encoded payload
7. Number of fill bits (0-5)
8. Checksum

### SOTDMA Technology

AIS uses Self-Organizing Time Division Multiple Access (SOTDMA) technology to avoid transmission collisions. This allows multiple vessels to share the same frequency by allocating specific time slots for each transmission.

## Message Types

There are 27 different AIS message types, but Traccar's implementation focuses on the most common position report messages:

### Supported Message Types in Traccar

- **Message Type 1**: Position Report (Scheduled)
- **Message Type 2**: Position Report (Assigned)
- **Message Type 3**: Position Report (Special)
- **Message Type 18**: Standard Class B Equipment Position Report

These message types contain similar information, including:
- MMSI (Maritime Mobile Service Identity)
- Navigation status (for types 1-3)
- Rate of turn (for types 1-3)
- Speed over ground
- Position accuracy
- Longitude and latitude
- Course over ground
- True heading
- Time stamp

## Traccar Implementation

Traccar implements AIS protocol support through two main classes:

### AisProtocol Class

The `AisProtocol` class extends `BaseProtocol` and sets up the server configuration for receiving AIS messages. It configures a TCP server that listens for AIS data and processes it using the AIS protocol decoder.

```java
public class AisProtocol extends BaseProtocol {

    @Inject
    public AisProtocol(Config config) {
        addServer(new TrackerServer(config, getName(), false) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline, Config config) {
                pipeline.addLast(new StringDecoder());
                pipeline.addLast(new AisProtocolDecoder(AisProtocol.this));
            }
        });
    }
}
```

The AIS protocol implementation in Traccar:
- Uses TCP for receiving AIS data
- Employs a `StringDecoder` to convert the incoming bytes to strings
- Processes the NMEA sentences using the `AisProtocolDecoder`

### AisProtocolDecoder Class

The `AisProtocolDecoder` class is responsible for parsing AIS messages and converting them into Traccar position objects. It implements:

1. A pattern matcher for AIVDM sentences
2. A payload decoder that extracts position and vessel information
3. Support for handling multi-fragment messages

```java
private static final Pattern PATTERN = new PatternBuilder()
        .text("!AIVDM,")
        .number("(d+),")                     // count
        .number("(d+),")                     // index
        .number("(d+)?,")                    // id
        .expression(".,")                    // radio channel
        .expression("([^,]+),")              // payload
        .any()
        .compile();
```

The decoder specifically handles message types 1, 2, 3, and 18, which contain position information. It extracts the following data:

- MMSI (used as the device identifier)
- Position (latitude and longitude)
- Speed
- Course
- Status (for types 1-3)
- Turn rate (for types 1-3)
- Heading

The decoder processes the binary payload using a `BitBuffer` to extract the fields according to the AIS specification:

```java
private Position decodePayload(Channel channel, SocketAddress remoteAddress, BitBuffer buf) {

    int type = buf.readUnsigned(6);
    if (type == 1 || type == 2 || type == 3 || type == 18) {

        buf.readUnsigned(2);
        int mmsi = buf.readUnsigned(30);

        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, String.valueOf(mmsi));
        if (deviceSession == null) {
            return null;
        }

        Position position = new Position(getProtocolName());
        position.setDeviceId(deviceSession.getDeviceId());

        position.setTime(new Date());

        if (type == 18) {
            buf.readUnsigned(8); // reserved
        } else {
            position.set(Position.KEY_STATUS, buf.readUnsigned(4));
            position.set("turn", buf.readSigned(8));
        }

        position.setSpeed(buf.readUnsigned(10) * 0.1);
        position.setValid(buf.readUnsigned(1) != 0);
        position.setLongitude(buf.readSigned(28) * 0.0001 / 60.0);
        position.setLatitude(buf.readSigned(27) * 0.0001 / 60.0);
        position.setCourse(buf.readUnsigned(12) * 0.1);

        position.set("heading", buf.readUnsigned(9));

        return position;
    }

    return null;
}
```

## Configuration and Usage

### Server Configuration

To enable AIS protocol support in Traccar:

1. Ensure the AIS protocol is enabled in the `traccar.xml` configuration file
2. Configure the appropriate port for AIS data reception (default TCP port varies by installation)

Example configuration in `traccar.xml`:
```xml
<entry key='ais.port'>5002</entry>
```

### Device Setup

AIS receivers can be connected to Traccar in several ways:

1. **Direct Network Connection**: AIS receivers with network capability can send data directly to the Traccar server
2. **Through a Gateway**: AIS data can be forwarded through a computer or gateway device
3. **AIS Base Stations**: Shore-based AIS stations can forward vessel data to Traccar

### Data Flow

The typical data flow for AIS messages in Traccar is:

1. AIS receiver captures VHF transmissions from vessels
2. Receiver converts signals to NMEA sentences
3. Sentences are transmitted to Traccar server via TCP
4. Traccar decodes the AIVDM/AIVDO sentences
5. Position information is extracted and stored

### Data Interpretation

When Traccar receives AIS data, it:

1. Identifies vessels by their MMSI number
2. Creates devices in the system for each unique MMSI (if configured to do so)
3. Generates position reports with the extracted information
4. Makes this data available through the Traccar web interface and API

### Multi-Fragment Message Handling

The AIS protocol allows for messages to be split across multiple NMEA sentences when they exceed the maximum sentence length. Traccar's implementation handles this by:

1. Identifying fragments using the count, index, and ID fields
2. Storing incomplete fragments in a buffer
3. Reassembling the complete message when all fragments are received
4. Processing the complete message once all fragments are available

## Limitations and Considerations

### Current Limitations

- Traccar's AIS implementation focuses on position reporting messages (types 1, 2, 3, and 18)
- Static vessel information (name, call sign, dimensions, etc.) from message types 5 and 24 is not currently processed
- Binary messages and application-specific messages are not supported
- AIS is receive-only in Traccar; the system does not transmit AIS messages

### Performance Considerations

- AIS data can be high volume in busy waterways
- Consider the server capacity when enabling AIS in areas with heavy maritime traffic
- Database growth may be significant when tracking many vessels

### Security Considerations

- AIS data is transmitted in the clear and can be spoofed
- MMSI numbers should be validated against other sources when security is critical
- AIS should not be the sole source of vessel position information for critical applications

## References

- ITU Recommendation M.1371: Technical characteristics for an automatic identification system using time division multiple access in the VHF maritime mobile frequency band
- IEC 61993-2: Maritime navigation and radiocommunication equipment and systems
- IALA Guidelines on AIS
- IMO SOLAS Chapter V
- NMEA 0183 Standard for Interfacing Marine Electronic Devices
- AIVDM/AIVDO Protocol Decoding: https://gpsd.gitlab.io/gpsd/AIVDM.html
- U.S. Coast Guard Navigation Center AIS Information: https://www.navcen.uscg.gov/?pageName=AISmain