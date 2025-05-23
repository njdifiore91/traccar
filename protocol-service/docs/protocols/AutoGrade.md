# AutoGrade Protocol Documentation

## Overview

AutoGrade is an Indian manufacturer of road safety devices and vehicle tracking systems. Their GPS tracking solution, known as TANK Pro, works on GPS-GSM architecture and provides real-time vehicle tracking capabilities. This document describes the protocol used by AutoGrade GPS tracking devices and its implementation in the Traccar platform.

The AutoGrade protocol is a text-based protocol where messages are terminated with a closing parenthesis character ')'. Each message contains position information, device status, and additional sensor data.

## Transport

The AutoGrade protocol operates over:

- TCP (default port: 5159)

## Message Format

AutoGrade messages follow this format:

```
(INDEX_NUMBER)(IMEI)(DATE)(VALIDITY)(LATITUDE)(LONGITUDE)(SPEED)(TIME)(COURSE)(STATUS)(ADC_VALUES)(CAN_VALUES)
```

Where:

| Field | Description | Example |
|-------|-------------|--------|
| INDEX_NUMBER | 12-digit sequence number | 000000000123 |
| IMEI | 15-digit device identifier | 123456789012345 |
| DATE | Date in DDMMYY format | 010120 |
| VALIDITY | Position validity flag (A=valid, V=invalid) | A |
| LATITUDE | Latitude in degrees and decimal minutes (DDmm.mmmm) with hemisphere (N/S) | 1030.7652N |
| LONGITUDE | Longitude in degrees and decimal minutes (DDDmm.mmmm) with hemisphere (E/W) | 07651.6543E |
| SPEED | Speed in knots with decimal places | 005.3 |
| TIME | Time in HHMMSS format | 102030 |
| COURSE | Course/heading in degrees with decimal places | 015.26 |
| STATUS | Single character status code | 1 |
| ADC_VALUES | Five 4-digit hexadecimal ADC values prefixed with A-E | Axxxx Bxxxx Cxxxx Dxxxx Exxxx |
| CAN_VALUES | Five 4-digit hexadecimal CAN bus values prefixed with K-O | Kxxxx Lxxxx Mxxxx Nxxxx Oxxxx |

### Status Field

The status field is a single character that contains bit-encoded information. The least significant bit (bit 0) represents the ignition status (1 = on, 0 = off).

## Traccar Implementation

The Traccar platform implements the AutoGrade protocol through two main classes:

1. **AutoGradeProtocol.java**: Defines the protocol and configures the pipeline with appropriate decoders.
2. **AutoGradeProtocolDecoder.java**: Handles the parsing of AutoGrade messages and conversion to the Traccar Position model.

### Protocol Configuration

The AutoGradeProtocol class sets up a pipeline with the following components:

- CharacterDelimiterFrameDecoder (with delimiter ')')
- StringDecoder
- StringEncoder
- AutoGradeProtocolDecoder

### Decoder Implementation

The AutoGradeProtocolDecoder uses a regular expression pattern to parse the message fields. It extracts the following information:

- Device identifier (IMEI)
- Position validity
- Latitude and longitude
- Speed
- Date and time
- Course
- Status (including ignition state)
- ADC values (5 channels)
- CAN bus values (5 channels)

## Configuration in Traccar

To configure Traccar to work with AutoGrade devices:

1. Add a device with the correct unique identifier (IMEI)
2. Set the protocol to "autograde"
3. Configure the device to send data to your Traccar server IP and port 5159

## Message Examples

### Example 1: Valid Position

```
(000000000123123456789012345010120A1030.7652N07651.6543E005.3102030015.261A1234B5678C9012D3456E7890K1234L5678M9012N3456O7890)
```

Decoded information:
- Index: 000000000123
- IMEI: 123456789012345
- Date: January 1, 2020
- Valid position: Yes
- Latitude: 10° 30.7652' N
- Longitude: 76° 51.6543' E
- Speed: 5.3 knots
- Time: 10:20:30 UTC
- Course: 15.26°
- Status: 1 (ignition on)
- ADC values: A1234, B5678, C9012, D3456, E7890
- CAN values: K1234, L5678, M9012, N3456, O7890

### Example 2: Invalid Position

```
(000000000124123456789012345010120V0000.0000N00000.0000E000.0102030000.001A0000B0000C0000D0000E0000K0000L0000M0000N0000O0000)
```

Decoded information:
- Index: 000000000124
- IMEI: 123456789012345
- Date: January 1, 2020
- Valid position: No
- Latitude: 0° 0.0000' N (invalid)
- Longitude: 0° 0.0000' E (invalid)
- Speed: 0.0 knots
- Time: 10:20:30 UTC
- Course: 0.00°
- Status: 1 (ignition on)
- ADC values: All zeros
- CAN values: All zeros

## References

1. [AutoGrade Official Website](https://www.autograde.in/)
2. [AutoGrade TANK Pro Vehicle Tracking System](https://www.autograde.in/TANK-Pro.php)
3. [Traccar AutoGrade Protocol Implementation](https://github.com/traccar/traccar/blob/master/src/main/java/org/traccar/protocol/AutoGradeProtocol.java)