# Protocol Message Samples

## Overview

This document describes the protocol message samples available in the fixtures directory and provides guidance on how to use them in protocol tests. These fixtures are essential for testing the Protocol Service's ability to decode and encode messages from 200+ GPS device protocols in the Traccar microservices architecture.

> **Note:** This documentation is part of the Traccar microservices refactoring project, which transforms the monolithic application into a distributed architecture while maintaining complete functional equivalence.

## Purpose

The test fixtures serve several critical purposes:

1. **Standardized Testing**: Provide consistent test data across all protocol implementations
2. **Regression Prevention**: Ensure protocol decoders/encoders continue to work as expected after changes
3. **Documentation**: Serve as practical examples of each protocol's message format
4. **Development Support**: Aid in the development of new protocol implementations
5. **Microservice Validation**: Verify the Protocol Service correctly processes messages and publishes them to the message broker
6. **Contract Verification**: Ensure the Protocol Service produces messages that conform to the expected schema
7. **Integration Testing**: Support testing of the complete processing pipeline from device to position service
8. **Performance Benchmarking**: Provide standard inputs for measuring decoder performance
9. **Compatibility Testing**: Verify compatibility with different protocol versions and variants

## Directory Structure

The fixtures are organized in the following structure:

```
protocol-service/src/test/resources/fixtures/
├── protocol-message-samples.md   # This documentation file
├── command.json                  # Standard command template for encoder testing
├── device.json                   # Standard device configuration for testing
├── position.json                 # Standard position object for verification
├── binary/                       # Binary protocol message samples
│   ├── tk103/                    # Samples organized by protocol
│   ├── meitrack/
│   ├── teltonika/
│   ├── atrack/
│   ├── totem/
│   ├── meiligao/
│   └── ...
└── json/                         # JSON protocol message samples
    ├── osmand.json               # OsmAnd protocol samples
    ├── owntracks.json            # OwnTracks protocol samples
    ├── hoopo.json                # Hoopo protocol samples
    ├── teratrack.json            # TeraTrack protocol samples
    ├── outsafe.json              # Outsafe protocol samples
    ├── b2316.json                # B2316 protocol samples
    └── ...
```

The structure mirrors the organization in the Protocol Service, where each protocol has its own decoder and encoder implementation.

## Fixture Types

### Standard Fixtures

1. **command.json**: Contains standard command templates used for testing protocol encoders. Includes command types, parameters, and expected output formats for protocols that support bidirectional communication.

2. **device.json**: Contains device configuration objects used for protocol testing. Includes device identification, protocol type, and configuration parameters.

3. **position.json**: Contains a standard position object that serves as a baseline for protocol testing. Includes all fields that might be populated by various protocols.

### Binary Protocol Samples

Binary protocol samples are stored in the `binary/` directory, organized by protocol name. Each protocol directory contains one or more binary message samples in the following formats:

- **Raw Binary Files**: `.bin` files containing the raw binary message as received from the device
- **Hex String Files**: `.txt` files containing the hexadecimal representation of the message

These samples are used by the protocol decoder tests to verify correct parsing of binary messages into position objects.

### JSON Protocol Samples

JSON protocol samples are stored in the `json/` directory, with each file named after the corresponding protocol. These files contain sample JSON payloads as would be received from devices that communicate using JSON over HTTP or WebSocket.

Each JSON file contains multiple sample messages to test different aspects of the protocol, such as:
- Basic position reporting
- Event reporting
- Status updates
- Device configuration

## Using Fixtures in Protocol Tests

### Binary Protocol Testing

For binary protocols, use the `ProtocolTest` base class which provides utilities for testing protocol decoders and encoders:

```java
public class Tk103ProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        Tk103ProtocolDecoder decoder = new Tk103ProtocolDecoder(null);
        
        // Test with a binary fixture
        verifyPosition(decoder, binary("tk103/position1"));
        
        // Test with a hex string
        verifyPosition(decoder, buffer("(013500001111BP05000013632782450080641936251120A2234.0297N11405.9101E000.0040331309.0000000000L00000000)"));
        
        // Test with a hex string that should result in null
        verifyNull(decoder, buffer("(013500001111BP00000013632782450080641936251120A2234.0297N11405.9101E000.0040331309.0000000000L00000000)"));
    }
}
```

The `binary()` method loads a binary fixture from the `binary/` directory, while the `buffer()` method creates a binary buffer from a hex string.

#### Testing Protocol Encoders

For protocols that support bidirectional communication, test the encoder using the command fixture:

```java
public class Tk103ProtocolEncoderTest extends ProtocolTest {

    @Test
    public void testEncode() throws Exception {
        Tk103ProtocolEncoder encoder = new Tk103ProtocolEncoder(null);
        
        // Test position request command
        Command command = new Command();
        command.setDeviceId(1);  
        command.setType(Command.TYPE_POSITION_SINGLE);
        
        assertEquals("(013500001111AP01)", 
                encoder.encodeCommand(command));
        
        // Test engine stop command
        command.setType(Command.TYPE_ENGINE_STOP);
        
        assertEquals("(013500001111AV010)", 
                encoder.encodeCommand(command));
    }
}
```
```

The `binary()` method loads a binary fixture from the `binary/` directory, while the `buffer()` method creates a binary buffer from a hex string.

### JSON Protocol Testing

For JSON protocols, use the `ProtocolTest` base class which provides utilities for testing JSON protocol decoders:

```java
public class OsmAndProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        OsmAndProtocolDecoder decoder = new OsmAndProtocolDecoder(null);
        
        // Test with a JSON fixture
        verifyPositions(decoder, request(HttpMethod.POST, "/",
                json("osmand")));
        
        // Test with an inline JSON string
        verifyPosition(decoder, request(HttpMethod.POST, "/",
                buffer("{\"lat\":51.0,\"lon\":13.0,\"timestamp\":1346269322,\"altitude\":300,\"speed\":5.55,\"hdop\":1.0,\"deviceid\":\"123456\"}"))); 
    }
}
```

The `json()` method loads a JSON fixture from the `json/` directory, while the `request()` method creates an HTTP request with the specified content.

## Adding New Protocol Samples

When adding support for a new protocol or enhancing tests for an existing one, follow these guidelines:

### For Binary Protocols

1. Create a directory for the protocol in `binary/` if it doesn't exist
2. Add sample messages as binary files or hex strings
3. Name files descriptively to indicate the type of message (e.g., `position.bin`, `alarm.bin`)
4. Include a variety of message types to test different aspects of the protocol
5. Document any special considerations in comments in the test class

### For JSON Protocols

1. Create a JSON file for the protocol in `json/` if it doesn't exist
2. Add sample messages as JSON objects in an array
3. Include a variety of message types to test different aspects of the protocol
4. Ensure the JSON is valid and properly formatted
5. Document the structure and fields in comments in the test class

### Sample Format Requirements

- **Binary Samples**: Should be raw binary data as received from the device
- **Hex Strings**: Should be the exact hexadecimal representation without additional formatting
- **JSON Samples**: Should match the exact format expected by the protocol decoder

## Best Practices

1. **Comprehensive Coverage**: Include samples for all message types supported by the protocol
2. **Edge Cases**: Include samples that test boundary conditions and special cases
3. **Real-World Data**: Use actual messages captured from devices when possible
4. **Anonymization**: Remove or replace sensitive information (e.g., real device IDs, exact locations)
5. **Documentation**: Add comments explaining any non-obvious aspects of the samples
6. **Validation**: Verify that all samples can be correctly decoded before committing
7. **Consistency**: Maintain consistent naming and organization across all protocol fixtures
8. **Versioning**: Include samples for different firmware versions when protocol behavior varies
9. **Minimal Examples**: Keep samples focused on specific features to simplify debugging
10. **Cross-Protocol Testing**: For multi-protocol devices, include samples in each relevant protocol directory

## Integration with Microservices Testing

These fixtures play a crucial role in the microservices testing strategy:

1. **Unit Testing**: Protocol decoders/encoders are tested in isolation using these fixtures
2. **Integration Testing**: The Protocol Service's ability to process messages and publish to the message broker is verified
3. **Contract Testing**: Ensures the Protocol Service produces messages that conform to the expected schema
4. **End-to-End Testing**: Used in simulated device connections to test the complete processing pipeline

### Message Flow in Microservices Architecture

In the microservices architecture, protocol messages follow this flow:

1. Device sends a message to the Protocol Service
2. Protocol Service decodes the message using the appropriate protocol decoder
3. Protocol Service converts the decoded data to a standardized Position message
4. Protocol Service publishes the Position message to the message broker
5. Position Processing Service consumes the message for further processing

The test fixtures ensure that step 2 works correctly across all supported protocols, which is critical for the entire system's functionality.

## Maintenance

Regularly review and update the fixtures to ensure they remain relevant and comprehensive:

1. **Add New Variants**: When devices with new firmware versions or variants are encountered
2. **Update Existing Samples**: When protocol specifications change
3. **Remove Obsolete Samples**: When protocols are deprecated or no longer supported
4. **Verify Consistency**: Ensure all samples follow the same formatting and naming conventions

## Troubleshooting

If you encounter issues with protocol tests:

1. **Verify Fixture Format**: Ensure the fixture matches the expected format for the protocol
2. **Check Decoder Logic**: Verify that the decoder correctly handles the message format
3. **Examine Hex Representation**: For binary protocols, check the hex representation for accuracy
4. **Validate JSON Structure**: For JSON protocols, validate the JSON structure against the protocol specification
5. **Compare with Working Examples**: Compare with existing working samples for the same protocol
6. **Enable Debug Logging**: Set appropriate logging levels to see detailed decoding steps
7. **Isolate the Issue**: Test with minimal examples to identify the specific problem
8. **Check Protocol Documentation**: Refer to the protocol specification or device manual
9. **Verify Message Broker Integration**: For integration tests, check message publication
10. **Review Protocol Changes**: Check if the device firmware or protocol has been updated

## Working with Protocol Buffers

In the microservices architecture, the Protocol Service uses Protocol Buffers (protobuf) for efficient serialization when publishing messages to the message broker. The standard position object from the fixtures is converted to a protobuf message before publishing.

### Position Protobuf Schema

The Position protobuf schema is defined in `common/src/main/proto/position.proto` and includes all fields that might be present in a position report:

```protobuf
syntax = "proto3";

package org.traccar.proto;

option java_multiple_files = true;

message Position {
  int64 device_id = 1;
  string protocol = 2;
  int64 time = 3;
  double latitude = 4;
  double longitude = 5;
  double altitude = 6;
  double speed = 7;
  double course = 8;
  map<string, string> attributes = 9;
  // Additional fields...
}
```

When testing the Protocol Service's integration with the message broker, ensure that the decoded position data can be correctly serialized to this protobuf format.

## Conclusion

The protocol message samples are a vital resource for developing and maintaining the Protocol Service in the Traccar microservices architecture. By following the guidelines in this document, you can effectively use and extend these fixtures to ensure robust protocol support across the system.

These test fixtures not only help maintain the quality of the Protocol Service but also ensure that the entire position processing pipeline functions correctly in the distributed architecture.