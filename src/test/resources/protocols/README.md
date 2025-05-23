# Protocol Test Resources

## Overview

This directory contains test resources for validating the functionality of Traccar's protocol implementations. These resources support testing of 200+ GPS device protocols across both monolithic and microservices architectures. The test data is organized in a consistent, maintainable structure to ensure reliable protocol testing and verification.

## Directory Structure

The protocol test resources are organized as follows:

```
protocols/
├── README.md                 # This documentation file
├── common/                   # Shared resources used across multiple protocols
│   ├── test_config.json      # Common test configuration settings
│   ├── common_commands.json  # Standard command templates
│   ├── binary_test_utils.json # Utilities for binary protocol testing
│   ├── test_utils.json       # Common test helper functions
│   └── base_position.json    # Standard Position object template
├── protocol1/                # Protocol-specific test resources (e.g., meitrack)
│   ├── command_samples.json  # Command objects and expected encoded outputs
│   └── expected_positions.json # Expected Position objects from decoding
├── protocol2/                # Another protocol (e.g., teltonika)
│   ├── config.json           # Protocol-specific test configuration
│   ├── expected_attributes.json # Expected attribute key-value pairs
│   └── expected_position.json # Expected Position object data
└── ...
```

## Resource Types

### Common Resources

- **test_config.json**: Contains shared configuration settings for protocol tests, including common test parameters, timeouts, mock settings, and test environment configurations.

- **common_commands.json**: Defines standard command templates used across multiple protocol encoder tests, ensuring consistent command testing.

- **binary_test_utils.json**: Provides specialized utilities for testing binary protocols, including binary conversion functions and message parsing helpers.

- **test_utils.json**: Contains common test utilities and helper functions used across protocol tests to promote code reuse and consistency.

- **base_position.json**: Provides a standardized template for Position objects with common fields that all protocol decoders should populate.

### Protocol-Specific Resources

- **command_samples.json**: Contains sample Command objects and their expected encoded representations for testing protocol encoders.

- **expected_positions.json**: Defines expected Position objects that should result from decoding sample protocol messages.

- **config.json**: Configuration file for protocol-specific tests, containing settings and parameters used during test execution.

- **expected_attributes.json**: Contains expected attribute key-value pairs after parsing protocol messages.

## Usage in Tests

### Decoder Testing

Protocol decoder tests use these resources to verify that binary or text messages are correctly parsed into Position objects:

```java
@Test
public void testDecode() throws Exception {
    // Load test resources
    String binary = loadResource("/protocols/meitrack/binary_sample.hex");
    JSONObject expectedPosition = loadJsonResource("/protocols/meitrack/expected_positions.json");
    
    // Test the decoder
    MeitrackProtocolDecoder decoder = new MeitrackProtocolDecoder(null);
    Position position = decoder.decode(null, null, binary);
    
    // Verify the result matches the expected position
    verifyPosition(position, expectedPosition);
}
```

### Encoder Testing

Protocol encoder tests use these resources to verify that Command objects are correctly encoded into protocol-specific formats:

```java
@Test
public void testEncode() throws Exception {
    // Load test resources
    JSONObject commandSamples = loadJsonResource("/protocols/meitrack/command_samples.json");
    Command command = createCommand(commandSamples.getJSONObject("positionSingle"));
    String expected = commandSamples.getString("positionSingleEncoded");
    
    // Test the encoder
    MeitrackProtocolEncoder encoder = new MeitrackProtocolEncoder(null);
    String result = encoder.encodeCommand(command);
    
    // Verify the result matches the expected output
    assertEquals(expected, result);
}
```

## Microservices Testing

In the microservices architecture, these test resources are used to verify correct message parsing across service boundaries:

1. **Protocol Service Tests**: Verify that device messages are correctly decoded into standardized Position objects and commands are properly encoded.

2. **Integration Tests**: Ensure that Position objects created by the Protocol Service can be properly consumed by other services like the Position Service.

3. **Contract Tests**: Validate that the Protocol Service adheres to the expected message formats and data structures required by other services.

## Guidelines for Adding New Test Data

When adding test resources for a new protocol or extending existing ones:

1. **Follow the established directory structure**: Create a new directory for the protocol if it doesn't exist.

2. **Use standardized formats**: Follow the JSON structure used in existing test resources.

3. **Include comprehensive test cases**: Cover normal operation, edge cases, and error conditions.

4. **Document protocol-specific details**: Include comments in the JSON files explaining any non-obvious aspects of the protocol.

5. **Reuse common resources**: Leverage the resources in the `common` directory where possible.

6. **Maintain backward compatibility**: Ensure new test resources work with both monolithic and microservices testing approaches.

7. **Include binary samples**: For binary protocols, include both the hex-encoded binary data and the expected decoded result.

## Binary Sample Format

Binary protocol samples should be provided in a standardized format:

1. **Hex-encoded strings**: Binary data should be represented as hex-encoded strings (e.g., "01020304").

2. **Annotated sections**: Complex binary messages should include comments explaining the purpose of different message sections.

3. **Multiple samples**: Include samples for different message types and scenarios.

4. **Expected results**: Each binary sample should have a corresponding expected result in JSON format.

## Conclusion

These test resources are critical for ensuring the reliability and correctness of Traccar's protocol implementations. By maintaining a consistent and comprehensive set of test data, we can ensure that the system correctly handles the wide variety of GPS device protocols supported by Traccar, both in the monolithic architecture and in the new microservices architecture.