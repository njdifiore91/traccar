# Position Service Message Schemas

This document describes the message schemas used by the Position Processing Service for communication via the message broker. It details the structure of messages consumed from the 'raw-positions' topic and produced to the 'enriched-positions' topic.

## Overview

The Position Processing Service is a core component of the Traccar microservices architecture that processes and enriches raw position data received from GPS devices. It communicates with other services primarily through an asynchronous event-driven messaging pattern using a centralized message broker (Kafka/RabbitMQ).

### Message Flow

```
Protocol Service → raw-positions topic → Position Processing Service → enriched-positions topic → Event Processing Service
```

## Message Formats

All inter-service messages use Protocol Buffers (protobuf) for efficient binary serialization. The message schemas are defined in the `position.proto` file located in the common library.

### Raw Position Message

Raw position messages are consumed from the 'raw-positions' topic. These messages contain the basic position data decoded from device protocols by the Protocol Service.

#### Schema Structure

```protobuf
message RawPositionMessage {
  // Message format version for backward compatibility
  int32 version = 1;
  
  // Device identifier
  string device_id = 2;
  
  // Protocol used by the device
  string protocol = 3;
  
  // Timestamps
  int64 device_time = 4;  // Time reported by the device (milliseconds since epoch)
  int64 server_time = 5;  // Time when the message was received by the server
  
  // Position data
  double latitude = 6;    // Latitude in degrees
  double longitude = 7;   // Longitude in degrees
  double altitude = 8;    // Altitude in meters
  double speed = 9;       // Speed in knots
  double course = 10;     // Course in degrees
  
  // Position validity flag
  bool valid = 11;
  
  // Additional attributes as key-value pairs
  map<string, AttributeValue> attributes = 12;
  
  // Network information (optional)
  NetworkInfo network = 13;
}

// Represents different types of attribute values
message AttributeValue {
  oneof value {
    string string_value = 1;
    double double_value = 2;
    int64 int_value = 3;
    bool bool_value = 4;
  }
}

// Network information
message NetworkInfo {
  repeated CellTower cell_towers = 1;
  repeated WifiAccessPoint wifi_access_points = 2;
}

// Cell tower information
message CellTower {
  int32 mobile_country_code = 1;
  int32 mobile_network_code = 2;
  int32 location_area_code = 3;
  int32 cell_id = 4;
  int32 signal_strength = 5;
}

// WiFi access point information
message WifiAccessPoint {
  string mac_address = 1;
  int32 signal_strength = 2;
}
```

#### Field Descriptions

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| version | int32 | Message format version for backward compatibility | Yes |
| device_id | string | Unique identifier of the device | Yes |
| protocol | string | Name of the protocol used by the device | Yes |
| device_time | int64 | Time reported by the device (milliseconds since epoch) | Yes |
| server_time | int64 | Time when the message was received by the server | Yes |
| latitude | double | Latitude in degrees (-90 to 90) | Yes |
| longitude | double | Longitude in degrees (-180 to 180) | Yes |
| altitude | double | Altitude in meters | No |
| speed | double | Speed in knots | No |
| course | double | Course in degrees (0 to 360) | No |
| valid | bool | Indicates if the position is valid | Yes |
| attributes | map | Additional attributes as key-value pairs | No |
| network | NetworkInfo | Network information for geolocation | No |

#### Common Attributes

The `attributes` field can contain various device-specific and protocol-specific values. Common attributes include:

| Attribute Key | Type | Description |
|--------------|------|-------------|
| sat | int | Number of satellites in use |
| hdop | double | Horizontal dilution of precision |
| battery | double | Battery level in volts |
| batteryLevel | int | Battery level as percentage |
| ignition | bool | Ignition status |
| motion | bool | Motion status |
| odometer | double | Odometer value in meters |
| fuel | double | Fuel level in liters |
| rpm | int | Engine RPM |
| raw | string | Original raw message from the device |

### Enriched Position Message

Enriched position messages are produced to the 'enriched-positions' topic after processing and enrichment by the Position Processing Service.

#### Schema Structure

```protobuf
message EnrichedPositionMessage {
  // Message format version for backward compatibility
  int32 version = 1;
  
  // Device identifier
  string device_id = 2;
  
  // Protocol used by the device
  string protocol = 3;
  
  // Timestamps
  int64 device_time = 4;  // Time reported by the device (milliseconds since epoch)
  int64 server_time = 5;  // Time when the message was received by the server
  int64 fix_time = 6;     // Time of the position fix
  
  // Position data
  double latitude = 7;    // Latitude in degrees
  double longitude = 8;   // Longitude in degrees
  double altitude = 9;    // Altitude in meters
  double speed = 10;      // Speed in knots
  double course = 11;     // Course in degrees
  
  // Position validity flag
  bool valid = 12;
  
  // Additional attributes as key-value pairs
  map<string, AttributeValue> attributes = 13;
  
  // Network information (optional)
  NetworkInfo network = 14;
  
  // Enriched data
  string address = 15;           // Reverse geocoded address
  double accuracy = 16;          // Position accuracy in meters
  repeated int64 geofence_ids = 17;  // IDs of geofences containing this position
  bool outdated = 18;            // Flag indicating if the position is outdated
}
```

#### Field Descriptions

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| version | int32 | Message format version for backward compatibility | Yes |
| device_id | string | Unique identifier of the device | Yes |
| protocol | string | Name of the protocol used by the device | Yes |
| device_time | int64 | Time reported by the device (milliseconds since epoch) | Yes |
| server_time | int64 | Time when the message was received by the server | Yes |
| fix_time | int64 | Time of the position fix | Yes |
| latitude | double | Latitude in degrees (-90 to 90) | Yes |
| longitude | double | Longitude in degrees (-180 to 180) | Yes |
| altitude | double | Altitude in meters | No |
| speed | double | Speed in knots | No |
| course | double | Course in degrees (0 to 360) | No |
| valid | bool | Indicates if the position is valid | Yes |
| attributes | map | Additional attributes as key-value pairs | No |
| network | NetworkInfo | Network information for geolocation | No |
| address | string | Reverse geocoded address | No |
| accuracy | double | Position accuracy in meters | No |
| geofence_ids | repeated int64 | IDs of geofences containing this position | No |
| outdated | bool | Flag indicating if the position is outdated | No |

#### Enriched Attributes

In addition to the attributes from the raw position message, the enriched position message may contain additional computed attributes:

| Attribute Key | Type | Description |
|--------------|------|-------------|
| distance | double | Distance traveled since the last position in meters |
| totalDistance | double | Total distance traveled by the device in meters |
| hours | double | Engine hours in milliseconds |
| motion | bool | Computed motion status based on speed and other factors |
| speedLimit | double | Speed limit at the current location in knots |
| index | int | Position index in the current trip |

## Message Validation

The Position Processing Service performs the following validations on incoming raw position messages:

1. **Required Fields**: Ensures all required fields are present
2. **Coordinate Range**: Validates that latitude is between -90 and 90 degrees and longitude is between -180 and 180 degrees
3. **Timestamp Validity**: Checks that timestamps are reasonable (not in the future, not too old)
4. **Device Existence**: Verifies that the device ID corresponds to a registered device

Messages that fail validation are logged and may be sent to a dead-letter queue for further analysis.

## Schema Versioning and Evolution

The message schemas include a version field to support backward compatibility during service evolution. The versioning strategy follows these principles:

1. **Backward Compatibility**: New versions of the schema must be able to process messages created with older versions
2. **Field Addition**: New fields can be added without breaking compatibility
3. **Field Deprecation**: Fields can be marked as deprecated but should not be removed immediately
4. **Version Increment**: The version number is incremented for any schema changes

## Example Messages

### Raw Position Message Example

```json
{
  "version": 1,
  "device_id": "123456789",
  "protocol": "teltonika",
  "device_time": 1625145600000,
  "server_time": 1625145605000,
  "latitude": 37.7749,
  "longitude": -122.4194,
  "altitude": 10.5,
  "speed": 15.2,
  "course": 45.3,
  "valid": true,
  "attributes": {
    "sat": { "int_value": 8 },
    "hdop": { "double_value": 1.2 },
    "battery": { "double_value": 12.4 },
    "ignition": { "bool_value": true },
    "raw": { "string_value": "0864895031016725..." }
  },
  "network": {
    "cell_towers": [
      {
        "mobile_country_code": 310,
        "mobile_network_code": 410,
        "location_area_code": 54321,
        "cell_id": 12345,
        "signal_strength": -85
      }
    ]
  }
}
```

### Enriched Position Message Example

```json
{
  "version": 1,
  "device_id": "123456789",
  "protocol": "teltonika",
  "device_time": 1625145600000,
  "server_time": 1625145605000,
  "fix_time": 1625145600000,
  "latitude": 37.7749,
  "longitude": -122.4194,
  "altitude": 10.5,
  "speed": 15.2,
  "course": 45.3,
  "valid": true,
  "attributes": {
    "sat": { "int_value": 8 },
    "hdop": { "double_value": 1.2 },
    "battery": { "double_value": 12.4 },
    "ignition": { "bool_value": true },
    "distance": { "double_value": 125.7 },
    "totalDistance": { "double_value": 15280.5 },
    "speedLimit": { "double_value": 25.0 },
    "motion": { "bool_value": true }
  },
  "network": {
    "cell_towers": [
      {
        "mobile_country_code": 310,
        "mobile_network_code": 410,
        "location_area_code": 54321,
        "cell_id": 12345,
        "signal_strength": -85
      }
    ]
  },
  "address": "123 Main St, San Francisco, CA 94105, USA",
  "accuracy": 15.0,
  "geofence_ids": [5, 8, 12],
  "outdated": false
}
```

## References

- [Position.java](../src/main/java/org/traccar/model/Position.java) - Java model class for position data
- [position.proto](../../common/src/main/proto/position.proto) - Protocol Buffer schema definition
- [ProcessingHandler.java](../src/main/java/org/traccar/ProcessingHandler.java) - Position processing pipeline implementation