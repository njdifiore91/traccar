# Traccar Common Module

## Overview

The Traccar Common Module is a shared library that provides core functionality, models, and utilities used across all Traccar microservices. This module helps ensure consistent implementations, prevents code duplication, and establishes standardized interfaces for cross-service communication.

## Purpose

As part of Traccar's microservices architecture, this module serves several critical purposes:

- **Shared Domain Models**: Provides consistent data representations across all services
- **Messaging Infrastructure**: Standardizes communication patterns between services
- **Common Utilities**: Offers reusable utility functions for common operations
- **Security Components**: Implements shared security mechanisms
- **Protocol Definitions**: Contains Protocol Buffer definitions for service interfaces

## Module Structure

```
common/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── org/
│   │   │       └── traccar/
│   │   │           ├── model/         # Domain model classes
│   │   │           ├── messaging/     # Messaging interfaces and implementations
│   │   │           ├── security/      # Security components
│   │   │           ├── proto/         # Protocol buffer definitions
│   │   │           └── util/          # Shared utilities
│   │   ├── proto/                     # Protocol buffer source files
│   │   └── resources/                 # Shared resources
│   └── test/                          # Unit tests for common components
└── build.gradle                       # Build configuration
```

## Key Components

### Domain Models

The `model` package contains the core domain entities used throughout the Traccar system:

- `Position`: Represents GPS position data with attributes like coordinates, speed, and device information
- `Device`: Represents a tracking device with its configuration and status
- `Event`: Represents system events like geofence transitions or alerts
- `Notification`: Represents notifications to be delivered to users

These models are designed to be serializable for both database persistence and message passing between services.

### Messaging Infrastructure

The `messaging` package provides abstractions for inter-service communication:

- `MessageProducer`: Interface for publishing messages to the message broker
- `MessageConsumer`: Interface for consuming messages from the message broker
- Implementation-specific adapters for Kafka and RabbitMQ

### Security Components

The `security` package contains shared security mechanisms:

- Authentication providers and utilities
- Authorization handlers
- Cryptography utilities

### Protocol Definitions

The `proto` package contains Protocol Buffer definitions that standardize the message formats for both synchronous and asynchronous communication:

- `position.proto`: Position data message schema
- `event.proto`: Event data message schema
- `notification.proto`: Notification data message schema
- `command.proto`: Command execution schema

### Utility Classes

The `util` package provides reusable utility functions:

- `DateUtil`: Date/time manipulation utilities
- `DistanceCalculator`: Geospatial distance calculation
- `GeoUtils`: Geospatial utilities for coordinate operations

## Usage Guidelines

### Including the Common Module

To use the common module in a Traccar microservice, add it as a dependency in your `build.gradle` file:

```gradle
dependencies {
    implementation project(':common')
    // Other dependencies
}
```

### Using Domain Models

Import and use the shared domain models to ensure consistency across services:

```java
import org.traccar.model.Position;
import org.traccar.model.Device;

public class PositionProcessor {
    public void process(Position position) {
        // Process position data
    }
}
```

### Implementing Messaging

Use the messaging interfaces to standardize communication with the message broker:

```java
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Position;

@Service
public class PositionService {
    private final MessageProducer messageProducer;
    
    @Autowired
    public PositionService(MessageProducer messageProducer) {
        this.messageProducer = messageProducer;
    }
    
    public void processAndPublish(Position position) {
        // Process position
        messageProducer.publish("enriched-positions", position);
    }
}
```

### Using Protocol Buffers

Leverage the shared Protocol Buffer definitions for service communication:

```java
import org.traccar.proto.PositionOuterClass.Position;

public class PositionHandler {
    public void handlePositionMessage(Position positionProto) {
        // Handle position protocol buffer message
    }
}
```

## Contribution Guidelines

When contributing to the common module, please follow these guidelines:

1. **Backward Compatibility**: Changes to existing models or interfaces must maintain backward compatibility
2. **Minimal Dependencies**: Avoid adding external dependencies unless absolutely necessary
3. **Comprehensive Testing**: All components must have thorough unit tests
4. **Clear Documentation**: Document all public classes, methods, and interfaces
5. **Version Compatibility**: Ensure compatibility with all supported Java versions (Java 17+)

### Adding New Components

When adding new shared components:

1. Evaluate whether the component truly belongs in the common module
2. Ensure the component is used by multiple services
3. Design with flexibility and extensibility in mind
4. Document usage examples
5. Add appropriate unit tests

## Versioning

The common module follows semantic versioning:

- **Major version**: Incompatible API changes
- **Minor version**: Backward-compatible functionality additions
- **Patch version**: Backward-compatible bug fixes

All microservices should specify the exact version of the common module they depend on to ensure consistency.

## License

This module is part of the Traccar GPS tracking system and is licensed under the Apache License 2.0.