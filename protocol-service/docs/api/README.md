# Protocol Service API Documentation

## Overview

The Protocol Service is responsible for handling device connections and protocol implementations, decoding raw messages from 200+ supported device protocols into a standardized format. This documentation provides a comprehensive reference for all APIs exposed by the Protocol Service, enabling seamless integration with other microservices in the Traccar ecosystem.

## API Types

The Protocol Service exposes several types of APIs for different integration purposes:

| API Type | Purpose | Primary Consumers | Documentation |
|----------|---------|-------------------|---------------|
| gRPC API | Protocol management and device command execution | API Gateway, Administration Services | [gRPC API Documentation](grpc.md) |
| Message Broker | Publishing decoded positions and device connection events | Position Service, Event Service | [Broker Integration](broker-integration.md) |
| Service Discovery | Service registration and health reporting | Service Registry, Orchestration Platform | [Service Discovery](service-discovery.md) |
| Health & Metrics | Service health monitoring and performance metrics | Monitoring Systems, Orchestration Platform | [Health API](health.md) |
| Command API | Device command execution | API Gateway, Administration Services | [Command API](command.md) |

## Authentication and Authorization

The Protocol Service implements secure authentication and authorization mechanisms for all its APIs:

### Service-to-Service Authentication

For internal service-to-service communication, the Protocol Service supports:

1. **Mutual TLS (mTLS)**: Services authenticate each other using X.509 certificates
2. **JWT Tokens**: For scenarios where mTLS is not feasible

### Authorization

Access to Protocol Service APIs is controlled through:

1. **Role-Based Access Control (RBAC)**: Services are assigned specific roles that determine their access permissions
2. **Resource-Level Permissions**: Fine-grained control over specific operations

Refer to each specific API documentation for detailed authentication requirements.

## Integration Examples

### Sending a Command to a Device

```java
// Example: Sending a position request command to a device using gRPC
DeviceCommandServiceGrpc.DeviceCommandServiceBlockingStub commandService = 
    DeviceCommandServiceGrpc.newBlockingStub(channel);

DataCommandRequest request = DataCommandRequest.newBuilder()
    .setDeviceId("123456")
    .setCommand(Command.newBuilder()
        .setType("positionSingle")
        .build())
    .build();

CommandResponse response = commandService.sendDataCommand(request);
if (response.getSuccess()) {
    System.out.println("Command sent successfully: " + response.getResult());
} else {
    System.out.println("Command failed: " + response.getMessage());
}
```

### Consuming Position Data from Message Broker

```java
// Example: Consuming position data from Kafka
@KafkaListener(topics = "raw.positions", groupId = "position-processor")
public void processPosition(Position position) {
    System.out.println("Received position for device: " + position.getDeviceId());
    // Process the position data
}
```

### Discovering Protocol Service

```java
// Example: Discovering Protocol Service using Consul
ConsulClient consulClient = new ConsulClient("consul.service.consul");
Response<List<ServiceHealth>> response = consulClient.getHealthyServiceInstances("protocol-service");
ServiceHealth instance = loadBalancer.choose(response.getValue());
String serviceUrl = instance.getService().getAddress() + ":" + instance.getService().getPort();
```

## API Reference

### gRPC API

The Protocol Service exposes gRPC interfaces for protocol management and device command execution. These interfaces enable other services to query protocol capabilities, send commands to devices, and retrieve protocol statistics.

[Detailed gRPC API Documentation](grpc.md)

### Message Broker Integration

The Protocol Service publishes decoded position data and device connection events to a message broker (Kafka/RabbitMQ) for consumption by downstream services.

[Detailed Broker Integration Documentation](broker-integration.md)

### Service Discovery

The Protocol Service registers with service discovery mechanisms (Consul/Kubernetes) to enable dynamic discovery by other services.

[Detailed Service Discovery Documentation](service-discovery.md)

### Health and Metrics API

The Protocol Service exposes health endpoints for liveness and readiness probes, as well as Prometheus-compatible metrics for monitoring.

[Detailed Health API Documentation](health.md)

### Command API

The Protocol Service provides a dedicated API for sending commands to connected devices, supporting various command types across different protocols.

[Detailed Command API Documentation](command.md)

## Protocol Support

The Protocol Service supports over 200 device protocols, each with its own capabilities and command support. For protocol-specific documentation, refer to the [Protocol Documentation](../protocols/README.md).

## Versioning and Compatibility

The Protocol Service follows semantic versioning (MAJOR.MINOR.PATCH) for its APIs:

- **MAJOR**: Incompatible API changes
- **MINOR**: Backward-compatible functionality additions
- **PATCH**: Backward-compatible bug fixes

All APIs maintain backward compatibility within the same major version. Breaking changes are introduced only in major version updates and are clearly documented in release notes.

## Error Handling

All Protocol Service APIs use standard error codes and provide detailed error messages to help diagnose issues:

| Error Code | Description | Resolution |
|------------|-------------|------------|
| INVALID_ARGUMENT | The request contains invalid parameters | Check request parameters against API documentation |
| NOT_FOUND | The requested resource was not found | Verify resource identifiers (device ID, protocol name) |
| PERMISSION_DENIED | Insufficient permissions for the operation | Check service authentication and authorization |
| UNAUTHENTICATED | Authentication failed | Verify authentication credentials |
| UNAVAILABLE | The service is temporarily unavailable | Implement retry with exponential backoff |
| INTERNAL | An internal server error occurred | Check service logs for details |

## Support and Feedback

For questions, issues, or feedback regarding the Protocol Service APIs, please contact the Traccar development team or open an issue in the project repository.