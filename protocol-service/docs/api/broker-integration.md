# Protocol Service Message Broker Integration

## Overview

The Protocol Service is responsible for decoding messages from GPS tracking devices and publishing the resulting data to a message broker for downstream processing. This document outlines the integration patterns, topic structures, message formats, and configuration options for the Protocol Service's message broker integration.

## Message Broker Technology

The Protocol Service supports integration with the following message brokers:

- **Apache Kafka** (v3.6.1) - Primary message broker for high-throughput scenarios
- **RabbitMQ** (v3.12.12) - Alternative message broker for specific deployment scenarios

The choice of message broker is configurable through the service configuration and does not affect the core functionality of the Protocol Service.

## Topic Structure

The Protocol Service publishes to the following topics:

| Topic Name | Purpose | Message Type | Partitioning Strategy |
|------------|---------|--------------|----------------------|
| `raw.positions` | Decoded position data from devices | Position message | By device ID |
| `device.connections` | Device connection state changes | Connection event message | By device ID |

### Topic Naming Convention

Topics follow a consistent naming convention:

- Use lowercase letters, numbers, and dots (`.`) as separators
- Use nouns rather than verbs (e.g., `raw.positions` not `position.created`)
- Use the format `[domain].[entity]` for clarity

## Message Schemas

### Position Message Schema

Position messages use Protocol Buffers for serialization. The schema is defined in `common/src/main/proto/position.proto`:

```protobuf
syntax = "proto3";

package org.traccar.proto;

import "google/protobuf/timestamp.proto";

message Position {
  int64 id = 1;
  int64 device_id = 2;
  string protocol = 3;
  google.protobuf.Timestamp device_time = 4;
  google.protobuf.Timestamp server_time = 5;
  bool outdated = 6;
  bool valid = 7;
  double latitude = 8;
  double longitude = 9;
  double altitude = 10;
  double speed = 11;
  double course = 12;
  int32 satellites = 13;
  map<string, AttributeValue> attributes = 14;
}

message AttributeValue {
  oneof value {
    string string_value = 1;
    bool boolean_value = 2;
    int32 int_value = 3;
    double double_value = 4;
    bytes binary_value = 5;
  }
}
```

### Connection Event Message Schema

Connection event messages use Protocol Buffers for serialization. The schema is defined in `common/src/main/proto/connection.proto`:

```protobuf
syntax = "proto3";

package org.traccar.proto;

import "google/protobuf/timestamp.proto";

message ConnectionEvent {
  enum Status {
    UNKNOWN = 0;
    ONLINE = 1;
    OFFLINE = 2;
  }
  
  int64 device_id = 1;
  string unique_id = 2;
  Status status = 3;
  google.protobuf.Timestamp timestamp = 4;
  string protocol = 5;
  string remote_address = 6;
}
```

### Schema Versioning Strategy

The Protocol Service implements a schema versioning strategy to ensure backward compatibility:

1. **Schema Registry**: All message schemas are registered in a central schema registry
2. **Versioning**: Schema versions are included in message headers
3. **Compatibility**: New schema versions maintain backward compatibility
4. **Evolution**: Fields can be added but not removed or changed in type

## Producer Configuration

### Kafka Producer Configuration

The Protocol Service configures Kafka producers with the following settings:

```yaml
kafka:
  bootstrap.servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
  client.id: protocol-service-${HOSTNAME:local}
  key.serializer: org.apache.kafka.common.serialization.StringSerializer
  value.serializer: org.traccar.kafka.ProtobufSerializer
  acks: all                    # Ensures data is replicated before acknowledgment
  retries: 3                   # Number of retry attempts
  retry.backoff.ms: 100        # Delay between retries
  max.in.flight.requests.per.connection: 1  # Ensures ordering during retries
  enable.idempotence: true     # Prevents duplicate messages
  compression.type: lz4        # Efficient compression for position data
  batch.size: 16384            # Batch size in bytes
  linger.ms: 5                 # Slight delay to improve batching
  buffer.memory: 33554432      # 32MB producer buffer
```

### RabbitMQ Producer Configuration

The Protocol Service configures RabbitMQ producers with the following settings:

```yaml
rabbitmq:
  host: ${RABBITMQ_HOST:localhost}
  port: ${RABBITMQ_PORT:5672}
  username: ${RABBITMQ_USERNAME:guest}
  password: ${RABBITMQ_PASSWORD:guest}
  virtual-host: ${RABBITMQ_VHOST:/}
  connection-timeout: 5000
  publisher-confirms: true      # Enables publisher confirmation mode
  publisher-returns: true       # Enables returned message callbacks
  mandatory: true               # Messages must be routable
  delivery-mode: 2              # Persistent messages
```

## Message Partitioning

The Protocol Service implements device ID-based partitioning to ensure that messages from the same device are processed in order by downstream services.

### Kafka Partitioning

For Kafka, the device ID is used as the message key, which ensures that all messages for a specific device are sent to the same partition:

```java
ProducerRecord<String, PositionMessage> record = new ProducerRecord<>(
    "raw.positions",
    String.valueOf(position.getDeviceId()),  // Key (device ID as string)
    positionMessage                          // Value (position message)
);
```

This approach ensures that:

1. Messages from the same device are processed in order
2. Load is distributed evenly across partitions
3. Parallel processing is possible for different devices

### RabbitMQ Routing

For RabbitMQ, the Protocol Service uses a combination of exchanges and routing keys:

```java
channel.basicPublish(
    "positions.exchange",                    // Exchange name
    String.valueOf(position.getDeviceId()),  // Routing key (device ID)
    properties,                              // Message properties
    messageBytes                             // Serialized message
);
```

## Delivery Guarantees

The Protocol Service provides the following delivery guarantees:

### At-Least-Once Delivery

The default configuration ensures at-least-once delivery semantics, meaning that messages will be delivered to the broker at least once, but may be delivered multiple times in failure scenarios. Downstream services should implement idempotent processing to handle potential duplicates.

### Synchronous Acknowledgment

The Protocol Service waits for acknowledgment from the broker before considering a message successfully published. This ensures that messages are not lost due to broker unavailability.

### Configurable Retry Policy

The retry policy is configurable through the service configuration:

```yaml
protocol:
  broker:
    retry:
      max-attempts: 3           # Maximum number of retry attempts
      initial-interval: 100      # Initial retry interval in milliseconds
      multiplier: 2.0           # Backoff multiplier for subsequent retries
      max-interval: 1000        # Maximum retry interval in milliseconds
```

## Error Handling

The Protocol Service implements comprehensive error handling for broker integration:

### Transient Failures

Transient failures (e.g., network issues, broker unavailability) are handled through the retry mechanism with exponential backoff.

### Persistent Failures

If a message cannot be published after all retry attempts, the Protocol Service:

1. Logs the error with detailed context
2. Increments failure metrics for monitoring
3. Optionally stores the failed message in a local buffer for later retry

### Dead Letter Queue

Messages that cannot be published after all retries can be sent to a dead letter queue for later analysis and manual reprocessing:

```yaml
protocol:
  broker:
    dead-letter:
      enabled: true
      topic: "dead.letter.queue"
```

## Monitoring and Metrics

The Protocol Service exposes the following metrics for monitoring broker integration:

| Metric Name | Type | Description |
|-------------|------|-------------|
| `protocol_broker_messages_published_total` | Counter | Total number of messages published to the broker |
| `protocol_broker_messages_failed_total` | Counter | Total number of messages that failed to publish |
| `protocol_broker_publish_latency_ms` | Histogram | Latency of message publication operations |
| `protocol_broker_retry_count` | Histogram | Distribution of retry attempts per message |

These metrics are exposed through the service's Prometheus endpoint at `/metrics`.

## Configuration Reference

### Common Configuration

```yaml
protocol:
  broker:
    enabled: true                # Enable/disable broker integration
    type: kafka                  # Broker type: kafka or rabbitmq
    topic:
      positions: raw.positions   # Topic for position messages
      connections: device.connections  # Topic for connection events
    batch:
      enabled: true              # Enable message batching
      size: 100                  # Maximum batch size
      interval-ms: 100           # Maximum batch interval
```

### Kafka-Specific Configuration

```yaml
protocol:
  broker:
    kafka:
      bootstrap-servers: localhost:9092
      client-id: protocol-service
      acks: all
      compression-type: lz4
      # Additional Kafka-specific settings
```

### RabbitMQ-Specific Configuration

```yaml
protocol:
  broker:
    rabbitmq:
      host: localhost
      port: 5672
      username: guest
      password: guest
      virtual-host: /
      # Additional RabbitMQ-specific settings
```

## Implementation Details

The Protocol Service implements broker integration through the following components:

### Message Publisher Interface

```java
public interface MessagePublisher {
    CompletableFuture<Void> publishPosition(Position position);
    CompletableFuture<Void> publishConnectionEvent(long deviceId, String uniqueId, boolean connected);
    void close();
}
```

### Kafka Implementation

```java
public class KafkaMessagePublisher implements MessagePublisher {
    private final KafkaProducer<String, Object> producer;
    private final String positionsTopic;
    private final String connectionsTopic;
    
    // Implementation details
}
```

### RabbitMQ Implementation

```java
public class RabbitMQMessagePublisher implements MessagePublisher {
    private final Connection connection;
    private final Channel channel;
    private final String positionsExchange;
    private final String connectionsExchange;
    
    // Implementation details
}
```

### Integration with Protocol Decoders

The Protocol Service integrates the message publisher with the protocol decoders through the `BaseProtocolDecoder` class:

```java
public abstract class BaseProtocolDecoder extends ExtendedObjectDecoder {
    
    @Inject
    private MessagePublisher messagePublisher;
    
    @Override
    protected void onMessageEvent(
            Channel channel, SocketAddress remoteAddress, 
            Object originalMessage, Object decodedMessage) {
        
        // Process decoded positions
        if (decodedMessage instanceof Position position) {
            messagePublisher.publishPosition(position);
        } else if (decodedMessage instanceof Collection) {
            Collection<Position> positions = (Collection) decodedMessage;
            for (Position position : positions) {
                messagePublisher.publishPosition(position);
            }
        }
        
        // Update device connection status
        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress);
        if (deviceSession != null) {
            messagePublisher.publishConnectionEvent(
                deviceSession.getDeviceId(), 
                deviceSession.getDeviceUniqueId(), 
                true);
        }
    }
}
```

## Conclusion

The Protocol Service's message broker integration provides a reliable, configurable, and scalable mechanism for publishing decoded position data and device connection events to downstream services. By leveraging industry-standard message brokers like Kafka and RabbitMQ, the service ensures high throughput, fault tolerance, and flexible deployment options.