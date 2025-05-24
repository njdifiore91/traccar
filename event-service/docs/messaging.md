# Event Processing Service - Messaging Integration

## Overview

This document describes how the Event Processing Service integrates with the message broker to consume position data, detect events, and publish those events to other services in the Traccar ecosystem. The service follows an event-driven architecture pattern, using asynchronous messaging for reliable and scalable event processing.

## Message Broker Integration

The Event Processing Service communicates with other microservices primarily through a message broker. This approach provides several benefits:

- **Loose coupling**: Services can evolve independently without direct dependencies
- **Scalability**: The service can scale horizontally to handle increased load
- **Resilience**: Message persistence ensures no data loss during service outages
- **Asynchronous processing**: Events can be processed without blocking other operations

## Topics and Channels

The Event Processing Service interacts with the following message broker topics:

| Topic Name | Direction | Purpose |
|------------|-----------|----------|
| `enriched-positions` | Consume | Receives enriched position data from the Position Processing Service |
| `events` | Produce | Publishes detected events for consumption by other services (e.g., Notification Service) |
| `events-dlq` | Produce | Dead letter queue for messages that couldn't be processed after retries |

## Message Formats

All messages exchanged through the message broker use Protocol Buffers (protobuf) for serialization. Protocol Buffers provide several advantages over text-based formats like JSON:

- Smaller message size (binary format)
- Faster serialization/deserialization
- Strongly typed schemas
- Language-neutral format
- Forward and backward compatibility

### Position Message Schema

The Event Processing Service consumes position messages with the following protobuf schema (simplified):

```protobuf
syntax = "proto3";
package org.traccar.proto;

import "google/protobuf/timestamp.proto";

message Position {
  int64 id = 1;
  int64 device_id = 2;
  google.protobuf.Timestamp timestamp = 3;
  double latitude = 4;
  double longitude = 5;
  double altitude = 6;
  double speed = 7;
  double course = 8;
  map<string, string> attributes = 9;
  // Additional fields omitted for brevity
}
```

### Event Message Schema

The Event Processing Service produces event messages with the following protobuf schema (simplified):

```protobuf
syntax = "proto3";
package org.traccar.proto;

import "google/protobuf/timestamp.proto";
import "position.proto";

message Event {
  int64 id = 1;
  string type = 2;
  int64 device_id = 3;
  google.protobuf.Timestamp timestamp = 4;
  Position position = 5;
  map<string, string> attributes = 6;
  // Additional fields omitted for brevity
}
```

## Consumer Configuration

The Event Processing Service consumes messages from the `enriched-positions` topic with the following configuration:

### Consumer Group

The service uses a consumer group named `event-service` to enable horizontal scaling. Multiple instances of the Event Processing Service can be deployed to handle increased load, with the message broker distributing messages among the instances.

### Partition Assignment

Messages are partitioned by `device_id` to ensure that all positions for a specific device are processed by the same service instance. This maintains the correct order of position processing for each device.

### Offset Management

The service uses automatic offset commits with a commit interval of 5 seconds. This ensures that messages are not reprocessed in case of service restart, while also providing reasonable performance.

## Producer Configuration

The Event Processing Service produces messages to the `events` topic with the following configuration:

### Message Keys

Event messages use the `device_id` as the message key to ensure that all events for a specific device are sent to the same partition. This helps maintain ordering for consumers that need to process events in sequence.

### Delivery Guarantees

The service uses an "at-least-once" delivery guarantee with acknowledgments from the broker. This ensures that no events are lost, although it may result in duplicate events in rare failure scenarios.

## Message Processing Flow

1. The Event Processing Service consumes enriched position messages from the `enriched-positions` topic
2. For each position, the service runs it through all registered event handlers (geofence, overspeed, motion, etc.)
3. When an event is detected, it is published to the `events` topic
4. The service acknowledges the position message after successful processing

## Error Handling

The Event Processing Service implements robust error handling to ensure reliable message processing:

### Retry Mechanism

When a transient error occurs during message processing, the service implements an exponential backoff retry strategy:

- First retry: 1 second delay
- Second retry: 2 seconds delay
- Third retry: 4 seconds delay
- Maximum retries: 3

### Dead Letter Queue

If a message cannot be processed after the maximum number of retries, it is sent to the `events-dlq` topic with the original message payload and additional metadata:

```json
{
  "original_topic": "enriched-positions",
  "error_message": "Detailed error description",
  "timestamp": "2023-06-01T12:34:56Z",
  "retry_count": 3
}
```

### Error Monitoring

All processing errors are logged with correlation IDs to enable tracing through the system. The service exposes metrics for monitoring error rates and DLQ message counts.

## Horizontal Scaling

The Event Processing Service is designed for horizontal scalability:

1. Multiple instances can be deployed to handle increased load
2. The message broker distributes messages among instances based on partitions
3. Each instance processes a subset of devices, determined by partition assignment
4. Scaling up or down is handled gracefully with partition rebalancing

## Implementation Details

The Event Processing Service uses the following components for message broker integration:

### Message Consumer

The service implements a message consumer that:

1. Deserializes position messages from Protocol Buffers format
2. Passes positions to the appropriate event handlers
3. Manages acknowledgments and error handling

### Event Handlers

Event handlers implement the `BaseEventHandler` interface and use the callback mechanism to report detected events:

```java
public abstract class BaseEventHandler {
    public interface Callback {
        void eventDetected(Event event);
    }
    
    public void analyzePosition(Position position, Callback callback) {
        try {
            onPosition(position, callback);
        } catch (RuntimeException e) {
            // Error handling
        }
    }
    
    public abstract void onPosition(Position position, Callback callback);
}
```

### Message Producer

When an event is detected, the service:

1. Serializes the event to Protocol Buffers format
2. Publishes it to the `events` topic with the appropriate headers
3. Ensures successful delivery with acknowledgments

## Configuration Properties

The following configuration properties control the message broker integration:

```yaml
event-service:
  messaging:
    consumer:
      topic: enriched-positions
      group-id: event-service
      auto-offset-reset: earliest
      max-poll-records: 500
      session-timeout-ms: 30000
    producer:
      topic: events
      acks: all
      retries: 3
      batch-size: 16384
      linger-ms: 5
    error:
      retry-max-attempts: 3
      retry-backoff-ms: 1000
      dlq-topic: events-dlq
```

## Monitoring and Observability

The Event Processing Service exposes the following metrics for monitoring its messaging integration:

- `messages.consumed.total`: Total number of position messages consumed
- `messages.consumed.rate`: Rate of position message consumption
- `events.produced.total`: Total number of events produced
- `events.produced.rate`: Rate of event production
- `processing.errors.total`: Total number of processing errors
- `processing.errors.rate`: Rate of processing errors
- `dlq.messages.total`: Total number of messages sent to DLQ
- `consumer.lag`: Lag between latest message and consumer position

## Troubleshooting

### Common Issues

1. **High consumer lag**: Check if the service has enough resources or if it needs to be scaled horizontally
2. **Increasing DLQ messages**: Investigate the error patterns in logs and address the root cause
3. **Message deserialization errors**: Ensure schema compatibility between producers and consumers
4. **Slow processing**: Check for performance bottlenecks in event handlers or database operations

### Debugging Tools

- Use the message broker's admin tools to inspect topics, consumer groups, and message contents
- Enable DEBUG logging for detailed message processing information
- Use distributed tracing to follow messages through the system