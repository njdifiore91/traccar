# Event Processing Service API Reference

## Overview

The Event Processing Service is responsible for analyzing position data to detect significant events such as geofence transitions, speed violations, device status changes, and maintenance alerts. This document provides a comprehensive reference for the service's REST and gRPC interfaces.

## Table of Contents

- [Authentication](#authentication)
- [REST API Endpoints](#rest-api-endpoints)
  - [Health and Metrics](#health-and-metrics)
  - [Event Management](#event-management)
  - [Event Rules](#event-rules)
  - [Event History](#event-history)
- [gRPC Interfaces](#grpc-interfaces)
  - [Event Service](#event-service)
  - [Rule Service](#rule-service)
- [Message Broker Integration](#message-broker-integration)
  - [Consumed Topics](#consumed-topics)
  - [Published Topics](#published-topics)
  - [Message Formats](#message-formats)
- [Error Handling](#error-handling)
  - [HTTP Status Codes](#http-status-codes)
  - [Error Response Format](#error-response-format)
  - [Common Error Codes](#common-error-codes)
- [Examples](#examples)
  - [REST API Examples](#rest-api-examples)
  - [gRPC Client Examples](#grpc-client-examples)

## Authentication

All API requests to the Event Processing Service require authentication. The service supports two authentication methods:

### Service-to-Service Authentication

For internal service-to-service communication, mutual TLS (mTLS) authentication is used. Each service must present a valid client certificate that is verified against the trusted certificate authority.

### API Gateway Authentication

External requests coming through the API Gateway use JWT token authentication. The API Gateway validates the JWT token and forwards the authenticated request to the Event Processing Service with appropriate user context headers.

Required headers for forwarded requests:

```
X-User-ID: <user identifier>
X-User-Role: <user role>
Authorization: Bearer <JWT token>
```

## REST API Endpoints

### Health and Metrics

#### GET /health/liveness

Checks if the service is running.

**Response:**

```json
{
  "status": "UP"
}
```

#### GET /health/readiness

Checks if the service is ready to accept requests.

**Response:**

```json
{
  "status": "UP",
  "components": {
    "broker": {
      "status": "UP"
    },
    "database": {
      "status": "UP"
    }
  }
}
```

#### GET /actuator/metrics

Returns service metrics in Prometheus format.

**Response:**

Prometheus-formatted metrics text.

### Event Management

#### GET /api/v1/events

Returns a list of events based on the provided filters.

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| deviceId | long | Filter events by device ID |
| type | string | Filter events by type (e.g., "geofenceEnter", "deviceOverspeed") |
| from | ISO date | Start date for event filtering |
| to | ISO date | End date for event filtering |
| page | integer | Page number for pagination (default: 0) |
| size | integer | Page size for pagination (default: 20) |

**Response:**

```json
{
  "content": [
    {
      "id": 123,
      "type": "geofenceEnter",
      "eventTime": "2023-06-15T14:30:00Z",
      "deviceId": 456,
      "positionId": 789,
      "geofenceId": 101,
      "maintenanceId": null,
      "attributes": {
        "speed": 35.5,
        "geofenceName": "Warehouse Zone"
      }
    }
  ],
  "page": 0,
  "size": 20,
  "totalElements": 1,
  "totalPages": 1
}
```

#### GET /api/v1/events/{id}

Returns a specific event by ID.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| id | long | Event ID |

**Response:**

```json
{
  "id": 123,
  "type": "geofenceEnter",
  "eventTime": "2023-06-15T14:30:00Z",
  "deviceId": 456,
  "positionId": 789,
  "geofenceId": 101,
  "maintenanceId": null,
  "attributes": {
    "speed": 35.5,
    "geofenceName": "Warehouse Zone"
  }
}
```

#### POST /api/v1/events

Manually creates a new event.

**Request Body:**

```json
{
  "type": "customEvent",
  "deviceId": 456,
  "attributes": {
    "message": "Custom event triggered by user"
  }
}
```

**Response:**

```json
{
  "id": 124,
  "type": "customEvent",
  "eventTime": "2023-06-15T15:45:00Z",
  "deviceId": 456,
  "positionId": null,
  "geofenceId": null,
  "maintenanceId": null,
  "attributes": {
    "message": "Custom event triggered by user"
  }
}
```

### Event Rules

#### GET /api/v1/rules

Returns a list of event detection rules.

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| deviceId | long | Filter rules by device ID |
| type | string | Filter rules by event type |
| page | integer | Page number for pagination (default: 0) |
| size | integer | Page size for pagination (default: 20) |

**Response:**

```json
{
  "content": [
    {
      "id": 201,
      "deviceId": 456,
      "type": "deviceOverspeed",
      "enabled": true,
      "description": "Speed limit alert",
      "conditions": {
        "speedThreshold": 80.0
      },
      "notificationEnabled": true
    }
  ],
  "page": 0,
  "size": 20,
  "totalElements": 1,
  "totalPages": 1
}
```

#### GET /api/v1/rules/{id}

Returns a specific rule by ID.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| id | long | Rule ID |

**Response:**

```json
{
  "id": 201,
  "deviceId": 456,
  "type": "deviceOverspeed",
  "enabled": true,
  "description": "Speed limit alert",
  "conditions": {
    "speedThreshold": 80.0
  },
  "notificationEnabled": true
}
```

#### POST /api/v1/rules

Creates a new event detection rule.

**Request Body:**

```json
{
  "deviceId": 456,
  "type": "geofenceExit",
  "enabled": true,
  "description": "Exit from warehouse zone",
  "conditions": {
    "geofenceId": 101
  },
  "notificationEnabled": true
}
```

**Response:**

```json
{
  "id": 202,
  "deviceId": 456,
  "type": "geofenceExit",
  "enabled": true,
  "description": "Exit from warehouse zone",
  "conditions": {
    "geofenceId": 101
  },
  "notificationEnabled": true
}
```

#### PUT /api/v1/rules/{id}

Updates an existing event detection rule.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| id | long | Rule ID |

**Request Body:**

```json
{
  "deviceId": 456,
  "type": "geofenceExit",
  "enabled": false,
  "description": "Exit from warehouse zone (disabled)",
  "conditions": {
    "geofenceId": 101
  },
  "notificationEnabled": false
}
```

**Response:**

```json
{
  "id": 202,
  "deviceId": 456,
  "type": "geofenceExit",
  "enabled": false,
  "description": "Exit from warehouse zone (disabled)",
  "conditions": {
    "geofenceId": 101
  },
  "notificationEnabled": false
}
```

#### DELETE /api/v1/rules/{id}

Deletes an event detection rule.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| id | long | Rule ID |

**Response:**

```
204 No Content
```

### Event History

#### GET /api/v1/devices/{deviceId}/events

Returns event history for a specific device.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| deviceId | long | Device ID |

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| type | string | Filter events by type |
| from | ISO date | Start date for event filtering |
| to | ISO date | End date for event filtering |
| page | integer | Page number for pagination (default: 0) |
| size | integer | Page size for pagination (default: 20) |

**Response:**

```json
{
  "content": [
    {
      "id": 123,
      "type": "geofenceEnter",
      "eventTime": "2023-06-15T14:30:00Z",
      "deviceId": 456,
      "positionId": 789,
      "geofenceId": 101,
      "maintenanceId": null,
      "attributes": {
        "speed": 35.5,
        "geofenceName": "Warehouse Zone"
      }
    }
  ],
  "page": 0,
  "size": 20,
  "totalElements": 1,
  "totalPages": 1
}
```

## gRPC Interfaces

The Event Processing Service exposes gRPC interfaces for efficient service-to-service communication. The service definition files (.proto) are available in the `src/main/proto` directory.

### Event Service

```protobuf
service EventService {
  // Get events by filter criteria
  rpc GetEvents(EventRequest) returns (EventResponse);
  
  // Get a specific event by ID
  rpc GetEvent(EventIdRequest) returns (Event);
  
  // Create a new event
  rpc CreateEvent(CreateEventRequest) returns (Event);
  
  // Process a position update and detect events
  rpc ProcessPosition(Position) returns (ProcessPositionResponse);
}
```

#### Message Definitions

```protobuf
message EventRequest {
  optional int64 device_id = 1;
  optional string type = 2;
  optional google.protobuf.Timestamp from = 3;
  optional google.protobuf.Timestamp to = 4;
  int32 page = 5;
  int32 size = 6;
}

message EventResponse {
  repeated Event events = 1;
  int32 page = 2;
  int32 size = 3;
  int64 total_elements = 4;
  int32 total_pages = 5;
}

message EventIdRequest {
  int64 id = 1;
}

message Event {
  int64 id = 1;
  string type = 2;
  google.protobuf.Timestamp event_time = 3;
  int64 device_id = 4;
  optional int64 position_id = 5;
  optional int64 geofence_id = 6;
  optional int64 maintenance_id = 7;
  map<string, google.protobuf.Value> attributes = 8;
}

message CreateEventRequest {
  string type = 1;
  int64 device_id = 2;
  optional int64 position_id = 3;
  optional int64 geofence_id = 4;
  optional int64 maintenance_id = 5;
  map<string, google.protobuf.Value> attributes = 6;
}

message Position {
  int64 id = 1;
  int64 device_id = 2;
  google.protobuf.Timestamp timestamp = 3;
  double latitude = 4;
  double longitude = 5;
  optional double altitude = 6;
  optional double speed = 7;
  optional double course = 8;
  optional int32 satellites = 9;
  map<string, google.protobuf.Value> attributes = 10;
}

message ProcessPositionResponse {
  repeated Event detected_events = 1;
}
```

### Rule Service

```protobuf
service RuleService {
  // Get rules by filter criteria
  rpc GetRules(RuleRequest) returns (RuleResponse);
  
  // Get a specific rule by ID
  rpc GetRule(RuleIdRequest) returns (Rule);
  
  // Create a new rule
  rpc CreateRule(CreateRuleRequest) returns (Rule);
  
  // Update an existing rule
  rpc UpdateRule(UpdateRuleRequest) returns (Rule);
  
  // Delete a rule
  rpc DeleteRule(RuleIdRequest) returns (google.protobuf.Empty);
}
```

#### Message Definitions

```protobuf
message RuleRequest {
  optional int64 device_id = 1;
  optional string type = 2;
  int32 page = 3;
  int32 size = 4;
}

message RuleResponse {
  repeated Rule rules = 1;
  int32 page = 2;
  int32 size = 3;
  int64 total_elements = 4;
  int32 total_pages = 5;
}

message RuleIdRequest {
  int64 id = 1;
}

message Rule {
  int64 id = 1;
  int64 device_id = 2;
  string type = 3;
  bool enabled = 4;
  string description = 5;
  map<string, google.protobuf.Value> conditions = 6;
  bool notification_enabled = 7;
}

message CreateRuleRequest {
  int64 device_id = 1;
  string type = 2;
  bool enabled = 3;
  string description = 4;
  map<string, google.protobuf.Value> conditions = 5;
  bool notification_enabled = 6;
}

message UpdateRuleRequest {
  int64 id = 1;
  int64 device_id = 2;
  string type = 3;
  bool enabled = 4;
  string description = 5;
  map<string, google.protobuf.Value> conditions = 6;
  bool notification_enabled = 7;
}
```

## Message Broker Integration

The Event Processing Service integrates with the message broker (Kafka/RabbitMQ) for asynchronous communication with other services.

### Consumed Topics

#### enriched-positions

The service consumes enriched position data from this topic to detect events based on position updates.

**Message Format:**

```json
{
  "id": 789,
  "deviceId": 456,
  "timestamp": "2023-06-15T14:29:55Z",
  "latitude": 37.7749,
  "longitude": -122.4194,
  "altitude": 10.5,
  "speed": 35.5,
  "course": 180.0,
  "satellites": 8,
  "attributes": {
    "batteryLevel": 85,
    "ignition": true,
    "address": "123 Main St, San Francisco, CA"
  }
}
```

#### command-results

The service consumes command execution results to generate command-related events.

**Message Format:**

```json
{
  "deviceId": 456,
  "commandId": 301,
  "type": "engineStop",
  "success": true,
  "timestamp": "2023-06-15T15:00:00Z",
  "attributes": {
    "responseMessage": "Engine stopped successfully"
  }
}
```

### Published Topics

#### events

The service publishes detected events to this topic for consumption by other services (e.g., Notification Service).

**Message Format:**

```json
{
  "id": 123,
  "type": "geofenceEnter",
  "eventTime": "2023-06-15T14:30:00Z",
  "deviceId": 456,
  "positionId": 789,
  "geofenceId": 101,
  "maintenanceId": null,
  "attributes": {
    "speed": 35.5,
    "geofenceName": "Warehouse Zone"
  }
}
```

### Message Formats

All messages exchanged via the message broker use Protocol Buffers for efficient serialization. The message definitions are available in the `src/main/proto` directory.

## Error Handling

### HTTP Status Codes

The REST API uses standard HTTP status codes to indicate the success or failure of requests:

| Status Code | Description |
|-------------|-------------|
| 200 OK | The request was successful |
| 201 Created | A new resource was created successfully |
| 204 No Content | The request was successful but there is no content to return |
| 400 Bad Request | The request was invalid or cannot be served |
| 401 Unauthorized | Authentication is required or failed |
| 403 Forbidden | The authenticated user does not have permission to access the resource |
| 404 Not Found | The requested resource does not exist |
| 409 Conflict | The request conflicts with the current state of the server |
| 500 Internal Server Error | An error occurred on the server |

### Error Response Format

When an error occurs, the API returns a standardized error response:

```json
{
  "timestamp": "2023-06-15T16:00:00Z",
  "status": 400,
  "error": "Bad Request",
  "message": "Invalid event type",
  "path": "/api/v1/events",
  "code": "EVENT_INVALID_TYPE",
  "details": {
    "allowedTypes": ["geofenceEnter", "geofenceExit", "deviceOverspeed"]
  }
}
```

### Common Error Codes

| Error Code | Description |
|------------|-------------|
| EVENT_NOT_FOUND | The specified event was not found |
| EVENT_INVALID_TYPE | The event type is invalid |
| RULE_NOT_FOUND | The specified rule was not found |
| RULE_INVALID_TYPE | The rule type is invalid |
| RULE_INVALID_CONDITION | The rule condition is invalid |
| DEVICE_NOT_FOUND | The specified device was not found |
| GEOFENCE_NOT_FOUND | The specified geofence was not found |
| MAINTENANCE_NOT_FOUND | The specified maintenance was not found |
| INVALID_DATE_RANGE | The specified date range is invalid |
| UNAUTHORIZED_ACCESS | The user does not have permission to access the resource |
| INTERNAL_ERROR | An internal server error occurred |

## Examples

### REST API Examples

#### Retrieving Events for a Device

**Request:**

```bash
curl -X GET \
  'https://api.example.com/api/v1/devices/456/events?from=2023-06-15T00:00:00Z&to=2023-06-16T00:00:00Z' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...' \
  -H 'Content-Type: application/json'
```

**Response:**

```json
{
  "content": [
    {
      "id": 123,
      "type": "geofenceEnter",
      "eventTime": "2023-06-15T14:30:00Z",
      "deviceId": 456,
      "positionId": 789,
      "geofenceId": 101,
      "maintenanceId": null,
      "attributes": {
        "speed": 35.5,
        "geofenceName": "Warehouse Zone"
      }
    },
    {
      "id": 124,
      "type": "deviceOverspeed",
      "eventTime": "2023-06-15T15:45:00Z",
      "deviceId": 456,
      "positionId": 790,
      "geofenceId": null,
      "maintenanceId": null,
      "attributes": {
        "speed": 85.2,
        "speedLimit": 80.0
      }
    }
  ],
  "page": 0,
  "size": 20,
  "totalElements": 2,
  "totalPages": 1
}
```

#### Creating a New Event Rule

**Request:**

```bash
curl -X POST \
  'https://api.example.com/api/v1/rules' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...' \
  -H 'Content-Type: application/json' \
  -d '{
    "deviceId": 456,
    "type": "deviceOverspeed",
    "enabled": true,
    "description": "Highway speed limit alert",
    "conditions": {
      "speedThreshold": 120.0
    },
    "notificationEnabled": true
  }'
```

**Response:**

```json
{
  "id": 203,
  "deviceId": 456,
  "type": "deviceOverspeed",
  "enabled": true,
  "description": "Highway speed limit alert",
  "conditions": {
    "speedThreshold": 120.0
  },
  "notificationEnabled": true
}
```

### gRPC Client Examples

#### Java Client Example

```java
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.traccar.event.EventServiceGrpc;
import org.traccar.event.EventRequest;
import org.traccar.event.EventResponse;
import com.google.protobuf.Timestamp;

public class EventServiceClient {
    public static void main(String[] args) {
        // Create a channel to the server
        ManagedChannel channel = ManagedChannelBuilder
            .forAddress("event-service.example.com", 9090)
            .usePlaintext() // For testing only, use TLS in production
            .build();
        
        try {
            // Create a stub
            EventServiceGrpc.EventServiceBlockingStub stub = EventServiceGrpc.newBlockingStub(channel);
            
            // Build request
            Timestamp from = Timestamp.newBuilder()
                .setSeconds(1623715200) // 2023-06-15T00:00:00Z
                .build();
            
            Timestamp to = Timestamp.newBuilder()
                .setSeconds(1623801600) // 2023-06-16T00:00:00Z
                .build();
            
            EventRequest request = EventRequest.newBuilder()
                .setDeviceId(456)
                .setFrom(from)
                .setTo(to)
                .setPage(0)
                .setSize(20)
                .build();
            
            // Call the service
            EventResponse response = stub.getEvents(request);
            
            // Process the response
            System.out.println("Received " + response.getEventsCount() + " events");
            response.getEventsList().forEach(event -> {
                System.out.println("Event ID: " + event.getId());
                System.out.println("Type: " + event.getType());
                System.out.println("Time: " + event.getEventTime());
                System.out.println("---");
            });
        } finally {
            // Shutdown the channel
            channel.shutdown();
        }
    }
}
```

#### Python Client Example

```python
import grpc
import event_pb2
import event_pb2_grpc
from google.protobuf.timestamp_pb2 import Timestamp
from datetime import datetime

def timestamp_from_datetime(dt):
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts

# Create a channel
channel = grpc.insecure_channel('event-service.example.com:9090')  # Use secure channel in production

# Create a stub
stub = event_pb2_grpc.EventServiceStub(channel)

# Build request
from_date = timestamp_from_datetime(datetime(2023, 6, 15))
to_date = timestamp_from_datetime(datetime(2023, 6, 16))

request = event_pb2.EventRequest(
    device_id=456,
    from_=from_date,
    to=to_date,
    page=0,
    size=20
)

# Call the service
response = stub.GetEvents(request)

# Process the response
print(f"Received {len(response.events)} events")
for event in response.events:
    print(f"Event ID: {event.id}")
    print(f"Type: {event.type}")
    print(f"Time: {event.event_time.ToDatetime()}")
    print("---")

# Close the channel
channel.close()
```