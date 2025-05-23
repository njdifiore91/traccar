# Position Processing Service API Specification

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [gRPC Service Definitions](#grpc-service-definitions)
4. [REST API Endpoints](#rest-api-endpoints)
5. [Data Models](#data-models)
6. [Error Handling](#error-handling)
7. [Rate Limiting](#rate-limiting)
8. [Examples](#examples)

## Overview

The Position Processing Service is responsible for processing and enriching raw GPS position data received from the Protocol Service. It performs various operations including geocoding, geolocation, distance calculation, motion detection, and other contextual information enrichment.

This document specifies the API interfaces for interacting with the Position Processing Service, including both gRPC and REST endpoints.

### Service Responsibilities

- Processing raw position data from GPS devices
- Enriching position data with additional context (geocoding, geofencing, etc.)
- Calculating derived metrics (distance, speed, etc.)
- Detecting motion states
- Providing access to current and historical position data

### Communication Patterns

The Position Processing Service supports two primary communication patterns:

1. **Asynchronous Event-Driven Messaging**: The service consumes raw position data from the Protocol Service via a message broker (Kafka/RabbitMQ) and publishes enriched position data for consumption by the Event Processing Service.

2. **Synchronous API Calls**: The service exposes gRPC and REST endpoints for direct access to position data and processing capabilities.

## Authentication

All API requests to the Position Processing Service must be authenticated. The service supports the following authentication methods:

### Service-to-Service Authentication

For internal service-to-service communication, the Position Processing Service uses mutual TLS (mTLS) authentication. Services must present valid certificates issued by the organization's certificate authority.

Additionally, service-to-service calls require a JWT token in the authorization header:

```
Authorization: Bearer <jwt_token>
```

The JWT token must include the following claims:
- `sub`: Service identifier
- `iss`: Issuing authority
- `exp`: Expiration time
- `iat`: Issued at time
- `aud`: "position-service"

### API Gateway Authentication

Requests coming through the API Gateway will have already been authenticated. The API Gateway will include the authenticated user's information in the request headers:

```
X-User-ID: <user_id>
X-User-Role: <user_role>
```

## gRPC Service Definitions

The Position Processing Service exposes the following gRPC services:

### PositionService

```protobuf
syntax = "proto3";

package org.traccar.position;

import "google/protobuf/timestamp.proto";
import "google/protobuf/empty.proto";

service PositionService {
  // Get the latest position for a device
  rpc GetLatestPosition(DeviceIdRequest) returns (Position);
  
  // Get positions for a device within a time range
  rpc GetPositions(PositionRequest) returns (PositionResponse);
  
  // Stream positions for a device in real-time
  rpc StreamPositions(DeviceIdRequest) returns (stream Position);
  
  // Process a raw position (primarily used by Protocol Service)
  rpc ProcessPosition(RawPosition) returns (Position);
  
  // Send a command to a device
  rpc SendCommand(DeviceCommand) returns (CommandResponse);
  
  // Get the current status of a device
  rpc GetDeviceStatus(DeviceIdRequest) returns (DeviceStatus);
  
  // Calculate distance between positions
  rpc CalculateDistance(DistanceRequest) returns (DistanceResponse);
}

message DeviceIdRequest {
  int64 device_id = 1;
}

message PositionRequest {
  int64 device_id = 1;
  google.protobuf.Timestamp from = 2;
  google.protobuf.Timestamp to = 3;
  int32 limit = 4;
}

message PositionResponse {
  repeated Position positions = 1;
}

message Position {
  int64 id = 1;
  int64 device_id = 2;
  string protocol = 3;
  google.protobuf.Timestamp device_time = 4;
  google.protobuf.Timestamp fix_time = 5;
  google.protobuf.Timestamp server_time = 6;
  bool outdated = 7;
  bool valid = 8;
  double latitude = 9;
  double longitude = 10;
  double altitude = 11; // meters
  double speed = 12; // knots
  double course = 13; // degrees
  string address = 14;
  double accuracy = 15; // meters
  map<string, string> attributes = 16;
}

message RawPosition {
  int64 device_id = 1;
  string protocol = 2;
  google.protobuf.Timestamp device_time = 3;
  double latitude = 4;
  double longitude = 5;
  double altitude = 6; // meters
  double speed = 7; // knots
  double course = 8; // degrees
  map<string, string> attributes = 9;
}

message DeviceCommand {
  int64 device_id = 1;
  string type = 2;
  map<string, string> attributes = 3;
}

message CommandResponse {
  bool success = 1;
  string status = 2;
  string message = 3;
}

message DeviceStatus {
  int64 device_id = 1;
  string status = 2; // online, offline, unknown
  google.protobuf.Timestamp last_update = 3;
  Position last_position = 4;
  map<string, string> attributes = 5;
}

message DistanceRequest {
  repeated Position positions = 1;
  bool use_altitude = 2;
}

message DistanceResponse {
  double distance = 1; // meters
}
```

### GeofenceService

```protobuf
syntax = "proto3";

package org.traccar.position;

import "google/protobuf/empty.proto";

service GeofenceService {
  // Check if a position is within any geofences
  rpc CheckGeofences(GeofenceCheckRequest) returns (GeofenceCheckResponse);
  
  // Get all geofences for a device
  rpc GetDeviceGeofences(DeviceIdRequest) returns (GeofenceList);
}

message GeofenceCheckRequest {
  Position position = 1;
  repeated int64 geofence_ids = 2; // Optional: if empty, checks all geofences for the device
}

message GeofenceCheckResponse {
  repeated int64 matching_geofence_ids = 1;
}

message GeofenceList {
  repeated Geofence geofences = 1;
}

message Geofence {
  int64 id = 1;
  string name = 2;
  string description = 3;
  string area = 4; // WKT format
  map<string, string> attributes = 5;
}
```

### EnrichmentService

```protobuf
syntax = "proto3";

package org.traccar.position;

service EnrichmentService {
  // Geocode a position (reverse geocoding)
  rpc Geocode(GeocodeRequest) returns (GeocodeResponse);
  
  // Get speed limit for a location
  rpc GetSpeedLimit(SpeedLimitRequest) returns (SpeedLimitResponse);
}

message GeocodeRequest {
  double latitude = 1;
  double longitude = 2;
}

message GeocodeResponse {
  string address = 1;
  string country = 2;
  string country_code = 3;
  string region = 4;
  string locality = 5;
  string street = 6;
  string house_number = 7;
  string postal_code = 8;
}

message SpeedLimitRequest {
  double latitude = 1;
  double longitude = 2;
}

message SpeedLimitResponse {
  double speed_limit = 1; // km/h
  string road_type = 2;
}
```

## REST API Endpoints

The Position Processing Service also exposes a REST API for clients that cannot use gRPC. The REST API provides equivalent functionality to the gRPC services.

### Position Endpoints

#### Get Latest Position

```
GET /api/v1/positions/latest/{deviceId}
```

Parameters:
- `deviceId` (path): Device ID

Response:
```json
{
  "id": 123456,
  "deviceId": 42,
  "protocol": "tcp",
  "deviceTime": "2023-06-01T12:30:45Z",
  "fixTime": "2023-06-01T12:30:40Z",
  "serverTime": "2023-06-01T12:30:50Z",
  "outdated": false,
  "valid": true,
  "latitude": 37.7749,
  "longitude": -122.4194,
  "altitude": 10.5,
  "speed": 5.2,
  "course": 45.3,
  "address": "123 Main St, San Francisco, CA",
  "accuracy": 5.0,
  "attributes": {
    "batteryLevel": "87",
    "ignition": "true",
    "motion": "true"
  }
}
```

#### Get Positions

```
GET /api/v1/positions
```

Parameters:
- `deviceId` (query, required): Device ID
- `from` (query, required): Start time (ISO-8601 format)
- `to` (query, required): End time (ISO-8601 format)
- `limit` (query, optional): Maximum number of positions to return

Response:
```json
{
  "positions": [
    {
      "id": 123456,
      "deviceId": 42,
      "protocol": "tcp",
      "deviceTime": "2023-06-01T12:30:45Z",
      "fixTime": "2023-06-01T12:30:40Z",
      "serverTime": "2023-06-01T12:30:50Z",
      "outdated": false,
      "valid": true,
      "latitude": 37.7749,
      "longitude": -122.4194,
      "altitude": 10.5,
      "speed": 5.2,
      "course": 45.3,
      "address": "123 Main St, San Francisco, CA",
      "accuracy": 5.0,
      "attributes": {
        "batteryLevel": "87",
        "ignition": "true",
        "motion": "true"
      }
    },
    // Additional positions...
  ]
}
```

#### Process Position

```
POST /api/v1/positions/process
```

Request Body:
```json
{
  "deviceId": 42,
  "protocol": "tcp",
  "deviceTime": "2023-06-01T12:30:45Z",
  "latitude": 37.7749,
  "longitude": -122.4194,
  "altitude": 10.5,
  "speed": 5.2,
  "course": 45.3,
  "attributes": {
    "batteryLevel": "87",
    "ignition": "true"
  }
}
```

Response:
```json
{
  "id": 123456,
  "deviceId": 42,
  "protocol": "tcp",
  "deviceTime": "2023-06-01T12:30:45Z",
  "fixTime": "2023-06-01T12:30:40Z",
  "serverTime": "2023-06-01T12:30:50Z",
  "outdated": false,
  "valid": true,
  "latitude": 37.7749,
  "longitude": -122.4194,
  "altitude": 10.5,
  "speed": 5.2,
  "course": 45.3,
  "address": "123 Main St, San Francisco, CA",
  "accuracy": 5.0,
  "attributes": {
    "batteryLevel": "87",
    "ignition": "true",
    "motion": "true"
  }
}
```

#### Send Command

```
POST /api/v1/commands
```

Request Body:
```json
{
  "deviceId": 42,
  "type": "engineStop",
  "attributes": {
    "reason": "Unauthorized use"
  }
}
```

Response:
```json
{
  "success": true,
  "status": "queued",
  "message": "Command queued for delivery"
}
```

#### Get Device Status

```
GET /api/v1/devices/{deviceId}/status
```

Parameters:
- `deviceId` (path): Device ID

Response:
```json
{
  "deviceId": 42,
  "status": "online",
  "lastUpdate": "2023-06-01T12:30:50Z",
  "lastPosition": {
    "id": 123456,
    "deviceId": 42,
    "protocol": "tcp",
    "deviceTime": "2023-06-01T12:30:45Z",
    "fixTime": "2023-06-01T12:30:40Z",
    "serverTime": "2023-06-01T12:30:50Z",
    "outdated": false,
    "valid": true,
    "latitude": 37.7749,
    "longitude": -122.4194,
    "altitude": 10.5,
    "speed": 5.2,
    "course": 45.3,
    "address": "123 Main St, San Francisco, CA",
    "accuracy": 5.0,
    "attributes": {
      "batteryLevel": "87",
      "ignition": "true",
      "motion": "true"
    }
  },
  "attributes": {
    "lastReportedBatteryLevel": "87",
    "ignitionStatus": "on",
    "motionStatus": "moving"
  }
}
```

#### Calculate Distance

```
POST /api/v1/positions/distance
```

Request Body:
```json
{
  "positions": [
    {
      "latitude": 37.7749,
      "longitude": -122.4194,
      "altitude": 10.5
    },
    {
      "latitude": 37.7750,
      "longitude": -122.4195,
      "altitude": 11.0
    },
    // Additional positions...
  ],
  "useAltitude": true
}
```

Response:
```json
{
  "distance": 152.5
}
```

### Geofence Endpoints

#### Check Geofences

```
POST /api/v1/geofences/check
```

Request Body:
```json
{
  "position": {
    "latitude": 37.7749,
    "longitude": -122.4194
  },
  "geofenceIds": [1, 2, 3]
}
```

Response:
```json
{
  "matchingGeofenceIds": [1, 3]
}
```

#### Get Device Geofences

```
GET /api/v1/devices/{deviceId}/geofences
```

Parameters:
- `deviceId` (path): Device ID

Response:
```json
{
  "geofences": [
    {
      "id": 1,
      "name": "Office",
      "description": "Company headquarters",
      "area": "POLYGON((...))",
      "attributes": {
        "color": "#FF0000",
        "type": "work"
      }
    },
    // Additional geofences...
  ]
}
```

### Enrichment Endpoints

#### Geocode

```
GET /api/v1/geocode
```

Parameters:
- `latitude` (query, required): Latitude
- `longitude` (query, required): Longitude

Response:
```json
{
  "address": "123 Main St, San Francisco, CA 94105, USA",
  "country": "United States",
  "countryCode": "US",
  "region": "California",
  "locality": "San Francisco",
  "street": "Main St",
  "houseNumber": "123",
  "postalCode": "94105"
}
```

#### Get Speed Limit

```
GET /api/v1/speedlimit
```

Parameters:
- `latitude` (query, required): Latitude
- `longitude` (query, required): Longitude

Response:
```json
{
  "speedLimit": 50.0,
  "roadType": "urban"
}
```

## Data Models

### Position

The Position model represents a GPS position with additional enriched data.

| Field | Type | Description |
|-------|------|-------------|
| id | int64 | Unique identifier for the position |
| deviceId | int64 | Device identifier |
| protocol | string | Protocol used to receive the position |
| deviceTime | timestamp | Time reported by the device |
| fixTime | timestamp | Time when the GPS fix was acquired |
| serverTime | timestamp | Time when the position was received by the server |
| outdated | boolean | Whether the position is considered outdated |
| valid | boolean | Whether the position is valid |
| latitude | double | Latitude in degrees |
| longitude | double | Longitude in degrees |
| altitude | double | Altitude in meters |
| speed | double | Speed in knots |
| course | double | Course in degrees |
| address | string | Geocoded address |
| accuracy | double | Accuracy in meters |
| attributes | map<string, string> | Additional attributes |

### RawPosition

The RawPosition model represents a GPS position as received from a device before processing.

| Field | Type | Description |
|-------|------|-------------|
| deviceId | int64 | Device identifier |
| protocol | string | Protocol used to receive the position |
| deviceTime | timestamp | Time reported by the device |
| latitude | double | Latitude in degrees |
| longitude | double | Longitude in degrees |
| altitude | double | Altitude in meters |
| speed | double | Speed in knots |
| course | double | Course in degrees |
| attributes | map<string, string> | Additional attributes |

### DeviceStatus

The DeviceStatus model represents the current status of a device.

| Field | Type | Description |
|-------|------|-------------|
| deviceId | int64 | Device identifier |
| status | string | Status (online, offline, unknown) |
| lastUpdate | timestamp | Time of the last update |
| lastPosition | Position | Last known position |
| attributes | map<string, string> | Additional attributes |

### Geofence

The Geofence model represents a geographic boundary.

| Field | Type | Description |
|-------|------|-------------|
| id | int64 | Unique identifier for the geofence |
| name | string | Name of the geofence |
| description | string | Description of the geofence |
| area | string | WKT representation of the geofence area |
| attributes | map<string, string> | Additional attributes |

## Error Handling

The Position Processing Service uses standard HTTP status codes for REST API responses and gRPC status codes for gRPC responses.

### HTTP Status Codes

| Status Code | Description |
|-------------|-------------|
| 200 | OK - The request was successful |
| 400 | Bad Request - The request was invalid or cannot be served |
| 401 | Unauthorized - Authentication is required or has failed |
| 403 | Forbidden - The authenticated user does not have permission |
| 404 | Not Found - The requested resource does not exist |
| 409 | Conflict - The request conflicts with the current state |
| 422 | Unprocessable Entity - The request was well-formed but contains semantic errors |
| 429 | Too Many Requests - Rate limit exceeded |
| 500 | Internal Server Error - An error occurred on the server |
| 503 | Service Unavailable - The service is temporarily unavailable |

### gRPC Status Codes

| Status Code | Description |
|-------------|-------------|
| OK | The request was successful |
| INVALID_ARGUMENT | The request contains invalid arguments |
| UNAUTHENTICATED | Authentication is required or has failed |
| PERMISSION_DENIED | The authenticated user does not have permission |
| NOT_FOUND | The requested resource does not exist |
| ALREADY_EXISTS | The resource already exists |
| FAILED_PRECONDITION | The request cannot be executed in the current system state |
| RESOURCE_EXHAUSTED | Rate limit exceeded |
| INTERNAL | An internal error occurred |
| UNAVAILABLE | The service is temporarily unavailable |

### Error Response Format

REST API error responses follow this format:

```json
{
  "error": {
    "code": "RESOURCE_NOT_FOUND",
    "message": "Device with ID 42 not found",
    "details": {
      "resourceType": "Device",
      "resourceId": "42"
    }
  }
}
```

gRPC error responses include error details in the status message and may include additional metadata.

## Rate Limiting

The Position Processing Service implements rate limiting to protect the service from excessive use. Rate limits are applied per client based on the authenticated user or service.

### Default Rate Limits

| Endpoint | Rate Limit |
|----------|------------|
| GetLatestPosition | 100 requests per minute |
| GetPositions | 60 requests per minute |
| StreamPositions | 10 concurrent streams per client |
| ProcessPosition | 1000 requests per minute |
| SendCommand | 60 requests per minute |
| GetDeviceStatus | 100 requests per minute |
| CalculateDistance | 60 requests per minute |
| CheckGeofences | 100 requests per minute |
| GetDeviceGeofences | 60 requests per minute |
| Geocode | 100 requests per minute |
| GetSpeedLimit | 100 requests per minute |

### Rate Limit Headers

REST API responses include rate limit headers:

```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1623456789
```

## Examples

### gRPC Example (Go)

```go
package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	pb "org.traccar.position/proto"
)

func main() {
	// Set up a connection to the server with TLS credentials
	creds, err := credentials.NewClientTLSFromFile("cert.pem", "")
	if err != nil {
		log.Fatalf("Failed to load credentials: %v", err)
	}

	conn, err := grpc.Dial("position-service:9090", grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewPositionServiceClient(conn)

	// Get the latest position for a device
	req := &pb.DeviceIdRequest{DeviceId: 42}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	position, err := client.GetLatestPosition(ctx, req)
	if err != nil {
		log.Fatalf("Could not get position: %v", err)
	}

	log.Printf("Latest position: %v, %v", position.Latitude, position.Longitude)
}
```

### REST API Example (cURL)

```bash
# Get the latest position for a device
curl -X GET \
  "https://api.traccar.org/api/v1/positions/latest/42" \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." \
  -H "Content-Type: application/json"

# Process a raw position
curl -X POST \
  "https://api.traccar.org/api/v1/positions/process" \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." \
  -H "Content-Type: application/json" \
  -d '{
    "deviceId": 42,
    "protocol": "tcp",
    "deviceTime": "2023-06-01T12:30:45Z",
    "latitude": 37.7749,
    "longitude": -122.4194,
    "altitude": 10.5,
    "speed": 5.2,
    "course": 45.3,
    "attributes": {
      "batteryLevel": "87",
      "ignition": "true"
    }
  }'
```

### WebSocket Example (JavaScript)

```javascript
// Connect to the WebSocket endpoint for streaming positions
const token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...';
const ws = new WebSocket(`wss://api.traccar.org/api/v1/positions/stream?deviceId=42&token=${token}`);

ws.onopen = () => {
  console.log('WebSocket connection established');
};

ws.onmessage = (event) => {
  const position = JSON.parse(event.data);
  console.log(`New position: ${position.latitude}, ${position.longitude}`);
};

ws.onerror = (error) => {
  console.error('WebSocket error:', error);
};

ws.onclose = (event) => {
  console.log(`WebSocket connection closed: ${event.code} ${event.reason}`);
};
```