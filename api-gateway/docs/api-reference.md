# Traccar API Reference

## Overview

This document provides a comprehensive reference for the Traccar API Gateway, which serves as the unified entry point for all client interactions with the Traccar GPS tracking system. The API Gateway routes requests to appropriate backend microservices while maintaining backward compatibility with existing API contracts.

## Base URL

```
https://{server-address}/api
```

## Authentication

The API Gateway supports multiple authentication methods:

### Basic Authentication

Use HTTP Basic Authentication with your username and password.

```
Authorization: Basic {base64-encoded-credentials}
```

### Token Authentication

Use a bearer token for authentication.

```
Authorization: Bearer {token}
```

### Session Authentication

For web applications, session-based authentication is supported using cookies.

## Common Headers

| Header | Description |
| ------ | ----------- |
| `Authorization` | Authentication credentials |
| `Accept` | Response format (application/json, application/xml, etc.) |
| `Content-Type` | Request body format (application/json, application/xml, etc.) |
| `Accept-Language` | Preferred language for responses |

## Response Format

All API responses are returned in JSON format by default. The standard response structure includes:

```json
{
  "id": 1,
  "attribute1": "value1",
  "attribute2": "value2",
  ...
}
```

For collection responses:

```json
[
  {
    "id": 1,
    "attribute1": "value1",
    ...
  },
  {
    "id": 2,
    "attribute1": "value2",
    ...
  }
]
```

## Error Handling

Errors are returned with appropriate HTTP status codes and a JSON body containing error details:

```json
{
  "status": 400,
  "message": "Error message",
  "details": "Detailed error information"
}
```

### Common Error Codes

| Status Code | Description |
| ----------- | ----------- |
| 400 | Bad Request - Invalid parameters or request format |
| 401 | Unauthorized - Authentication required or failed |
| 403 | Forbidden - Insufficient permissions |
| 404 | Not Found - Resource not found |
| 409 | Conflict - Resource conflict |
| 500 | Internal Server Error - Server-side error |

## Pagination, Filtering, and Sorting

### Pagination

Pagination is supported using the following query parameters:

| Parameter | Description | Default |
| --------- | ----------- | ------- |
| `page` | Page number (0-based) | 0 |
| `limit` | Number of items per page | 25 |

Example:
```
GET /api/devices?page=1&limit=10
```

### Filtering

Filtering is supported using query parameters specific to each resource:

| Parameter | Description | Example |
| --------- | ----------- | ------- |
| `userId` | Filter by user ID | `userId=1` |
| `groupId` | Filter by group ID | `groupId=2` |
| `deviceId` | Filter by device ID | `deviceId=3` |
| `from` | Start date/time for time-based queries | `from=2023-01-01T00:00:00Z` |
| `to` | End date/time for time-based queries | `to=2023-01-31T23:59:59Z` |
| `all` | Include all accessible resources | `all=true` |

Example:
```
GET /api/positions?deviceId=123&from=2023-01-01T00:00:00Z&to=2023-01-01T23:59:59Z
```

### Sorting

Sorting is applied automatically based on the resource type. Most resources are sorted by name or date.

## API Endpoints

### Session

#### Login

```
POST /api/session
```

Request body:
```json
{
  "email": "user@example.com",
  "password": "password"
}
```

Response:
```json
{
  "id": 1,
  "name": "John Smith",
  "email": "user@example.com",
  "administrator": false,
  "...": "..."
}
```

#### Logout

```
DELETE /api/session
```

#### Get Current Session

```
GET /api/session
```

Response:
```json
{
  "id": 1,
  "name": "John Smith",
  "email": "user@example.com",
  "administrator": false,
  "...": "..."
}
```

### Users

#### Get All Users

```
GET /api/users
```

Query parameters:
- `all=true` - Include all accessible users (admin only)
- `userId={id}` - Filter by user ID

Response:
```json
[
  {
    "id": 1,
    "name": "John Smith",
    "email": "user@example.com",
    "administrator": false,
    "...": "..."
  },
  {...}
]
```

#### Get User

```
GET /api/users/{id}
```

Response:
```json
{
  "id": 1,
  "name": "John Smith",
  "email": "user@example.com",
  "administrator": false,
  "...": "..."
}
```

#### Create User

```
POST /api/users
```

Request body:
```json
{
  "name": "John Smith",
  "email": "user@example.com",
  "password": "password",
  "administrator": false,
  "...": "..."
}
```

Response:
```json
{
  "id": 1,
  "name": "John Smith",
  "email": "user@example.com",
  "administrator": false,
  "...": "..."
}
```

#### Update User

```
PUT /api/users/{id}
```

Request body:
```json
{
  "id": 1,
  "name": "John Smith",
  "email": "user@example.com",
  "administrator": false,
  "...": "..."
}
```

Response:
```json
{
  "id": 1,
  "name": "John Smith",
  "email": "user@example.com",
  "administrator": false,
  "...": "..."
}
```

#### Delete User

```
DELETE /api/users/{id}
```

### Devices

#### Get All Devices

```
GET /api/devices
```

Query parameters:
- `all=true` - Include all accessible devices (admin only)
- `userId={id}` - Filter by user ID
- `groupId={id}` - Filter by group ID

Response:
```json
[
  {
    "id": 1,
    "name": "Device 1",
    "uniqueId": "123456789",
    "status": "online",
    "lastUpdate": "2023-01-01T12:00:00Z",
    "...": "..."
  },
  {...}
]
```

#### Get Device

```
GET /api/devices/{id}
```

Response:
```json
{
  "id": 1,
  "name": "Device 1",
  "uniqueId": "123456789",
  "status": "online",
  "lastUpdate": "2023-01-01T12:00:00Z",
  "...": "..."
}
```

#### Create Device

```
POST /api/devices
```

Request body:
```json
{
  "name": "Device 1",
  "uniqueId": "123456789",
  "...": "..."
}
```

Response:
```json
{
  "id": 1,
  "name": "Device 1",
  "uniqueId": "123456789",
  "status": "offline",
  "...": "..."
}
```

#### Update Device

```
PUT /api/devices/{id}
```

Request body:
```json
{
  "id": 1,
  "name": "Device 1 Updated",
  "uniqueId": "123456789",
  "...": "..."
}
```

Response:
```json
{
  "id": 1,
  "name": "Device 1 Updated",
  "uniqueId": "123456789",
  "status": "online",
  "lastUpdate": "2023-01-01T12:00:00Z",
  "...": "..."
}
```

#### Delete Device

```
DELETE /api/devices/{id}
```

### Positions

#### Get Positions

```
GET /api/positions
```

Query parameters:
- `deviceId={id}` - Filter by device ID (required)
- `from={datetime}` - Start date/time (ISO 8601 format)
- `to={datetime}` - End date/time (ISO 8601 format)
- `id={id}` - Filter by specific position ID(s)

Response:
```json
[
  {
    "id": 1,
    "deviceId": 1,
    "protocol": "tcp",
    "deviceTime": "2023-01-01T12:00:00Z",
    "fixTime": "2023-01-01T12:00:00Z",
    "serverTime": "2023-01-01T12:00:01Z",
    "latitude": 37.7749,
    "longitude": -122.4194,
    "altitude": 10,
    "speed": 0,
    "course": 0,
    "address": "123 Main St, San Francisco, CA",
    "attributes": {...}
  },
  {...}
]
```

#### Get Latest Positions

```
GET /api/positions/latest
```

Query parameters:
- `deviceId={id}` - Filter by device ID
- `groupId={id}` - Filter by group ID

Response:
```json
[
  {
    "id": 1,
    "deviceId": 1,
    "protocol": "tcp",
    "deviceTime": "2023-01-01T12:00:00Z",
    "fixTime": "2023-01-01T12:00:00Z",
    "serverTime": "2023-01-01T12:00:01Z",
    "latitude": 37.7749,
    "longitude": -122.4194,
    "altitude": 10,
    "speed": 0,
    "course": 0,
    "address": "123 Main St, San Francisco, CA",
    "attributes": {...}
  },
  {...}
]
```

### Groups

#### Get All Groups

```
GET /api/groups
```

Query parameters:
- `all=true` - Include all accessible groups (admin only)
- `userId={id}` - Filter by user ID

Response:
```json
[
  {
    "id": 1,
    "name": "Group 1",
    "groupId": 0,
    "...": "..."
  },
  {...}
]
```

#### Get Group

```
GET /api/groups/{id}
```

Response:
```json
{
  "id": 1,
  "name": "Group 1",
  "groupId": 0,
  "...": "..."
}
```

#### Create Group

```
POST /api/groups
```

Request body:
```json
{
  "name": "Group 1",
  "groupId": 0,
  "...": "..."
}
```

Response:
```json
{
  "id": 1,
  "name": "Group 1",
  "groupId": 0,
  "...": "..."
}
```

#### Update Group

```
PUT /api/groups/{id}
```

Request body:
```json
{
  "id": 1,
  "name": "Group 1 Updated",
  "groupId": 0,
  "...": "..."
}
```

Response:
```json
{
  "id": 1,
  "name": "Group 1 Updated",
  "groupId": 0,
  "...": "..."
}
```

#### Delete Group

```
DELETE /api/groups/{id}
```

### Geofences

#### Get All Geofences

```
GET /api/geofences
```

Query parameters:
- `all=true` - Include all accessible geofences (admin only)
- `userId={id}` - Filter by user ID
- `groupId={id}` - Filter by group ID
- `deviceId={id}` - Filter by device ID

Response:
```json
[
  {
    "id": 1,
    "name": "Geofence 1",
    "description": "Description",
    "area": "POLYGON((...))",
    "...": "..."
  },
  {...}
]
```

#### Get Geofence

```
GET /api/geofences/{id}
```

Response:
```json
{
  "id": 1,
  "name": "Geofence 1",
  "description": "Description",
  "area": "POLYGON((...))",
  "...": "..."
}
```

#### Create Geofence

```
POST /api/geofences
```

Request body:
```json
{
  "name": "Geofence 1",
  "description": "Description",
  "area": "POLYGON((...))",
  "...": "..."
}
```

Response:
```json
{
  "id": 1,
  "name": "Geofence 1",
  "description": "Description",
  "area": "POLYGON((...))",
  "...": "..."
}
```

#### Update Geofence

```
PUT /api/geofences/{id}
```

Request body:
```json
{
  "id": 1,
  "name": "Geofence 1 Updated",
  "description": "Description Updated",
  "area": "POLYGON((...))",
  "...": "..."
}
```

Response:
```json
{
  "id": 1,
  "name": "Geofence 1 Updated",
  "description": "Description Updated",
  "area": "POLYGON((...))",
  "...": "..."
}
```

#### Delete Geofence

```
DELETE /api/geofences/{id}
```

### Events

#### Get Events

```
GET /api/events
```

Query parameters:
- `deviceId={id}` - Filter by device ID
- `groupId={id}` - Filter by group ID
- `type={type}` - Filter by event type
- `from={datetime}` - Start date/time (ISO 8601 format)
- `to={datetime}` - End date/time (ISO 8601 format)

Response:
```json
[
  {
    "id": 1,
    "type": "deviceOnline",
    "serverTime": "2023-01-01T12:00:00Z",
    "deviceId": 1,
    "positionId": 1,
    "geofenceId": 0,
    "maintenanceId": 0,
    "attributes": {...}
  },
  {...}
]
```

### Reports

#### Get Events Report

```
GET /api/reports/events
```

Query parameters:
- `deviceId={id}` - Filter by device ID
- `groupId={id}` - Filter by group ID
- `type={type}` - Filter by event type
- `from={datetime}` - Start date/time (ISO 8601 format)
- `to={datetime}` - End date/time (ISO 8601 format)
- `mail={boolean}` - Send report by email
- `format={format}` - Report format (csv, xlsx)

#### Get Trips Report

```
GET /api/reports/trips
```

Query parameters:
- `deviceId={id}` - Filter by device ID
- `groupId={id}` - Filter by group ID
- `from={datetime}` - Start date/time (ISO 8601 format)
- `to={datetime}` - End date/time (ISO 8601 format)
- `mail={boolean}` - Send report by email
- `format={format}` - Report format (csv, xlsx)

#### Get Route Report

```
GET /api/reports/route
```

Query parameters:
- `deviceId={id}` - Filter by device ID
- `from={datetime}` - Start date/time (ISO 8601 format)
- `to={datetime}` - End date/time (ISO 8601 format)
- `mail={boolean}` - Send report by email
- `format={format}` - Report format (csv, xlsx, gpx)

#### Get Summary Report

```
GET /api/reports/summary
```

Query parameters:
- `deviceId={id}` - Filter by device ID
- `groupId={id}` - Filter by group ID
- `from={datetime}` - Start date/time (ISO 8601 format)
- `to={datetime}` - End date/time (ISO 8601 format)
- `mail={boolean}` - Send report by email
- `format={format}` - Report format (csv, xlsx)

### Commands

#### Get Supported Commands

```
GET /api/commands/types
```

Response:
```json
[
  {
    "type": "custom",
    "parameters": [
      {
        "id": "data",
        "type": "string",
        "name": "Data"
      }
    ]
  },
  {
    "type": "positionPeriodic",
    "parameters": [
      {
        "id": "frequency",
        "type": "number",
        "name": "Frequency"
      }
    ]
  },
  {...}
]
```

#### Send Command

```
POST /api/commands
```

Request body:
```json
{
  "deviceId": 1,
  "type": "positionPeriodic",
  "attributes": {
    "frequency": 60
  }
}
```

Response:
```json
{
  "id": 1,
  "deviceId": 1,
  "type": "positionPeriodic",
  "attributes": {
    "frequency": 60
  }
}
```

### Permissions

#### Get Permissions

```
GET /api/permissions
```

Response:
```json
{
  "users": [
    {
      "userId": 1,
      "deviceId": 1
    },
    {...}
  ],
  "devices": [
    {
      "userId": 1,
      "deviceId": 1
    },
    {...}
  ],
  "groups": [
    {
      "userId": 1,
      "groupId": 1
    },
    {...}
  ],
  "geofences": [
    {
      "userId": 1,
      "geofenceId": 1
    },
    {...}
  ],
  "notifications": [
    {
      "userId": 1,
      "notificationId": 1
    },
    {...}
  ],
  "calendars": [
    {
      "userId": 1,
      "calendarId": 1
    },
    {...}
  ]
}
```

#### Link Device to User

```
POST /api/permissions/devices/{userId}/{deviceId}
```

#### Unlink Device from User

```
DELETE /api/permissions/devices/{userId}/{deviceId}
```

#### Link Group to User

```
POST /api/permissions/groups/{userId}/{groupId}
```

#### Unlink Group from User

```
DELETE /api/permissions/groups/{userId}/{groupId}
```

#### Link Geofence to User

```
POST /api/permissions/geofences/{userId}/{geofenceId}
```

#### Unlink Geofence from User

```
DELETE /api/permissions/geofences/{userId}/{geofenceId}
```

## WebSocket API

The Traccar API Gateway provides a WebSocket interface for real-time updates.

### Connection

```
wss://{server-address}/api/socket
```

Authentication is required using the same methods as the REST API.

### Message Format

Messages are sent as JSON objects with the following structure:

```json
{
  "type": "messageType",
  "data": {...}
}
```

### Message Types

#### Position Updates

```json
{
  "type": "position",
  "data": {
    "id": 1,
    "deviceId": 1,
    "protocol": "tcp",
    "deviceTime": "2023-01-01T12:00:00Z",
    "fixTime": "2023-01-01T12:00:00Z",
    "serverTime": "2023-01-01T12:00:01Z",
    "latitude": 37.7749,
    "longitude": -122.4194,
    "altitude": 10,
    "speed": 0,
    "course": 0,
    "address": "123 Main St, San Francisco, CA",
    "attributes": {...}
  }
}
```

#### Device Updates

```json
{
  "type": "device",
  "data": {
    "id": 1,
    "name": "Device 1",
    "uniqueId": "123456789",
    "status": "online",
    "lastUpdate": "2023-01-01T12:00:00Z",
    "...": "..."
  }
}
```

#### Events

```json
{
  "type": "event",
  "data": {
    "id": 1,
    "type": "deviceOnline",
    "serverTime": "2023-01-01T12:00:00Z",
    "deviceId": 1,
    "positionId": 1,
    "geofenceId": 0,
    "maintenanceId": 0,
    "attributes": {...}
  }
}
```

## Microservices Architecture

The Traccar API Gateway routes requests to the following backend microservices:

- **Protocol Service**: Handles device communications across 200+ protocols
- **Position Processing Service**: Processes GPS positions and related data
- **Event Processing Service**: Detects and processes events from position data
- **Notification Service**: Handles multi-channel notification delivery
- **Reporting Service**: Generates and delivers reports

All client interactions are handled through the API Gateway, which provides a unified API surface while maintaining backward compatibility with existing API contracts.

## Rate Limiting

The API Gateway implements rate limiting to prevent abuse. Limits are applied per user and may vary based on the endpoint.

| Endpoint | Rate Limit |
| -------- | ---------- |
| Authentication endpoints | 10 requests per minute |
| Read operations | 60 requests per minute |
| Write operations | 30 requests per minute |
| WebSocket connections | 5 connections per user |

When rate limits are exceeded, the API returns a 429 Too Many Requests status code.

## Versioning

The current API version is v1. The API Gateway maintains backward compatibility with existing clients.

## Additional Resources

- [Traccar Documentation](https://www.traccar.org/documentation/)
- [Traccar GitHub Repository](https://github.com/traccar/traccar)
- [API Examples](https://www.traccar.org/api-reference/)