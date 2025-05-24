# Notification Service API Specification

## Table of Contents

- [Introduction](#introduction)
- [Authentication and Authorization](#authentication-and-authorization)
- [REST API Endpoints](#rest-api-endpoints)
  - [Notification Templates](#notification-templates)
  - [Notification Rules](#notification-rules)
  - [Notification History](#notification-history)
  - [Notification Channels](#notification-channels)
  - [Manual Notifications](#manual-notifications)
- [Message-Based Interfaces](#message-based-interfaces)
  - [Event Consumption](#event-consumption)
  - [Notification Status Updates](#notification-status-updates)
- [Data Models](#data-models)
- [Error Handling](#error-handling)
- [Rate Limiting](#rate-limiting)
- [Examples](#examples)

## Introduction

The Notification Service is responsible for managing and delivering alerts through multiple channels (email, SMS, push notifications, webhooks) based on detected events. This document serves as a reference for developers integrating with the Notification Service, providing details on available endpoints, message formats, and integration patterns.

The service provides both synchronous REST APIs for notification management and asynchronous message-based interfaces for event-driven notification processing.

## Authentication and Authorization

### Authentication

All REST API requests to the Notification Service must include authentication credentials. The service supports the following authentication methods:

1. **JWT Bearer Token** (Recommended)
   - Include a valid JWT token in the Authorization header
   - Format: `Authorization: Bearer <token>`
   - Tokens are issued by the API Gateway Service

2. **Basic Authentication**
   - Include Base64-encoded credentials in the Authorization header
   - Format: `Authorization: Basic <base64-encoded-credentials>`
   - Not recommended for production use

### Authorization

Access to notification resources is controlled by the following permission model:

| Resource | User | Manager | Administrator |
|----------|------|---------|---------------|
| View own notifications | ✓ | ✓ | ✓ |
| Create notification for own devices | ✓ | ✓ | ✓ |
| View all notifications | - | ✓ | ✓ |
| Manage notification templates | - | ✓ | ✓ |
| Configure notification channels | - | - | ✓ |
| System-wide notification settings | - | - | ✓ |

Authorization is enforced at both the API Gateway level and within the Notification Service. Requests without proper permissions will receive a `403 Forbidden` response.

## REST API Endpoints

The Notification Service exposes the following REST API endpoints:

### Notification Templates

#### List Templates

```
GET /api/v1/templates
```

Retrieve a list of notification templates available to the authenticated user.

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| type | string | Filter by template type (email, sms, push, etc.) |
| page | integer | Page number for pagination (default: 1) |
| limit | integer | Number of items per page (default: 20, max: 100) |

**Response:**

```json
{
  "templates": [
    {
      "id": "1a2b3c4d",
      "name": "Speed Alert",
      "type": "email",
      "subject": "Speed Alert for ${device.name}",
      "body": "Device ${device.name} has exceeded the speed limit of ${speedLimit} km/h. Current speed: ${position.speed} km/h at ${position.time}.",
      "createdAt": "2023-06-15T10:30:00Z",
      "updatedAt": "2023-06-15T10:30:00Z"
    },
    {
      "id": "2b3c4d5e",
      "name": "Geofence Entry",
      "type": "sms",
      "body": "${device.name} has entered geofence ${geofence.name} at ${position.time}.",
      "createdAt": "2023-06-14T15:45:00Z",
      "updatedAt": "2023-06-14T15:45:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "totalItems": 2,
    "totalPages": 1
  }
}
```

#### Get Template

```
GET /api/v1/templates/{templateId}
```

Retrieve a specific notification template by ID.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| templateId | string | Unique identifier of the template |

**Response:**

```json
{
  "id": "1a2b3c4d",
  "name": "Speed Alert",
  "type": "email",
  "subject": "Speed Alert for ${device.name}",
  "body": "Device ${device.name} has exceeded the speed limit of ${speedLimit} km/h. Current speed: ${position.speed} km/h at ${position.time}.",
  "variables": ["device.name", "speedLimit", "position.speed", "position.time"],
  "createdAt": "2023-06-15T10:30:00Z",
  "updatedAt": "2023-06-15T10:30:00Z"
}
```

#### Create Template

```
POST /api/v1/templates
```

Create a new notification template.

**Request Body:**

```json
{
  "name": "Low Battery Alert",
  "type": "email",
  "subject": "Low Battery Alert for ${device.name}",
  "body": "Device ${device.name} has low battery level: ${position.attributes.batteryLevel}% at ${position.time}."
}
```

**Response:**

```json
{
  "id": "3c4d5e6f",
  "name": "Low Battery Alert",
  "type": "email",
  "subject": "Low Battery Alert for ${device.name}",
  "body": "Device ${device.name} has low battery level: ${position.attributes.batteryLevel}% at ${position.time}.",
  "variables": ["device.name", "position.attributes.batteryLevel", "position.time"],
  "createdAt": "2023-06-16T09:15:00Z",
  "updatedAt": "2023-06-16T09:15:00Z"
}
```

#### Update Template

```
PUT /api/v1/templates/{templateId}
```

Update an existing notification template.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| templateId | string | Unique identifier of the template |

**Request Body:**

```json
{
  "name": "Low Battery Warning",
  "subject": "Low Battery Warning for ${device.name}",
  "body": "Device ${device.name} has low battery level: ${position.attributes.batteryLevel}% at ${position.time}. Please charge soon."
}
```

**Response:**

```json
{
  "id": "3c4d5e6f",
  "name": "Low Battery Warning",
  "type": "email",
  "subject": "Low Battery Warning for ${device.name}",
  "body": "Device ${device.name} has low battery level: ${position.attributes.batteryLevel}% at ${position.time}. Please charge soon.",
  "variables": ["device.name", "position.attributes.batteryLevel", "position.time"],
  "createdAt": "2023-06-16T09:15:00Z",
  "updatedAt": "2023-06-16T10:20:00Z"
}
```

#### Delete Template

```
DELETE /api/v1/templates/{templateId}
```

Delete a notification template.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| templateId | string | Unique identifier of the template |

**Response:**

```
204 No Content
```

### Notification Rules

#### List Rules

```
GET /api/v1/rules
```

Retrieve a list of notification rules configured for the authenticated user.

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| deviceId | string | Filter by device ID |
| eventType | string | Filter by event type |
| page | integer | Page number for pagination (default: 1) |
| limit | integer | Number of items per page (default: 20, max: 100) |

**Response:**

```json
{
  "rules": [
    {
      "id": "4d5e6f7g",
      "name": "Speeding Notification",
      "eventType": "deviceSpeedingEvent",
      "deviceId": "device123",
      "templateId": "1a2b3c4d",
      "channels": ["email", "sms"],
      "recipients": ["user1@example.com", "+12345678901"],
      "active": true,
      "notifyOnce": false,
      "createdAt": "2023-06-15T11:30:00Z",
      "updatedAt": "2023-06-15T11:30:00Z"
    },
    {
      "id": "5e6f7g8h",
      "name": "Geofence Entry Alert",
      "eventType": "geofenceEnterEvent",
      "deviceId": "device456",
      "geofenceId": "geo789",
      "templateId": "2b3c4d5e",
      "channels": ["push"],
      "recipients": ["user2@example.com"],
      "active": true,
      "notifyOnce": true,
      "createdAt": "2023-06-14T16:45:00Z",
      "updatedAt": "2023-06-14T16:45:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "totalItems": 2,
    "totalPages": 1
  }
}
```

#### Get Rule

```
GET /api/v1/rules/{ruleId}
```

Retrieve a specific notification rule by ID.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| ruleId | string | Unique identifier of the rule |

**Response:**

```json
{
  "id": "4d5e6f7g",
  "name": "Speeding Notification",
  "eventType": "deviceSpeedingEvent",
  "deviceId": "device123",
  "templateId": "1a2b3c4d",
  "channels": ["email", "sms"],
  "recipients": ["user1@example.com", "+12345678901"],
  "active": true,
  "notifyOnce": false,
  "schedule": {
    "enabled": true,
    "days": [1, 2, 3, 4, 5],
    "startTime": "08:00:00",
    "endTime": "18:00:00",
    "timezone": "America/New_York"
  },
  "createdAt": "2023-06-15T11:30:00Z",
  "updatedAt": "2023-06-15T11:30:00Z"
}
```

#### Create Rule

```
POST /api/v1/rules
```

Create a new notification rule.

**Request Body:**

```json
{
  "name": "Low Battery Notification",
  "eventType": "deviceLowBatteryEvent",
  "deviceId": "device123",
  "templateId": "3c4d5e6f",
  "channels": ["email"],
  "recipients": ["user1@example.com"],
  "active": true,
  "notifyOnce": true,
  "schedule": {
    "enabled": true,
    "days": [1, 2, 3, 4, 5, 6, 7],
    "startTime": "00:00:00",
    "endTime": "23:59:59",
    "timezone": "UTC"
  }
}
```

**Response:**

```json
{
  "id": "6f7g8h9i",
  "name": "Low Battery Notification",
  "eventType": "deviceLowBatteryEvent",
  "deviceId": "device123",
  "templateId": "3c4d5e6f",
  "channels": ["email"],
  "recipients": ["user1@example.com"],
  "active": true,
  "notifyOnce": true,
  "schedule": {
    "enabled": true,
    "days": [1, 2, 3, 4, 5, 6, 7],
    "startTime": "00:00:00",
    "endTime": "23:59:59",
    "timezone": "UTC"
  },
  "createdAt": "2023-06-16T10:30:00Z",
  "updatedAt": "2023-06-16T10:30:00Z"
}
```

#### Update Rule

```
PUT /api/v1/rules/{ruleId}
```

Update an existing notification rule.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| ruleId | string | Unique identifier of the rule |

**Request Body:**

```json
{
  "name": "Critical Low Battery Notification",
  "channels": ["email", "sms"],
  "recipients": ["user1@example.com", "+12345678901"],
  "active": true
}
```

**Response:**

```json
{
  "id": "6f7g8h9i",
  "name": "Critical Low Battery Notification",
  "eventType": "deviceLowBatteryEvent",
  "deviceId": "device123",
  "templateId": "3c4d5e6f",
  "channels": ["email", "sms"],
  "recipients": ["user1@example.com", "+12345678901"],
  "active": true,
  "notifyOnce": true,
  "schedule": {
    "enabled": true,
    "days": [1, 2, 3, 4, 5, 6, 7],
    "startTime": "00:00:00",
    "endTime": "23:59:59",
    "timezone": "UTC"
  },
  "createdAt": "2023-06-16T10:30:00Z",
  "updatedAt": "2023-06-16T11:15:00Z"
}
```

#### Delete Rule

```
DELETE /api/v1/rules/{ruleId}
```

Delete a notification rule.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| ruleId | string | Unique identifier of the rule |

**Response:**

```
204 No Content
```

### Notification History

#### List Notifications

```
GET /api/v1/notifications
```

Retrieve a list of notifications sent to or by the authenticated user.

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| deviceId | string | Filter by device ID |
| eventType | string | Filter by event type |
| status | string | Filter by status (pending, sent, delivered, failed) |
| channel | string | Filter by channel (email, sms, push, etc.) |
| startTime | string | Filter by start time (ISO 8601 format) |
| endTime | string | Filter by end time (ISO 8601 format) |
| page | integer | Page number for pagination (default: 1) |
| limit | integer | Number of items per page (default: 20, max: 100) |

**Response:**

```json
{
  "notifications": [
    {
      "id": "7g8h9i0j",
      "eventId": "event123",
      "eventType": "deviceSpeedingEvent",
      "deviceId": "device123",
      "ruleId": "4d5e6f7g",
      "templateId": "1a2b3c4d",
      "channel": "email",
      "recipient": "user1@example.com",
      "subject": "Speed Alert for Device A",
      "body": "Device A has exceeded the speed limit of 80 km/h. Current speed: 95 km/h at 2023-06-15T12:30:45Z.",
      "status": "delivered",
      "sentAt": "2023-06-15T12:31:00Z",
      "deliveredAt": "2023-06-15T12:31:05Z"
    },
    {
      "id": "8h9i0j1k",
      "eventId": "event456",
      "eventType": "geofenceEnterEvent",
      "deviceId": "device456",
      "geofenceId": "geo789",
      "ruleId": "5e6f7g8h",
      "templateId": "2b3c4d5e",
      "channel": "push",
      "recipient": "user2@example.com",
      "body": "Device B has entered geofence Home at 2023-06-15T18:45:30Z.",
      "status": "delivered",
      "sentAt": "2023-06-15T18:45:45Z",
      "deliveredAt": "2023-06-15T18:45:50Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "totalItems": 2,
    "totalPages": 1
  }
}
```

#### Get Notification

```
GET /api/v1/notifications/{notificationId}
```

Retrieve a specific notification by ID.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| notificationId | string | Unique identifier of the notification |

**Response:**

```json
{
  "id": "7g8h9i0j",
  "eventId": "event123",
  "eventType": "deviceSpeedingEvent",
  "deviceId": "device123",
  "ruleId": "4d5e6f7g",
  "templateId": "1a2b3c4d",
  "channel": "email",
  "recipient": "user1@example.com",
  "subject": "Speed Alert for Device A",
  "body": "Device A has exceeded the speed limit of 80 km/h. Current speed: 95 km/h at 2023-06-15T12:30:45Z.",
  "status": "delivered",
  "sentAt": "2023-06-15T12:31:00Z",
  "deliveredAt": "2023-06-15T12:31:05Z",
  "metadata": {
    "ipAddress": "203.0.113.45",
    "userAgent": "Mozilla/5.0",
    "messageId": "SMTP-123456",
    "retryCount": 0
  }
}
```

#### Resend Notification

```
POST /api/v1/notifications/{notificationId}/resend
```

Resend a previously sent notification.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| notificationId | string | Unique identifier of the notification |

**Request Body:**

```json
{
  "channels": ["email", "sms"],
  "recipients": ["user1@example.com", "+12345678901"]
}
```

**Response:**

```json
{
  "id": "9i0j1k2l",
  "originalNotificationId": "7g8h9i0j",
  "eventId": "event123",
  "eventType": "deviceSpeedingEvent",
  "deviceId": "device123",
  "ruleId": "4d5e6f7g",
  "templateId": "1a2b3c4d",
  "channel": "email",
  "recipient": "user1@example.com",
  "subject": "Speed Alert for Device A",
  "body": "Device A has exceeded the speed limit of 80 km/h. Current speed: 95 km/h at 2023-06-15T12:30:45Z.",
  "status": "pending",
  "sentAt": null,
  "deliveredAt": null
}
```

### Notification Channels

#### List Channels

```
GET /api/v1/channels
```

Retrieve a list of configured notification channels.

**Response:**

```json
{
  "channels": [
    {
      "id": "email",
      "name": "Email",
      "enabled": true,
      "config": {
        "smtpHost": "smtp.example.com",
        "smtpPort": 587,
        "smtpUsername": "notifications@example.com",
        "smtpSecurity": "tls"
      }
    },
    {
      "id": "sms",
      "name": "SMS",
      "enabled": true,
      "config": {
        "provider": "twilio",
        "accountSid": "AC1234567890",
        "fromNumber": "+19876543210"
      }
    },
    {
      "id": "push",
      "name": "Push Notifications",
      "enabled": true,
      "config": {
        "provider": "firebase",
        "projectId": "traccar-app-123"
      }
    },
    {
      "id": "webhook",
      "name": "Webhook",
      "enabled": true,
      "config": {
        "url": "https://webhook.example.com/traccar",
        "method": "POST",
        "headers": {
          "X-API-Key": "secret-key-123"
        }
      }
    }
  ]
}
```

#### Get Channel

```
GET /api/v1/channels/{channelId}
```

Retrieve a specific notification channel by ID.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| channelId | string | Unique identifier of the channel |

**Response:**

```json
{
  "id": "email",
  "name": "Email",
  "enabled": true,
  "config": {
    "smtpHost": "smtp.example.com",
    "smtpPort": 587,
    "smtpUsername": "notifications@example.com",
    "smtpSecurity": "tls",
    "smtpFrom": "Traccar Notifications <notifications@example.com>",
    "smtpReplyTo": "support@example.com"
  },
  "rateLimit": {
    "enabled": true,
    "maxPerMinute": 60,
    "maxPerHour": 1000
  },
  "retryPolicy": {
    "maxRetries": 3,
    "initialBackoff": 30,
    "maxBackoff": 300
  }
}
```

#### Update Channel

```
PUT /api/v1/channels/{channelId}
```

Update an existing notification channel configuration.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| channelId | string | Unique identifier of the channel |

**Request Body:**

```json
{
  "name": "Email Notifications",
  "enabled": true,
  "config": {
    "smtpHost": "smtp.example.com",
    "smtpPort": 587,
    "smtpUsername": "notifications@example.com",
    "smtpPassword": "password123",
    "smtpSecurity": "tls",
    "smtpFrom": "Traccar Alerts <alerts@example.com>",
    "smtpReplyTo": "support@example.com"
  },
  "rateLimit": {
    "enabled": true,
    "maxPerMinute": 100,
    "maxPerHour": 2000
  },
  "retryPolicy": {
    "maxRetries": 5,
    "initialBackoff": 60,
    "maxBackoff": 600
  }
}
```

**Response:**

```json
{
  "id": "email",
  "name": "Email Notifications",
  "enabled": true,
  "config": {
    "smtpHost": "smtp.example.com",
    "smtpPort": 587,
    "smtpUsername": "notifications@example.com",
    "smtpSecurity": "tls",
    "smtpFrom": "Traccar Alerts <alerts@example.com>",
    "smtpReplyTo": "support@example.com"
  },
  "rateLimit": {
    "enabled": true,
    "maxPerMinute": 100,
    "maxPerHour": 2000
  },
  "retryPolicy": {
    "maxRetries": 5,
    "initialBackoff": 60,
    "maxBackoff": 600
  }
}
```

#### Test Channel

```
POST /api/v1/channels/{channelId}/test
```

Send a test notification through a specific channel.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| channelId | string | Unique identifier of the channel |

**Request Body:**

```json
{
  "recipient": "user@example.com",
  "subject": "Test Notification",
  "body": "This is a test notification from the Traccar Notification Service."
}
```

**Response:**

```json
{
  "success": true,
  "messageId": "test-123456",
  "sentAt": "2023-06-16T14:30:00Z",
  "status": "sent"
}
```

### Manual Notifications

#### Send Notification

```
POST /api/v1/send
```

Send a manual notification to specified recipients.

**Request Body:**

```json
{
  "templateId": "1a2b3c4d",
  "channels": ["email", "sms"],
  "recipients": ["user1@example.com", "+12345678901"],
  "data": {
    "device": {
      "id": "device123",
      "name": "Device A"
    },
    "position": {
      "speed": 95,
      "time": "2023-06-16T15:30:45Z"
    },
    "speedLimit": 80
  }
}
```

**Response:**

```json
{
  "notifications": [
    {
      "id": "0j1k2l3m",
      "channel": "email",
      "recipient": "user1@example.com",
      "subject": "Speed Alert for Device A",
      "body": "Device A has exceeded the speed limit of 80 km/h. Current speed: 95 km/h at 2023-06-16T15:30:45Z.",
      "status": "pending",
      "sentAt": null,
      "deliveredAt": null
    },
    {
      "id": "1k2l3m4n",
      "channel": "sms",
      "recipient": "+12345678901",
      "body": "Device A has exceeded the speed limit of 80 km/h. Current speed: 95 km/h at 2023-06-16T15:30:45Z.",
      "status": "pending",
      "sentAt": null,
      "deliveredAt": null
    }
  ]
}
```

## Message-Based Interfaces

In addition to the REST API, the Notification Service provides asynchronous message-based interfaces for event-driven notification processing.

### Event Consumption

The Notification Service consumes events from the message broker to trigger notifications based on configured rules.

**Topic:** `events`

**Message Format:**

```json
{
  "id": "event789",
  "type": "deviceSpeedingEvent",
  "deviceId": "device123",
  "positionId": "position456",
  "geofenceId": null,
  "maintenanceId": null,
  "attributes": {
    "speed": 95,
    "speedLimit": 80
  },
  "timestamp": "2023-06-16T15:30:45Z"
}
```

### Notification Status Updates

The Notification Service publishes notification status updates to the message broker.

**Topic:** `notification-status`

**Message Format:**

```json
{
  "id": "0j1k2l3m",
  "eventId": "event789",
  "deviceId": "device123",
  "channel": "email",
  "recipient": "user1@example.com",
  "status": "delivered",
  "sentAt": "2023-06-16T15:31:00Z",
  "deliveredAt": "2023-06-16T15:31:05Z",
  "metadata": {
    "messageId": "SMTP-789012",
    "retryCount": 0
  },
  "timestamp": "2023-06-16T15:31:05Z"
}
```

## Data Models

### NotificationMessage

Represents a notification message with subject and body content.

```json
{
  "subject": "Speed Alert for Device A",
  "body": "Device A has exceeded the speed limit of 80 km/h. Current speed: 95 km/h at 2023-06-16T15:30:45Z."
}
```

### NotificationTemplate

Defines a template for generating notification content.

```json
{
  "id": "1a2b3c4d",
  "name": "Speed Alert",
  "type": "email",
  "subject": "Speed Alert for ${device.name}",
  "body": "Device ${device.name} has exceeded the speed limit of ${speedLimit} km/h. Current speed: ${position.speed} km/h at ${position.time}.",
  "variables": ["device.name", "speedLimit", "position.speed", "position.time"],
  "createdAt": "2023-06-15T10:30:00Z",
  "updatedAt": "2023-06-15T10:30:00Z"
}
```

### NotificationRule

Defines when and how notifications should be sent based on events.

```json
{
  "id": "4d5e6f7g",
  "name": "Speeding Notification",
  "eventType": "deviceSpeedingEvent",
  "deviceId": "device123",
  "templateId": "1a2b3c4d",
  "channels": ["email", "sms"],
  "recipients": ["user1@example.com", "+12345678901"],
  "active": true,
  "notifyOnce": false,
  "schedule": {
    "enabled": true,
    "days": [1, 2, 3, 4, 5],
    "startTime": "08:00:00",
    "endTime": "18:00:00",
    "timezone": "America/New_York"
  },
  "createdAt": "2023-06-15T11:30:00Z",
  "updatedAt": "2023-06-15T11:30:00Z"
}
```

### Notification

Represents a specific notification sent to a recipient.

```json
{
  "id": "7g8h9i0j",
  "eventId": "event123",
  "eventType": "deviceSpeedingEvent",
  "deviceId": "device123",
  "ruleId": "4d5e6f7g",
  "templateId": "1a2b3c4d",
  "channel": "email",
  "recipient": "user1@example.com",
  "subject": "Speed Alert for Device A",
  "body": "Device A has exceeded the speed limit of 80 km/h. Current speed: 95 km/h at 2023-06-15T12:30:45Z.",
  "status": "delivered",
  "sentAt": "2023-06-15T12:31:00Z",
  "deliveredAt": "2023-06-15T12:31:05Z",
  "metadata": {
    "ipAddress": "203.0.113.45",
    "userAgent": "Mozilla/5.0",
    "messageId": "SMTP-123456",
    "retryCount": 0
  }
}
```

### NotificationChannel

Defines a delivery channel for notifications.

```json
{
  "id": "email",
  "name": "Email",
  "enabled": true,
  "config": {
    "smtpHost": "smtp.example.com",
    "smtpPort": 587,
    "smtpUsername": "notifications@example.com",
    "smtpSecurity": "tls",
    "smtpFrom": "Traccar Notifications <notifications@example.com>",
    "smtpReplyTo": "support@example.com"
  },
  "rateLimit": {
    "enabled": true,
    "maxPerMinute": 60,
    "maxPerHour": 1000
  },
  "retryPolicy": {
    "maxRetries": 3,
    "initialBackoff": 30,
    "maxBackoff": 300
  }
}
```

## Error Handling

The Notification Service uses standard HTTP status codes to indicate the success or failure of API requests. In case of an error, the response body will contain additional information about the error.

### Error Response Format

```json
{
  "status": 400,
  "error": "Bad Request",
  "message": "Invalid template format",
  "details": "Subject field is required for email templates",
  "timestamp": "2023-06-16T16:45:30Z",
  "path": "/api/v1/templates",
  "correlationId": "abc-123-xyz-789"
}
```

### Common Error Codes

| Status Code | Description |
|-------------|-------------|
| 400 | Bad Request - The request was malformed or contained invalid parameters |
| 401 | Unauthorized - Authentication is required or failed |
| 403 | Forbidden - The authenticated user does not have permission to access the resource |
| 404 | Not Found - The requested resource was not found |
| 409 | Conflict - The request conflicts with the current state of the resource |
| 422 | Unprocessable Entity - The request was well-formed but contains semantic errors |
| 429 | Too Many Requests - Rate limit exceeded |
| 500 | Internal Server Error - An unexpected error occurred on the server |
| 503 | Service Unavailable - The service is temporarily unavailable |

### Error Types

#### MessageException

Thrown when there is an error processing or delivering a notification message.

```json
{
  "status": 500,
  "error": "Internal Server Error",
  "message": "Failed to send notification",
  "details": "SMTP server connection failed",
  "timestamp": "2023-06-16T16:50:30Z",
  "path": "/api/v1/send",
  "correlationId": "def-456-uvw-789"
}
```

## Rate Limiting

The Notification Service implements rate limiting to prevent abuse and ensure fair usage of resources. Rate limits are applied at multiple levels:

1. **API Rate Limiting**: Limits the number of API requests per client
2. **Channel Rate Limiting**: Limits the number of notifications sent through each channel
3. **Recipient Rate Limiting**: Limits the number of notifications sent to each recipient

### Rate Limit Headers

When rate limiting is applied, the following headers are included in the response:

```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1623861600
```

### Rate Limit Exceeded Response

When a rate limit is exceeded, the API returns a 429 Too Many Requests response:

```json
{
  "status": 429,
  "error": "Too Many Requests",
  "message": "Rate limit exceeded",
  "details": "You have exceeded the rate limit of 100 requests per minute",
  "timestamp": "2023-06-16T16:55:30Z",
  "path": "/api/v1/send",
  "correlationId": "ghi-789-rst-123",
  "retryAfter": 60
}
```

## Examples

### Creating a Template and Rule for Speed Alerts

1. Create a notification template for speed alerts:

```http
POST /api/v1/templates
Content-Type: application/json
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

{
  "name": "Speed Alert",
  "type": "email",
  "subject": "Speed Alert for ${device.name}",
  "body": "Device ${device.name} has exceeded the speed limit of ${speedLimit} km/h. Current speed: ${position.speed} km/h at ${position.time}."
}
```

2. Create a notification rule using the template:

```http
POST /api/v1/rules
Content-Type: application/json
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

{
  "name": "Speeding Notification",
  "eventType": "deviceSpeedingEvent",
  "deviceId": "device123",
  "templateId": "1a2b3c4d",
  "channels": ["email"],
  "recipients": ["user1@example.com"],
  "active": true,
  "notifyOnce": false,
  "schedule": {
    "enabled": true,
    "days": [1, 2, 3, 4, 5],
    "startTime": "08:00:00",
    "endTime": "18:00:00",
    "timezone": "America/New_York"
  }
}
```

### Sending a Manual Notification

```http
POST /api/v1/send
Content-Type: application/json
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

{
  "templateId": "1a2b3c4d",
  "channels": ["email", "sms"],
  "recipients": ["user1@example.com", "+12345678901"],
  "data": {
    "device": {
      "id": "device123",
      "name": "Device A"
    },
    "position": {
      "speed": 95,
      "time": "2023-06-16T15:30:45Z"
    },
    "speedLimit": 80
  }
}
```

### Configuring an Email Channel

```http
PUT /api/v1/channels/email
Content-Type: application/json
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

{
  "name": "Email Notifications",
  "enabled": true,
  "config": {
    "smtpHost": "smtp.example.com",
    "smtpPort": 587,
    "smtpUsername": "notifications@example.com",
    "smtpPassword": "password123",
    "smtpSecurity": "tls",
    "smtpFrom": "Traccar Alerts <alerts@example.com>",
    "smtpReplyTo": "support@example.com"
  },
  "rateLimit": {
    "enabled": true,
    "maxPerMinute": 100,
    "maxPerHour": 2000
  },
  "retryPolicy": {
    "maxRetries": 5,
    "initialBackoff": 60,
    "maxBackoff": 600
  }
}
```