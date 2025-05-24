# Traccar Push Notification Channel

## Overview

The Traccar push notification channel enables the Notification Service to deliver push notifications to Traccar mobile applications (Android and iOS). This document explains how to configure and use the Traccar push notification system, including API configuration, device token management, notification payload structure, and troubleshooting common issues.

## Configuration

### Required Configuration

The Traccar push notification channel requires an API key for authentication with the Traccar push service. Configure this key in your application properties:

```properties
notificator.traccar.key=your-traccar-push-api-key
```

This key is used to authenticate requests to the Traccar push notification service at `https://www.traccar.org/push/`.

## Device Token Management

### Token Storage

Device tokens (also known as registration tokens) are stored in the user's attributes under the key `notificationTokens`. Multiple tokens for a single user (representing multiple devices) are stored as a comma or space-separated list:

```
token1,token2,token3
```

or

```
token1 token2 token3
```

### Token Validation and Cleanup

The Traccar push notification channel automatically handles token validation and cleanup:

1. When a notification is sent, the system checks the response from the Traccar push service
2. If a token is invalid or no longer registered, it is automatically removed from the user's attributes
3. If all tokens for a user become invalid, the `notificationTokens` attribute is completely removed

This automatic cleanup ensures that notifications are only sent to valid devices and prevents unnecessary API calls.

## Notification Payload Structure

The Traccar push notification channel constructs a JSON payload with the following structure:

```json
{
  "registration_ids": ["token1", "token2", ...],
  "notification": {
    "title": "Notification Title",
    "body": "Notification Body",
    "sound": "default"
  }
}
```

Where:
- `registration_ids`: Array of device tokens to receive the notification
- `notification.title`: The notification title (derived from the notification message subject)
- `notification.body`: The notification content (derived from the notification message body)
- `notification.sound`: The sound to play (always set to "default")

## Implementation Details

The Traccar push notification channel is implemented in the `NotificatorTraccar` class, which:

1. Extracts device tokens from the user's attributes
2. Constructs the notification payload
3. Sends an HTTP POST request to the Traccar push API
4. Processes the response to handle any token errors
5. Updates the user's tokens if any are invalid

## Troubleshooting

### Common Issues

#### Notifications Not Being Delivered

1. **Invalid API Key**: Ensure your `notificator.traccar.key` is correctly configured
2. **Missing Device Tokens**: Verify that users have valid `notificationTokens` in their attributes
3. **Network Issues**: Check connectivity to `https://www.traccar.org/push/`

#### Error Codes

The Traccar push service may return the following error codes:

- `messaging/invalid-argument`: The token format is invalid
- `messaging/registration-token-not-registered`: The token is no longer valid (device uninstalled the app or cleared data)

These errors are automatically handled by removing the invalid tokens from the user's attributes.

### Logging

The Traccar push notification channel logs warnings for push errors. Check your application logs for messages like:

```
WARN  [NotificatorTraccar] Push user {userId} error - {error message}
```

or

```
WARN  [NotificatorTraccar] Push error
```

These log messages can help identify issues with the push notification delivery.

## References

- [Traccar Mobile Apps](https://www.traccar.org/client/)
- [Firebase Cloud Messaging](https://firebase.google.com/docs/cloud-messaging) (underlying technology used by Traccar push service)