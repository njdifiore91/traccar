# Pushover Notification Channel

## Overview

Pushover is a service that makes it easy to send real-time notifications to your Android and iOS devices, Android Wear and Apple Watch, as well as desktops. The Notification Service integrates with Pushover's API to deliver instant notifications to mobile devices and desktops.

This document covers the configuration, usage, and troubleshooting of the Pushover notification channel in the Traccar Notification Service.

### Key Features

- **Cross-Platform Delivery**: Send notifications to iOS, Android, and desktop devices
- **User-Specific Configuration**: Configure different Pushover settings for each user
- **Device Targeting**: Send notifications to specific devices
- **Reliable Delivery**: Notifications are delivered even when devices are offline and synchronized when they reconnect
- **Simple Integration**: Easy to configure with just an application token and user key

## Configuration

### API Credentials

To use the Pushover notification channel, you need to configure the following credentials:

1. **Application Token**: A unique identifier for your Traccar application in Pushover
2. **User Key**: The user or group key that will receive notifications

These credentials are configured in the Notification Service configuration:

```properties
notificator.pushover.token=YOUR_APPLICATION_TOKEN
notificator.pushover.user=DEFAULT_USER_KEY
```

### Creating a Pushover Application

To obtain an application token:

1. Log in to your Pushover account at [pushover.net](https://pushover.net)
2. Navigate to the bottom of your dashboard and click "Create an Application/API Token"
3. Fill in the required information:
   - Name: "Traccar" (or your preferred name)
   - Type: Application
   - Description: "GPS tracking notifications"
   - URL: Your Traccar server URL (optional)
   - Icon: Upload a custom icon (optional)
4. After creating the application, you'll receive an API token/key to use in your configuration

### User Key

The user key is found on your Pushover dashboard after logging in. This key identifies which user or group will receive the notifications.

### User-Specific Configuration

The Notification Service supports user-specific Pushover configuration through user attributes:

- `pushoverUserKey`: A user-specific Pushover user key that overrides the default key
- `pushoverDeviceNames`: Comma-separated list of device names to target (e.g., "phone,desktop,tablet")

When these attributes are set for a user, the Notification Service will use them instead of the default configuration.

## Priority Levels

Pushover supports different priority levels for notifications. The Traccar Notification Service currently sends notifications with standard priority, but understanding the available priorities is helpful:

| Priority | Value | Description |
|----------|-------|-------------|
| Lowest   | -2    | Silent notification, appears in the app without alerting the user |
| Low      | -1    | Quiet notification, appears without sound or vibration |
| Normal   | 0     | Standard notification with sound and vibration (default) |
| High     | 1     | High-priority notification that bypasses quiet hours |
| Emergency| 2     | Emergency notification that repeats until acknowledged |

### Emergency Priority Considerations

Emergency priority (2) notifications have special requirements:

- They require additional parameters (`retry` and `expire`)
- They will repeat until acknowledged by the user
- They generate a receipt that can be used to check acknowledgment status
- They should be used sparingly for truly critical situations

## Implementation Details

### Message Construction

The `NotificatorPushover` class constructs a JSON payload with the following fields:

- `token`: The application token
- `user`: The user key (from user attributes or default configuration)
- `device`: Device names to target (optional)
- `title`: The notification subject
- `message`: The notification body

```java
// Example from NotificatorPushover.java
public class Message {
    @JsonProperty("token")
    private String token;
    @JsonProperty("user")
    private String user;
    @JsonProperty("device")
    private String device;
    @JsonProperty("title")
    private String title;
    @JsonProperty("message")
    private String message;
}
```

### API Request

The notification is sent as an HTTP POST request to the Pushover API endpoint:

```
https://api.pushover.net/1/messages.json
```

The request includes the JSON payload with the notification details. The Notification Service uses Jakarta RESTful Web Services (JAX-RS) client to perform the HTTP request:

```java
// Example from NotificatorPushover.java
client.target(url).request().post(Entity.json(message)).close();
```

This sends the message object as JSON in the request body and closes the response after receiving it.

## Troubleshooting

### Common Issues

1. **Notifications not being delivered**:
   - Verify your application token and user key are correct
   - Check if the user has the Pushover app installed and set up
   - Ensure the user's devices are online and connected
   - Verify the user hasn't reached their monthly notification limit

2. **Incorrect device targeting**:
   - Check the `pushoverDeviceNames` attribute format (comma-separated, no spaces)
   - Verify the device names match exactly as they appear in the Pushover account
   - Ensure targeted devices are registered and active in Pushover

3. **API errors**:
   - HTTP 400 errors typically indicate invalid parameters (check token and user key)
   - HTTP 429 errors indicate rate limiting (too many requests in a short period)
   - HTTP 500 errors indicate server issues with Pushover

### Debugging Steps

1. Test sending a notification directly from the Pushover dashboard to verify the account is working
2. Check the Notification Service logs for any error responses from the Pushover API
3. Verify network connectivity from the Notification Service to the Pushover API
4. If using IPv6, try forcing IPv4 connections as some servers have IPv6 connectivity issues
5. Check for notification delays:
   - Ensure your device isn't in battery optimization mode that restricts background services
   - Verify that your device has a stable internet connection
   - Check if the Pushover app is running in the foreground (notifications may not make sounds when the app is open)
6. For emergency priority notifications that aren't repeating properly:
   - Verify that the `retry` and `expire` parameters are properly configured
   - Check if the notification has been acknowledged on another device

### API Response Codes

The Pushover API returns a JSON response with a `status` field:

- `status=1`: Success
- `status=0`: Error (check the `errors` array in the response)

Common error messages include:

- `invalid_token`: The application token is invalid
- `invalid_user`: The user key is invalid or the user is not active
- `message_too_long`: The message exceeds the maximum length
- `user_exceeded_quota`: The user has reached their monthly notification limit

## Advanced Configuration

### Circuit Breaker Integration

In the microservices architecture, the Notification Service implements a circuit breaker pattern for external API calls, including Pushover. This prevents cascading failures when the Pushover API is experiencing issues:

- If the Pushover API becomes unresponsive or returns errors, the circuit breaker will open after a threshold of failures
- While open, requests to Pushover will fail fast without attempting to contact the API
- After a cooldown period, the circuit breaker will allow test requests to determine if the API has recovered
- When the API is stable again, the circuit breaker closes and normal operation resumes

### Message Broker Integration

Notifications are processed asynchronously through a message broker:

1. Events that trigger notifications are published to the `notifications` topic
2. The Notification Service subscribes to this topic and processes incoming events
3. For external notificators like Pushover, messages are processed with circuit breaker protection
4. Failed deliveries are published to a dead-letter queue for later retry

## References

- [Pushover API Documentation](https://pushover.net/api)
- [Pushover Application Dashboard](https://pushover.net/apps)
- [Pushover Support Knowledge Base](https://support.pushover.net/)
- [Notification Service Architecture](../architecture.md)