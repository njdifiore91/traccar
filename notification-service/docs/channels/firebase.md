# Firebase Push Notification Channel

## Overview

The Firebase push notification channel enables the Traccar Notification Service to deliver real-time alerts to mobile devices through Firebase Cloud Messaging (FCM). This document details the implementation, configuration, and troubleshooting of Firebase push notifications within the Traccar microservices architecture.

## Integration Architecture

The Firebase notification channel is implemented as part of the Notification Service's channel distribution system. When an event occurs that requires notification, the system processes it through the following workflow:

1. Event Processing Service detects an event and publishes it to the notifications topic
2. Notification Service consumes the event and determines appropriate notification channels
3. For mobile recipients, the Firebase channel constructs and sends push notifications via FCM
4. FCM delivers the notification to registered mobile devices

## Configuration Requirements

### Firebase Project Setup

Before using the Firebase notification channel, you must:

1. Create a Firebase project in the [Firebase Console](https://console.firebase.google.com/)
2. Generate a private key for your Firebase service account:
   - Navigate to Project Settings > Service Accounts
   - Click "Generate New Private Key"
   - Save the JSON file securely

### Notification Service Configuration

The Firebase notification channel requires the following configuration in the Notification Service:

```properties
# Firebase configuration
notificator.firebase.enabled=true
notificator.firebase.serviceAccountKey=/path/to/firebase-service-account.json
```

The service account key file must be accessible to the Notification Service container.

## Device Token Management

### Token Registration

Mobile devices register with FCM to receive a unique registration token. These tokens must be stored in the Traccar system to enable push notification delivery.

User device tokens are stored in the `notificationTokens` attribute of the User entity as a comma-separated list. Multiple devices per user are supported.

### Token Validation and Cleanup

The Firebase notification channel automatically validates tokens during message delivery:

1. When sending notifications, the system tracks which tokens result in errors
2. Invalid or unregistered tokens are automatically removed from the user's token list
3. The updated token list is persisted to the database
4. The user cache is invalidated to ensure all services see the updated token list

This self-cleaning mechanism ensures that notification delivery remains efficient by removing tokens that are no longer valid.

## Notification Payload Structure

The Firebase notification channel constructs a `MulticastMessage` with the following components:

### Basic Notification

```java
Notification.builder()
    .setTitle(message.getSubject())
    .setBody(message.getBody())
    .build()
```

### Android-Specific Configuration

```java
AndroidConfig.builder()
    .setNotification(AndroidNotification.builder()
        .setSound("default")
        .build())
    .build()
```

### iOS-Specific Configuration

```java
ApnsConfig.builder()
    .setAps(Aps.builder()
        .setSound("default")
        .build())
    .build()
```

### Event Data

When notifications are triggered by events, the event ID is included in the data payload:

```java
messageBuilder.putData("eventId", String.valueOf(event.getId()));
```

This allows mobile applications to fetch additional information about the event that triggered the notification.

## Sending Process

### Message Construction and Delivery

The `NotificatorFirebase` class handles the construction and delivery of push notifications:

1. Retrieves the user's device tokens from the `notificationTokens` attribute
2. Constructs a `MulticastMessage` with appropriate notification content and configuration
3. Sends the message to all registered tokens using `sendEachForMulticast`
4. Processes the response to identify successful and failed deliveries

### Error Handling

The Firebase notification channel implements robust error handling:

1. Each token's delivery result is individually tracked
2. For failed deliveries, the error code is examined to determine the cause
3. Tokens that fail due to `INVALID_ARGUMENT` or `UNREGISTERED` errors are removed from the user's token list
4. The updated token list is persisted to the database
5. All errors are logged with appropriate context for troubleshooting

## Troubleshooting

### Common Issues

#### Notifications Not Being Delivered

1. **Invalid Firebase Configuration**
   - Verify the service account key file is correctly formatted and accessible
   - Check that the Firebase project has FCM API enabled

2. **Invalid Device Tokens**
   - Ensure mobile applications are correctly registering with FCM
   - Verify tokens are being properly stored in the user's `notificationTokens` attribute

3. **Network Connectivity Issues**
   - Check network connectivity between the Notification Service and FCM servers
   - Verify firewall rules allow outbound connections to FCM endpoints

#### Error Codes and Meanings

| Error Code | Description | Action Required |
|------------|-------------|------------------|
| `INVALID_ARGUMENT` | The registration token is not a valid FCM registration token | Token is automatically removed |
| `UNREGISTERED` | The registration token is no longer valid | Token is automatically removed |
| `SENDER_ID_MISMATCH` | The registration token does not match the sender ID | Verify correct Firebase project configuration |
| `QUOTA_EXCEEDED` | Messaging quota exceeded | Implement rate limiting or request quota increase |
| `UNAVAILABLE` | FCM servers are temporarily unavailable | Implement retry mechanism with backoff |
| `INTERNAL` | Internal FCM error | Report to Firebase support if persistent |

### Debugging Strategies

1. **Enable Debug Logging**
   - Set the logging level for `org.traccar.notificators.NotificatorFirebase` to DEBUG
   - Review logs for detailed information about message construction and delivery

2. **Token Validation**
   - Manually verify token validity using the Firebase Admin SDK
   - Check token format and length (FCM tokens are typically 140+ characters)

3. **Test Notifications**
   - Use the Firebase Console to send test messages to specific tokens
   - Compare results with notifications sent through the Traccar system

4. **Monitor FCM Status**
   - Check the [Firebase Status Dashboard](https://status.firebase.google.com/) for service disruptions

## References

- [Firebase Cloud Messaging Documentation](https://firebase.google.com/docs/cloud-messaging)
- [Firebase Admin SDK for Java](https://firebase.google.com/docs/admin/setup)
- [FCM HTTP v1 API Reference](https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages)