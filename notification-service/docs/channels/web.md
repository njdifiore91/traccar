# Web Notification Channel

## Overview

The Web Notification Channel enables the Notification Service to deliver real-time notifications to connected web clients through WebSocket connections. This document explains how web notifications are configured, formatted, and delivered to users' browser sessions.

## Architecture

The Web Notification Channel uses a WebSocket-based approach to deliver notifications to connected web clients in real-time. The system follows these key architectural principles:

- **Real-time Delivery**: Notifications are pushed immediately to connected clients without polling
- **Connection Management**: Active WebSocket connections are tracked in a distributed registry
- **User-specific Routing**: Notifications are only delivered to the specific users they are intended for
- **Stateless Processing**: The notification service can scale horizontally with multiple instances

## Implementation Details

### NotificatorWeb Component

The `NotificatorWeb` class is the core component responsible for delivering notifications to web clients. It:

1. Receives notification events from the notification processing pipeline
2. Formats the notification content for web display
3. Uses the ConnectionManager to deliver the notification to connected user sessions

```java
@Singleton
public final class NotificatorWeb extends Notificator {

    private final ConnectionManager connectionManager;
    private final NotificationFormatter notificationFormatter;

    @Inject
    public NotificatorWeb(ConnectionManager connectionManager, NotificationFormatter notificationFormatter) {
        super(null, null);
        this.connectionManager = connectionManager;
        this.notificationFormatter = notificationFormatter;
    }

    @Override
    public void send(Notification notification, User user, Event event, Position position) {
        // Format and deliver notification to web client
        // ...
    }
}
```

### Event Formatting

When a notification is triggered, the `NotificatorWeb` component:

1. Creates a copy of the original event to avoid modifying the source
2. Preserves all essential event properties (ID, device ID, type, time, etc.)
3. Uses the `NotificationFormatter` to generate a human-readable message
4. Attaches the formatted message to the event object

The formatted event is then ready for delivery to the web client.

### Connection Management

The `ConnectionManager` handles the delivery of notifications to connected web clients:

1. Maintains a registry of active WebSocket connections indexed by user ID
2. Tracks connection state and session information
3. Routes notifications to the appropriate user connections
4. Handles connection lifecycle events (connect, disconnect, timeout)

In a distributed environment, the connection registry is maintained in Redis to allow multiple API Gateway instances to share connection information.

### WebSocket Delivery

Notifications are delivered to web clients through these steps:

1. The `NotificatorWeb` calls `connectionManager.updateEvent()` with the user ID and formatted event
2. The ConnectionManager looks up active connections for the specified user
3. The notification is serialized to JSON and sent through the WebSocket connection
4. The web client receives and displays the notification to the user

## Configuration

### Server Configuration

To enable web notifications, ensure the following configuration is set in your `notification-service.conf` file:

```properties
# Enable web notifications
notificators.web.enabled = true

# Configure connection registry (for distributed deployments)
connection.registry.type = redis
connection.registry.redis.host = redis-host
connection.registry.redis.port = 6379
```

### Client Integration

Web clients need to establish a WebSocket connection to receive notifications. The typical client-side implementation involves:

1. Establishing an authenticated WebSocket connection
2. Listening for notification events
3. Displaying notifications to the user

Example client code:

```javascript
// Establish WebSocket connection
const socket = new WebSocket('wss://your-server/api/socket');

// Listen for notifications
socket.onmessage = function(event) {
  const data = JSON.parse(event.data);
  
  // Handle notification events
  if (data.type === 'notification') {
    displayNotification(data.message);
  }
};

// Display notification to user
function displayNotification(message) {
  // Show notification in UI
  // ...
}
```

## Troubleshooting

### Common Issues

#### Notifications Not Being Delivered

**Possible causes:**
- WebSocket connection is not established or has disconnected
- User authentication has expired
- Connection registry is not properly configured
- Message broker connectivity issues

**Resolution steps:**
1. Verify WebSocket connection status in client browser console
2. Check user authentication and session validity
3. Inspect connection registry (Redis) for active connections
4. Verify message broker connectivity and topic subscriptions

#### Delayed Notifications

**Possible causes:**
- High system load or resource constraints
- Network latency between services
- Message broker queue backlog

**Resolution steps:**
1. Monitor system resource usage (CPU, memory, network)
2. Check message broker queue depths and processing rates
3. Verify network latency between notification service and API gateway

#### Duplicate Notifications

**Possible causes:**
- Multiple WebSocket connections for the same user
- Retry logic sending the same notification multiple times
- Client-side display logic issues

**Resolution steps:**
1. Check for multiple active sessions for the same user
2. Verify deduplication logic in notification processing
3. Implement client-side deduplication based on notification ID

### Monitoring

Monitor the health of the web notification channel using these metrics:

- **Active WebSocket Connections**: Total number of active connections
- **Notification Delivery Rate**: Notifications delivered per second
- **Delivery Success Rate**: Percentage of notifications successfully delivered
- **Delivery Latency**: Time from notification creation to client delivery

### Logs

Relevant log entries for troubleshooting web notifications:

```
# Successful notification delivery
INFO  [NotificatorWeb] Notification delivered to user 123 via web channel: EVENT_TYPE

# Failed notification delivery
WARN  [NotificatorWeb] Failed to deliver notification to user 123: No active connections

# Connection registry issues
ERROR [ConnectionManager] Failed to access connection registry: Connection refused
```

## Performance Considerations

- Each active WebSocket connection consumes server resources
- For high-volume deployments, consider scaling the API Gateway horizontally
- The connection registry (Redis) should be sized appropriately for the expected number of concurrent users
- Implement proper connection cleanup to prevent resource leaks

## Security Considerations

- All WebSocket connections must be authenticated
- Notifications should only contain information the user is authorized to access
- Consider implementing message signing for sensitive notifications
- Use TLS for all WebSocket connections to prevent eavesdropping