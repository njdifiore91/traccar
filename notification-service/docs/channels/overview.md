# Notification Channels Overview

## Introduction

The Notification Service is a critical component in Traccar's microservices architecture, responsible for delivering alerts and notifications through multiple channels based on events detected in the system. This document provides a comprehensive overview of the notification channel system, explaining the core components, interfaces, and workflow that enable reliable notification delivery.

## Base Notificator Class

At the heart of the notification system is the abstract `Notificator` class, which serves as the foundation for all channel-specific implementations. This class provides common functionality and a standardized interface for all notification channels.

```java
public abstract class Notificator {

    private final NotificationFormatter notificationFormatter;
    private final String templatePath;

    public Notificator(NotificationFormatter notificationFormatter, String templatePath) {
        this.notificationFormatter = notificationFormatter;
        this.templatePath = templatePath;
    }

    public void send(Notification notification, User user, Event event, Position position) throws MessageException {
        var message = notificationFormatter.formatMessage(notification, user, event, position, templatePath);
        send(user, message, event, position);
    }

    public void send(User user, NotificationMessage message, Event event, Position position) throws MessageException {
        throw new UnsupportedOperationException();
    }
}
```

Key responsibilities of the `Notificator` class:

1. **Template-based Formatting**: Uses the `NotificationFormatter` to apply templates to notification content, ensuring consistent formatting across channels.

2. **Channel-agnostic Interface**: Provides a common `send()` method that channel-specific implementations must override.

3. **Context Propagation**: Passes all relevant context (user, event, position) to the formatter and channel-specific implementation.

4. **Error Handling**: Propagates `MessageException` for notification delivery failures.

## Notification Workflow in Microservices Architecture

The notification process in the microservices architecture follows an event-driven pattern:

1. **Event Detection**: The Event Processing Service detects events (geofence transitions, speed violations, etc.) and publishes them to the message broker's "events" topic.

2. **Event Consumption**: The Notification Service subscribes to the "events" topic and processes incoming events.

3. **Notification Selection**:
   - The service checks if the event is recent enough to process
   - It matches the event against notification rules
   - It verifies if the notification is within scheduled delivery times

4. **Recipient Determination**:
   - The service identifies users linked to the notification
   - It filters recipients based on permissions
   - It applies user preferences for notification channels

5. **Content Preparation**:
   - The `NotificationFormatter` applies templates to create channel-specific content
   - Position data and address information are included
   - Content is formatted appropriately for each delivery channel

6. **Channel Distribution**:
   - The service publishes formatted notifications to channel-specific topics in the message broker:
     - `email-out` topic for email notifications
     - `sms-out` topic for SMS notifications
     - `push-out` topic for push notifications
     - `web-out` topic for web UI notifications
   - External notification services (Telegram, Pushover, etc.) are invoked via REST calls protected by circuit breakers

7. **Delivery Confirmation**: Each channel handler reports delivery status back to the message broker for tracking and potential retry.

## Channel-Specific Handlers

The Notification Service implements several channel-specific handlers, each consuming from its dedicated message broker topic:

### Email Handler

The `NotificatorMail` implementation sends notifications via email using SMTP:

- Consumes from the `email-out` topic
- Configures SMTP client based on system settings
- Formats emails with HTML or plain text content
- Handles delivery failures with appropriate error reporting

### SMS Handler

The `NotificatorSms` implementation delivers notifications via SMS:

- Consumes from the `sms-out` topic
- Supports multiple SMS providers (HTTP gateways, AWS SNS)
- Implements provider-specific formatting and authentication
- Uses circuit breakers to prevent cascading failures when providers are unavailable

### Push Notification Handler

The `NotificatorFirebase` implementation sends push notifications to mobile devices:

- Consumes from the `push-out` topic
- Uses Firebase Admin SDK to send messages via FCM
- Manages device tokens and handles token invalidation
- Supports notification payload customization

### Web UI Handler

The `NotificatorWeb` implementation delivers notifications to the web interface:

- Consumes from the `web-out` topic
- Finds active user sessions
- Sends notifications via WebSocket connections
- Handles disconnected sessions appropriately

### External Service Handlers

Implementations for third-party services like Telegram (`NotificatorTelegram`) and Pushover (`NotificatorPushover`):

- Exposed via REST endpoints for synchronous notification requests
- Implement service-specific APIs with appropriate authentication
- Use circuit breakers to handle external service failures
- Support custom formatting for each platform

## Message Broker Integration

The Notification Service leverages a message broker (Kafka/RabbitMQ) for reliable, asynchronous notification processing:

1. **Topic-Based Communication**:
   - Consumes events from the `events` topic
   - Publishes to channel-specific topics (`email-out`, `sms-out`, `push-out`, `web-out`)
   - Uses a dead-letter queue (`dead-letter-queue`) for failed deliveries

2. **Message Schema**:
   - Standardized message formats using Protocol Buffers
   - Includes all necessary context for notification processing
   - Supports versioning for backward compatibility

3. **Delivery Guarantees**:
   - At-least-once delivery semantics
   - Message persistence for reliability
   - Ordered processing within partitions

4. **Error Handling**:
   - Failed deliveries are published to a dead-letter queue
   - Includes original message context and failure reason
   - Supports manual or automated reprocessing

## Common Interfaces and Components

The notification system relies on several shared interfaces and components:

### NotificationFormatter

Responsible for applying templates to notification content:

- Supports multiple template formats (Velocity, Freemarker)
- Handles internationalization and localization
- Provides context variables for dynamic content

### NotificationMessage

Represents a formatted notification ready for delivery:

- Contains subject and body content
- Includes attachments if applicable
- Stores metadata for delivery tracking

### MessageException

Standardized exception for notification delivery failures:

- Provides error categorization
- Includes context for troubleshooting
- Supports retry decision logic

## Resilience and Error Handling

The notification system implements several resilience patterns:

1. **Circuit Breakers**: Prevent cascading failures when external services are degraded.

2. **Retry Mechanisms**: Implement exponential backoff for transient failures.

3. **Dead Letter Queue**: Store failed notifications for later analysis and reprocessing.

4. **Fallback Strategies**: Provide alternative delivery methods when primary channels fail.

5. **Rate Limiting**: Prevent overwhelming external services with too many requests.

## Conclusion

The notification channel system in Traccar's microservices architecture provides a flexible, reliable mechanism for delivering alerts through multiple channels. By leveraging a common base class, standardized interfaces, and asynchronous message-based communication, the system ensures consistent notification delivery while maintaining the resilience and scalability benefits of the microservices approach.

Channel-specific implementations extend the base functionality to address the unique requirements of each notification medium, while the overall workflow ensures that notifications are properly formatted, delivered to the right recipients, and tracked for successful completion.