# Notification Service Architecture

## 1. Overview

The Notification Service is a critical component of the Traccar microservices architecture, responsible for managing and delivering alerts through multiple channels based on events detected in the system. This service consumes events from the message broker, applies notification rules, and delivers notifications to users through various channels including email, SMS, push notifications, web notifications, and external services like Telegram and Pushover.

The Notification Service implements a resilient, scalable architecture with comprehensive error handling and retry mechanisms to ensure reliable notification delivery even when external services experience degradation or outages.

## 2. Architecture Components

The Notification Service consists of the following key components:

### 2.1 Core Components

- **Notification Subscriber**: Consumes events from the message broker's notifications topic and initiates the notification processing pipeline.

- **Notification Selection**: Filters events based on age, matching rules, and calendar schedules to determine if a notification should be triggered.

- **Recipient Determination**: Identifies users who should receive notifications based on permissions, user-notification links, and user preferences.

- **Content Preparation**: Formats notification content using templates, includes position data, address information, and prepares content for different delivery channels.

- **Channel Distribution**: Routes notifications to appropriate delivery channels (email, SMS, push, web, external services).

- **Dead Letter Queue Handler**: Processes failed notification deliveries for retry or administrative review.

### 2.2 Supporting Components

- **Health Check & Metrics Endpoints**: Exposes service health and performance metrics for monitoring systems.

- **Service Discovery Registration**: Registers the service with the service discovery system for dynamic endpoint resolution.

- **Circuit Breakers**: Protects the service from cascading failures when external notification services are degraded.

- **Retry Service**: Implements consistent retry strategies with exponential backoff across all notification channels.

## 3. Message Flow

The notification processing follows a well-defined flow:

1. **Event Consumption**: The Event Processing Service publishes events to the "notifications" topic in the Message Broker.

2. **Notification Selection**:
   - Events are checked for age (too old events are skipped)
   - Events are matched against notification rules
   - Calendar schedules are checked to determine if notifications should be sent

3. **Recipient Determination**:
   - User-notification links are retrieved
   - Permissions are checked to filter recipients
   - User preferences are applied
   - A final recipient list is built

4. **Content Preparation**:
   - The NotificationFormatter applies templates
   - Position data is included if available
   - Address information is added
   - Content is formatted for different channels

5. **Channel Distribution**:
   - Notifications are routed to appropriate channels:
     - Email notifications are published to the email-out topic
     - SMS notifications are published to the sms-out topic
     - Push notifications are published to the push-out topic
     - Web notifications are published to the web-out topic
     - External service notifications (Telegram, Pushover, etc.) are sent via REST calls protected by circuit breakers

6. **Delivery Status Tracking**:
   - Successful deliveries are marked as delivered
   - Failed deliveries are published to the dead-letter-queue topic for retry

## 4. Notification Channels

The Notification Service supports multiple delivery channels, each implemented as a dedicated handler:

### 4.1 Email Notifications

Handled by `NotificatorMail`, which:
- Checks if SMTP is properly configured
- Uses an SMTP client to send emails
- Falls back to logging if email cannot be sent
- Publishes failed deliveries to the dead-letter queue

### 4.2 SMS Notifications

Handled by `NotificatorSms`, which:
- Supports multiple SMS providers (HTTP Gateway, AWS SNS)
- Uses circuit breakers to protect against provider outages
- Implements provider-specific error handling
- Publishes failed deliveries to the dead-letter queue

### 4.3 Push Notifications

Handled by `NotificatorFirebase`, which:
- Uses Firebase Admin SDK to send push notifications
- Manages device tokens for targeted delivery
- Implements Firebase-specific error handling
- Publishes failed deliveries to the dead-letter queue

### 4.4 Web Notifications

Handled by `NotificatorWeb`, which:
- Finds active user sessions
- Sends notifications via WebSocket connections
- Handles disconnected sessions gracefully
- Publishes failed deliveries to the dead-letter queue

### 4.5 External Service Notifications

Handled by various notificators including:
- `NotificatorTelegram`: Sends notifications via Telegram Bot API
- `NotificatorPushover`: Sends notifications via Pushover API
- `NotificatorCommand`: Sends device commands
- `NotificatorTraccar`: Sends notifications to the Traccar mobile app

All external service calls are protected by circuit breakers to prevent cascading failures.

## 5. Resilience Patterns

The Notification Service implements several resilience patterns to ensure reliable operation:

### 5.1 Circuit Breakers

All external API calls (SMS gateways, Firebase, Telegram, Pushover) are protected by circuit breakers that:
- Monitor failure rates of external service calls
- Trip open when failure thresholds are exceeded
- Prevent cascading failures by failing fast
- Automatically transition to half-open state after a configurable delay
- Allow limited test calls in half-open state to check if the underlying issue is resolved

Circuit breakers are implemented using Resilience4j with service-specific configurations.

### 5.2 Retry Mechanisms

The service implements a centralized Retry Service that:
- Applies consistent retry strategies across all notification channels
- Implements exponential backoff with jitter to prevent thundering herd problems
- Maintains correlation IDs across retry attempts for tracing
- Respects different retry policies based on error types
- Implements maximum retry limits to prevent infinite retry loops

### 5.3 Dead Letter Queue

Failed notification deliveries are published to a dead-letter queue that:
- Preserves the original notification context and correlation ID
- Enables administrative review of failed notifications
- Supports manual or automated reprocessing of failed notifications
- Provides insights into common failure patterns

### 5.4 Graceful Degradation

The service implements graceful degradation strategies:
- Prioritizes critical notifications during high load
- Falls back to alternative delivery channels when primary channels fail
- Implements notification batching and deduplication under load
- Caches templates and frequently used data to reduce external dependencies

## 6. Integration with Message Broker

The Notification Service integrates with the message broker (Kafka/RabbitMQ) for asynchronous communication:

### 6.1 Consumed Topics

- **notifications**: Events published by the Event Processing Service that may trigger notifications

### 6.2 Published Topics

- **email-out**: Email notifications for delivery
- **sms-out**: SMS notifications for delivery
- **push-out**: Push notifications for delivery
- **web-out**: Web notifications for delivery
- **dead-letter-queue**: Failed notification deliveries for retry
- **notification-status**: Delivery status updates for monitoring

### 6.3 Message Schemas

All messages use standardized schemas defined in Protocol Buffers:
- `notification.proto`: Defines the structure of notification messages
- `event.proto`: Defines the structure of event messages that trigger notifications

## 7. Service Discovery Integration

The Notification Service registers with the service discovery system (Consul/Kubernetes) to enable dynamic endpoint resolution:

### 7.1 Service Registration

During startup, the service registers with the discovery system providing:
- Service identification (name, version, instance ID)
- Endpoint information (host, port, protocol)
- Health check configuration (URL, interval, timeout)
- Service metadata (tags, environment, dependencies)

### 7.2 Health Checks

The service exposes health endpoints that report:
- Service operational status
- Dependency availability (message broker, external services)
- Resource utilization metrics
- Service-specific health indicators

### 7.3 Service Resolution

The service uses the discovery system to locate other services it needs to communicate with, such as:
- External notification services
- Monitoring and tracing services
- Administrative interfaces

## 8. Error Handling and Recovery

The Notification Service implements comprehensive error handling and recovery mechanisms:

### 8.1 Error Classification

Errors are classified into specific categories:
- **Network Errors**: Transient connectivity issues
- **Authentication Errors**: Invalid credentials for external services
- **Rate Limiting Errors**: Throttling by external services
- **Service Unavailable Errors**: External service outages
- **Invalid Format Errors**: Content formatting issues
- **Message Broker Errors**: Issues with message publishing/consuming
- **Service Discovery Errors**: Problems locating required services

### 8.2 Recovery Strategies

Different recovery strategies are applied based on error type:
- **Retry with Backoff**: For transient errors
- **Administrative Alerts**: For credential and configuration issues
- **Throttling Awareness**: Adaptive rate limiting based on provider responses
- **Service Status Checks**: Active monitoring of external service health
- **Content Validation**: Pre-delivery format checking
- **Broker Health Monitoring**: Pause and resume based on broker availability
- **Cached Endpoints**: Fallback to cached service endpoints when discovery fails

### 8.3 Correlation ID Propagation

Every notification request generates a unique correlation ID that is:
- Propagated through all services and included in logs
- Maintained across retry attempts
- Used for distributed tracing across service boundaries
- Exposed through tracing endpoints for troubleshooting

## 9. Diagrams

### 9.1 Event to Notification Process

```mermaid
flowchart TD
    subgraph "Event Processing Service"
        A[Event Generated] --> B[Event Stored]
        B --> C[Message Publisher]
        C --> MBE[Publish to notifications topic]
    end
    
    subgraph "Message Broker"
        MBE --> MBT[(notifications topic)]
    end
    
    subgraph "Notification Service"
        MBT --> NS[Notification Subscriber]
        
        subgraph "Notification Selection"
            NS --> D{Event Age Check}
            D -->|Too Old| E[Skip]
            D -->|Recent| F{Matching Rules?}
            F -->|No| E
            F -->|Yes| G{Calendar Check}
            G -->|Outside Schedule| E
            G -->|Within Schedule| H[Notification Triggered]
        end
        
        subgraph "Recipient Determination"
            H --> I[Get User-Notification Links]
            I --> J[Filter by Permission]
            J --> K[Apply User Preferences]
            K --> L[Build Recipient List]
        end
        
        subgraph "Content Preparation"
            L --> M[NotificationFormatter]
            M --> N[Apply Templates]
            N --> O[Include Position Data]
            O --> P[Add Address Info]
            P --> Q[Format for Channels]
        end
        
        subgraph "Channel Distribution"
            Q --> R{Channel Type}
            R -->|Email| MB1[Publish to email-out topic]
            R -->|SMS| MB2[Publish to sms-out topic]
            R -->|Push| MB3[Publish to push-out topic]
            R -->|Web| MB4[Publish to web-out topic]
            R -->|External| CB[REST w/ Circuit Breaker]
            CB --> EXT[External Notificator Services]
        end
        
        HC[/Health Check & Metrics Endpoints\]
        SD[/Service Discovery Registration\]
    end
    
    subgraph "Message Broker Output"
        MB1 --> EMB[(email-out topic)]
        MB2 --> SMB[(sms-out topic)]
        MB3 --> PMB[(push-out topic)]
        MB4 --> WMB[(web-out topic)]
    end
```

### 9.2 Multi-Channel Notification Delivery

```mermaid
flowchart TD
    subgraph "Message Broker Topics"
        T1[(email-out topic)]
        T2[(sms-out topic)]
        T3[(push-out topic)]
        T4[(web-out topic)]
        T5[(dead-letter-queue topic)]
    end
    
    subgraph "Notification Service"
        subgraph "Email Handler"
            T1 --> E1[NotificatorMail]
            E1 --> E2{SMTP Configured?}
            E2 -->|Yes| E3[SMTP Client]
            E2 -->|No| E4[Log Fallback]
            E3 --> E5[Send Email]
            E5 --> E6{Success?}
            E6 -->|No| E7[Publish to DLQ]
            E7 --> T5
            E4 --> E8[Log Message]
            E6 -->|Yes| E9[Mark Delivered]
        end
    
        subgraph "SMS Handler"
            T2 --> S1[NotificatorSms]
            S1 --> S2{SMS Provider}
            S2 -->|HTTP Gateway| S3[HTTP Client w/ Circuit Breaker]
            S2 -->|AWS SNS| S4[SNS Client w/ Circuit Breaker]
            S3 --> S5[Send SMS via HTTP]
            S4 --> S6[Send SMS via SNS]
            S5 --> S7{Success?}
            S6 --> S7
            S7 -->|No| S8[Publish to DLQ]
            S8 --> T5
            S7 -->|Yes| S9[Mark Delivered]
        end
    
        subgraph "Push Handler"
            T3 --> P1[NotificatorFirebase]
            P1 --> P2[Firebase Admin SDK]
            P2 --> P3[FCM Message]
            P3 --> P4[Device Tokens]
            P4 --> P5[Send Push Notification]
            P5 --> P6{Success?}
            P6 -->|No| P7[Publish to DLQ]
            P7 --> T5
            P6 -->|Yes| P8[Mark Delivered]
        end
    
        subgraph "Web Handler"
            T4 --> W1[NotificatorWeb]
            W1 --> W2[Find Active Sessions]
            W2 --> W3[WebSocket Connection]
            W3 --> W4[Send UI Notification]
            W4 --> W5{Success?}
            W5 -->|No| W6[Publish to DLQ]
            W6 --> T5
            W5 -->|Yes| W7[Mark Delivered]
        end
        
        subgraph "External Service Handler"
            X1[REST Controller] --> X2{Service Type}
            X2 -->|Telegram| X3[NotificatorTelegram]
            X2 -->|Pushover| X4[NotificatorPushover]
            X2 -->|Command| X5[NotificatorCommand]
            X3 --> X6[Telegram Bot API w/ Circuit Breaker]
            X4 --> X7[Pushover API w/ Circuit Breaker]
            X5 --> X8[Device Command]
            X6 --> X9{Success?}
            X7 --> X9
            X8 --> X9
            X9 -->|No| X10[Publish to DLQ]
            X10 --> T5
            X9 -->|Yes| X11[Mark Delivered]
        end

        DLQ[Dead Letter Queue Handler]
        T5 --> DLQ
    end
```

### 9.3 Error Handling and Retry Mechanisms

```mermaid
flowchart TD
    subgraph "Error Detection"
        A[Notification Failure] --> B{Error Type}
        B -->|Network| F[Transient Error]
        B -->|Authentication| G[Credential Error]
        B -->|Rate Limiting| H[Throttling Error]
        B -->|Service Unavailable| I[Service Error]
        B -->|Invalid Format| J[Content Error]
        B -->|Broker Failure| BF[Message Broker Error]
        B -->|Service Discovery| SD[Discovery Error]
    end
    
    subgraph "Notification Service"
        subgraph "Retry Service"
            F --> K{Retry Policy}
            G --> L[Log Authentication Issue]
            H --> M[Apply Backoff]
            I --> N[Service Status Check]
            J --> O[Content Validation]
            BF --> BP[Broker Health Check]
            SD --> SDP[Refresh Service Registry]
            
            K -->|Retry| P[Queue for Retry]
            K -->|No Retry| Q[Log Failure]
            L --> R[Alert Administrator]
            M --> S{Max Retries?}
            N --> S
            O --> T[Fallback Format]
            BP --> BPR{Broker Ready?}
            SDP --> SDPR{Registry Available?}
            
            P --> U[Delay Based on Attempt]
            S -->|Under Limit| P
            S -->|Limit Reached| Q
            T --> V{Valid Alternative?}
            V -->|Yes| W[Retry with Fallback]
            V -->|No| Q
            BPR -->|Yes| P
            BPR -->|No| BPA[Wait for Broker]
            SDPR -->|Yes| P
            SDPR -->|No| SDPA[Use Cached Endpoints]
            
            R --> X[Mark for Review]
            U --> Y[Attach Correlation ID]
            Y --> Z[Retry Delivery]
            W --> Y
            BPA -->|Broker Restored| P
            BPA -->|Timeout| Q
            SDPA --> U
        end
        
        subgraph "Dead Letter Handler"
            DLQ[Consume from Dead Letter Queue] --> DLA[Parse Message with Context]
            DLA --> DLB[Extract Correlation ID]
            DLB --> DLC[Log Failure Details]
            DLC --> DLD{Categorize Error}
            DLD --> DLE[Apply Recovery Strategy]
            DLE --> DLF[Send to Retry Service]
            DLF --> P
        end
        
        subgraph "Distributed Tracing"
            ZZ[Original Request] --> ZA[Generate Correlation ID]
            ZA --> ZB[Propagate through Services]
            ZB --> ZC[Log with Context]
            ZC --> ZD[Expose Tracing Endpoints]
            ZD --> ZE[Aggregate in Monitoring]
        end
    end
    
    Z --> CB{Success?}
    CB -->|Yes| CC[Notification Delivered]
    CB -->|No| CD{Critical Failure?}
    CD -->|Yes| CE[Alert Operations]
    CD -->|No| CF[Return to Retry Policy]
    CF --> K
```

## 10. Implementation Details

### 10.1 Notificator Manager

The `NotificatorManager` class is responsible for managing different notification types and instantiating the appropriate notificator based on the notification type:

```java
@Singleton
public class NotificatorManager {

    private static final Map<String, Class<? extends Notificator>> NOTIFICATORS_ALL = Map.of(
            "command", NotificatorCommand.class,
            "web", NotificatorWeb.class,
            "mail", NotificatorMail.class,
            "sms", NotificatorSms.class,
            "firebase", NotificatorFirebase.class,
            "traccar", NotificatorTraccar.class,
            "telegram", NotificatorTelegram.class,
            "pushover", NotificatorPushover.class);

    // ... implementation details
}
```

### 10.2 Base Notificator

The `Notificator` abstract class provides the foundation for all notification channel implementations:

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

### 10.3 Notification Formatting

The `NotificationFormatter` class handles the preparation of notification content using templates and context data:

```java
@Singleton
public class NotificationFormatter {

    private final CacheManager cacheManager;
    private final TextTemplateFormatter textTemplateFormatter;

    // ... implementation details

    public NotificationMessage formatMessage(
            Notification notification, User user, Event event, Position position, String templatePath) {
        // Prepare context with server, user, device, event, position, etc.
        // Apply template formatting
        // Return formatted notification message
    }
}
```

## 11. Conclusion

The Notification Service architecture provides a robust, scalable solution for delivering notifications through multiple channels. By leveraging asynchronous messaging, circuit breakers, and comprehensive error handling, the service ensures reliable notification delivery even in the face of external service degradation or outages.

Key architectural benefits include:

- **Loose coupling** with other services through message broker integration
- **Resilience** through circuit breakers and retry mechanisms
- **Scalability** through independent processing of different notification channels
- **Observability** through health checks, metrics, and distributed tracing
- **Flexibility** to add new notification channels without modifying existing code

This architecture supports the Traccar platform's mission-critical notification requirements while providing a foundation for future enhancements and additional notification channels.