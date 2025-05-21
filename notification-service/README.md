# Traccar Notification Service

## Overview

The Notification Service is a critical component of the Traccar GPS tracking system's microservices architecture. It is responsible for managing and delivering alerts through multiple channels based on events detected in the system. This service ensures that users receive timely notifications about important events related to their tracked devices.

## Features

- Multi-channel notification delivery (Email, SMS, Push, Web, Telegram, etc.)
- Template-based message formatting
- Notification prioritization and rate limiting
- Delivery retry mechanisms with exponential backoff
- Dead letter queue for failed notifications
- Circuit breaker pattern for external notification services
- Support for user preferences and notification rules

## Architecture

The Notification Service follows a message-driven architecture that processes events from the Event Processing Service and delivers notifications through various channels:

```
Event Processing Service → Message Broker → Notification Service → Notification Channels
```

### Integration Points

- **Inbound**: Consumes events from the message broker (Kafka/RabbitMQ)
- **Outbound**: 
  - Delivers notifications through multiple channels (Email, SMS, Push, etc.)
  - Publishes delivery status back to the message broker
  - Exposes REST API for direct notification requests

### Notification Flow

1. Event messages are consumed from the message broker
2. Events are filtered based on notification rules and user preferences
3. Notification content is formatted using templates
4. Notifications are routed to appropriate channel handlers
5. Delivery attempts are made with retry mechanisms for failures
6. Delivery status is published back to the message broker

## Supported Notification Channels

| Channel | Description | Configuration Key |
|---------|-------------|-------------------|
| Email | SMTP-based email delivery | `mail.*` |
| SMS | SMS gateway integration | `sms.*` |
| Firebase | Push notifications via Firebase Cloud Messaging | `firebase.*` |
| Web | In-app notifications via WebSocket | `web.*` |
| Telegram | Telegram bot integration | `telegram.*` |
| Pushover | Pushover notification service | `pushover.*` |
| Discord | Discord webhook integration | `discord.*` |
| Slack | Slack webhook integration | `slack.*` |
| Command | Device command execution | `command.*` |

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------||
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` |
| `RABBITMQ_HOST` | RabbitMQ host | `localhost` |
| `RABBITMQ_PORT` | RabbitMQ port | `5672` |
| `RABBITMQ_USERNAME` | RabbitMQ username | `guest` |
| `RABBITMQ_PASSWORD` | RabbitMQ password | `guest` |
| `NOTIFICATION_RETRY_ATTEMPTS` | Maximum retry attempts for failed notifications | `3` |
| `NOTIFICATION_RETRY_DELAY` | Initial delay between retries (ms) | `1000` |
| `NOTIFICATION_RETRY_MULTIPLIER` | Backoff multiplier for retry delays | `2.0` |
| `NOTIFICATION_RETRY_MAX_DELAY` | Maximum delay between retries (ms) | `60000` |
| `NOTIFICATION_TEMPLATE_PATH` | Path to notification templates | `/templates` |
| `MAIL_SMTP_HOST` | SMTP server host | - |
| `MAIL_SMTP_PORT` | SMTP server port | `25` |
| `MAIL_SMTP_USERNAME` | SMTP authentication username | - |
| `MAIL_SMTP_PASSWORD` | SMTP authentication password | - |
| `MAIL_SMTP_FROM` | Default sender email address | - |
| `MAIL_SMTP_AUTH` | Enable SMTP authentication | `false` |
| `MAIL_SMTP_STARTTLS_ENABLE` | Enable STARTTLS | `false` |
| `MAIL_SMTP_SSL_ENABLE` | Enable SSL for SMTP | `false` |
| `SMS_PROVIDER` | SMS gateway provider (http, aws) | - |
| `SMS_HTTP_URL` | HTTP SMS gateway URL | - |
| `SMS_HTTP_AUTHORIZATION` | HTTP SMS gateway authorization header | - |
| `SMS_AWS_REGION` | AWS region for SNS | - |
| `SMS_AWS_ACCESS_KEY` | AWS access key for SNS | - |
| `SMS_AWS_SECRET_KEY` | AWS secret key for SNS | - |
| `FIREBASE_SERVICE_ACCOUNT_PATH` | Path to Firebase service account JSON | - |
| `TELEGRAM_BOT_TOKEN` | Telegram bot API token | - |
| `PUSHOVER_USER_KEY` | Pushover user/group key | - |
| `PUSHOVER_APP_TOKEN` | Pushover application token | - |

### Configuration File

The service can also be configured using a YAML configuration file:

```yaml
messaging:
  type: kafka  # or rabbitmq
  kafka:
    bootstrapServers: localhost:9092
    consumer:
      groupId: notification-service
      autoOffsetReset: earliest
    producer:
      acks: all
  rabbitmq:
    host: localhost
    port: 5672
    username: guest
    password: guest
    virtualHost: /

notification:
  retry:
    attempts: 3
    initialDelay: 1000
    multiplier: 2.0
    maxDelay: 60000
  templatePath: /templates

mail:
  smtp:
    host: smtp.example.com
    port: 587
    username: user
    password: password
    from: noreply@example.com
    auth: true
    starttlsEnable: true
    sslEnable: false

sms:
  provider: http
  http:
    url: https://sms-gateway.example.com/send
    authorization: Bearer token
  aws:
    region: us-east-1
    accessKey: your-access-key
    secretKey: your-secret-key

firebase:
  serviceAccountPath: /path/to/firebase-service-account.json

telegram:
  botToken: your-telegram-bot-token

pushover:
  userKey: your-user-key
  appToken: your-app-token
```

## Deployment

### Kubernetes Deployment

The Notification Service can be deployed to Kubernetes using the provided Helm chart or YAML manifests.

#### Using Helm

```bash
helm install notification-service ./setup/helm/charts/notification
```

#### Using kubectl

```bash
kubectl apply -f notification-service/kubernetes/notification-service.yaml
```

Example Kubernetes manifest:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: notification-service
  labels:
    app: notification-service
spec:
  replicas: 2
  selector:
    matchLabels:
      app: notification-service
  template:
    metadata:
      labels:
        app: notification-service
    spec:
      containers:
      - name: notification-service
        image: traccar/notification-service:latest
        ports:
        - containerPort: 8080
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka:9092"
        - name: NOTIFICATION_RETRY_ATTEMPTS
          value: "3"
        livenessProbe:
          httpGet:
            path: /actuator/health/liveness
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 15
        readinessProbe:
          httpGet:
            path: /actuator/health/readiness
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "750m"
---
apiVersion: v1
kind: Service
metadata:
  name: notification-service
spec:
  selector:
    app: notification-service
  ports:
  - port: 8080
    targetPort: 8080
  type: ClusterIP
```

### Standalone Deployment

The service can also be run as a standalone Java application:

```bash
java -jar notification-service.jar --spring.config.location=file:./config.yml
```

Or using Docker:

```bash
docker run -d \
  --name notification-service \
  -p 8080:8080 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -v /path/to/config.yml:/app/config.yml \
  traccar/notification-service:latest
```

## Message Broker Integration

The Notification Service integrates with message brokers (Kafka or RabbitMQ) for event-driven communication with other services.

### Kafka Topics

| Topic | Description | Direction |
|-------|-------------|----------|
| `events` | System events that may trigger notifications | Consume |
| `notification-status` | Notification delivery status updates | Produce |
| `email-out` | Email notifications for delivery | Produce |
| `sms-out` | SMS notifications for delivery | Produce |
| `push-out` | Push notifications for delivery | Produce |
| `web-out` | Web notifications for delivery | Produce |
| `dead-letter-queue` | Failed notifications after retry exhaustion | Produce |

### RabbitMQ Queues

| Queue | Description | Direction |
|-------|-------------|----------|
| `events` | System events that may trigger notifications | Consume |
| `notification.status` | Notification delivery status updates | Produce |
| `notification.email` | Email notifications for delivery | Produce |
| `notification.sms` | SMS notifications for delivery | Produce |
| `notification.push` | Push notifications for delivery | Produce |
| `notification.web` | Web notifications for delivery | Produce |
| `notification.dlq` | Failed notifications after retry exhaustion | Produce |

## Monitoring and Health Checks

The Notification Service exposes health check and metrics endpoints for monitoring:

- `/actuator/health` - Overall service health
- `/actuator/health/liveness` - Liveness check for Kubernetes
- `/actuator/health/readiness` - Readiness check for Kubernetes
- `/actuator/metrics` - Prometheus-compatible metrics

Key metrics include:

- `notification.delivery.attempts` - Number of notification delivery attempts
- `notification.delivery.success` - Number of successful notification deliveries
- `notification.delivery.failure` - Number of failed notification deliveries
- `notification.retry.count` - Number of notification retries
- `notification.processing.time` - Notification processing time

## Troubleshooting

### Common Issues

#### Notification Service Not Consuming Events

- Verify message broker connectivity
- Check consumer group configuration
- Ensure the events topic exists and has messages

#### Email Notifications Not Being Sent

- Verify SMTP server configuration
- Check email templates exist and are valid
- Look for authentication failures in logs

#### SMS Notifications Failing

- Verify SMS provider configuration
- Check API credentials and rate limits
- Look for HTTP error responses in logs

### Logs

The service uses structured JSON logging with the following log levels:

- `ERROR` - Critical issues preventing notification delivery
- `WARN` - Non-critical issues that may affect some notifications
- `INFO` - Normal operational events
- `DEBUG` - Detailed information for troubleshooting

Example log output:

```json
{
  "timestamp": "2023-06-01T12:34:56.789Z",
  "level": "INFO",
  "thread": "notification-consumer-1",
  "logger": "org.traccar.notification.service.NotificationService",
  "message": "Notification delivered successfully",
  "notification_id": "550e8400-e29b-41d4-a716-446655440000",
  "channel": "email",
  "recipient": "user@example.com",
  "event_id": "123456",
  "traceId": "4fd0b6aa1bc7e711",
  "spanId": "b7ad6b7169203331"
}
```

## API Reference

The Notification Service exposes a REST API for direct notification requests:

### Send Notification

```
POST /api/notifications
```

Request body:

```json
{
  "userId": 123,
  "deviceId": 456,
  "type": "deviceOverspeed",
  "attributes": {
    "speed": 120.5,
    "speedLimit": 100.0,
    "deviceName": "Truck 42"
  },
  "channels": ["email", "sms"]
}
```

Response:

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "queued"
}
```

### Get Notification Status

```
GET /api/notifications/{id}
```

Response:

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "delivered",
  "channels": {
    "email": "delivered",
    "sms": "delivered"
  },
  "timestamp": "2023-06-01T12:34:56.789Z"
}
```

## License

Apache License 2.0