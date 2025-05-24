# Notification Service Operations Guide

## 1. Introduction

The Notification Service is a critical component of the Traccar microservices architecture responsible for delivering notifications across multiple channels including email, SMS, push notifications, web notifications, and external notification services. This guide provides comprehensive information for deploying, configuring, monitoring, and troubleshooting the Notification Service in production environments.

### 1.1 Service Overview

The Notification Service:

- Consumes notification events from a message broker (Kafka/RabbitMQ)
- Processes notifications based on rules, schedules, and user preferences
- Delivers notifications through multiple channels (email, SMS, push, web, external services)
- Implements retry mechanisms for failed deliveries
- Provides monitoring endpoints for health and performance metrics

### 1.2 Architecture Context

The Notification Service operates within the broader microservices ecosystem:

- Receives notification requests from the Event Processing Service via message broker
- Publishes to channel-specific topics for asynchronous processing
- Integrates with external notification providers (SMTP, SMS gateways, Firebase, etc.)
- Registers with service discovery (Consul or Kubernetes)
- Reports metrics, logs, and traces to centralized observability systems

## 2. Deployment

### 2.1 Prerequisites

Before deploying the Notification Service, ensure the following prerequisites are met:

- Kubernetes cluster (v1.24+) with Helm (v3.8+)
- Message broker (Kafka 3.6.1+ or RabbitMQ 3.12.12+) deployed and accessible
- Service discovery mechanism (Kubernetes DNS or Consul 1.17.0+)
- Observability infrastructure (Prometheus, ELK Stack, Jaeger/Zipkin)
- External notification service credentials (SMTP, SMS gateway, Firebase, etc.)

### 2.2 Kubernetes Deployment

The Notification Service is deployed using Helm charts:

```bash
# Add the Traccar Helm repository
helm repo add traccar https://charts.traccar.org
helm repo update

# Deploy the Notification Service
helm install notification-service traccar/notification-service \
  --namespace traccar \
  --values custom-values.yaml
```

#### 2.2.1 Helm Chart Configuration

Create a `custom-values.yaml` file to override default configuration:

```yaml
# Basic service configuration
replicaCount: 2

# Container resources
resources:
  requests:
    cpu: 500m
    memory: 512Mi
  limits:
    cpu: 1000m
    memory: 1Gi

# Horizontal Pod Autoscaler
horizontalPodAutoscaler:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70

# Service configuration
service:
  type: ClusterIP
  port: 8080
  metricsPort: 8081

# Message broker configuration
messageBroker:
  type: kafka  # or rabbitmq
  kafka:
    bootstrapServers: kafka-headless:9092
    consumerGroupId: notification-service
    topics:
      notifications: notifications
      emailOut: email-out
      smsOut: sms-out
      pushOut: push-out
      webOut: web-out
      deadLetterQueue: notification-dlq
  rabbitmq:
    host: rabbitmq
    port: 5672
    username: user
    password: password
    virtualHost: /
    queues:
      notifications: notifications
      emailOut: email-out
      smsOut: sms-out
      pushOut: push-out
      webOut: web-out
      deadLetterQueue: notification-dlq

# External notification services
notificationChannels:
  email:
    enabled: true
    smtp:
      host: smtp.example.com
      port: 587
      username: user
      password: password
      from: notifications@example.com
      starttls: true
  sms:
    enabled: true
    provider: http  # or aws-sns
    http:
      url: https://sms-gateway.example.com/send
      method: POST
      headers:
        Authorization: Bearer token
    awsSns:
      region: us-east-1
      accessKey: your-access-key
      secretKey: your-secret-key
  push:
    enabled: true
    firebase:
      credentialsFile: firebase-credentials.json
  web:
    enabled: true

# Observability configuration
observability:
  prometheus:
    enabled: true
  jaeger:
    enabled: true
    endpoint: http://jaeger-collector:14268/api/traces
  logging:
    level: INFO
    format: json
```

### 2.3 Verifying Deployment

After deployment, verify that the service is running correctly:

```bash
# Check pod status
kubectl get pods -n traccar -l app=notification-service

# Check service status
kubectl get svc -n traccar -l app=notification-service

# Check logs
kubectl logs -n traccar -l app=notification-service

# Check health endpoint
kubectl port-forward -n traccar svc/notification-service 8081:8081
curl http://localhost:8081/actuator/health
```

## 3. Configuration

### 3.1 Environment Variables

The Notification Service can be configured using the following environment variables:

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `SPRING_PROFILES_ACTIVE` | Active Spring profiles | `production` | No |
| `SERVER_PORT` | HTTP server port | `8080` | No |
| `MANAGEMENT_SERVER_PORT` | Management/metrics port | `8081` | No |
| `LOG_LEVEL` | Root logging level | `INFO` | No |
| `LOG_FORMAT` | Logging format (json or plain) | `json` | No |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | - | Yes (if using Kafka) |
| `KAFKA_CONSUMER_GROUP_ID` | Kafka consumer group ID | `notification-service` | No |
| `RABBITMQ_HOST` | RabbitMQ host | - | Yes (if using RabbitMQ) |
| `RABBITMQ_PORT` | RabbitMQ port | `5672` | No |
| `RABBITMQ_USERNAME` | RabbitMQ username | - | Yes (if using RabbitMQ) |
| `RABBITMQ_PASSWORD` | RabbitMQ password | - | Yes (if using RabbitMQ) |
| `RABBITMQ_VIRTUAL_HOST` | RabbitMQ virtual host | `/` | No |
| `NOTIFICATION_TOPIC` | Notification topic/queue name | `notifications` | No |
| `EMAIL_OUT_TOPIC` | Email output topic/queue | `email-out` | No |
| `SMS_OUT_TOPIC` | SMS output topic/queue | `sms-out` | No |
| `PUSH_OUT_TOPIC` | Push output topic/queue | `push-out` | No |
| `WEB_OUT_TOPIC` | Web output topic/queue | `web-out` | No |
| `DLQ_TOPIC` | Dead letter queue topic/queue | `notification-dlq` | No |
| `SMTP_HOST` | SMTP server host | - | Yes (for email) |
| `SMTP_PORT` | SMTP server port | `587` | No |
| `SMTP_USERNAME` | SMTP username | - | Yes (for email) |
| `SMTP_PASSWORD` | SMTP password | - | Yes (for email) |
| `SMTP_FROM` | SMTP from address | - | Yes (for email) |
| `SMTP_STARTTLS` | Enable STARTTLS | `true` | No |
| `SMS_PROVIDER` | SMS provider (http or aws-sns) | `http` | No |
| `SMS_HTTP_URL` | SMS HTTP gateway URL | - | Yes (for HTTP SMS) |
| `SMS_HTTP_METHOD` | SMS HTTP method | `POST` | No |
| `SMS_HTTP_HEADERS` | SMS HTTP headers (JSON) | `{}` | No |
| `AWS_REGION` | AWS region for SNS | - | Yes (for AWS SNS) |
| `AWS_ACCESS_KEY` | AWS access key | - | Yes (for AWS SNS) |
| `AWS_SECRET_KEY` | AWS secret key | - | Yes (for AWS SNS) |
| `FIREBASE_CREDENTIALS_FILE` | Path to Firebase credentials | - | Yes (for push) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OpenTelemetry collector endpoint | - | No |
| `OTEL_SERVICE_NAME` | Service name for tracing | `notification-service` | No |
| `OTEL_TRACES_SAMPLER` | Tracing sampler | `parentbased_traceidratio` | No |
| `OTEL_TRACES_SAMPLER_ARG` | Sampling ratio (0.0-1.0) | `0.1` | No |
| `NOTIFICATOR_TYPES` | Enabled notification types | `web,mail,sms,firebase,traccar,telegram,pushover` | No |

### 3.2 Configuration Files

In addition to environment variables, the service can be configured using the following files:

#### 3.2.1 application.yml

The main configuration file for the service:

```yaml
spring:
  application:
    name: notification-service
  profiles:
    active: ${SPRING_PROFILES_ACTIVE:production}

server:
  port: ${SERVER_PORT:8080}

management:
  server:
    port: ${MANAGEMENT_SERVER_PORT:8081}
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  endpoint:
    health:
      show-details: when_authorized
      group:
        liveness:
          include: livenessState
        readiness:
          include: readinessState,db,messageBroker

logging:
  level:
    root: ${LOG_LEVEL:INFO}
    org.traccar: ${LOG_LEVEL_org.traccar:INFO}
  pattern:
    console: ${LOG_FORMAT:json}

traccar:
  notification:
    types: ${NOTIFICATOR_TYPES:web,mail,sms,firebase,traccar,telegram,pushover}
    retry:
      maxAttempts: 3
      initialDelay: 1000
      multiplier: 2.0
      maxDelay: 60000
```

#### 3.2.2 logback-spring.xml

Detailed logging configuration:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder class="net.logstash.logback.encoder.LogstashEncoder" />
    </appender>
    
    <root level="${LOG_LEVEL:-INFO}">
        <appender-ref ref="CONSOLE" />
    </root>
    
    <logger name="org.traccar" level="${LOG_LEVEL_org.traccar:-INFO}" />
</configuration>
```

### 3.3 Notification Channel Configuration

#### 3.3.1 Email Configuration

Email notifications require SMTP server configuration:

```yaml
traccar:
  notification:
    email:
      host: ${SMTP_HOST}
      port: ${SMTP_PORT:587}
      username: ${SMTP_USERNAME}
      password: ${SMTP_PASSWORD}
      from: ${SMTP_FROM}
      starttls: ${SMTP_STARTTLS:true}
      ssl: ${SMTP_SSL:false}
      timeout: ${SMTP_TIMEOUT:10000}
      templates:
        path: /app/templates/email
```

#### 3.3.2 SMS Configuration

SMS notifications can be configured for HTTP gateway or AWS SNS:

```yaml
traccar:
  notification:
    sms:
      provider: ${SMS_PROVIDER:http}
      http:
        url: ${SMS_HTTP_URL}
        method: ${SMS_HTTP_METHOD:POST}
        headers: ${SMS_HTTP_HEADERS:{}}
        bodyTemplate: '{"to":"${to}","message":"${message}"}'
      aws:
        region: ${AWS_REGION}
        accessKey: ${AWS_ACCESS_KEY}
        secretKey: ${AWS_SECRET_KEY}
```

#### 3.3.3 Push Notification Configuration

Push notifications require Firebase configuration:

```yaml
traccar:
  notification:
    push:
      firebase:
        credentialsFile: ${FIREBASE_CREDENTIALS_FILE}
        databaseUrl: ${FIREBASE_DATABASE_URL:https://traccar.firebaseio.com}
```

## 4. Monitoring and Observability

### 4.1 Health Checks

The Notification Service exposes health check endpoints that can be used by Kubernetes probes and monitoring systems:

- **Liveness Probe**: `/actuator/health/live`
- **Readiness Probe**: `/actuator/health/ready`
- **Startup Probe**: `/actuator/health/startup`
- **Overall Health**: `/actuator/health`

Example Kubernetes probe configuration:

```yaml
livenessProbe:
  httpGet:
    path: /actuator/health/live
    port: 8081
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /actuator/health/ready
    port: 8081
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3

startupProbe:
  httpGet:
    path: /actuator/health/startup
    port: 8081
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 12
```

### 4.2 Metrics

The Notification Service exposes Prometheus-compatible metrics at `/actuator/prometheus` on the management port (default: 8081). Key metrics include:

| Metric | Type | Description |
|--------|------|-------------|
| `traccar_notifications_received_total` | Counter | Total number of notifications received |
| `traccar_notifications_processed_total` | Counter | Total number of notifications processed |
| `traccar_notifications_failed_total` | Counter | Total number of failed notifications |
| `traccar_notifications_processing_time_seconds` | Histogram | Notification processing time |
| `traccar_email_sent_total` | Counter | Total number of emails sent |
| `traccar_email_failed_total` | Counter | Total number of failed emails |
| `traccar_sms_sent_total` | Counter | Total number of SMS sent |
| `traccar_sms_failed_total` | Counter | Total number of failed SMS |
| `traccar_push_sent_total` | Counter | Total number of push notifications sent |
| `traccar_push_failed_total` | Counter | Total number of failed push notifications |
| `traccar_web_sent_total` | Counter | Total number of web notifications sent |
| `traccar_web_failed_total` | Counter | Total number of failed web notifications |
| `traccar_external_sent_total` | Counter | Total number of external notifications sent |
| `traccar_external_failed_total` | Counter | Total number of failed external notifications |
| `traccar_dlq_published_total` | Counter | Total number of messages published to DLQ |
| `traccar_retry_attempts_total` | Counter | Total number of retry attempts |
| `jvm_memory_used_bytes` | Gauge | JVM memory usage |
| `jvm_threads_states_threads` | Gauge | JVM thread states |
| `process_cpu_usage` | Gauge | Process CPU usage |
| `system_cpu_usage` | Gauge | System CPU usage |

### 4.3 Logging

The Notification Service uses structured JSON logging that integrates with centralized logging systems like the ELK Stack. Log entries include the following standard fields:

- `timestamp`: ISO-8601 timestamp
- `level`: Log level (INFO, WARN, ERROR, etc.)
- `logger`: Logger name
- `thread`: Thread name
- `message`: Log message
- `exception`: Exception details (if applicable)
- `service`: Service name (notification-service)
- `traceId`: Distributed tracing ID
- `spanId`: Span ID for the current operation

Example log entry:

```json
{
  "@timestamp": "2023-05-24T12:34:56.789Z",
  "level": "INFO",
  "logger": "org.traccar.notification.NotificationProcessor",
  "thread": "kafka-consumer-1",
  "message": "Processed notification: id=123, type=deviceOffline",
  "service": "notification-service",
  "traceId": "1234567890abcdef",
  "spanId": "abcdef1234567890",
  "notification": {
    "id": 123,
    "type": "deviceOffline",
    "userId": 456,
    "deviceId": 789
  }
}
```

### 4.4 Distributed Tracing

The Notification Service implements distributed tracing using OpenTelemetry, which can be visualized in Jaeger or Zipkin. Key traced operations include:

- Message consumption from notification topic
- Notification processing pipeline stages
- Channel-specific notification delivery
- External service calls (SMTP, SMS gateway, Firebase, etc.)
- Retry attempts and error handling

Tracing is configured via environment variables:

```yaml
OTEL_EXPORTER_OTLP_ENDPOINT: http://jaeger-collector:14268/api/traces
OTEL_SERVICE_NAME: notification-service
OTEL_TRACES_SAMPLER: parentbased_traceidratio
OTEL_TRACES_SAMPLER_ARG: 0.1
OTEL_PROPAGATORS: tracecontext,baggage,b3
```

### 4.5 Alerting

Recommended Prometheus alerting rules for the Notification Service:

```yaml
groups:
- name: notification-service-alerts
  rules:
  - alert: NotificationServiceDown
    expr: up{job="notification-service"} == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "Notification Service is down"
      description: "Notification Service instance has been down for more than 1 minute."

  - alert: HighNotificationFailureRate
    expr: sum(rate(traccar_notifications_failed_total[5m])) / sum(rate(traccar_notifications_processed_total[5m])) > 0.1
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High notification failure rate"
      description: "Notification failure rate is above 10% for the last 5 minutes."

  - alert: NotificationProcessingDelay
    expr: histogram_quantile(0.95, sum(rate(traccar_notifications_processing_time_seconds_bucket[5m])) by (le)) > 10
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Notification processing delay"
      description: "95th percentile of notification processing time is above 10 seconds."

  - alert: DeadLetterQueueGrowing
    expr: rate(traccar_dlq_published_total[5m]) > 0
    for: 15m
    labels:
      severity: warning
    annotations:
      summary: "Dead Letter Queue is growing"
      description: "Messages are being published to the Dead Letter Queue for the last 15 minutes."

  - alert: HighCpuUsage
    expr: process_cpu_usage{job="notification-service"} > 0.8
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High CPU usage"
      description: "Notification Service is using more than 80% CPU for the last 5 minutes."

  - alert: HighMemoryUsage
    expr: sum(jvm_memory_used_bytes{job="notification-service",area="heap"}) / sum(jvm_memory_max_bytes{job="notification-service",area="heap"}) > 0.8
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High memory usage"
      description: "Notification Service is using more than 80% of heap memory for the last 5 minutes."
```

## 5. Troubleshooting

### 5.1 Common Issues and Solutions

#### 5.1.1 Service Won't Start

**Symptoms:**
- Pod is in CrashLoopBackOff state
- Logs show startup errors

**Possible Causes and Solutions:**

1. **Message Broker Connectivity Issues**
   - Check if Kafka/RabbitMQ is accessible
   - Verify credentials and connection parameters
   - Check network policies and firewall rules

2. **Configuration Errors**
   - Verify environment variables are correctly set
   - Check for syntax errors in configuration files
   - Ensure required configuration is provided

3. **Resource Constraints**
   - Check if the pod has sufficient CPU and memory
   - Increase resource limits if necessary

#### 5.1.2 Notifications Not Being Processed

**Symptoms:**
- No errors in logs
- Notifications are not being delivered
- Metrics show low or zero processing rate

**Possible Causes and Solutions:**

1. **Message Broker Topic/Queue Issues**
   - Verify topic/queue exists and is correctly configured
   - Check consumer group status (for Kafka)
   - Verify message format and schema

2. **Consumer Not Running**
   - Check if consumer is active and subscribed
   - Verify consumer group ID is correct
   - Check for errors in consumer initialization

3. **Message Filtering**
   - Check if notifications are being filtered out by rules
   - Verify calendar/schedule settings
   - Check user notification preferences

#### 5.1.3 Email Notifications Failing

**Symptoms:**
- Logs show SMTP errors
- Metrics show high email failure rate
- Emails not being received

**Possible Causes and Solutions:**

1. **SMTP Configuration Issues**
   - Verify SMTP server address and port
   - Check username and password
   - Ensure STARTTLS/SSL settings are correct

2. **Email Content Issues**
   - Check for template rendering errors
   - Verify email format is valid
   - Check for missing required fields

3. **SMTP Server Restrictions**
   - Check for rate limiting by SMTP provider
   - Verify sender domain is authorized
   - Check for IP blacklisting

#### 5.1.4 SMS Notifications Failing

**Symptoms:**
- Logs show SMS gateway errors
- Metrics show high SMS failure rate
- SMS not being received

**Possible Causes and Solutions:**

1. **SMS Gateway Configuration Issues**
   - Verify gateway URL and authentication
   - Check request format and parameters
   - Ensure phone numbers are in correct format

2. **Rate Limiting or Quota Issues**
   - Check for rate limiting by SMS provider
   - Verify account has sufficient credits/quota
   - Implement backoff strategy for retries

3. **Network Connectivity Issues**
   - Check network connectivity to SMS gateway
   - Verify outbound internet access is available
   - Check for firewall or proxy issues

#### 5.1.5 Push Notifications Failing

**Symptoms:**
- Logs show Firebase errors
- Metrics show high push failure rate
- Push notifications not being received

**Possible Causes and Solutions:**

1. **Firebase Configuration Issues**
   - Verify Firebase credentials file is correct
   - Check Firebase project settings
   - Ensure service account has proper permissions

2. **Device Token Issues**
   - Verify device tokens are valid and current
   - Check for expired or invalid tokens
   - Implement token refresh mechanism

3. **Message Format Issues**
   - Check for payload size limits
   - Verify notification format is correct
   - Ensure required fields are present

### 5.2 Diagnostic Commands

#### 5.2.1 Checking Service Status

```bash
# Check pod status
kubectl get pods -n traccar -l app=notification-service

# Describe pod for detailed status
kubectl describe pod -n traccar -l app=notification-service

# Check logs
kubectl logs -n traccar -l app=notification-service

# Check events
kubectl get events -n traccar --sort-by='.lastTimestamp'
```

#### 5.2.2 Checking Message Broker Status

**For Kafka:**

```bash
# List topics
kafka-topics.sh --bootstrap-server kafka-headless:9092 --list

# Check consumer group status
kafka-consumer-groups.sh --bootstrap-server kafka-headless:9092 --describe --group notification-service

# Check message count in topic
kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list kafka-headless:9092 --topic notifications --time -1
```

**For RabbitMQ:**

```bash
# List queues
rabbitmqctl list_queues

# Check queue details
rabbitmqctl list_queues name messages_ready messages_unacknowledged

# Check consumer status
rabbitmqctl list_consumers
```

#### 5.2.3 Checking Metrics

```bash
# Port forward to access metrics endpoint
kubectl port-forward -n traccar svc/notification-service 8081:8081

# Get all metrics
curl http://localhost:8081/actuator/prometheus

# Get specific metrics
curl http://localhost:8081/actuator/prometheus | grep traccar_notifications
```

#### 5.2.4 Checking Health Status

```bash
# Port forward to access health endpoint
kubectl port-forward -n traccar svc/notification-service 8081:8081

# Get overall health status
curl http://localhost:8081/actuator/health

# Get detailed health status
curl http://localhost:8081/actuator/health -H "Authorization: Bearer <token>"

# Check specific health indicators
curl http://localhost:8081/actuator/health/kafka
curl http://localhost:8081/actuator/health/rabbitmq
curl http://localhost:8081/actuator/health/mail
```

### 5.3 Log Analysis

Common log patterns to look for when troubleshooting:

#### 5.3.1 Startup Issues

```
Failed to connect to Kafka broker
Failed to connect to RabbitMQ
Failed to initialize notification channel
Missing required configuration
```

#### 5.3.2 Processing Issues

```
Failed to process notification
Failed to deserialize notification message
Notification validation failed
Notification filtered out by rules
```

#### 5.3.3 Delivery Issues

```
Failed to send email: Connection refused
Failed to send SMS: Authentication failed
Failed to send push notification: Invalid token
Failed to send web notification: No active sessions
```

#### 5.3.4 Retry and Recovery

```
Retrying notification delivery (attempt 2 of 3)
Publishing message to dead letter queue
Recovered from temporary failure
Circuit breaker opened for external service
```

## 6. Maintenance Procedures

### 6.1 Scaling

The Notification Service can be scaled horizontally to handle increased load:

```bash
# Manual scaling
kubectl scale deployment -n traccar notification-service --replicas=5

# Update HPA configuration
kubectl edit hpa -n traccar notification-service
```

Considerations when scaling:

- Ensure message broker can handle increased consumers
- Adjust resource limits and requests as needed
- Monitor external service rate limits (SMTP, SMS, etc.)
- Consider scaling related services (Event Service, etc.)

### 6.2 Updating

To update the Notification Service to a new version:

```bash
# Update using Helm
helm upgrade notification-service traccar/notification-service \
  --namespace traccar \
  --values custom-values.yaml \
  --set image.tag=new-version
```

Best practices for updates:

- Always review release notes before updating
- Test updates in staging environment first
- Use rolling updates to minimize downtime
- Monitor service during and after update
- Have rollback plan ready

### 6.3 Backup and Restore

The Notification Service itself is stateless, but configuration and templates should be backed up:

1. **Configuration Backup**:
   - Back up Helm values file
   - Back up Kubernetes secrets and configmaps
   - Store in version control or secure storage

2. **Template Backup**:
   - Back up notification templates
   - Store in version control

3. **Restore Procedure**:
   - Restore configuration from backup
   - Redeploy service using Helm

### 6.4 Performance Tuning

Parameters that can be adjusted to optimize performance:

1. **JVM Settings**:
   - Heap size (`-Xmx`, `-Xms`)
   - Garbage collection settings
   - Thread pool sizes

2. **Consumer Settings**:
   - Consumer thread count
   - Batch size and processing timeout
   - Prefetch count (RabbitMQ)
   - Poll interval (Kafka)

3. **Connection Pools**:
   - SMTP connection pool size
   - HTTP client connection pool size
   - Database connection pool size

4. **Circuit Breaker Settings**:
   - Failure threshold
   - Timeout duration
   - Reset timeout

Example performance tuning configuration:

```yaml
traccar:
  notification:
    threadPool:
      coreSize: 10
      maxSize: 20
      queueCapacity: 100
    consumer:
      threads: 5
      batchSize: 100
      timeout: 5000
    circuitBreaker:
      failureThreshold: 50
      resetTimeout: 30000
```

## 7. References

- [Notification Service API Documentation](https://docs.traccar.org/notification-service/api)
- [Traccar Microservices Architecture](https://docs.traccar.org/architecture)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Helm Documentation](https://helm.sh/docs/)
- [Spring Boot Actuator Documentation](https://docs.spring.io/spring-boot/docs/current/reference/html/actuator.html)
- [Prometheus Documentation](https://prometheus.io/docs/)
- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)