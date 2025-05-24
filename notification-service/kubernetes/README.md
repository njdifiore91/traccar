# Notification Service - Kubernetes Deployment Guide

## Overview

The Notification Service is a critical component of the Traccar GPS tracking system's microservices architecture. It is responsible for managing and delivering alerts through multiple channels (email, SMS, push notifications, webhooks) based on detected events from the Event Processing Service.

This document provides instructions for deploying, configuring, and managing the Notification Service in a Kubernetes environment.

## Architecture

The Notification Service follows an event-driven architecture:

1. Consumes events from the message broker (Kafka/RabbitMQ)
2. Processes notifications based on configured rules and templates
3. Delivers notifications through multiple channels:
   - Email (SMTP)
   - SMS (various providers)
   - Push notifications (Firebase)
   - Web notifications (WebSocket)
   - External services (Telegram, Pushover, etc.)
4. Reports delivery status back to the message broker

## Prerequisites

Before deploying the Notification Service, ensure you have:

- A functioning Kubernetes cluster (v1.19+)
- Kubectl configured to communicate with your cluster
- A message broker (Kafka or RabbitMQ) deployed and accessible
- Service discovery mechanism (Consul or Kubernetes native)
- Access to container registry containing Traccar images
- Persistent storage for configuration (if not using ConfigMaps)
- SMTP server or other notification channel providers configured

## Installation

### Using kubectl

1. Create the namespace (if not already existing):

```bash
kubectl create namespace traccar
```

2. Apply the Notification Service manifests:

```bash
kubectl apply -f notification-service.yaml -n traccar
```

### Using Helm

1. Add the Traccar Helm repository:

```bash
helm repo add traccar https://charts.traccar.org
helm repo update
```

2. Install the Notification Service chart:

```bash
helm install notification-service traccar/notification-service \
  --namespace traccar \
  --create-namespace \
  --set kafka.bootstrapServers=kafka-headless:9092 \
  --set smtp.host=smtp.example.com \
  --set smtp.port=587
```

## Configuration

The Notification Service can be configured using environment variables, ConfigMaps, or Secrets.

### Core Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|--------|
| `SPRING_PROFILES_ACTIVE` | Active Spring profiles | `kubernetes,prod` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `kafka:9092` |
| `RABBITMQ_HOST` | RabbitMQ host (if using RabbitMQ) | `rabbitmq` |
| `CONSUL_HOST` | Consul service discovery host | `consul` |
| `LOGGING_LEVEL_ORG_TRACCAR` | Logging level | `INFO` |

### Notification Channel Configuration

#### Email Configuration

| Parameter | Description | Default |
|-----------|-------------|--------|
| `SPRING_MAIL_HOST` | SMTP server host | `smtp.example.com` |
| `SPRING_MAIL_PORT` | SMTP server port | `587` |
| `SPRING_MAIL_USERNAME` | SMTP username | - |
| `SPRING_MAIL_PASSWORD` | SMTP password | - |
| `SPRING_MAIL_PROPERTIES_MAIL_SMTP_AUTH` | Enable SMTP authentication | `true` |
| `SPRING_MAIL_PROPERTIES_MAIL_SMTP_STARTTLS_ENABLE` | Enable STARTTLS | `true` |

#### SMS Configuration

| Parameter | Description | Default |
|-----------|-------------|--------|
| `SMS_PROVIDER` | SMS provider (http, aws, twilio) | `http` |
| `SMS_HTTP_URL` | HTTP SMS gateway URL | - |
| `SMS_HTTP_AUTHORIZATION` | HTTP SMS gateway auth token | - |
| `SMS_AWS_REGION` | AWS region for SNS | `us-east-1` |
| `SMS_AWS_ACCESS_KEY` | AWS access key | - |
| `SMS_AWS_SECRET_KEY` | AWS secret key | - |

#### Push Notification Configuration

| Parameter | Description | Default |
|-----------|-------------|--------|
| `FIREBASE_CONFIG_FILE` | Path to Firebase config file | `/config/firebase-config.json` |
| `PUSH_ENABLED` | Enable push notifications | `true` |

### Using ConfigMaps and Secrets

1. Create a ConfigMap for non-sensitive configuration:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: notification-service-config
  namespace: traccar
data:
  application.yml: |
    spring:
      profiles:
        active: kubernetes,prod
      application:
        name: notification-service
      cloud:
        consul:
          host: consul
          port: 8500
          discovery:
            instanceId: ${spring.application.name}:${random.value}
            healthCheckPath: /actuator/health
            healthCheckInterval: 15s
    
    management:
      endpoints:
        web:
          exposure:
            include: health,info,prometheus
      metrics:
        export:
          prometheus:
            enabled: true
    
    logging:
      level:
        org.traccar: INFO
```

2. Create Secrets for sensitive configuration:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: notification-service-secrets
  namespace: traccar
type: Opaque
data:
  smtp-password: base64encodedpassword
  sms-api-key: base64encodedapikey
  firebase-config.json: base64encodedfirebaseconfig
```

## Resource Requirements

The Notification Service has the following recommended resource allocations:

### Minimum Requirements

- CPU: 250m (0.25 CPU cores)
- Memory: 512Mi
- Storage: 1Gi (for logs and temporary files)

### Recommended Production Requirements

- CPU: 500m-750m (0.5-0.75 CPU cores)
- Memory: 1Gi-2Gi
- Storage: 5Gi

### Scaling Considerations

The Notification Service scales horizontally based on notification volume. Consider the following scaling factors:

- 1 CPU core can handle approximately 1,000 notifications per minute
- Memory requirements increase with template complexity and concurrent notifications
- Scale the service when notification queue depth exceeds 1,000 messages

### Horizontal Pod Autoscaler Configuration

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: notification-service-hpa
  namespace: traccar
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: notification-service
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 300
```

## Environment-Specific Configurations

### Development Environment

```yaml
resources:
  requests:
    cpu: 250m
    memory: 512Mi
  limits:
    cpu: 500m
    memory: 1Gi

replicas: 1

logging:
  level:
    org.traccar: DEBUG
```

### Production Environment

```yaml
resources:
  requests:
    cpu: 500m
    memory: 1Gi
  limits:
    cpu: 1000m
    memory: 2Gi

replicas: 3

podAntiAffinity: true  # Ensure pods are distributed across nodes

logging:
  level:
    org.traccar: INFO
```

## Monitoring and Health Checks

The Notification Service exposes the following endpoints for monitoring:

- `/actuator/health` - Health check endpoint
- `/actuator/info` - Service information
- `/actuator/prometheus` - Prometheus metrics

Key metrics to monitor:

- `notification_delivery_total` - Total notifications delivered
- `notification_delivery_failures` - Failed notification deliveries
- `notification_processing_time` - Time to process notifications
- `notification_queue_depth` - Current notification queue depth

## Troubleshooting

### Common Issues

#### Service Fails to Start

**Symptoms:**
- Pod remains in `CrashLoopBackOff` state
- Logs show connection errors

**Possible Causes:**
- Message broker (Kafka/RabbitMQ) is not accessible
- Service discovery (Consul) is not available
- Configuration errors in application.yml

**Resolution:**
1. Check connectivity to message broker and service discovery
2. Verify configuration values in ConfigMaps and Secrets
3. Check for port conflicts or network policies blocking communication

#### Notifications Not Being Delivered

**Symptoms:**
- Events are generated but notifications are not received
- Logs show successful processing but no delivery

**Possible Causes:**
- SMTP/SMS/Push configuration is incorrect
- External service providers are unavailable
- Network policies blocking outbound connections

**Resolution:**
1. Verify channel-specific configurations (SMTP, SMS, etc.)
2. Check connectivity to external service providers
3. Examine logs for specific delivery errors
4. Ensure necessary outbound network access is allowed

#### High Memory Usage

**Symptoms:**
- Pods are being OOM killed
- Memory usage grows over time

**Possible Causes:**
- Memory leaks in notification processing
- Too many concurrent notifications
- Insufficient memory limits

**Resolution:**
1. Increase memory limits in deployment configuration
2. Implement rate limiting for high-volume notification scenarios
3. Check for memory leaks in custom notification templates

### Accessing Logs

To view logs for the Notification Service:

```bash
kubectl logs -f deployment/notification-service -n traccar
```

For a specific pod:

```bash
kubectl logs -f pod/notification-service-pod-name -n traccar
```

### Debugging

To enable debug logging:

1. Update the ConfigMap to change the logging level:

```bash
kubectl patch configmap notification-service-config -n traccar --patch '
{
  "data": {
    "application.yml": "logging:\n  level:\n    org.traccar: DEBUG\n"
  }
}'
```

2. Restart the pods to apply the new configuration:

```bash
kubectl rollout restart deployment/notification-service -n traccar
```

## Backup and Restore

The Notification Service primarily processes data in-memory and relies on the message broker for message persistence. However, configuration and templates should be backed up regularly.

### Backing Up Configuration

```bash
kubectl get configmap notification-service-config -n traccar -o yaml > notification-config-backup.yaml
kubectl get secret notification-service-secrets -n traccar -o yaml > notification-secrets-backup.yaml
```

### Restoring Configuration

```bash
kubectl apply -f notification-config-backup.yaml
kubectl apply -f notification-secrets-backup.yaml
kubectl rollout restart deployment/notification-service -n traccar
```

## Upgrading

To upgrade the Notification Service to a new version:

### Using kubectl

```bash
kubectl set image deployment/notification-service notification-service=traccar/notification-service:new-version -n traccar
```

### Using Helm

```bash
helm upgrade notification-service traccar/notification-service \
  --namespace traccar \
  --set image.tag=new-version
```

## Support and Additional Resources

- [Traccar Documentation](https://www.traccar.org/documentation/)
- [Traccar Forums](https://www.traccar.org/forums/)
- [GitHub Repository](https://github.com/traccar/traccar)

For commercial support options, please visit [Traccar Support](https://www.traccar.org/support/).