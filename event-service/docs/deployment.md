# Event Processing Service Deployment Guide

This document provides comprehensive instructions for deploying the Event Processing Service in various environments. The Event Processing Service is responsible for analyzing position data to detect significant events such as geofence transitions, speed violations, device status changes, and maintenance alerts.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Docker Deployment](#docker-deployment)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Configuration](#configuration)
- [Resource Requirements](#resource-requirements)
- [Scaling Considerations](#scaling-considerations)
- [High Availability Setup](#high-availability-setup)
- [Deployment Verification](#deployment-verification)
- [Troubleshooting](#troubleshooting)

## Overview

The Event Processing Service is a critical component of the Traccar microservices architecture. It consumes enriched position data from the message broker, analyzes this data using various event handlers, and publishes detected events back to the message broker for further processing by the Notification Service.

This service is designed to be stateless, allowing for horizontal scaling to handle varying loads. It integrates with the following components:

- **Message Broker** (Kafka/RabbitMQ): For consuming enriched position data and publishing detected events
- **Service Discovery**: For registering the service and discovering other services
- **Database**: For accessing configuration data and storing detected events
- **Monitoring System**: For exposing health, metrics, and tracing information

## Prerequisites

Before deploying the Event Processing Service, ensure you have the following prerequisites:

- Docker Engine 20.10+ (for Docker deployment)
- Kubernetes 1.23+ (for Kubernetes deployment)
- Access to the container registry containing the Event Processing Service image
- Message broker (Kafka 3.x or RabbitMQ 3.9+) properly configured
- Service discovery mechanism (Consul or Kubernetes) in place
- Database with required schema initialized
- Network connectivity between all components

## Docker Deployment

### Pulling the Image

```bash
# Pull the latest stable release
docker pull registry/traccar/event:latest

# Or pull a specific version
docker pull registry/traccar/event:1.0.0

# For minimal Alpine-based image
docker pull registry/traccar/event:1.0.0-alpine
```

### Running the Container

```bash
docker run -d \
  --name traccar-event-service \
  -p 8082:8082 \
  -e SPRING_PROFILES_ACTIVE=prod \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e DATABASE_URL=jdbc:postgresql://db:5432/traccar \
  -e DATABASE_USERNAME=traccar \
  -e DATABASE_PASSWORD=password \
  -e SERVICE_DISCOVERY_URI=consul://consul:8500 \
  -v /path/to/config:/app/config \
  --restart unless-stopped \
  --memory=1g \
  --cpus=1.0 \
  registry/traccar/event:latest
```

### Environment Variables

The Event Processing Service supports the following environment variables:

| Variable | Description | Default |
|----------|-------------|--------|
| `SPRING_PROFILES_ACTIVE` | Active Spring profiles | `default` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` |
| `RABBITMQ_HOST` | RabbitMQ host (if using RabbitMQ) | `localhost` |
| `RABBITMQ_PORT` | RabbitMQ port | `5672` |
| `RABBITMQ_USERNAME` | RabbitMQ username | `guest` |
| `RABBITMQ_PASSWORD` | RabbitMQ password | `guest` |
| `DATABASE_URL` | JDBC URL for database connection | `jdbc:h2:./data/database` |
| `DATABASE_USERNAME` | Database username | `sa` |
| `DATABASE_PASSWORD` | Database password | `` |
| `SERVICE_DISCOVERY_URI` | Service discovery URI | `consul://localhost:8500` |
| `JAVA_OPTS` | JVM options | `-Xms512m -Xmx1g` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `POSITION_TOPIC` | Topic for consuming enriched positions | `enriched-positions` |
| `EVENT_TOPIC` | Topic for publishing detected events | `events` |

### Docker Compose

Here's an example `docker-compose.yml` configuration for the Event Processing Service:

```yaml
version: '3.8'

services:
  event-service:
    image: registry/traccar/event:latest
    container_name: traccar-event-service
    restart: unless-stopped
    ports:
      - "8082:8082"
    environment:
      - SPRING_PROFILES_ACTIVE=prod
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - DATABASE_URL=jdbc:postgresql://db:5432/traccar
      - DATABASE_USERNAME=traccar
      - DATABASE_PASSWORD=password
      - SERVICE_DISCOVERY_URI=consul://consul:8500
      - LOG_LEVEL=INFO
    volumes:
      - ./config:/app/config
    depends_on:
      - kafka
      - db
      - consul
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8082/actuator/health/liveness"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 1G
        reservations:
          cpus: '0.5'
          memory: 512M
```

## Kubernetes Deployment

The Event Processing Service can be deployed to Kubernetes using the provided manifests in the `kubernetes` directory.

### Deployment Manifest

Create a file named `event-service-deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: event-service
  labels:
    app: event-service
spec:
  replicas: 2
  selector:
    matchLabels:
      app: event-service
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  template:
    metadata:
      labels:
        app: event-service
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8082"
        prometheus.io/path: "/actuator/prometheus"
    spec:
      containers:
      - name: event-service
        image: registry/traccar/event:1.0.0
        imagePullPolicy: Always
        ports:
        - name: http
          containerPort: 8082
        env:
        - name: SPRING_PROFILES_ACTIVE
          value: "prod"
        - name: KAFKA_BOOTSTRAP_SERVERS
          valueFrom:
            configMapKeyRef:
              name: traccar-config
              key: kafka.bootstrap-servers
        - name: DATABASE_URL
          valueFrom:
            configMapKeyRef:
              name: traccar-config
              key: database.url
        - name: DATABASE_USERNAME
          valueFrom:
            secretKeyRef:
              name: traccar-db-credentials
              key: username
        - name: DATABASE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: traccar-db-credentials
              key: password
        - name: SERVICE_DISCOVERY_URI
          value: "kubernetes://default.svc.cluster.local"
        - name: LOG_LEVEL
          value: "INFO"
        resources:
          requests:
            cpu: "500m"
            memory: "512Mi"
          limits:
            cpu: "1000m"
            memory: "1Gi"
        livenessProbe:
          httpGet:
            path: /actuator/health/liveness
            port: 8082
          initialDelaySeconds: 60
          periodSeconds: 15
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /actuator/health/readiness
            port: 8082
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 3
          successThreshold: 1
          failureThreshold: 3
        volumeMounts:
        - name: config-volume
          mountPath: /app/config
      volumes:
      - name: config-volume
        configMap:
          name: event-service-config
      securityContext:
        runAsUser: 1000
        runAsGroup: 1000
        fsGroup: 1000
```

### Service Manifest

Create a file named `event-service-service.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: event-service
  labels:
    app: event-service
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8082"
    prometheus.io/path: "/actuator/prometheus"
spec:
  selector:
    app: event-service
  ports:
  - name: http
    port: 8082
    targetPort: 8082
  type: ClusterIP
```

### ConfigMap Manifest

Create a file named `event-service-configmap.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: event-service-config
data:
  application.yml: |
    spring:
      application:
        name: event-service
      kafka:
        consumer:
          group-id: event-service
          auto-offset-reset: earliest
          key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
          value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
          properties:
            spring.json.trusted.packages: "org.traccar.model"
        producer:
          key-serializer: org.apache.kafka.common.serialization.StringSerializer
          value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    
    management:
      endpoints:
        web:
          exposure:
            include: health,info,prometheus,metrics
      endpoint:
        health:
          probes:
            enabled: true
          show-details: always
          group:
            readiness:
              include: db, kafka
    
    traccar:
      event:
        handlers:
          geofence:
            enabled: true
          overspeed:
            enabled: true
          motion:
            enabled: true
          ignition:
            enabled: true
          maintenance:
            enabled: true
```

### Horizontal Pod Autoscaler

Create a file named `event-service-hpa.yaml`:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: event-service-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: event-service
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
      policies:
      - type: Percent
        value: 100
        periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 10
        periodSeconds: 120
```

### Network Policy

Create a file named `event-service-networkpolicy.yaml`:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: event-service-network-policy
spec:
  podSelector:
    matchLabels:
      app: event-service
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: api-gateway
    ports:
    - protocol: TCP
      port: 8082
  - from:
    - namespaceSelector:
        matchLabels:
          name: monitoring
    ports:
    - protocol: TCP
      port: 8082
  egress:
  - to:
    - podSelector:
        matchLabels:
          app: kafka
    ports:
    - protocol: TCP
      port: 9092
  - to:
    - podSelector:
        matchLabels:
          app: database
    ports:
    - protocol: TCP
      port: 5432
  - to:
    - namespaceSelector:
        matchLabels:
          name: kube-system
    - podSelector:
        matchLabels:
          k8s-app: kube-dns
    ports:
    - protocol: UDP
      port: 53
    - protocol: TCP
      port: 53
```

### Deploying to Kubernetes

Apply the manifests using kubectl:

```bash
kubectl apply -f event-service-configmap.yaml
kubectl apply -f event-service-deployment.yaml
kubectl apply -f event-service-service.yaml
kubectl apply -f event-service-hpa.yaml
kubectl apply -f event-service-networkpolicy.yaml
```

## Configuration

The Event Processing Service can be configured through environment variables, configuration files, or a combination of both.

### Configuration Files

The service looks for configuration files in the following locations:

1. `/app/config/application.yml` (inside the container)
2. `/app/config/application-{profile}.yml` (for profile-specific configuration)

You can mount these files as volumes when running the container.

### Event Handler Configuration

The Event Processing Service supports various event handlers, each with its own configuration options:

```yaml
traccar:
  event:
    handlers:
      geofence:
        enabled: true
      overspeed:
        enabled: true
        threshold: 5  # km/h over the speed limit
      motion:
        enabled: true
        speedThreshold: 0.01  # km/h
        tripsEnabled: true
        stopsEnabled: true
        minimalTripDistance: 500  # meters
        minimalTripDuration: 300  # seconds
        minimalParkingDuration: 300  # seconds
      ignition:
        enabled: true
      maintenance:
        enabled: true
      fuel:
        enabled: true
        threshold: 0.1  # threshold for fuel level changes
      driver:
        enabled: true
      alarm:
        enabled: true
```

## Resource Requirements

The Event Processing Service has the following recommended resource allocations:

| Resource | Minimum | Recommended | High Load |
|----------|---------|-------------|----------|
| CPU | 0.25 cores | 0.5 cores | 1+ cores |
| Memory | 256MB | 512MB | 1GB+ |
| Disk | 1GB | 2GB | 5GB+ |

These requirements scale linearly with the number of positions processed per second and the complexity of event detection rules.

### Sizing Guidelines

- **CPU**: 1 CPU core can handle approximately 10,000 event evaluations per second
- **Memory**: 2GB per 10,000 evaluations per second
- **Network**: 500 Kbps per 1,000 evaluations per second

## Scaling Considerations

The Event Processing Service is designed to scale horizontally to handle increased load. Consider the following when scaling the service:

### Horizontal Scaling

The service can be scaled horizontally by increasing the number of replicas. When using Kafka as the message broker, ensure that:

1. The consumer group ID is consistent across all instances
2. The number of partitions in the input topic is greater than or equal to the maximum number of instances
3. The instances have sufficient resources to handle their share of the load

### Vertical Scaling

For workloads with complex event detection rules or high position throughput, consider increasing the resources allocated to each instance:

1. Increase CPU allocation for faster event processing
2. Increase memory allocation for handling more concurrent evaluations
3. Adjust JVM parameters to optimize garbage collection

### Auto-Scaling

In Kubernetes environments, use the Horizontal Pod Autoscaler (HPA) to automatically scale the service based on CPU utilization, memory usage, or custom metrics:

- Scale up when CPU utilization exceeds 70%
- Scale up when memory utilization exceeds 80%
- Scale up when event processing time exceeds 200ms (requires custom metrics)
- Scale down when resources are underutilized for an extended period

## High Availability Setup

To ensure high availability of the Event Processing Service, consider the following recommendations:

1. **Multiple Replicas**: Run at least 2 replicas of the service at all times
2. **Pod Anti-Affinity**: Distribute replicas across different nodes to prevent single-node failures from affecting all instances
3. **Zone Distribution**: In multi-zone Kubernetes clusters, distribute replicas across availability zones
4. **Resource Guarantees**: Use resource requests to ensure pods have guaranteed resources
5. **Graceful Shutdown**: Configure proper termination grace periods to allow for clean shutdown
6. **Health Checks**: Implement comprehensive liveness and readiness probes

### Example Pod Anti-Affinity Configuration

```yaml
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
    - labelSelector:
        matchExpressions:
        - key: app
          operator: In
          values:
          - event-service
      topologyKey: "kubernetes.io/hostname"
```

## Deployment Verification

After deploying the Event Processing Service, verify that it's functioning correctly:

### Health Check

Verify the service health endpoints:

```bash
# For Docker deployment
curl http://localhost:8082/actuator/health

# For Kubernetes deployment
kubectl exec -it $(kubectl get pods -l app=event-service -o jsonpath='{.items[0].metadata.name}') -- curl http://localhost:8082/actuator/health
```

The response should include status information for the service and its dependencies.

### Metrics Check

Verify that metrics are being exposed:

```bash
# For Docker deployment
curl http://localhost:8082/actuator/prometheus

# For Kubernetes deployment
kubectl exec -it $(kubectl get pods -l app=event-service -o jsonpath='{.items[0].metadata.name}') -- curl http://localhost:8082/actuator/prometheus
```

The response should include various metrics related to the Event Processing Service.

### Log Verification

Check the service logs for any errors or warnings:

```bash
# For Docker deployment
docker logs traccar-event-service

# For Kubernetes deployment
kubectl logs -l app=event-service
```

### End-to-End Testing

Verify that the Event Processing Service is correctly processing positions and generating events:

1. Send a test position to the Position Processing Service
2. Verify that the position is published to the enriched-positions topic
3. Check that the Event Processing Service consumes the position
4. Verify that appropriate events are generated and published to the events topic
5. Confirm that the Notification Service processes the events

## Troubleshooting

### Common Issues

#### Service Fails to Start

- Check if the message broker is accessible
- Verify database connection parameters
- Ensure service discovery is properly configured
- Check for port conflicts

#### No Events Being Generated

- Verify that positions are being published to the enriched-positions topic
- Check that the Event Processing Service is consuming from the correct topic
- Ensure event handlers are enabled in the configuration
- Verify that the position data meets the criteria for event generation

#### High CPU Usage

- Check for complex event detection rules
- Verify the number of positions being processed
- Consider scaling horizontally or vertically
- Optimize JVM parameters

#### Memory Issues

- Adjust JVM heap size settings
- Check for memory leaks
- Monitor garbage collection metrics
- Consider increasing memory allocation

### Diagnostic Commands

```bash
# Check service status
kubectl get pods -l app=event-service

# View detailed pod information
kubectl describe pod -l app=event-service

# Check service logs
kubectl logs -l app=event-service

# Check resource usage
kubectl top pod -l app=event-service

# Check Kafka consumer group status
kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group event-service
```

### Support Resources

If you encounter issues that cannot be resolved using this guide, consider the following resources:

- [Event Processing Service Documentation](../README.md)
- [Traccar Community Forums](https://forum.traccar.org/)
- [GitHub Issues](https://github.com/traccar/traccar/issues)
- [Commercial Support](https://www.traccar.org/support/)