# Protocol Service Deployment Guide

This document provides comprehensive instructions for deploying the Protocol Service in various environments, with a focus on containerized deployment using Docker and Kubernetes.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Docker Deployment](#docker-deployment)
  - [Environment Variables](#environment-variables)
  - [Resource Requirements](#resource-requirements)
  - [Running with Docker](#running-with-docker)
- [Kubernetes Deployment](#kubernetes-deployment)
  - [Deployment Manifest](#deployment-manifest)
  - [Service Configuration](#service-configuration)
  - [ConfigMap](#configmap)
  - [Health Checks](#health-checks)
  - [Horizontal Pod Autoscaling](#horizontal-pod-autoscaling)
- [Service Discovery](#service-discovery)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## Overview

The Protocol Service is responsible for handling device connections and protocol implementations, decoding raw messages from 200+ supported device protocols into a standardized format. It acts as the entry point for all device data, with horizontal scalability for supporting thousands of concurrent connections.

## Prerequisites

Before deploying the Protocol Service, ensure you have the following prerequisites:

- Docker Engine 20.10+ (for Docker deployment)
- Kubernetes 1.23+ (for Kubernetes deployment)
- Access to the container registry where Protocol Service images are stored
- Message broker (Kafka/RabbitMQ) already deployed and accessible
- Service discovery mechanism (Consul/Kubernetes) already deployed

## Docker Deployment

### Environment Variables

The Protocol Service can be configured using the following environment variables:

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `PROTOCOL_SERVICE_PORT` | HTTP port for the service API | `8090` | No |
| `PROTOCOL_SERVICE_PROTOCOLS` | Comma-separated list of enabled protocols | All protocols | No |
| `PROTOCOL_SERVICE_BROKER_TYPE` | Message broker type (kafka/rabbitmq) | `kafka` | No |
| `PROTOCOL_SERVICE_BROKER_URL` | Message broker connection URL | - | Yes |
| `PROTOCOL_SERVICE_DISCOVERY_TYPE` | Service discovery type (consul/kubernetes) | `kubernetes` | No |
| `PROTOCOL_SERVICE_DISCOVERY_URL` | Service discovery connection URL | - | Yes |
| `PROTOCOL_SERVICE_MAX_CONNECTIONS` | Maximum number of concurrent connections | `10000` | No |
| `PROTOCOL_SERVICE_THREAD_POOL_SIZE` | Size of the worker thread pool | `20` | No |
| `JAVA_OPTS` | JVM options | `-Xms512m -Xmx1g` | No |

### Resource Requirements

The Protocol Service has the following recommended resource allocations:

| Environment | CPU | Memory | Connections per Instance |
|-------------|-----|--------|-------------------------|
| Development | 0.5 cores | 512MB | Up to 500 |
| Testing | 0.5 cores | 512MB | Up to 500 |
| Production (Small) | 1 core | 1GB | Up to 1,000 |
| Production (Medium) | 2 cores | 2GB | Up to 2,500 |
| Production (Large) | 4 cores | 4GB | Up to 5,000 |

### Running with Docker

To run the Protocol Service using Docker:

```bash
docker run -d --name protocol-service \
  -p 8090:8090 \
  -p 5000-5090:5000-5090 \
  -e PROTOCOL_SERVICE_BROKER_URL=kafka:9092 \
  -e PROTOCOL_SERVICE_DISCOVERY_URL=consul:8500 \
  -e PROTOCOL_SERVICE_MAX_CONNECTIONS=1000 \
  -e JAVA_OPTS="-Xms512m -Xmx1g" \
  --memory=1g \
  --cpus=1 \
  registry/traccar/protocol:latest
```

This command:
- Maps the service API port (8090) and protocol-specific ports (5000-5090)
- Sets the message broker and service discovery connection URLs
- Configures maximum connections and JVM memory settings
- Limits container resources to 1GB memory and 1 CPU core

## Kubernetes Deployment

### Deployment Manifest

Create a file named `protocol-service-deployment.yaml` with the following content:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: protocol-service
  labels:
    app: protocol-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: protocol-service
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  template:
    metadata:
      labels:
        app: protocol-service
    spec:
      containers:
      - name: protocol-service
        image: registry/traccar/protocol:latest
        imagePullPolicy: Always
        ports:
        - name: http
          containerPort: 8090
        - name: osmand
          containerPort: 5055
        - name: teltonika
          containerPort: 5027
        - name: meitrack
          containerPort: 5020
        - name: tk103
          containerPort: 5006
        env:
        - name: PROTOCOL_SERVICE_BROKER_URL
          valueFrom:
            configMapKeyRef:
              name: protocol-service-config
              key: broker-url
        - name: PROTOCOL_SERVICE_DISCOVERY_TYPE
          value: "kubernetes"
        - name: PROTOCOL_SERVICE_MAX_CONNECTIONS
          value: "1000"
        - name: JAVA_OPTS
          value: "-Xms512m -Xmx1g"
        resources:
          requests:
            cpu: "500m"
            memory: "512Mi"
          limits:
            cpu: "1"
            memory: "1Gi"
        livenessProbe:
          tcpSocket:
            port: 5055
          initialDelaySeconds: 60
          periodSeconds: 15
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /health/readiness
            port: 8090
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 3
          successThreshold: 1
          failureThreshold: 3
```

Apply the deployment manifest:

```bash
kubectl apply -f protocol-service-deployment.yaml
```

### Service Configuration

Create a file named `protocol-service-service.yaml` with the following content:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: protocol-service
  labels:
    app: protocol-service
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8090"
    prometheus.io/path: "/actuator/prometheus"
spec:
  selector:
    app: protocol-service
  ports:
  - name: http
    port: 8090
    targetPort: 8090
  - name: osmand
    port: 5055
    targetPort: 5055
    protocol: TCP
  - name: teltonika
    port: 5027
    targetPort: 5027
    protocol: TCP
  - name: meitrack
    port: 5020
    targetPort: 5020
    protocol: TCP
  - name: tk103
    port: 5006
    targetPort: 5006
    protocol: TCP
  type: ClusterIP
---
apiVersion: v1
kind: Service
metadata:
  name: protocol-service-tcp
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: nlb
spec:
  selector:
    app: protocol-service
  ports:
  - name: osmand
    port: 5055
    protocol: TCP
  - name: teltonika
    port: 5027
    protocol: TCP
  - name: meitrack
    port: 5020
    protocol: TCP
  - name: tk103
    port: 5006
    protocol: TCP
  type: LoadBalancer
```

Apply the service manifest:

```bash
kubectl apply -f protocol-service-service.yaml
```

### ConfigMap

Create a file named `protocol-service-configmap.yaml` with the following content:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: protocol-service-config
data:
  broker-url: "kafka-service:9092"
  protocols-enabled: "osmand,teltonika,meitrack,tk103"
  log-level: "INFO"
```

Apply the ConfigMap:

```bash
kubectl apply -f protocol-service-configmap.yaml
```

### Health Checks

The Protocol Service exposes the following health check endpoints:

- Liveness probe: TCP socket check on protocol-specific ports (e.g., 5055 for OsmAnd)
- Readiness probe: HTTP GET `/health/readiness` on port 8090

These health checks are already configured in the deployment manifest above.

### Horizontal Pod Autoscaling

Create a file named `protocol-service-hpa.yaml` with the following content:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: protocol-service-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: protocol-service
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
  - type: Pods
    pods:
      metric:
        name: active_connections
      target:
        type: AverageValue
        averageValue: 800
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

This HPA configuration will automatically scale the Protocol Service based on:
- CPU utilization (target: 70%)
- Memory utilization (target: 80%)
- Active connections (target: 800 connections per pod)

Apply the HPA manifest:

```bash
kubectl apply -f protocol-service-hpa.yaml
```

## Service Discovery

The Protocol Service automatically registers with the configured service discovery mechanism on startup.

### Kubernetes Service Discovery

When using Kubernetes as the service discovery mechanism (default), the Protocol Service will use the Kubernetes API to discover other services. No additional configuration is required beyond setting `PROTOCOL_SERVICE_DISCOVERY_TYPE=kubernetes`.

### Consul Service Discovery

When using Consul as the service discovery mechanism, set the following environment variables:

```
PROTOCOL_SERVICE_DISCOVERY_TYPE=consul
PROTOCOL_SERVICE_DISCOVERY_URL=consul-server:8500
```

The Protocol Service will register itself with Consul on startup with the following service definition:

```json
{
  "name": "protocol-service",
  "id": "protocol-service-${HOSTNAME}",
  "address": "${POD_IP}",
  "port": 8090,
  "tags": ["traccar", "protocol"],
  "checks": [
    {
      "http": "http://${POD_IP}:8090/health/readiness",
      "interval": "15s",
      "timeout": "5s"
    }
  ]
}
```

## Monitoring

The Protocol Service exposes metrics in Prometheus format at the `/actuator/prometheus` endpoint. These metrics include:

- JVM metrics (memory, garbage collection, threads)
- System metrics (CPU, load, disk space)
- Application metrics:
  - `protocol_active_connections`: Number of active device connections
  - `protocol_messages_received_total`: Total number of messages received
  - `protocol_messages_processed_total`: Total number of messages successfully processed
  - `protocol_messages_failed_total`: Total number of messages that failed processing
  - `protocol_message_processing_time_seconds`: Message processing time histogram

To view these metrics:

```bash
curl http://protocol-service:8090/actuator/prometheus
```

## Troubleshooting

### Common Issues

1. **Service fails to start**
   - Check logs: `kubectl logs deployment/protocol-service`
   - Verify message broker connectivity
   - Ensure service discovery is accessible

2. **Devices cannot connect**
   - Verify LoadBalancer service is properly configured
   - Check network policies allow traffic on protocol ports
   - Ensure protocol is enabled in the configuration

3. **High resource usage**
   - Increase resource limits
   - Scale horizontally by adding more replicas
   - Optimize JVM settings with appropriate garbage collection strategy

4. **Service discovery issues**
   - Verify service discovery type is correctly configured
   - Check connectivity to service discovery endpoint
   - Inspect service registration status

### Logs

To view logs for the Protocol Service:

```bash
# Docker
docker logs protocol-service

# Kubernetes
kubectl logs -f deployment/protocol-service
```

Increase log verbosity by setting the log level in the ConfigMap:

```yaml
log-level: "DEBUG"
```

### Support

For additional support, contact the Traccar support team or refer to the internal documentation portal.