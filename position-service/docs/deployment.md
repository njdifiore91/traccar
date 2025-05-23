# Position Processing Service Deployment Guide

This document provides comprehensive guidance for deploying and operating the Position Processing Service in production environments. It covers containerization, Kubernetes deployment, scaling considerations, resource requirements, and operational procedures.

## Table of Contents

- [Overview](#overview)
- [Container Configuration](#container-configuration)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Scaling Considerations](#scaling-considerations)
- [Resource Requirements](#resource-requirements)
- [Operational Procedures](#operational-procedures)
- [Troubleshooting](#troubleshooting)

## Overview

The Position Processing Service is a critical component in the Traccar microservices architecture responsible for processing and enriching position data from GPS devices. It consumes raw position data from the Protocol Service via a message broker, applies various processing handlers (geocoding, geolocation, distance calculation, motion detection, etc.), and publishes enriched position data for consumption by other services.

### Key Responsibilities

- Consuming raw position data from the message broker
- Enriching positions with geocoding, geolocation, and other contextual information
- Calculating distances, detecting motion, and processing attributes
- Applying filters and validations to position data
- Publishing enriched positions to the message broker for downstream services
- Handling position forwarding to external systems

## Container Configuration

### Docker Image

The Position Processing Service is packaged as a Docker container with the following specifications:

```dockerfile
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# Add non-root user for security
RUN addgroup -S traccar && adduser -S traccar -G traccar

# Copy application JAR and configuration
COPY --chown=traccar:traccar target/position-service.jar /app/
COPY --chown=traccar:traccar src/main/resources/config.yml /app/config/

# Set environment variables
ENV JAVA_OPTS="-Xms512m -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
ENV SPRING_CONFIG_LOCATION=file:/app/config/

# Expose service ports
EXPOSE 8080 9090

# Switch to non-root user
USER traccar

# Start the service
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar position-service.jar"]
```

### Environment Variables

The following environment variables can be used to configure the service:

| Variable | Description | Default |
|----------|-------------|--------|
| `KAFKA_BOOTSTRAP_SERVERS` | Comma-separated list of Kafka broker addresses | `kafka:9092` |
| `KAFKA_CONSUMER_GROUP_ID` | Consumer group ID for position service | `position-service` |
| `KAFKA_TOPIC_RAW_POSITIONS` | Topic for consuming raw positions | `raw-positions` |
| `KAFKA_TOPIC_ENRICHED_POSITIONS` | Topic for publishing enriched positions | `enriched-positions` |
| `SPRING_PROFILES_ACTIVE` | Active Spring profiles | `prod` |
| `JAVA_OPTS` | JVM options | `-Xms512m -Xmx2g` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `GEOCODER_ENABLED` | Enable/disable geocoding | `true` |
| `GEOLOCATION_ENABLED` | Enable/disable geolocation | `true` |
| `SERVICE_REGISTRY_URL` | Service registry URL | `http://consul:8500` |

### Volume Mounts

The following volume mounts are recommended:

| Path | Purpose |
|------|--------|
| `/app/config` | Configuration files |
| `/app/logs` | Log files |
| `/app/data` | Persistent data storage |

## Kubernetes Deployment

### Deployment Manifest

Below is a sample Kubernetes deployment manifest for the Position Processing Service:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: position-service
  labels:
    app: position-service
spec:
  replicas: 2
  selector:
    matchLabels:
      app: position-service
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  template:
    metadata:
      labels:
        app: position-service
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/actuator/prometheus"
    spec:
      containers:
      - name: position-service
        image: traccar/position-service:latest
        imagePullPolicy: Always
        ports:
        - name: http
          containerPort: 8080
        - name: grpc
          containerPort: 9090
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          valueFrom:
            configMapKeyRef:
              name: kafka-config
              key: bootstrap.servers
        - name: SPRING_PROFILES_ACTIVE
          value: "prod"
        - name: LOG_LEVEL
          value: "INFO"
        - name: JAVA_OPTS
          value: "-Xms512m -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
        resources:
          requests:
            cpu: 500m
            memory: 1Gi
          limits:
            cpu: 1500m
            memory: 2Gi
        readinessProbe:
          httpGet:
            path: /actuator/health/readiness
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 3
          successThreshold: 1
          failureThreshold: 3
        livenessProbe:
          httpGet:
            path: /actuator/health/liveness
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 15
          timeoutSeconds: 3
          successThreshold: 1
          failureThreshold: 3
        volumeMounts:
        - name: config-volume
          mountPath: /app/config
        - name: logs-volume
          mountPath: /app/logs
      volumes:
      - name: config-volume
        configMap:
          name: position-service-config
      - name: logs-volume
        emptyDir: {}
```

### Service Manifest

```yaml
apiVersion: v1
kind: Service
metadata:
  name: position-service
  labels:
    app: position-service
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8080"
spec:
  selector:
    app: position-service
  ports:
  - name: http
    port: 8080
    targetPort: 8080
  - name: grpc
    port: 9090
    targetPort: 9090
  type: ClusterIP
```

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: position-service-config
data:
  application.yml: |
    spring:
      application:
        name: position-service
      cloud:
        consul:
          host: ${SERVICE_REGISTRY_HOST:consul}
          port: ${SERVICE_REGISTRY_PORT:8500}
          discovery:
            instanceId: ${spring.application.name}:${random.value}
            healthCheckPath: /actuator/health
            healthCheckInterval: 15s
        stream:
          kafka:
            binder:
              brokers: ${KAFKA_BOOTSTRAP_SERVERS:kafka:9092}
              auto-create-topics: true
            bindings:
              position-input:
                consumer:
                  enableDlq: true
                  dlqName: position-input.dlq
                  retryTemporaryErrors: true
                  maxAttempts: 5
    
    management:
      endpoints:
        web:
          exposure:
            include: health,info,prometheus,metrics
      health:
        livenessstate:
          enabled: true
        readinessstate:
          enabled: true
    
    traccar:
      position:
        geocoder:
          enabled: ${GEOCODER_ENABLED:true}
          type: google
          url: https://maps.googleapis.com/maps/api/geocode/json
          key: ${GEOCODER_API_KEY:}
        geolocation:
          enabled: ${GEOLOCATION_ENABLED:true}
          type: mozilla
          url: https://location.services.mozilla.com/v1/geolocate
          key: ${GEOLOCATION_API_KEY:test}
        distance:
          minimalDistance: 10
```

### Horizontal Pod Autoscaler

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: position-service-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: position-service
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
        name: messages_per_second
      target:
        type: AverageValue
        averageValue: 500
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

## Scaling Considerations

### Scaling Approach

The Position Processing Service is designed to scale horizontally to handle increasing position data volume. The service can be scaled based on several metrics:

1. **CPU Utilization**: Scale when CPU usage exceeds 70% across pods
2. **Memory Utilization**: Scale when memory usage exceeds 80% across pods
3. **Messages Per Second**: Scale based on the rate of position messages being processed
4. **Consumer Lag**: Scale based on Kafka consumer lag to prevent backlog buildup

### Scaling Factors

| Metric | Scale Up Threshold | Scale Down Threshold |
|--------|-------------------|---------------------|
| CPU Utilization | >70% | <50% |
| Memory Utilization | >80% | <60% |
| Messages Per Second | >500 per instance | <100 per instance |
| Consumer Lag | >1000 messages | <100 messages |

### Capacity Planning Guidelines

For capacity planning, use the following formula:

```
Required Instances = (Total Messages Per Second / 500) + 1
```

Example: For 2,000 positions per second:
- Required Instances = (2,000 / 500) + 1 = 5 instances

Add additional capacity for peak loads and redundancy:

```
Total Instances = Required Instances * 1.5
```

Example: 5 * 1.5 = 7.5, round up to 8 instances

## Resource Requirements

### Resource Allocation Strategy

The Position Processing Service has the following resource requirements:

| Resource | Minimum | Recommended | Per 1,000 positions/sec |
|----------|---------|-------------|-------------------------|
| CPU | 500m | 1500m | 1 CPU core |
| Memory | 1Gi | 2Gi | 2GB |
| Disk Space | 2Gi | 5Gi | 1GB |
| Network Bandwidth | - | - | 1 Mbps |

### JVM Configuration

Optimal JVM settings for the Position Processing Service:

```
-Xms512m -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/app/logs/heapdump.hprof
```

For high-throughput environments (>5,000 positions/sec), consider increasing memory allocation:

```
-Xms2g -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/app/logs/heapdump.hprof
```

### Node Sizing and Affinity

For optimal performance, deploy the Position Processing Service on compute-optimized nodes with the following characteristics:

- At least 2 vCPUs
- At least 4GB RAM
- SSD storage for logs and temporary data
- High network throughput (at least 1 Gbps)

Use node affinity to ensure proper placement:

```yaml
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
      - matchExpressions:
        - key: kubernetes.io/instance-type
          operator: In
          values:
          - c5.large
          - c5.xlarge
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      podAffinityTerm:
        labelSelector:
          matchExpressions:
          - key: app
            operator: In
            values:
            - position-service
        topologyKey: "kubernetes.io/hostname"
```

## Operational Procedures

### Monitoring

The Position Processing Service exposes metrics via Prometheus endpoints. Key metrics to monitor include:

| Metric | Description | Alert Threshold |
|--------|-------------|----------------|
| `position_messages_received_total` | Total number of position messages received | N/A (trend) |
| `position_messages_processed_total` | Total number of position messages processed | N/A (trend) |
| `position_processing_time_seconds` | Time taken to process a position | >1s avg |
| `position_processing_errors_total` | Total number of processing errors | >1% error rate |
| `kafka_consumer_lag` | Lag in consuming messages from Kafka | >1000 messages |
| `jvm_memory_used_bytes` | JVM memory usage | >80% of max |
| `system_cpu_usage` | CPU usage | >80% sustained |
| `geocoder_requests_total` | Total geocoder requests | N/A (trend) |
| `geocoder_errors_total` | Total geocoder errors | >5% error rate |

Recommended Grafana dashboard panels:

1. Position Message Processing Rate
2. Position Processing Time (p50, p95, p99)
3. Error Rate
4. Kafka Consumer Lag
5. JVM Memory Usage
6. CPU Usage
7. Geocoder Success Rate
8. Active Instances Count

### Logging

The Position Processing Service uses structured JSON logging with the following log levels:

| Level | Usage |
|-------|-------|
| ERROR | Service failures, data loss, critical issues |
| WARN | Temporary failures, retryable errors, degraded service |
| INFO | Service startup/shutdown, configuration changes, scaling events |
| DEBUG | Detailed processing information (high volume) |
| TRACE | Very detailed debugging information (very high volume) |

Log files are written to `/app/logs/` with daily rotation and 7-day retention by default.

Important log patterns to monitor:

- `Position handler failed` - Indicates issues with position processing pipeline
- `Failed to connect to Kafka` - Messaging system connectivity issues
- `Geocoder service unavailable` - External geocoding service issues
- `High message processing time detected` - Performance degradation

### Backup and Recovery

The Position Processing Service is stateless, with all persistent data stored in Kafka and the database. However, the following backup procedures are recommended:

1. **Configuration Backup**: Regularly backup the ConfigMap containing service configuration
2. **Log Backup**: Archive logs for troubleshooting and audit purposes
3. **Kafka Topic Backup**: Implement Kafka topic mirroring for disaster recovery

Recovery procedures:

1. **Pod Failure**: Kubernetes automatically restarts failed pods
2. **Service Failure**: Redeploy the service using the deployment manifest
3. **Data Recovery**: Reprocess positions from Kafka if needed (positions are retained according to Kafka retention policy)

### Maintenance Procedures

#### Routine Maintenance

1. **Version Updates**:
   - Use rolling updates to deploy new versions without downtime
   - Monitor service health during and after updates
   - Keep a rollback plan ready in case of issues

2. **Configuration Updates**:
   - Update the ConfigMap with new configuration
   - Restart pods to apply configuration changes
   - Monitor service behavior after configuration changes

3. **Scaling Operations**:
   - Scale up before anticipated traffic increases
   - Scale down during low-traffic periods to save resources
   - Monitor performance metrics during scaling operations

#### Planned Downtime

If planned downtime is necessary:

1. Notify dependent services about the maintenance window
2. Scale up Protocol Service buffer capacity to handle message backlog
3. Gradually scale down Position Service instances
4. Perform maintenance
5. Scale up Position Service instances
6. Monitor Kafka consumer lag until backlog is processed

## Troubleshooting

### Common Issues

#### High Message Processing Time

**Symptoms**:
- Increased `position_processing_time_seconds` metric
- Growing Kafka consumer lag
- Slow position updates in downstream services

**Possible Causes**:
- Insufficient resources (CPU/memory)
- External service (geocoder/geolocation) latency
- Database performance issues
- Network latency

**Resolution**:
1. Check CPU and memory usage, scale up if necessary
2. Check external service response times in logs
3. Temporarily disable non-critical position enrichment
4. Increase processing parallelism

#### Message Processing Errors

**Symptoms**:
- Increased `position_processing_errors_total` metric
- Error logs with "Position handler failed"
- Missing positions in downstream services

**Possible Causes**:
- Malformed position data
- External service failures
- Database connectivity issues
- Configuration errors

**Resolution**:
1. Check logs for specific error messages
2. Verify external service availability
3. Check database connectivity
4. Verify service configuration
5. Check for recent code or configuration changes

#### Kafka Consumer Lag

**Symptoms**:
- Increasing `kafka_consumer_lag` metric
- Delayed position updates in downstream services
- High CPU usage

**Possible Causes**:
- Insufficient processing capacity
- Kafka broker issues
- Network bottlenecks
- Slow position processing

**Resolution**:
1. Scale up Position Service instances
2. Check Kafka broker health
3. Optimize position processing pipeline
4. Increase consumer parallelism

#### Service Startup Failures

**Symptoms**:
- Pods failing to start
- Readiness probe failures
- Error logs during startup

**Possible Causes**:
- Missing or invalid configuration
- Dependency service unavailability
- Resource constraints
- Permission issues

**Resolution**:
1. Check logs for startup errors
2. Verify ConfigMap contents
3. Check dependency service availability
4. Verify resource requests and limits
5. Check container permissions

### Diagnostic Commands

#### Checking Service Logs

```bash
# Get pod names
kubectl get pods -l app=position-service

# Check logs for a specific pod
kubectl logs <pod-name>

# Follow logs in real-time
kubectl logs -f <pod-name>

# Check logs with specific error pattern
kubectl logs <pod-name> | grep "Position handler failed"
```

#### Checking Service Health

```bash
# Get service health endpoint
kubectl port-forward svc/position-service 8080:8080
curl http://localhost:8080/actuator/health

# Check detailed health indicators
curl http://localhost:8080/actuator/health/details

# Check metrics
curl http://localhost:8080/actuator/prometheus
```

#### Checking Kafka Consumer Status

```bash
# List consumer groups
kafka-consumer-groups.sh --bootstrap-server <kafka-broker> --list

# Check consumer group status
kafka-consumer-groups.sh --bootstrap-server <kafka-broker> --group position-service --describe
```

#### Checking Pod Resource Usage

```bash
# Get pod resource usage
kubectl top pod -l app=position-service

# Get detailed pod description
kubectl describe pod <pod-name>
```

### Escalation Procedures

If issues cannot be resolved through standard troubleshooting:

1. **Level 1**: Operations team investigates using the troubleshooting guide
2. **Level 2**: Engage service developers with diagnostic information
3. **Level 3**: Engage platform team for infrastructure-related issues

Provide the following information when escalating:

- Service version and configuration
- Relevant logs and error messages
- Metrics and alerts that triggered
- Timeline of the issue
- Actions already taken
- Impact assessment (severity, affected users/devices)

---

## Additional Resources

- [Position Service Architecture](./architecture.md)
- [API Documentation](./api.md)
- [Configuration Reference](./configuration.md)
- [Monitoring Guide](./monitoring.md)
- [Kafka Topic Schema](./kafka-schema.md)