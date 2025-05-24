# Event Processing Service - Kubernetes Deployment Guide

## Overview

The Event Processing Service is a core component of the Traccar GPS tracking platform that analyzes position data to detect significant events such as geofence transitions, speed violations, device status changes, and maintenance alerts. This service implements complex event processing with stateful operations for time-window events.

This guide provides instructions for deploying and managing the Event Processing Service in a Kubernetes environment.

## Architecture

The Event Processing Service:

- Consumes enriched position data from the Message Broker (Kafka/RabbitMQ)
- Analyzes positions using configurable rule sets to detect events
- Publishes detected events to the Message Broker for notification processing
- Exposes gRPC endpoints for direct service-to-service communication
- Registers with the Service Discovery mechanism for dynamic service location

## Prerequisites

Before deploying the Event Processing Service, ensure you have:

- Kubernetes cluster (v1.19+)
- Kubectl configured to communicate with your cluster
- Helm v3+ (optional, for Helm-based deployments)
- Message Broker (Kafka/RabbitMQ) deployed and accessible
- Service Discovery mechanism (Consul/Kubernetes) configured
- Position Processing Service deployed and operational

## Installation

### Using kubectl

1. Create the namespace (if not already created):

```bash
kubectl create namespace traccar
```

2. Apply the Event Service configuration:

```bash
kubectl apply -f event-service-configmap.yaml -n traccar
```

3. Deploy the Event Service:

```bash
kubectl apply -f event-service-deployment.yaml -n traccar
```

4. Create the service:

```bash
kubectl apply -f event-service-service.yaml -n traccar
```

### Using Helm

1. Add the Traccar Helm repository:

```bash
helm repo add traccar https://charts.traccar.org
helm repo update
```

2. Install the Event Service chart:

```bash
helm install event-service traccar/event-service \
  --namespace traccar \
  --create-namespace \
  --set kafka.bootstrapServers=kafka-headless.kafka.svc.cluster.local:9092 \
  --set serviceDiscovery.type=kubernetes
```

## Configuration

The Event Service can be configured through environment variables or ConfigMaps. Key configuration parameters include:

### Core Settings

| Parameter | Description | Default |
|-----------|-------------|--------|
| `EVENT_SERVICE_PORT` | gRPC service port | `9090` |
| `EVENT_SERVICE_HTTP_PORT` | HTTP port for health checks | `8080` |
| `EVENT_SERVICE_LOG_LEVEL` | Logging level | `INFO` |

### Message Broker Configuration

| Parameter | Description | Default |
|-----------|-------------|--------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `kafka:9092` |
| `KAFKA_CONSUMER_GROUP_ID` | Consumer group ID | `event-service` |
| `KAFKA_POSITIONS_TOPIC` | Topic for enriched positions | `enriched-positions` |
| `KAFKA_EVENTS_TOPIC` | Topic for detected events | `events` |

### Service Discovery

| Parameter | Description | Default |
|-----------|-------------|--------|
| `SERVICE_DISCOVERY_TYPE` | Service discovery type (consul/kubernetes) | `kubernetes` |
| `CONSUL_HOST` | Consul host (if using Consul) | `consul` |
| `CONSUL_PORT` | Consul port (if using Consul) | `8500` |

### Event Processing

| Parameter | Description | Default |
|-----------|-------------|--------|
| `EVENT_BATCH_SIZE` | Maximum events to process in a batch | `100` |
| `EVENT_PROCESSING_THREADS` | Number of event processing threads | `5` |
| `EVENT_RULE_REFRESH_INTERVAL` | Interval to refresh rules from database (seconds) | `60` |

## Environment-Specific Configurations

### Development

For development environments, you can use the following configuration:

```yaml
resources:
  requests:
    cpu: 250m
    memory: 512Mi
  limits:
    cpu: 500m
    memory: 1Gi

replicas: 1

eventProcessing:
  batchSize: 50
  processingThreads: 2
  ruleRefreshInterval: 30
```

### Production

For production environments, consider the following configuration:

```yaml
resources:
  requests:
    cpu: 1000m
    memory: 2Gi
  limits:
    cpu: 2000m
    memory: 4Gi

replicas: 3

affinityConfig:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
    - labelSelector:
        matchExpressions:
        - key: app
          operator: In
          values:
          - event-service
      topologyKey: "kubernetes.io/hostname"

eventProcessing:
  batchSize: 200
  processingThreads: 8
  ruleRefreshInterval: 120
```

## Resource Requirements

The Event Processing Service resource requirements depend on the number of devices, position frequency, and rule complexity:

| Scale | Devices | Positions/sec | CPU | Memory | Replicas |
|-------|---------|---------------|-----|--------|----------|
| Small | <1,000 | <100 | 0.5 CPU | 1Gi | 1 |
| Medium | 1,000-5,000 | 100-500 | 1 CPU | 2Gi | 2 |
| Large | 5,000-20,000 | 500-2,000 | 2 CPU | 4Gi | 3+ |
| Enterprise | >20,000 | >2,000 | 4+ CPU | 8Gi+ | 5+ |

### Scaling Considerations

The Event Service can be scaled horizontally by increasing the number of replicas. For optimal performance:

- Scale based on CPU utilization (target: 70%)
- Monitor message broker consumer lag
- Consider event processing time as a scaling metric

Example HorizontalPodAutoscaler configuration:

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
  - type: Pods
    pods:
      metric:
        name: event_processing_time_p95
      target:
        type: AverageValue
        averageValue: 200
```

## Health Checks and Monitoring

The Event Service exposes the following health and monitoring endpoints:

- `/actuator/health/liveness` - Basic operational status
- `/actuator/health/readiness` - Ability to handle requests
- `/actuator/prometheus` - Prometheus metrics

Key metrics to monitor:

- `event_processing_time_seconds` - Event processing latency
- `events_detected_total` - Total number of detected events by type
- `rule_evaluation_time_seconds` - Rule evaluation time
- `kafka_consumer_lag` - Consumer lag for position topic

## Troubleshooting

### Common Issues

#### Service Won't Start

**Symptoms**: Pods fail to start or crash immediately

**Possible causes and solutions**:

1. **Message broker connectivity issues**
   - Check Kafka/RabbitMQ connection parameters
   - Verify network policies allow connectivity
   - Check broker logs for authentication failures

2. **Service discovery registration failure**
   - Verify Consul/Kubernetes service discovery configuration
   - Check for permission issues with service account

3. **Resource constraints**
   - Check if pods are being terminated due to OOM
   - Increase memory limits if necessary

#### High Event Processing Latency

**Symptoms**: Events are detected with significant delay

**Possible causes and solutions**:

1. **Insufficient resources**
   - Check CPU utilization and increase if consistently high
   - Monitor JVM memory usage and garbage collection metrics

2. **Message broker bottlenecks**
   - Check consumer lag metrics
   - Increase partition count for high-volume topics
   - Verify broker has sufficient resources

3. **Complex rule evaluation**
   - Review rule complexity and optimize where possible
   - Increase processing threads for parallel evaluation

#### Missing Events

**Symptoms**: Expected events are not being detected

**Possible causes and solutions**:

1. **Rule configuration issues**
   - Verify rules are correctly configured in the database
   - Check rule refresh interval and force a refresh if needed

2. **Position data quality issues**
   - Verify position data contains required attributes for rules
   - Check for missing or corrupted position messages

3. **Consumer group rebalancing**
   - Check for frequent consumer group rebalancing
   - Ensure stable pod deployments to minimize rebalancing

### Diagnostic Commands

To check pod status:

```bash
kubectl get pods -n traccar -l app=event-service
```

To view logs:

```bash
kubectl logs -n traccar -l app=event-service --tail=100
```

To describe the deployment:

```bash
kubectl describe deployment -n traccar event-service
```

To check consumer group status (if using Kafka):

```bash
kubectl exec -it kafka-0 -n kafka -- kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group event-service
```

## Related Services

The Event Processing Service interacts with the following services:

- **Position Processing Service**: Provides enriched position data
- **Notification Service**: Consumes detected events for notification delivery
- **API Gateway**: Provides access to event data via REST API
- **Message Broker**: Facilitates asynchronous communication
- **Service Discovery**: Enables service registration and location

## References

- [Event Service API Documentation](../docs/api/README.md)
- [Event Types Reference](../docs/events/README.md)
- [Rule Configuration Guide](../docs/rules/README.md)
- [Traccar Microservices Architecture](../../docs/architecture/README.md)