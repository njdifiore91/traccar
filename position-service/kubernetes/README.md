# Position Service Kubernetes Deployment Guide

## Overview

The Position Processing Service is a core component of the Traccar GPS tracking platform's microservices architecture. This service is responsible for processing, validating, and enriching position data received from the Protocol Service. It implements a comprehensive handler pipeline that performs operations such as distance calculation, geofence checking, reverse geocoding, and speed limit validation.

## Architecture

The Position Service fits into the Traccar microservices architecture as follows:

1. **Receives data from**: Protocol Service (via message broker)
2. **Sends data to**: Event Processing Service (via message broker)
3. **Stores data in**: Database (PostgreSQL/MySQL)
4. **Exposes APIs to**: API Gateway Service

## Prerequisites

Before deploying the Position Service, ensure you have:

- Kubernetes cluster (v1.23+)
- Kubectl configured to communicate with your cluster
- Helm (v3.11+) if using Helm charts
- Message broker (Kafka/RabbitMQ) deployed and configured
- Database (PostgreSQL/MySQL) deployed and configured
- Service discovery mechanism (Kubernetes native or Consul) in place
- Namespace created for the Position Service (`kubectl create namespace position-service`)

## Deployment Files

The Position Service deployment consists of the following Kubernetes manifests:

- `deployment.yaml`: Defines the Position Service deployment with container specifications, resource requirements, and health checks
- `service.yaml`: Exposes the Position Service to other services within the cluster
- `configmap.yaml`: Contains configuration for the Position Service including processing pipeline settings
- `hpa.yaml`: Horizontal Pod Autoscaler for automatically scaling the service based on load
- `networkpolicy.yaml`: Defines network access rules for the Position Service

## Installation

### Using kubectl

1. Apply the ConfigMap first to ensure configuration is available:

```bash
kubectl apply -f configmap.yaml
```

2. Deploy the Position Service:

```bash
kubectl apply -f deployment.yaml
```

3. Create the Service to expose the deployment:

```bash
kubectl apply -f service.yaml
```

4. Apply the HorizontalPodAutoscaler for automatic scaling:

```bash
kubectl apply -f hpa.yaml
```

5. Apply the NetworkPolicy to secure the service:

```bash
kubectl apply -f networkpolicy.yaml
```

Alternatively, you can apply all resources at once:

```bash
kubectl apply -f .
```

### Using Helm

If you're using Helm to manage your Traccar deployment:

```bash
helm upgrade --install position-service ../helm/charts/position -n position-service
```

## Configuration

The Position Service is configured through the ConfigMap in `configmap.yaml`. Key configuration sections include:

### Processing Pipeline

The processing pipeline defines the sequence of handlers that process position data:

```yaml
pipeline:
  enabled: true
  handlers:
    - org.traccar.handler.CopyAttributesHandler
    - org.traccar.handler.DistanceHandler
    - org.traccar.handler.EngineHoursHandler
    # Additional handlers...
```

You can customize this list to add, remove, or reorder handlers based on your requirements.

### Handler-Specific Configuration

Each handler can be configured with specific parameters:

```yaml
motion:
  processInvalidPositions: false
  speedThreshold: 0.01
distance:
  minimalDistance: 10
geocode:
  enabled: true
  # Additional geocoder settings...
```

### Message Broker Configuration

Configure the connection to your message broker:

```yaml
messaging:
  type: kafka  # Options: kafka, rabbitmq
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:kafka:9092}
    # Additional Kafka settings...
  rabbitmq:
    host: ${RABBITMQ_HOST:rabbitmq}
    # Additional RabbitMQ settings...
```

### Database Configuration

Configure the database connection:

```yaml
database:
  driver: org.postgresql.Driver
  url: jdbc:postgresql://postgres:5432/traccar
  username: ${DB_USERNAME:traccar}
  password: ${DB_PASSWORD:traccar}
  # Connection pool settings...
```

## Resource Requirements

The Position Service has the following resource requirements:

| Resource | Request | Limit | Notes |
| --- | --- | --- | --- |
| CPU | 1 core | 2 cores | Computation-intensive service |
| Memory | 1GB | 2GB | For batched processing |

These values can be adjusted in the `deployment.yaml` file based on your specific workload.

## Scaling

The Position Service is configured to scale automatically using the Horizontal Pod Autoscaler (HPA) based on:

- CPU utilization (target: 70%)
- Memory utilization (target: 80%)
- Message processing rate (target: 500 messages per second per pod)

The HPA configuration in `hpa.yaml` sets:

- Minimum replicas: 2 (for high availability)
- Maximum replicas: 10 (adjust based on your expected load)

Scaling behavior is configured to:
- Scale up quickly (100% increase every 60 seconds)
- Scale down conservatively (10% decrease every 120 seconds with 300s stabilization window)

## Monitoring and Observability

The Position Service exposes the following endpoints for monitoring:

- `/health/liveness`: Liveness probe endpoint
- `/health/readiness`: Readiness probe endpoint
- `/actuator/prometheus`: Prometheus metrics endpoint

Key metrics to monitor include:

- `position_processing_rate`: Number of positions processed per second
- `position_processing_latency`: Time taken to process a position
- `geocoding_requests`: Number of geocoding requests made
- `geocoding_latency`: Time taken for geocoding operations
- `database_operations`: Number of database operations
- `message_broker_operations`: Number of messages published/consumed

## Troubleshooting

### Common Issues

#### Service Fails to Start

**Symptoms**: Pods are in `CrashLoopBackOff` state

**Possible causes and solutions**:

1. **Database connection issues**:
   - Check database credentials in ConfigMap
   - Verify database is accessible from the Kubernetes cluster
   - Check database logs for connection errors

2. **Message broker connection issues**:
   - Verify Kafka/RabbitMQ is running and accessible
   - Check broker connection settings in ConfigMap
   - Ensure topics/queues exist and are properly configured

3. **Resource constraints**:
   - Check if the pod is being terminated due to OOM (Out of Memory)
   - Increase memory limits in `deployment.yaml`

#### Position Processing Delays

**Symptoms**: Positions are processed with high latency

**Possible causes and solutions**:

1. **Insufficient resources**:
   - Check CPU and memory usage
   - Increase resource limits or adjust HPA to scale earlier

2. **Geocoding service issues**:
   - Check geocoding service availability
   - Consider disabling geocoding temporarily if the service is down

3. **Database performance**:
   - Check database query performance
   - Optimize database indexes

#### Service Not Scaling

**Symptoms**: Service doesn't scale despite high load

**Possible causes and solutions**:

1. **HPA misconfiguration**:
   - Verify HPA is correctly targeting the deployment
   - Check metrics-server is running

2. **Custom metrics not available**:
   - Ensure Prometheus adapter is configured correctly
   - Check if custom metrics are being collected

### Viewing Logs

To view logs for the Position Service:

```bash
kubectl logs -f -l app=position-service -n position-service
```

For a specific pod:

```bash
kubectl logs -f <pod-name> -n position-service
```

### Debugging

To debug configuration issues, you can exec into a running pod:

```bash
kubectl exec -it <pod-name> -n position-service -- /bin/sh
```

To check the configuration loaded by the application:

```bash
kubectl exec -it <pod-name> -n position-service -- cat /app/config/application.yml
```

## Security Considerations

The Position Service deployment includes several security measures:

1. **Non-root execution**: The service runs as a non-privileged user (UID 1000)
2. **Read-only filesystem**: The root filesystem is mounted as read-only
3. **Network policies**: Strict network policies limit communication to necessary services only
4. **Resource limits**: Prevent resource exhaustion attacks

## Maintenance

### Updating the Service

To update the Position Service to a new version:

```bash
kubectl set image deployment/position-service position-service=traccar/position:new-version -n position-service
```

Or update the image tag in `deployment.yaml` and apply:

```bash
kubectl apply -f deployment.yaml
```

### Backup and Restore

The Position Service itself is stateless, but the position data is stored in the database. Ensure you have a proper backup strategy for the database.

## Integration with Other Services

The Position Service integrates with other Traccar microservices:

1. **Protocol Service**: Consumes raw position data from the message broker
2. **Event Service**: Publishes enriched positions for event detection
3. **API Gateway**: Provides REST API access to position data
4. **Reporting Service**: Provides position data for report generation

## Additional Resources

- [Traccar Documentation](https://www.traccar.org/documentation/)
- [Position Service API Documentation](https://www.traccar.org/api-reference/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)