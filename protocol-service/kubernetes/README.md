# Protocol Service Kubernetes Deployment Guide

## Overview

The Protocol Service is a critical component of the Traccar microservices architecture, responsible for handling device connections and protocol implementations for 200+ GPS device protocols. This service acts as the entry point for all device data, receiving raw messages from GPS devices, decoding them into a standardized format, and publishing them to the message broker for further processing by other services.

### Key Responsibilities

- Managing device connections across 200+ protocols using Netty for network I/O
- Decoding protocol-specific messages into standardized position data
- Publishing raw position data to the message broker
- Handling device commands and responses
- Maintaining connection state and session information

## Prerequisites

Before deploying the Protocol Service, ensure you have the following prerequisites:

- Kubernetes cluster (version 1.23+)
- kubectl CLI configured to access your cluster
- Message broker (Kafka/RabbitMQ) deployed and accessible
- Service discovery mechanism (Consul/Kubernetes) configured
- Namespace created for the Protocol Service (`protocol-service` recommended)

## Installation

### Using kubectl

1. Clone the repository or download the Kubernetes manifests

2. Create the namespace if it doesn't exist:

   ```bash
   kubectl create namespace protocol-service
   ```

3. Apply the ConfigMap:

   ```bash
   kubectl apply -f protocol-service/kubernetes/configmap.yaml
   ```

4. Apply the Service:

   ```bash
   kubectl apply -f protocol-service/kubernetes/service.yaml
   ```

5. Apply the Deployment:

   ```bash
   kubectl apply -f protocol-service/kubernetes/deployment.yaml
   ```

6. Apply the HorizontalPodAutoscaler (optional):

   ```bash
   kubectl apply -f protocol-service/kubernetes/hpa.yaml
   ```

7. Apply the NetworkPolicy (recommended):

   ```bash
   kubectl apply -f protocol-service/kubernetes/networkpolicy.yaml
   ```

### Using Helm

If you're using Helm for deployment, follow these steps:

1. Add the Traccar Helm repository:

   ```bash
   helm repo add traccar https://traccar.github.io/helm-charts
   helm repo update
   ```

2. Install the Protocol Service chart:

   ```bash
   helm install protocol-service traccar/protocol-service \
     --namespace protocol-service \
     --create-namespace \
     --set messagebroker.type=kafka \
     --set messagebroker.host=kafka.infrastructure \
     --set discovery.type=kubernetes
   ```

## Configuration

The Protocol Service can be configured through the ConfigMap or by providing environment variables to the deployment.

### Essential Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|--------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `kafka:9092` |
| `KAFKA_TOPIC_RAW_POSITIONS` | Topic for raw position data | `raw-positions` |
| `SERVICE_DISCOVERY_TYPE` | Service discovery mechanism | `kubernetes` |
| `SERVICE_DISCOVERY_HOST` | Service discovery host | `consul.infrastructure` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `PROTOCOL_ENABLED_LIST` | Comma-separated list of enabled protocols | All protocols |
| `MAX_CONNECTIONS` | Maximum number of concurrent connections | `1000` |
| `CONNECTION_TIMEOUT` | Connection timeout in seconds | `300` |

### Environment-Specific Configuration

#### Development Environment

```yaml
resources:
  requests:
    cpu: 0.2
    memory: 256Mi
  limits:
    cpu: 0.5
    memory: 512Mi
replicas: 1
protocolEnabledList: "osmand,teltonika,meitrack,gt06"
```

#### Production Environment

```yaml
resources:
  requests:
    cpu: 0.5
    memory: 512Mi
  limits:
    cpu: 1
    memory: 1Gi
replicas: 3
autoscaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70
  targetMemoryUtilizationPercentage: 80
```

## Port Configuration

The Protocol Service requires specific ports to be exposed for different device protocols. The service configuration includes the following port mappings:

| Port | Protocol | Description |
|------|----------|-------------|
| 5000 | TCP | Default protocol port |
| 5001 | TCP | OsmAnd protocol |
| 5002 | TCP | Teltonika protocol |
| 5003 | TCP | Meitrack protocol |
| 5004 | TCP | Suntech protocol |
| 5005 | TCP | GT06 protocol |
| 5006 | TCP | Concox protocol |
| 5007 | TCP | Coban protocol |
| 5008 | TCP | Meiligao protocol |
| 5009 | TCP | Totem protocol |
| 5010 | TCP | Xexun protocol |
| 5011 | TCP | TK103 protocol |
| 5012 | TCP | Atrack protocol |
| 5013 | UDP | GPS103 protocol |
| 5014 | UDP | H02 protocol |
| 5015 | UDP | Eelink protocol |
| 5016-5090 | TCP/UDP | Other protocols |
| 8090 | HTTP | Management API |

> **Note**: For a complete list of all 200+ supported protocols and their port configurations, refer to the [Protocol Documentation](../docs/protocols/README.md).

## Resource Requirements

The Protocol Service has the following recommended resource allocations:

- **CPU**: 0.5 cores (request), 1 core (limit)
- **Memory**: 512MB (request), 1GB (limit)
- **Storage**: No persistent storage required
- **Network**: Requires external access for device connections

### Scaling Considerations

The Protocol Service scales horizontally based on the following metrics:

- CPU utilization (target: 70%)
- Memory utilization (target: 80%)
- Active connection count (target: <1000 connections per instance)

For production deployments, it's recommended to enable the HorizontalPodAutoscaler to automatically adjust the number of replicas based on these metrics.

## Troubleshooting

### Common Issues

#### Devices Cannot Connect to the Service

1. Verify that the Service is properly exposed:
   ```bash
   kubectl get svc -n protocol-service
   ```

2. Check if the ports are correctly configured in the Service:
   ```bash
   kubectl describe svc protocol-service -n protocol-service
   ```

3. Ensure that any external load balancers or ingress controllers are properly configured to forward traffic to the Service.

4. Verify that network policies allow incoming connections:
   ```bash
   kubectl describe networkpolicy -n protocol-service
   ```

#### Service Fails to Start

1. Check the pod status:
   ```bash
   kubectl get pods -n protocol-service
   ```

2. Examine the pod logs:
   ```bash
   kubectl logs <pod-name> -n protocol-service
   ```

3. Verify that the ConfigMap is correctly applied:
   ```bash
   kubectl describe configmap protocol-service-config -n protocol-service
   ```

4. Check if the message broker is accessible from the Protocol Service pods.

#### High Resource Usage

1. Monitor resource usage:
   ```bash
   kubectl top pods -n protocol-service
   ```

2. Check if the HorizontalPodAutoscaler is working correctly:
   ```bash
   kubectl describe hpa protocol-service -n protocol-service
   ```

3. Consider adjusting resource limits or scaling parameters if consistently high resource usage is observed.

### Logging

The Protocol Service logs can be accessed using the following command:

```bash
kubectl logs -f <pod-name> -n protocol-service
```

For more detailed logging, adjust the `LOG_LEVEL` parameter in the ConfigMap or deployment environment variables.

## References

- [Protocol Service Architecture](../docs/architecture.md)
- [Protocol Documentation](../docs/protocols/README.md)
- [Traccar Microservices Overview](../../docs/microservices.md)
- [Kubernetes Best Practices](../../docs/kubernetes.md)