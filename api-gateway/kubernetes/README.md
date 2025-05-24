# API Gateway Service - Kubernetes Deployment

## Overview

The API Gateway Service is a critical component of the Traccar microservices architecture, serving as the unified entry point for all external API interactions. This service routes client requests to appropriate backend services, handles authentication and authorization, and maintains WebSocket connections for real-time updates.

### Key Responsibilities

- Routing client requests to appropriate backend microservices
- Authentication and authorization of API requests
- WebSocket session management for real-time updates
- Request throttling and rate limiting
- Maintaining backward compatibility with existing client applications

## Prerequisites

Before deploying the API Gateway Service, ensure you have the following:

- Kubernetes cluster (version 1.23+)
- kubectl configured to communicate with your cluster
- Helm (version 3.11+) if using Helm charts for deployment
- Access to the Traccar container registry

## Installation

### Using kubectl

1. Create the namespace for the API Gateway Service:

```bash
kubectl create namespace api-gateway
```

2. Apply the Kubernetes manifests:

```bash
kubectl apply -f api-gateway-deployment.yaml -n api-gateway
kubectl apply -f api-gateway-service.yaml -n api-gateway
kubectl apply -f api-gateway-configmap.yaml -n api-gateway
```

3. Verify the deployment:

```bash
kubectl get pods -n api-gateway
kubectl get services -n api-gateway
```

### Using Helm

1. Add the Traccar Helm repository:

```bash
helm repo add traccar https://charts.traccar.org
helm repo update
```

2. Install the API Gateway Service chart:

```bash
helm install api-gateway traccar/api-gateway -n api-gateway --create-namespace
```

3. Customize the installation using values file:

```bash
helm install api-gateway traccar/api-gateway -n api-gateway --create-namespace -f custom-values.yaml
```

## Configuration

### Environment Variables

The API Gateway Service can be configured using the following environment variables:

| Variable | Description | Default |
|----------|-------------|--------|
| `SERVER_PORT` | HTTP port for REST API | `8080` |
| `WEBSOCKET_PORT` | WebSocket port | `8082` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `SERVICE_DISCOVERY_URL` | Service discovery endpoint | `http://consul:8500` |
| `AUTH_ENABLED` | Enable authentication | `true` |
| `AUTH_JWT_SECRET` | JWT secret for token validation | - |
| `RATE_LIMIT_ENABLED` | Enable rate limiting | `true` |
| `RATE_LIMIT_REQUESTS_PER_SECOND` | Rate limit threshold | `100` |

### ConfigMap and Secrets

Sensitive configuration should be stored in Kubernetes Secrets:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: api-gateway-secrets
  namespace: api-gateway
type: Opaque
data:
  auth-jwt-secret: <base64-encoded-secret>
```

Non-sensitive configuration can be stored in ConfigMaps:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: api-gateway-config
  namespace: api-gateway
data:
  application.yml: |
    server:
      port: 8080
    websocket:
      port: 8082
    logging:
      level:
        root: INFO
        org.traccar: DEBUG
```

## Port Configuration

The API Gateway Service exposes the following ports:

| Port | Protocol | Description |
|------|----------|-------------|
| 8080 | HTTP | REST API interface |
| 8082 | WebSocket | Real-time updates interface |
| 8081 | HTTP | Management and health check endpoints |

These ports can be customized using environment variables or ConfigMaps.

## Customization for Different Environments

### Development Environment

```yaml
replicas: 1
resources:
  requests:
    cpu: 0.2
    memory: 256Mi
  limits:
    cpu: 0.5
    memory: 512Mi
logLevel: DEBUG
rateLimit:
  enabled: false
```

### Production Environment

```yaml
replicas: 3
resources:
  requests:
    cpu: 0.5
    memory: 512Mi
  limits:
    cpu: 1
    memory: 1Gi
logLevel: INFO
rateLimit:
  enabled: true
  requestsPerSecond: 100
autoscaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70
```

## Scaling Configuration

The API Gateway Service can be scaled horizontally to handle increased load. Configure the Horizontal Pod Autoscaler (HPA) for automatic scaling:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: api-gateway-hpa
  namespace: api-gateway
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: api-gateway
  minReplicas: 3
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
```

## Health Checks

The API Gateway Service exposes health check endpoints that are used by Kubernetes to determine the health of the service:

- Liveness Probe: `/health/liveness`
- Readiness Probe: `/health/readiness`

These endpoints are configured in the deployment manifest:

```yaml
livenessProbe:
  httpGet:
    path: /health/liveness
    port: 8081
  initialDelaySeconds: 60
  periodSeconds: 15
  timeoutSeconds: 5
  failureThreshold: 3
readinessProbe:
  httpGet:
    path: /health/readiness
    port: 8081
  initialDelaySeconds: 30
  periodSeconds: 10
  successThreshold: 1
  failureThreshold: 3
```

## Troubleshooting

### Common Issues

#### Service Unavailable

**Symptoms**: 503 Service Unavailable errors when accessing the API

**Possible causes**:
- Backend services are not available
- Service discovery is not functioning correctly

**Resolution**:
1. Check the status of backend services:
   ```bash
   kubectl get pods -n position-service
   kubectl get pods -n event-service
   ```
2. Verify service discovery configuration:
   ```bash
   kubectl describe configmap api-gateway-config -n api-gateway
   ```
3. Check API Gateway logs for connection errors:
   ```bash
   kubectl logs -l app=api-gateway -n api-gateway
   ```

#### Authentication Failures

**Symptoms**: 401 Unauthorized errors when accessing protected endpoints

**Possible causes**:
- JWT secret misconfiguration
- Token validation issues

**Resolution**:
1. Verify JWT secret configuration:
   ```bash
   kubectl describe secret api-gateway-secrets -n api-gateway
   ```
2. Check API Gateway logs for authentication errors:
   ```bash
   kubectl logs -l app=api-gateway -n api-gateway | grep "Authentication"
   ```

#### WebSocket Connection Issues

**Symptoms**: Clients unable to establish or maintain WebSocket connections

**Possible causes**:
- Ingress controller WebSocket configuration
- Network policies blocking WebSocket traffic

**Resolution**:
1. Verify ingress controller configuration for WebSocket support:
   ```yaml
   annotations:
     nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
     nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
   ```
2. Check network policies:
   ```bash
   kubectl get networkpolicies -n api-gateway
   ```
3. Examine API Gateway logs for WebSocket errors:
   ```bash
   kubectl logs -l app=api-gateway -n api-gateway | grep "WebSocket"
   ```

### Viewing Logs

To view logs for the API Gateway Service:

```bash
kubectl logs -l app=api-gateway -n api-gateway
```

For continuous log streaming:

```bash
kubectl logs -l app=api-gateway -n api-gateway -f
```

Filter logs for specific content:

```bash
kubectl logs -l app=api-gateway -n api-gateway | grep "ERROR"
```

### Monitoring

The API Gateway Service exposes Prometheus metrics at the `/actuator/prometheus` endpoint. Configure Prometheus to scrape these metrics for monitoring:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: api-gateway-monitor
  namespace: monitoring
spec:
  selector:
    matchLabels:
      app: api-gateway
  endpoints:
  - port: management
    path: /actuator/prometheus
    interval: 15s
```

Key metrics to monitor:
- HTTP request rate and latency
- WebSocket connection count
- Authentication success/failure rate
- Error rate by endpoint

## Additional Resources

- [API Gateway Service Documentation](https://docs.traccar.org/api-gateway/)
- [Traccar Microservices Architecture](https://docs.traccar.org/architecture/)
- [Kubernetes Best Practices](https://kubernetes.io/docs/concepts/configuration/overview/)