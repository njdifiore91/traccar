# API Gateway Operations Guide

## 1. Introduction

This document provides operational procedures for deploying, monitoring, and maintaining the API Gateway service in the Traccar microservices architecture. The API Gateway serves as the unified entry point for all client requests, handling authentication, request routing, and WebSocket connections.

### 1.1 Purpose

The API Gateway service is a critical component that:
- Routes client requests to appropriate backend microservices
- Handles authentication and authorization
- Manages WebSocket connections for real-time updates
- Provides a unified API interface to clients

### 1.2 Target Audience

This guide is intended for:
- System administrators responsible for deployment and maintenance
- DevOps engineers managing the infrastructure
- Support personnel handling troubleshooting
- SRE teams monitoring system health

## 2. Deployment Procedures

### 2.1 Prerequisites

Before deploying the API Gateway service, ensure the following prerequisites are met:

- Kubernetes cluster is operational (v1.22+)
- Helm is installed (v3.8+)
- Access to container registry with API Gateway images
- Service discovery mechanism is operational (Kubernetes Service objects)
- Message broker (Kafka/RabbitMQ) is deployed and accessible
- Required secrets and ConfigMaps are created

### 2.2 Containerized Deployment

The API Gateway service is deployed as a containerized application using Kubernetes and Helm:

```bash
# Add the Traccar Helm repository if not already added
helm repo add traccar https://traccar.github.io/helm-charts
helm repo update

# Deploy the API Gateway service
helm install api-gateway traccar/api-gateway \
  --namespace traccar \
  --values custom-values.yaml
```

#### 2.2.1 Configuration Values

Create a `custom-values.yaml` file with environment-specific configurations:

```yaml
replicas: 3

image:
  repository: your-registry/traccar/api-gateway
  tag: latest
  pullPolicy: Always

resources:
  requests:
    cpu: 500m
    memory: 512Mi
  limits:
    cpu: 1000m
    memory: 1Gi

service:
  type: ClusterIP
  port: 8082
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/path: "/actuator/prometheus"
    prometheus.io/port: "8081"

ingress:
  enabled: true
  annotations:
    kubernetes.io/ingress.class: nginx
    cert-manager.io/cluster-issuer: letsencrypt-prod
  hosts:
    - host: api.yourdomain.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: api-gateway-tls
      hosts:
        - api.yourdomain.com

config:
  logLevel: INFO
  logFormat: json
  correlationIdHeader: X-Correlation-ID

livenessProbe:
  path: /actuator/health/live
  initialDelaySeconds: 60
  periodSeconds: 10

readinessProbe:
  path: /actuator/health/ready
  initialDelaySeconds: 30
  periodSeconds: 10

startupProbe:
  path: /actuator/health/startup
  failureThreshold: 30
  periodSeconds: 10
```

### 2.3 Deployment Verification

After deployment, verify that the API Gateway service is running correctly:

```bash
# Check if pods are running
kubectl get pods -n traccar -l app=api-gateway

# Check service endpoints
kubectl get endpoints -n traccar api-gateway

# Verify logs for startup completion
kubectl logs -n traccar -l app=api-gateway

# Test health endpoint
kubectl port-forward -n traccar svc/api-gateway 8082:8082
curl http://localhost:8082/actuator/health
```

### 2.4 Scaling

The API Gateway service can be scaled horizontally to handle increased load:

```bash
# Manual scaling
kubectl scale deployment -n traccar api-gateway --replicas=5

# Automatic scaling with HPA
kubectl apply -f - <<EOF
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: api-gateway
  namespace: traccar
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: api-gateway
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
EOF
```

### 2.5 Upgrading

To upgrade the API Gateway service to a new version:

```bash
# Update the Helm repository
helm repo update

# Upgrade the API Gateway service
helm upgrade api-gateway traccar/api-gateway \
  --namespace traccar \
  --values custom-values.yaml
```

For zero-downtime upgrades, ensure that:
- Multiple replicas are running
- Proper readiness probes are configured
- Pod disruption budget is in place

```bash
# Create a PodDisruptionBudget
kubectl apply -f - <<EOF
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: api-gateway-pdb
  namespace: traccar
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: api-gateway
EOF
```

## 3. Monitoring and Health Checks

### 3.1 Health Check Endpoints

The API Gateway service exposes the following health check endpoints:

| Endpoint | Description | Success Criteria |
| --- | --- | --- |
| `/actuator/health/live` | Liveness check | Service is running and not deadlocked |
| `/actuator/health/ready` | Readiness check | Service is ready to accept traffic |
| `/actuator/health/startup` | Startup check | Service has completed initialization |

These endpoints return HTTP 200 when healthy and HTTP 503 when unhealthy.

### 3.2 Metrics

The API Gateway service exposes Prometheus-compatible metrics at `/actuator/prometheus` on port 8081. Key metrics include:

| Metric | Description | Type | Labels |
| --- | --- | --- | --- |
| `http_server_requests_seconds` | API request latency | Histogram | `method`, `uri`, `status`, `outcome` |
| `http_server_requests_total` | API request count | Counter | `method`, `uri`, `status`, `outcome` |
| `api_gateway_route_requests_total` | Requests per backend service | Counter | `service`, `method`, `status` |
| `api_gateway_circuit_breaker_state` | Circuit breaker state | Gauge | `service`, `state` |
| `jvm_memory_used_bytes` | JVM memory usage | Gauge | `area` |
| `jvm_gc_pause_seconds` | GC pause time | Summary | `action`, `cause` |
| `system_cpu_usage` | CPU usage | Gauge | - |

### 3.3 Monitoring Dashboard

A Grafana dashboard for the API Gateway service is available in the monitoring stack. The dashboard includes:

- Request rate and latency by endpoint
- Error rate and status code distribution
- Circuit breaker status
- Backend service routing metrics
- JVM and system resource utilization
- WebSocket connection statistics

Access the dashboard at: `https://grafana.yourdomain.com/d/api-gateway`

### 3.4 Alerts

The following alerts are configured for the API Gateway service:

| Alert | Condition | Severity | Action |
| --- | --- | --- | --- |
| ApiGatewayHighErrorRate | Error rate > 5% for 5m | Warning | Check logs for error patterns |
| ApiGatewayHighErrorRate | Error rate > 10% for 5m | Critical | Page on-call engineer |
| ApiGatewayHighLatency | p95 latency > 500ms for 5m | Warning | Check for performance issues |
| ApiGatewayCircuitBreakerOpen | Any circuit breaker open | Warning | Check affected backend service |
| ApiGatewayHighMemoryUsage | Memory usage > 85% for 5m | Warning | Consider scaling up |
| ApiGatewayInstanceDown | Instance not responding | Critical | Page on-call engineer |

## 4. Logging Configuration

### 4.1 Log Format

The API Gateway service uses structured JSON logging with the following fields:

```json
{
  "timestamp": "2023-05-24T12:34:56.789Z",
  "level": "INFO",
  "thread": "http-nio-8082-exec-1",
  "logger": "org.traccar.api.ApiGatewayController",
  "message": "Request processed successfully",
  "service": "api-gateway",
  "traceId": "4bdb00ebe2b70b06",
  "spanId": "4bdb00ebe2b70b06",
  "requestId": "123e4567-e89b-12d3-a456-426614174000",
  "userId": "1001",
  "path": "/api/devices",
  "method": "GET",
  "statusCode": 200,
  "duration": 45,
  "remoteAddr": "192.168.1.1"
}
```

### 4.2 Correlation IDs

The API Gateway service generates and propagates correlation IDs for request tracing across services:

- Incoming requests without a correlation ID header have one generated
- The correlation ID is included in all log entries related to the request
- The correlation ID is propagated to backend services via HTTP headers
- The correlation ID is included in message broker messages

The default header name is `X-Correlation-ID` and can be configured via the `config.correlationIdHeader` property.

### 4.3 Log Collection

Logs are collected using Fluent Bit as a sidecar container and forwarded to the centralized logging stack:

```yaml
sidecars:
  - name: fluent-bit
    image: fluent/fluent-bit:1.9
    volumeMounts:
      - name: varlog
        mountPath: /var/log
      - name: fluent-bit-config
        mountPath: /fluent-bit/etc/
    resources:
      limits:
        cpu: 100m
        memory: 128Mi
      requests:
        cpu: 50m
        memory: 64Mi

volumes:
  - name: varlog
    emptyDir: {}
  - name: fluent-bit-config
    configMap:
      name: api-gateway-fluent-bit-config
```

### 4.4 Log Levels

Log levels can be configured via environment variables or ConfigMap:

```yaml
config:
  logLevel: INFO  # Default log level
  logLevelOrgTraccar: DEBUG  # Package-specific log level
```

Available log levels: TRACE, DEBUG, INFO, WARN, ERROR

## 5. Troubleshooting

### 5.1 Common Issues

#### 5.1.1 Service Unavailable

**Symptoms:**
- HTTP 503 responses from the API Gateway
- Readiness probe failures

**Troubleshooting Steps:**
1. Check if backend services are available:
   ```bash
   kubectl get endpoints -n traccar
   ```
2. Verify service discovery is working:
   ```bash
   kubectl logs -n traccar -l app=api-gateway | grep "service discovery"
   ```
3. Check for circuit breaker activations in metrics:
   ```bash
   curl http://api-gateway:8081/actuator/prometheus | grep circuit_breaker
   ```

**Resolution:**
- Restart affected backend services if they are unhealthy
- Check network policies if services cannot communicate
- Reset circuit breakers if stuck in open state

#### 5.1.2 High Latency

**Symptoms:**
- Slow response times reported by clients
- Increased latency metrics

**Troubleshooting Steps:**
1. Check API Gateway resource utilization:
   ```bash
   kubectl top pods -n traccar -l app=api-gateway
   ```
2. Analyze request latency by endpoint:
   ```bash
   curl http://api-gateway:8081/actuator/prometheus | grep http_server_requests_seconds
   ```
3. Check backend service latency using distributed tracing in Jaeger

**Resolution:**
- Scale up API Gateway if resource-constrained
- Optimize slow endpoints or backend services
- Implement caching for frequently accessed data

#### 5.1.3 Authentication Failures

**Symptoms:**
- Increased HTTP 401/403 responses
- Authentication-related errors in logs

**Troubleshooting Steps:**
1. Check authentication service connectivity:
   ```bash
   kubectl logs -n traccar -l app=api-gateway | grep "authentication"
   ```
2. Verify JWT token validation configuration
3. Check for expired certificates if using mTLS

**Resolution:**
- Restart authentication service if unhealthy
- Update JWT validation keys if expired
- Renew TLS certificates if expired

### 5.2 Diagnostic Commands

#### 5.2.1 Checking Service Health

```bash
# Get overall health status
kubectl exec -n traccar deploy/api-gateway -- curl -s http://localhost:8082/actuator/health | jq

# Get detailed health information
kubectl exec -n traccar deploy/api-gateway -- curl -s http://localhost:8082/actuator/health/details | jq

# Check specific health indicator
kubectl exec -n traccar deploy/api-gateway -- curl -s http://localhost:8082/actuator/health/readiness | jq
```

#### 5.2.2 Analyzing Logs

```bash
# Get recent logs
kubectl logs -n traccar -l app=api-gateway --tail=100

# Get logs with errors
kubectl logs -n traccar -l app=api-gateway | grep -i error

# Get logs for a specific request using correlation ID
kubectl logs -n traccar -l app=api-gateway | grep "4bdb00ebe2b70b06"

# Stream logs in real-time
kubectl logs -n traccar -l app=api-gateway -f
```

#### 5.2.3 Checking Connectivity

```bash
# Test connectivity to backend services
kubectl exec -n traccar deploy/api-gateway -- curl -s http://position-service:8080/actuator/health
kubectl exec -n traccar deploy/api-gateway -- curl -s http://device-service:8080/actuator/health

# Test message broker connectivity
kubectl exec -n traccar deploy/api-gateway -- curl -s http://localhost:8082/actuator/health/kafka
```

### 5.3 Collecting Diagnostic Information

For comprehensive troubleshooting, collect the following diagnostic information:

```bash
# Create a diagnostic directory
mkdir -p api-gateway-diagnostics

# Collect pod information
kubectl describe pods -n traccar -l app=api-gateway > api-gateway-diagnostics/pods.txt

# Collect logs
kubectl logs -n traccar -l app=api-gateway --tail=1000 > api-gateway-diagnostics/logs.txt

# Collect health information
kubectl exec -n traccar deploy/api-gateway -- curl -s http://localhost:8082/actuator/health > api-gateway-diagnostics/health.json

# Collect metrics
kubectl exec -n traccar deploy/api-gateway -- curl -s http://localhost:8081/actuator/prometheus > api-gateway-diagnostics/metrics.txt

# Collect environment information
kubectl exec -n traccar deploy/api-gateway -- curl -s http://localhost:8082/actuator/env > api-gateway-diagnostics/env.json

# Collect thread dump
kubectl exec -n traccar deploy/api-gateway -- curl -s http://localhost:8082/actuator/threaddump > api-gateway-diagnostics/threaddump.json

# Collect heap dump (if memory issues)
kubectl exec -n traccar deploy/api-gateway -- jmap -dump:format=b,file=/tmp/heapdump.hprof 1
kubectl cp traccar/$(kubectl get pods -n traccar -l app=api-gateway -o jsonpath='{.items[0].metadata.name}'):/tmp/heapdump.hprof api-gateway-diagnostics/heapdump.hprof
```

## 6. Performance Tuning

### 6.1 JVM Tuning

Optimize JVM settings for the API Gateway service:

```yaml
extraEnv:
  - name: JAVA_OPTS
    value: >-
      -Xms512m
      -Xmx1g
      -XX:+UseG1GC
      -XX:MaxGCPauseMillis=200
      -XX:+HeapDumpOnOutOfMemoryError
      -XX:HeapDumpPath=/tmp/heapdump.hprof
      -XX:+UseStringDeduplication
      -Dserver.tomcat.max-threads=200
      -Dserver.tomcat.accept-count=100
```

### 6.2 Connection Pooling

Optimize connection pools for backend service communication:

```yaml
config:
  connectionPool:
    maxTotal: 200
    maxPerRoute: 50
    validateAfterInactivity: 1000
    timeToLive: 60000
    keepAliveTime: 30000
```

### 6.3 Caching

Implement caching for frequently accessed data:

```yaml
config:
  cache:
    enabled: true
    ttl: 300  # Time-to-live in seconds
    maxSize: 1000  # Maximum number of entries
```

Cacheable resources include:
- User permissions
- Device metadata
- Static configuration

### 6.4 Rate Limiting

Configure rate limiting to protect the API Gateway and backend services:

```yaml
config:
  rateLimit:
    enabled: true
    defaultLimit: 100  # Requests per minute per client
    ipHeaderName: X-Forwarded-For
```

Custom rate limits can be defined for specific endpoints:

```yaml
config:
  rateLimit:
    paths:
      - path: /api/session
        limit: 10  # Login attempts per minute
      - path: /api/devices
        limit: 200  # Device requests per minute
```

### 6.5 Circuit Breakers

Configure circuit breakers to prevent cascading failures:

```yaml
config:
  circuitBreaker:
    default:
      slidingWindowSize: 100
      failureRateThreshold: 50
      waitDurationInOpenState: 60000
      permittedNumberOfCallsInHalfOpenState: 10
    services:
      position-service:
        slidingWindowSize: 50
        failureRateThreshold: 30
      device-service:
        slidingWindowSize: 50
        failureRateThreshold: 30
```

### 6.6 WebSocket Optimization

Optimize WebSocket connections for real-time updates:

```yaml
config:
  webSocket:
    maxSessionsPerUser: 5
    maxTextMessageSize: 65536
    maxBinaryMessageSize: 65536
    asyncSendTimeout: 30000
    idleTimeout: 300000
```

## 7. References

- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Helm Documentation](https://helm.sh/docs/)
- [Spring Boot Actuator Documentation](https://docs.spring.io/spring-boot/docs/current/reference/html/actuator.html)
- [Prometheus Documentation](https://prometheus.io/docs/)
- [Fluent Bit Documentation](https://docs.fluentbit.io/)
- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)