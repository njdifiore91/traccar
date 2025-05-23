# Protocol Service Operations Guide

## Table of Contents

1. [Introduction](#introduction)
2. [Monitoring and Metrics](#monitoring-and-metrics)
3. [Logging](#logging)
4. [Scaling](#scaling)
5. [Health Checks](#health-checks)
6. [Maintenance Procedures](#maintenance-procedures)
7. [Performance Tuning](#performance-tuning)

## Introduction

This operations guide provides comprehensive instructions for day-to-day management of the Protocol Service in production environments. The Protocol Service is responsible for handling device connections and protocol implementations, decoding raw messages from 200+ supported device protocols into a standardized format. It acts as the entry point for all device data, with horizontal scalability for supporting thousands of concurrent connections.

### Service Responsibilities

- Managing device connections using Netty for non-blocking I/O
- Implementing 200+ device protocols for message decoding
- Publishing standardized position data to the message broker
- Maintaining device session state
- Executing device commands

## Monitoring and Metrics

The Protocol Service exposes metrics in Prometheus format for comprehensive monitoring of its operational status and performance.

### Available Metrics

The Protocol Service exposes the following metrics categories:

#### Connection Metrics

```
# HELP traccar_protocol_active_connections Current number of active device connections
# TYPE traccar_protocol_active_connections gauge
traccar_protocol_active_connections{protocol="tk103"} 157
traccar_protocol_active_connections{protocol="teltonika"} 243
```

#### Message Processing Metrics

```
# HELP traccar_protocol_messages_received_total Total number of messages received from devices
# TYPE traccar_protocol_messages_received_total counter
traccar_protocol_messages_received_total{protocol="tk103"} 15701
traccar_protocol_messages_received_total{protocol="teltonika"} 24389

# HELP traccar_protocol_messages_processed_total Total number of messages successfully processed
# TYPE traccar_protocol_messages_processed_total counter
traccar_protocol_messages_processed_total{protocol="tk103"} 15695
traccar_protocol_messages_processed_total{protocol="teltonika"} 24385

# HELP traccar_protocol_messages_dropped_total Total number of messages that failed processing
# TYPE traccar_protocol_messages_dropped_total counter
traccar_protocol_messages_dropped_total{protocol="tk103"} 6
traccar_protocol_messages_dropped_total{protocol="teltonika"} 4
```

#### Performance Metrics

```
# HELP traccar_protocol_message_processing_time_seconds Time taken to process a message
# TYPE traccar_protocol_message_processing_time_seconds histogram
traccar_protocol_message_processing_time_seconds_bucket{protocol="tk103",le="0.005"} 15690
traccar_protocol_message_processing_time_seconds_bucket{protocol="tk103",le="0.01"} 15694
traccar_protocol_message_processing_time_seconds_bucket{protocol="tk103",le="0.025"} 15695
traccar_protocol_message_processing_time_seconds_bucket{protocol="tk103",le="0.05"} 15695
traccar_protocol_message_processing_time_seconds_bucket{protocol="tk103",le="0.1"} 15695
traccar_protocol_message_processing_time_seconds_bucket{protocol="tk103",le="0.25"} 15695
traccar_protocol_message_processing_time_seconds_bucket{protocol="tk103",le="0.5"} 15695
traccar_protocol_message_processing_time_seconds_bucket{protocol="tk103",le="1.0"} 15695
traccar_protocol_message_processing_time_seconds_bucket{protocol="tk103",le="+Inf"} 15695
```

#### Resource Utilization Metrics

```
# HELP jvm_memory_used_bytes The amount of used memory
# TYPE jvm_memory_used_bytes gauge
jvm_memory_used_bytes{area="heap"} 1024458752
jvm_memory_used_bytes{area="nonheap"} 125685768

# HELP jvm_threads_states_threads The current number of threads
# TYPE jvm_threads_states_threads gauge
jvm_threads_states_threads{state="runnable"} 12
jvm_threads_states_threads{state="blocked"} 0
jvm_threads_states_threads{state="waiting"} 8
jvm_threads_states_threads{state="timed-waiting"} 4
```

#### Message Broker Metrics

```
# HELP traccar_protocol_broker_publish_total Total number of messages published to the broker
# TYPE traccar_protocol_broker_publish_total counter
traccar_protocol_broker_publish_total 40080

# HELP traccar_protocol_broker_publish_failures_total Total number of failed broker publish attempts
# TYPE traccar_protocol_broker_publish_failures_total counter
traccar_protocol_broker_publish_failures_total 12
```

### Accessing Metrics

Metrics are exposed via the `/actuator/prometheus` endpoint on port 8081 (management port):

```bash
curl http://<protocol-service-host>:8081/actuator/prometheus
```

In Kubernetes environments, metrics are automatically scraped by Prometheus using service discovery based on annotations:

```yaml
metadata:
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8081"
    prometheus.io/path: "/actuator/prometheus"
```

### Recommended Grafana Dashboards

The following Grafana dashboards are available for monitoring the Protocol Service:

1. **Protocol Service Overview**: General service health and performance metrics
2. **Protocol Service Connections**: Detailed connection metrics by protocol
3. **Protocol Service Message Processing**: Message throughput and error rates
4. **JVM Metrics**: JVM performance and garbage collection metrics

These dashboards are stored in the `deployment/terraform/modules/kubernetes/addons/monitoring/dashboards/` directory and are automatically provisioned when deploying the monitoring stack.

### Alert Configuration

The following alerts are preconfigured for the Protocol Service:

#### ProtocolServiceHighConnectionCount

```yaml
alert: ProtocolServiceHighConnectionCount
expr: sum(traccar_protocol_active_connections) > 5000
for: 5m
labels:
  severity: warning
annotations:
  summary: "High connection count on Protocol Service"
  description: "Protocol Service has {{ $value }} active connections, which is above the warning threshold of 5000."
```

#### ProtocolServiceHighMessageDropRate

```yaml
alert: ProtocolServiceHighMessageDropRate
expr: sum(rate(traccar_protocol_messages_dropped_total[5m])) / sum(rate(traccar_protocol_messages_received_total[5m])) > 0.01
for: 5m
labels:
  severity: warning
annotations:
  summary: "High message drop rate on Protocol Service"
  description: "Protocol Service is dropping {{ $value | humanizePercentage }} of messages, which is above the warning threshold of 1%."
```

#### ProtocolServiceBrokerPublishFailures

```yaml
alert: ProtocolServiceBrokerPublishFailures
expr: rate(traccar_protocol_broker_publish_failures_total[5m]) > 0
for: 5m
labels:
  severity: warning
annotations:
  summary: "Message broker publish failures on Protocol Service"
  description: "Protocol Service is experiencing failures publishing messages to the broker at a rate of {{ $value }} per second."
```

## Logging

The Protocol Service implements structured logging with JSON format to facilitate log aggregation and analysis.

### Log Configuration

Logging is configured through the following environment variables:

| Variable | Description | Default |
|----------|-------------|--------|
| `LOG_LEVEL` | Root logging level | `INFO` |
| `LOG_LEVEL_org.traccar` | Package-specific log level | `INFO` |
| `LOG_FORMAT` | Log format (json or plain) | `json` |
| `LOG_INCLUDE_EXCEPTION_STACKTRACE` | Include full stack traces | `true` |

Additionally, specific protocol logging can be enabled with:

```
LOG_LEVEL_org.traccar.protocol.tk103=DEBUG
```

### Log Structure

Structured logs include the following standard fields:

```json
{
  "timestamp": "2023-06-01T12:34:56.789Z",
  "level": "INFO",
  "thread": "nioEventLoopGroup-3-1",
  "logger": "org.traccar.protocol.tk103.Tk103ProtocolDecoder",
  "message": "Received message: 'LOGON 359710049095095 150145'",
  "serviceName": "protocol-service",
  "instanceId": "protocol-service-5d4f7c8b59-2nlzd",
  "traceId": "4bdb3f1cb975d0ed",
  "spanId": "fc27d8e902d5a3ee"
}
```

### Common Log Patterns

#### Device Connection

```
{"timestamp":"2023-06-01T12:34:56.789Z","level":"INFO","thread":"nioEventLoopGroup-3-1","logger":"org.traccar.protocol.tk103.Tk103ProtocolDecoder","message":"New device connected: 359710049095095","serviceName":"protocol-service"}
```

#### Message Processing

```
{"timestamp":"2023-06-01T12:34:57.123Z","level":"INFO","thread":"nioEventLoopGroup-3-1","logger":"org.traccar.protocol.tk103.Tk103ProtocolDecoder","message":"Position decoded: lat=37.7749, lon=-122.4194, speed=0.0, course=0.0","serviceName":"protocol-service"}
```

#### Error Handling

```
{"timestamp":"2023-06-01T12:35:01.456Z","level":"ERROR","thread":"nioEventLoopGroup-3-1","logger":"org.traccar.protocol.tk103.Tk103ProtocolDecoder","message":"Failed to decode message: 'LOGON 359710049095095 X'","exception":"java.lang.IllegalArgumentException: Invalid message format","stackTrace":"java.lang.IllegalArgumentException: Invalid message format\n\tat org.traccar.protocol.tk103.Tk103ProtocolDecoder.decode(Tk103ProtocolDecoder.java:157)\n...","serviceName":"protocol-service"}
```

### Log Aggregation

Logs are collected using Fluent Bit sidecars in Kubernetes environments and forwarded to Elasticsearch for centralized storage and analysis. The following Kibana dashboards are available for log analysis:

1. **Protocol Service Overview**: General service logs with filtering by level and component
2. **Protocol Errors**: Focused view of error logs with protocol-specific filters
3. **Device Connections**: Logs related to device connections and disconnections

### Log Retention

Logs are retained according to the following policy:

- Hot storage: 7 days
- Warm storage: 30 days
- Cold storage: 90 days

## Scaling

The Protocol Service is designed for horizontal scalability to handle increasing numbers of device connections.

### Scaling Triggers

Consider scaling the Protocol Service when:

1. The number of active connections per instance exceeds 2,000
2. CPU utilization consistently exceeds 70%
3. Memory utilization consistently exceeds 80%
4. Message processing latency exceeds 100ms (p95)

### Horizontal Pod Autoscaler Configuration

In Kubernetes environments, the Protocol Service uses the Horizontal Pod Autoscaler (HPA) for automatic scaling based on metrics:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: protocol-service
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: protocol-service
  minReplicas: 3
  maxReplicas: 20
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
        name: traccar_protocol_active_connections
      target:
        type: AverageValue
        averageValue: 2000
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

### Manual Scaling

To manually scale the Protocol Service in Kubernetes:

```bash
kubectl scale deployment protocol-service --replicas=5
```

For non-Kubernetes deployments, add additional service instances and update the load balancer configuration accordingly.

### Load Balancing Considerations

The Protocol Service requires TCP/UDP load balancing with the following considerations:

1. **Session Affinity**: Enable session affinity (sticky sessions) to ensure that all messages from a device are routed to the same service instance.

2. **Connection Draining**: Configure proper connection draining during scaling operations to allow existing connections to complete their transactions.

3. **Protocol-Specific Ports**: Ensure that all required protocol-specific ports are properly configured on the load balancer.

Example load balancer configuration for AWS Network Load Balancer:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: protocol-service-tcp
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: nlb
spec:
  type: LoadBalancer
  ports:
  - name: osmand
    port: 5055
    protocol: TCP
  - name: teltonika
    port: 5027
    protocol: TCP
  - name: tk103
    port: 5006
    protocol: TCP
  selector:
    app: protocol-service
```

### Resource Requirements

The Protocol Service has the following resource requirements per instance:

| Resource | Minimum | Recommended | High Load |
|----------|---------|-------------|----------|
| CPU | 0.5 cores | 1 core | 2 cores |
| Memory | 512 MB | 1 GB | 2 GB |
| Network | 100 Kbps per active device | 100 Kbps per active device | 100 Kbps per active device |

For capacity planning, use the following formula:

```
Required instances = Ceiling(Total active devices / 2,000)
```

Ensure that you maintain at least N+1 redundancy for high availability.

## Health Checks

The Protocol Service exposes health check endpoints that provide information about its operational status.

### Available Health Endpoints

#### Liveness Probe

The liveness probe verifies that the service is running and responsive:

```
GET /actuator/health/liveness
```

Example response:

```json
{
  "status": "UP"
}
```

#### Readiness Probe

The readiness probe verifies that the service is ready to accept traffic:

```
GET /actuator/health/readiness
```

Example response:

```json
{
  "status": "UP",
  "components": {
    "broker": {
      "status": "UP"
    },
    "diskSpace": {
      "status": "UP",
      "details": {
        "total": 21474836480,
        "free": 12938104832,
        "threshold": 10485760
      }
    },
    "ping": {
      "status": "UP"
    }
  }
}
```

#### Detailed Health Information

Detailed health information is available at:

```
GET /actuator/health
```

Example response:

```json
{
  "status": "UP",
  "components": {
    "broker": {
      "status": "UP",
      "details": {
        "type": "rabbit",
        "version": "3.8.9"
      }
    },
    "diskSpace": {
      "status": "UP",
      "details": {
        "total": 21474836480,
        "free": 12938104832,
        "threshold": 10485760
      }
    },
    "livenessState": {
      "status": "UP"
    },
    "ping": {
      "status": "UP"
    },
    "readinessState": {
      "status": "UP"
    }
  }
}
```

### Kubernetes Probe Configuration

In Kubernetes deployments, configure the following probes:

```yaml
livenessProbe:
  httpGet:
    path: /actuator/health/liveness
    port: 8081
  initialDelaySeconds: 60
  periodSeconds: 15
  timeoutSeconds: 5
  failureThreshold: 3
readinessProbe:
  httpGet:
    path: /actuator/health/readiness
    port: 8081
  initialDelaySeconds: 30
  periodSeconds: 10
  successThreshold: 1
  failureThreshold: 3
startupProbe:
  httpGet:
    path: /actuator/health/liveness
    port: 8081
  initialDelaySeconds: 30
  periodSeconds: 10
  failureThreshold: 30
```

### Health Check Interpretation

| Status | Component | Description | Action Required |
|--------|-----------|-------------|----------------|
| `UP` | All | Service is healthy | None |
| `DOWN` | `broker` | Cannot connect to message broker | Check broker connectivity and credentials |
| `DOWN` | `diskSpace` | Disk space below threshold | Increase disk space or clean up logs |
| `DOWN` | `ping` | Service is unresponsive | Restart service and check logs |

## Maintenance Procedures

### Routine Maintenance

#### Log Rotation

In non-Kubernetes environments, log rotation is handled by the container runtime or systemd. Ensure that log rotation is properly configured to prevent disk space issues.

For Kubernetes deployments, logs are automatically collected and managed by the logging infrastructure.

#### Configuration Updates

To update the service configuration:

1. Update the ConfigMap or environment variables
2. Restart the service or perform a rolling update

```bash
# Update ConfigMap
kubectl apply -f protocol-service-config.yaml

# Restart pods to apply changes
kubectl rollout restart deployment protocol-service
```

#### Protocol Updates

When new protocol versions are released:

1. Deploy the updated Protocol Service version
2. Monitor logs for any protocol-specific issues
3. Be prepared to rollback if issues are detected

### Backup and Restore

The Protocol Service is stateless, with device session information stored in memory. No specific backup procedures are required for the service itself.

However, ensure that the following items are backed up:

1. Service configuration (ConfigMaps, environment files)
2. Custom protocol implementations (if any)
3. Load balancer and network configurations

### Upgrade Procedures

To perform a zero-downtime upgrade of the Protocol Service:

1. Deploy the new version alongside the existing version
2. Gradually shift traffic to the new version
3. Monitor for any issues
4. Complete the transition once stability is confirmed

In Kubernetes environments, use rolling updates:

```bash
kubectl set image deployment/protocol-service protocol-service=traccar/protocol-service:new-version
```

Or apply an updated deployment manifest:

```bash
kubectl apply -f protocol-service-deployment.yaml
```

### Troubleshooting Common Issues

#### High Message Drop Rate

**Symptoms:**
- Increasing `traccar_protocol_messages_dropped_total` metric
- Error logs showing message decoding failures

**Resolution:**
1. Check logs for specific protocol errors
2. Verify that the protocol implementation matches the device firmware version
3. Increase logging level for the specific protocol for detailed debugging
4. Consider updating the protocol implementation if necessary

#### Connection Failures

**Symptoms:**
- Devices unable to connect
- Low `traccar_protocol_active_connections` metric

**Resolution:**
1. Verify network connectivity and firewall rules
2. Check that the correct ports are open and properly forwarded
3. Verify load balancer configuration
4. Check for any rate limiting or connection throttling

#### High CPU or Memory Usage

**Symptoms:**
- High CPU or memory metrics
- Service instability or crashes

**Resolution:**
1. Scale horizontally by adding more instances
2. Check for memory leaks by analyzing heap dumps
3. Optimize protocol decoders if specific protocols are causing high resource usage
4. Adjust JVM parameters for better performance

## Performance Tuning

### JVM Tuning

The Protocol Service runs on the JVM with the following recommended settings:

```
JAVA_OPTS="-Xms512m -Xmx1g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/var/log/protocol-service/heapdump.hprof"
```

For high-load environments, consider the following adjustments:

```
JAVA_OPTS="-Xms1g -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1ReservePercent=10 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/var/log/protocol-service/heapdump.hprof"
```

### Netty Configuration

The Protocol Service uses Netty for network I/O. The following parameters can be tuned for optimal performance:

| Parameter | Description | Default | High Load |
|-----------|-------------|---------|----------|
| `server.bossThreads` | Number of threads accepting connections | 1 | 2 |
| `server.workerThreads` | Number of threads processing I/O | CPU cores | CPU cores * 2 |
| `server.timeout` | Connection idle timeout (seconds) | 600 | 300 |

Example configuration for high-load environments:

```yaml
server:
  bossThreads: 2
  workerThreads: 16
  timeout: 300
```

### Protocol-Specific Optimizations

Some protocols may require specific optimizations:

#### High-Frequency Protocols (e.g., Teltonika)

For protocols that send data at high frequency:

```yaml
protocol:
  teltonika:
    messageBufferSize: 8192
    positionForwardingDelay: 1000
```

This configuration increases the buffer size for incoming messages and batches position updates to reduce broker load.

#### Memory-Intensive Protocols (e.g., OsmAnd)

For protocols that require more memory per connection:

```yaml
protocol:
  osmand:
    maxConnections: 1000
```

This limits the number of concurrent connections for this specific protocol to prevent memory issues.

### Network Configuration

Optimize network settings for high-throughput environments:

```yaml
server:
  socketBufferSize: 65536
  tcpNoDelay: true
  keepAlive: true
```

In Kubernetes environments, ensure that the container has appropriate sysctls configured:

```yaml
securityContext:
  sysctls:
  - name: net.core.somaxconn
    value: "4096"
  - name: net.ipv4.tcp_max_syn_backlog
    value: "4096"
```

### Message Broker Configuration

Optimize message broker settings for efficient position publishing:

```yaml
broker:
  batchSize: 100
  lingerMs: 10
  compressionType: lz4
  retries: 3
  bufferMemory: 33554432
```

This configuration enables message batching, compression, and adequate retry behavior for reliable delivery.

---

For additional assistance or to report issues with the Protocol Service, please contact the platform team or refer to the troubleshooting guide.