# Position Service Monitoring Guide

## Overview

This document provides comprehensive information about the monitoring capabilities, health checks, and observability features of the Position Processing Service. It is intended for operations teams to effectively monitor the service's health and performance in production environments.

## Metrics

The Position Service exposes a variety of metrics that provide insights into its operational status, performance, and resource utilization.

### Metrics Endpoint

Metrics are exposed via a standard `/metrics` endpoint in OpenMetrics format (Prometheus-compatible):

```
HTTP GET http://<position-service-host>:8081/metrics
```

This endpoint is automatically discovered by Prometheus through service discovery annotations in Kubernetes.

### Core Metrics

| Metric Name | Type | Description | Labels |
|-------------|------|-------------|--------|
| `position_messages_received_total` | Counter | Total number of position messages received | `protocol`, `device_id` |
| `position_messages_processed_total` | Counter | Total number of position messages successfully processed | `protocol`, `device_id` |
| `position_messages_filtered_total` | Counter | Total number of position messages filtered out | `protocol`, `device_id`, `filter_type` |
| `position_processing_time_seconds` | Histogram | Time taken to process a position message | `handler_type` |
| `position_queue_size` | Gauge | Current size of the position processing queue | `device_id` |
| `position_drop_ratio` | Gauge | Ratio of dropped to received position messages | - |
| `active_devices` | Gauge | Number of devices currently being processed | - |
| `geocoder_requests_total` | Counter | Total number of geocoder requests made | `status` |
| `geolocation_requests_total` | Counter | Total number of geolocation requests made | `status` |
| `database_operations_total` | Counter | Total number of database operations | `operation_type`, `status` |
| `external_service_requests_total` | Counter | Total number of requests to external services | `service_name`, `status` |

### JVM Metrics

Standard JVM metrics are also exposed:

| Metric Name | Type | Description |
|-------------|------|-------------|
| `jvm_memory_used_bytes` | Gauge | JVM memory usage by memory pool |
| `jvm_memory_committed_bytes` | Gauge | JVM memory committed by memory pool |
| `jvm_memory_max_bytes` | Gauge | JVM memory maximum by memory pool |
| `jvm_gc_collection_seconds` | Summary | GC collection time and count |
| `jvm_threads_current` | Gauge | Current thread count |
| `jvm_threads_daemon` | Gauge | Daemon thread count |
| `jvm_threads_peak` | Gauge | Peak thread count |

### Handler-Specific Metrics

Each position handler exposes specific metrics:

| Metric Name | Type | Description | Labels |
|-------------|------|-------------|--------|
| `handler_execution_time_seconds` | Histogram | Time taken by each handler to process a position | `handler_name` |
| `handler_errors_total` | Counter | Number of errors encountered by each handler | `handler_name`, `error_type` |
| `geocoder_cache_hits_total` | Counter | Number of geocoder cache hits | - |
| `geofence_checks_total` | Counter | Number of geofence checks performed | `result` |
| `distance_calculations_total` | Counter | Number of distance calculations performed | - |

## Health Checks

The Position Service implements standardized health check endpoints that align with container orchestration best practices.

### Health Check Endpoints

| Endpoint | Description | Use Case |
|----------|-------------|----------|
| `/actuator/health/live` | Basic liveness check | Kubernetes liveness probe, detects hung or deadlocked services |
| `/actuator/health/ready` | Readiness check including dependencies | Kubernetes readiness probe, determines if service can handle traffic |
| `/actuator/health/startup` | Startup check | Kubernetes startup probe, allows for longer initialization periods |
| `/actuator/health` | Aggregated health status | Manual health verification, includes detailed component status |

### Health Indicators

The Position Service implements the following health indicators:

| Health Indicator | Success Criteria | Failure Impact |
|------------------|------------------|----------------|
| Database Connectivity | Connection pool validates connections | Service marked as not ready |
| Message Processing | Drop ratio below configured threshold (default: 0.1) | Service marked as not ready |
| External Services | Services respond within timeout | Dependent on service criticality |
| Disk Space | Available space above threshold | Service marked as not ready when critical |
| Memory Utilization | Memory usage below threshold | Service marked as not ready when critical |

### Health Check Response Example

```json
{
  "status": "UP",
  "components": {
    "db": {
      "status": "UP",
      "details": {
        "database": "PostgreSQL",
        "validationQuery": "isValid()"
      }
    },
    "diskSpace": {
      "status": "UP",
      "details": {
        "total": 10737418240,
        "free": 8795836416,
        "threshold": 10485760
      }
    },
    "messageBroker": {
      "status": "UP",
      "details": {
        "type": "kafka"
      }
    },
    "ping": {
      "status": "UP"
    },
    "positionProcessing": {
      "status": "UP",
      "details": {
        "dropRatio": 0.01,
        "queueSize": 5
      }
    }
  }
}
```

## Logging

The Position Service implements structured logging to facilitate log aggregation and analysis.

### Log Format

Logs are emitted in JSON format with standardized fields:

```json
{
  "timestamp": "2023-06-01T12:34:56.789Z",
  "level": "INFO",
  "thread": "position-processor-1",
  "logger": "org.traccar.handler.BasePositionHandler",
  "message": "Processed position from device 123456",
  "serviceName": "position-service",
  "instanceId": "position-service-5d4f8",
  "traceId": "4bdb312e9d7b4ebd",
  "spanId": "fc8956a9d547c32b",
  "deviceId": "123456",
  "protocol": "osmand",
  "positionId": "789012"
}
```

### Log Levels

| Log Level | Usage |
|-----------|-------|
| ERROR | Service failures, unrecoverable errors, critical issues requiring immediate attention |
| WARN | Potential issues, degraded functionality, recoverable errors |
| INFO | Normal operational events, service startup/shutdown, configuration changes |
| DEBUG | Detailed information for troubleshooting, handler execution details |
| TRACE | Very detailed diagnostic information, including raw message contents (disabled in production) |

### Log Configuration

Logging is configured through environment variables and the `logback-spring.xml` configuration file:

| Parameter | Description | Default | Impact |
|-----------|-------------|---------|--------|
| `LOG_LEVEL` | Root logging level | `INFO` | Controls overall logging verbosity |
| `LOG_FORMAT` | Logging format (json or plain) | `json` | Determines log output format |
| `LOG_LEVEL_org.traccar` | Package-specific log level | `INFO` | Fine-grained logging control |
| `LOG_INCLUDE_EXCEPTION_STACKTRACE` | Include full stack traces | `true` | Controls exception detail level |
| `LOG_POSITION_ATTRIBUTES` | Position attributes to log | Empty | Controls position logging detail |

### Important Loggers

| Logger Name | Purpose | Recommended Level |
|-------------|---------|-------------------|
| `org.traccar.ProcessingHandler` | Main position processing pipeline | INFO |
| `org.traccar.handler` | Individual position handlers | INFO |
| `org.traccar.database` | Database operations | INFO |
| `org.traccar.geocoder` | Geocoding operations | INFO |
| `org.traccar.geolocation` | Geolocation operations | INFO |
| `org.traccar.handler.events` | Event detection | INFO |

## Distributed Tracing

The Position Service implements distributed tracing using OpenTelemetry to provide end-to-end visibility of request flows.

### Trace Context Propagation

Trace context is propagated through:

- HTTP headers using W3C Trace Context format
- Message broker messages with trace headers
- Database queries with trace annotations

### Instrumented Operations

| Operation | Instrumentation | Data Captured |
|-----------|----------------|---------------|
| Position Processing | Custom span creation | Device ID, position attributes, processing time |
| Handler Execution | Method-level tracing | Handler name, execution time, result |
| Database Operations | JDBC instrumentation | Query type, table, duration, row count |
| External API Calls | HTTP client instrumentation | URL, method, status code, duration |
| Message Operations | Messaging instrumentation | Topic, message key, processing duration |

### Tracing Configuration

Distributed tracing is configured through environment variables:

| Parameter | Description | Default |
|-----------|-------------|--------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OpenTelemetry collector endpoint | `http://otel-collector:4317` |
| `OTEL_SERVICE_NAME` | Service identifier for traces | `position-service` |
| `OTEL_RESOURCE_ATTRIBUTES` | Additional resource attributes | `deployment.environment=production` |
| `OTEL_TRACES_SAMPLER` | Sampling strategy | `parentbased_traceidratio` |
| `OTEL_TRACES_SAMPLER_ARG` | Sampling ratio (0.0-1.0) | `0.1` |
| `OTEL_PROPAGATORS` | Context propagation mechanisms | `tracecontext,baggage,b3` |

## Integration with Monitoring Tools

### Prometheus Integration

The Position Service is designed to be scraped by Prometheus for metrics collection:

```yaml
scrape_configs:
  - job_name: 'position-service'
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app]
        regex: position-service
        action: keep
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        regex: true
        action: keep
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
        regex: (.+)
        target_label: __metrics_path__
        action: replace
      - source_labels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_port]
        regex: ([^:]+)(?::\d+)?;(\d+)
        target_label: __address__
        replacement: $1:$2
        action: replace
```

### Grafana Dashboards

Recommended Grafana dashboards for the Position Service include:

1. **Position Service Overview**
   - Service health status
   - Message throughput (received, processed, filtered)
   - Processing latency percentiles
   - Active devices count
   - Error rates by handler

2. **Position Service Details**
   - Per-handler performance metrics
   - Queue depths by device
   - Database operation metrics
   - External service call metrics
   - Cache hit rates

3. **JVM Metrics**
   - Memory usage (heap, non-heap)
   - Garbage collection metrics
   - Thread utilization
   - CPU usage

### Alerting Recommendations

| Alert | Warning Threshold | Critical Threshold | Description |
|-------|-------------------|---------------------|-------------|
| PositionServiceDown | N/A | Service unreachable | Position service is not responding to health checks |
| HighPositionDropRatio | > 0.05 (5%) | > 0.1 (10%) | High ratio of dropped position messages |
| PositionProcessingLatency | p95 > 200ms | p95 > 500ms | Position processing is taking too long |
| QueueBacklog | > 1000 messages | > 5000 messages | Position processing queue is building up |
| DatabaseConnectionPool | > 80% utilized | > 90% utilized | Database connection pool is near capacity |
| ExternalServiceErrors | > 5% error rate | > 10% error rate | High error rate for external service calls |
| HighMemoryUsage | > 80% heap used | > 90% heap used | JVM memory usage is high |
| GarbageCollectionTime | > 10% CPU time | > 20% CPU time | Excessive time spent in garbage collection |

## Resource Requirements and Scaling

### Resource Requirements

| Resource | Minimum | Recommended | Per 1000 devices |
|----------|---------|-------------|------------------|
| CPU | 0.5 cores | 1 core | +0.2 cores |
| Memory | 512 MB | 1 GB | +200 MB |
| Disk Space | 1 GB | 2 GB | +100 MB |
| Network Bandwidth | 10 Mbps | 100 Mbps | +5 Mbps |

### Scaling Considerations

The Position Service scales horizontally based on the following metrics:

- **CPU Utilization**: Scale when CPU usage exceeds 70% for 5 minutes
- **Memory Usage**: Scale when memory usage exceeds 80% for 5 minutes
- **Messages Per Second**: Scale when processing more than 500 messages per second per instance
- **Processing Latency**: Scale when p95 latency exceeds 200ms for 5 minutes

Kubernetes HorizontalPodAutoscaler configuration example:

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
    scaleDown:
      stabilizationWindowSeconds: 300
```

## Troubleshooting

### Common Issues and Resolutions

| Issue | Symptoms | Troubleshooting Steps |
|-------|----------|----------------------|
| High position drop ratio | Increasing `position_drop_ratio` metric, WARNING logs | Check handler error logs, verify external service connectivity, check database performance |
| Slow position processing | Increasing `position_processing_time_seconds` metric | Check database performance, external service latency, JVM garbage collection metrics |
| Queue backlog | Increasing `position_queue_size` metric | Check processing capacity, consider scaling horizontally, verify database performance |
| External service failures | Increasing `external_service_requests_total` with error status | Check external service health, verify network connectivity, check for rate limiting |
| Database connectivity issues | Database health indicator DOWN, database operation errors | Check database server health, connection pool settings, network connectivity |
| Memory leaks | Steadily increasing memory usage without corresponding load increase | Analyze heap dumps, check for resource leaks in handlers, review GC logs |

### Diagnostic Commands

```bash
# Check service health
curl -X GET http://position-service:8081/actuator/health

# View detailed metrics
curl -X GET http://position-service:8081/metrics

# View specific metric
curl -X GET http://position-service:8081/metrics/position_messages_processed_total

# Check service info
curl -X GET http://position-service:8081/actuator/info

# View environment configuration
curl -X GET http://position-service:8081/actuator/env
```

## Conclusion

Effective monitoring of the Position Service is critical for ensuring the reliability and performance of the Traccar platform. By leveraging the metrics, health checks, and logging capabilities described in this document, operations teams can proactively identify and address issues before they impact users.

For additional information, refer to the following resources:

- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
- [Spring Boot Actuator Documentation](https://docs.spring.io/spring-boot/docs/current/reference/html/actuator.html)