# Protocol Service Health and Metrics Endpoints

This document describes the health and metrics endpoints provided by the Protocol Service for monitoring its health and performance.

## Health Endpoints

The Protocol Service exposes standardized health endpoints that follow Spring Boot Actuator conventions, designed for integration with container orchestration platforms like Kubernetes.

### Base Health Endpoint

```
GET /actuator/health
```

Returns the overall health status of the service, aggregating all health indicators.

**Example Response:**

```json
{
  "status": "UP",
  "components": {
    "db": {
      "status": "UP"
    },
    "messageBroker": {
      "status": "UP"
    },
    "diskSpace": {
      "status": "UP",
      "details": {
        "total": 499963170816,
        "free": 328492896256,
        "threshold": 10485760
      }
    }
  }
}
```

### Liveness Probe

```
GET /actuator/health/liveness
```

Used by Kubernetes to determine if the service is running. A successful response indicates the service is alive and not deadlocked.

**Example Response:**

```json
{
  "status": "UP"
}
```

### Readiness Probe

```
GET /actuator/health/readiness
```

Used by Kubernetes to determine if the service is ready to accept traffic. This checks all dependencies including database connections, message broker connectivity, and other required services.

**Example Response:**

```json
{
  "status": "UP",
  "components": {
    "db": {
      "status": "UP"
    },
    "messageBroker": {
      "status": "UP"
    },
    "serviceDiscovery": {
      "status": "UP"
    }
  }
}
```

Possible status values:
- `UP`: The service is healthy and ready to accept requests
- `DOWN`: The service is unhealthy and should not receive traffic
- `OUT_OF_SERVICE`: The service is temporarily unavailable
- `UNKNOWN`: The health status could not be determined

### Health Indicators

The Protocol Service implements the following health indicators:

| Indicator | Description | Failure Criteria |
|-----------|-------------|------------------|
| `db` | Database connectivity | Connection pool cannot validate connections |
| `messageBroker` | Message broker connectivity | Cannot connect to broker or publish messages |
| `diskSpace` | Available disk space | Free space below configured threshold |
| `serviceDiscovery` | Service registry connectivity | Cannot register with service discovery |
| `protocolConnections` | Protocol connection status | Connection drop ratio exceeds threshold |

## Metrics Endpoint

The Protocol Service exposes a Prometheus-compatible metrics endpoint:

```
GET /actuator/prometheus
```

This endpoint returns metrics in the OpenMetrics format, suitable for scraping by Prometheus.

### Key Metrics

The Protocol Service exposes the following categories of metrics:

#### Service-Level Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `protocol_service_active_connections` | Gauge | Number of active device connections per protocol |
| `protocol_service_messages_received_total` | Counter | Total number of messages received per protocol |
| `protocol_service_messages_processed_total` | Counter | Total number of messages successfully processed |
| `protocol_service_messages_dropped_total` | Counter | Total number of messages dropped due to errors |
| `protocol_service_message_processing_time_seconds` | Histogram | Message processing time distribution |
| `protocol_service_position_published_total` | Counter | Total number of positions published to message broker |

#### JVM Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `jvm_memory_used_bytes` | Gauge | JVM memory usage |
| `jvm_memory_max_bytes` | Gauge | Maximum available JVM memory |
| `jvm_gc_pause_seconds_count` | Counter | Number of garbage collection pauses |
| `jvm_gc_pause_seconds_sum` | Counter | Total time spent in garbage collection pauses |
| `jvm_threads_states_threads` | Gauge | Thread count by state |

#### System Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `system_cpu_usage` | Gauge | System CPU usage |
| `process_cpu_usage` | Gauge | Process CPU usage |
| `system_load_average_1m` | Gauge | System load average (1 minute) |
| `disk_free_bytes` | Gauge | Free disk space in bytes |
| `disk_total_bytes` | Gauge | Total disk space in bytes |

#### Protocol-Specific Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `protocol_<name>_active_connections` | Gauge | Active connections for specific protocol |
| `protocol_<name>_messages_received_total` | Counter | Messages received for specific protocol |
| `protocol_<name>_message_processing_time_seconds` | Histogram | Processing time for specific protocol |

## OpenTelemetry Integration

The Protocol Service implements OpenTelemetry for distributed tracing and correlation of logs, metrics, and traces.

### Trace Context Propagation

The service propagates trace context using W3C Trace Context headers in all outgoing requests and message broker publications. This enables end-to-end tracing across service boundaries.

Key trace information included:
- Trace ID: Unique identifier for the entire request flow
- Span ID: Identifier for the current operation
- Trace Flags: Sampling decisions and other trace metadata

### Correlation with Logs and Metrics

All logs include trace and span IDs when available, allowing correlation between logs, metrics, and traces. Metrics can be filtered by trace ID for detailed performance analysis of specific request flows.

## Kubernetes Integration

### Example Probe Configuration

```yaml
livenessProbe:
  httpGet:
    path: /actuator/health/liveness
    port: 8080
  initialDelaySeconds: 60
  periodSeconds: 15
  timeoutSeconds: 5
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /actuator/health/readiness
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10
  successThreshold: 1
  failureThreshold: 3
```

### Prometheus Scraping Configuration

```yaml
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/path: "/actuator/prometheus"
  prometheus.io/port: "8080"
```

## Monitoring Integration

The health and metrics endpoints can be integrated with various monitoring systems:

- **Prometheus**: Scrape metrics from `/actuator/prometheus` endpoint
- **Grafana**: Visualize metrics using Prometheus as a data source
- **Kubernetes**: Use health endpoints for liveness and readiness probes
- **Jaeger/Zipkin**: Collect distributed traces via OpenTelemetry
- **ELK Stack**: Aggregate logs with correlation IDs for trace context

## Troubleshooting

If the service reports unhealthy status:

1. Check the detailed health endpoint (`/actuator/health`) to identify which component is failing
2. Verify connectivity to dependencies (database, message broker, service discovery)
3. Check logs for error messages with the same timestamp as the health check failure
4. Verify resource utilization (CPU, memory, disk space) using metrics endpoint
5. Check for circuit breakers in OPEN state that might indicate dependency failures