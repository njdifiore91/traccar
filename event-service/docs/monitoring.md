# Event Processing Service Monitoring Guide

This document provides comprehensive information about monitoring, logging, metrics, and health checks for the Event Processing Service. It is intended for operators and SREs responsible for maintaining the service in production environments.

## Table of Contents

1. [Overview](#overview)
2. [Health Checks](#health-checks)
3. [Metrics](#metrics)
4. [Logging](#logging)
5. [Distributed Tracing](#distributed-tracing)
6. [Alerts and Dashboards](#alerts-and-dashboards)
7. [Troubleshooting](#troubleshooting)

## Overview

The Event Processing Service is responsible for detecting and processing events based on position data. It consumes position messages from the message broker, applies event detection rules, and publishes resulting events to the notification topic.

The service implements a comprehensive observability stack with:
- Health check endpoints for liveness and readiness probes
- Prometheus metrics for performance monitoring
- Structured JSON logging with correlation IDs
- OpenTelemetry distributed tracing

## Health Checks

The Event Processing Service exposes the following health check endpoints:

### Endpoints

| Endpoint | Purpose | Success Code | Failure Code |
|----------|---------|--------------|--------------||
| `/actuator/health/live` | Liveness probe - verifies the service is running | 200 OK | 503 Service Unavailable |
| `/actuator/health/ready` | Readiness probe - verifies the service can process requests | 200 OK | 503 Service Unavailable |
| `/actuator/health/startup` | Startup probe - verifies the service has completed initialization | 200 OK | 503 Service Unavailable |

### Health Indicators

The service includes the following health indicators:

| Indicator | Description | Failure Conditions |
|-----------|-------------|--------------------|
| `messageBroker` | Checks connectivity to the message broker | Connection failure, authentication issues |
| `database` | Checks database connectivity | Connection pool exhaustion, query timeouts |
| `messageProcessing` | Monitors event processing pipeline | Drop ratio exceeding threshold (default: 0.1 or 10%) |
| `diskSpace` | Monitors available disk space | Available space below threshold (default: 10%) |
| `memory` | Monitors JVM memory usage | Heap usage above threshold (default: 90%) |

### Health Check Configuration

Health check behavior can be configured through the following parameters:

```yaml
management:
  endpoint:
    health:
      show-details: when_authorized
      group:
        liveness:
          include: livenessState
        readiness:
          include: readinessState,db,messageBroker
  endpoints:
    web:
      exposure:
        include: health,info,metrics
  server:
    port: 8081

message:
  processing:
    drop-threshold: 0.1
```

## Metrics

The Event Processing Service exposes metrics via a Prometheus-compatible endpoint at `/actuator/prometheus`. These metrics provide insights into the service's performance, resource utilization, and business operations.

### Core Metrics

| Metric Name | Type | Description | Labels |
|-------------|------|-------------|--------|
| `event_processing_total` | Counter | Total number of events processed | `event_type`, `device_id`, `geofence_id` |
| `event_processing_duration_seconds` | Histogram | Time taken to process events | `event_type` |
| `event_detection_total` | Counter | Total number of events detected | `event_type` |
| `event_handler_errors_total` | Counter | Total number of errors in event handlers | `handler_name`, `error_type` |
| `message_processing_lag_seconds` | Gauge | Time difference between message timestamp and processing time | `topic` |
| `message_processing_rate` | Gauge | Current rate of message processing per second | `topic` |
| `message_drop_ratio` | Gauge | Ratio of dropped messages to total messages | `topic` |

### JVM Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `jvm_memory_used_bytes` | Gauge | JVM memory usage by memory pool |
| `jvm_memory_max_bytes` | Gauge | Maximum available JVM memory |
| `jvm_gc_pause_seconds_count` | Counter | Count of garbage collection pauses |
| `jvm_gc_pause_seconds_sum` | Counter | Total time spent in garbage collection pauses |
| `jvm_threads_states_threads` | Gauge | Current thread count by state |

### System Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `process_cpu_usage` | Gauge | CPU usage of the process |
| `system_cpu_usage` | Gauge | System CPU usage |
| `process_files_open` | Gauge | Number of open file descriptors |
| `system_load_average_1m` | Gauge | System load average for 1 minute |

### Message Broker Metrics

| Metric Name | Type | Description | Labels |
|-------------|------|-------------|--------|
| `broker_messages_consumed_total` | Counter | Total messages consumed from broker | `topic` |
| `broker_messages_published_total` | Counter | Total messages published to broker | `topic` |
| `broker_consumer_lag` | Gauge | Consumer lag in messages | `topic`, `consumer_group` |

### Circuit Breaker Metrics

| Metric Name | Type | Description | Labels |
|-------------|------|-------------|--------|
| `resilience4j_circuitbreaker_state` | Gauge | State of circuit breakers (0=closed, 1=open, 2=half-open) | `name` |
| `resilience4j_circuitbreaker_calls` | Counter | Count of circuit breaker calls | `name`, `kind` |
| `resilience4j_circuitbreaker_failure_rate` | Gauge | Failure rate of circuit breaker calls | `name` |

## Logging

The Event Processing Service uses structured JSON logging to facilitate log aggregation and analysis in centralized logging systems.

### Log Format

All logs are emitted in JSON format with the following standard fields:

```json
{
  "timestamp": "2023-05-24T12:34:56.789Z",
  "level": "INFO",
  "thread": "main",
  "logger": "org.traccar.handler.events.BaseEventHandler",
  "message": "Processing event",
  "serviceName": "event-service",
  "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
  "spanId": "fc2f8ae9df4b12cd",
  "instanceId": "event-service-5d87f79f5-xjl2p",
  "context": {
    "eventId": "e123456",
    "eventType": "deviceOverspeed",
    "deviceId": "d123456",
    "positionId": "p123456"
  }
}
```

### Log Levels

| Level | Usage |
|-------|-------|
| ERROR | Service failures, unrecoverable errors, critical issues requiring immediate attention |
| WARN | Potential issues, degraded functionality, recoverable errors |
| INFO | Operational events, service lifecycle events, significant business events |
| DEBUG | Detailed information for troubleshooting (not enabled in production by default) |
| TRACE | Very detailed diagnostic information (not enabled in production by default) |

### Correlation IDs

All logs include correlation IDs (`traceId` and `spanId`) that link related log entries across services. These IDs are propagated through:

- Message headers in broker messages
- HTTP headers in REST calls
- Thread context in asynchronous operations

### Log Configuration

Logging can be configured through the following parameters:

```yaml
logging:
  config: classpath:logback-spring.xml

LOG_LEVEL: INFO  # Environment variable for root logging level
LOG_FORMAT: json  # Environment variable for log format (json or plain)
LOG_LEVEL_org.traccar: INFO  # Package-specific log level
LOG_INCLUDE_EXCEPTION_STACKTRACE: true  # Include full stack traces
```

## Distributed Tracing

The Event Processing Service implements distributed tracing using OpenTelemetry to provide end-to-end visibility of request flows across service boundaries.

### Trace Context Propagation

Trace context is propagated through:

- W3C Trace Context headers in HTTP requests
- Message headers in broker messages
- Thread-local storage for in-process operations

### Instrumented Operations

| Operation | Attributes | Description |
|-----------|------------|-------------|
| Message consumption | `messaging.system`, `messaging.destination`, `messaging.operation` | Consuming messages from broker |
| Event processing | `event.type`, `event.id`, `device.id` | Processing events from positions |
| Database operations | `db.system`, `db.operation`, `db.statement` | Database interactions |
| External service calls | `http.method`, `http.url`, `http.status_code` | Calls to external services |

### Trace Configuration

Tracing can be configured through the following environment variables:

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
OTEL_SERVICE_NAME=event-service
OTEL_RESOURCE_ATTRIBUTES=deployment.environment=production
OTEL_TRACES_SAMPLER=parentbased_traceidratio
OTEL_TRACES_SAMPLER_ARG=0.1
OTEL_PROPAGATORS=tracecontext,baggage,b3
```

## Alerts and Dashboards

### Recommended Alerts

| Alert | Condition | Severity | Description |
|-------|-----------|----------|-------------|
| EventServiceDown | Instance count < 1 | Critical | Event service is not running |
| EventServiceHighErrorRate | Error rate > 5% for 5m | Warning | High rate of errors in event processing |
| EventServiceHighLatency | p95 latency > 500ms for 5m | Warning | Event processing is experiencing high latency |
| EventServiceMessageLag | Consumer lag > 1000 for 5m | Warning | Event service is falling behind in processing messages |
| EventServiceCircuitBreakerOpen | Circuit breaker state == OPEN | Warning | Circuit breaker to a dependency is open |
| EventServiceHighMemory | Memory usage > 85% for 5m | Warning | Service is approaching memory limits |
| EventServiceRestarting | Restart count > 3 in 1h | Warning | Service is restarting frequently |

### Alert Configuration

Prometheus alert rules for the Event Processing Service:

```yaml
groups:
- name: event-service-alerts
  rules:
  - alert: EventServiceDown
    expr: up{job="event-service"} == 0
    for: 1m
    labels:
      severity: critical
      service: event-service
    annotations:
      summary: "Event Service is down"
      description: "Event Service instance {{ $labels.instance }} has been down for more than 1 minute."

  - alert: EventServiceHighErrorRate
    expr: sum(rate(event_handler_errors_total[5m])) / sum(rate(event_processing_total[5m])) > 0.05
    for: 5m
    labels:
      severity: warning
      service: event-service
    annotations:
      summary: "High error rate in Event Service"
      description: "Event Service has a high error rate: {{ $value | humanizePercentage }} errors."

  - alert: EventServiceHighLatency
    expr: histogram_quantile(0.95, sum(rate(event_processing_duration_seconds_bucket[5m])) by (le)) > 0.5
    for: 5m
    labels:
      severity: warning
      service: event-service
    annotations:
      summary: "High latency in Event Service"
      description: "Event Service p95 latency is {{ $value | humanizeDuration }}."

  - alert: EventServiceMessageLag
    expr: broker_consumer_lag{topic=~"position.*", consumer_group="event-service"} > 1000
    for: 5m
    labels:
      severity: warning
      service: event-service
    annotations:
      summary: "Message processing lag in Event Service"
      description: "Event Service is lagging by {{ $value }} messages on topic {{ $labels.topic }}."
```

### Recommended Dashboards

#### Event Service Overview Dashboard

Key panels to include:

- Service health status
- Instance count and uptime
- Event processing rate by type
- Error rate by handler
- Processing latency (p50, p95, p99)
- Message broker lag
- JVM memory usage
- GC activity
- Thread count

#### Event Processing Performance Dashboard

Key panels to include:

- Events processed per second by type
- Event processing duration by type
- Event handler error count by type
- Database operation latency
- Circuit breaker status
- Message processing rate vs. capacity

#### Event Service Resource Usage Dashboard

Key panels to include:

- CPU usage per instance
- Memory usage per instance
- Heap vs. non-heap memory
- Thread states
- File descriptors
- Network I/O

## Troubleshooting

### Common Issues

#### High Event Processing Latency

Possible causes:
- Database connection pool exhaustion
- High CPU load
- GC pauses
- Slow external service dependencies

Diagnostic steps:
1. Check `event_processing_duration_seconds` metrics
2. Examine GC metrics for long pauses
3. Check database connection pool metrics
4. Verify external service response times
5. Review CPU and memory usage

#### Message Processing Lag

Possible causes:
- Insufficient processing capacity
- Slow event handlers
- Database bottlenecks
- Message broker issues

Diagnostic steps:
1. Check `broker_consumer_lag` metrics
2. Examine event processing rate vs. incoming message rate
3. Look for slow event handlers in `event_processing_duration_seconds` metrics
4. Verify database performance
5. Consider scaling the service horizontally

#### High Error Rate

Possible causes:
- Invalid message format
- Database connectivity issues
- External service failures
- Business logic errors

Diagnostic steps:
1. Check `event_handler_errors_total` metrics by error type
2. Examine logs for detailed error messages
3. Verify database health indicators
4. Check circuit breaker status for external dependencies
5. Review recent code or configuration changes

### Useful Commands

#### Checking Service Health

```bash
# Check liveness status
curl -s http://event-service:8081/actuator/health/live | jq

# Check readiness status
curl -s http://event-service:8081/actuator/health/ready | jq

# Get detailed health information
curl -s http://event-service:8081/actuator/health | jq
```

#### Querying Metrics

```bash
# Get all metrics
curl -s http://event-service:8081/actuator/prometheus

# Query specific metrics using Prometheus
promql 'sum(rate(event_processing_total[5m])) by (event_type)'

# Check message lag
promql 'broker_consumer_lag{consumer_group="event-service"}'
```

#### Viewing Logs

```bash
# View logs from Kubernetes
kubectl logs -l app=event-service -n traccar

# Filter logs by trace ID
kubectl logs -l app=event-service -n traccar | grep '"traceId":"4bf92f3577b34da6a3ce929d0e0e4736"'

# View logs in Kibana
# Use query: serviceName:"event-service" AND level:ERROR
```

#### Examining Traces

```bash
# Open Jaeger UI and search for:
# Service: event-service
# Tags: event.type=deviceOverspeed
```