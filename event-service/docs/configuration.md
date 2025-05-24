# Event Processing Service Configuration Guide

## Overview

The Event Processing Service is responsible for analyzing position data to detect significant events such as geofence transitions, speed violations, device status changes, and maintenance alerts. This document provides comprehensive guidance on configuring the service for different deployment scenarios.

## Configuration Methods

The Event Processing Service supports multiple configuration methods, with the following precedence (highest to lowest):

1. Environment variables
2. Configuration file (YAML/properties)
3. Default values

## Environment Variables

### Core Service Configuration

| Variable | Description | Default | Example |
|----------|-------------|---------|--------|
| `EVENT_SERVICE_PORT` | HTTP port for the service | `8090` | `8090` |
| `EVENT_SERVICE_HOST` | Bind address for the service | `0.0.0.0` | `0.0.0.0` |
| `EVENT_SERVICE_CONTEXT_PATH` | Base context path | `/api/events` | `/api/events` |
| `EVENT_SERVICE_THREADS` | Number of worker threads | `10` | `20` |
| `EVENT_SERVICE_MAX_CONNECTIONS` | Maximum concurrent connections | `100` | `200` |
| `EVENT_SERVICE_GRACEFUL_SHUTDOWN_SECONDS` | Graceful shutdown period in seconds | `30` | `60` |

### Event Detection Configuration

| Variable | Description | Default | Example |
|----------|-------------|---------|--------|
| `EVENT_OVERSPEED_MINIMAL_DURATION` | Minimum duration in seconds for overspeed events | `10` | `30` |
| `EVENT_OVERSPEED_PREFER_LOWEST` | Whether to prefer lowest speed limit when multiple apply | `true` | `false` |
| `EVENT_OVERSPEED_THRESHOLD_MULTIPLIER` | Multiplier for speed limit to trigger overspeed events | `1.0` | `1.1` |
| `EVENT_OVERSPEED_LIMIT` | Default speed limit in km/h if not specified elsewhere | `0` | `80` |
| `EVENT_MOTION_SPEED_THRESHOLD` | Speed threshold in km/h for motion detection | `0.01` | `1.0` |
| `EVENT_MOTION_STATIONARY_PERIOD` | Time in seconds before considering a device stationary | `180` | `300` |
| `EVENT_GEOFENCE_OPTIMIZATION` | Enable geofence calculation optimization | `true` | `false` |
| `EVENT_GEOFENCE_LIMIT` | Maximum number of geofences to check per position | `100` | `500` |
| `EVENT_IGNORE_DUPLICATE_ALERTS` | Ignore duplicate alerts within time window | `true` | `false` |
| `EVENT_DUPLICATE_ALERT_WINDOW` | Time window in seconds for duplicate alert detection | `300` | `600` |

### Message Broker Configuration

| Variable | Description | Default | Example |
|----------|-------------|---------|--------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `localhost:9092` | `kafka:9092` |
| `KAFKA_GROUP_ID` | Consumer group ID for the service | `event-service` | `event-service-prod` |
| `KAFKA_POSITION_TOPIC` | Topic for consuming position data | `enriched-positions` | `positions-processed` |
| `KAFKA_EVENT_TOPIC` | Topic for publishing detected events | `events` | `device-events` |
| `KAFKA_AUTO_OFFSET_RESET` | Auto offset reset policy | `latest` | `earliest` |
| `KAFKA_MAX_POLL_RECORDS` | Maximum records per poll | `500` | `1000` |
| `KAFKA_MAX_POLL_INTERVAL_MS` | Maximum poll interval in milliseconds | `300000` | `600000` |
| `KAFKA_ENABLE_AUTO_COMMIT` | Enable auto commit | `false` | `true` |

### Service Discovery Configuration

| Variable | Description | Default | Example |
|----------|-------------|---------|--------|
| `SERVICE_DISCOVERY_ENABLED` | Enable service discovery | `true` | `false` |
| `SERVICE_DISCOVERY_TYPE` | Service discovery type | `kubernetes` | `consul` |
| `SERVICE_DISCOVERY_KUBERNETES_NAMESPACE` | Kubernetes namespace | `default` | `traccar` |
| `SERVICE_DISCOVERY_CONSUL_HOST` | Consul host | `localhost` | `consul` |
| `SERVICE_DISCOVERY_CONSUL_PORT` | Consul port | `8500` | `8500` |
| `SERVICE_DISCOVERY_REGISTER` | Register with service discovery | `true` | `false` |
| `SERVICE_DISCOVERY_DEREGISTER_ON_SHUTDOWN` | Deregister on shutdown | `true` | `false` |

### Database Configuration

| Variable | Description | Default | Example |
|----------|-------------|---------|--------|
| `DATABASE_URL` | JDBC URL | `jdbc:h2:./database` | `jdbc:postgresql://postgres:5432/traccar` |
| `DATABASE_USERNAME` | Database username | `sa` | `traccar` |
| `DATABASE_PASSWORD` | Database password | `` | `password` |
| `DATABASE_MAX_POOL_SIZE` | Maximum connection pool size | `10` | `20` |
| `DATABASE_MIN_IDLE` | Minimum idle connections | `2` | `5` |
| `DATABASE_MAX_LIFETIME` | Maximum connection lifetime in milliseconds | `30000` | `60000` |

### Resilience Configuration

| Variable | Description | Default | Example |
|----------|-------------|---------|--------|
| `CIRCUIT_BREAKER_ENABLED` | Enable circuit breakers | `true` | `false` |
| `CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD` | Failure rate threshold percentage | `50` | `75` |
| `CIRCUIT_BREAKER_WAIT_DURATION_OPEN_STATE` | Wait duration in open state (seconds) | `60` | `120` |
| `RETRY_ENABLED` | Enable retry mechanism | `true` | `false` |
| `RETRY_MAX_ATTEMPTS` | Maximum retry attempts | `3` | `5` |
| `RETRY_BACKOFF_MULTIPLIER` | Backoff multiplier for retries | `2` | `1.5` |

### Logging Configuration

| Variable | Description | Default | Example |
|----------|-------------|---------|--------|
| `LOGGING_LEVEL_ROOT` | Root logging level | `INFO` | `DEBUG` |
| `LOGGING_LEVEL_ORG_TRACCAR` | Traccar logging level | `INFO` | `DEBUG` |
| `LOGGING_FILE_PATH` | Log file path | `logs/event-service.log` | `/var/log/traccar/event-service.log` |
| `LOGGING_FILE_MAX_SIZE` | Maximum log file size | `10MB` | `100MB` |
| `LOGGING_FILE_MAX_HISTORY` | Maximum log file history | `10` | `30` |

### Metrics and Monitoring

| Variable | Description | Default | Example |
|----------|-------------|---------|--------|
| `METRICS_ENABLED` | Enable metrics collection | `true` | `false` |
| `METRICS_EXPORT_PROMETHEUS` | Enable Prometheus metrics endpoint | `true` | `false` |
| `METRICS_PROMETHEUS_PORT` | Prometheus metrics port | `9090` | `9091` |
| `HEALTH_ENABLED` | Enable health endpoints | `true` | `false` |
| `HEALTH_SHOW_DETAILS` | Show health check details | `always` | `never` |

## Configuration File

The Event Processing Service can be configured using a YAML configuration file. By default, the service looks for a file named `application.yml` in the current working directory or in the classpath.

Example configuration file:

```yaml
server:
  port: ${EVENT_SERVICE_PORT:8090}
  address: ${EVENT_SERVICE_HOST:0.0.0.0}
  servlet:
    context-path: ${EVENT_SERVICE_CONTEXT_PATH:/api/events}
  tomcat:
    threads:
      max: ${EVENT_SERVICE_THREADS:10}
    max-connections: ${EVENT_SERVICE_MAX_CONNECTIONS:100}
    connection-timeout: 5000

event:
  overspeed:
    minimal-duration: ${EVENT_OVERSPEED_MINIMAL_DURATION:10}
    prefer-lowest: ${EVENT_OVERSPEED_PREFER_LOWEST:true}
    threshold-multiplier: ${EVENT_OVERSPEED_THRESHOLD_MULTIPLIER:1.0}
    limit: ${EVENT_OVERSPEED_LIMIT:0}
  motion:
    speed-threshold: ${EVENT_MOTION_SPEED_THRESHOLD:0.01}
    stationary-period: ${EVENT_MOTION_STATIONARY_PERIOD:180}
  geofence:
    optimization: ${EVENT_GEOFENCE_OPTIMIZATION:true}
    limit: ${EVENT_GEOFENCE_LIMIT:100}
  duplicate-alert:
    ignore: ${EVENT_IGNORE_DUPLICATE_ALERTS:true}
    window: ${EVENT_DUPLICATE_ALERT_WINDOW:300}

spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      group-id: ${KAFKA_GROUP_ID:event-service}
      auto-offset-reset: ${KAFKA_AUTO_OFFSET_RESET:latest}
      max-poll-records: ${KAFKA_MAX_POLL_RECORDS:500}
      max-poll-interval-ms: ${KAFKA_MAX_POLL_INTERVAL_MS:300000}
      enable-auto-commit: ${KAFKA_ENABLE_AUTO_COMMIT:false}
    topics:
      position: ${KAFKA_POSITION_TOPIC:enriched-positions}
      event: ${KAFKA_EVENT_TOPIC:events}

  datasource:
    url: ${DATABASE_URL:jdbc:h2:./database}
    username: ${DATABASE_USERNAME:sa}
    password: ${DATABASE_PASSWORD:}
    hikari:
      maximum-pool-size: ${DATABASE_MAX_POOL_SIZE:10}
      minimum-idle: ${DATABASE_MIN_IDLE:2}
      max-lifetime: ${DATABASE_MAX_LIFETIME:30000}

resilience4j:
  circuitbreaker:
    enabled: ${CIRCUIT_BREAKER_ENABLED:true}
    configs:
      default:
        failure-rate-threshold: ${CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD:50}
        wait-duration-in-open-state: ${CIRCUIT_BREAKER_WAIT_DURATION_OPEN_STATE:60s}
  retry:
    enabled: ${RETRY_ENABLED:true}
    configs:
      default:
        max-attempts: ${RETRY_MAX_ATTEMPTS:3}
        exponential-backoff-multiplier: ${RETRY_BACKOFF_MULTIPLIER:2}

logging:
  level:
    root: ${LOGGING_LEVEL_ROOT:INFO}
    org.traccar: ${LOGGING_LEVEL_ORG_TRACCAR:INFO}
  file:
    path: ${LOGGING_FILE_PATH:logs/event-service.log}
    max-size: ${LOGGING_FILE_MAX_SIZE:10MB}
    max-history: ${LOGGING_FILE_MAX_HISTORY:10}

management:
  metrics:
    export:
      prometheus:
        enabled: ${METRICS_EXPORT_PROMETHEUS:true}
  server:
    port: ${METRICS_PROMETHEUS_PORT:9090}
  endpoints:
    web:
      exposure:
        include: health,info,prometheus
  health:
    show-details: ${HEALTH_SHOW_DETAILS:always}
```

## Common Configuration Scenarios

### Development Environment

```yaml
server:
  port: 8090

spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      auto-offset-reset: earliest

logging:
  level:
    org.traccar: DEBUG
```

### Production Environment

```yaml
server:
  port: 8090
  tomcat:
    threads:
      max: 50
    max-connections: 500

spring:
  kafka:
    bootstrap-servers: kafka-1:9092,kafka-2:9092,kafka-3:9092
    consumer:
      group-id: event-service-prod

  datasource:
    url: jdbc:postgresql://postgres:5432/traccar
    username: traccar
    password: ${DATABASE_PASSWORD}
    hikari:
      maximum-pool-size: 20

logging:
  file:
    path: /var/log/traccar/event-service.log
    max-size: 100MB
    max-history: 30
```

### High-Performance Configuration

```yaml
server:
  port: 8090
  tomcat:
    threads:
      max: 100
    max-connections: 1000

event:
  geofence:
    optimization: true
    limit: 1000

spring:
  kafka:
    consumer:
      max-poll-records: 1000

  datasource:
    hikari:
      maximum-pool-size: 50
```

### Kubernetes Environment

```yaml
server:
  port: 8090

spring:
  kafka:
    bootstrap-servers: ${KAFKA_SERVICE_HOST}:${KAFKA_SERVICE_PORT}

service-discovery:
  type: kubernetes
  kubernetes:
    namespace: traccar
    enabled: true
```

## Performance Tuning

### Memory Allocation

The Event Processing Service memory allocation can be tuned using JVM parameters:

```
JAVA_OPTS="-Xms512m -Xmx1g -XX:MetaspaceSize=96M -XX:MaxMetaspaceSize=256m"
```

### Thread Pool Sizing

For optimal performance, configure the thread pool size based on the number of available CPU cores:

```yaml
server:
  tomcat:
    threads:
      max: ${CPU_CORES * 8}
```

### Kafka Consumer Tuning

Tune Kafka consumer settings based on message volume and processing requirements:

```yaml
spring:
  kafka:
    consumer:
      fetch-max-wait-ms: 500
      fetch-min-size: 1
      max-poll-records: 1000
      max-partition-fetch-bytes: 1048576
```

### Database Connection Pool

Optimize database connection pool for your workload:

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
      idle-timeout: 30000
      max-lifetime: 1800000
      connection-timeout: 30000
```

## Integration with Other Services

### Position Service Integration

The Event Processing Service consumes enriched position data from the Position Service via Kafka:

```yaml
spring:
  kafka:
    topics:
      position: enriched-positions
```

### Notification Service Integration

The Event Processing Service publishes detected events to be consumed by the Notification Service:

```yaml
spring:
  kafka:
    topics:
      event: events
```

### Service Discovery Integration

Configure service discovery for dynamic service location:

```yaml
service-discovery:
  type: consul
  consul:
    host: consul
    port: 8500
    register: true
    deregister-on-shutdown: true
```

## Logging and Monitoring

### Structured Logging

Configure structured JSON logging for better log analysis:

```yaml
logging:
  pattern:
    console: '{"timestamp":"%d{yyyy-MM-dd HH:mm:ss.SSS}","level":"%p","thread":"%t","class":"%c{1}","message":%m}%n'
    file: '{"timestamp":"%d{yyyy-MM-dd HH:mm:ss.SSS}","level":"%p","thread":"%t","class":"%c{1}","message":%m}%n'
```

### Prometheus Metrics

Enable and configure Prometheus metrics endpoint:

```yaml
management:
  metrics:
    export:
      prometheus:
        enabled: true
  endpoints:
    web:
      exposure:
        include: health,info,prometheus
  server:
    port: 9090
```

### Health Checks

Configure health check endpoints for monitoring service health:

```yaml
management:
  health:
    diskspace:
      enabled: true
    db:
      enabled: true
    kafka:
      enabled: true
    show-details: always
```

## Security Configuration

### TLS Configuration

Configure TLS for secure communication:

```yaml
server:
  ssl:
    enabled: true
    key-store: classpath:keystore.p12
    key-store-password: ${KEYSTORE_PASSWORD}
    key-store-type: PKCS12
    key-alias: event-service
```

### Authentication

Configure authentication for service-to-service communication:

```yaml
security:
  enabled: true
  service-auth:
    enabled: true
    token-header: X-Service-Auth
    token-value: ${SERVICE_AUTH_TOKEN}
```

## Container Environment Variables

When running the Event Processing Service in a container, you can configure it using environment variables. Here's an example Docker run command with environment variables:

```bash
docker run -d \
  --name event-service \
  -p 8090:8090 \
  -p 9090:9090 \
  -e EVENT_SERVICE_PORT=8090 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e KAFKA_GROUP_ID=event-service \
  -e DATABASE_URL=jdbc:postgresql://postgres:5432/traccar \
  -e DATABASE_USERNAME=traccar \
  -e DATABASE_PASSWORD=password \
  -e LOGGING_FILE_PATH=/var/log/traccar/event-service.log \
  -v /var/log/traccar:/var/log/traccar \
  traccar/event-service:latest
```

## Kubernetes ConfigMap Example

When deploying to Kubernetes, you can use a ConfigMap to manage configuration:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: event-service-config
data:
  application.yml: |
    server:
      port: 8090
    
    event:
      overspeed:
        minimal-duration: 10
        prefer-lowest: true
        threshold-multiplier: 1.0
      motion:
        speed-threshold: 0.01
        stationary-period: 180
      geofence:
        optimization: true
        limit: 100
    
    spring:
      kafka:
        bootstrap-servers: ${KAFKA_SERVICE_HOST}:${KAFKA_SERVICE_PORT}
        consumer:
          group-id: event-service
      
      datasource:
        url: jdbc:postgresql://postgres:5432/traccar
        username: ${DATABASE_USERNAME}
        password: ${DATABASE_PASSWORD}
    
    logging:
      level:
        org.traccar: INFO
      file:
        path: /var/log/traccar/event-service.log
```

## Troubleshooting

### Common Issues

1. **Kafka Connection Issues**
   - Check Kafka bootstrap servers configuration
   - Verify network connectivity to Kafka brokers
   - Check consumer group ID is unique

2. **Database Connection Issues**
   - Verify database URL, username, and password
   - Check database server is running and accessible
   - Verify connection pool settings

3. **Memory Issues**
   - Increase JVM heap size using `-Xmx` parameter
   - Monitor memory usage with metrics
   - Check for memory leaks

4. **Performance Issues**
   - Tune thread pool size
   - Optimize Kafka consumer settings
   - Adjust database connection pool
   - Enable geofence optimization

### Diagnostic Commands

Check service health:
```bash
curl http://localhost:8090/actuator/health
```

Check service metrics:
```bash
curl http://localhost:9090/actuator/prometheus
```

Check service info:
```bash
curl http://localhost:8090/actuator/info
```

## Conclusion

This configuration guide provides comprehensive information for setting up and tuning the Event Processing Service for different deployment scenarios. For additional assistance, refer to the Traccar documentation or contact support.