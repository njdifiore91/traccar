# Protocol Service Configuration Guide

> Comprehensive guide to configuring the Protocol Service, including environment variables, configuration files, protocol-specific settings, and runtime options.

## Table of Contents

- [Overview](#overview)
- [Configuration Methods](#configuration-methods)
  - [Configuration Files](#configuration-files)
  - [Environment Variables](#environment-variables)
  - [Kubernetes ConfigMaps and Secrets](#kubernetes-configmaps-and-secrets)
- [General Configuration Parameters](#general-configuration-parameters)
  - [Service Configuration](#service-configuration)
  - [Network Configuration](#network-configuration)
  - [Logging Configuration](#logging-configuration)
  - [Health and Monitoring](#health-and-monitoring)
- [Protocol-Specific Configuration](#protocol-specific-configuration)
  - [Common Protocol Parameters](#common-protocol-parameters)
  - [Protocol-Specific Parameters](#protocol-specific-parameters)
- [Message Broker Configuration](#message-broker-configuration)
- [Service Discovery Configuration](#service-discovery-configuration)
- [Examples](#examples)
  - [Basic Configuration](#basic-configuration)
  - [Multiple Protocol Configuration](#multiple-protocol-configuration)
  - [Kubernetes Deployment](#kubernetes-deployment)
- [Reference](#reference)

## Overview

The Protocol Service is responsible for handling device connections and protocol implementations, decoding raw messages from 200+ supported device protocols into a standardized format. As part of Traccar's microservices architecture, the Protocol Service acts as the entry point for all device data, with horizontal scalability for supporting thousands of concurrent connections.

This document provides a comprehensive guide to configuring the Protocol Service, including all available configuration parameters, environment variable mappings, and integration with Kubernetes.

## Configuration Methods

The Protocol Service supports multiple configuration methods that can be used individually or in combination.

### Configuration Files

The Protocol Service uses XML configuration files by default. The primary configuration file is loaded at startup and specified using the `configFile` parameter:

```bash
java -jar protocol-service.jar configFile=/path/to/config.xml
```

The configuration file follows this XML structure:

```xml
<?xml version="1.0"?>
<properties>
    <entry key="protocol.port">5055</entry>
    <entry key="protocol.osmand.port">5055</entry>
    <entry key="protocol.teltonika.port">5027</entry>
    <!-- Additional configuration parameters -->
</properties>
```

### Environment Variables

All configuration parameters can be set using environment variables. The Protocol Service automatically maps configuration keys to environment variables by:

1. Converting dots (`.`) to underscores (`_`)
2. Converting camelCase to SNAKE_CASE (inserting an underscore before each capital letter)
3. Converting the entire string to uppercase

For example:
- `protocol.port` becomes `PROTOCOL_PORT`
- `protocol.teltonika.port` becomes `PROTOCOL_TELTONIKA_PORT`
- `server.timeout` becomes `SERVER_TIMEOUT`

To enable environment variable configuration, set:

```bash
CONFIG_USE_ENVIRONMENT_VARIABLES=true
```

or add to your configuration file:

```xml
<entry key="config.useEnvironmentVariables">true</entry>
```

### Kubernetes ConfigMaps and Secrets

When deploying the Protocol Service in Kubernetes, you can use ConfigMaps for configuration and Secrets for sensitive data.

**ConfigMap Example:**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: protocol-service-config
data:
  config.xml: |
    <?xml version="1.0"?>
    <properties>
      <entry key="protocol.port">5055</entry>
      <entry key="protocol.osmand.port">5055</entry>
      <entry key="protocol.teltonika.port">5027</entry>
      <!-- Additional configuration parameters -->
    </properties>
```

**Secret Example (for sensitive data):**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: protocol-service-secrets
type: Opaque
data:
  DATABASE_USER: dXNlcm5hbWU=  # base64 encoded username
  DATABASE_PASSWORD: cGFzc3dvcmQ=  # base64 encoded password
```

Mount these in your deployment:

```yaml
volumeMounts:
- name: config-volume
  mountPath: /opt/traccar/conf
```

## General Configuration Parameters

### Service Configuration

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `service.name` | `SERVICE_NAME` | Service name for registration | `protocol-service` |
| `service.version` | `SERVICE_VERSION` | Service version | `1.0.0` |
| `service.instance.id` | `SERVICE_INSTANCE_ID` | Unique instance identifier | Auto-generated |

### Network Configuration

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `server.address` | `SERVER_ADDRESS` | Network interface to bind to | All interfaces |
| `server.timeout` | `SERVER_TIMEOUT` | Connection timeout in seconds | `600` |
| `server.bossThreads` | `SERVER_BOSS_THREADS` | Number of Netty boss threads | `1` |
| `server.workerThreads` | `SERVER_WORKER_THREADS` | Number of Netty worker threads | CPU cores * 2 |
| `server.buffering.threshold` | `SERVER_BUFFERING_THRESHOLD` | Buffering threshold in milliseconds | `0` (disabled) |
| `server.instantAcknowledgement` | `SERVER_INSTANT_ACKNOWLEDGEMENT` | Send device responses immediately | `false` |

### Logging Configuration

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `logger.console` | `LOGGER_CONSOLE` | Output logs to console | `false` |
| `logger.file` | `LOGGER_FILE` | Log file path | `./logs/protocol-service.log` |
| `logger.level` | `LOGGER_LEVEL` | Logging level (off, severe, warning, info, config, fine, finer, finest, all) | `info` |
| `logger.rotate` | `LOGGER_ROTATE` | Create a new log file daily | `true` |
| `logger.rotate.interval` | `LOGGER_ROTATE_INTERVAL` | Log rotation interval (day, hour) | `day` |
| `logger.fullStackTraces` | `LOGGER_FULL_STACK_TRACES` | Print full exception traces | `false` |
| `logger.decodeTextData` | `LOGGER_DECODE_TEXT_DATA` | Log network data as text if printable | `true` |
| `logger.queries` | `LOGGER_QUERIES` | Log executed SQL queries | `false` |
| `logger.attributes` | `LOGGER_ATTRIBUTES` | Position attributes to log | `time,position,speed,course,accuracy,result` |

### Health and Monitoring

The Protocol Service exposes health and metrics endpoints for monitoring by orchestration systems like Kubernetes and monitoring tools like Prometheus.

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `health.port` | `HEALTH_PORT` | Health check HTTP port | `8090` |
| `health.path.liveness` | `HEALTH_PATH_LIVENESS` | Liveness probe path | `/health/liveness` |
| `health.path.readiness` | `HEALTH_PATH_READINESS` | Readiness probe path | `/health/readiness` |
| `health.path.metrics` | `HEALTH_PATH_METRICS` | Metrics endpoint path | `/health/metrics` |
| `metrics.enable` | `METRICS_ENABLE` | Enable Prometheus metrics | `true` |

The health endpoints return the following status codes:

- `200 OK`: Service is healthy
- `503 Service Unavailable`: Service is unhealthy

The metrics endpoint exposes Prometheus-compatible metrics including:

- `protocol_connections_active`: Number of active device connections
- `protocol_messages_received_total`: Total number of messages received
- `protocol_messages_processed_total`: Total number of messages successfully processed
- `protocol_messages_error_total`: Total number of message processing errors
- `protocol_message_processing_time_seconds`: Message processing time histogram

## Protocol-Specific Configuration

### Common Protocol Parameters

These parameters can be applied to any protocol by replacing `{protocol}` with the protocol name (e.g., `osmand`, `teltonika`).

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `protocol.{protocol}.enable` | `PROTOCOL_{PROTOCOL}_ENABLE` | Enable/disable specific protocol | `true` |
| `protocol.{protocol}.port` | `PROTOCOL_{PROTOCOL}_PORT` | Port number for the protocol | Protocol-specific |
| `protocol.{protocol}.address` | `PROTOCOL_{PROTOCOL}_ADDRESS` | Network interface for the protocol | All interfaces |
| `protocol.{protocol}.timeout` | `PROTOCOL_{PROTOCOL}_TIMEOUT` | Connection timeout in seconds | `600` |
| `protocol.{protocol}.ssl` | `PROTOCOL_{PROTOCOL}_SSL` | Enable SSL support | `false` |

### Protocol-Specific Parameters

#### General Protocol Options

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `protocol.{protocol}.devicePassword` | `PROTOCOL_{PROTOCOL}_DEVICE_PASSWORD` | Device password for commands | - |
| `protocol.{protocol}.extended` | `PROTOCOL_{PROTOCOL}_EXTENDED` | Enable extended functionality | `false` |
| `protocol.{protocol}.utf8` | `PROTOCOL_{PROTOCOL}_UTF8` | Decode string as UTF8 instead of ASCII | `false` |
| `protocol.{protocol}.can` | `PROTOCOL_{PROTOCOL}_CAN` | Enable CAN decoding | `false` |
| `protocol.{protocol}.ack` | `PROTOCOL_{PROTOCOL}_ACK` | Server acknowledgement required | `false` |
| `protocol.{protocol}.ignoreFixTime` | `PROTOCOL_{PROTOCOL}_IGNORE_FIX_TIME` | Ignore device reported fix time | `false` |
| `protocol.{protocol}.disableCommands` | `PROTOCOL_{PROTOCOL}_DISABLE_COMMANDS` | Disable commands for the protocol | `false` |
| `protocol.{protocol}.format` | `PROTOCOL_{PROTOCOL}_FORMAT` | Protocol format | Protocol-specific |

#### Polling Protocols

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `protocol.{protocol}.devices` | `PROTOCOL_{PROTOCOL}_DEVICES` | List of devices for polling protocols | - |
| `protocol.{protocol}.interval` | `PROTOCOL_{PROTOCOL}_INTERVAL` | Polling interval in seconds | `60` |

#### Protocol-Specific Options

**TK103 Protocol:**

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `protocol.tk103.decodeLow` | `PROTOCOL_TK103_DECODE_LOW` | Decode additional TK103 attributes | `false` |

**Atrack Protocol:**

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `protocol.atrack.longDate` | `PROTOCOL_ATRACK_LONG_DATE` | Use long date format | `false` |
| `protocol.atrack.decimalFuel` | `PROTOCOL_ATRACK_DECIMAL_FUEL` | Use decimal fuel value format | `false` |
| `protocol.atrack.custom` | `PROTOCOL_ATRACK_CUSTOM` | Additional custom attributes | `false` |
| `protocol.atrack.form` | `PROTOCOL_ATRACK_FORM` | Custom format string | - |
| `protocol.atrack.alarmMap` | `PROTOCOL_ATRACK_ALARM_MAP` | Alarm mapping | - |

**Suntech Protocol:**

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `suntech.protocolType` | `SUNTECH_PROTOCOL_TYPE` | Protocol type for Suntech | - |
| `suntech.hbm` | `SUNTECH_HBM` | Suntech HBM configuration value | - |
| `protocol.suntech.includeAdc` | `PROTOCOL_SUNTECH_INCLUDE_ADC` | Format includes ADC value | `false` |
| `protocol.suntech.includeRpm` | `PROTOCOL_SUNTECH_INCLUDE_RPM` | Format includes RPM value | `false` |
| `protocol.suntech.includeTemp` | `PROTOCOL_SUNTECH_INCLUDE_TEMP` | Format includes temperature values | `false` |

**TAIP Protocol:**

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `protocol.taip.prefix` | `PROTOCOL_TAIP_PREFIX` | Indicates whether TAIP protocol should have prefixes for messages | `false` |

## Message Broker Configuration

The Protocol Service publishes decoded position data to a message broker for consumption by other services. This asynchronous communication pattern is the primary integration backbone for the Traccar microservices architecture.

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `messaging.type` | `MESSAGING_TYPE` | Message broker type (kafka, rabbitmq) | `kafka` |
| `messaging.topic.positions` | `MESSAGING_TOPIC_POSITIONS` | Topic for position messages | `positions` |
| `messaging.topic.events` | `MESSAGING_TOPIC_EVENTS` | Topic for event messages | `events` |
| `messaging.topic.commands` | `MESSAGING_TOPIC_COMMANDS` | Topic for command messages | `commands` |

### Kafka Configuration

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `kafka.bootstrap.servers` | `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `localhost:9092` |
| `kafka.producer.acks` | `KAFKA_PRODUCER_ACKS` | Producer acknowledgement level | `all` |
| `kafka.producer.retries` | `KAFKA_PRODUCER_RETRIES` | Number of retries | `3` |
| `kafka.producer.batch.size` | `KAFKA_PRODUCER_BATCH_SIZE` | Producer batch size | `16384` |
| `kafka.producer.linger.ms` | `KAFKA_PRODUCER_LINGER_MS` | Producer linger time in ms | `1` |
| `kafka.producer.buffer.memory` | `KAFKA_PRODUCER_BUFFER_MEMORY` | Producer buffer memory | `33554432` |

### RabbitMQ Configuration

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `rabbitmq.host` | `RABBITMQ_HOST` | RabbitMQ host | `localhost` |
| `rabbitmq.port` | `RABBITMQ_PORT` | RabbitMQ port | `5672` |
| `rabbitmq.username` | `RABBITMQ_USERNAME` | RabbitMQ username | `guest` |
| `rabbitmq.password` | `RABBITMQ_PASSWORD` | RabbitMQ password | `guest` |
| `rabbitmq.virtual.host` | `RABBITMQ_VIRTUAL_HOST` | RabbitMQ virtual host | `/` |
| `rabbitmq.exchange` | `RABBITMQ_EXCHANGE` | RabbitMQ exchange | `traccar` |
| `rabbitmq.connection.timeout` | `RABBITMQ_CONNECTION_TIMEOUT` | Connection timeout in ms | `60000` |

## Service Discovery Configuration

The Protocol Service can register with service discovery systems for dynamic service location. Service discovery is implemented through integration with Consul or Kubernetes API, allowing services to locate and communicate with each other dynamically.

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `discovery.type` | `DISCOVERY_TYPE` | Service discovery type (consul, kubernetes) | `kubernetes` |

### Consul Configuration

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `consul.host` | `CONSUL_HOST` | Consul host | `localhost` |
| `consul.port` | `CONSUL_PORT` | Consul port | `8500` |
| `consul.check.interval` | `CONSUL_CHECK_INTERVAL` | Health check interval in seconds | `30` |
| `consul.check.timeout` | `CONSUL_CHECK_TIMEOUT` | Health check timeout in seconds | `5` |

### Kubernetes Configuration

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `kubernetes.namespace` | `KUBERNETES_NAMESPACE` | Kubernetes namespace | `default` |
| `kubernetes.labels` | `KUBERNETES_LABELS` | Labels for service selection | `app=protocol-service` |

## Examples

### Basic Configuration

**config.xml:**

```xml
<?xml version="1.0"?>
<properties>
    <!-- Service Configuration -->
    <entry key="service.name">protocol-service</entry>
    
    <!-- Network Configuration -->
    <entry key="server.timeout">600</entry>
    <entry key="server.workerThreads">16</entry>
    
    <!-- Protocol Configuration -->
    <entry key="protocol.osmand.port">5055</entry>
    <entry key="protocol.teltonika.port">5027</entry>
    <entry key="protocol.meitrack.port">5020</entry>
    
    <!-- Logging Configuration -->
    <entry key="logger.file">./logs/protocol-service.log</entry>
    <entry key="logger.level">info</entry>
    
    <!-- Message Broker Configuration -->
    <entry key="messaging.type">kafka</entry>
    <entry key="kafka.bootstrap.servers">kafka:9092</entry>
</properties>
```

### Multiple Protocol Configuration

**config.xml:**

```xml
<?xml version="1.0"?>
<properties>
    <!-- Service Configuration -->
    <entry key="service.name">protocol-service</entry>
    
    <!-- Network Configuration -->
    <entry key="server.timeout">600</entry>
    
    <!-- Protocol Configuration -->
    <!-- OsmAnd Protocol -->
    <entry key="protocol.osmand.port">5055</entry>
    <entry key="protocol.osmand.extended">true</entry>
    
    <!-- Teltonika Protocol -->
    <entry key="protocol.teltonika.port">5027</entry>
    <entry key="protocol.teltonika.timeout">300</entry>
    
    <!-- TK103 Protocol -->
    <entry key="protocol.tk103.port">5006</entry>
    <entry key="protocol.tk103.decodeLow">true</entry>
    
    <!-- Suntech Protocol -->
    <entry key="protocol.suntech.port">5012</entry>
    <entry key="suntech.protocolType">1</entry>
    <entry key="protocol.suntech.includeAdc">true</entry>
    <entry key="protocol.suntech.includeTemp">true</entry>
    
    <!-- Message Broker Configuration -->
    <entry key="messaging.type">kafka</entry>
    <entry key="kafka.bootstrap.servers">kafka:9092</entry>
</properties>
```

### Kubernetes Deployment

**ConfigMap:**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: protocol-service-config
data:
  config.xml: |
    <?xml version="1.0"?>
    <properties>
      <entry key="service.name">protocol-service</entry>
      <entry key="config.useEnvironmentVariables">true</entry>
      <entry key="protocol.osmand.port">5055</entry>
      <entry key="protocol.teltonika.port">5027</entry>
      <entry key="protocol.meitrack.port">5020</entry>
      <entry key="logger.file">/var/log/traccar/protocol-service.log</entry>
      <entry key="discovery.type">kubernetes</entry>
    </properties>
```

**Secret:**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: protocol-service-secrets
type: Opaque
data:
  KAFKA_USERNAME: a2Fma2FVc2Vy  # base64 encoded
  KAFKA_PASSWORD: a2Fma2FQYXNzd29yZA==  # base64 encoded
```

**Deployment:**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: protocol-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: protocol-service
  template:
    metadata:
      labels:
        app: protocol-service
    spec:
      containers:
      - name: protocol-service
        image: traccar/protocol-service:latest
        ports:
        - containerPort: 5055
          name: osmand
        - containerPort: 5027
          name: teltonika
        - containerPort: 5020
          name: meitrack
        - containerPort: 8090
          name: health
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-service:9092"
        - name: MESSAGING_TYPE
          value: "kafka"
        - name: KAFKA_USERNAME
          valueFrom:
            secretKeyRef:
              name: protocol-service-secrets
              key: KAFKA_USERNAME
        - name: KAFKA_PASSWORD
          valueFrom:
            secretKeyRef:
              name: protocol-service-secrets
              key: KAFKA_PASSWORD
        volumeMounts:
        - name: config-volume
          mountPath: /opt/traccar/conf
        - name: logs-volume
          mountPath: /var/log/traccar
        resources:
          requests:
            cpu: 500m
            memory: 512Mi
          limits:
            cpu: 1000m
            memory: 1Gi
        livenessProbe:
          httpGet:
            path: /health/liveness
            port: health
          initialDelaySeconds: 60
          periodSeconds: 15
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /health/readiness
            port: health
          initialDelaySeconds: 30
          periodSeconds: 10
          successThreshold: 1
          failureThreshold: 3
      volumes:
      - name: config-volume
        configMap:
          name: protocol-service-config
      - name: logs-volume
        emptyDir: {}
```

**Service:**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: protocol-service
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: nlb
    prometheus.io/scrape: "true"
    prometheus.io/port: "8090"
    prometheus.io/path: "/health/metrics"
spec:
  selector:
    app: protocol-service
  ports:
  - name: osmand
    port: 5055
    protocol: TCP
  - name: teltonika
    port: 5027
    protocol: TCP
  - name: meitrack
    port: 5020
    protocol: TCP
  - name: health
    port: 8090
    protocol: TCP
  type: LoadBalancer
```

**Horizontal Pod Autoscaler:**

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: protocol-service-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: protocol-service
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
        name: protocol_connections_active
      target:
        type: AverageValue
        averageValue: 800
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 300
```

## Resilience Configuration

The Protocol Service implements resilience patterns to handle failures gracefully.

| Parameter | Environment Variable | Description | Default |
|-----------|----------------------|-------------|--------|
| `resilience.circuitBreaker.enabled` | `RESILIENCE_CIRCUIT_BREAKER_ENABLED` | Enable circuit breaker pattern | `true` |
| `resilience.circuitBreaker.failureRateThreshold` | `RESILIENCE_CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD` | Failure rate threshold percentage | `50` |
| `resilience.circuitBreaker.waitDurationInOpenState` | `RESILIENCE_CIRCUIT_BREAKER_WAIT_DURATION_IN_OPEN_STATE` | Wait time in open state (ms) | `10000` |
| `resilience.retry.enabled` | `RESILIENCE_RETRY_ENABLED` | Enable retry mechanism | `true` |
| `resilience.retry.maxAttempts` | `RESILIENCE_RETRY_MAX_ATTEMPTS` | Maximum retry attempts | `3` |
| `resilience.retry.waitDuration` | `RESILIENCE_RETRY_WAIT_DURATION` | Wait duration between retries (ms) | `1000` |

## Reference

For more information about specific protocols and their configuration options, refer to the following resources:

- [Traccar Protocol Documentation](https://www.traccar.org/devices/)
- [Protocol Service API Documentation](../api/README.md)
- [Kubernetes ConfigMaps and Secrets](https://kubernetes.io/docs/concepts/configuration/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [RabbitMQ Documentation](https://www.rabbitmq.com/documentation.html)
- [Resilience4j Documentation](https://resilience4j.readme.io/docs)