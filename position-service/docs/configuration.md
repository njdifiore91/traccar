# Position Processing Service Configuration

## Overview

This document provides comprehensive configuration information for the Position Processing Service, which is responsible for processing, validating, and enriching position data from GPS devices. The service consumes raw position messages from the message broker, applies various processing handlers, and publishes enriched positions for downstream services.

## Configuration Methods

The Position Processing Service supports multiple configuration methods, with the following precedence (highest to lowest):

1. Environment variables
2. Configuration files
3. Default values

### Environment Variables

Environment variables take precedence over configuration files. The service automatically converts configuration keys to environment variable names using the following pattern:

- Replace dots (`.`) with underscores (`_`)
- Convert to uppercase
- Add uppercase letters with an underscore prefix

For example, `position.filter.distance` becomes `POSITION_FILTER_DISTANCE`.

To enable environment variable configuration, set:

```
CONFIG_USE_ENVIRONMENT_VARIABLES=true
```

### Configuration Files

The service uses the following configuration files:

- `config.yml` - Main configuration file
- `config-{env}.yml` - Environment-specific overrides (dev, test, prod)
- `application.yml` - Spring Boot application properties
- `application-kafka.yml` - Kafka-specific configuration
- `application-resilience4j.yml` - Resilience4j configuration

In Kubernetes deployments, these files are typically mounted from ConfigMaps.

## Core Configuration Parameters

### Service Configuration

| Parameter | Description | Default Value | Environment Variable |
|-----------|-------------|---------------|----------------------|
| `service.name` | Service identifier | `position-service` | `SERVICE_NAME` |
| `service.version` | Service version | `1.0.0` | `SERVICE_VERSION` |
| `service.port` | HTTP port for API and health endpoints | `8080` | `SERVICE_PORT` |
| `service.grpc.port` | gRPC port for service-to-service communication | `9090` | `SERVICE_GRPC_PORT` |

### Message Broker Configuration

#### Kafka Configuration

| Parameter | Description | Default Value | Environment Variable |
|-----------|-------------|---------------|----------------------|
| `kafka.bootstrap-servers` | Comma-separated list of Kafka brokers | `kafka:9092` | `KAFKA_BOOTSTRAP_SERVERS` |
| `kafka.consumer.group-id` | Consumer group identifier | `position-processors` | `KAFKA_CONSUMER_GROUP_ID` |
| `kafka.consumer.auto-offset-reset` | Offset reset policy | `earliest` | `KAFKA_CONSUMER_AUTO_OFFSET_RESET` |
| `kafka.consumer.enable-auto-commit` | Enable automatic offset commits | `false` | `KAFKA_CONSUMER_ENABLE_AUTO_COMMIT` |
| `kafka.consumer.max-poll-records` | Maximum records per poll | `500` | `KAFKA_CONSUMER_MAX_POLL_RECORDS` |
| `kafka.consumer.concurrency` | Number of consumer threads | `10` | `KAFKA_CONSUMER_CONCURRENCY` |
| `kafka.topics.raw-positions` | Topic for consuming raw positions | `raw.positions` | `KAFKA_TOPICS_RAW_POSITIONS` |
| `kafka.topics.enriched-positions` | Topic for publishing enriched positions | `enriched.positions` | `KAFKA_TOPICS_ENRICHED_POSITIONS` |

#### RabbitMQ Configuration

| Parameter | Description | Default Value | Environment Variable |
|-----------|-------------|---------------|----------------------|
| `rabbitmq.host` | RabbitMQ host | `rabbitmq` | `RABBITMQ_HOST` |
| `rabbitmq.port` | RabbitMQ port | `5672` | `RABBITMQ_PORT` |
| `rabbitmq.username` | RabbitMQ username | `guest` | `RABBITMQ_USERNAME` |
| `rabbitmq.password` | RabbitMQ password | `guest` | `RABBITMQ_PASSWORD` |
| `rabbitmq.virtual-host` | RabbitMQ virtual host | `/` | `RABBITMQ_VIRTUAL_HOST` |
| `rabbitmq.queues.raw-positions` | Queue for consuming raw positions | `raw.positions` | `RABBITMQ_QUEUES_RAW_POSITIONS` |
| `rabbitmq.queues.enriched-positions` | Queue for publishing enriched positions | `enriched.positions` | `RABBITMQ_QUEUES_ENRICHED_POSITIONS` |
| `rabbitmq.exchange` | Exchange for position messages | `traccar` | `RABBITMQ_EXCHANGE` |

### Database Configuration

| Parameter | Description | Default Value | Environment Variable |
|-----------|-------------|---------------|----------------------|
| `database.url` | JDBC connection URL | `jdbc:postgresql://postgres:5432/traccar` | `DATABASE_URL` |
| `database.username` | Database username | `traccar` | `DATABASE_USERNAME` |
| `database.password` | Database password | `traccar` | `DATABASE_PASSWORD` |
| `database.max-pool-size` | Connection pool size | `20` | `DATABASE_MAX_POOL_SIZE` |
| `database.check-connection` | SQL query to check connection | `SELECT 1` | `DATABASE_CHECK_CONNECTION` |

## Position Processing Configuration

### Position Filtering

| Parameter | Description | Default Value | Environment Variable |
|-----------|-------------|---------------|----------------------|
| `filter.enable` | Enable position filtering | `true` | `FILTER_ENABLE` |
| `filter.invalid` | Filter invalid positions | `false` | `FILTER_INVALID` |
| `filter.zero` | Filter zero coordinates | `false` | `FILTER_ZERO` |
| `filter.duplicate` | Filter duplicate positions | `false` | `FILTER_DUPLICATE` |
| `filter.outdated` | Filter positions without GPS location | `false` | `FILTER_OUTDATED` |
| `filter.future` | Filter positions with future timestamps (seconds) | `86400` | `FILTER_FUTURE` |
| `filter.past` | Filter positions with past timestamps (seconds) | `0` | `FILTER_PAST` |
| `filter.accuracy` | Filter positions with accuracy less than specified (meters) | `0` | `FILTER_ACCURACY` |
| `filter.approximate` | Filter cell and wifi locations | `false` | `FILTER_APPROXIMATE` |
| `filter.static` | Filter positions with zero speed | `false` | `FILTER_STATIC` |
| `filter.distance` | Filter positions by distance (meters) | `0` | `FILTER_DISTANCE` |
| `filter.max-speed` | Filter positions by maximum speed (knots) | `0` | `FILTER_MAX_SPEED` |
| `filter.min-period` | Filter positions by minimum time period (seconds) | `0` | `FILTER_MIN_PERIOD` |
| `filter.daily-limit` | Daily position limit per device | `0` | `FILTER_DAILY_LIMIT` |
| `filter.daily-limit-interval` | Throttling interval when limit exceeded (seconds) | `0` | `FILTER_DAILY_LIMIT_INTERVAL` |
| `filter.relative` | Check filters against preceding position | `false` | `FILTER_RELATIVE` |
| `filter.skip-limit` | Time limit for filtering (seconds) | `0` | `FILTER_SKIP_LIMIT` |
| `filter.skip-attributes.enable` | Enable attribute skipping | `false` | `FILTER_SKIP_ATTRIBUTES_ENABLE` |
| `filter.skip-attributes` | Comma-separated list of attributes to skip filtering | `""` | `FILTER_SKIP_ATTRIBUTES` |

### Geocoding Configuration

| Parameter | Description | Default Value | Environment Variable |
|-----------|-------------|---------------|----------------------|
| `geocoder.enable` | Enable reverse geocoding | `true` | `GEOCODER_ENABLE` |
| `geocoder.type` | Geocoder provider type | `locationiq` | `GEOCODER_TYPE` |
| `geocoder.url` | Geocoder server URL | | `GEOCODER_URL` |
| `geocoder.key` | Provider API key | `pk.689d849289c8c63708068b2ff1f63b2d` | `GEOCODER_KEY` |
| `geocoder.language` | Language parameter for localization | | `GEOCODER_LANGUAGE` |
| `geocoder.format` | Address format string | | `GEOCODER_FORMAT` |
| `geocoder.cache-size` | Cache size for geocoding results | `0` | `GEOCODER_CACHE_SIZE` |
| `geocoder.ignore-positions` | Disable automatic geocoding for all positions | `true` | `GEOCODER_IGNORE_POSITIONS` |
| `geocoder.process-invalid-positions` | Apply geocoding to invalid positions | `false` | `GEOCODER_PROCESS_INVALID_POSITIONS` |
| `geocoder.reuse-distance` | Minimum distance for new geocoding request (meters) | `0` | `GEOCODER_REUSE_DISTANCE` |
| `geocoder.on-request` | Perform geocoding for reports and notifications | `true` | `GEOCODER_ON_REQUEST` |

### Geolocation Configuration

| Parameter | Description | Default Value | Environment Variable |
|-----------|-------------|---------------|----------------------|
| `geolocation.enable` | Enable LBS location resolution | `false` | `GEOLOCATION_ENABLE` |
| `geolocation.type` | Provider for LBS location | | `GEOLOCATION_TYPE` |
| `geolocation.url` | Geolocation provider API URL | | `GEOLOCATION_URL` |
| `geolocation.key` | Provider API key | | `GEOLOCATION_KEY` |
| `geolocation.process-invalid-positions` | Apply geolocation to invalid positions | `false` | `GEOLOCATION_PROCESS_INVALID_POSITIONS` |
| `geolocation.reuse` | Reuse last result if network details unchanged | `false` | `GEOLOCATION_REUSE` |
| `geolocation.require-wifi` | Process geolocation only with Wi-Fi information | `false` | `GEOLOCATION_REQUIRE_WIFI` |
| `geolocation.mcc` | Default MCC value | | `GEOLOCATION_MCC` |
| `geolocation.mnc` | Default MNC value | | `GEOLOCATION_MNC` |

### Speed Limit Configuration

| Parameter | Description | Default Value | Environment Variable |
|-----------|-------------|---------------|----------------------|
| `speed-limit.enable` | Enable speed limit API | `false` | `SPEED_LIMIT_ENABLE` |
| `speed-limit.type` | Provider for speed limit | `overpass` | `SPEED_LIMIT_TYPE` |
| `speed-limit.url` | Speed limit provider API URL | | `SPEED_LIMIT_URL` |
| `speed-limit.accuracy` | Search radius for speed limit (meters) | `100` | `SPEED_LIMIT_ACCURACY` |

### Coordinates Processing

| Parameter | Description | Default Value | Environment Variable |
|-----------|-------------|---------------|----------------------|
| `coordinates.filter` | Enable coordinates filtering | `false` | `COORDINATES_FILTER` |
| `coordinates.min-error` | Minimum distance for coordinate changes (meters) | | `COORDINATES_MIN_ERROR` |
| `coordinates.max-error` | Maximum distance for coordinate changes (meters) | | `COORDINATES_MAX_ERROR` |
| `location.latitude-hemisphere` | Override latitude sign/hemisphere (N/S) | | `LOCATION_LATITUDE_HEMISPHERE` |
| `location.longitude-hemisphere` | Override longitude sign/hemisphere (E/W) | | `LOCATION_LONGITUDE_HEMISPHERE` |

### Processing Options

| Parameter | Description | Default Value | Environment Variable |
|-----------|-------------|---------------|----------------------|
| `processing.remote-address.enable` | Save device IP addresses | `false` | `PROCESSING_REMOTE_ADDRESS_ENABLE` |
| `processing.use-linked-driver` | Use linked driver ID for positions | `false` | `PROCESSING_USE_LINKED_DRIVER` |
| `processing.copy-attributes.enable` | Copy missing attributes from last position | `false` | `PROCESSING_COPY_ATTRIBUTES_ENABLE` |
| `processing.copy-attributes` | List of attributes to copy | | `PROCESSING_COPY_ATTRIBUTES` |
| `processing.computed-attributes.device-attributes` | Include device attributes in computed context | `false` | `PROCESSING_COMPUTED_ATTRIBUTES_DEVICE_ATTRIBUTES` |
| `processing.computed-attributes.last-attributes` | Include last position attributes in computed context | `false` | `PROCESSING_COMPUTED_ATTRIBUTES_LAST_ATTRIBUTES` |
| `processing.computed-attributes.local-variables` | Enable local variables declaration | `false` | `PROCESSING_COMPUTED_ATTRIBUTES_LOCAL_VARIABLES` |
| `processing.computed-attributes.loops` | Enable loops processing | `false` | `PROCESSING_COMPUTED_ATTRIBUTES_LOOPS` |
| `processing.computed-attributes.new-instance-creation` | Enable new instances creation | `false` | `PROCESSING_COMPUTED_ATTRIBUTES_NEW_INSTANCE_CREATION` |

## Resilience Configuration

### Circuit Breaker Configuration

| Parameter | Description | Default Value | Environment Variable |
|-----------|-------------|---------------|----------------------|
| `resilience4j.circuitbreaker.instances.geocodingService.slidingWindowType` | Window type for failure rate calculation | `COUNT_BASED` | `RESILIENCE4J_CIRCUITBREAKER_INSTANCES_GEOCODINGSERVICE_SLIDINGWINDOWTYPE` |
| `resilience4j.circuitbreaker.instances.geocodingService.slidingWindowSize` | Window size for failure rate calculation | `100` | `RESILIENCE4J_CIRCUITBREAKER_INSTANCES_GEOCODINGSERVICE_SLIDINGWINDOWSIZE` |
| `resilience4j.circuitbreaker.instances.geocodingService.failureRateThreshold` | Failure rate threshold percentage | `50` | `RESILIENCE4J_CIRCUITBREAKER_INSTANCES_GEOCODINGSERVICE_FAILURERATETHRESHOLD` |
| `resilience4j.circuitbreaker.instances.geocodingService.waitDurationInOpenState` | Wait time in open state (ms) | `10000` | `RESILIENCE4J_CIRCUITBREAKER_INSTANCES_GEOCODINGSERVICE_WAITDURATIONINOPENSTATE` |

### Retry Configuration

| Parameter | Description | Default Value | Environment Variable |
|-----------|-------------|---------------|----------------------|
| `resilience4j.retry.instances.geocodingService.maxAttempts` | Maximum retry attempts | `3` | `RESILIENCE4J_RETRY_INSTANCES_GEOCODINGSERVICE_MAXATTEMPTS` |
| `resilience4j.retry.instances.geocodingService.waitDuration` | Wait duration between retries (ms) | `1000` | `RESILIENCE4J_RETRY_INSTANCES_GEOCODINGSERVICE_WAITDURATION` |
| `resilience4j.retry.instances.geocodingService.enableExponentialBackoff` | Enable exponential backoff | `true` | `RESILIENCE4J_RETRY_INSTANCES_GEOCODINGSERVICE_ENABLEEXPONENTIALBACKOFF` |
| `resilience4j.retry.instances.geocodingService.exponentialBackoffMultiplier` | Backoff multiplier | `2` | `RESILIENCE4J_RETRY_INSTANCES_GEOCODINGSERVICE_EXPONENTIALBACKOFFMULTIPLIER` |

## Resource Allocation

The Position Processing Service has the following default resource allocations:

| Resource | Request | Limit |
|----------|---------|-------|
| CPU | 500m | 1500m |
| Memory | 1Gi | 2Gi |
| Disk | 2Gi | N/A |

These values can be adjusted in the Kubernetes deployment configuration based on workload requirements.

### Scaling Considerations

The Position Processing Service scales horizontally based on position message throughput. The recommended scaling formula is:

```
Required CPU = 0.5 CPU (base) + (1 CPU per 5,000 positions/sec × Expected positions/sec)
```

For example, with 10,000 positions per second:
- CPU: 0.5 CPU (base) + (1 CPU per 5,000 positions/sec × 10,000 positions/sec) = 2.5 CPU cores
- Memory: 1GB (base) + (2GB per 5,000 positions/sec × 10,000 positions/sec) = 5GB

## Health and Monitoring

The Position Processing Service exposes the following endpoints for health checking and monitoring:

| Endpoint | Description |
|----------|-------------|
| `/actuator/health/liveness` | Basic operational status |
| `/actuator/health/readiness` | Ability to handle requests |
| `/actuator/metrics` | Detailed performance metrics |

These endpoints are used by Kubernetes for liveness and readiness probes:

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

## Configuration Examples

### Basic Configuration

```yaml
service:
  name: position-service
  port: 8080
  grpc.port: 9090

kafka:
  bootstrap-servers: kafka:9092
  consumer:
    group-id: position-processors
    auto-offset-reset: earliest
    enable-auto-commit: false
    max-poll-records: 500
    concurrency: 10
  topics:
    raw-positions: raw.positions
    enriched-positions: enriched.positions

database:
  url: jdbc:postgresql://postgres:5432/traccar
  username: traccar
  password: traccar
  max-pool-size: 20

filter:
  enable: true
  invalid: true
  zero: true
  duplicate: true
  distance: 10
  max-speed: 240

geocoder:
  enable: true
  type: google
  key: YOUR_API_KEY
  cache-size: 10000
  reuse-distance: 50
```

### High-Performance Configuration

```yaml
kafka:
  consumer:
    max-poll-records: 1000
    concurrency: 20

database:
  max-pool-size: 50

filter:
  enable: true
  invalid: true
  zero: true
  duplicate: true
  distance: 5
  max-speed: 240
  min-period: 1

geocoder:
  enable: true
  type: google
  key: YOUR_API_KEY
  cache-size: 50000
  reuse-distance: 25
  ignore-positions: true
  on-request: true

processing:
  copy-attributes.enable: true
  copy-attributes: "alarm,ignition,motion"
```

### Low-Resource Configuration

```yaml
kafka:
  consumer:
    max-poll-records: 100
    concurrency: 5

database:
  max-pool-size: 10

filter:
  enable: true
  invalid: true
  zero: true
  duplicate: true
  distance: 20
  max-speed: 240
  min-period: 5

geocoder:
  enable: false

geolocation:
  enable: false
```

## Environment-Specific Configuration

### Development Environment

```yaml
# config-dev.yml
kafka:
  bootstrap-servers: localhost:9092

database:
  url: jdbc:postgresql://localhost:5432/traccar
  username: traccar
  password: traccar

geocoder:
  enable: false
```

### Production Environment

```yaml
# config-prod.yml
kafka:
  bootstrap-servers: kafka-0.kafka-headless.kafka.svc.cluster.local:9092,kafka-1.kafka-headless.kafka.svc.cluster.local:9092,kafka-2.kafka-headless.kafka.svc.cluster.local:9092

database:
  url: jdbc:postgresql://traccar-postgresql.database.svc.cluster.local:5432/traccar
  username: ${DATABASE_USERNAME}
  password: ${DATABASE_PASSWORD}
  max-pool-size: 50

filter:
  enable: true
  invalid: true
  zero: true
  duplicate: true
  distance: 5
  max-speed: 240

geocoder:
  enable: true
  type: google
  key: ${GEOCODER_API_KEY}
  cache-size: 50000
  reuse-distance: 25
```

## Kubernetes ConfigMap Example

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: position-service-config
data:
  config.yml: |
    service:
      name: position-service
      port: 8080
      grpc.port: 9090

    kafka:
      bootstrap-servers: kafka:9092
      consumer:
        group-id: position-processors
        auto-offset-reset: earliest
        enable-auto-commit: false
        max-poll-records: 500
        concurrency: 10
      topics:
        raw-positions: raw.positions
        enriched-positions: enriched.positions

    database:
      url: jdbc:postgresql://postgres:5432/traccar
      username: traccar
      password: traccar
      max-pool-size: 20

    filter:
      enable: true
      invalid: true
      zero: true
      duplicate: true
      distance: 10
      max-speed: 240

    geocoder:
      enable: true
      type: google
      key: YOUR_API_KEY
      cache-size: 10000
      reuse-distance: 50
```

## Troubleshooting

### Common Issues

1. **Service fails to start**
   - Check database connection parameters
   - Verify Kafka/RabbitMQ connection parameters
   - Ensure required environment variables are set

2. **Position messages not being processed**
   - Verify Kafka/RabbitMQ topics are correctly configured
   - Check consumer group configuration
   - Ensure message format is compatible

3. **High memory usage**
   - Reduce `kafka.consumer.max-poll-records`
   - Decrease `database.max-pool-size`
   - Adjust JVM heap settings

4. **Slow position processing**
   - Increase `kafka.consumer.concurrency`
   - Optimize filtering parameters
   - Consider disabling geocoding for all positions

### Logging Configuration

Adjust logging levels in `application.yml`:

```yaml
logging:
  level:
    root: INFO
    org.traccar: INFO
    org.traccar.processing: DEBUG
    org.traccar.handler: DEBUG
```

For more detailed logging during troubleshooting:

```yaml
logging:
  level:
    root: INFO
    org.traccar: DEBUG
    org.traccar.processing: TRACE
    org.traccar.handler: TRACE
    org.apache.kafka: INFO
```