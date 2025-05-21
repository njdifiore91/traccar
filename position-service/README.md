# Position Processing Service

## Overview

The Position Processing Service is a core component of the Traccar GPS tracking platform's microservices architecture. This service is responsible for processing, validating, and enriching raw GPS position data received from tracking devices via the Protocol Service. It implements a comprehensive pipeline of position handlers that sequentially process position data to add context, calculate metrics, and prepare it for downstream event processing.

## Key Responsibilities

- Consuming raw position messages from the message broker
- Processing and validating position data through a handler pipeline
- Filtering invalid or redundant positions
- Enriching positions with additional context (geocoding, geofencing, etc.)
- Calculating trip metrics (distance, speed, motion status)
- Detecting geofence interactions
- Performing reverse geocoding to convert coordinates to addresses
- Publishing enriched positions to the message broker for downstream services
- Storing processed positions in the database

## Architecture

The Position Processing Service follows a stateless microservice design pattern, allowing for horizontal scaling to handle varying loads. It integrates with other services primarily through asynchronous messaging via a message broker (Kafka/RabbitMQ).

### System Context

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │     │                 │
│ Protocol Service│────▶│Position Service │────▶│  Event Service  │────▶│Notification Svc │
│                 │     │                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
         │                       │                       │                       │
         │                       │                       │                       │
         ▼                       ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    Message Broker                                        │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Position Processing Pipeline

The service implements a sequential handler chain pattern for position processing:

```
Raw Position → Validation → Filtering → Distance Calculation → Motion Detection → 
Geofence Checking → Reverse Geocoding → Speed Limit Validation → Attribute Enrichment → 
Enriched Position
```

Each handler in the pipeline performs a specific function and can be configured or disabled as needed.

## Technologies

- Java 17
- Spring Boot
- Kafka/RabbitMQ for message consumption and production
- Spatial libraries for geofence calculations
- Asynchronous geocoding clients
- OpenTelemetry for distributed tracing and metrics
- Docker and Kubernetes for containerization and orchestration
- HikariCP for database connection pooling

## Setup for Local Development

### Prerequisites

- Java 17 or higher
- Maven 3.8+
- Docker and Docker Compose
- Git

### Clone the Repository

```bash
git clone https://github.com/traccar/traccar-microservices.git
cd traccar-microservices/position-service
```

### Build the Service

```bash
mvn clean package
```

### Run with Docker Compose

A Docker Compose file is provided to run the service with its dependencies:

```bash
docker-compose up -d
```

This will start the Position Processing Service along with Kafka/RabbitMQ and other required services.

### Run Locally for Development

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=dev
```

## Configuration

The service can be configured through application properties or environment variables.

### Core Configuration Properties

| Property | Description | Default |
|----------|-------------|--------|
| `position.processing.enabled` | Enable/disable position processing | `true` |
| `position.handlers.geocoder.enabled` | Enable/disable geocoding | `true` |
| `position.handlers.geofence.enabled` | Enable/disable geofence checking | `true` |
| `position.handlers.distance.enabled` | Enable/disable distance calculation | `true` |
| `position.handlers.motion.enabled` | Enable/disable motion detection | `true` |
| `position.handlers.speedlimit.enabled` | Enable/disable speed limit validation | `true` |
| `position.filter.invalid` | Filter positions with invalid coordinates | `true` |
| `position.filter.accuracy` | Maximum position accuracy in meters (0 to disable) | `0` |
| `position.filter.duplicate` | Filter duplicate positions | `true` |
| `position.filter.outdated` | Maximum position age in seconds (0 to disable) | `0` |

### Message Broker Configuration

#### Kafka Configuration

```yaml
spring:
  kafka:
    bootstrap-servers: kafka:9092
    consumer:
      group-id: position-processors
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.traccar.kafka.PositionDeserializer
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.traccar.kafka.PositionSerializer
```

#### RabbitMQ Configuration

```yaml
spring:
  rabbitmq:
    host: rabbitmq
    port: 5672
    username: guest
    password: guest
    listener:
      simple:
        concurrency: 5
        max-concurrency: 10
```

## Message Broker Integration

The Position Processing Service integrates with the message broker in the following ways:

### Consuming Messages

- Subscribes to the `raw.positions` topic/queue from the Protocol Service
- Uses consumer group `position-processors` for load distribution
- Implements at-least-once delivery semantics with idempotent processing
- Configurable concurrency level per instance

### Publishing Messages

- Publishes processed positions to the `enriched.positions` topic/queue
- Uses the device ID as the message key for partitioning
- Implements the transactional outbox pattern for reliable publishing

## Deployment

### Docker Image

The service is packaged as a Docker container:

```bash
docker build -t traccar/position-service:latest .
```

### Kubernetes Deployment

A sample Kubernetes deployment configuration:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: position-service
spec:
  replicas: 2
  selector:
    matchLabels:
      app: position-service
  template:
    metadata:
      labels:
        app: position-service
    spec:
      containers:
      - name: position-service
        image: traccar/position-service:latest
        ports:
        - containerPort: 8080
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /actuator/health/liveness
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 15
        readinessProbe:
          httpGet:
            path: /actuator/health/readiness
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        env:
        - name: SPRING_PROFILES_ACTIVE
          value: "prod"
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka:9092"
```

## Scaling Considerations

The Position Processing Service is designed to be horizontally scalable:

- Stateless architecture allows adding more instances as load increases
- Consumer groups distribute message processing across instances
- Partition assignment by device ID ensures ordering guarantees
- Configurable concurrency within each instance
- Resource limits should be set based on expected throughput

Typical scaling metrics:
- 1 CPU core can handle approximately 5,000 positions per second
- 2GB memory is recommended per instance
- Scale horizontally when CPU utilization exceeds 70%

## Health and Monitoring

The service exposes health and metrics endpoints:

- `/actuator/health` - Overall service health
- `/actuator/health/liveness` - Container liveness
- `/actuator/health/readiness` - Service readiness to handle requests
- `/actuator/metrics` - Prometheus-compatible metrics

Key metrics to monitor:
- `position.processing.rate` - Positions processed per second
- `position.processing.latency` - Processing time per position
- `position.filter.rejected` - Number of positions rejected by filters
- `kafka.consumer.lag` - Consumer lag behind producer

## API Documentation

The service exposes a REST API for direct interaction:

- `/api/v1/positions` - CRUD operations for positions
- `/api/v1/positions/latest/{deviceId}` - Get latest position for a device
- `/api/v1/positions/process` - Manually submit a position for processing

API documentation is available at `/swagger-ui.html` when running the service.

## Contributing

Contributions to the Position Processing Service are welcome. Please follow the standard Traccar contribution guidelines:

1. Fork the repository
2. Create a feature branch
3. Implement your changes
4. Add tests for your changes
5. Submit a pull request

## License

The Position Processing Service, like all Traccar components, is licensed under the Apache License 2.0.