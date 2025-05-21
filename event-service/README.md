# Event Processing Service

## Overview

The Event Processing Service is a core component of the Traccar microservices architecture responsible for analyzing position data to detect significant events such as geofence transitions, speed violations, device status changes, and maintenance alerts. This service consumes enriched position data from the message broker, applies complex event detection rules, and publishes detected events back to the message broker for further processing by other services.

## Key Responsibilities

- Consuming enriched position data from the message broker
- Analyzing position data using various event detection algorithms
- Detecting significant events based on configurable rules
- Publishing detected events to the message broker
- Persisting events to the database using the transactional outbox pattern
- Providing event query capabilities via gRPC endpoints

## Architecture

The Event Processing Service follows a stateless microservice architecture designed for horizontal scalability. It implements the following architectural patterns:

- **Event-Driven Architecture**: Consumes and produces events via a message broker
- **Stateless Processing**: Maintains no persistent state between requests for horizontal scaling
- **Circuit Breaker Pattern**: Prevents cascading failures when dependent services are degraded
- **Retry and Fallback Mechanisms**: Ensures reliable event processing with exponential backoff
- **Bulkhead Pattern**: Isolates failures through thread pool separation

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    Event Processing Service                      │
│                                                                 │
│  ┌───────────────┐    ┌───────────────┐    ┌───────────────┐    │
│  │ Message Broker│    │ Event Detector│    │ Event Publisher│   │
│  │   Consumer    │───▶│    Engine     │───▶│               │    │
│  └───────────────┘    └───────────────┘    └───────────────┘    │
│                              │                     │            │
│                              ▼                     ▼            │
│                      ┌───────────────┐    ┌───────────────┐    │
│                      │ Rule Evaluator│    │ Message Broker│    │
│                      │               │    │   Producer    │    │
│                      └───────────────┘    └───────────────┘    │
│                              │                                  │
│                              ▼                                  │
│                      ┌───────────────┐                          │
│                      │ Event Storage │                          │
│                      │               │                          │
│                      └───────────────┘                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. The service consumes enriched position data from the message broker (Kafka/RabbitMQ)
2. Position data is passed through the Event Detector Engine
3. The Rule Evaluator applies configured rules to detect events
4. Detected events are persisted to the database
5. Events are published to the event topic on the message broker
6. Other services (like the Notification Service) consume these events for further processing

## Event Types

The Event Processing Service detects various types of events, including:

- **Geofence Events**: Entry, exit, and dwell time within defined geographic boundaries
- **Speed Events**: Speeding, harsh acceleration, harsh braking
- **Device Status Events**: Motion start/stop, ignition on/off, power status changes
- **Maintenance Events**: Service due, engine hours thresholds
- **Driver Behavior Events**: Cornering, lane changes, idle time
- **Custom Events**: User-defined events based on device attributes and position data

## Setup and Configuration

### Prerequisites

- Java 17 or higher
- Docker and Docker Compose (for local development)
- Access to a Kafka or RabbitMQ instance
- Access to a PostgreSQL database

### Environment Variables

| Variable | Description | Default |
|----------|-------------|--------|
| `SPRING_PROFILES_ACTIVE` | Active Spring profiles | `dev` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` |
| `KAFKA_CONSUMER_GROUP_ID` | Consumer group for this service | `event-service` |
| `DATABASE_URL` | JDBC URL for database connection | `jdbc:postgresql://localhost:5432/traccar` |
| `DATABASE_USERNAME` | Database username | `postgres` |
| `DATABASE_PASSWORD` | Database password | `postgres` |
| `SERVICE_DISCOVERY_URI` | Service discovery endpoint | `http://localhost:8500` |

### Local Development

1. Clone the repository
   ```bash
   git clone https://github.com/traccar/traccar-event-service.git
   cd traccar-event-service
   ```

2. Start the required infrastructure using Docker Compose
   ```bash
   docker-compose -f docker-compose.dev.yml up -d
   ```

3. Build and run the service
   ```bash
   ./mvnw clean package
   java -jar target/event-service.jar
   ```

4. The service will be available at http://localhost:8082

### Configuration Files

The service uses the following configuration files:

- `application.yml`: Main application configuration
- `application-dev.yml`: Development environment overrides
- `application-prod.yml`: Production environment overrides

## API and Integration

### gRPC Endpoints

The Event Processing Service exposes the following gRPC endpoints:

- `EventService.getEvents`: Retrieve events by criteria
- `EventService.getEventById`: Retrieve a specific event by ID
- `EventService.getEventsByDeviceId`: Retrieve events for a specific device

### Message Broker Topics

The service interacts with the following message broker topics:

- Consumes from: `enriched-positions`
- Publishes to: `events`

## Deployment

### Kubernetes

The Event Processing Service can be deployed to Kubernetes using the provided Helm chart:

```bash
helm install event-service ./setup/helm/charts/event
```

The Helm chart includes:

- Deployment configuration with appropriate resource limits
- Service definition for gRPC endpoints
- ConfigMap for environment-specific settings
- Health check probes for liveness and readiness

### Docker

A Docker image is available on Docker Hub:

```bash
docker pull traccar/event-service:latest
docker run -p 8082:8082 -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 traccar/event-service:latest
```

## Monitoring and Observability

The Event Processing Service exposes the following endpoints for monitoring:

- `/actuator/health`: Health check endpoint
- `/actuator/metrics`: Prometheus-compatible metrics
- `/actuator/info`: Service information

Key metrics include:

- `event_processing_time`: Time taken to process each position
- `events_detected_count`: Number of events detected
- `rule_evaluation_time`: Time taken to evaluate rules
- `message_broker_publish_time`: Time taken to publish events

## Troubleshooting

### Common Issues

1. **Service fails to start**: Check database connectivity and message broker availability
2. **No events being detected**: Verify rule configurations and position data quality
3. **High processing latency**: Check resource allocation and rule complexity

### Logs

The service uses structured JSON logging with the following log levels:

- `ERROR`: Critical issues requiring immediate attention
- `WARN`: Potential issues that don't affect core functionality
- `INFO`: Normal operational information
- `DEBUG`: Detailed information for troubleshooting

## Contributing

Contributions to the Event Processing Service are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

Please ensure your code follows the project's coding standards and includes appropriate tests.

## License

This project is licensed under the Apache 2.0 License - see the LICENSE file for details.