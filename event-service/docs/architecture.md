# Event Processing Service Architecture

## 1. Overview

The Event Processing Service is a critical component in the Traccar microservices architecture, responsible for analyzing position data to detect significant events such as geofence transitions, speed violations, device status changes, and maintenance alerts. This service consumes enriched position data from the Position Processing Service via a message broker, applies various event detection algorithms, and publishes detected events to the Notification Service for delivery to users.

### 1.1 Service Responsibilities

The Event Processing Service has the following core responsibilities:

- Consuming enriched position data from the message broker
- Processing position data through multiple specialized event handlers
- Detecting various types of events based on position data and device context
- Publishing detected events to the message broker for consumption by other services
- Registering with the service discovery mechanism for dynamic service location
- Exposing health endpoints for monitoring and orchestration

### 1.2 Position in the Microservices Architecture

The Event Processing Service operates as part of the following data flow in the Traccar microservices ecosystem:

```
Protocol Service → Message Broker → Position Processing Service → Message Broker → Event Processing Service → Message Broker → Notification Service
```

This service is designed to be horizontally scalable, with multiple instances consuming from the same message broker topics using consumer groups to distribute the processing load.

## 2. Internal Architecture

### 2.1 Component Structure

The Event Processing Service is composed of the following major components:

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Event Processing Service                        │
│                                                                     │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐  │
│  │                 │    │                 │    │                 │  │
│  │  Position       │    │  Event Handler  │    │  Event          │  │
│  │  Consumer       │───▶│  Pipeline       │───▶│  Producer       │  │
│  │                 │    │                 │    │                 │  │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘  │
│                                                                     │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐  │
│  │                 │    │                 │    │                 │  │
│  │  Service        │    │  Health Check   │    │  Metrics        │  │
│  │  Registry       │    │  Endpoints      │    │  Collection     │  │
│  │                 │    │                 │    │                 │  │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

#### 2.1.1 Position Consumer

The Position Consumer component subscribes to the enriched position data topic in the message broker (Kafka/RabbitMQ) and deserializes the messages into Position objects. It implements:

- Consumer group configuration for horizontal scaling
- Partition assignment by device ID to ensure ordered processing
- Dead letter queue handling for unprocessable messages
- Retry mechanisms with exponential backoff for transient failures
- Correlation ID propagation for distributed tracing

#### 2.1.2 Event Handler Pipeline

The Event Handler Pipeline consists of multiple specialized event handlers, each responsible for detecting specific types of events. The pipeline includes:

- **BaseEventHandler**: Abstract base class for all event handlers
- **MotionEventHandler**: Detects motion start/stop events
- **GeofenceEventHandler**: Detects geofence entry/exit events
- **OverspeedEventHandler**: Detects speed limit violations
- **IgnitionEventHandler**: Detects ignition on/off events
- **FuelEventHandler**: Detects fuel level changes
- **MaintenanceEventHandler**: Detects maintenance-related events
- **AlarmEventHandler**: Detects device-reported alarms
- **BehaviorEventHandler**: Detects driver behavior events
- **CommandResultEventHandler**: Processes command execution results
- **DriverEventHandler**: Detects driver assignment changes
- **MediaEventHandler**: Processes media-related events

Each handler implements circuit breaker patterns for resilience, metrics collection for monitoring, and distributed tracing for observability.

#### 2.1.3 Event Producer

The Event Producer component serializes detected events and publishes them to the events topic in the message broker. It implements:

- Event type as message key for topic partitioning
- Transaction outbox pattern for reliable message publishing
- Configurable acknowledgment settings
- Retry mechanisms for failed message publishing
- Correlation ID propagation for distributed tracing

#### 2.1.4 Service Registry

The Service Registry component handles service registration and discovery, allowing the Event Processing Service to register itself with the service discovery mechanism (Consul/Kubernetes) and discover other services it needs to communicate with. It supports:

- Service registration at startup
- Health check registration
- Service deregistration on shutdown
- Service discovery for locating other services

#### 2.1.5 Health Check Endpoints

The Health Check Endpoints component exposes HTTP endpoints for monitoring the health and readiness of the service. It provides:

- Liveness probe: Basic operational status
- Readiness probe: Ability to handle requests
- Detailed health status: Component-level health information

#### 2.1.6 Metrics Collection

The Metrics Collection component gathers and exposes metrics about the service's operation for monitoring and alerting. It collects:

- Event detection rates by event type
- Processing latency metrics
- Message broker operation metrics
- Resource utilization metrics

### 2.2 Data Flow

The data flow through the Event Processing Service follows these steps:

1. The Position Consumer receives enriched position data from the message broker
2. The position data is passed through the Event Handler Pipeline
3. Each event handler analyzes the position data and detects relevant events
4. Detected events are passed to the Event Producer
5. The Event Producer publishes the events to the message broker
6. The Notification Service consumes the events for delivery to users

## 3. Integration with Other Services

### 3.1 Message Broker Integration

The Event Processing Service integrates with the message broker (Kafka/RabbitMQ) for asynchronous communication with other services:

- **Consumes from**: `enriched.positions` topic (from Position Processing Service)
- **Publishes to**: `events` topic (for Notification Service)

The service uses consumer groups for horizontal scaling, with multiple instances sharing the processing load. It implements exactly-once processing semantics to ensure reliable event detection without duplicates.

### 3.2 Service Discovery Integration

The Event Processing Service registers with the service discovery mechanism (Consul/Kubernetes) to enable other services to locate it dynamically. It provides:

- Service identification (name, version, instance ID)
- Endpoint information (host, port, protocol)
- Health check configuration
- Service metadata

### 3.3 Distributed Tracing

The service implements distributed tracing using OpenTelemetry to provide end-to-end visibility of requests flowing through the system. It propagates correlation IDs across service boundaries, enabling tracing of position data from ingestion through event detection to notification delivery.

## 4. Resilience Patterns

The Event Processing Service implements several resilience patterns to ensure robust operation:

### 4.1 Circuit Breakers

Circuit breakers protect the service from cascading failures when interacting with external dependencies. They are implemented using Resilience4j with service-specific parameters:

- Sliding window size: 100 requests
- Failure rate threshold: 50%
- Wait duration in open state: 10 seconds
- Permitted calls in half-open state: 10

### 4.2 Retry Mechanisms

Retry mechanisms with exponential backoff are implemented for transient failures:

- Maximum attempts: 3
- Initial wait duration: 1 second
- Exponential backoff multiplier: 2
- Randomized wait factor: 0.5

### 4.3 Dead Letter Queues

Dead letter queues handle messages that cannot be processed after multiple retry attempts. These messages are sent to dedicated dead letter topics for later analysis and potential reprocessing.

### 4.4 Graceful Degradation

The service implements graceful degradation strategies to maintain core functionality during partial failures:

- Prioritizing critical events over informational events
- Reducing rule evaluation frequency under high load
- Falling back to simpler rule evaluation strategies

## 5. Deployment and Scaling

### 5.1 Containerization

The Event Processing Service is packaged as a Docker container with the following characteristics:

- Base image: Eclipse Temurin JRE 17
- Exposed ports: 8080 (HTTP), 8081 (Management)
- Health check endpoint: /actuator/health
- Resource requirements: 250m-1000m CPU, 512Mi-1Gi memory

### 5.2 Kubernetes Deployment

The service is deployed in Kubernetes with the following configuration:

- Deployment with configurable replica count
- Horizontal Pod Autoscaler based on CPU utilization and custom metrics
- Liveness and readiness probes for health monitoring
- ConfigMaps for configuration
- Service for network access

### 5.3 Scaling Strategy

The Event Processing Service scales horizontally based on the following factors:

- Position message throughput
- Event rule complexity and count
- Processing latency

The service uses consumer groups to distribute the processing load across multiple instances, with partition assignment strategies ensuring that positions from the same device are processed by the same instance to maintain order.

## 6. Configuration

The Event Processing Service is configured through the following mechanisms:

### 6.1 Application Properties

The service uses Spring Boot application properties for configuration, with support for profiles:

- `application.yml`: Base configuration
- `application-dev.yml`: Development environment overrides
- `application-prod.yml`: Production environment overrides

### 6.2 Environment Variables

Key configuration parameters can be overridden using environment variables, following the standard Spring Boot convention:

- `EVENT_SERVICE_PORT`: HTTP port (default: 8080)
- `EVENT_SERVICE_MANAGEMENT_PORT`: Management port (default: 8081)
- `EVENT_SERVICE_BROKER_TYPE`: Message broker type (kafka/rabbitmq)
- `EVENT_SERVICE_REGISTRY_TYPE`: Service registry type (consul/kubernetes)

### 6.3 ConfigMaps and Secrets

In Kubernetes, the service uses ConfigMaps for configuration and Secrets for sensitive information:

- `event-service-config`: General configuration
- `event-service-broker-config`: Message broker configuration
- `event-service-registry-config`: Service registry configuration

## 7. Monitoring and Observability

### 7.1 Health Endpoints

The service exposes the following health endpoints:

- `/actuator/health/liveness`: Basic operational status
- `/actuator/health/readiness`: Ability to handle requests
- `/actuator/health`: Detailed health status

### 7.2 Metrics

The service exposes Prometheus-compatible metrics at `/actuator/prometheus`, including:

- `event_detection_total`: Total number of events detected, by event type
- `event_detection_latency`: Event detection latency in milliseconds
- `position_processing_total`: Total number of positions processed
- `position_processing_latency`: Position processing latency in milliseconds

### 7.3 Logging

The service uses structured JSON logging with consistent fields:

- Timestamp
- Service name
- Log level
- Correlation ID
- Message
- Additional context

### 7.4 Distributed Tracing

The service implements distributed tracing using OpenTelemetry, with traces exported to a compatible backend (Jaeger/Zipkin). Each trace includes:

- Service name
- Operation name
- Start and end timestamps
- Correlation ID
- Tags and attributes

## 8. Conclusion

The Event Processing Service is a critical component in the Traccar microservices architecture, responsible for detecting events based on position data. It is designed to be horizontally scalable, resilient to failures, and observable through comprehensive monitoring. The service integrates with the message broker for asynchronous communication and the service discovery mechanism for dynamic service location, enabling it to operate effectively in a distributed environment.