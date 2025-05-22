# [Traccar](https://www.traccar.org)

## Overview

Traccar is an open source GPS tracking system. This repository contains the Traccar platform implemented as a distributed microservices architecture. It supports more than 200 GPS protocols and more than 2000 models of GPS tracking devices. Traccar can be used with any major SQL database system. It also provides easy to use [REST API](https://www.traccar.org/traccar-api/).

Other parts of Traccar solution include:

- [Traccar web app](https://github.com/traccar/traccar-web)
- [Traccar Manager Android app](https://github.com/traccar/traccar-manager-android)
- [Traccar Manager iOS app](https://github.com/traccar/traccar-manager-ios)

There is also a set of mobile apps that you can use for tracking mobile devices:

- [Traccar Client Android app](https://github.com/traccar/traccar-client-android)
- [Traccar Client iOS app](https://github.com/traccar/traccar-client-ios)

## Microservices Architecture

Traccar has been refactored from a monolithic architecture to a distributed microservices architecture. This transformation enables independent scaling, deployment, and maintenance of each service while maintaining complete functional equivalence and backward compatibility for clients.

### Core Microservices Components

- **Protocol Service**: Handles device connections and protocol decoding for 200+ GPS device protocols
- **Position Processing Service**: Processes and enriches position data with validation, filtering, and geolocation
- **Event Processing Service**: Analyzes position data to detect events like geofence entry/exit, speeding, etc.
- **Notification Service**: Manages delivery of alerts through multiple channels (email, SMS, push)
- **API Gateway Service**: Routes client requests and provides unified API access with WebSocket support
- **Reporting Service**: Generates reports and analytics with various output formats

### Inter-Service Communication

Services communicate primarily through asynchronous event-driven patterns using a message broker:

- **Message Broker Integration**: Kafka/RabbitMQ for reliable, scalable communication between services
- **Event-Based Architecture**: Services publish events to topics that other services can subscribe to
- **Transactional Outbox Pattern**: Ensures reliable message delivery and data consistency

### Service Discovery

Dynamic service registration and discovery is implemented using:

- **Consul/Kubernetes API**: Enables services to find and communicate with each other
- **Health Checks**: Automated monitoring of service health and availability
- **Load Balancing**: Intelligent request routing between service instances

## Features

Some of the available features include:

- Real-time GPS tracking
- Driver behaviour monitoring
- Detailed and summary reports
- Geofencing functionality
- Alarms and notifications
- Account and device management
- Email and SMS support

## Build and Deployment

### Building from Source

Please read [build from source documentation](https://www.traccar.org/build/) on the official website.

### Containerization

Each microservice is containerized using Docker:

```
# Example: Building the Protocol Service
docker build -t traccar/protocol:latest ./protocol-service
```

Container images follow a consistent naming convention:
- `registry/traccar/{service}:{semver}` - Standard release
- `registry/traccar/{service}:{semver}-alpine` - Alpine-based minimal image
- `registry/traccar/{service}:latest` - Latest stable release

### Orchestration

Traccar microservices can be deployed using:

- **Kubernetes with Helm**: Recommended for production environments
  ```
  # Deploy the complete Traccar platform
  helm install traccar ./setup/helm
  ```

- **Docker Compose**: For local development only
  ```
  # Start all services locally
  docker-compose up -d
  ```

## Development Workflow

Working with the distributed system requires understanding the service boundaries and communication patterns:

1. **Service-Specific Development**: Each service has its own codebase, configuration, and tests
2. **Local Environment**: Use Docker Compose to run dependent services locally
3. **Integration Testing**: Test service interactions using message broker and service discovery
4. **Observability**: Monitor service behavior using distributed tracing and metrics

Refer to the documentation in each service directory for service-specific development guidelines.

## Team

- Anton Tananaev ([anton@traccar.org](mailto:anton@traccar.org))
- Andrey Kunitsyn ([andrey@traccar.org](mailto:andrey@traccar.org))

## License

    Apache License, Version 2.0

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.