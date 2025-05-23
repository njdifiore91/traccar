# Protocol Service

## Overview

The Protocol Service is a core component of the Traccar GPS tracking system, responsible for handling device communications across 200+ GPS protocols. It serves as the entry point for all device data, decoding raw device messages into standardized Position objects and publishing them to a message broker for downstream processing by other microservices.

This service is built on Netty for high-performance, non-blocking network I/O, enabling it to handle thousands of concurrent device connections efficiently. The Protocol Service maintains device connections and session state, ensuring reliable communication with tracking devices.

## Key Features

- **Multi-Protocol Support**: Handles 200+ GPS device protocols through a unified architecture
- **High-Performance I/O**: Uses Netty for asynchronous, non-blocking network communication
- **Scalable Architecture**: Designed to scale horizontally to handle thousands of concurrent connections
- **Message Broker Integration**: Publishes decoded positions to Kafka/RabbitMQ for downstream processing
- **Service Discovery**: Registers with service discovery mechanism for dynamic endpoint resolution
- **Containerized Deployment**: Packaged as Docker containers and orchestrated with Kubernetes
- **Comprehensive Monitoring**: Exposes metrics, health checks, and structured logging
- **Device Command Support**: Enables sending commands to devices using protocol-specific encoders

## Documentation

This documentation provides comprehensive information about the Protocol Service. Use the links below to navigate to specific documentation sections:

- [Architecture](architecture.md) - Detailed explanation of the service's internal architecture and components
- [Configuration](configuration.md) - Guide to configuring the Protocol Service and its supported protocols
- [Deployment](deployment.md) - Instructions for deploying the service in various environments
- [Operations](operations.md) - Guide for day-to-day operations, monitoring, and maintenance
- [Troubleshooting](troubleshooting.md) - Solutions for common issues and diagnostic procedures

## Quick Start

### Prerequisites

- Docker and Docker Compose (for local development)
- Kubernetes cluster (for production deployment)
- Message broker (Kafka or RabbitMQ)
- Service discovery mechanism (Consul or Kubernetes)

### Running Locally with Docker

```bash
# Clone the repository
git clone https://github.com/traccar/protocol-service.git
cd protocol-service

# Build the Docker image
docker build -t traccar/protocol-service .

# Run with Docker Compose (includes dependencies)
docker-compose up -d
```

### Basic Configuration

Create a `config.yml` file with the following minimal configuration:

```yaml
server:
  port: 8080

protocols:
  - name: osmand
    port: 5055
  - name: teltonika
    port: 5056

messaging:
  broker: kafka
  bootstrap-servers: kafka:9092
  topics:
    positions: raw.positions
    connections: device.connections

discovery:
  type: kubernetes
  namespace: traccar
```

### Deploying to Kubernetes

```bash
# Apply the Kubernetes manifests
kubectl apply -f kubernetes/protocol-service.yaml

# Check the deployment status
kubectl get pods -l app=protocol-service

# View the logs
kubectl logs -l app=protocol-service
```

## Architecture Overview

The Protocol Service implements a three-layer architecture:

1. **Network Layer**: Manages TCP/UDP connections using Netty
2. **Protocol Abstraction Layer**: Decodes protocol-specific messages into standardized Position objects
3. **Session Management Layer**: Tracks device connections and maintains session state

The service publishes decoded positions to a message broker (Kafka/RabbitMQ) for consumption by other services in the Traccar ecosystem, particularly the Position Processing Service.

For a more detailed explanation of the architecture, see the [Architecture Documentation](architecture.md).

## Requirements and Dependencies

- Java 17 or higher
- Netty 4.1.x for network I/O
- Kafka or RabbitMQ for message broker integration
- Consul or Kubernetes for service discovery
- Prometheus for metrics collection (optional)
- Redis for distributed session storage (optional, for multi-instance deployments)

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