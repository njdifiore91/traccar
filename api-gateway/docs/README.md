# API Gateway Service

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Documentation](#documentation)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Version Information](#version-information)

## Overview

The API Gateway Service is the central entry point for all client interactions with the Traccar microservices ecosystem. It provides a unified interface for accessing backend services while maintaining backward compatibility with existing API contracts.

### Key Features

- **Unified API Access**: Single entry point for all client requests
- **Request Routing**: Directs API calls to appropriate microservices
- **Authentication & Authorization**: Centralizes security enforcement
- **WebSocket Support**: Manages real-time updates to clients
- **Backward Compatibility**: Maintains existing API contracts
- **Service Discovery**: Dynamically locates available service instances
- **Circuit Breaking**: Prevents cascading failures when downstream services are unavailable

## Architecture

The API Gateway Service implements a facade pattern, sitting between clients and backend microservices:

```
Clients (Web/Mobile) → API Gateway → Backend Microservices
```

### Key Components

- **REST API Endpoints**: JAX-RS resources for CRUD operations
- **WebSocket Server**: Handles real-time updates to clients
- **Authentication Filters**: Validate user credentials and tokens
- **Request Router**: Routes requests to appropriate backend services
- **Service Discovery Client**: Locates available service instances
- **Circuit Breaker**: Prevents cascading failures

### Technology Stack

- Java 17
- Jetty 11.0.24 (Embedded web server)
- Jersey 3.1.10 (JAX-RS implementation)
- Jackson 2.18.2 (JSON processing)
- JWT for authentication tokens

## Quick Start

### Prerequisites

- JDK 17 or higher
- Maven 3.8+
- Docker and Docker Compose (for local development)

### Running Locally

1. Clone the repository
2. Navigate to the api-gateway directory
3. Build the service:
   ```bash
   mvn clean package
   ```
4. Run with default configuration:
   ```bash
   java -jar target/api-gateway.jar
   ```

### Docker

```bash
docker build -t traccar/api-gateway .
docker run -p 8082:8082 traccar/api-gateway
```

## Documentation

### API Documentation

- [REST API Reference](./api/README.md) - Comprehensive API documentation
- [WebSocket Protocol](./api/websocket.md) - Real-time updates protocol
- [Authentication](./api/authentication.md) - Authentication mechanisms

### Developer Guides

- [Development Guide](./development.md) - Guide for developers working with the API Gateway
- [Integration Guide](./integration.md) - How to integrate with the API Gateway
- [Customization Guide](./customization.md) - Customizing the API Gateway

### Operator Guides

- [Deployment Guide](./deployment.md) - Deployment instructions
- [Configuration Guide](./configuration.md) - Configuration options
- [Monitoring Guide](./monitoring.md) - Monitoring and observability

## Configuration

The API Gateway Service can be configured through environment variables, configuration files, or Kubernetes ConfigMaps.

### Key Configuration Options

- `API_PORT` - HTTP port (default: 8082)
- `API_HOST` - Bind address (default: 0.0.0.0)
- `SERVICE_DISCOVERY_URL` - Service discovery endpoint
- `AUTH_JWT_SECRET` - JWT signing secret
- `CORS_ALLOWED_ORIGINS` - CORS configuration
- `WEB_TIMEOUT` - WebSocket timeout in milliseconds

See the [Configuration Guide](./configuration.md) for a complete list of options.

## Deployment

### Kubernetes

The API Gateway Service is designed to be deployed in a Kubernetes environment:

```yaml
# Example Kubernetes deployment snippet
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api-gateway
spec:
  replicas: 2
  selector:
    matchLabels:
      app: api-gateway
  template:
    metadata:
      labels:
        app: api-gateway
    spec:
      containers:
      - name: api-gateway
        image: traccar/api-gateway:latest
        ports:
        - containerPort: 8082
```

See the [Deployment Guide](./deployment.md) for complete deployment instructions.

## Version Information

### Current Version

- Version: 1.0.0
- Release Date: 2024-05-24

### Compatibility

The API Gateway Service maintains backward compatibility with the following:

- All existing Traccar API clients (web, mobile, third-party)
- Traccar API v1.0 and above
- WebSocket protocol v1.0 and above

### Changelog

See the [CHANGELOG.md](./CHANGELOG.md) file for a detailed list of changes between versions.