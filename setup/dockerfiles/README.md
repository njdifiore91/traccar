# Traccar Microservices Dockerfiles

This directory contains Dockerfiles for building container images for each of the Traccar microservices. These Dockerfiles implement best practices for containerization, including multi-stage builds, security hardening, and optimized image sizes.

> **Note:** This documentation covers the containerization approach for the new microservices architecture, which replaces the previous monolithic container image.

## Overview

The Traccar microservices architecture consists of six distinct services, each with its own Dockerfile:

- `protocol.Dockerfile` - Protocol Service for handling device communications and protocol decoding
- `position.Dockerfile` - Position Processing Service for processing, validating, and enriching position data
- `event.Dockerfile` - Event Processing Service for detecting events based on position data
- `notification.Dockerfile` - Notification Service for delivering alerts via multiple channels
- `api-gateway.Dockerfile` - API Gateway Service for routing client requests and handling authentication
- `reporting.Dockerfile` - Reporting Service for generating various report types and analytics

## Base Images

All Dockerfiles use a standardized base image approach:

- **Build Stage**: Eclipse Temurin OpenJDK 17 (full JDK) for compilation and building
- **Runtime Stage**: Two variants are provided:
  - **Standard**: Eclipse Temurin OpenJDK 17 slim for general usage
  - **Alpine**: Alpine Linux variant for minimal production footprint

## Multi-Stage Build Pattern

Each Dockerfile implements a multi-stage build pattern with two primary stages:

1. **Build Stage**: Contains the full JDK and build tools needed to compile and package the application
2. **Runtime Stage**: Contains only the minimal runtime dependencies required to run the application

This approach provides several benefits:

- Smaller final image size by excluding build tools and intermediate artifacts
- Improved security by reducing the attack surface
- Better layer caching for faster rebuilds
- Separation of build-time and runtime dependencies

## Building the Images

### Basic Build

To build a standard image for a service:

```bash
docker build -f setup/dockerfiles/protocol.Dockerfile -t traccar/protocol:latest .
```

### Alpine Variant

To build the Alpine variant with a smaller footprint:

```bash
docker build --target alpine -f setup/dockerfiles/protocol.Dockerfile -t traccar/protocol:latest-alpine .
```

### Building All Services

A convenience script is provided to build all services at once:

```bash
# Build all services with standard images
./setup/build-images.sh

# Build all services with Alpine variants
./setup/build-images.sh --alpine

# Build specific services
./setup/build-images.sh --services "protocol,position,api-gateway"
```

### Build Arguments

The Dockerfiles support several build arguments to customize the build process:

| Argument | Description | Default |
|----------|-------------|--------|
| `BASE_IMAGE` | Base image for the runtime stage | `eclipse-temurin:17-jre-jammy` |
| `ALPINE_IMAGE` | Base image for the Alpine runtime stage | `eclipse-temurin:17-jre-alpine` |
| `APP_USER` | Username for the non-root user | `traccar` |
| `APP_UID` | User ID for the non-root user | `1000` |
| `APP_GID` | Group ID for the non-root user | `1000` |
| `JAVA_OPTS` | Default Java options | `-Xms512m -Xmx512m` |

Example with custom build arguments:

```bash
docker build -f setup/dockerfiles/api-gateway.Dockerfile \
  --build-arg JAVA_OPTS="-Xms1g -Xmx1g" \
  --build-arg APP_UID=1001 \
  -t traccar/api-gateway:latest .
```

## Environment Variables

The container images support configuration through environment variables. Common variables across all services include:

| Variable | Description | Default |
|----------|-------------|--------|
| `JAVA_OPTS` | JVM options | `-Xms512m -Xmx512m` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `CONFIG_FILE` | Path to configuration file | `/opt/traccar/conf/config.xml` |
| `DISCOVERY_TYPE` | Service discovery type (consul, kubernetes) | `kubernetes` |
| `DISCOVERY_HOST` | Service discovery host | `localhost` |
| `DISCOVERY_PORT` | Service discovery port | `8500` |
| `BROKER_TYPE` | Message broker type (kafka, rabbitmq) | `rabbitmq` |
| `BROKER_HOST` | Message broker host | `localhost` |
| `BROKER_PORT` | Message broker port | Depends on broker type |

Service-specific environment variables are documented in each service's respective documentation.

## Security Considerations

The Dockerfiles implement several security best practices:

### Non-Root User Execution

All services run as a non-privileged user (UID 1000 by default) to reduce the risk of container breakout vulnerabilities. The user is created during the build process and has minimal permissions.

```dockerfile
# Example from Dockerfile
RUN groupadd -g ${APP_GID} ${APP_USER} && \
    useradd -u ${APP_UID} -g ${APP_GID} -s /bin/bash -m ${APP_USER}

USER ${APP_USER}
```

### Minimal Included Packages

The runtime images contain only the necessary dependencies to run the application, reducing the attack surface. Development tools and debugging utilities are excluded from the production images.

```dockerfile
# Example from Alpine variant
RUN apk add --no-cache tzdata ca-certificates && \
    rm -rf /var/cache/apk/*
```

### Resource Constraints

Container resource limits should be set when deploying to prevent resource exhaustion attacks. Example Kubernetes configuration:

```yaml
resources:
  limits:
    cpu: "1"
    memory: "1Gi"
  requests:
    cpu: "200m"
    memory: "512Mi"
```

### Read-Only File System

Where possible, containers should be run with a read-only file system, with specific writable volumes mounted only where needed:

```bash
docker run --read-only \
  --tmpfs /tmp \
  -v traccar-logs:/opt/traccar/logs \
  traccar/protocol:latest
```

### Health Checks

All services expose health check endpoints that can be used to monitor container health:

```bash
docker run \
  --health-cmd="curl -f http://localhost:8082/health || exit 1" \
  --health-interval=30s \
  --health-timeout=5s \
  --health-retries=3 \
  traccar/api-gateway:latest
```

### Security Scanning

All container images should be scanned for vulnerabilities before deployment:

```bash
# Using Trivy scanner
trivy image traccar/protocol:latest

# Using Docker Scout
docker scout cves traccar/api-gateway:latest
```

## Customization Examples

### Custom Configuration

Mount a custom configuration file:

```bash
docker run -v $(pwd)/custom-config.xml:/opt/traccar/conf/config.xml traccar/protocol:latest
```

### External Database

Configure an external database connection:

```bash
docker run \
  -e DATABASE_URL="jdbc:postgresql://db-host:5432/traccar" \
  -e DATABASE_USER="traccar" \
  -e DATABASE_PASSWORD="password" \
  traccar/api-gateway:latest
```

### Custom Logging Configuration

Mount a custom logging configuration:

```bash
docker run -v $(pwd)/logback.xml:/opt/traccar/conf/logback.xml traccar/position:latest
```

### Service-Specific Port Mapping

Each service requires specific port mappings:

```bash
# Protocol Service - Device protocol ports
docker run -p 5000-5100:5000-5100 traccar/protocol:latest

# API Gateway Service - API and WebSocket ports
docker run -p 8082:8082 -p 8001:8001 traccar/api-gateway:latest

# Position/Event/Notification/Reporting Services - Management API ports
docker run -p 8083:8083 traccar/position:latest
docker run -p 8084:8084 traccar/event:latest
docker run -p 8085:8085 traccar/notification:latest
docker run -p 8086:8086 traccar/reporting:latest
```

### Custom JVM Options

Adjust Java runtime settings:

```bash
docker run \
  -e JAVA_OPTS="-Xms1g -Xmx1g -XX:+UseG1GC -XX:MaxGCPauseMillis=200" \
  traccar/reporting:latest
```

## Kubernetes Deployment

The container images are designed to work well in Kubernetes environments. Example Kubernetes deployment snippet:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: traccar-protocol
spec:
  replicas: 2
  selector:
    matchLabels:
      app: traccar-protocol
  template:
    metadata:
      labels:
        app: traccar-protocol
    spec:
      containers:
      - name: protocol
        image: traccar/protocol:latest
        ports:
        - containerPort: 5000-5100
        env:
        - name: DISCOVERY_TYPE
          value: "kubernetes"
        - name: BROKER_HOST
          value: "rabbitmq-service"
        resources:
          limits:
            cpu: "1"
            memory: "1Gi"
          requests:
            cpu: "200m"
            memory: "512Mi"
        livenessProbe:
          httpGet:
            path: /health
            port: 8082
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 8082
          initialDelaySeconds: 5
          periodSeconds: 5
        securityContext:
          runAsUser: 1000
          runAsGroup: 1000
          readOnlyRootFilesystem: true
          allowPrivilegeEscalation: false
```

## Image Versioning

The Traccar microservices follow a consistent image tagging convention:

- `traccar/{service}:{semver}` - Standard release with Temurin slim
- `traccar/{service}:{semver}-alpine` - Alpine-based minimal production image
- `traccar/{service}:latest` - Latest stable release of each service
- `traccar/{service}:edge` - Development builds for testing

Where `{service}` is one of: `protocol`, `position`, `event`, `notification`, `api-gateway`, or `reporting`.

## Troubleshooting

### Common Issues

1. **Container fails to start with permission errors**
   - Ensure volumes mounted into the container have appropriate permissions for the non-root user (UID 1000)
   - Example fix: `chown -R 1000:1000 /path/to/mounted/volume`

2. **Out of memory errors**
   - Adjust the `JAVA_OPTS` environment variable to provide appropriate heap settings
   - Example fix: `-e JAVA_OPTS="-Xms512m -Xmx1g -XX:+HeapDumpOnOutOfMemoryError"`

3. **Service discovery failures**
   - Verify the `DISCOVERY_HOST` and `DISCOVERY_PORT` environment variables are correctly set
   - Check network connectivity between the container and the service discovery system
   - Example fix: `-e DISCOVERY_TYPE=kubernetes` for Kubernetes environments

4. **Message broker connectivity issues**
   - Verify the `BROKER_HOST` and `BROKER_PORT` environment variables are correctly set
   - Check network connectivity between the container and the message broker
   - Example fix: `-e BROKER_TYPE=rabbitmq -e BROKER_HOST=rabbitmq-service`

5. **Protocol service not receiving connections**
   - Ensure proper port mapping for the specific protocols being used
   - Verify firewall rules allow incoming connections
   - Example fix: `-p 5055:5055` for specific protocol port

### Debugging

For debugging purposes, you can enable debug logging:

```bash
docker run -e LOG_LEVEL=DEBUG traccar/protocol:latest
```

To access the container for troubleshooting:

```bash
docker exec -it <container_id> /bin/sh
```

To view container logs:

```bash
# Follow logs in real-time
docker logs -f <container_id>

# View last 100 lines
docker logs --tail 100 <container_id>
```

To check container health status:

```bash
docker inspect --format='{{.State.Health.Status}}' <container_id>
```

## Microservices Architecture

The containerized microservices architecture provides several advantages over the previous monolithic approach:

- **Independent Scaling**: Each service can be scaled independently based on its resource requirements
- **Isolated Development**: Teams can develop, test, and deploy services independently
- **Technology Flexibility**: Services can evolve with different dependencies without affecting others
- **Resilience**: Failures are isolated to specific services rather than bringing down the entire system
- **Resource Efficiency**: Resources can be allocated more precisely to services that need them

### Service Interactions

The microservices interact primarily through asynchronous messaging via a message broker:

1. **Protocol Service** receives device data and publishes position messages
2. **Position Service** consumes position messages, processes them, and publishes processed positions
3. **Event Service** consumes processed positions, detects events, and publishes event messages
4. **Notification Service** consumes event messages and sends notifications through various channels
5. **API Gateway** provides a unified interface for clients and manages WebSocket connections
6. **Reporting Service** generates reports based on historical position and event data

## Additional Resources

- [Traccar Documentation](https://www.traccar.org/documentation/)
- [Docker Documentation](https://docs.docker.com/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Multi-stage Builds](https://docs.docker.com/build/building/multi-stage/)
- [Docker Security Best Practices](https://docs.docker.com/develop/security-best-practices/)