# Protocol Service

## Overview

The Protocol Service is a core component of the Traccar GPS tracking system's microservices architecture. It serves as the entry point for all device communications, handling connections and protocol implementations for 200+ GPS device protocols. This service is responsible for decoding raw device messages into a standardized format and publishing them to the message broker for further processing by other services.

## Features

- Support for 200+ GPS device protocols
- Horizontally scalable for handling thousands of concurrent connections
- Netty-based non-blocking I/O for high-performance network handling
- Protocol-specific buffer sizes and timeouts
- Dynamic protocol detection and negotiation
- Connection pooling for device communications
- Device command execution capabilities
- Health check endpoints for monitoring

## Architecture

The Protocol Service is built on the Netty framework, providing high-performance, non-blocking I/O capabilities. For each network channel or connection, the service creates a pipeline of event handlers that process incoming messages:

1. **Connection Acceptance**: Listens on configured ports for device connections
2. **Protocol Detection**: Identifies the device protocol based on message format
3. **Message Framing**: Extracts complete messages from the incoming byte stream
4. **Protocol Decoding**: Transforms raw device messages into standardized Position objects
5. **Message Publishing**: Publishes decoded position data to the message broker
6. **Command Handling**: Processes and sends commands to devices when requested

### Integration Points

The Protocol Service integrates with other components through the following mechanisms:

- **Message Broker**: Publishes decoded position data to a dedicated topic (Kafka/RabbitMQ)
- **Service Discovery**: Registers with Consul or Kubernetes for service location
- **API Gateway**: Receives device command requests via gRPC
- **Health Monitoring**: Exposes health check endpoints for orchestration systems

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|--------|
| `SERVER_PORT` | HTTP port for health checks and metrics | `8082` |
| `PROTOCOL_PORTS` | Comma-separated list of protocol ports to listen on | `5001-5050` |
| `PROTOCOL_ENABLED` | Comma-separated list of enabled protocols | `all` |
| `MESSAGE_BROKER_TYPE` | Type of message broker (kafka, rabbitmq) | `kafka` |
| `MESSAGE_BROKER_URL` | URL of the message broker | `kafka:9092` |
| `POSITION_TOPIC` | Topic name for publishing position data | `raw-positions` |
| `COMMAND_TOPIC` | Topic name for subscribing to command requests | `device-commands` |
| `SERVICE_DISCOVERY_URL` | URL of the service discovery system | `consul:8500` |
| `LOG_LEVEL` | Logging level (INFO, DEBUG, etc.) | `INFO` |
| `NETTY_BOSS_THREADS` | Number of Netty boss threads | `1` |
| `NETTY_WORKER_THREADS` | Number of Netty worker threads | `0` (CPU*2) |
| `CONNECTION_TIMEOUT` | Device connection timeout in seconds | `600` |
| `DATABASE_URL` | JDBC URL for database connection | `jdbc:mysql://localhost:3306/traccar` |
| `DATABASE_USERNAME` | Database username | `traccar` |
| `DATABASE_PASSWORD` | Database password | `traccar` |

### Configuration File

In addition to environment variables, the Protocol Service can be configured using a YAML configuration file. Create a file named `config.yml` in the `/config` directory with the following structure:

```yaml
server:
  port: 8082
  
protocol:
  ports:
    - 5001-5050  # Port range for protocols
  enabled:
    - osmand      # Enable specific protocols
    - teltonika
    - meitrack
    # Add more protocols as needed
    
messageBroker:
  type: kafka     # or rabbitmq
  url: kafka:9092
  topics:
    position: raw-positions
    command: device-commands
    
serviceDiscovery:
  type: consul    # or kubernetes
  url: consul:8500
  
netty:
  bossThreads: 1
  workerThreads: 0  # 0 means CPU count * 2
  connectionTimeout: 600
  
database:
  url: jdbc:mysql://localhost:3306/traccar
  username: traccar
  password: traccar
  
logging:
  level: INFO
```

## Deployment

### Docker

To run the Protocol Service using Docker:

```bash
docker run -d --name protocol-service \
  -p 8082:8082 \
  -p 5001-5050:5001-5050 \
  -v /path/to/config:/opt/traccar/config \
  -e MESSAGE_BROKER_URL=kafka:9092 \
  traccar/protocol-service:latest
```

### Kubernetes

To deploy the Protocol Service on Kubernetes, create a deployment YAML file:

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
        - containerPort: 8082
          name: http
        - containerPort: 5001
          name: osmand
        - containerPort: 5003
          name: teltonika
        # Add more protocol ports as needed
        env:
        - name: MESSAGE_BROKER_URL
          value: "kafka-service:9092"
        - name: SERVICE_DISCOVERY_URL
          value: "consul-service:8500"
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
            port: 8082
          initialDelaySeconds: 60
          periodSeconds: 15
        readinessProbe:
          httpGet:
            path: /health/readiness
            port: 8082
          initialDelaySeconds: 30
          periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: protocol-service
spec:
  selector:
    app: protocol-service
  ports:
  - name: http
    port: 8082
    targetPort: 8082
  - name: osmand
    port: 5001
    targetPort: 5001
  - name: teltonika
    port: 5003
    targetPort: 5003
  # Add more protocol ports as needed
```

Apply the configuration using:

```bash
kubectl apply -f protocol-service-deployment.yaml
```

### Standalone

To run the Protocol Service as a standalone Java application:

1. Download the latest release JAR file from the releases page
2. Create a configuration file as described above
3. Run the service using:

```bash
java -jar protocol-service.jar --config=/path/to/config.yml
```

## Protocol Support

The Protocol Service supports over 200 GPS device protocols. Some of the most commonly used protocols include:

- OsmAnd (port 5055)
- Teltonika (port 5027)
- Meitrack (port 5020)
- TK103 (port 5006)
- GT06 (port 5023)
- H02 (port 5003)
- Coban (port 5005)

To enable a specific protocol, add it to the `protocol.enabled` list in the configuration file or include it in the `PROTOCOL_ENABLED` environment variable.

## Troubleshooting

### Common Issues

#### Service Won't Start

- Check if the required ports are already in use
- Verify database connection settings
- Ensure message broker is accessible
- Check service discovery connectivity

#### Device Connection Issues

- Verify the device is using the correct server address and port
- Check if the protocol is enabled in the configuration
- Examine logs for protocol-specific errors
- Ensure firewall rules allow the protocol port

#### Message Publishing Failures

- Verify message broker connectivity
- Check topic configuration
- Examine broker logs for errors
- Ensure sufficient disk space for message storage

### Logging

The Protocol Service uses structured logging with the following levels:

- ERROR: Critical issues that require immediate attention
- WARN: Potential problems that don't affect core functionality
- INFO: General operational information
- DEBUG: Detailed information for troubleshooting
- TRACE: Very detailed protocol-level message tracing

To enable debug logging, set the `LOG_LEVEL` environment variable to `DEBUG` or update the configuration file.

### Monitoring

The Protocol Service exposes the following endpoints for monitoring:

- `/health/liveness`: Basic operational status
- `/health/readiness`: Ability to handle requests
- `/metrics`: Prometheus-compatible metrics
- `/info`: Service information and version

## Development

### Building from Source

To build the Protocol Service from source:

```bash
git clone https://github.com/traccar/protocol-service.git
cd protocol-service
./mvnw clean package
```

### Adding a New Protocol

To add support for a new protocol:

1. Create a new protocol handler class in the `org.traccar.protocol` package
2. Implement the necessary decoders and encoders
3. Register the protocol in the `ProtocolManager` class
4. Add the protocol to the default configuration

Refer to the developer documentation for detailed instructions on implementing new protocols.

## License

The Protocol Service is licensed under the Apache License 2.0. See the LICENSE file for details.