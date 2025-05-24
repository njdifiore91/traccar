# Event Processing Service Troubleshooting Guide

## Introduction

The Event Processing Service is a critical component in the Traccar microservices architecture responsible for analyzing position data to detect significant events such as geofence transitions, speed violations, device status changes, and maintenance alerts. This service consumes enriched position data from the message broker, processes it through various event handlers, and publishes detected events back to the message broker for consumption by the Notification Service.

This guide provides troubleshooting procedures for common issues that may occur during the operation of the Event Processing Service.

## Architecture Overview

The Event Processing Service consists of the following key components:

- **Position Consumer**: Subscribes to the `enriched.positions` topic to receive position data from the Position Processing Service
- **Event Handlers**: Specialized components that analyze position data to detect specific types of events (geofence, overspeed, motion, etc.)
- **Event Producer**: Publishes detected events to the `events` topic for consumption by the Notification Service
- **Service Discovery**: Registers the service with Consul or Kubernetes for discovery by other services
- **Health Checks**: Monitors the health of the service and its dependencies

## Common Issues and Solutions

### 1. Message Broker Connectivity Issues

#### Symptoms
- No events being detected despite position data being available
- Error logs showing connection failures to Kafka or RabbitMQ
- Health check failures for broker connectivity

#### Troubleshooting Steps

1. **Verify broker connectivity**:
   ```bash
   # For Kafka
   kubectl exec -it event-service-pod-name -- bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
   
   # For RabbitMQ
   kubectl exec -it event-service-pod-name -- rabbitmqctl list_queues
   ```

2. **Check broker configuration**:
   - Verify that the broker connection settings in `application.yml` are correct
   - Ensure that the required topics/exchanges exist
   - Check that the service has appropriate permissions to access the broker

3. **Inspect broker logs**:
   ```bash
   kubectl logs -f kafka-pod-name
   # or
   kubectl logs -f rabbitmq-pod-name
   ```

4. **Verify network connectivity**:
   ```bash
   kubectl exec -it event-service-pod-name -- ping kafka
   kubectl exec -it event-service-pod-name -- telnet kafka 9092
   ```

#### Solutions

1. **Correct configuration**:
   - Update broker connection settings in `application.yml`
   - Ensure that the broker address is correctly specified

2. **Restart the service**:
   ```bash
   kubectl rollout restart deployment event-service
   ```

3. **Check broker health**:
   - Ensure that the broker is running and healthy
   - Verify that the broker has sufficient resources

### 2. Event Detection Problems

#### Symptoms
- Expected events not being generated
- Incorrect event data
- Duplicate events

#### Troubleshooting Steps

1. **Check event handler configuration**:
   - Verify that the appropriate event handlers are enabled
   - Check the configuration parameters for each handler

2. **Inspect position data**:
   ```bash
   # For Kafka
   kubectl exec -it event-service-pod-name -- bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic enriched.positions --from-beginning
   ```

3. **Enable debug logging**:
   - Set the log level to DEBUG for event handlers
   ```yaml
   logging:
     level:
       org.traccar.handler.events: DEBUG
   ```

4. **Verify event rules**:
   - Check that the event detection rules are correctly configured in the database
   - Ensure that the device has the appropriate attributes for event detection

#### Solutions

1. **Update event handler configuration**:
   - Modify the configuration parameters for the relevant event handlers
   - Restart the service to apply changes

2. **Fix data issues**:
   - Ensure that position data contains the required attributes for event detection
   - Verify that the Position Processing Service is correctly enriching position data

3. **Check database connectivity**:
   - Ensure that the service can access the database for device and geofence information
   - Verify that the database contains the correct data

### 3. Service Discovery Issues

#### Symptoms
- Service not registering with Consul or Kubernetes
- Other services unable to discover the Event Processing Service
- Health check failures

#### Troubleshooting Steps

1. **Check service registry status**:
   ```bash
   # For Consul
   curl http://consul:8500/v1/catalog/service/event-service
   
   # For Kubernetes
   kubectl get endpoints event-service
   ```

2. **Verify service registry configuration**:
   - Check that the service registry settings in `application.yml` are correct
   - Ensure that the service is configured to register with the correct registry

3. **Inspect service logs for registration errors**:
   ```bash
   kubectl logs -f event-service-pod-name | grep -i discovery
   ```

4. **Check health check endpoints**:
   ```bash
   kubectl exec -it event-service-pod-name -- curl http://localhost:8080/actuator/health
   ```

#### Solutions

1. **Update service registry configuration**:
   - Correct the service registry settings in `application.yml`
   - Ensure that the service has the correct permissions to register

2. **Restart the service**:
   ```bash
   kubectl rollout restart deployment event-service
   ```

3. **Verify network connectivity to the registry**:
   - Ensure that the service can reach the registry service
   - Check network policies and firewall rules

### 4. Performance Issues

#### Symptoms
- High CPU or memory usage
- Slow event processing
- Increasing message lag

#### Troubleshooting Steps

1. **Monitor resource usage**:
   ```bash
   kubectl top pod event-service-pod-name
   ```

2. **Check message broker lag**:
   ```bash
   # For Kafka
   kubectl exec -it event-service-pod-name -- bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group event-processors
   ```

3. **Analyze metrics**:
   - Review Prometheus metrics for event processing rates and latencies
   - Check JVM memory usage and garbage collection statistics

4. **Inspect logs for slow operations**:
   ```bash
   kubectl logs -f event-service-pod-name | grep -i slow
   ```

#### Solutions

1. **Scale the service**:
   ```bash
   kubectl scale deployment event-service --replicas=3
   ```

2. **Optimize configuration**:
   - Adjust thread pool sizes for event handlers
   - Optimize database query patterns
   - Configure appropriate batch sizes for message consumption

3. **Increase resource limits**:
   - Update CPU and memory limits in the deployment configuration
   ```yaml
   resources:
     requests:
       memory: "512Mi"
       cpu: "500m"
     limits:
       memory: "1Gi"
       cpu: "1000m"
   ```

### 5. Circuit Breaker and Retry Failures

#### Symptoms
- Frequent circuit breaker open states
- Excessive retries
- Error logs showing circuit breaker trips

#### Troubleshooting Steps

1. **Check circuit breaker status**:
   ```bash
   curl http://event-service:8080/actuator/circuitbreakers
   ```

2. **Inspect retry metrics**:
   ```bash
   curl http://event-service:8080/actuator/retries
   ```

3. **Analyze logs for circuit breaker events**:
   ```bash
   kubectl logs -f event-service-pod-name | grep -i circuit
   ```

4. **Verify dependent service health**:
   - Check the health of services that the Event Processing Service depends on
   - Ensure that the database and message broker are functioning correctly

#### Solutions

1. **Adjust circuit breaker configuration**:
   - Modify the circuit breaker parameters in `application.yml`
   ```yaml
   resilience4j:
     circuitbreaker:
       instances:
         cacheManager:
           slidingWindowSize: 100
           failureRateThreshold: 50
           waitDurationInOpenState: 10s
   ```

2. **Update retry policies**:
   - Configure appropriate retry settings for transient failures
   ```yaml
   resilience4j:
     retry:
       instances:
         cacheManager:
           maxAttempts: 3
           waitDuration: 1s
   ```

3. **Fix underlying issues**:
   - Address the root cause of failures in dependent services
   - Ensure that the service has sufficient resources

## Debugging Techniques

### Logging and Monitoring

#### Enable Debug Logging

To enable debug logging for specific components:

```yaml
logging:
  level:
    org.traccar.handler.events: DEBUG
    org.traccar.messaging: DEBUG
    org.traccar.discovery: DEBUG
```

Apply these changes to the ConfigMap and restart the service:

```bash
kubectl edit configmap event-service-config
kubectl rollout restart deployment event-service
```

#### Access Logs

To view logs for the Event Processing Service:

```bash
kubectl logs -f deployment/event-service

# Filter for specific log levels
kubectl logs -f deployment/event-service | grep -i error
kubectl logs -f deployment/event-service | grep -i warn

# Filter for specific components
kubectl logs -f deployment/event-service | grep -i "GeofenceEventHandler"
kubectl logs -f deployment/event-service | grep -i "PositionConsumer"
```

#### Prometheus Metrics

The Event Processing Service exposes metrics through the `/actuator/prometheus` endpoint. Key metrics to monitor include:

- `event_processing_rate`: Events processed per second
- `event_processing_time`: Time taken to process events
- `event_detection_count`: Number of events detected by type
- `message_consumer_lag`: Lag in message consumption
- `circuit_breaker_state`: State of circuit breakers

To access these metrics:

```bash
curl http://event-service:8080/actuator/prometheus
```

### Distributed Tracing

The Event Processing Service integrates with OpenTelemetry for distributed tracing. Traces can be viewed in Jaeger or Zipkin.

To access traces:

1. Open the Jaeger UI (typically available at http://jaeger-ui:16686)
2. Select "event-service" from the service dropdown
3. Filter for specific operations or time ranges
4. Examine the trace details to identify bottlenecks or errors

Key spans to look for:

- `consumePosition`: Position consumption from the message broker
- `processPosition`: Position processing through event handlers
- `detectEvent`: Event detection in specific handlers
- `publishEvent`: Event publication to the message broker

### Health Checks

The Event Processing Service exposes health check endpoints through Spring Boot Actuator:

```bash
# Overall health
curl http://event-service:8080/actuator/health

# Component-specific health
curl http://event-service:8080/actuator/health/kafka
curl http://event-service:8080/actuator/health/db

# Liveness and readiness
curl http://event-service:8080/actuator/health/liveness
curl http://event-service:8080/actuator/health/readiness
```

### Message Broker Inspection

#### Kafka

To inspect Kafka topics and messages:

```bash
# List topics
kubectl exec -it kafka-pod-name -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Describe a topic
kubectl exec -it kafka-pod-name -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic enriched.positions

# Consume messages from a topic
kubectl exec -it kafka-pod-name -- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic enriched.positions --from-beginning

# Check consumer group lag
kubectl exec -it kafka-pod-name -- bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group event-processors
```

#### RabbitMQ

To inspect RabbitMQ queues and messages:

```bash
# List queues
kubectl exec -it rabbitmq-pod-name -- rabbitmqctl list_queues

# List exchanges
kubectl exec -it rabbitmq-pod-name -- rabbitmqctl list_exchanges

# List bindings
kubectl exec -it rabbitmq-pod-name -- rabbitmqctl list_bindings

# Get queue details
kubectl exec -it rabbitmq-pod-name -- rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

## Support Resources

### Documentation

- [Event Processing Service Architecture](../README.md)
- [Event Handler Documentation](../src/main/java/org/traccar/handler/events/README.md)
- [Message Broker Configuration](../src/main/resources/README.md)
- [Service Discovery Integration](../src/main/java/org/traccar/discovery/README.md)

### Contact Information

- **Technical Support**: support@traccar.org
- **Development Team**: dev@traccar.org
- **Issue Tracker**: https://github.com/traccar/traccar/issues

### Community Resources

- **Forum**: https://forum.traccar.org
- **Slack Channel**: #event-service on Traccar Slack
- **Knowledge Base**: https://www.traccar.org/knowledge-base/

## Appendix: Common Error Messages

### Message Broker Errors

- `Connection refused to Kafka broker`: Check network connectivity and broker status
- `Topic not found: enriched.positions`: Verify topic creation and permissions
- `Consumer group rebalance in progress`: Normal during scaling, but excessive rebalancing may indicate issues

### Event Handler Errors

- `Failed to process position for device ID X`: Check device configuration and position data
- `Error accessing cache for geofence ID Y`: Verify cache manager and database connectivity
- `Circuit breaker OPEN for cacheManager`: Indicates persistent failures accessing the cache

### Service Discovery Errors

- `Failed to register with Consul`: Check Consul connectivity and configuration
- `Kubernetes API access denied`: Verify service account permissions
- `Health check failed`: Investigate component-specific health issues

### Performance Warnings

- `Slow event processing detected`: Indicates performance bottlenecks
- `High message consumer lag`: Service not keeping up with incoming messages
- `Memory pressure detected`: JVM memory usage approaching limits