# Protocol Service Troubleshooting Guide

## Table of Contents

1. [Introduction](#introduction)
2. [Connection Issues](#connection-issues)
   - [Device Connection Failures](#device-connection-failures)
   - [Connection Drops](#connection-drops)
   - [Network Configuration Problems](#network-configuration-problems)
   - [SSL/TLS Issues](#ssltls-issues)
3. [Protocol Errors](#protocol-errors)
   - [Message Parsing Failures](#message-parsing-failures)
   - [Protocol Identification Issues](#protocol-identification-issues)
   - [Unsupported Protocol Versions](#unsupported-protocol-versions)
   - [Authentication Failures](#authentication-failures)
4. [Performance Issues](#performance-issues)
   - [High CPU Usage](#high-cpu-usage)
   - [Memory Leaks](#memory-leaks)
   - [Slow Message Processing](#slow-message-processing)
   - [Connection Backlog](#connection-backlog)
5. [Integration Problems](#integration-problems)
   - [Message Broker Connectivity](#message-broker-connectivity)
   - [Service Discovery Issues](#service-discovery-issues)
   - [Database Connectivity](#database-connectivity)
6. [Common Error Messages](#common-error-messages)
   - [Error Message Reference](#error-message-reference)
   - [Resolution Steps](#resolution-steps)
7. [Diagnostic Procedures](#diagnostic-procedures)
   - [Checking Service Health](#checking-service-health)
   - [Analyzing Logs](#analyzing-logs)
   - [Monitoring Metrics](#monitoring-metrics)
   - [Network Diagnostics](#network-diagnostics)
8. [Recovery Procedures](#recovery-procedures)
   - [Service Restart](#service-restart)
   - [Connection Reset](#connection-reset)
   - [Data Recovery](#data-recovery)

## Introduction

This troubleshooting guide provides solutions for common issues encountered with the Protocol Service. The Protocol Service is responsible for handling device communications across 200+ protocols, decoding raw messages, and publishing standardized position data to the message broker for downstream processing.

Use this guide to diagnose and resolve problems related to device connections, protocol handling, performance, and integration with other services.

## Connection Issues

### Device Connection Failures

| Symptom | Possible Causes | Resolution |
|---------|-----------------|------------|
| Devices cannot connect to the service | - Port not open<br>- Firewall blocking<br>- Incorrect protocol port<br>- Service not running | 1. Verify the Protocol Service is running: `kubectl get pods -l app=protocol-service`<br>2. Check port configuration: `kubectl describe service protocol-service`<br>3. Ensure firewall allows traffic: `kubectl describe networkpolicy`<br>4. Verify protocol port mapping in config.yml |
| Connection attempts time out | - Network latency<br>- Service overloaded<br>- Incorrect endpoint | 1. Check network connectivity between device and service<br>2. Verify service has sufficient resources<br>3. Confirm correct IP address and port configuration |
| Connection refused errors | - Service not listening on port<br>- Port conflicts | 1. Verify service is bound to correct ports: `netstat -tulpn \| grep protocol-service`<br>2. Check for port conflicts with other services<br>3. Restart service if necessary |

**Diagnostic Steps:**

1. Check if the Protocol Service is running and healthy:
   ```bash
   kubectl get pods -l app=protocol-service
   kubectl describe pod protocol-service-[pod-id]
   ```

2. Verify the service is exposed on the correct ports:
   ```bash
   kubectl get service protocol-service -o yaml
   ```

3. Test connectivity to the service ports from an external network:
   ```bash
   telnet [service-ip] [protocol-port]
   ```

4. Check service logs for connection attempts:
   ```bash
   kubectl logs -l app=protocol-service --tail=100
   ```

### Connection Drops

| Symptom | Possible Causes | Resolution |
|---------|-----------------|------------|
| Devices disconnect unexpectedly | - Network instability<br>- Idle timeout<br>- Service restart<br>- Protocol errors | 1. Check network stability between devices and service<br>2. Adjust idle timeout settings in config.yml<br>3. Implement reconnection logic on devices<br>4. Check for protocol errors in logs |
| Periodic disconnections | - Keepalive issues<br>- Load balancer timeout<br>- Resource constraints | 1. Configure proper keepalive settings<br>2. Adjust load balancer timeout settings<br>3. Scale up service resources if needed |
| Connections drop during high load | - Resource exhaustion<br>- Connection limits reached<br>- Network congestion | 1. Increase service resources (CPU/memory)<br>2. Scale horizontally by adding more pods<br>3. Optimize network configuration |

**Diagnostic Steps:**

1. Check for patterns in disconnection timing:
   ```bash
   kubectl logs -l app=protocol-service | grep "Connection closed" | head -n 20
   ```

2. Monitor connection count metrics:
   ```bash
   curl http://[service-ip]:8080/actuator/metrics/protocol.connections.active
   ```

3. Check for resource constraints:
   ```bash
   kubectl top pod -l app=protocol-service
   ```

4. Verify network policies and load balancer settings:
   ```bash
   kubectl describe networkpolicy
   kubectl describe service protocol-service
   ```

### Network Configuration Problems

| Symptom | Possible Causes | Resolution |
|---------|-----------------|------------|
| Devices connect but no data flows | - Incorrect protocol selection<br>- Firewall blocking data<br>- NAT issues | 1. Verify protocol configuration<br>2. Check firewall rules for data traffic<br>3. Test with simplified network configuration |
| Intermittent connectivity | - DNS resolution issues<br>- Network instability<br>- Load balancer problems | 1. Verify DNS resolution<br>2. Check network stability<br>3. Inspect load balancer logs |
| One-way communication | - Asymmetric routing<br>- Firewall rules<br>- NAT configuration | 1. Verify bidirectional network path<br>2. Check firewall for return traffic<br>3. Test with direct connection |

**Diagnostic Steps:**

1. Test basic connectivity with ping and traceroute:
   ```bash
   ping [device-ip]
   traceroute [device-ip]
   ```

2. Capture network traffic for analysis:
   ```bash
   kubectl exec -it [protocol-service-pod] -- tcpdump -i eth0 -n port [protocol-port]
   ```

3. Check for network policy restrictions:
   ```bash
   kubectl describe networkpolicy
   ```

### SSL/TLS Issues

| Symptom | Possible Causes | Resolution |
|---------|-----------------|------------|
| SSL handshake failures | - Certificate issues<br>- Protocol version mismatch<br>- Cipher suite incompatibility | 1. Verify certificate validity and trust chain<br>2. Check supported TLS versions<br>3. Ensure compatible cipher suites |
| Certificate validation errors | - Expired certificates<br>- Hostname mismatch<br>- Untrusted CA | 1. Renew expired certificates<br>2. Ensure certificate matches hostname<br>3. Add CA to trusted store |
| Secure connection performance issues | - TLS session resumption disabled<br>- Inefficient cipher selection<br>- Missing hardware acceleration | 1. Enable TLS session resumption<br>2. Configure efficient cipher preferences<br>3. Use hardware acceleration if available |

**Diagnostic Steps:**

1. Check certificate validity:
   ```bash
   openssl x509 -in [certificate.pem] -text -noout
   ```

2. Test SSL/TLS handshake:
   ```bash
   openssl s_client -connect [service-ip]:[protocol-port]
   ```

3. Verify TLS configuration in service:
   ```bash
   kubectl exec -it [protocol-service-pod] -- cat /etc/protocol-service/config.yml | grep -A 10 "ssl:"
   ```

## Protocol Errors

### Message Parsing Failures

| Symptom | Possible Causes | Resolution |
|---------|-----------------|------------|
| "Cannot parse message" errors | - Corrupted data<br>- Protocol mismatch<br>- Unsupported message format | 1. Verify device is sending correct protocol format<br>2. Check for data corruption in transmission<br>3. Update protocol decoder if needed |
| Partial message processing | - Fragmented messages<br>- Buffer size issues<br>- Incomplete implementation | 1. Adjust buffer sizes in config.yml<br>2. Ensure complete message transmission<br>3. Update protocol implementation |
| Checksum validation failures | - Data corruption<br>- Incorrect checksum algorithm<br>- Implementation error | 1. Verify data integrity<br>2. Check checksum algorithm implementation<br>3. Compare with protocol specification |

**Diagnostic Steps:**

1. Enable debug logging for the specific protocol:
   ```bash
   kubectl exec -it [protocol-service-pod] -- curl -X POST http://localhost:8080/actuator/loggers/org.traccar.protocol.[protocol-name] -H 'Content-Type: application/json' -d '{"configuredLevel": "DEBUG"}'
   ```

2. Capture raw message data:
   ```bash
   kubectl logs -l app=protocol-service | grep "raw:" | tail -n 20
   ```

3. Verify message format against protocol specification.

### Protocol Identification Issues

| Symptom | Possible Causes | Resolution |
|---------|-----------------|------------|
| "Unknown protocol" errors | - Protocol not supported<br>- Incorrect port configuration<br>- Message format issues | 1. Verify protocol is supported<br>2. Check port-to-protocol mapping<br>3. Ensure correct message format |
| Protocol misidentification | - Similar protocol formats<br>- Ambiguous headers<br>- Configuration issues | 1. Use dedicated ports for each protocol<br>2. Ensure unique protocol identifiers<br>3. Update protocol detection logic |
| Protocol negotiation failures | - Version incompatibility<br>- Missing negotiation steps<br>- Timeout issues | 1. Verify protocol version compatibility<br>2. Ensure complete negotiation sequence<br>3. Adjust timeout settings |

**Diagnostic Steps:**

1. Check protocol-to-port mapping configuration:
   ```bash
   kubectl exec -it [protocol-service-pod] -- cat /etc/protocol-service/config.yml | grep -A 20 "protocols:"
   ```

2. Monitor protocol identification attempts:
   ```bash
   kubectl logs -l app=protocol-service | grep "protocol identification"
   ```

3. Test connection with known protocol data:
   ```bash
   echo -ne "[protocol-specific-data]" | nc [service-ip] [protocol-port]
   ```

### Unsupported Protocol Versions

| Symptom | Possible Causes | Resolution |
|---------|-----------------|------------|
| "Unsupported protocol version" errors | - Device using newer/older version<br>- Protocol implementation outdated<br>- Configuration mismatch | 1. Update protocol implementation<br>2. Configure device to use supported version<br>3. Check version compatibility |
| Partial functionality | - Version-specific features<br>- Incomplete implementation<br>- Configuration issues | 1. Implement missing features<br>2. Configure to use common feature set<br>3. Update documentation on limitations |
| Protocol extension issues | - Custom extensions not supported<br>- Vendor-specific features<br>- Non-standard implementations | 1. Implement custom extensions<br>2. Document vendor-specific handling<br>3. Create protocol adapter if needed |

**Diagnostic Steps:**

1. Identify protocol version from logs:
   ```bash
   kubectl logs -l app=protocol-service | grep -i "version"
   ```

2. Check supported versions in implementation:
   ```bash
   kubectl exec -it [protocol-service-pod] -- find /app -name "*Protocol*.java" -exec grep -l "version" {} \;
   ```

3. Compare device documentation with implementation capabilities.

### Authentication Failures

| Symptom | Possible Causes | Resolution |
|---------|-----------------|------------|
| "Authentication failed" errors | - Incorrect credentials<br>- Device not registered<br>- Authentication method mismatch | 1. Verify device credentials<br>2. Ensure device is registered in system<br>3. Check authentication method configuration |
| Session rejection | - Session limit reached<br>- IP filtering<br>- Rate limiting | 1. Increase session limits<br>2. Verify IP whitelist configuration<br>3. Adjust rate limiting settings |
| Intermittent authentication issues | - Credential caching problems<br>- Database connectivity<br>- Race conditions | 1. Check credential caching mechanism<br>2. Verify database connectivity<br>3. Implement proper synchronization |

**Diagnostic Steps:**

1. Check authentication attempts in logs:
   ```bash
   kubectl logs -l app=protocol-service | grep -i "auth"
   ```

2. Verify device registration in database:
   ```bash
   kubectl exec -it [database-pod] -- psql -U postgres -d traccar -c "SELECT * FROM devices WHERE uniqueid = '[device-id]';"
   ```

3. Test authentication with known credentials:
   ```bash
   # Protocol-specific authentication test
   ```

## Performance Issues

### High CPU Usage

| Symptom | Possible Causes | Resolution |
|---------|-----------------|------------|
| Sustained high CPU utilization | - Too many connections<br>- Inefficient protocol parsing<br>- Resource constraints | 1. Scale horizontally by adding more pods<br>2. Optimize protocol parsers<br>3. Increase CPU allocation |
| CPU spikes | - Message bursts<br>- Garbage collection<br>- Background tasks | 1. Implement rate limiting<br>2. Tune JVM garbage collection<br>3. Optimize background task scheduling |
| Single-core bottleneck | - Thread contention<br>- Sequential processing<br>- Lock contention | 1. Improve parallelization<br>2. Reduce lock contention<br>3. Distribute workload across cores |

**Diagnostic Steps:**

1. Monitor CPU usage over time:
   ```bash
   kubectl top pod -l app=protocol-service --containers
   ```

2. Check for CPU throttling:
   ```bash
   kubectl exec -it [protocol-service-pod] -- cat /sys/fs/cgroup/cpu/cpu.stat
   ```

3. Analyze thread dumps during high CPU usage:
   ```bash
   kubectl exec -it [protocol-service-pod] -- jstack 1 > thread_dump.txt
   ```

4. Profile the application if possible:
   ```bash
   # Using JMX or other profiling tools
   ```

### Memory Leaks

| Symptom | Possible Causes | Resolution |
|---------|-----------------|------------|
| Increasing memory usage over time | - Session objects not released<br>- Buffer leaks<br>- Cache growth | 1. Audit session cleanup code<br>2. Ensure proper buffer release<br>3. Set cache size limits |
| OutOfMemoryError exceptions | - Memory leak<br>- Insufficient heap size<br>- Large message buffers | 1. Increase heap size<br>2. Fix memory leaks<br>3. Limit buffer sizes |
| High garbage collection activity | - Object churn<br>- Memory pressure<br>- Inefficient object creation | 1. Optimize object creation/reuse<br>2. Tune garbage collection<br>3. Increase memory allocation |

**Diagnostic Steps:**

1. Monitor memory usage over time:
   ```bash
   kubectl exec -it [protocol-service-pod] -- jstat -gcutil 1 1000 10
   ```

2. Check for memory leaks with heap dumps:
   ```bash
   kubectl exec -it [protocol-service-pod] -- jmap -dump:format=b,file=/tmp/heap.bin 1
   kubectl cp [protocol-service-pod]:/tmp/heap.bin ./heap.bin
   # Analyze with tools like Eclipse MAT
   ```

3. Monitor active sessions and connections:
   ```bash
   curl http://[service-ip]:8080/actuator/metrics/protocol.sessions.active
   ```

### Slow Message Processing

| Symptom | Possible Causes | Resolution |
|---------|-----------------|------------|
| Increasing message queue depth | - Processing bottlenecks<br>- Resource constraints<br>- Downstream service issues | 1. Optimize message processing<br>2. Scale up resources<br>3. Check downstream service health |
| High message latency | - Network latency<br>- Processing delays<br>- Queue backlog | 1. Optimize network configuration<br>2. Improve processing efficiency<br>3. Increase processing capacity |
| Batch processing delays | - Large batch sizes<br>- Sequential processing<br>- Resource contention | 1. Reduce batch sizes<br>2. Implement parallel processing<br>3. Eliminate resource contention |

**Diagnostic Steps:**

1. Monitor message processing metrics:
   ```bash
   curl http://[service-ip]:8080/actuator/metrics/protocol.messages.processed
   curl http://[service-ip]:8080/actuator/metrics/protocol.messages.processing_time
   ```

2. Check for bottlenecks in processing pipeline:
   ```bash
   kubectl logs -l app=protocol-service | grep "processing time"
   ```

3. Verify downstream service health:
   ```bash
   curl http://[service-ip]:8080/actuator/health
   ```

### Connection Backlog

| Symptom | Possible Causes | Resolution |
|---------|-----------------|------------|
| Increasing connection queue | - Connection rate too high<br>- Slow connection processing<br>- Resource exhaustion | 1. Increase connection processing threads<br>2. Scale horizontally<br>3. Implement connection rate limiting |
| Connection timeouts | - Backlog queue full<br>- Processing delays<br>- Network issues | 1. Increase backlog queue size<br>2. Optimize connection handling<br>3. Check for network bottlenecks |
| Rejected connections | - Connection limits reached<br>- Resource constraints<br>- DoS protection | 1. Increase connection limits<br>2. Scale up resources<br>3. Review DoS protection settings |

**Diagnostic Steps:**

1. Check connection backlog:
   ```bash
   kubectl exec -it [protocol-service-pod] -- netstat -s | grep -i "listen"
   ```

2. Monitor connection rate:
   ```bash
   curl http://[service-ip]:8080/actuator/metrics/protocol.connections.rate
   ```

3. Verify connection limits:
   ```bash
   kubectl exec -it [protocol-service-pod] -- cat /proc/sys/net/core/somaxconn
   ```

## Integration Problems

### Message Broker Connectivity

| Symptom | Possible Causes | Resolution |
|---------|-----------------|------------|
| Failed to publish messages | - Broker unavailable<br>- Authentication issues<br>- Topic configuration | 1. Verify broker health<br>2. Check credentials<br>3. Ensure topics exist and are accessible |
| Message backlog | - Broker performance issues<br>- Network congestion<br>- Consumer slowness | 1. Scale broker resources<br>2. Optimize network<br>3. Check consumer performance |
| Intermittent connectivity | - Network issues<br>- Broker restarts<br>- Connection pool exhaustion | 1. Implement connection retry logic<br>2. Configure connection pooling<br>3. Monitor broker stability |

**Diagnostic Steps:**

1. Check broker connectivity:
   ```bash
   kubectl exec -it [protocol-service-pod] -- curl -v telnet://[broker-host]:[broker-port]
   ```

2. Verify broker health:
   ```bash
   kubectl describe pod -l app=kafka # or rabbitmq
   ```

3. Monitor message publication metrics:
   ```bash
   curl http://[service-ip]:8080/actuator/metrics/protocol.messages.published
   ```

### Service Discovery Issues

| Symptom | Possible Causes | Resolution |
|---------|-----------------|------------|
| Service registration failures | - Discovery service unavailable<br>- Network issues<br>- Configuration problems | 1. Verify discovery service health<br>2. Check network connectivity<br>3. Validate registration configuration |
| Service resolution failures | - Service not registered<br>- Stale cache<br>- DNS issues | 1. Ensure service is registered<br>2. Clear discovery cache<br>3. Check DNS resolution |
| Intermittent discovery issues | - Discovery service instability<br>- Network flakiness<br>- Race conditions | 1. Monitor discovery service<br>2. Implement retry logic<br>3. Use fallback mechanisms |

**Diagnostic Steps:**

1. Check service registration status:
   ```bash
   # For Kubernetes
   kubectl get endpoints protocol-service
   
   # For Consul
   curl http://[consul-ip]:8500/v1/catalog/service/protocol-service
   ```

2. Verify discovery client configuration:
   ```bash
   kubectl exec -it [protocol-service-pod] -- cat /etc/protocol-service/config.yml | grep -A 10 "discovery:"
   ```

3. Test service resolution:
   ```bash
   kubectl exec -it [protocol-service-pod] -- nslookup protocol-service.default.svc.cluster.local
   ```

### Database Connectivity

| Symptom | Possible Causes | Resolution |
|---------|-----------------|------------|
| Database connection failures | - Database unavailable<br>- Authentication issues<br>- Connection limits | 1. Verify database health<br>2. Check credentials<br>3. Increase connection limits |
| Slow database operations | - Query performance<br>- Resource constraints<br>- Lock contention | 1. Optimize queries<br>2. Scale database resources<br>3. Reduce lock contention |
| Connection pool exhaustion | - Pool size too small<br>- Connection leaks<br>- Long-running transactions | 1. Increase pool size<br>2. Fix connection leaks<br>3. Optimize transaction duration |

**Diagnostic Steps:**

1. Check database connectivity:
   ```bash
   kubectl exec -it [protocol-service-pod] -- pg_isready -h [db-host] -p [db-port] -U [db-user]
   ```

2. Monitor connection pool metrics:
   ```bash
   curl http://[service-ip]:8080/actuator/metrics/hikaricp.connections
   ```

3. Verify database health:
   ```bash
   kubectl describe pod -l app=postgresql
   ```

## Common Error Messages

### Error Message Reference

| Error Message | Description | Possible Causes |
|---------------|-------------|------------------|
| `Cannot decode message` | Protocol decoder failed to parse the message | - Corrupted data<br>- Unsupported message format<br>- Protocol version mismatch |
| `Device not found` | Device identifier not registered in the system | - Device not provisioned<br>- Incorrect device ID<br>- Database synchronization issues |
| `Connection limit exceeded` | Maximum number of connections reached | - Too many devices<br>- Connection leaks<br>- Resource constraints |
| `Message broker unavailable` | Cannot connect to message broker | - Broker down<br>- Network issues<br>- Authentication failure |
| `Protocol not supported` | Requested protocol is not implemented | - Misconfiguration<br>- Missing protocol implementation<br>- Wrong port mapping |
| `Buffer overflow` | Message exceeds maximum buffer size | - Malformed message<br>- Buffer size too small<br>- DoS attack |
| `Authentication failed` | Device authentication unsuccessful | - Wrong credentials<br>- Expired credentials<br>- Authentication method mismatch |
| `Session expired` | Device session no longer valid | - Timeout<br>- Server restart<br>- Session eviction |
| `Cannot publish position` | Failed to publish position to broker | - Broker issues<br>- Message format problems<br>- Queue full |
| `Out of memory` | Service has exhausted available memory | - Memory leak<br>- Insufficient resources<br>- Large message handling |

### Resolution Steps

#### Cannot decode message

1. Enable debug logging for the specific protocol:
   ```bash
   kubectl exec -it [protocol-service-pod] -- curl -X POST http://localhost:8080/actuator/loggers/org.traccar.protocol.[protocol-name] -H 'Content-Type: application/json' -d '{"configuredLevel": "DEBUG"}'
   ```

2. Capture and analyze the raw message data:
   ```bash
   kubectl logs -l app=protocol-service | grep "raw:" | tail -n 20
   ```

3. Verify the message format against the protocol specification.

4. Check for protocol version compatibility between device and server.

5. Update the protocol decoder if necessary.

#### Device not found

1. Verify the device is registered in the database:
   ```bash
   kubectl exec -it [database-pod] -- psql -U postgres -d traccar -c "SELECT * FROM devices WHERE uniqueid = '[device-id]';"
   ```

2. Check the device identifier being sent by the device:
   ```bash
   kubectl logs -l app=protocol-service | grep "[device-id]" | grep "identification"
   ```

3. Ensure the device is using the correct identifier format.

4. Register the device if it's not already in the system.

#### Connection limit exceeded

1. Check current connection count:
   ```bash
   curl http://[service-ip]:8080/actuator/metrics/protocol.connections.active
   ```

2. Verify connection limit configuration:
   ```bash
   kubectl exec -it [protocol-service-pod] -- cat /etc/protocol-service/config.yml | grep -A 5 "connection:"
   ```

3. Scale the Protocol Service horizontally:
   ```bash
   kubectl scale deployment protocol-service --replicas=3
   ```

4. Increase connection limit if resources allow:
   ```bash
   # Update config map with higher limits
   kubectl edit configmap protocol-service-config
   ```

#### Message broker unavailable

1. Check broker health:
   ```bash
   kubectl get pods -l app=kafka # or rabbitmq
   kubectl describe pod -l app=kafka # or rabbitmq
   ```

2. Verify network connectivity to broker:
   ```bash
   kubectl exec -it [protocol-service-pod] -- curl -v telnet://[broker-host]:[broker-port]
   ```

3. Check broker credentials and configuration:
   ```bash
   kubectl exec -it [protocol-service-pod] -- cat /etc/protocol-service/config.yml | grep -A 10 "broker:"
   ```

4. Restart the broker if necessary:
   ```bash
   kubectl rollout restart statefulset kafka # or rabbitmq
   ```

## Diagnostic Procedures

### Checking Service Health

1. Verify the Protocol Service is running:
   ```bash
   kubectl get pods -l app=protocol-service
   ```

2. Check the health endpoint:
   ```bash
   curl http://[service-ip]:8080/actuator/health
   ```

3. Inspect detailed health components:
   ```bash
   curl http://[service-ip]:8080/actuator/health/liveness
   curl http://[service-ip]:8080/actuator/health/readiness
   ```

4. Check resource utilization:
   ```bash
   kubectl top pod -l app=protocol-service
   ```

### Analyzing Logs

1. View recent logs:
   ```bash
   kubectl logs -l app=protocol-service --tail=100
   ```

2. Search for specific error patterns:
   ```bash
   kubectl logs -l app=protocol-service | grep -i "error"
   kubectl logs -l app=protocol-service | grep -i "exception"
   kubectl logs -l app=protocol-service | grep -i "fail"
   ```

3. Follow logs in real-time:
   ```bash
   kubectl logs -l app=protocol-service -f
   ```

4. Adjust log levels for detailed debugging:
   ```bash
   kubectl exec -it [protocol-service-pod] -- curl -X POST http://localhost:8080/actuator/loggers/org.traccar -H 'Content-Type: application/json' -d '{"configuredLevel": "DEBUG"}'
   ```

### Monitoring Metrics

1. View available metrics:
   ```bash
   curl http://[service-ip]:8080/actuator/metrics
   ```

2. Check connection metrics:
   ```bash
   curl http://[service-ip]:8080/actuator/metrics/protocol.connections.active
   curl http://[service-ip]:8080/actuator/metrics/protocol.connections.rate
   ```

3. Monitor message processing:
   ```bash
   curl http://[service-ip]:8080/actuator/metrics/protocol.messages.received
   curl http://[service-ip]:8080/actuator/metrics/protocol.messages.processed
   curl http://[service-ip]:8080/actuator/metrics/protocol.messages.published
   ```

4. Check JVM metrics:
   ```bash
   curl http://[service-ip]:8080/actuator/metrics/jvm.memory.used
   curl http://[service-ip]:8080/actuator/metrics/jvm.threads.live
   curl http://[service-ip]:8080/actuator/metrics/system.cpu.usage
   ```

### Network Diagnostics

1. Check network connectivity:
   ```bash
   kubectl exec -it [protocol-service-pod] -- ping -c 4 [target-ip]
   ```

2. Trace network route:
   ```bash
   kubectl exec -it [protocol-service-pod] -- traceroute [target-ip]
   ```

3. Inspect network interfaces:
   ```bash
   kubectl exec -it [protocol-service-pod] -- ip addr
   ```

4. Monitor network traffic:
   ```bash
   kubectl exec -it [protocol-service-pod] -- tcpdump -i eth0 -n port [protocol-port]
   ```

5. Check DNS resolution:
   ```bash
   kubectl exec -it [protocol-service-pod] -- nslookup [service-name]
   ```

## Recovery Procedures

### Service Restart

1. Restart a specific pod:
   ```bash
   kubectl delete pod [protocol-service-pod]
   ```

2. Restart the entire deployment:
   ```bash
   kubectl rollout restart deployment protocol-service
   ```

3. Perform a rolling restart:
   ```bash
   kubectl rollout restart deployment protocol-service --max-unavailable=25%
   ```

4. Monitor restart progress:
   ```bash
   kubectl rollout status deployment protocol-service
   ```

### Connection Reset

1. Identify problematic connections:
   ```bash
   kubectl exec -it [protocol-service-pod] -- netstat -tnp | grep ESTABLISHED
   ```

2. Reset specific connections:
   ```bash
   kubectl exec -it [protocol-service-pod] -- ss -K dst [device-ip] dport = [protocol-port]
   ```

3. Force device reconnection by restarting the service (use with caution):
   ```bash
   kubectl rollout restart deployment protocol-service
   ```

### Data Recovery

1. Check for message backlog in broker:
   ```bash
   # For Kafka
   kubectl exec -it [kafka-pod] -- kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group protocol-service
   
   # For RabbitMQ
   kubectl exec -it [rabbitmq-pod] -- rabbitmqctl list_queues name messages_ready messages_unacknowledged
   ```

2. Reprocess messages from dead letter queue:
   ```bash
   # Implementation-specific command to move messages from DLQ to main queue
   ```

3. Verify data consistency after recovery:
   ```bash
   # Check position counts and timestamps
   kubectl exec -it [database-pod] -- psql -U postgres -d traccar -c "SELECT count(*), min(devicetime), max(devicetime) FROM positions WHERE devicetime > now() - interval '1 day';"
   ```