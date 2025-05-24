# Mock Discovery Configurations for Position Service

## Overview

This directory contains mock configurations for testing the Position Service's service discovery functionality. These mocks enable unit and integration tests to verify service registration, discovery, and health check interactions without requiring actual service discovery infrastructure (like Consul or Kubernetes) during testing.

## Purpose

In the microservices architecture, Position Service needs to:
1. Register itself with the service discovery mechanism
2. Discover other services it depends on (Protocol Service, Event Service, etc.)
3. Report its health status to the discovery mechanism
4. Handle service discovery failures gracefully

These mock configurations allow developers to test all these scenarios in isolation, ensuring the Position Service correctly implements service discovery patterns.

## Mock Files Structure

| File | Purpose |
|------|--------|
| `consul-mock.json` | Simulates Consul API responses for service registration, catalog queries, and health checks |
| `kubernetes-mock.json` | Simulates Kubernetes Services and Endpoints API responses for service discovery |
| `position-service-registration.json` | Defines the Position Service registration template with service identity, endpoints, and health checks |
| `service-failure-scenarios.json` | Contains predefined failure scenarios for resilience testing |
| `health-check-responses.json` | Provides mock health check responses in various states (healthy, degraded, unhealthy) |
| `service-resolution-scenarios.json` | Defines test scenarios for resolving dependencies through service discovery |

## Usage

### Basic Service Discovery Testing

To test basic service discovery functionality, use the mock configurations as follows:

```java
@Test
public void testServiceDiscovery() {
    // Load mock discovery configuration
    ServiceDiscoveryMock discoveryMock = new ServiceDiscoveryMock(
        "src/test/resources/mocks/discovery/consul-mock.json");
    
    // Create service discovery client with mock
    ServiceDiscoveryClient client = new ServiceDiscoveryClient(discoveryMock);
    
    // Test service resolution
    ServiceInstance instance = client.resolveService("protocol-service");
    
    // Verify correct service was discovered
    assertNotNull(instance);
    assertEquals("protocol-service", instance.getServiceId());
    assertEquals("10.0.0.1", instance.getHost());
    assertEquals(8082, instance.getPort());
}
```

### Testing Service Registration

To test that Position Service correctly registers itself with the discovery mechanism:

```java
@Test
public void testServiceRegistration() {
    // Load mock discovery configuration
    ServiceDiscoveryMock discoveryMock = new ServiceDiscoveryMock();
    
    // Create service discovery client with mock
    ServiceDiscoveryClient client = new ServiceDiscoveryClient(discoveryMock);
    
    // Load expected registration from template
    ServiceRegistration expectedRegistration = loadRegistrationTemplate(
        "src/test/resources/mocks/discovery/position-service-registration.json");
    
    // Register service
    client.register("position-service", "localhost", 8080);
    
    // Verify registration matches expected template
    ServiceRegistration actualRegistration = discoveryMock.getRegisteredService("position-service");
    assertEquals(expectedRegistration.getServiceId(), actualRegistration.getServiceId());
    assertEquals(expectedRegistration.getHealthCheckUrl(), actualRegistration.getHealthCheckUrl());
    // Verify other registration properties...
}
```

### Testing Failure Scenarios

To test how Position Service handles service discovery failures:

```java
@Test
public void testServiceDiscoveryFailure() {
    // Load mock discovery configuration with failure scenario
    ServiceDiscoveryMock discoveryMock = new ServiceDiscoveryMock(
        "src/test/resources/mocks/discovery/service-failure-scenarios.json");
    discoveryMock.activateScenario("service-not-found");
    
    // Create service discovery client with mock
    ServiceDiscoveryClient client = new ServiceDiscoveryClient(discoveryMock);
    
    // Test service resolution with circuit breaker
    CircuitBreaker circuitBreaker = new CircuitBreaker();
    Optional<ServiceInstance> instance = circuitBreaker.executeWithFallback(
        () -> Optional.of(client.resolveService("protocol-service")),
        (e) -> Optional.empty()
    );
    
    // Verify fallback was used
    assertFalse(instance.isPresent());
    assertTrue(circuitBreaker.isOpen());
}
```

### Testing Health Check Reporting

To test health check reporting and monitoring:

```java
@Test
public void testHealthCheckReporting() {
    // Load mock discovery configuration
    ServiceDiscoveryMock discoveryMock = new ServiceDiscoveryMock(
        "src/test/resources/mocks/discovery/health-check-responses.json");
    
    // Create service discovery client with mock
    ServiceDiscoveryClient client = new ServiceDiscoveryClient(discoveryMock);
    
    // Register service with health check
    client.register("position-service", "localhost", 8080);
    
    // Simulate health check execution
    discoveryMock.triggerHealthCheck("position-service");
    
    // Verify health status was reported correctly
    HealthStatus status = discoveryMock.getHealthStatus("position-service");
    assertEquals(HealthStatus.UP, status);
}
```

## Common Test Scenarios

### 1. Service Resolution

Test that Position Service can discover and connect to its dependencies:
- Protocol Service for receiving position data
- Event Service for publishing events
- Notification Service for sending alerts
- Message Broker for asynchronous communication

### 2. Resilience Testing

Test how Position Service handles various failure scenarios:
- Service not found (dependency unavailable)
- Network partitions (intermittent connectivity)
- Timeout conditions (slow responses)
- Service instance failure (instance becomes unhealthy)

### 3. Load Balancing

Test service discovery with multiple instances of the same service:
- Round-robin load balancing
- Health-aware instance selection
- Sticky sessions for stateful interactions

### 4. Health Status Propagation

Test health status reporting and monitoring:
- Liveness checks (is the service running)
- Readiness checks (is the service ready to accept requests)
- Dependency health checks (are required dependencies available)
- Degraded service operation (partial functionality available)

## Extending the Mocks

### Adding New Service Dependencies

To add a new service dependency for testing:

1. Update `consul-mock.json` and `kubernetes-mock.json` with the new service definition:

```json
{
  "services": [
    {
      "id": "new-service-1",
      "name": "new-service",
      "address": "10.0.0.5",
      "port": 8085,
      "tags": ["production", "v1"],
      "meta": {
        "version": "1.0.0"
      }
    }
  ]
}
```

2. Add resolution scenarios in `service-resolution-scenarios.json`:

```json
{
  "scenarios": [
    {
      "name": "resolve-new-service",
      "service": "new-service",
      "instances": [
        {
          "id": "new-service-1",
          "address": "10.0.0.5",
          "port": 8085,
          "healthy": true
        }
      ]
    }
  ]
}
```

### Adding New Failure Scenarios

To add new failure scenarios for testing resilience patterns:

1. Update `service-failure-scenarios.json` with the new scenario:

```json
{
  "scenarios": [
    {
      "name": "intermittent-failure",
      "service": "protocol-service",
      "behavior": {
        "type": "intermittent",
        "failureRate": 0.5,
        "failureMode": "timeout",
        "timeoutMs": 3000
      }
    }
  ]
}
```

### Customizing Health Check Responses

To add custom health check responses:

1. Update `health-check-responses.json` with new health states:

```json
{
  "services": [
    {
      "id": "position-service-1",
      "name": "position-service",
      "checks": [
        {
          "id": "service:position-service-1",
          "name": "Service 'position-service' check",
          "status": "warning",
          "output": "Database connection pool at 80% capacity"
        }
      ]
    }
  ]
}
```

## Relationship to Actual Service Discovery

These mock configurations simulate the behavior of actual service discovery implementations:

- **Consul**: The `consul-mock.json` file mimics Consul's HTTP API responses for service registration, catalog queries, and health checks. It follows the same JSON structure as actual Consul API responses.

- **Kubernetes**: The `kubernetes-mock.json` file simulates Kubernetes Services and Endpoints API responses, allowing tests to verify both DNS-based service resolution and direct API-based service discovery.

The mock implementations allow tests to run without actual infrastructure while still verifying that Position Service correctly implements the service discovery patterns required in the production environment.

## Best Practices

1. **Keep mocks up-to-date**: When service discovery configurations change in production, update the mock files accordingly.

2. **Test both happy and failure paths**: Always include tests for both successful service discovery and various failure scenarios.

3. **Verify resilience patterns**: Use the failure scenarios to verify circuit breakers, retries, and fallbacks work correctly.

4. **Test realistic scenarios**: Configure the mocks to represent realistic service topologies and failure modes.

5. **Extend for new requirements**: As new service discovery requirements emerge, extend the mock configurations to support testing them.