# Mock Discovery Configurations

## Overview

This directory contains mock configurations for testing Protocol Service's service discovery functionality. These mocks enable comprehensive testing of service registration, discovery, health checking, and resilience patterns without requiring actual service discovery infrastructure (Consul or Kubernetes) during unit and integration tests.

## Purpose

Service discovery is a critical component of Traccar's microservices architecture, allowing Protocol Service to:

- Register itself with the service registry during startup
- Discover and communicate with other services dynamically
- Report health status and receive health updates from dependencies
- Implement resilience patterns for handling service failures

These mock configurations provide a controlled environment for testing these capabilities, ensuring Protocol Service correctly implements service discovery patterns in isolation before integration with the full system.

## File Structure

This directory contains the following mock configuration files:

| File | Purpose |
|------|--------|
| `service-resolution-scenarios.json` | Defines test scenarios for resolving various service dependencies |
| `health-check-responses.json` | Provides mock health check responses in various states |
| `service-failure-scenarios.json` | Contains predefined failure scenarios for resilience testing |
| `protocol-service-registration.json` | Defines the Protocol Service registration template |
| `kubernetes-mock.json` | Contains mock Kubernetes API responses for service discovery |
| `consul-mock.json` | Provides mock Consul API responses for service discovery |

## Relationship to Actual Service Discovery

In production, Protocol Service interacts with service discovery through two primary mechanisms:

1. **Consul-based Discovery**: Protocol Service registers with Consul during startup and queries the Consul catalog to locate other services. Health checks are registered with Consul and used for service routing decisions.

2. **Kubernetes-based Discovery**: In Kubernetes environments, Protocol Service uses either DNS-based resolution (`service-name.namespace.svc.cluster.local`) or direct API queries to the Kubernetes API server to discover service endpoints.

These mock configurations simulate both approaches, allowing tests to verify correct behavior without requiring actual Consul or Kubernetes infrastructure.

### Mapping to Production Configuration

| Mock File | Production Equivalent |
|-----------|----------------------|
| `protocol-service-registration.json` | `deployment/consul/services/protocol-service.json` |
| `kubernetes-mock.json` | `deployment/kubernetes/protocol-service.yaml` |
| `consul-mock.json` | Consul HTTP API responses |

## Usage Examples

### Basic Service Resolution Testing

To test that Protocol Service correctly resolves dependencies:

```java
@Test
void testPositionServiceDiscovery() {
    // Configure mock discovery to return predefined service endpoints
    MockDiscovery mockDiscovery = new MockDiscovery("service-resolution-scenarios.json");
    mockDiscovery.setScenario("position-service-normal");
    
    // Create Protocol Service with mock discovery
    ProtocolService service = new ProtocolService(mockDiscovery);
    
    // Verify service correctly resolves Position Service endpoint
    PositionServiceClient client = service.getPositionServiceClient();
    assertNotNull(client);
    assertEquals("position-service:9090", client.getEndpoint());
}
```

### Health Check Testing

To test Protocol Service's health reporting and monitoring:

```java
@Test
void testHealthCheckReporting() {
    // Configure mock discovery with health check responses
    MockDiscovery mockDiscovery = new MockDiscovery("health-check-responses.json");
    mockDiscovery.setHealthCheckResponse("protocol-service", "healthy");
    
    // Create Protocol Service with mock discovery
    ProtocolService service = new ProtocolService(mockDiscovery);
    
    // Verify service reports correct health status
    HealthStatus status = service.checkHealth();
    assertTrue(status.isHealthy());
    assertEquals("UP", status.getStatus());
}
```

### Resilience Testing

To test Protocol Service's behavior during service discovery failures:

```java
@Test
void testServiceDiscoveryResilience() {
    // Configure mock discovery with failure scenario
    MockDiscovery mockDiscovery = new MockDiscovery("service-failure-scenarios.json");
    mockDiscovery.setScenario("position-service-not-found");
    
    // Create Protocol Service with mock discovery
    ProtocolService service = new ProtocolService(mockDiscovery);
    
    // Verify service handles discovery failure gracefully
    PositionServiceClient client = service.getPositionServiceClient();
    
    // Should use circuit breaker and fallback
    Position position = client.processPosition(createTestPosition());
    assertNotNull(position); // Fallback should return a valid position
    assertTrue(position.hasAttribute("processed_by_fallback"));
}
```

## Common Test Scenarios

### Service Resolution Scenarios

The `service-resolution-scenarios.json` file contains predefined scenarios for testing service resolution:

- **Normal Resolution**: Basic service discovery with healthy services
- **Multiple Instances**: Discovery with multiple service instances for load balancing
- **Service Filtering**: Resolution with service filtering by tags or metadata
- **Cross-Region Discovery**: Resolution across different regions or availability zones

### Health Check Scenarios

The `health-check-responses.json` file provides various health status responses:

- **Healthy Service**: All components functioning normally
- **Degraded Service**: Service operating with reduced capabilities
- **Unhealthy Service**: Service completely unavailable
- **Dependency Failures**: Service healthy but dependencies failing
- **Partial Health**: Some components healthy, others failing

### Failure Scenarios

The `service-failure-scenarios.json` file defines failure patterns for resilience testing:

- **Service Not Found**: Service missing from discovery
- **Network Partition**: Network failures between services
- **Timeout Conditions**: Services responding slowly
- **Intermittent Failures**: Services failing sporadically
- **Cascading Failures**: Multiple dependent services failing

## Testing Circuit Breaker Patterns

Protocol Service implements circuit breaker patterns using Resilience4j to prevent cascading failures. These mock configurations enable testing of circuit breaker behavior:

```java
@Test
void testCircuitBreakerTripping() {
    // Configure mock discovery with intermittent failure scenario
    MockDiscovery mockDiscovery = new MockDiscovery("service-failure-scenarios.json");
    mockDiscovery.setScenario("position-service-intermittent");
    
    // Create Protocol Service with mock discovery
    ProtocolService service = new ProtocolService(mockDiscovery);
    PositionServiceClient client = service.getPositionServiceClient();
    
    // Generate failures to trip circuit breaker
    for (int i = 0; i < 10; i++) {
        try {
            client.processPosition(createTestPosition());
        } catch (Exception e) {
            // Expected exceptions during failure scenario
        }
    }
    
    // Verify circuit breaker is now open
    CircuitBreaker circuitBreaker = service.getCircuitBreaker("positionService");
    assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getState());
    
    // Verify fallback is used when circuit is open
    Position position = client.processPosition(createTestPosition());
    assertTrue(position.hasAttribute("processed_by_fallback"));
}
```

## Testing Service Registration

To test that Protocol Service correctly registers itself with service discovery:

```java
@Test
void testServiceRegistration() {
    // Configure mock discovery with registration template
    MockDiscovery mockDiscovery = new MockDiscovery("protocol-service-registration.json");
    
    // Create Protocol Service with mock discovery
    ProtocolService service = new ProtocolService(mockDiscovery);
    service.start();
    
    // Verify service registered correctly
    ServiceRegistration registration = mockDiscovery.getRegisteredService("protocol-service");
    assertNotNull(registration);
    assertEquals("protocol-service", registration.getName());
    assertTrue(registration.getTags().contains("protocol-handler"));
    
    // Verify health check was registered
    HealthCheck healthCheck = registration.getHealthCheck();
    assertEquals("/actuator/health/liveness", healthCheck.getHttpPath());
}
```

## Extending Mock Configurations

### Adding New Service Resolution Scenarios

To add a new service resolution scenario, edit `service-resolution-scenarios.json` and add a new scenario definition:

```json
{
  "scenarios": [
    {
      "name": "new-service-scenario",
      "services": [
        {
          "name": "new-service",
          "instances": [
            {
              "id": "new-service-1",
              "address": "10.0.0.1",
              "port": 8080,
              "healthy": true,
              "metadata": {
                "version": "1.0.0",
                "region": "us-east-1"
              }
            }
          ]
        }
      ]
    }
  ]
}
```

### Creating Custom Health Check Responses

To add custom health check responses, edit `health-check-responses.json`:

```json
{
  "responses": [
    {
      "name": "custom-health-state",
      "status": "DEGRADED",
      "components": [
        {
          "name": "database",
          "status": "UP"
        },
        {
          "name": "message-broker",
          "status": "DOWN",
          "details": {
            "error": "Connection refused",
            "since": "2023-06-01T12:00:00Z"
          }
        }
      ]
    }
  ]
}
```

### Adding New Failure Scenarios

To create new failure scenarios, edit `service-failure-scenarios.json`:

```json
{
  "scenarios": [
    {
      "name": "custom-failure-scenario",
      "description": "Service responds with 503 after 5 successful requests",
      "service": "target-service",
      "behavior": {
        "type": "count-based",
        "successCount": 5,
        "thenFailWith": {
          "statusCode": 503,
          "response": {
            "error": "Service Unavailable",
            "message": "Service is temporarily unavailable"
          }
        },
        "failureDuration": 30,
        "thenRecover": true
      }
    }
  ]
}
```

## Best Practices

1. **Keep Mocks in Sync**: When service interfaces change, update the corresponding mock configurations to ensure tests remain valid.

2. **Test Both Discovery Mechanisms**: Include tests for both Consul and Kubernetes discovery patterns to ensure compatibility with different deployment environments.

3. **Validate Resilience Patterns**: Always test failure scenarios to verify circuit breakers, retries, and fallbacks work correctly.

4. **Match Production Configuration**: Keep mock configurations aligned with production service discovery settings to catch configuration issues early.

5. **Test Health Reporting**: Verify both health reporting (Protocol Service's own health) and health consumption (reacting to dependency health).

## Troubleshooting

### Common Issues

1. **Mock Not Found**: Ensure the mock configuration files are in the correct location and properly referenced in tests.

2. **Scenario Not Found**: Verify the scenario name matches exactly what's defined in the configuration file.

3. **Unexpected Service Behavior**: Check that the mock configuration accurately represents the expected service behavior.

4. **Circuit Breaker Not Tripping**: Ensure failure thresholds match the test scenario configuration.

### Debugging Tips

1. Enable debug logging for service discovery components:

```properties
logging.level.org.traccar.discovery=DEBUG
logging.level.org.traccar.resilience=DEBUG
```

2. Use the `MockDiscovery.setVerboseLogging(true)` method to see detailed mock interactions.

3. Inspect the actual requests and responses between Protocol Service and the mock discovery service.

## Conclusion

These mock discovery configurations provide a powerful framework for testing Protocol Service's service discovery capabilities in isolation. By using these mocks, you can verify correct service registration, discovery, health reporting, and resilience patterns without requiring actual service discovery infrastructure.

For additional information, refer to the Protocol Service documentation and the service discovery implementation details in the codebase.