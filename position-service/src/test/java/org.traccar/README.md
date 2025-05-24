# Position Service Test Suite

## Overview

This document provides comprehensive information about the Position Service test suite, including its structure, how to run tests, what they cover, and special considerations for test development and maintenance.

The Position Service is a critical component in Traccar's microservices architecture, responsible for processing, validating, and enriching position data from GPS devices. This test suite ensures the reliability and correctness of these operations.

## Test Structure and Organization

### Test Types

The Position Service test suite includes several types of tests:

1. **Unit Tests**: Test individual components in isolation with mocked dependencies
2. **Integration Tests**: Test interactions between components and with infrastructure
3. **Contract Tests**: Verify service interface compatibility with other services

### Directory Structure

Tests follow the same package structure as the main code:

```
src/test/java/org/traccar/
├── geocoder/           # Tests for geocoding functionality
├── geofence/           # Tests for geofence detection
├── handler/            # Tests for position handlers
│   └── events/         # Tests for event detection handlers
├── forward/            # Tests for position forwarding
├── geolocation/        # Tests for geolocation services
├── config/             # Tests for configuration components
└── messaging/          # Tests for message broker integration
```

### Test Resources

Test resources are organized in the following structure:

```
src/test/resources/
├── config/             # Test configuration files
├── contracts/          # Service contract definitions
├── fixtures/           # Test data fixtures
├── mocks/              # Mock service responses
│   ├── broker/         # Message broker mocks
│   ├── discovery/      # Service discovery mocks
│   ├── geocoding/      # Geocoding service mocks
│   └── notification/   # Notification service mocks
└── db/                 # Database initialization scripts
```

## Running Tests

### Prerequisites

Before running tests, ensure you have:

- JDK 17 or higher installed
- Docker installed (for integration tests using Testcontainers)
- Gradle 8.0+ (or use the included Gradle wrapper)

### Running Tests Locally

#### All Tests

To run all tests:

```bash
./gradlew :position-service:test
```

#### Unit Tests Only

To run only unit tests:

```bash
./gradlew :position-service:test --tests "org.traccar.*Test" --exclude-task integrationTest
```

#### Integration Tests Only

To run only integration tests:

```bash
./gradlew :position-service:integrationTest
```

#### Contract Tests

To run contract tests:

```bash
./gradlew :position-service:contractTest
```

### Running Tests with Docker Compose

For a more comprehensive test environment that includes all dependent services:

```bash
# Start the test environment
docker-compose -f docker-compose.test.yml up -d

# Run tests against the environment
./gradlew :position-service:integrationTest -Dspring.profiles.active=compose-test

# Shut down the environment when done
docker-compose -f docker-compose.test.yml down
```

### Running Tests in CI

Tests are automatically run in CI through GitHub Actions when:

- Code is pushed to the `master` branch
- Code is pushed to branches matching the pattern `position-*`
- A pull request is created targeting the `master` branch

The CI workflow is defined in `.github/workflows/position-service.yml`.

## Test Data and Mocks

### Test Data Factory

The Position Service uses a shared `TestDataFactory` from the common-test module to generate standard test entities:

```java
// Generate a standard test position
Position position = TestDataFactory.createPosition(1, 60.0, 30.0);

// Generate a standard test device
Device device = TestDataFactory.createDevice("test-device");
```

### Mocking Strategy

External dependencies are mocked using Mockito:

```java
// Service client mocking
var deviceServiceClient = mock(DeviceServiceClient.class);
when(deviceServiceClient.getDevice(any(DeviceRequest.class)))
    .thenReturn(DeviceResponse.newBuilder().setId(1).setName("test-device").build());

// Message broker mocking
var kafkaTemplate = mock(KafkaTemplate.class);
doNothing().when(kafkaTemplate).send(eq("positions"), any(PositionMessage.class));
```

### Infrastructure Testing

Testcontainers is used for tests requiring real infrastructure:

```java
@Testcontainers
class DatabaseIntegrationTest {
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:14-alpine")
        .withDatabaseName("traccar")
        .withUsername("test")
        .withPassword("test");
        
    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.0"));
    
    // Test methods using real infrastructure
}
```

## Integration with Other Services

The Position Service interacts with several other services in the Traccar ecosystem:

### Protocol Service

The Position Service consumes position data published by the Protocol Service via the message broker. Contract tests verify compatibility between these services.

### Event Service

The Position Service publishes enriched position data that is consumed by the Event Service. Contract tests ensure the Event Service can correctly interpret the published data.

### Notification Service

Indirectly connected through the Event Service, which may trigger notifications based on position data.

## Special Considerations

### Code Coverage Requirements

The Position Service has a minimum code coverage requirement of 80% line coverage. Coverage is verified during CI builds using JaCoCo, and builds will fail if coverage falls below this threshold.

To check coverage locally:

```bash
./gradlew :position-service:jacocoTestReport
```

The report will be generated at `position-service/build/reports/jacoco/test/html/index.html`.

### Flaky Test Handling

Tests known to be flaky should be explicitly marked with annotations:

```java
@Tag("flaky")
@Flaky(issue = "TRACCAR-1234", description = "Network timing issues")
@Test
void testDeviceCommunicationUnderLoad() {
    // Test implementation
}
```

Flaky tests are run separately in CI and do not cause build failures, but are monitored for frequency of failures.

### Performance Testing

Performance tests for the Position Service verify:

- Position processing rate: ≥ 2000 positions/sec
- 95th percentile end-to-end position processing latency: < 50ms

These tests are run as part of the performance test suite and not with regular unit/integration tests.

## Troubleshooting

### Common Test Issues

1. **Port Conflicts**: If integration tests fail due to port conflicts, you can set custom ports using environment variables:

   ```bash
   export TEST_KAFKA_PORT=9093
   export TEST_POSTGRES_PORT=5433
   ./gradlew :position-service:integrationTest
   ```

2. **Docker Issues**: If Testcontainers fails to start containers, verify Docker is running and has sufficient resources allocated.

3. **Test Data Cleanup**: If tests fail due to leftover test data, you can force a clean environment:

   ```bash
   ./gradlew :position-service:cleanTest :position-service:test
   ```

### Debugging Tests

To run tests with remote debugging enabled:

```bash
./gradlew :position-service:test --debug-jvm
```

Then connect your IDE to port 5005.

## Contributing

When adding or modifying tests:

1. Follow the existing package structure
2. Ensure proper cleanup of test resources
3. Use the TestDataFactory for standard test entities
4. Mock external dependencies appropriately
5. Add appropriate assertions for both positive and negative cases
6. Update this documentation if you introduce significant changes to the test structure or process

## References

- [JUnit 5 Documentation](https://junit.org/junit5/docs/current/user-guide/)
- [Mockito Documentation](https://javadoc.io/doc/org.mockito/mockito-core/latest/org/mockito/Mockito.html)
- [Testcontainers Documentation](https://www.testcontainers.org/)
- [Position Service Architecture](../../main/java/org/traccar/README.md)