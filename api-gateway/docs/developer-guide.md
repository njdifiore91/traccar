# API Gateway Developer Guide

## Table of Contents

1. [Introduction](#introduction)
2. [Architecture Overview](#architecture-overview)
3. [Code Organization](#code-organization)
4. [Development Environment Setup](#development-environment-setup)
5. [Extension Points](#extension-points)
6. [Testing Guidelines](#testing-guidelines)
7. [Contribution Workflow](#contribution-workflow)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)
10. [References](#references)

> **Note**: This guide is for developers working with the API Gateway service in the Traccar microservices architecture. For operational procedures, see the [Operations Guide](operations.md). For security details, see the [Security Documentation](security.md).

## Introduction

The API Gateway service is a critical component in the Traccar microservices architecture, serving as the unified entry point for all client requests. This developer guide provides comprehensive information for developers who need to modify, extend, or maintain the API Gateway service.

### Purpose and Responsibilities

The API Gateway is responsible for:

- **Request Routing**: Directing API calls to the appropriate microservice based on the resource type
- **WebSocket Management**: Handling real-time connections for position updates and events
- **Authentication & Authorization**: Centralizing security enforcement before forwarding requests
- **API Unification**: Providing a consistent API surface for clients regardless of backend implementation
- **Backward Compatibility**: Ensuring existing clients continue to function with the new microservices architecture
- **Response Aggregation**: Combining results from multiple microservices when needed
- **Circuit Breaking**: Preventing cascading failures when downstream services are unavailable

### Technology Stack

The API Gateway is built on the following core technologies:

- **Jetty 11.0.24**: High-performance HTTP server and servlet container
- **Jersey 3.1.10**: JAX-RS implementation for RESTful web services
- **Jackson 2.18.2**: JSON processing library for serialization/deserialization
- **JWT**: JSON Web Tokens for authentication
- **WebSocket**: For real-time bidirectional communication
- **OpenTelemetry**: For distributed tracing and metrics

This guide will help you understand the internal structure of the API Gateway, how to set up your development environment, and how to extend its functionality while maintaining compatibility and following best practices.

## Architecture Overview

The API Gateway service implements a facade pattern, providing a single entry point for all client interactions with the Traccar system. It abstracts the underlying microservices architecture from clients, allowing them to interact with what appears to be a single, unified API.

### Key Components

1. **REST API Controllers**: Handle HTTP requests and route them to appropriate backend services
2. **WebSocket Handlers**: Manage real-time connections with clients
3. **Authentication/Authorization**: Verify user identity and permissions
4. **Service Discovery**: Locate and communicate with backend microservices
5. **Request/Response Transformation**: Ensure backward compatibility

![API Gateway Architecture](https://www.traccar.org/images/api-gateway-architecture.png)

*Figure 1: High-level architecture of the API Gateway service showing its position in the microservices ecosystem.*

### Request Flow

A typical request flow through the API Gateway:

1. Client sends request to API Gateway
2. Authentication filter validates credentials
3. Authorization filter checks permissions
4. Request is routed to appropriate backend service
5. Response from backend service is transformed if needed
6. Response is returned to client

### WebSocket Flow

For real-time updates:

1. Client establishes WebSocket connection to API Gateway
2. API Gateway authenticates the connection
3. API Gateway subscribes to relevant message broker topics
4. Events from backend services are published to message broker
5. API Gateway consumes events and forwards to appropriate WebSocket clients

## Code Organization

The API Gateway codebase follows a structured organization to maintain separation of concerns and facilitate extensibility.

### Package Structure

The API Gateway follows a modular package structure that separates concerns and facilitates maintenance and extension:

```
org.traccar.api/
├── controller/       # REST API controllers
├── websocket/        # WebSocket handlers
├── security/         # Authentication and authorization
│   ├── signature/    # Token and signature management
│   └── permission/   # Permission enforcement
├── resource/         # Resource classes (endpoints)
├── filter/           # Request/response filters
├── model/            # Data transfer objects
├── service/          # Business logic services
├── client/           # Backend service clients
├── exception/        # Exception handling
└── util/             # Utility classes
```

### Key Classes

#### Base Classes

- `BaseResource`: Foundation class for all API resources, providing access to common services like storage and permissions.

```java
public class BaseResource {
    @Context
    private SecurityContext securityContext;

    @Inject
    protected Storage storage;

    @Inject
    protected PermissionsService permissionsService;

    protected long getUserId() {
        UserPrincipal principal = (UserPrincipal) securityContext.getUserPrincipal();
        if (principal != null) {
            return principal.getUserId();
        }
        return 0;
    }
}
```

- `BaseObjectResource<T>`: Extends BaseResource to provide CRUD operations for model objects.

```java
public abstract class BaseObjectResource<T extends BaseModel> extends BaseResource {
    // Implements GET, POST, PUT, DELETE operations for model objects
    // ...
}
```

- `ExtendedObjectResource<T>`: Extends BaseObjectResource with additional query capabilities.

```java
public class ExtendedObjectResource<T extends BaseModel> extends BaseObjectResource<T> {
    // Adds support for filtering by user, group, device, etc.
    // ...
}
```

#### WebSocket Support

- `AsyncSocket`: Handles WebSocket connections and message routing.
- `AsyncSocketServlet`: Servlet implementation for WebSocket endpoint.

#### Security

- `SecurityRequestFilter`: Handles authentication for incoming requests.
- `PermissionsService`: Enforces authorization rules.

## Development Environment Setup

This section guides you through setting up your development environment for working with the API Gateway service.

### Prerequisites

- Java Development Kit (JDK) 17 or later
- Maven 3.8.x or later
- Docker and Docker Compose
- Git
- IDE of your choice (IntelliJ IDEA, Eclipse, VS Code)

### Getting the Source Code

```bash
# Clone the repository
git clone https://github.com/traccar/traccar-api-gateway.git
cd traccar-api-gateway
```

### Building the Project

```bash
# Build with Maven
mvn clean install
```

### Running Locally

#### Option 1: Standalone Mode

```bash
# Run the API Gateway with an embedded Jetty server
mvn exec:java -Dexec.mainClass="org.traccar.Main"
```

#### Option 2: Docker Compose

```bash
# Start the API Gateway and dependencies
docker-compose up -d
```

### Configuration

The API Gateway can be configured through:

1. **config.yml**: Main configuration file
2. **Environment Variables**: Override configuration for containerized environments
3. **JVM System Properties**: Override configuration for development

Example development configuration:

```yaml
# config.yml for development
server:
  port: 8082
  address: localhost

logger:
  level: DEBUG
  file: api-gateway.log

database:
  url: jdbc:h2:./target/database
  user: sa
  password: 

serviceDiscovery:
  type: static
  services:
    position-service: http://localhost:8090
    event-service: http://localhost:8091
    notification-service: http://localhost:8092
    protocol-service: http://localhost:8093
    reporting-service: http://localhost:8094
```

## Extension Points

The API Gateway is designed to be extensible through well-defined extension points. This architecture allows developers to add new functionality without modifying the core components, ensuring maintainability and reducing the risk of regressions.

### Adding New API Endpoints

To add a new API endpoint, create a new resource class extending one of the base resource classes:

```java
@Path("myresource")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MyResource extends BaseResource {
    
    @GET
    public Response getAll() {
        // Implementation
        return Response.ok(result).build();
    }
    
    @Path("{id}")
    @GET
    public Response getSingle(@PathParam("id") long id) {
        // Implementation
        return Response.ok(result).build();
    }
    
    // Additional methods...
}
```

Register your resource in the `JerseyConfig` class:

```java
public class JerseyConfig extends ResourceConfig {
    public JerseyConfig() {
        // Register existing resources
        // ...
        
        // Register your new resource
        register(MyResource.class);
    }
}
```

### Creating a New Object Resource

For resources that manage persistent objects, extend `BaseObjectResource` or `ExtendedObjectResource`:

```java
@Path("myobjects")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MyObjectResource extends ExtendedObjectResource<MyObject> {
    public MyObjectResource() {
        super(MyObject.class, "uniqueId");
    }
    
    // Add custom methods beyond CRUD operations
    @Path("custom/{id}")
    @GET
    public Response customOperation(@PathParam("id") long id) {
        // Implementation
        return Response.ok(result).build();
    }
}
```

### Adding Custom Filters

Implement request or response filters to modify the processing pipeline:

```java
@Provider
public class MyRequestFilter implements ContainerRequestFilter {
    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        // Filter implementation
    }
}
```

Register your filter in the `JerseyConfig` class.

### Extending WebSocket Functionality

To add new WebSocket message types or handlers:

1. Extend the `AsyncSocket` class or implement a new handler
2. Modify the `WebSocketManager` to route messages to your handler
3. Update the message broker subscriptions if needed

### Adding Backend Service Integration

One of the primary functions of the API Gateway is to integrate with backend microservices. To integrate with a new backend microservice:

1. Create a client interface for the service
2. Implement the client using REST, gRPC, or direct message broker integration
3. Register the client with the dependency injection system
4. Use the client in your resource classes
5. Update service discovery configuration to include the new service

Example service client:

```java
@Singleton
public class MyServiceClient {
    private final WebClient webClient;
    private final ServiceDiscovery serviceDiscovery;
    
    @Inject
    public MyServiceClient(WebClient.Builder webClientBuilder, ServiceDiscovery serviceDiscovery) {
        this.serviceDiscovery = serviceDiscovery;
        this.webClient = webClientBuilder.build();
    }
    
    public CompletableFuture<MyData> fetchData(long id) {
        String serviceUrl = serviceDiscovery.getServiceUrl("my-service");
        return webClient.get()
            .uri(serviceUrl + "/api/data/" + id)
            .retrieve()
            .bodyToMono(MyData.class)
            .toFuture();
    }
}
```

## Testing Guidelines

Testing is a critical part of the development process for the API Gateway. This section outlines the testing approach and guidelines.

### Test Categories

The API Gateway testing strategy includes multiple test categories to ensure comprehensive coverage:

1. **Unit Tests**: Test individual components in isolation
   - Controllers, filters, security components, utility classes
   - Use mocking for dependencies
   - Focus on business logic and edge cases

2. **Integration Tests**: Test interactions between components
   - Resource classes with actual database connections
   - Service clients with mock backend services
   - Authentication and authorization flows

3. **API Tests**: Test the REST API endpoints
   - Request/response validation
   - Error handling
   - Content negotiation
   - Authentication and authorization

4. **WebSocket Tests**: Test WebSocket functionality
   - Connection establishment and authentication
   - Message subscription and delivery
   - Reconnection handling
   - Performance under load

5. **End-to-End Tests**: Test complete flows through the system
   - User journeys across multiple endpoints
   - Integration with actual backend services
   - UI interaction (if applicable)

### Unit Testing

Unit tests should be written for all business logic and utility classes. Use JUnit 5 and Mockito for unit testing.

Example unit test:

```java
@ExtendWith(MockitoExtension.class)
public class PermissionsServiceTest {
    
    @Mock
    private Storage storage;
    
    @InjectMocks
    private PermissionsService permissionsService;
    
    @Test
    public void testCheckPermission() {
        // Test setup
        when(storage.getPermissions(anyLong(), any(), anyLong()))
            .thenReturn(Collections.singletonList(new Permission(...)));
        
        // Test execution
        permissionsService.checkPermission(Device.class, 1L, 2L);
        
        // Verification
        verify(storage).getPermissions(1L, Device.class, 2L);
    }
    
    @Test
    public void testCheckPermissionNoAccess() {
        // Test setup
        when(storage.getPermissions(anyLong(), any(), anyLong()))
            .thenReturn(Collections.emptyList());
        
        // Test execution & verification
        assertThrows(SecurityException.class, () -> 
            permissionsService.checkPermission(Device.class, 1L, 2L));
    }
}
```

### Integration Testing

Integration tests should verify the interaction between components. Use test containers for dependencies like databases and message brokers.

Example integration test:

```java
@SpringBootTest
@Testcontainers
public class UserResourceIntegrationTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:14")
        .withDatabaseName("testdb")
        .withUsername("test")
        .withPassword("test");
    
    @Autowired
    private WebTestClient webTestClient;
    
    @Test
    public void testCreateUser() {
        // Test execution
        webTestClient.post().uri("/api/users")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue("{\"name\":\"Test User\",\"email\":\"test@example.com\"}")
            .exchange()
            .expectStatus().isCreated()
            .expectBody()
            .jsonPath("$.id").isNotEmpty()
            .jsonPath("$.name").isEqualTo("Test User");
    }
}
```

### API Testing

API tests should verify the behavior of the REST endpoints. Use REST Assured or WebTestClient for API testing.

### WebSocket Testing

WebSocket tests should verify the real-time communication functionality. Use WebSocket client libraries for testing.

Example WebSocket test:

```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class WebSocketTest {
    
    @LocalServerPort
    private int port;
    
    private WebSocketClient webSocketClient;
    private StompSession session;
    
    @BeforeEach
    public void setup() throws Exception {
        webSocketClient = new StandardWebSocketClient();
        WebSocketStompClient stompClient = new WebSocketStompClient(webSocketClient);
        stompClient.setMessageConverter(new MappingJackson2MessageConverter());
        
        session = stompClient.connect("ws://localhost:" + port + "/api/socket", new StompSessionHandlerAdapter() {})
            .get(5, TimeUnit.SECONDS);
    }
    
    @Test
    public void testPositionUpdates() throws Exception {
        // Subscribe to position updates
        BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<>(1);
        session.subscribe("/topic/positions", new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
                return String.class;
            }
            
            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
                blockingQueue.add((String) payload);
            }
        });
        
        // Trigger a position update (e.g., through a test API endpoint)
        // ...
        
        // Verify the update was received
        String message = blockingQueue.poll(5, TimeUnit.SECONDS);
        assertNotNull(message);
        assertTrue(message.contains("deviceId"));
    }
}
```

### End-to-End Testing

End-to-end tests should verify complete flows through the system. Use tools like Selenium or Cypress for UI-driven end-to-end tests.

### Test Coverage

Aim for high test coverage, especially for critical components:

- Core business logic: 90%+ coverage
- API resources: 80%+ coverage
- Utility classes: 70%+ coverage

Use JaCoCo for measuring test coverage:

```xml
<plugin>
    <groupId>org.jacoco</groupId>
    <artifactId>jacoco-maven-plugin</artifactId>
    <version>0.8.10</version>
    <executions>
        <execution>
            <goals>
                <goal>prepare-agent</goal>
            </goals>
        </execution>
        <execution>
            <id>report</id>
            <phase>test</phase>
            <goals>
                <goal>report</goal>
            </goals>
        </execution>
    </executions>
</plugin>
```

## Contribution Workflow

This section outlines the process for contributing changes to the API Gateway service.

### Development Process

1. **Create a Feature Branch**: Always create a new branch for your changes
   ```bash
   git checkout -b feature/my-new-feature
   ```

2. **Implement Changes**: Make your code changes following the coding standards

3. **Write Tests**: Add tests for your changes

4. **Run Local Tests**: Ensure all tests pass locally
   ```bash
   mvn clean test
   ```

5. **Create Pull Request**: Submit your changes for review

6. **Code Review**: Address feedback from reviewers

7. **Merge**: Once approved, your changes will be merged

### Coding Standards

Follow these coding standards for consistency:

1. **Java Code Style**: Follow Google Java Style Guide
2. **Documentation**: Add Javadoc comments for all public classes and methods
3. **Logging**: Use SLF4J for logging with appropriate log levels
4. **Exception Handling**: Use appropriate exception types and include context
5. **Dependency Injection**: Use constructor injection for required dependencies

### Commit Message Guidelines

Use conventional commit messages for clarity:

```
<type>(<scope>): <subject>

<body>

<footer>
```

Where:
- `<type>`: feat, fix, docs, style, refactor, test, chore
- `<scope>`: component affected (e.g., api, websocket, security)
- `<subject>`: brief description in present tense
- `<body>`: detailed description (optional)
- `<footer>`: reference issues, breaking changes (optional)

Examples:
```
feat(api): add new endpoint for device commands

Implements a new REST endpoint for sending commands to devices.

Closes #123
```

```
fix(security): correct permission check in device resource

Fixes a bug where users could access devices they don't own.
```

### Pull Request Process

1. Ensure your code follows the coding standards
2. Update documentation if necessary
3. Include tests for your changes
4. Ensure CI pipeline passes
5. Request review from at least one team member
6. Address review comments
7. Merge once approved

## Best Practices

Following these best practices will help ensure that your contributions to the API Gateway service are maintainable, secure, and performant. These guidelines have been developed based on real-world experience with the Traccar system and industry standards for API gateway implementations.

### API Design

1. **Consistency**: Follow REST principles consistently
2. **Versioning**: Use URL or header-based versioning for breaking changes
3. **Error Handling**: Return standardized error responses
4. **Pagination**: Support pagination for collection endpoints
5. **Filtering**: Allow filtering by relevant attributes
6. **Documentation**: Document all endpoints with examples

### Performance

1. **Caching**: Use appropriate caching for frequently accessed data
2. **Asynchronous Processing**: Use non-blocking I/O for external service calls
3. **Connection Pooling**: Configure appropriate connection pool sizes
4. **Resource Limits**: Set appropriate timeouts and resource limits
5. **Monitoring**: Add metrics for performance-critical operations

### Security

Security is a critical concern for the API Gateway as it serves as the entry point to the entire system:

1. **Input Validation**: Validate all client input
   - Use Bean Validation (JSR 380) for model validation
   - Sanitize inputs to prevent injection attacks
   - Validate content types and request formats

2. **Authentication**: Always verify user identity
   - Use the SecurityRequestFilter for all protected endpoints
   - Implement proper token validation and expiration
   - Support multiple authentication methods (Basic, JWT, API key)

3. **Authorization**: Check permissions for all operations
   - Use the PermissionsService to check access rights
   - Implement principle of least privilege
   - Document permission requirements for each endpoint

4. **Sensitive Data**: Never log sensitive information
   - Mask sensitive data in logs (passwords, tokens, personal information)
   - Use appropriate log levels
   - Implement proper exception handling to avoid leaking sensitive information

5. **Rate Limiting**: Implement rate limiting for public endpoints
   - Use the RateLimiter service for high-traffic endpoints
   - Implement tiered rate limits based on user roles
   - Provide clear rate limit information in responses

### Resilience

1. **Circuit Breakers**: Use circuit breakers for external service calls
2. **Retry Policies**: Implement appropriate retry policies
3. **Fallbacks**: Provide fallback mechanisms for critical operations
4. **Timeouts**: Set appropriate timeouts for all external calls
5. **Graceful Degradation**: Design for partial system availability

## Troubleshooting

When developing or extending the API Gateway, you may encounter various issues. This section provides guidance for troubleshooting common problems and offers diagnostic approaches to identify and resolve issues efficiently.

### Common Issues

#### Connection Refused

**Symptoms**: Clients cannot connect to the API Gateway

**Possible Causes**:
- API Gateway service is not running
- Incorrect port or address configuration
- Firewall blocking connections

**Solutions**:
- Check service status
- Verify configuration
- Check network connectivity

#### Authentication Failures

**Symptoms**: Clients receive 401 Unauthorized responses

**Possible Causes**:
- Invalid credentials
- Expired tokens
- Incorrect authentication configuration

**Solutions**:
- Verify credentials
- Check token expiration
- Review authentication configuration

#### Service Unavailable

**Symptoms**: Clients receive 503 Service Unavailable responses

**Possible Causes**:
- Backend service is down
- Service discovery failure
- Circuit breaker open

**Solutions**:
- Check backend service status
- Verify service discovery configuration
- Check circuit breaker status

### Logging

Enable debug logging for troubleshooting:

```yaml
logger:
  level: DEBUG
  file: api-gateway.log
```

Key log files to check:
- `api-gateway.log`: Main API Gateway log
- `access.log`: HTTP access log
- `error.log`: Error log

### Monitoring and Debugging

The API Gateway provides several endpoints and tools for monitoring and debugging:

#### Monitoring Endpoints

- `/health`: Health check endpoint that reports the overall status of the API Gateway and its dependencies
- `/metrics`: Prometheus-compatible metrics endpoint for performance monitoring
- `/info`: Service information endpoint with version and build details

#### Debugging Tools

- **Request Tracing**: Enable request tracing for detailed request flow information
  ```yaml
  server:
    requestTracing:
      enabled: true
      threshold: 500ms  # Trace requests taking longer than 500ms
  ```

- **Remote Debugging**: For development environments, enable remote debugging
  ```bash
  java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -jar api-gateway.jar
  ```

- **Thread Dumps**: Generate thread dumps for analyzing deadlocks or performance issues
  ```bash
  kill -3 <pid>  # On Unix-like systems
  jstack <pid>    # Using JDK tools
  ```

## References

### Internal Documentation

- [API Gateway Architecture Overview](architecture-overview.md)
- [API Reference Documentation](api-reference.md)
- [WebSocket Interface Documentation](websocket-interface.md)
- [Security Documentation](security.md)
- [Operations Guide](operations.md)
- [Configuration Guide](configuration-guide.md)

### External Resources

- [API Gateway Source Code](https://github.com/traccar/traccar-api-gateway)
- [Traccar Documentation](https://www.traccar.org/documentation/)
- [Jersey Documentation](https://eclipse-ee4j.github.io/jersey/)
- [Jetty Documentation](https://www.eclipse.org/jetty/documentation/)
- [Jackson Documentation](https://github.com/FasterXML/jackson-docs)
- [WebSocket API](https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API)
- [Microservices Pattern: API Gateway](https://microservices.io/patterns/apigateway.html)
- [REST API Design Best Practices](https://restfulapi.net/)