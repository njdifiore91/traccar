# API Gateway Configuration Guide

## Introduction

This document provides comprehensive configuration instructions for the Traccar API Gateway service. The API Gateway serves as the unified entry point for all external API interactions, handling authentication, request routing, and WebSocket connections for real-time updates.

## Configuration Methods

The API Gateway service supports multiple configuration methods, with the following precedence (highest to lowest):

1. Environment variables
2. Configuration file (YAML/properties)
3. Default values

### Environment Variables

Environment variables are the recommended configuration method for containerized deployments. All configuration parameters can be set using environment variables with the following naming convention:

```
TRACCAR_SECTION_PARAMETER=value
```

For example, to set the HTTP port:

```
TRACCAR_WEB_PORT=8082
```

### Configuration File

The API Gateway can be configured using a YAML configuration file. By default, the service looks for `config.yml` in the following locations:

1. The current working directory
2. `/opt/traccar/config.yml`
3. `/etc/traccar/config.yml`

A custom configuration file location can be specified using the `--config` command-line argument or the `TRACCAR_CONFIG` environment variable.

Example YAML configuration:

```yaml
web:
  port: 8082
  path: /api
  origin: '*'

service:
  discovery:
    type: kubernetes
    namespace: default
```

## Core Configuration Parameters

### Server Settings

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `server.port` | `TRACCAR_SERVER_PORT` | Main server port for device connections | `8082` |
| `server.address` | `TRACCAR_SERVER_ADDRESS` | Bind address for the server | `0.0.0.0` |
| `server.timeout` | `TRACCAR_SERVER_TIMEOUT` | Connection timeout in seconds | `900` |
| `server.bossThreads` | `TRACCAR_SERVER_BOSSTHREADS` | Number of Netty boss threads | `1` |
| `server.workerThreads` | `TRACCAR_SERVER_WORKERTHREADS` | Number of Netty worker threads | `CPU cores * 2` |

### Web Interface Settings

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `web.port` | `TRACCAR_WEB_PORT` | Web server port | `8082` |
| `web.path` | `TRACCAR_WEB_PATH` | Base path for web interface | `/api` |
| `web.address` | `TRACCAR_WEB_ADDRESS` | Bind address for web server | `0.0.0.0` |
| `web.origin` | `TRACCAR_WEB_ORIGIN` | Allowed CORS origins (comma-separated or '*') | `null` |
| `web.timeout` | `TRACCAR_WEB_TIMEOUT` | Web request timeout in seconds | `300` |
| `web.sessionTimeout` | `TRACCAR_WEB_SESSIONTIMEOUT` | Session timeout in seconds | `3600` |
| `web.debug` | `TRACCAR_WEB_DEBUG` | Enable web server debug mode | `false` |

### API Settings

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `api.limit` | `TRACCAR_API_LIMIT` | API request rate limit per minute | `0` (unlimited) |
| `api.limitUser` | `TRACCAR_API_LIMITUSER` | API request rate limit per user per minute | `0` (unlimited) |
| `api.limitDevice` | `TRACCAR_API_LIMITDEVICE` | API request rate limit per device per minute | `0` (unlimited) |
| `api.limitCommand` | `TRACCAR_API_LIMITCOMMAND` | Command rate limit per device per minute | `0` (unlimited) |
| `api.longPolling` | `TRACCAR_API_LONGPOLLING` | Enable long polling for position updates | `false` |
| `api.longPollingTimeout` | `TRACCAR_API_LONGPOLLINGTIMEOUT` | Long polling timeout in seconds | `60` |

## Service Discovery Configuration

The API Gateway uses service discovery to locate and communicate with backend microservices. The following service discovery methods are supported:

### Kubernetes Service Discovery

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `service.discovery.type` | `TRACCAR_SERVICE_DISCOVERY_TYPE` | Service discovery type | `kubernetes` |
| `service.discovery.namespace` | `TRACCAR_SERVICE_DISCOVERY_NAMESPACE` | Kubernetes namespace | `default` |
| `service.discovery.selector` | `TRACCAR_SERVICE_DISCOVERY_SELECTOR` | Label selector for services | `app=traccar` |
| `service.discovery.refresh` | `TRACCAR_SERVICE_DISCOVERY_REFRESH` | Service discovery refresh interval in seconds | `30` |

### Consul Service Discovery

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `service.discovery.type` | `TRACCAR_SERVICE_DISCOVERY_TYPE` | Service discovery type | `consul` |
| `service.discovery.host` | `TRACCAR_SERVICE_DISCOVERY_HOST` | Consul host | `localhost` |
| `service.discovery.port` | `TRACCAR_SERVICE_DISCOVERY_PORT` | Consul port | `8500` |
| `service.discovery.datacenter` | `TRACCAR_SERVICE_DISCOVERY_DATACENTER` | Consul datacenter | `dc1` |
| `service.discovery.token` | `TRACCAR_SERVICE_DISCOVERY_TOKEN` | Consul ACL token | `null` |
| `service.discovery.refresh` | `TRACCAR_SERVICE_DISCOVERY_REFRESH` | Service discovery refresh interval in seconds | `30` |

### Static Service Discovery

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `service.discovery.type` | `TRACCAR_SERVICE_DISCOVERY_TYPE` | Service discovery type | `static` |
| `service.protocol.url` | `TRACCAR_SERVICE_PROTOCOL_URL` | Protocol service URL | `http://protocol-service:8080` |
| `service.position.url` | `TRACCAR_SERVICE_POSITION_URL` | Position service URL | `http://position-service:8080` |
| `service.event.url` | `TRACCAR_SERVICE_EVENT_URL` | Event service URL | `http://event-service:8080` |
| `service.notification.url` | `TRACCAR_SERVICE_NOTIFICATION_URL` | Notification service URL | `http://notification-service:8080` |
| `service.reporting.url` | `TRACCAR_SERVICE_REPORTING_URL` | Reporting service URL | `http://reporting-service:8080` |

## Security Configuration

### Authentication Settings

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `security.auth.enabled` | `TRACCAR_SECURITY_AUTH_ENABLED` | Enable authentication | `true` |
| `security.auth.type` | `TRACCAR_SECURITY_AUTH_TYPE` | Authentication type (basic, jwt, oauth) | `basic` |
| `security.auth.header` | `TRACCAR_SECURITY_AUTH_HEADER` | Authentication header name | `Authorization` |
| `security.auth.sessionEnabled` | `TRACCAR_SECURITY_AUTH_SESSIONENABLED` | Enable session-based authentication | `true` |
| `security.auth.sessionTimeout` | `TRACCAR_SECURITY_AUTH_SESSIONTIMEOUT` | Session timeout in seconds | `3600` |
| `security.auth.sessionCookie` | `TRACCAR_SECURITY_AUTH_SESSIONCOOKIE` | Session cookie name | `JSESSIONID` |

### JWT Authentication

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `security.jwt.secret` | `TRACCAR_SECURITY_JWT_SECRET` | JWT secret key | `null` |
| `security.jwt.issuer` | `TRACCAR_SECURITY_JWT_ISSUER` | JWT issuer | `traccar` |
| `security.jwt.expiration` | `TRACCAR_SECURITY_JWT_EXPIRATION` | JWT token expiration in seconds | `3600` |
| `security.jwt.refreshExpiration` | `TRACCAR_SECURITY_JWT_REFRESHEXPIRATION` | JWT refresh token expiration in seconds | `86400` |

### OAuth Authentication

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `security.oauth.provider` | `TRACCAR_SECURITY_OAUTH_PROVIDER` | OAuth provider (google, github, custom) | `null` |
| `security.oauth.clientId` | `TRACCAR_SECURITY_OAUTH_CLIENTID` | OAuth client ID | `null` |
| `security.oauth.clientSecret` | `TRACCAR_SECURITY_OAUTH_CLIENTSECRET` | OAuth client secret | `null` |
| `security.oauth.redirectUri` | `TRACCAR_SECURITY_OAUTH_REDIRECTURI` | OAuth redirect URI | `null` |
| `security.oauth.authorizationUrl` | `TRACCAR_SECURITY_OAUTH_AUTHORIZATIONURL` | OAuth authorization URL (for custom provider) | `null` |
| `security.oauth.tokenUrl` | `TRACCAR_SECURITY_OAUTH_TOKENURL` | OAuth token URL (for custom provider) | `null` |
| `security.oauth.userInfoUrl` | `TRACCAR_SECURITY_OAUTH_USERINFOURL` | OAuth user info URL (for custom provider) | `null` |

### TLS Configuration

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `security.tls.enabled` | `TRACCAR_SECURITY_TLS_ENABLED` | Enable TLS | `false` |
| `security.tls.keyStore` | `TRACCAR_SECURITY_TLS_KEYSTORE` | Path to keystore file | `null` |
| `security.tls.keyStorePassword` | `TRACCAR_SECURITY_TLS_KEYSTOREPASSWORD` | Keystore password | `null` |
| `security.tls.keyStoreType` | `TRACCAR_SECURITY_TLS_KEYSTORETYPE` | Keystore type | `JKS` |
| `security.tls.keyAlias` | `TRACCAR_SECURITY_TLS_KEYALIAS` | Key alias | `null` |
| `security.tls.trustStore` | `TRACCAR_SECURITY_TLS_TRUSTSTORE` | Path to truststore file | `null` |
| `security.tls.trustStorePassword` | `TRACCAR_SECURITY_TLS_TRUSTSTOREPASSWORD` | Truststore password | `null` |
| `security.tls.trustStoreType` | `TRACCAR_SECURITY_TLS_TRUSTSTORETYPE` | Truststore type | `JKS` |

### CORS Configuration

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `web.origin` | `TRACCAR_WEB_ORIGIN` | Allowed CORS origins (comma-separated or '*') | `null` |
| `web.corsAllowCredentials` | `TRACCAR_WEB_CORSALLOWCREDENTIALS` | Allow credentials for CORS requests | `true` |
| `web.corsAllowMethods` | `TRACCAR_WEB_CORSALLOWMETHODS` | Allowed HTTP methods for CORS | `GET, POST, PUT, DELETE, OPTIONS` |
| `web.corsAllowHeaders` | `TRACCAR_WEB_CORSALLOWHEADERS` | Allowed HTTP headers for CORS | `origin, content-type, accept, authorization` |

## WebSocket Configuration

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `websocket.enabled` | `TRACCAR_WEBSOCKET_ENABLED` | Enable WebSocket support | `true` |
| `websocket.path` | `TRACCAR_WEBSOCKET_PATH` | WebSocket endpoint path | `/socket` |
| `websocket.maxSessionsPerUser` | `TRACCAR_WEBSOCKET_MAXSESSIONSPERUSER` | Maximum WebSocket sessions per user | `10` |
| `websocket.timeout` | `TRACCAR_WEBSOCKET_TIMEOUT` | WebSocket connection timeout in seconds | `60` |
| `websocket.maxTextMessageSize` | `TRACCAR_WEBSOCKET_MAXTEXTMESSAGESIZE` | Maximum WebSocket text message size in bytes | `65536` |
| `websocket.maxBinaryMessageSize` | `TRACCAR_WEBSOCKET_MAXBINARYMESSAGESIZE` | Maximum WebSocket binary message size in bytes | `65536` |

## Message Broker Configuration

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `broker.type` | `TRACCAR_BROKER_TYPE` | Message broker type (kafka, rabbitmq) | `kafka` |
| `broker.url` | `TRACCAR_BROKER_URL` | Message broker connection URL | `localhost:9092` |
| `broker.username` | `TRACCAR_BROKER_USERNAME` | Message broker username | `null` |
| `broker.password` | `TRACCAR_BROKER_PASSWORD` | Message broker password | `null` |
| `broker.clientId` | `TRACCAR_BROKER_CLIENTID` | Client ID for the broker connection | `api-gateway` |
| `broker.groupId` | `TRACCAR_BROKER_GROUPID` | Consumer group ID | `api-gateway-group` |
| `broker.topics.position` | `TRACCAR_BROKER_TOPICS_POSITION` | Position topic name | `positions` |
| `broker.topics.event` | `TRACCAR_BROKER_TOPICS_EVENT` | Event topic name | `events` |
| `broker.topics.command` | `TRACCAR_BROKER_TOPICS_COMMAND` | Command topic name | `commands` |
| `broker.topics.notification` | `TRACCAR_BROKER_TOPICS_NOTIFICATION` | Notification topic name | `notifications` |

## Performance Tuning

### Thread Pool Configuration

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `server.threadPool.minThreads` | `TRACCAR_SERVER_THREADPOOL_MINTHREADS` | Minimum number of threads in the thread pool | `10` |
| `server.threadPool.maxThreads` | `TRACCAR_SERVER_THREADPOOL_MAXTHREADS` | Maximum number of threads in the thread pool | `200` |
| `server.threadPool.queueSize` | `TRACCAR_SERVER_THREADPOOL_QUEUESIZE` | Thread pool queue size | `1000` |
| `server.threadPool.idleTimeout` | `TRACCAR_SERVER_THREADPOOL_IDLETIMEOUT` | Thread idle timeout in milliseconds | `60000` |

### Connection Pool Configuration

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `server.connectionPool.maxConnections` | `TRACCAR_SERVER_CONNECTIONPOOL_MAXCONNECTIONS` | Maximum number of connections | `1000` |
| `server.connectionPool.idleTimeout` | `TRACCAR_SERVER_CONNECTIONPOOL_IDLETIMEOUT` | Connection idle timeout in milliseconds | `60000` |

### Circuit Breaker Configuration

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `resilience.circuitBreaker.enabled` | `TRACCAR_RESILIENCE_CIRCUITBREAKER_ENABLED` | Enable circuit breakers | `true` |
| `resilience.circuitBreaker.failureRateThreshold` | `TRACCAR_RESILIENCE_CIRCUITBREAKER_FAILURERATETHRESHOLD` | Failure rate threshold percentage | `50` |
| `resilience.circuitBreaker.waitDurationInOpenState` | `TRACCAR_RESILIENCE_CIRCUITBREAKER_WAITDURATIONINOPENSTATE` | Wait duration in open state in milliseconds | `10000` |
| `resilience.circuitBreaker.slidingWindowSize` | `TRACCAR_RESILIENCE_CIRCUITBREAKER_SLIDINGWINDOWSIZE` | Sliding window size | `100` |
| `resilience.circuitBreaker.permittedNumberOfCallsInHalfOpenState` | `TRACCAR_RESILIENCE_CIRCUITBREAKER_PERMITTEDNUMBEROFCALLSINHALFOPENSTATE` | Permitted number of calls in half-open state | `10` |

## Environment-Specific Examples

### Development Environment

```yaml
web:
  port: 8082
  path: /api
  origin: '*'
  debug: true

service:
  discovery:
    type: static
    protocol.url: http://localhost:8090
    position.url: http://localhost:8091
    event.url: http://localhost:8092
    notification.url: http://localhost:8093
    reporting.url: http://localhost:8094

security:
  auth:
    enabled: true
    type: basic
  tls:
    enabled: false

websocket:
  enabled: true
  path: /socket

broker:
  type: kafka
  url: localhost:9092
```

### Production Environment

```yaml
web:
  port: 8082
  path: /api
  origin: https://traccar.example.com

service:
  discovery:
    type: kubernetes
    namespace: traccar
    selector: app=traccar

security:
  auth:
    enabled: true
    type: jwt
  jwt:
    secret: ${JWT_SECRET}
    expiration: 3600
  tls:
    enabled: true
    keyStore: /etc/traccar/keystore.jks
    keyStorePassword: ${KEYSTORE_PASSWORD}

websocket:
  enabled: true
  path: /socket
  maxSessionsPerUser: 5

broker:
  type: kafka
  url: kafka-broker:9092
  clientId: api-gateway-prod
  groupId: api-gateway-prod-group

resilience:
  circuitBreaker:
    enabled: true
    failureRateThreshold: 50
    waitDurationInOpenState: 30000
```

### High-Availability Production Environment

```yaml
web:
  port: 8082
  path: /api
  origin: https://traccar.example.com

service:
  discovery:
    type: kubernetes
    namespace: traccar
    selector: app=traccar
    refresh: 15

security:
  auth:
    enabled: true
    type: jwt
  jwt:
    secret: ${JWT_SECRET}
    expiration: 3600
  tls:
    enabled: true
    keyStore: /etc/traccar/keystore.jks
    keyStorePassword: ${KEYSTORE_PASSWORD}

websocket:
  enabled: true
  path: /socket
  maxSessionsPerUser: 5

broker:
  type: kafka
  url: kafka-broker-0.kafka-headless:9092,kafka-broker-1.kafka-headless:9092,kafka-broker-2.kafka-headless:9092
  clientId: api-gateway-prod
  groupId: api-gateway-prod-group

server:
  threadPool:
    minThreads: 20
    maxThreads: 400
    queueSize: 2000

resilience:
  circuitBreaker:
    enabled: true
    failureRateThreshold: 50
    waitDurationInOpenState: 30000
```

## Troubleshooting

### Common Issues

#### Service Discovery Failures

If the API Gateway cannot discover backend services:

1. Check that the service discovery configuration is correct
2. Verify that backend services are registered with the service discovery system
3. Check network connectivity between the API Gateway and service discovery system
4. Examine logs for service discovery errors

#### Authentication Issues

If users cannot authenticate:

1. Verify that the authentication configuration is correct
2. Check that the JWT secret or OAuth credentials are properly configured
3. Examine logs for authentication errors
4. Verify that the client is sending the correct authentication headers

#### WebSocket Connection Problems

If WebSocket connections fail:

1. Verify that WebSocket support is enabled
2. Check that the WebSocket path is correctly configured
3. Verify that the client is using the correct WebSocket URL
4. Examine logs for WebSocket connection errors

### Logging Configuration

The API Gateway uses SLF4J with Logback for logging. The logging level can be configured using the following parameters:

| Parameter | Environment Variable | Description | Default Value |
|-----------|----------------------|-------------|---------------|
| `logger.level` | `TRACCAR_LOGGER_LEVEL` | Root logging level | `INFO` |
| `logger.console` | `TRACCAR_LOGGER_CONSOLE` | Enable console logging | `true` |
| `logger.file.enable` | `TRACCAR_LOGGER_FILE_ENABLE` | Enable file logging | `true` |
| `logger.file.path` | `TRACCAR_LOGGER_FILE_PATH` | Log file path | `logs/traccar-api-gateway.log` |
| `logger.file.level` | `TRACCAR_LOGGER_FILE_LEVEL` | File logging level | `INFO` |

For more detailed logging, set the logging level to `DEBUG` or `TRACE`:

```yaml
logger:
  level: DEBUG
  console: true
  file:
    enable: true
    path: logs/traccar-api-gateway.log
    level: DEBUG
```

### Health Checks

The API Gateway provides health check endpoints that can be used to monitor the service status:

- `/actuator/health/liveness` - Checks if the service is running
- `/actuator/health/readiness` - Checks if the service is ready to handle requests
- `/actuator/health/metrics` - Provides detailed performance metrics

These endpoints can be used by Kubernetes or other orchestration systems to monitor the service health and perform automatic restarts if necessary.