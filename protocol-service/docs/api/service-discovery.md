# Protocol Service - Service Discovery

## Overview

This document describes how the Protocol Service integrates with service discovery mechanisms to enable dynamic service registration, health monitoring, and discovery by other services in the Traccar microservices ecosystem.

The Protocol Service supports two primary service discovery implementations:
- **Consul** - For traditional deployments and development environments
- **Kubernetes** - For containerized deployments in Kubernetes environments

## Service Registration Process

The Protocol Service automatically registers itself with the configured service discovery mechanism during startup. This registration process makes the service discoverable by other components in the system.

### Consul Registration

When using Consul for service discovery, the Protocol Service registers itself as follows:

```json
{
  "Name": "protocol-service",
  "ID": "protocol-service-${INSTANCE_ID}",
  "Address": "${SERVICE_HOST}",
  "Port": ${SERVICE_PORT},
  "Tags": ["protocol", "device", "traccar", "${ENVIRONMENT}"],
  "Meta": {
    "version": "${SERVICE_VERSION}",
    "environment": "${ENVIRONMENT}",
    "protocols": "${ENABLED_PROTOCOLS}"
  },
  "Check": {
    "HTTP": "http://${SERVICE_HOST}:${SERVICE_PORT}/actuator/health/liveness",
    "Interval": "15s",
    "Timeout": "5s",
    "DeregisterCriticalServiceAfter": "30s"
  }
}
```

The registration includes:
- A unique service ID combining the service name and instance ID
- The service's host address and port
- Tags for service categorization and filtering
- Metadata including version, environment, and supported protocols
- Health check configuration pointing to the service's health endpoint

### Kubernetes Registration

In Kubernetes environments, the Protocol Service leverages the built-in service discovery mechanism through Kubernetes Services and Endpoints. The service is defined in Kubernetes manifests:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: protocol-service
  labels:
    app: protocol-service
    component: device-communication
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8080"
    prometheus.io/path: "/actuator/prometheus"
spec:
  selector:
    app: protocol-service
  ports:
  - name: http
    port: 8080
    targetPort: 8080
  # Additional protocol-specific ports
  - name: osmand
    port: 5055
    targetPort: 5055
  - name: teltonika
    port: 5027
    targetPort: 5027
```

The Deployment or StatefulSet includes readiness and liveness probes that serve as health checks:

```yaml
readinessProbe:
  httpGet:
    path: /actuator/health/readiness
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 3
  failureThreshold: 3
livenessProbe:
  httpGet:
    path: /actuator/health/liveness
    port: 8080
  initialDelaySeconds: 60
  periodSeconds: 15
  timeoutSeconds: 5
  failureThreshold: 3
```

## Health Check Implementation

The Protocol Service exposes several health check endpoints that are used by service discovery mechanisms to determine service health and availability:

### Health Check Endpoints

| Endpoint | Purpose | Success Criteria |
|----------|---------|------------------|
| `/actuator/health/liveness` | Verifies the service is running and responsive | Basic application functionality is operational |
| `/actuator/health/readiness` | Verifies the service is ready to accept requests | All required dependencies are available and the service can process requests |
| `/actuator/health` | Comprehensive health status | All health indicators report UP status |

### Health Indicators

The Protocol Service implements the following health indicators:

1. **Basic Application Health** - Verifies the application is running
2. **Protocol Handler Health** - Verifies protocol handlers are initialized correctly
3. **Message Broker Health** - Verifies connectivity to the message broker
4. **Service Discovery Health** - Verifies connectivity to the service discovery mechanism itself

### Health Check Configuration

Health check behavior can be configured through the following properties:

```properties
# Enable/disable health check endpoints
management.endpoint.health.enabled=true

# Control health information exposure
management.endpoint.health.show-details=when_authorized

# Configure health groups
management.endpoint.health.group.liveness.include=livenessState
management.endpoint.health.group.readiness.include=readinessState,messageBroker,serviceDiscovery

# Health check thresholds
message.processing.drop-threshold=0.1
```

## Service Metadata and Tagging

The Protocol Service includes metadata and tags to facilitate service filtering and provide additional information to service consumers.

### Standard Tags

| Tag | Purpose | Example |
|-----|---------|--------|
| `protocol` | Identifies the service type | `protocol` |
| `device` | Indicates device communication capability | `device` |
| `traccar` | Identifies as part of Traccar platform | `traccar` |
| `environment` | Deployment environment | `production`, `staging`, `development` |

### Service Metadata

| Metadata Field | Purpose | Example |
|----------------|---------|--------|
| `version` | Service version | `1.0.0` |
| `environment` | Deployment environment | `production` |
| `protocols` | Comma-separated list of enabled protocols | `osmand,teltonika,meitrack` |
| `startTime` | Service start timestamp | `2023-06-01T12:00:00Z` |

## Service Discovery Client Implementation

Other services can discover and interact with the Protocol Service using service discovery clients.

### Consul Client Example

```java
// Example of discovering Protocol Service instances using Consul client
ConsulClient consulClient = new ConsulClient("consul.service.consul");
Response<List<ServiceHealth>> response = consulClient.getHealthyServiceInstances("protocol-service");

// Filter for specific environment if needed
List<ServiceHealth> productionInstances = response.getValue().stream()
    .filter(sh -> "production".equals(sh.getService().getMeta().get("environment")))
    .collect(Collectors.toList());

// Get service instance details
if (!productionInstances.isEmpty()) {
    ServiceHealth instance = productionInstances.get(0);
    String serviceHost = instance.getService().getAddress();
    int servicePort = instance.getService().getPort();
    String serviceUrl = "http://" + serviceHost + ":" + servicePort;
    // Use the service URL for API calls
}
```

### Kubernetes Client Example

```java
// In Kubernetes, service discovery is typically handled through DNS
// The Protocol Service can be accessed using its service name
String serviceUrl = "http://protocol-service:8080";

// For more advanced scenarios, the Kubernetes API can be used
ApiClient apiClient = Config.defaultClient();
CoreV1Api coreV1Api = new CoreV1Api(apiClient);

V1EndpointsList endpointsList = coreV1Api.listNamespacedEndpoints(
    "default", // namespace
    null, // pretty print
    null, // _continue
    "app=protocol-service", // field selector
    null, // label selector
    null, // limit
    null, // resource version
    null, // resource version match
    null, // timeout seconds
    null  // watch
);

// Process endpoints to get service instance details
for (V1Endpoints endpoints : endpointsList.getItems()) {
    for (V1EndpointSubset subset : endpoints.getSubsets()) {
        for (V1EndpointAddress address : subset.getAddresses()) {
            String podIp = address.getIp();
            // Use pod IP for direct communication if needed
        }
    }
}
```

## Troubleshooting

### Common Issues

1. **Service Registration Failure**
   - Verify Consul agent is running and accessible
   - Check network connectivity to Consul server
   - Verify service has appropriate permissions to register

2. **Health Check Failures**
   - Check service logs for dependency issues
   - Verify health check endpoints are accessible
   - Check for resource constraints affecting service health

3. **Service Not Discoverable**
   - Verify service is registered correctly
   - Check for deregistration due to failed health checks
   - Verify client is using correct service name and tags

### Debugging Tools

1. **Consul UI** - Available at `http://consul-server:8500/ui/`
2. **Kubernetes Dashboard** - For viewing service and endpoint status
3. **Protocol Service Logs** - Check for registration and health check events
4. **Consul API** - Direct API access for troubleshooting
   ```bash
   curl http://consul-server:8500/v1/catalog/service/protocol-service
   ```
5. **Kubernetes API** - For checking service and endpoint status
   ```bash
   kubectl get endpoints protocol-service
   kubectl describe service protocol-service
   ```

## Best Practices

1. **Use Consistent Service Names** - Maintain consistent naming across environments
2. **Include Version Metadata** - Always include version information in service metadata
3. **Implement Comprehensive Health Checks** - Include checks for all critical dependencies
4. **Use Environment-Specific Tags** - Tag services with their environment for filtering
5. **Monitor Registration Status** - Implement monitoring for service registration status
6. **Graceful Deregistration** - Properly deregister services during shutdown
7. **Implement Circuit Breakers** - Use circuit breakers when calling discovered services
8. **Cache Discovery Results** - Cache service discovery results to reduce lookup overhead