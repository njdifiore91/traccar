# Traccar Microservices Kubernetes Deployment

This directory contains Kubernetes manifests for deploying the Traccar GPS tracking system as a set of microservices without using Helm. These manifests provide a direct approach to deploying Traccar in Kubernetes environments where Helm may not be available or preferred.

## Prerequisites

- Kubernetes cluster version 1.25+ (required for production environments)
- `kubectl` command-line tool configured to communicate with your cluster
- Container registry access for Traccar microservice images
- Persistent storage provisioner in your cluster (for stateful components)
- Ingress controller installed in your cluster (for API Gateway access)

## Directory Structure

This directory contains the following Kubernetes manifest files:

| Filename | Description |
|----------|-------------|
| `namespaces.yaml` | Creates isolated namespaces for each service component |
| `infrastructure.yaml` | Deploys shared infrastructure components (Kafka, ZooKeeper, Redis, Service Registry) |
| `protocol.yaml` | Deploys the Protocol Service for device communications across 200+ protocols |
| `position.yaml` | Deploys the Position Processing Service for GPS data processing |
| `event.yaml` | Deploys the Event Processing Service for event detection |
| `notification.yaml` | Deploys the Notification Service for multi-channel alerts |
| `api-gateway.yaml` | Deploys the API Gateway Service for external API access |
| `reporting.yaml` | Deploys the Reporting Service for report generation |
| `network-policies.yaml` | Defines NetworkPolicy resources to control traffic flow between services |

## Installation

### 1. Create Namespaces

First, create the required namespaces for service isolation:

```bash
kubectl apply -f namespaces.yaml
```

### 2. Deploy Infrastructure Components

Deploy the shared infrastructure components (message broker, service registry, cache):

```bash
kubectl apply -f infrastructure.yaml
```

Wait for the infrastructure components to be ready before proceeding:

```bash
kubectl -n infrastructure wait --for=condition=ready pod -l app=kafka --timeout=300s
kubectl -n infrastructure wait --for=condition=ready pod -l app=zookeeper --timeout=300s
kubectl -n infrastructure wait --for=condition=ready pod -l app=redis --timeout=300s
kubectl -n infrastructure wait --for=condition=ready pod -l app=service-registry --timeout=300s
```

### 3. Deploy Core Services

Deploy the Protocol Service (handles device communications):

```bash
kubectl apply -f protocol.yaml
```

Deploy the Position Processing Service (processes GPS data):

```bash
kubectl apply -f position.yaml
```

Deploy the Event Processing Service (detects events from position data):

```bash
kubectl apply -f event.yaml
```

Deploy the Notification Service (handles multi-channel notifications):

```bash
kubectl apply -f notification.yaml
```

### 4. Deploy API Gateway and Reporting

Deploy the API Gateway Service (provides external API access):

```bash
kubectl apply -f api-gateway.yaml
```

Deploy the Reporting Service (generates reports and analytics):

```bash
kubectl apply -f reporting.yaml
```

### 5. Apply Network Policies

Finally, apply network policies to secure communication between services:

```bash
kubectl apply -f network-policies.yaml
```

## Configuration

### Environment-Specific Configuration

Each service manifest includes a ConfigMap that can be customized for different environments. To customize configuration for a specific environment:

1. Extract the ConfigMap from the service manifest:

```bash
kubectl -n protocol get configmap protocol-config -o yaml > protocol-config.yaml
```

2. Modify the configuration values in the extracted file

3. Apply the updated ConfigMap:

```bash
kubectl apply -f protocol-config.yaml
```

4. Restart the service to apply the new configuration:

```bash
kubectl -n protocol rollout restart deployment protocol-service
```

### Resource Allocation

Resource requests and limits are defined in each service manifest. For production environments, adjust these values based on your workload requirements:

| Service | CPU Request | CPU Limit | Memory Request | Memory Limit |
|---------|-------------|-----------|----------------|-------------|
| Protocol | 0.5 cores | 1 core | 512MB | 1GB |
| Position | 1 core | 2 cores | 1GB | 2GB |
| Event | 0.5 cores | 1 core | 512MB | 1GB |
| Notification | 0.3 cores | 0.5 cores | 256MB | 512MB |
| API Gateway | 0.3 cores | 0.5 cores | 256MB | 512MB |
| Reporting | 0.5 cores | 1 core | 512MB | 1GB |

To modify resource allocation, edit the deployment section in the respective service manifest.

### Scaling Configuration

Each service includes a HorizontalPodAutoscaler (HPA) configuration. To modify scaling parameters:

1. Extract the HPA configuration:

```bash
kubectl -n protocol get hpa protocol-service-hpa -o yaml > protocol-hpa.yaml
```

2. Modify the min/max replicas and scaling metrics

3. Apply the updated HPA configuration:

```bash
kubectl apply -f protocol-hpa.yaml
```

## Accessing the Services

### API Gateway

The API Gateway Service exposes the Traccar REST API and WebSocket interface. Access is configured through an Ingress resource in the `api-gateway.yaml` manifest.

To get the external IP or hostname:

```bash
kubectl -n api-gateway get ingress api-gateway-ingress
```

Access the API at: `https://<INGRESS_ADDRESS>/api`

### Protocol Service

The Protocol Service exposes multiple ports for different device protocols. These are configured as NodePort services to allow direct device connections.

To get the node ports for a specific protocol:

```bash
kubectl -n protocol get service protocol-service -o jsonpath='{.spec.ports[?(@.name=="<PROTOCOL_NAME>")].nodePort}'
```

Configure devices to connect to: `<NODE_IP>:<NODE_PORT>`

## Monitoring and Health Checks

Each service exposes health check endpoints that can be used for monitoring:

- Liveness probe: `/health/liveness`
- Readiness probe: `/health/readiness`

To check the health of a service:

```bash
kubectl -n <NAMESPACE> port-forward service/<SERVICE_NAME> <LOCAL_PORT>:<SERVICE_PORT>
```

Then access: `http://localhost:<LOCAL_PORT>/health/liveness`

## Troubleshooting

### Common Issues

#### Pods Stuck in Pending State

**Issue**: Pods remain in `Pending` state and don't start.

**Solution**: Check for resource constraints or PersistentVolumeClaim issues:

```bash
kubectl -n <NAMESPACE> describe pod <POD_NAME>
```

#### Service Communication Failures

**Issue**: Services cannot communicate with each other.

**Solution**: Verify network policies are correctly configured:

```bash
kubectl -n <NAMESPACE> get networkpolicies
kubectl -n <NAMESPACE> describe networkpolicy <POLICY_NAME>
```

#### Protocol Service Not Receiving Device Connections

**Issue**: Devices cannot connect to the Protocol Service.

**Solution**: Verify the service is exposed correctly and check firewall rules:

```bash
kubectl -n protocol get service protocol-service
```

Ensure your cluster's nodes allow incoming traffic on the exposed NodePort range.

#### Database Connection Issues

**Issue**: Services cannot connect to the database.

**Solution**: Check database connection settings in the service ConfigMaps and verify database credentials:

```bash
kubectl -n <NAMESPACE> get configmap <SERVICE_NAME>-config -o yaml
```

### Viewing Logs

To view logs for a specific service:

```bash
kubectl -n <NAMESPACE> logs -l app=<SERVICE_NAME> --tail=100
```

For continuous log streaming:

```bash
kubectl -n <NAMESPACE> logs -l app=<SERVICE_NAME> -f
```

### Restarting Services

To restart a service after configuration changes:

```bash
kubectl -n <NAMESPACE> rollout restart deployment <SERVICE_NAME>
```

## Upgrading

To upgrade to a newer version of Traccar microservices:

1. Update the image tags in the service manifests

2. Apply the updated manifests:

```bash
kubectl apply -f <SERVICE_NAME>.yaml
```

3. Monitor the rollout status:

```bash
kubectl -n <NAMESPACE> rollout status deployment <SERVICE_NAME>
```

## Backup and Restore

### Database Backup

The Traccar microservices use an external database that should be backed up regularly. Refer to your database documentation for specific backup procedures.

### Configuration Backup

To backup all Kubernetes resources for Traccar:

```bash
kubectl -n protocol get all,configmap,secret,pvc -o yaml > protocol-backup.yaml
kubectl -n position get all,configmap,secret,pvc -o yaml > position-backup.yaml
kubectl -n event get all,configmap,secret,pvc -o yaml > event-backup.yaml
kubectl -n notification get all,configmap,secret,pvc -o yaml > notification-backup.yaml
kubectl -n api-gateway get all,configmap,secret,pvc -o yaml > api-gateway-backup.yaml
kubectl -n reporting get all,configmap,secret,pvc -o yaml > reporting-backup.yaml
kubectl -n infrastructure get all,configmap,secret,pvc -o yaml > infrastructure-backup.yaml
```

## Security Considerations

- All services run as non-root users with minimal permissions
- Network policies restrict communication to only necessary paths
- Sensitive configuration is stored in Kubernetes Secrets
- TLS is configured for external access through the API Gateway
- Regular container image updates are recommended to address security vulnerabilities

## Additional Resources

- [Traccar Documentation](https://www.traccar.org/documentation/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Traccar Support](https://www.traccar.org/support/)