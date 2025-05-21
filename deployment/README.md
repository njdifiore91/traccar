# Traccar Microservices Deployment

## Overview

This directory contains deployment configurations and resources for the Traccar GPS tracking system microservices architecture. The deployment architecture is designed for scalability, resilience, and maintainability across various cloud environments.

## Architecture

Traccar has been refactored from a monolithic architecture to a distributed microservices architecture consisting of the following core services:

- **Protocol Service**: Handles device communications across 200+ protocols using Netty for network I/O
- **Position Processing Service**: Processes raw GPS position data through the complete handler pipeline
- **Event Processing Service**: Detects and processes events based on position data (geofencing, overspeed, etc.)
- **Notification Service**: Manages multi-channel notifications (email, SMS, push) based on events
- **API Gateway Service**: Provides external REST API and WebSocket interfaces while routing to backend services
- **Reporting Service**: Generates various reports based on historical position and event data

These services communicate primarily through asynchronous messaging via a message broker, with service discovery mechanisms for dynamic service registration and location.

## Deployment Options

### Recommended Production Environment

- **Kubernetes with Helm** (Kubernetes 1.25+ and Helm 3.11+)
  - Provides robust orchestration, scaling, and lifecycle management
  - Helm charts available in the `setup/helm` directory
  - Supports rolling updates and zero-downtime deployments

### Supported Cloud Providers

- **Amazon Web Services (AWS)**
  - Amazon Elastic Kubernetes Service (EKS)
  - Amazon RDS for database
  - Amazon MSK for Kafka message broker

- **Google Cloud Platform (GCP)**
  - Google Kubernetes Engine (GKE)
  - Cloud SQL for database
  - Confluent Cloud for Kafka

- **Microsoft Azure**
  - Azure Kubernetes Service (AKS)
  - Azure Database for PostgreSQL/MySQL
  - Azure Event Hubs for messaging

- **DigitalOcean**
  - DigitalOcean Kubernetes
  - DigitalOcean Managed Database
  - Self-hosted Kafka/RabbitMQ on Kubernetes

### Development Environment

- **Docker Compose**
  - Simplified local development setup
  - Available in the `deployment/docker-compose` directory
  - Not recommended for production use

## Infrastructure Components

### Kubernetes Resources

The Traccar microservices deployment uses the following Kubernetes resources:

- **Deployments**: For stateless services (Protocol, Position, Event, Notification, API Gateway, Reporting)
- **StatefulSets**: For stateful components (Message broker, Service discovery, Distributed cache)
- **Services**: For internal service discovery and load balancing
- **Ingress**: For external access to the API Gateway
- **ConfigMaps**: For service configuration
- **Secrets**: For sensitive configuration (credentials, tokens, etc.)
- **HorizontalPodAutoscalers**: For automatic scaling based on load

### Message Broker

The message broker is a critical component for inter-service communication:

- **Kafka** (recommended for high-throughput deployments)
  - Provides reliable, scalable messaging
  - Supports message persistence and replay
  - Enables event sourcing patterns

- **RabbitMQ** (alternative for smaller deployments)
  - Lower resource requirements
  - Simpler setup and management
  - Rich routing capabilities

### Service Discovery

Service discovery enables dynamic service registration and location:

- **Kubernetes Service Discovery** (default)
  - Uses Kubernetes Services for DNS-based discovery
  - Integrated with Kubernetes health checks

- **Consul** (optional for advanced use cases)
  - Provides additional service metadata
  - Supports cross-cluster discovery
  - Enhanced health checking capabilities

## Deployment Structure

```
deployment/
├── ci/                     # CI/CD pipeline configurations
├── scripts/                # Deployment and maintenance scripts
├── environments/           # Environment-specific configurations
│   ├── dev/                # Development environment
│   ├── staging/            # Staging environment
│   ├── prod/               # Production environment
│   └── common/             # Shared configurations
└── terraform/              # Infrastructure as Code
    ├── modules/            # Reusable Terraform modules
    ├── aws/                # AWS-specific configurations
    ├── gcp/                # GCP-specific configurations
    ├── azure/              # Azure-specific configurations
    └── digitalocean/       # DigitalOcean-specific configurations
```

## Getting Started

### Prerequisites

- Kubernetes cluster (v1.25+)
- Helm (v3.11+)
- kubectl configured to access your cluster
- Container registry access

### Deployment Steps

1. **Configure environment variables**

   Copy the appropriate environment template and customize it for your deployment:

   ```bash
   cp deployment/environments/prod/example.env deployment/environments/prod/.env
   # Edit .env file with your specific configuration
   ```

2. **Deploy infrastructure components**

   ```bash
   # Using Terraform (example for AWS)
   cd deployment/terraform/aws
   terraform init
   terraform apply
   ```

3. **Deploy Traccar microservices**

   ```bash
   # Using Helm
   cd setup/helm
   helm dependency update
   helm install traccar . -f values.yaml -f deployment/environments/prod/values-override.yaml
   ```

4. **Verify deployment**

   ```bash
   kubectl get pods -n traccar
   kubectl get services -n traccar
   ```

## Scaling

The Traccar microservices architecture supports both horizontal and vertical scaling:

### Horizontal Scaling

- **Manual scaling**: Adjust replica count in Helm values or using kubectl
  ```bash
  kubectl scale deployment protocol-service --replicas=5 -n traccar
  ```

- **Automatic scaling**: HorizontalPodAutoscaler based on CPU, memory, or custom metrics
  ```yaml
  # Example HPA configuration (already included in Helm charts)
  apiVersion: autoscaling/v2
  kind: HorizontalPodAutoscaler
  metadata:
    name: protocol-service-hpa
  spec:
    scaleTargetRef:
      apiVersion: apps/v1
      kind: Deployment
      name: protocol-service
    minReplicas: 2
    maxReplicas: 10
    metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
  ```

### Vertical Scaling

- Adjust resource requests and limits in Helm values
  ```yaml
  protocol:
    resources:
      requests:
        cpu: 0.5
        memory: 512Mi
      limits:
        cpu: 1
        memory: 1Gi
  ```

## Monitoring and Observability

The deployment includes comprehensive monitoring and observability capabilities:

### Logging

- Centralized logging with structured JSON format
- Correlation IDs for request tracing across services
- Log aggregation using Elasticsearch, Fluentd, and Kibana (EFK) stack

### Metrics

- Prometheus metrics exposed by all services
- Grafana dashboards for visualization
- Alerting based on service health and performance metrics

### Distributed Tracing

- OpenTelemetry integration for end-to-end request tracking
- Jaeger or Zipkin for trace visualization
- Performance bottleneck identification

## Troubleshooting

### Common Issues

1. **Service not starting**
   - Check pod logs: `kubectl logs <pod-name> -n traccar`
   - Verify ConfigMaps and Secrets are correctly mounted
   - Check resource constraints and node capacity

2. **Inter-service communication failures**
   - Verify message broker connectivity
   - Check service discovery registration
   - Inspect network policies

3. **Performance degradation**
   - Monitor resource utilization
   - Check database query performance
   - Analyze message broker lag

### Diagnostic Commands

```bash
# Check pod status
kubectl get pods -n traccar

# View pod logs
kubectl logs <pod-name> -n traccar

# Describe pod for events and conditions
kubectl describe pod <pod-name> -n traccar

# Check service endpoints
kubectl get endpoints -n traccar

# Port-forward to a service for direct access
kubectl port-forward svc/<service-name> <local-port>:<service-port> -n traccar
```

## Backup and Recovery

### Database Backup

- Automated daily backups configured via cloud provider
- Point-in-time recovery options
- Backup retention policies based on environment

### Configuration Backup

- All configuration stored in version control
- Helm release history maintained
- ConfigMaps and Secrets backed up regularly

### Disaster Recovery

- Multi-region deployment options for critical environments
- Documented recovery procedures in `deployment/docs/disaster-recovery.md`
- Regular recovery testing and validation

## Security Considerations

- Non-root container execution
- Network policies for service isolation
- Secret management using Kubernetes Secrets or external vault
- TLS encryption for all service communication
- Regular security scanning of container images

## Additional Resources

- [Traccar Documentation](https://www.traccar.org/documentation/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Helm Documentation](https://helm.sh/docs/)
- [Terraform Documentation](https://www.terraform.io/docs)

## Support

For issues related to deployment, please open an issue in the GitHub repository or contact the Traccar support team.