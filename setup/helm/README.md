# Traccar Helm Chart

## Introduction

This Helm chart deploys Traccar, an open-source GPS tracking system, on a Kubernetes cluster. This chart supports Traccar's microservices architecture, providing a scalable and resilient deployment for production environments.

Traccar's microservices architecture consists of the following components:

- **Protocol Service**: Handles device connections and protocol decoding for 200+ GPS protocols
- **Position Processing Service**: Processes and enriches position data
- **Event Processing Service**: Analyzes position data to detect events
- **Notification Service**: Manages delivery of alerts through multiple channels
- **API Gateway Service**: Routes client requests and provides unified API access
- **Reporting Service**: Generates reports and analytics

## Prerequisites

- Kubernetes 1.23+
- Helm 3.11+
- PV provisioner support in the underlying infrastructure (if persistence is enabled)
- Message broker (Kafka or RabbitMQ) for inter-service communication
- Database (MySQL, PostgreSQL, or other supported database)

## Installing the Chart

### Add the Traccar Helm repository

```bash
helm repo add traccar https://charts.traccar.org
helm repo update
```

### Install the chart with the release name `traccar`

```bash
helm install traccar traccar/traccar
```

The command deploys Traccar on the Kubernetes cluster with default configuration. The [Parameters](#parameters) section lists the parameters that can be configured during installation.

### Install with custom values

```bash
helm install traccar traccar/traccar -f values.yaml
```

## Uninstalling the Chart

To uninstall/delete the `traccar` deployment:

```bash
helm uninstall traccar
```

This removes all the Kubernetes components associated with the chart and deletes the release.

## Parameters

### Global Parameters

| Name                      | Description                                     | Value           |
| ------------------------- | ----------------------------------------------- | --------------- |
| `global.imageRegistry`    | Global Docker image registry                    | `""`            |
| `global.imagePullSecrets` | Global Docker registry secret names             | `[]`            |
| `global.storageClass`     | Global StorageClass for Persistent Volume(s)    | `""`            |

### Common Parameters

| Name                     | Description                                                                             | Value           |
| ------------------------ | --------------------------------------------------------------------------------------- | --------------- |
| `nameOverride`           | String to partially override common.names.fullname                                      | `""`            |
| `fullnameOverride`       | String to fully override common.names.fullname                                          | `""`            |
| `kubeVersion`            | Force target Kubernetes version (using Helm capabilities if not set)                   | `""`            |
| `clusterDomain`          | Default Kubernetes cluster domain                                                       | `cluster.local` |

### Database Parameters

| Name                                   | Description                                                               | Value                |
| -------------------------------------- | ------------------------------------------------------------------------- | -------------------- |
| `database.type`                        | Database type (mysql, postgresql)                                         | `mysql`              |
| `database.host`                        | Database host                                                             | `mysql`              |
| `database.port`                        | Database port                                                             | `3306`               |
| `database.name`                        | Database name                                                             | `traccar`            |
| `database.username`                    | Database username                                                          | `traccar`            |
| `database.password`                     | Database password                                                          | `""`                 |
| `database.existingSecret`              | Name of existing secret to use for database credentials                   | `""`                 |
| `database.existingSecretUsernameKey`   | Key of existing secret for database username                              | `username`           |
| `database.existingSecretPasswordKey`   | Key of existing secret for database password                              | `password`           |

### Message Broker Parameters

| Name                                   | Description                                                               | Value                |
| -------------------------------------- | ------------------------------------------------------------------------- | -------------------- |
| `messageBroker.type`                   | Message broker type (kafka, rabbitmq)                                     | `kafka`              |
| `messageBroker.host`                   | Message broker host                                                       | `kafka`              |
| `messageBroker.port`                   | Message broker port                                                       | `9092`               |
| `messageBroker.username`               | Message broker username                                                   | `""`                 |
| `messageBroker.password`               | Message broker password                                                   | `""`                 |
| `messageBroker.existingSecret`         | Name of existing secret to use for message broker credentials             | `""`                 |

### Protocol Service Parameters

| Name                                   | Description                                                               | Value                |
| -------------------------------------- | ------------------------------------------------------------------------- | -------------------- |
| `protocol.enabled`                     | Enable Protocol Service                                                   | `true`               |
| `protocol.image.repository`            | Protocol Service image repository                                         | `traccar/protocol`   |
| `protocol.image.tag`                   | Protocol Service image tag                                                | `latest`             |
| `protocol.image.pullPolicy`            | Protocol Service image pull policy                                        | `IfNotPresent`       |
| `protocol.replicaCount`                | Number of Protocol Service replicas                                       | `2`                  |
| `protocol.resources.limits.cpu`        | Protocol Service CPU limit                                               | `1`                  |
| `protocol.resources.limits.memory`     | Protocol Service memory limit                                            | `1Gi`                |
| `protocol.resources.requests.cpu`      | Protocol Service CPU request                                             | `500m`               |
| `protocol.resources.requests.memory`   | Protocol Service memory request                                          | `512Mi`              |
| `protocol.service.type`                | Protocol Service Kubernetes service type                                  | `ClusterIP`          |
| `protocol.service.ports`               | Protocol Service ports configuration                                      | See values.yaml      |
| `protocol.autoscaling.enabled`         | Enable autoscaling for Protocol Service                                   | `true`               |
| `protocol.autoscaling.minReplicas`     | Minimum number of Protocol Service replicas                              | `2`                  |
| `protocol.autoscaling.maxReplicas`     | Maximum number of Protocol Service replicas                              | `10`                 |
| `protocol.autoscaling.targetCPU`       | Target CPU utilization percentage for Protocol Service                    | `70`                 |
| `protocol.autoscaling.targetMemory`    | Target memory utilization percentage for Protocol Service                 | `80`                 |

### Position Service Parameters

| Name                                   | Description                                                               | Value                |
| -------------------------------------- | ------------------------------------------------------------------------- | -------------------- |
| `position.enabled`                     | Enable Position Service                                                   | `true`               |
| `position.image.repository`            | Position Service image repository                                         | `traccar/position`   |
| `position.image.tag`                   | Position Service image tag                                                | `latest`             |
| `position.image.pullPolicy`            | Position Service image pull policy                                        | `IfNotPresent`       |
| `position.replicaCount`                | Number of Position Service replicas                                       | `2`                  |
| `position.resources.limits.cpu`        | Position Service CPU limit                                               | `2`                  |
| `position.resources.limits.memory`     | Position Service memory limit                                            | `2Gi`                |
| `position.resources.requests.cpu`      | Position Service CPU request                                             | `1`                  |
| `position.resources.requests.memory`   | Position Service memory request                                          | `1Gi`                |

### Event Service Parameters

| Name                                   | Description                                                               | Value                |
| -------------------------------------- | ------------------------------------------------------------------------- | -------------------- |
| `event.enabled`                        | Enable Event Service                                                      | `true`               |
| `event.image.repository`               | Event Service image repository                                            | `traccar/event`      |
| `event.image.tag`                      | Event Service image tag                                                   | `latest`             |
| `event.image.pullPolicy`               | Event Service image pull policy                                           | `IfNotPresent`       |
| `event.replicaCount`                   | Number of Event Service replicas                                          | `2`                  |
| `event.resources.limits.cpu`           | Event Service CPU limit                                                  | `1`                  |
| `event.resources.limits.memory`        | Event Service memory limit                                               | `1Gi`                |
| `event.resources.requests.cpu`         | Event Service CPU request                                                | `500m`               |
| `event.resources.requests.memory`      | Event Service memory request                                             | `512Mi`              |

### Notification Service Parameters

| Name                                   | Description                                                               | Value                    |
| -------------------------------------- | ------------------------------------------------------------------------- | ------------------------ |
| `notification.enabled`                 | Enable Notification Service                                               | `true`                   |
| `notification.image.repository`        | Notification Service image repository                                     | `traccar/notification`   |
| `notification.image.tag`               | Notification Service image tag                                            | `latest`                 |
| `notification.image.pullPolicy`        | Notification Service image pull policy                                    | `IfNotPresent`           |
| `notification.replicaCount`            | Number of Notification Service replicas                                   | `2`                      |
| `notification.resources.limits.cpu`    | Notification Service CPU limit                                           | `500m`                   |
| `notification.resources.limits.memory` | Notification Service memory limit                                        | `512Mi`                  |
| `notification.resources.requests.cpu`  | Notification Service CPU request                                         | `300m`                   |
| `notification.resources.requests.memory` | Notification Service memory request                                    | `256Mi`                  |

### API Gateway Parameters

| Name                                   | Description                                                               | Value                    |
| -------------------------------------- | ------------------------------------------------------------------------- | ------------------------ |
| `apiGateway.enabled`                   | Enable API Gateway Service                                                | `true`                   |
| `apiGateway.image.repository`          | API Gateway Service image repository                                      | `traccar/api-gateway`    |
| `apiGateway.image.tag`                 | API Gateway Service image tag                                             | `latest`                 |
| `apiGateway.image.pullPolicy`          | API Gateway Service image pull policy                                     | `IfNotPresent`           |
| `apiGateway.replicaCount`              | Number of API Gateway Service replicas                                    | `2`                      |
| `apiGateway.resources.limits.cpu`      | API Gateway Service CPU limit                                            | `500m`                   |
| `apiGateway.resources.limits.memory`   | API Gateway Service memory limit                                         | `512Mi`                  |
| `apiGateway.resources.requests.cpu`    | API Gateway Service CPU request                                          | `300m`                   |
| `apiGateway.resources.requests.memory` | API Gateway Service memory request                                       | `256Mi`                  |
| `apiGateway.service.type`              | API Gateway Service Kubernetes service type                               | `ClusterIP`              |
| `apiGateway.service.port`              | API Gateway Service port                                                 | `8080`                   |
| `apiGateway.ingress.enabled`           | Enable ingress for API Gateway Service                                    | `false`                  |
| `apiGateway.ingress.className`         | Ingress class name                                                        | `""`                     |
| `apiGateway.ingress.hosts`             | Ingress hosts configuration                                               | See values.yaml          |
| `apiGateway.ingress.tls`               | Ingress TLS configuration                                                 | `[]`                     |

### Reporting Service Parameters

| Name                                   | Description                                                               | Value                    |
| -------------------------------------- | ------------------------------------------------------------------------- | ------------------------ |
| `reporting.enabled`                    | Enable Reporting Service                                                  | `true`                   |
| `reporting.image.repository`           | Reporting Service image repository                                        | `traccar/reporting`      |
| `reporting.image.tag`                  | Reporting Service image tag                                               | `latest`                 |
| `reporting.image.pullPolicy`           | Reporting Service image pull policy                                       | `IfNotPresent`           |
| `reporting.replicaCount`               | Number of Reporting Service replicas                                      | `2`                      |
| `reporting.resources.limits.cpu`       | Reporting Service CPU limit                                              | `1`                      |
| `reporting.resources.limits.memory`    | Reporting Service memory limit                                           | `1Gi`                    |
| `reporting.resources.requests.cpu`     | Reporting Service CPU request                                            | `500m`                   |
| `reporting.resources.requests.memory`  | Reporting Service memory request                                         | `512Mi`                  |

## Configuration

### Custom Configuration

You can customize the configuration for each service by providing a values file:

```yaml
# values.yaml
protocol:
  replicaCount: 3
  resources:
    limits:
      cpu: 2
      memory: 2Gi

database:
  host: my-database-host
  port: 3306
  name: traccar
  username: traccar
  password: my-password

messageBroker:
  type: kafka
  host: my-kafka-host
  port: 9092
```

Then install the chart with:

```bash
helm install traccar traccar/traccar -f values.yaml
```

### Using Existing Secrets

For sensitive information like database credentials, you can use existing Kubernetes secrets:

```yaml
database:
  existingSecret: traccar-db-credentials
  existingSecretUsernameKey: username
  existingSecretPasswordKey: password
```

## Upgrading

### To 1.0.0

This is the first major release of the Traccar Helm chart for the microservices architecture. If you're migrating from the monolithic Traccar deployment, please follow these steps:

1. Back up your existing Traccar database
2. Deploy the new microservices architecture with the same database configuration
3. Verify that all services are running correctly

```bash
helm upgrade traccar traccar/traccar -f values.yaml
```

## Examples

### Minimal Production Deployment

```yaml
# minimal-production.yaml
global:
  storageClass: "standard"

database:
  host: "production-db-host"
  name: "traccar"
  username: "traccar"
  existingSecret: "traccar-db-credentials"

messageBroker:
  type: "kafka"
  host: "kafka-host"
  port: 9092

protocol:
  replicaCount: 3

position:
  replicaCount: 3

event:
  replicaCount: 2

notification:
  replicaCount: 2

apiGateway:
  replicaCount: 3
  ingress:
    enabled: true
    className: "nginx"
    hosts:
      - host: traccar.example.com
        paths:
          - path: /
            pathType: Prefix
    tls:
      - secretName: traccar-tls
        hosts:
          - traccar.example.com

reporting:
  replicaCount: 2
```

### High Availability Deployment

```yaml
# high-availability.yaml
global:
  storageClass: "premium-rwo"

database:
  host: "ha-db-host"
  name: "traccar"
  username: "traccar"
  existingSecret: "traccar-db-credentials"

messageBroker:
  type: "kafka"
  host: "kafka-host"
  port: 9092

protocol:
  replicaCount: 5
  autoscaling:
    enabled: true
    minReplicas: 5
    maxReplicas: 20
    targetCPU: 70
    targetMemory: 80

position:
  replicaCount: 5
  autoscaling:
    enabled: true
    minReplicas: 5
    maxReplicas: 15

event:
  replicaCount: 3
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 10

notification:
  replicaCount: 3
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 10

apiGateway:
  replicaCount: 5
  autoscaling:
    enabled: true
    minReplicas: 5
    maxReplicas: 15
  ingress:
    enabled: true
    className: "nginx"
    hosts:
      - host: traccar.example.com
        paths:
          - path: /
            pathType: Prefix
    tls:
      - secretName: traccar-tls
        hosts:
          - traccar.example.com

reporting:
  replicaCount: 3
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 10
```

## Troubleshooting

### Common Issues

#### Services not starting

Check the logs of the failing service:

```bash
kubectl logs -l app.kubernetes.io/name=traccar-protocol -c protocol
```

Verify that the database and message broker are accessible from the Kubernetes cluster.

#### Database connection issues

Ensure that the database credentials are correct and that the database is accessible from the Kubernetes cluster. You can check the database connection by running a temporary pod:

```bash
kubectl run mysql-client --rm --tty -i --restart='Never' --image=mysql:5.7 --namespace default --command -- mysql -h <database-host> -u <username> -p<password>
```

#### Message broker connection issues

Verify that the message broker is accessible and properly configured:

```bash
# For Kafka
kubectl run kafka-client --rm --tty -i --restart='Never' --image=bitnami/kafka:latest --namespace default --command -- kafka-topics.sh --bootstrap-server <kafka-host>:<kafka-port> --list

# For RabbitMQ
kubectl run rabbitmq-client --rm --tty -i --restart='Never' --image=rabbitmq:management --namespace default --command -- rabbitmqctl -n rabbit@<rabbitmq-host> list_queues
```

## License

Traccar is licensed under the Apache License 2.0. See the [LICENSE](https://github.com/traccar/traccar/blob/master/LICENSE) file for details.