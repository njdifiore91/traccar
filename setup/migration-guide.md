# Traccar Migration Guide: Monolith to Microservices Architecture

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Migration Overview](#migration-overview)
4. [Pre-Migration Preparation](#pre-migration-preparation)
5. [Data Migration](#data-migration)
6. [Configuration Mapping](#configuration-mapping)
7. [Step-by-Step Migration Procedure](#step-by-step-migration-procedure)
8. [Post-Migration Verification](#post-migration-verification)
9. [Rollback Procedure](#rollback-procedure)
10. [Troubleshooting](#troubleshooting)
11. [Frequently Asked Questions](#frequently-asked-questions)
12. [Additional Resources](#additional-resources)

## Introduction

This guide provides comprehensive instructions for migrating from the traditional Traccar monolithic architecture to the new microservices-based architecture. The migration process has been designed to ensure minimal downtime, data integrity, and a smooth transition while maintaining all existing functionality and API compatibility.

> **Important**: This migration guide is essential for existing Traccar users who need to transition to the new microservices architecture. The guide ensures that you can maintain your data and configurations while benefiting from the improved scalability and resilience of the microservices approach.

The new microservices architecture offers several advantages:

- **Independent Scaling**: Scale individual components based on specific workload demands
- **Fault Isolation**: Failures in one service do not cascade to the entire system
- **Accelerated Deployment**: Faster release cadence through containerized services
- **Enhanced Resilience**: Improved system stability through asynchronous messaging
- **Centralized Observability**: Comprehensive monitoring through unified logging and metrics

## Prerequisites

### Hardware Requirements

| Component | Minimum Specification | Recommended Specification |
|-----------|------------------------|---------------------------|
| CPU | 4 cores | 8+ cores |
| RAM | 8 GB | 16+ GB |
| Storage | 50 GB SSD | 100+ GB SSD |
| Network | 100 Mbps | 1 Gbps |

### Software Requirements

- **Docker**: Version 20.10.0 or later
- **Kubernetes**: Version 1.24.0 or later (if using Kubernetes deployment)
- **Docker Compose**: Version 2.10.0 or later (if using Docker Compose deployment)
- **Database**: MySQL 8.0+, PostgreSQL 13.0+, or Microsoft SQL Server 2019+ (same as your current installation)
- **Message Broker**: Kafka 3.0+ or RabbitMQ 3.9+ (new requirement)

### Knowledge Prerequisites

- Basic understanding of containerization concepts (Docker)
- Familiarity with container orchestration (Kubernetes or Docker Compose)
- Understanding of database backup and restoration procedures
- Basic networking knowledge (ports, DNS, load balancing)

## Migration Overview

The migration process involves the following high-level steps:

1. **Preparation**: Backup data, document current configuration, and prepare the new environment
2. **Installation**: Deploy the microservices infrastructure components
3. **Data Migration**: Transfer existing data to the new architecture
4. **Configuration**: Map and apply existing configuration to the new services
5. **Verification**: Test functionality and performance
6. **Cutover**: Redirect traffic to the new system
7. **Monitoring**: Observe system behavior and address any issues

## Pre-Migration Preparation

### 1. Document Current Environment

Before beginning the migration, document your current Traccar environment:

```bash
# Create a directory for migration documentation
mkdir -p traccar-migration/docs

# Save current Traccar version
traccar version > traccar-migration/docs/version.txt

# Copy configuration files
cp /path/to/traccar/conf/traccar.xml traccar-migration/docs/
cp /path/to/traccar/conf/default.xml traccar-migration/docs/

# List installed plugins or extensions
ls -la /path/to/traccar/plugins/ > traccar-migration/docs/plugins.txt

# Document custom modifications (if any)
# Note any custom code, SQL scripts, or modifications made to the standard installation
```

### 2. Backup Existing Data

Create a complete backup of your Traccar database:

```bash
# For MySQL
mysqldump -u username -p --all-databases > traccar-migration/traccar-backup.sql

# For PostgreSQL
pg_dump -U username -W -F c -b -v -f traccar-migration/traccar-backup.dump traccar

# For Microsoft SQL Server
sqlcmd -S server -U username -P password -Q "BACKUP DATABASE traccar TO DISK='traccar-migration/traccar-backup.bak'"
```

Also backup any media files or additional data:

```bash
# Backup media directory if used
cp -r /path/to/traccar/media traccar-migration/media-backup
```

### 3. Test Backup Restoration

Verify that your backup can be successfully restored:

```bash
# Create a test database (example for MySQL)
mysql -u username -p -e "CREATE DATABASE traccar_test;"

# Restore backup to test database
mysql -u username -p traccar_test < traccar-migration/traccar-backup.sql

# Verify data integrity
mysql -u username -p -e "SELECT COUNT(*) FROM devices;" traccar_test
mysql -u username -p -e "SELECT COUNT(*) FROM positions;" traccar_test
```

### 4. Plan Maintenance Window

Schedule a maintenance window for the migration. The duration will depend on your database size and system complexity:

- Small installations (< 50 devices, < 1 million positions): 2-4 hours
- Medium installations (50-500 devices, 1-10 million positions): 4-8 hours
- Large installations (500+ devices, 10+ million positions): 8-24 hours

Notify all users of the planned maintenance window.

## Data Migration

### Database Migration Strategy

The microservices architecture maintains database schema compatibility with the monolithic version, allowing for a straightforward migration process. This ensures that all your historical position data, device configurations, geofences, and user accounts are preserved during migration. There are two recommended approaches:

#### Option 1: Direct Database Reuse (Recommended for smaller installations)

This approach uses your existing database with the new microservices:

1. Stop the monolithic Traccar server
2. Backup the database (as described in the preparation section)
3. Deploy the microservices with connection to the existing database
4. The services will automatically apply any necessary schema updates

#### Option 2: Parallel Database (Recommended for larger installations)

This approach creates a new database instance for the microservices:

1. Create a new database instance
2. Restore the backup to the new database
3. Deploy the microservices with connection to the new database
4. Run the data verification tools to ensure data integrity

### Media and Configuration Files

For media files and custom configurations:

1. Map the media directory to the appropriate microservice (typically the API Gateway)
2. Convert configuration settings as described in the Configuration Mapping section

## Configuration Mapping

The monolithic Traccar uses a centralized `traccar.xml` configuration file. In the microservices architecture, configuration is distributed across services. Use the following mapping to migrate your settings:

### Core Settings Mapping

| Monolithic Setting | Microservice | Configuration File | Notes |
|-------------------|--------------|-------------------|-------|
| `database.driver` | All services | `database.properties` | Database driver class |
| `database.url` | All services | `database.properties` | Database connection URL |
| `database.user` | All services | `database.properties` | Database username |
| `database.password` | All services | `database.properties` | Database password |
| `web.port` | API Gateway | `api-gateway.properties` | HTTP port for web interface |
| `web.path` | API Gateway | `api-gateway.properties` | Path to web app |
| `web.debug` | API Gateway | `api-gateway.properties` | Enable web debugging |
| `geocoder.enable` | Position Service | `position-service.properties` | Enable geocoder |
| `geocoder.type` | Position Service | `position-service.properties` | Geocoder implementation |
| `geocoder.url` | Position Service | `position-service.properties` | Geocoder server URL |
| `logger.enable` | All services | `logging.properties` | Enable logging |
| `logger.level` | All services | `logging.properties` | Log level |

### Protocol Settings Mapping

| Monolithic Setting | Microservice | Configuration File | Notes |
|-------------------|--------------|-------------------|-------|
| `protocol.xyz.port` | Protocol Service | `protocol-service.properties` | Protocol-specific port |
| `protocol.xyz.timeout` | Protocol Service | `protocol-service.properties` | Protocol connection timeout |

### Notification Settings Mapping

| Monolithic Setting | Microservice | Configuration File | Notes |
|-------------------|--------------|-------------------|-------|
| `mail.smtp.host` | Notification Service | `notification-service.properties` | SMTP server host |
| `mail.smtp.port` | Notification Service | `notification-service.properties` | SMTP server port |
| `mail.smtp.starttls.enable` | Notification Service | `notification-service.properties` | Enable STARTTLS |
| `mail.smtp.username` | Notification Service | `notification-service.properties` | SMTP username |
| `mail.smtp.password` | Notification Service | `notification-service.properties` | SMTP password |
| `sms.http.url` | Notification Service | `notification-service.properties` | SMS gateway URL |

### New Microservices-Specific Settings

| Setting | Microservice | Configuration File | Description |
|---------|--------------|-------------------|-------------|
| `kafka.bootstrap.servers` | All services | `messaging.properties` | Kafka broker addresses |
| `rabbitmq.addresses` | All services | `messaging.properties` | RabbitMQ addresses |
| `service.discovery.type` | All services | `discovery.properties` | Service discovery mechanism |
| `kubernetes.namespace` | All services | `discovery.properties` | Kubernetes namespace |

## Step-by-Step Migration Procedure

### 1. Deploy Infrastructure Components

First, deploy the required infrastructure components:

```bash
# Clone the Traccar microservices repository
git clone https://github.com/traccar/traccar-microservices.git
cd traccar-microservices

# Deploy infrastructure using Docker Compose or Kubernetes

# For Docker Compose:
cd setup/docker-compose
docker-compose up -d kafka zookeeper

# For Kubernetes:
cd setup/kubernetes
kubectl apply -f infrastructure/namespace.yaml
kubectl apply -f infrastructure/kafka.yaml
kubectl apply -f infrastructure/service-discovery.yaml
```

> **Note**: The choice between Kafka and RabbitMQ depends on your specific requirements. Kafka is recommended for high-throughput scenarios with many devices, while RabbitMQ may be simpler to set up and manage for smaller deployments.

### 2. Configure Services

Create configuration files for each service based on your existing configuration:

```bash
# Create configuration directory
mkdir -p config

# Generate service configurations using the migration tool
./bin/generate-configs.sh /path/to/traccar.xml config/
```

Review and adjust the generated configuration files as needed. The migration tool automatically maps your existing monolithic configuration to the appropriate microservice configurations based on the mapping tables provided in the [Configuration Mapping](#configuration-mapping) section.

### 3. Deploy Core Services

Deploy the core services in the following order:

```bash
# For Docker Compose:
cd setup/docker-compose
docker-compose up -d protocol-service position-service event-service notification-service

# For Kubernetes:
cd setup/kubernetes
kubectl apply -f services/protocol-service.yaml
kubectl apply -f services/position-service.yaml
kubectl apply -f services/event-service.yaml
kubectl apply -f services/notification-service.yaml
```

### 4. Deploy API Gateway and Reporting Service

Once the core services are running, deploy the API Gateway and Reporting Service:

```bash
# For Docker Compose:
cd setup/docker-compose
docker-compose up -d api-gateway reporting-service

# For Kubernetes:
cd setup/kubernetes
kubectl apply -f services/api-gateway.yaml
kubectl apply -f services/reporting-service.yaml
```

### 5. Verify Service Health

Check that all services are running correctly:

```bash
# For Docker Compose:
docker-compose ps

# For Kubernetes:
kubectl get pods -n traccar
```

Verify service logs for any errors:

```bash
# For Docker Compose:
docker-compose logs protocol-service

# For Kubernetes:
kubectl logs -n traccar deployment/protocol-service
```

### 6. Migrate Traffic

Once all services are verified as healthy, migrate traffic to the new system:

#### For direct replacement:

1. Stop the monolithic Traccar server
2. Ensure the API Gateway is accessible on the same port as the previous Traccar server
3. Update DNS records if necessary

#### For parallel deployment:

1. Configure a load balancer to direct traffic to the new API Gateway
2. Gradually shift traffic from the old system to the new one (10%, 25%, 50%, 100%)
3. Monitor for any issues during the transition

## Post-Migration Verification

### Functional Verification

Verify that all key functionality is working correctly:

1. **Device Connectivity**: Confirm that devices are connecting and sending positions
   - Check the Protocol Service logs for connection events
   - Verify positions are being received in the database
   - Test multiple protocol types if your deployment uses various device models

2. **User Interface**: Test the web interface for all major functions
   - Login and authentication
   - Device list and details view
   - Map visualization of positions
   - Historical tracks and playback
   - User and permission management

3. **API Access**: Verify that all API endpoints are accessible and returning correct data
   - Test authentication endpoints
   - Verify device and position data retrieval
   - Test command sending to devices
   - Check WebSocket connections for real-time updates

4. **Reports**: Generate reports to ensure reporting functionality works
   - Test route, event, summary, and trip reports
   - Verify PDF and Excel export functionality
   - Test scheduled report generation if configured

5. **Notifications**: Test that notifications are being sent correctly
   - Trigger test events to generate notifications
   - Verify email delivery
   - Check SMS delivery if configured
   - Test push notifications to mobile apps

6. **Geofencing**: Verify that geofence events are being triggered appropriately
   - Create test geofences
   - Move devices in/out of geofences
   - Verify entry/exit events are generated
   - Check that notifications for geofence events work

### Performance Verification

Monitor system performance metrics:

```bash
# For Docker Compose with Prometheus/Grafana:
docker-compose up -d prometheus grafana
# Access Grafana at http://your-server:3000

# For Kubernetes:
kubectl apply -f monitoring/prometheus.yaml
kubectl apply -f monitoring/grafana.yaml
# Access Grafana using port-forward or ingress
```

Key metrics to monitor:

- CPU and memory usage per service
- Database query performance
- Message broker throughput
- API response times
- End-to-end latency for position processing

## Rollback Procedure

If critical issues are encountered during migration, follow these steps to rollback:

### 1. Restore Traffic to Original System

```bash
# Redirect traffic back to the original Traccar server
# This may involve DNS changes, load balancer configuration, or firewall rules
```

### 2. Stop Microservices

```bash
# For Docker Compose:
cd setup/docker-compose
docker-compose down

# For Kubernetes:
cd setup/kubernetes
kubectl delete -f services/
```

### 3. Restore Database (if modified)

If you made changes to the database schema:

```bash
# Restore from pre-migration backup
mysql -u username -p traccar < traccar-migration/traccar-backup.sql
```

### 4. Restart Original Traccar Server

```bash
# Start the original Traccar server
service traccar start
# or
/path/to/traccar/bin/traccar start
```

## Troubleshooting

### Common Issues and Solutions

This section addresses the most common issues encountered during migration and provides detailed solutions.

#### Database Connection Problems

**Issue**: Services cannot connect to the database

**Solution**:
- Verify database credentials in service configurations
- Check database server accessibility from service containers
- Ensure database user has appropriate permissions

```bash
# Test database connection
mysql -h database-host -u username -p -e "SELECT 1;"
```

#### Message Broker Connectivity

**Issue**: Services cannot connect to Kafka/RabbitMQ

**Solution**:
- Verify broker addresses in service configurations
- Check network connectivity between services and broker
- Ensure broker is running and properly configured

```bash
# For Kafka, check topic list
kafka-topics.sh --bootstrap-server kafka:9092 --list

# For RabbitMQ, check status
rabbitmqctl status
```

#### Protocol Service Not Receiving Connections

**Issue**: Devices cannot connect to the Protocol Service

**Solution**:
- Verify port mappings in container/pod configurations
- Check firewall rules allowing traffic to protocol ports
- Ensure protocol-specific settings are correctly configured

```bash
# Check if ports are open
netstat -tulpn | grep LISTEN

# Test connectivity to a protocol port
telnet your-server 5023
```

#### API Gateway Not Accessible

**Issue**: Cannot access the web interface or API

**Solution**:
- Verify API Gateway service is running
- Check port mappings and network configuration
- Ensure web resources are correctly mounted

```bash
# Test API endpoint
curl -v http://your-server:8082/api/session
```

#### Service Discovery Issues

**Issue**: Services cannot discover each other

**Solution**:
- Verify service discovery configuration
- Check that services are properly registered
- Ensure network allows communication between services
- Verify DNS resolution between services

```bash
# For Kubernetes, check service endpoints
kubectl get endpoints -n traccar

# For Consul, check service catalog
consul catalog services

# Test DNS resolution from within a service container
kubectl exec -it deployment/api-gateway -- nslookup protocol-service
```

#### Data Migration Errors

**Issue**: Data appears incomplete or incorrect after migration

**Solution**:
- Verify database schema version compatibility
- Check for any error messages during migration
- Run data validation queries to identify discrepancies

```bash
# Compare record counts
mysql -u username -p -e "SELECT COUNT(*) FROM devices;" traccar
```

## Frequently Asked Questions

### General Questions

**Q: Will my existing devices continue to work after migration?**

A: Yes, the microservices architecture maintains complete protocol compatibility with all existing devices. No reconfiguration of devices is necessary. All 200+ supported protocols continue to function exactly as before.

**Q: Do I need to update my client applications?**

A: No, the API Gateway provides the same REST API and WebSocket interfaces as the monolithic version, ensuring backward compatibility for all client applications. This includes mobile apps, web clients, and any custom integrations you've built.

**Q: Can I migrate in phases?**

A: Yes, you can use a parallel deployment approach to gradually migrate traffic from the monolithic system to the microservices architecture. This allows for testing with a subset of devices or users before full migration.

**Q: Will my custom modifications be preserved?**

A: Custom modifications to the core code will need to be adapted to the microservices architecture. The migration guide includes instructions for identifying and migrating custom code. Configuration customizations are preserved through the configuration mapping process.

### Technical Questions

**Q: How do I scale individual services?**

A: With Kubernetes, use the kubectl scale command:
```bash
kubectl scale deployment protocol-service --replicas=3 -n traccar
```
With Docker Compose, use the scale option:
```bash
docker-compose up -d --scale protocol-service=3
```

**Q: How do I monitor the health of the microservices?**

A: Each service exposes health endpoints at `/health` and `/metrics`. You can use Prometheus and Grafana for comprehensive monitoring. The setup includes pre-configured dashboards for all services.

**Q: Can I use my existing database?**

A: Yes, the microservices can use your existing database. The schema is compatible, and any necessary updates will be applied automatically. This ensures data continuity during and after migration.

**Q: How do I update individual services?**

A: With Kubernetes:
```bash
kubectl set image deployment/protocol-service protocol-service=traccar/protocol-service:new-version -n traccar
```
With Docker Compose:
```bash
docker-compose pull protocol-service
docker-compose up -d protocol-service
```

**Q: What happens if one service fails?**

A: The microservices architecture is designed for resilience. If one service fails, others continue to function, and the failed service can be restarted independently without affecting the entire system. For example, if the reporting service fails, real-time tracking will continue to work.

**Q: What are the resource requirements compared to the monolithic version?**

A: The microservices architecture may require slightly more resources in total due to the overhead of running multiple services. However, this is offset by the ability to scale individual components based on demand and to run services on separate hardware if needed.

**Q: How are database connections managed across services?**

A: Each service maintains its own connection pool to the database, optimized for its specific access patterns. Connection pooling is configured to prevent database connection exhaustion while ensuring efficient resource utilization.

**Q: Can I run the microservices on multiple servers?**

A: Yes, the microservices architecture is designed to run across multiple servers. This provides both horizontal scaling and high availability. The service discovery mechanism ensures that services can find each other regardless of their physical location.

## Additional Resources

- [Traccar Microservices Documentation](https://www.traccar.org/documentation/microservices/)
- [Traccar API Reference](https://www.traccar.org/api-reference/)
- [Docker Documentation](https://docs.docker.com/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Traccar Community Forums](https://www.traccar.org/forums/)
- [GitHub Repository](https://github.com/traccar/traccar-microservices)
- [Microservices Architecture Diagram](https://www.traccar.org/documentation/microservices/architecture.html)
- [Migration Webinars and Tutorials](https://www.traccar.org/webinars/)
- [Performance Benchmarks](https://www.traccar.org/documentation/microservices/benchmarks.html)

---

For additional assistance, please contact support@traccar.org or visit the community forums at https://www.traccar.org/forums/