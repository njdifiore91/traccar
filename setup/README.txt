Traccar is a free and open source GPS tracking system.

## ARCHITECTURE

Traccar is available in two deployment architectures:

1. Traditional Monolithic Architecture - Single application deployment
2. Microservices Architecture - Distributed deployment with independent services

### Microservices Architecture

The microservices architecture consists of the following components:

- Protocol Service - Handles device communications across 200+ protocols
- Position Processing Service - Processes raw GPS position data
- Event Processing Service - Detects and processes events based on position data
- Notification Service - Manages multi-channel notifications (email, SMS, push)
- API Gateway Service - Provides external REST API and WebSocket interfaces
- Reporting Service - Generates various reports based on historical data

## INSTALLATION

Installation instructions for the monolithic version are available on the official website:

Windows - https://www.traccar.org/windows/
Linux   - https://www.traccar.org/linux/
Docker  - https://www.traccar.org/docker/
Other   - https://www.traccar.org/manual-installation/

## CONTAINERIZATION & ORCHESTRATION

For microservices deployment, Traccar uses containerization and orchestration:

Docker Images - https://www.traccar.org/docker-microservices/
Kubernetes - https://www.traccar.org/kubernetes/
Helm Charts - https://www.traccar.org/helm-charts/

### Local Development

For local development and testing, Docker Compose configurations are available:
https://www.traccar.org/docker-compose/

### Production Deployment

For production environments, Kubernetes is recommended with Helm charts:
https://www.traccar.org/kubernetes-production/

## MIGRATION GUIDE

Existing users can migrate from monolithic to microservices architecture:

1. Migration Planning - https://www.traccar.org/migration-planning/
2. Data Migration - https://www.traccar.org/data-migration/
3. Configuration Migration - https://www.traccar.org/config-migration/
4. Deployment Strategies - https://www.traccar.org/deployment-strategies/

The microservices architecture maintains full backward compatibility with existing clients and APIs.

## SUPPORT

If you have any questions or problems visit support page:

https://www.traccar.org/support/