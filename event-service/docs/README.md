# Event Processing Service

## Overview

The Event Processing Service is a core component of the Traccar GPS tracking platform's microservices architecture. This service is responsible for analyzing position data to detect significant events that require attention or notification.

## Service Responsibilities

The Event Processing Service performs the following key functions:

- Consumes enriched position data from the message broker
- Analyzes position data using various detection algorithms
- Detects significant events based on predefined rules and conditions
- Publishes detected events to the message broker for further processing
- Implements stateful operations for time-window and sequence-based events
- Prioritizes critical alerts over informational events

## Event Types

The service supports detection of various event types, including but not limited to:

- **Geofence Events**: Entry, exit, and dwell within defined geographic boundaries
- **Speed Events**: Speeding violations, harsh acceleration, and sudden braking
- **Device Status Events**: Power status, battery level, and connectivity changes
- **Motion Events**: Start, stop, idle, and movement detection
- **Maintenance Events**: Service intervals, engine hours, and odometer-based alerts
- **Custom Events**: User-defined conditions and compound event rules

## Architecture

The Event Processing Service follows an event-driven architecture pattern:

1. Subscribes to the `enriched-positions` topic on the message broker
2. Processes incoming position data through a pipeline of event handlers
3. Evaluates position data against configured rules and historical context
4. Generates events when rule conditions are met
5. Publishes detected events to the `events` topic on the message broker

## Documentation

This documentation provides comprehensive information about the Event Processing Service:

- [API Documentation](./api/README.md) - Service API endpoints and interfaces
- [Configuration Guide](./configuration/README.md) - Service configuration options
- [Event Rules](./rules/README.md) - Rule definition and evaluation logic
- [Deployment Guide](./deployment/README.md) - Deployment and scaling instructions
- [Development Guide](./development/README.md) - Information for developers extending the service
- [Monitoring](./monitoring/README.md) - Health checks and performance metrics

## Integration

The Event Processing Service integrates with other Traccar microservices:

- Consumes enriched position data from the **Position Processing Service**
- Publishes detected events to the **Notification Service**
- Provides event data to the **Reporting Service** for historical analysis
- Exposes event status information to the **API Gateway Service**

## Getting Started

To get started with the Event Processing Service:

1. Review the [Configuration Guide](./configuration/README.md) to understand available options
2. See the [Deployment Guide](./deployment/README.md) for setup instructions
3. Check the [API Documentation](./api/README.md) for service interfaces
4. Explore the [Event Rules](./rules/README.md) to understand event detection logic

## Contributing

Contributions to the Event Processing Service are welcome. Please refer to the [Development Guide](./development/README.md) for information on development setup, coding standards, and contribution process.