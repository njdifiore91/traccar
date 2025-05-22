/*
 * Copyright 2020 - 2023 Anton Tananaev (anton@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.config;

/**
 * Defines the scope and hierarchy of configuration keys in the Traccar system.
 * Key types determine where configuration values are stored and how they are inherited
 * across the system.
 */
public enum KeyType {
    /**
     * Global system-wide configuration that applies to all services.
     * This is the highest level in the configuration hierarchy.
     */
    CONFIG,

    /**
     * Server-specific configuration that applies to a single server instance.
     * Inherits from CONFIG level if not explicitly defined.
     */
    SERVER,

    /**
     * User-specific configuration that can be customized per user.
     * Inherits from SERVER level if not explicitly defined.
     */
    USER,

    /**
     * Device-specific configuration that can be customized per device.
     * Inherits from USER level if not explicitly defined.
     */
    DEVICE,

    /**
     * Service-specific configuration that applies to a particular microservice.
     * Inherits from CONFIG level if not explicitly defined.
     * Used for service-specific settings like ports, thread pools, and service behavior.
     */
    SERVICE,

    /**
     * Messaging-specific configuration for inter-service communication.
     * Includes settings for message brokers, topics, queues, and delivery guarantees.
     * Inherits from SERVICE level if not explicitly defined.
     */
    MESSAGING,

    /**
     * Tracing-specific configuration for distributed tracing and observability.
     * Includes settings for trace sampling, span collection, and exporter configuration.
     * Inherits from SERVICE level if not explicitly defined.
     */
    TRACING,

    /**
     * Metrics-specific configuration for performance monitoring and alerting.
     * Includes settings for metric collection, aggregation, and reporting.
     * Inherits from SERVICE level if not explicitly defined.
     */
    METRICS,

    /**
     * Resilience-specific configuration for fault tolerance and recovery.
     * Includes settings for circuit breakers, retries, timeouts, and bulkheads.
     * Inherits from SERVICE level if not explicitly defined.
     */
    RESILIENCE;

    /**
     * Checks if this key type is valid for the specified service.
     * 
     * @param serviceName The name of the service to check against
     * @return true if this key type is valid for the specified service, false otherwise
     */
    public boolean isValidForService(String serviceName) {
        switch (this) {
            case CONFIG:
            case SERVER:
                return true; // These are always valid for all services
            case SERVICE:
            case MESSAGING:
            case TRACING:
            case METRICS:
            case RESILIENCE:
                // These are valid for specific services based on their capabilities
                // Implementation would check if the service supports this key type
                return true; // Default to true for now, would be customized per service
            case USER:
            case DEVICE:
                // These are typically only valid for user-facing services
                return "api-gateway".equals(serviceName) || 
                       "position-service".equals(serviceName) || 
                       "event-service".equals(serviceName);
            default:
                return false;
        }
    }

    /**
     * Gets the parent key type in the configuration hierarchy.
     * Used for implementing configuration inheritance.
     * 
     * @return The parent key type, or null if this is the root type
     */
    public KeyType getParent() {
        switch (this) {
            case SERVER:
                return CONFIG;
            case USER:
                return SERVER;
            case DEVICE:
                return USER;
            case SERVICE:
                return CONFIG;
            case MESSAGING:
            case TRACING:
            case METRICS:
            case RESILIENCE:
                return SERVICE;
            default:
                return null; // CONFIG has no parent
        }
    }

    /**
     * Checks if this key type is applicable across multiple services.
     * 
     * @return true if this key type can be shared across services, false if it's service-specific
     */
    public boolean isCrossService() {
        switch (this) {
            case CONFIG:
            case SERVER:
            case USER:
            case DEVICE:
                return true;
            default:
                return false;
        }
    }
}