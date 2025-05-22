/*
 * Copyright 2020 - 2024 Anton Tananaev (anton@traccar.org)
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
 * Enum defining the scope types for configuration keys.
 */
public enum KeyType {
    /**
     * Global configuration applicable to the entire system.
     */
    CONFIG,
    
    /**
     * Server-specific configuration.
     */
    SERVER,
    
    /**
     * User-specific configuration.
     */
    USER,
    
    /**
     * Device-specific configuration.
     */
    DEVICE,
    
    /**
     * Protocol service configuration.
     */
    PROTOCOL_SERVICE,
    
    /**
     * Position service configuration.
     */
    POSITION_SERVICE,
    
    /**
     * Event service configuration.
     */
    EVENT_SERVICE,
    
    /**
     * Notification service configuration.
     */
    NOTIFICATION_SERVICE,
    
    /**
     * API Gateway service configuration.
     */
    API_GATEWAY,
    
    /**
     * Reporting service configuration.
     */
    REPORTING_SERVICE,
    
    /**
     * Message broker configuration.
     */
    MESSAGE_BROKER,
    
    /**
     * Service discovery configuration.
     */
    SERVICE_DISCOVERY,
    
    /**
     * Database configuration.
     */
    DATABASE,
    
    /**
     * Monitoring and observability configuration.
     */
    MONITORING
}