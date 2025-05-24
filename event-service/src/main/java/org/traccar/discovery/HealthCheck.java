/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.discovery;

import java.util.Map;

/**
 * Interface defining health check reporting for the Event Processing Service.
 * It provides methods to check if the service is healthy, ready to serve requests,
 * and to report detailed health status. This is crucial for service discovery to
 * determine if an Event Processing Service instance should receive traffic, ensuring
 * that only healthy instances are used for event detection and processing.
 */
public interface HealthCheck {

    /**
     * Enum representing the possible health states of the service or its components.
     */
    enum Status {
        /**
         * The service or component is functioning normally.
         */
        UP,
        
        /**
         * The service or component is functioning but with degraded performance or capabilities.
         */
        DEGRADED,
        
        /**
         * The service or component is not functioning properly but still operational.
         */
        WARNING,
        
        /**
         * The service or component is not functioning and is unavailable.
         */
        DOWN,
        
        /**
         * The service or component is in an unknown state.
         */
        UNKNOWN
    }

    /**
     * Checks if the service is healthy and can process events.
     * This is used for liveness probes in Kubernetes to determine if the service
     * is running and not deadlocked.
     *
     * @return true if the service is healthy, false otherwise
     */
    boolean isHealthy();

    /**
     * Checks if the service is ready to serve requests.
     * This is used for readiness probes in Kubernetes to determine if the service
     * should receive traffic. A service might be healthy (running) but not ready
     * (e.g., still initializing, waiting for dependencies).
     *
     * @return true if the service is ready to serve requests, false otherwise
     */
    boolean isReady();

    /**
     * Gets the detailed health status of the service and its components.
     * This provides a comprehensive view of the service health, including
     * the status of event handlers, message broker connectivity, and other
     * dependencies specific to event processing.
     *
     * @return a map containing component names as keys and their health status as values
     */
    Map<String, Status> getStatus();

    /**
     * Gets the health status of a specific component.
     * This allows checking the health of individual components like event handlers,
     * message broker connections, or other dependencies.
     *
     * @param component the name of the component to check
     * @return the health status of the specified component
     */
    Status getComponentStatus(String component);

    /**
     * Gets a detailed description of the current health status.
     * This provides additional information about the health status,
     * including error messages or warnings that might be useful for
     * troubleshooting.
     *
     * @return a string containing a detailed description of the health status
     */
    String getStatusDescription();

    /**
     * Checks if the service is in startup phase.
     * This is used for startup probes in Kubernetes to allow for longer
     * initialization times before liveness probes kick in.
     *
     * @return true if the service is still starting up, false if startup is complete
     */
    boolean isStarting();
}