/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.health;

/**
 * Interface for health check indicators in the system.
 * Health checks are used to report the health status of various components to the service health check system.
 */
public interface HealthCheck {

    /**
     * Gets the name of the health check indicator.
     * This name is used to identify the health check in the health check system.
     *
     * @return The name of the health check indicator
     */
    String getName();

    /**
     * Checks if the component is healthy.
     * This method should return true if the component is functioning correctly, false otherwise.
     *
     * @return true if the component is healthy, false otherwise
     */
    boolean isHealthy();

    /**
     * Gets additional details about the health check.
     * This can be used to provide more information about the health status of the component.
     *
     * @return An object containing additional details about the health check, or null if no details are available
     */
    Object getDetails();

}