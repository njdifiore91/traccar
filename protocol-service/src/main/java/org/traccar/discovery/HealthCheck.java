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
package org.traccar.discovery;

import java.util.Map;

/**
 * Interface defining health check reporting for services. It provides methods to check
 * if a service is healthy, ready to serve requests, and to report detailed health status.
 * This is crucial for service discovery to determine if a service instance should receive traffic.
 */
public interface HealthCheck {

    /**
     * Checks if the service is healthy.
     *
     * @return true if the service is healthy, false otherwise
     */
    boolean isHealthy();

    /**
     * Gets detailed health status information.
     *
     * @return A map containing health status details
     */
    Map<String, Object> getStatus();

}