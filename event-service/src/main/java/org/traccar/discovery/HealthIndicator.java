/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
 * Interface for custom health indicators that can be registered with the DefaultHealthCheck.
 * Allows for extensible health checking of event-specific components.
 */
public interface HealthIndicator {

    /**
     * Checks if the component is healthy.
     *
     * @return true if the component is healthy, false otherwise
     */
    boolean isHealthy();

    /**
     * Gets detailed status information for the component.
     *
     * @return Map containing component health status details
     */
    Map<String, Object> getStatus();
}