/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.sms;

import java.util.Map;

/**
 * Interface for components that provide health check information.
 * Implementations should provide details about their current health status
 * and relevant metrics for monitoring systems.
 */
public interface HealthCheck {

    /**
     * Checks if the component is currently healthy and operational.
     *
     * @return true if the component is healthy, false otherwise
     */
    boolean isHealthy();

    /**
     * Provides detailed metrics about the component's health and performance.
     *
     * @return A map of metric names to their values
     */
    Map<String, Object> getHealthMetrics();
}