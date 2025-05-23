/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.metrics;

import io.prometheus.client.Collector;

/**
 * Utility class for adding common labels to all metrics.
 * This ensures consistent labeling across all metrics in the Protocol Service.
 */
public final class CommonLabels {

    private static String serviceName;
    private static String instanceId;

    private CommonLabels() {
        // Utility class
    }

    /**
     * Initializes the common labels with the specified service name and instance ID.
     * These labels will be added to all metrics registered after this method is called.
     *
     * @param service the service name
     * @param instance the instance identifier
     */
    public static void initialize(String service, String instance) {
        serviceName = service;
        instanceId = instance;
        
        // Set common labels for all metrics
        Collector.commonLabels.clear();
        Collector.commonLabels.put("service", serviceName);
        Collector.commonLabels.put("instance", instanceId);
    }

    /**
     * Gets the service name used in metric labels.
     *
     * @return the service name
     */
    public static String getServiceName() {
        return serviceName;
    }

    /**
     * Gets the instance identifier used in metric labels.
     *
     * @return the instance identifier
     */
    public static String getInstanceId() {
        return instanceId;
    }
}