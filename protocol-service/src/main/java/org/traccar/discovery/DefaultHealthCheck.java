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

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.traccar.helper.Log;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of the HealthCheck interface that provides basic health status reporting.
 * It checks the status of critical dependencies like database connections and message brokers,
 * and reports the overall health of the service based on these checks.
 */
@Singleton
public class DefaultHealthCheck implements HealthCheck {

    private final ServiceRegistryConfig config;
    private final Set<HealthIndicator> healthIndicators;

    /**
     * Health status enum representing the current health state of the service or component.
     */
    public enum Status {
        UP,         // Service is healthy and fully operational
        DEGRADED,   // Service is operational but with reduced functionality
        DOWN        // Service is not operational
    }

    /**
     * Interface for custom health indicators that can be injected into the health check.
     */
    public interface HealthIndicator {
        /**
         * Gets the name of the health indicator.
         *
         * @return The indicator name
         */
        String getName();

        /**
         * Checks the health of a specific component or dependency.
         *
         * @return The health status of the component
         */
        Status check();

        /**
         * Gets detailed status information for the component.
         *
         * @return A map containing component-specific health details
         */
        Map<String, Object> getDetails();
    }

    /**
     * Constructs a new DefaultHealthCheck with the provided configuration and health indicators.
     *
     * @param config The service registry configuration
     * @param healthIndicators Optional set of custom health indicators
     */
    @Inject
    public DefaultHealthCheck(ServiceRegistryConfig config, Set<HealthIndicator> healthIndicators) {
        this.config = config;
        this.healthIndicators = healthIndicators;
    }

    /**
     * Checks if the service is healthy by evaluating all health indicators.
     * The service is considered healthy if all critical indicators are UP or DEGRADED.
     *
     * @return true if the service is healthy, false otherwise
     */
    @Override
    public boolean isHealthy() {
        Map<String, Object> status = getStatus();
        Status overallStatus = Status.valueOf(status.get("status").toString());
        return overallStatus != Status.DOWN;
    }

    /**
     * Gets detailed health status information including system resources and custom health indicators.
     *
     * @return A map containing health status details
     */
    @Override
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        Map<String, Object> components = new LinkedHashMap<>();
        Status overallStatus = Status.UP;

        // Check system resources
        Map<String, Object> systemStatus = checkSystemResources();
        Status systemHealthStatus = Status.valueOf(systemStatus.get("status").toString());
        components.put("system", systemStatus);
        overallStatus = worstStatus(overallStatus, systemHealthStatus);

        // Check custom health indicators
        if (healthIndicators != null && !healthIndicators.isEmpty()) {
            for (HealthIndicator indicator : healthIndicators) {
                try {
                    Status indicatorStatus = indicator.check();
                    Map<String, Object> details = indicator.getDetails();
                    details.put("status", indicatorStatus);
                    components.put(indicator.getName(), details);
                    overallStatus = worstStatus(overallStatus, indicatorStatus);
                } catch (Exception e) {
                    Map<String, Object> errorDetails = new HashMap<>();
                    errorDetails.put("status", Status.DOWN);
                    errorDetails.put("error", e.getMessage());
                    components.put(indicator.getName(), errorDetails);
                    overallStatus = Status.DOWN;
                    Log.warning("Health check failed for indicator: " + indicator.getName(), e);
                }
            }
        }

        // Build the final status response
        status.put("status", overallStatus);
        status.put("components", components);
        status.put("serviceId", config.getServiceId());
        status.put("serviceName", config.getServiceName());
        status.put("timestamp", System.currentTimeMillis());

        return status;
    }

    /**
     * Checks system resources including memory, CPU, and disk space.
     *
     * @return A map containing system resource health details
     */
    private Map<String, Object> checkSystemResources() {
        Map<String, Object> systemStatus = new HashMap<>();
        Status status = Status.UP;

        try {
            // Check memory usage
            MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
            long heapUsed = memoryMXBean.getHeapMemoryUsage().getUsed();
            long heapMax = memoryMXBean.getHeapMemoryUsage().getMax();
            double memoryUsageRatio = (double) heapUsed / heapMax;

            Map<String, Object> memoryDetails = new HashMap<>();
            memoryDetails.put("used", heapUsed);
            memoryDetails.put("max", heapMax);
            memoryDetails.put("usageRatio", memoryUsageRatio);

            // Memory status based on usage ratio
            if (memoryUsageRatio > 0.9) {
                memoryDetails.put("status", Status.DEGRADED);
                status = Status.DEGRADED;
            } else if (memoryUsageRatio > 0.95) {
                memoryDetails.put("status", Status.DOWN);
                status = Status.DOWN;
            } else {
                memoryDetails.put("status", Status.UP);
            }

            systemStatus.put("memory", memoryDetails);

            // Check CPU usage
            OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
            double systemLoadAverage = osBean.getSystemLoadAverage();
            int availableProcessors = osBean.getAvailableProcessors();
            double cpuUsageRatio = systemLoadAverage / availableProcessors;

            Map<String, Object> cpuDetails = new HashMap<>();
            cpuDetails.put("systemLoadAverage", systemLoadAverage);
            cpuDetails.put("availableProcessors", availableProcessors);
            cpuDetails.put("usageRatio", cpuUsageRatio);

            // CPU status based on usage ratio
            if (cpuUsageRatio > 0.8) {
                cpuDetails.put("status", Status.DEGRADED);
                status = worstStatus(status, Status.DEGRADED);
            } else if (cpuUsageRatio > 0.95) {
                cpuDetails.put("status", Status.DOWN);
                status = Status.DOWN;
            } else {
                cpuDetails.put("status", Status.UP);
            }

            systemStatus.put("cpu", cpuDetails);

            // Check disk space
            long[] storageSpace = Log.getStorageSpace();
            if (storageSpace.length >= 2) {
                long usableSpace = storageSpace[0];
                long totalSpace = storageSpace[1];
                double diskUsageRatio = 1.0 - ((double) usableSpace / totalSpace);

                Map<String, Object> diskDetails = new HashMap<>();
                diskDetails.put("usable", usableSpace);
                diskDetails.put("total", totalSpace);
                diskDetails.put("usageRatio", diskUsageRatio);

                // Disk status based on usage ratio
                if (diskUsageRatio > 0.9) {
                    diskDetails.put("status", Status.DEGRADED);
                    status = worstStatus(status, Status.DEGRADED);
                } else if (diskUsageRatio > 0.95) {
                    diskDetails.put("status", Status.DOWN);
                    status = Status.DOWN;
                } else {
                    diskDetails.put("status", Status.UP);
                }

                systemStatus.put("disk", diskDetails);
            }
        } catch (Exception e) {
            systemStatus.put("error", e.getMessage());
            status = Status.DOWN;
            Log.warning("System resource check failed", e);
        }

        systemStatus.put("status", status);
        return systemStatus;
    }

    /**
     * Determines the worst status between two status values.
     * DOWN is worse than DEGRADED, which is worse than UP.
     *
     * @param status1 First status
     * @param status2 Second status
     * @return The worse of the two statuses
     */
    private Status worstStatus(Status status1, Status status2) {
        if (status1 == Status.DOWN || status2 == Status.DOWN) {
            return Status.DOWN;
        } else if (status1 == Status.DEGRADED || status2 == Status.DEGRADED) {
            return Status.DEGRADED;
        } else {
            return Status.UP;
        }
    }
}