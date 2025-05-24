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
package org.traccar.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Metrics related to service health in the Position Service.
 * This class tracks service health status, resource utilization, and dependency health using Prometheus metrics.
 * It provides methods to record health check executions and failures, and to set the current health status
 * of the service and its dependencies.
 */
@Singleton
public class HealthMetrics {

    private final MeterRegistry registry;

    // Service health status (1 = UP, 0 = DOWN)
    private final AtomicInteger serviceHealthStatus;

    // Dependency health status by dependency name (1 = UP, 0 = DOWN)
    private final Map<String, AtomicInteger> dependencyHealthStatus;

    // Resource utilization metrics
    private final AtomicInteger cpuUtilization;
    private final AtomicInteger memoryUtilization;
    private final AtomicInteger diskUtilization;

    // Health check execution counter
    private final Counter healthCheckExecutions;

    // Health check failure counter
    private final Counter healthCheckFailures;

    /**
     * Creates a new instance of HealthMetrics.
     *
     * @param registry the Micrometer registry for registering metrics
     */
    @Inject
    public HealthMetrics(MeterRegistry registry) {
        this.registry = registry;

        // Initialize service health status gauge (1 = UP, 0 = DOWN)
        this.serviceHealthStatus = new AtomicInteger(1); // Default to UP
        Gauge.builder("position_service_health", serviceHealthStatus, AtomicInteger::get)
                .description("Current health status of the Position Service (1 = UP, 0 = DOWN)")
                .register(registry);

        // Initialize dependency health status map and gauges
        this.dependencyHealthStatus = new ConcurrentHashMap<>();

        // Initialize resource utilization gauges
        this.cpuUtilization = new AtomicInteger(0);
        Gauge.builder("position_service_cpu_utilization", cpuUtilization, AtomicInteger::get)
                .description("CPU utilization percentage of the Position Service")
                .baseUnit("percent")
                .register(registry);

        this.memoryUtilization = new AtomicInteger(0);
        Gauge.builder("position_service_memory_utilization", memoryUtilization, AtomicInteger::get)
                .description("Memory utilization percentage of the Position Service")
                .baseUnit("percent")
                .register(registry);

        this.diskUtilization = new AtomicInteger(0);
        Gauge.builder("position_service_disk_utilization", diskUtilization, AtomicInteger::get)
                .description("Disk utilization percentage of the Position Service")
                .baseUnit("percent")
                .register(registry);

        // Initialize health check counters
        this.healthCheckExecutions = Counter.builder("position_service_health_check_executions_total")
                .description("Total number of health check executions")
                .register(registry);

        this.healthCheckFailures = Counter.builder("position_service_health_check_failures_total")
                .description("Total number of health check failures")
                .register(registry);
    }

    /**
     * Sets the current health status of the Position Service.
     *
     * @param isHealthy true if the service is healthy, false otherwise
     */
    public void setServiceHealthStatus(boolean isHealthy) {
        serviceHealthStatus.set(isHealthy ? 1 : 0);
    }

    /**
     * Sets the health status of a dependency.
     *
     * @param dependencyName the name of the dependency
     * @param isHealthy true if the dependency is healthy, false otherwise
     */
    public void setDependencyHealthStatus(String dependencyName, boolean isHealthy) {
        AtomicInteger status = dependencyHealthStatus.computeIfAbsent(dependencyName, name -> {
            AtomicInteger newStatus = new AtomicInteger(isHealthy ? 1 : 0);
            Gauge.builder("position_service_dependency_health", newStatus, AtomicInteger::get)
                    .description("Health status of a dependency (1 = UP, 0 = DOWN)")
                    .tag("dependency", name)
                    .register(registry);
            return newStatus;
        });
        status.set(isHealthy ? 1 : 0);
    }

    /**
     * Sets the CPU utilization percentage of the Position Service.
     *
     * @param utilizationPercent the CPU utilization percentage (0-100)
     */
    public void setCpuUtilization(int utilizationPercent) {
        cpuUtilization.set(utilizationPercent);
    }

    /**
     * Sets the memory utilization percentage of the Position Service.
     *
     * @param utilizationPercent the memory utilization percentage (0-100)
     */
    public void setMemoryUtilization(int utilizationPercent) {
        memoryUtilization.set(utilizationPercent);
    }

    /**
     * Sets the disk utilization percentage of the Position Service.
     *
     * @param utilizationPercent the disk utilization percentage (0-100)
     */
    public void setDiskUtilization(int utilizationPercent) {
        diskUtilization.set(utilizationPercent);
    }

    /**
     * Records a health check execution.
     */
    public void recordHealthCheckExecution() {
        healthCheckExecutions.increment();
    }

    /**
     * Records a health check failure.
     */
    public void recordHealthCheckFailure() {
        healthCheckFailures.increment();
    }

    /**
     * Records a health check execution with the given result.
     *
     * @param success true if the health check was successful, false otherwise
     */
    public void recordHealthCheck(boolean success) {
        recordHealthCheckExecution();
        if (!success) {
            recordHealthCheckFailure();
        }
    }
}