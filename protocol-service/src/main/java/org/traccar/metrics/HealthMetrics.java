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

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

import jakarta.inject.Singleton;

/**
 * Metrics related to service health in the Protocol Service.
 * This class tracks service health status, resource utilization, and dependency health
 * using Prometheus metrics. It provides methods to record health check executions and failures,
 * and to set the current health status of the service and its dependencies.
 */
@Singleton
public class HealthMetrics {

    private final Gauge serviceHealthStatus;
    private final Gauge dependencyHealthStatus;
    private final Gauge resourceUtilization;
    private final Counter healthCheckExecutions;
    private final Counter healthCheckFailures;

    /**
     * Initializes health metrics with appropriate labels and descriptions.
     */
    public HealthMetrics() {
        // Service health status gauge (0 = unhealthy, 1 = healthy)
        serviceHealthStatus = Gauge.build()
                .name("service_health_status")
                .help("Current health status of the service (0 = unhealthy, 1 = healthy)")
                .register();

        // Dependency health status gauge (0 = unhealthy, 1 = healthy)
        dependencyHealthStatus = Gauge.build()
                .name("dependency_health_status")
                .help("Current health status of service dependencies (0 = unhealthy, 1 = healthy)")
                .labelNames("dependency")
                .register();

        // Resource utilization gauge (percentage)
        resourceUtilization = Gauge.build()
                .name("resource_utilization_percent")
                .help("Current resource utilization percentage")
                .labelNames("resource")
                .register();

        // Health check execution counter
        healthCheckExecutions = Counter.build()
                .name("health_check_executions_total")
                .help("Total number of health check executions")
                .labelNames("type")
                .register();

        // Health check failure counter
        healthCheckFailures = Counter.build()
                .name("health_check_failures_total")
                .help("Total number of health check failures")
                .labelNames("type")
                .register();
    }

    /**
     * Sets the current health status of the service.
     *
     * @param healthy true if the service is healthy, false otherwise
     */
    public void setServiceHealthStatus(boolean healthy) {
        serviceHealthStatus.set(healthy ? 1 : 0);
    }

    /**
     * Sets the current health status of a specific dependency.
     *
     * @param dependency the name of the dependency
     * @param healthy true if the dependency is healthy, false otherwise
     */
    public void setDependencyHealthStatus(String dependency, boolean healthy) {
        dependencyHealthStatus.labels(dependency).set(healthy ? 1 : 0);
    }

    /**
     * Sets the current utilization percentage of a specific resource.
     *
     * @param resource the name of the resource (e.g., "cpu", "memory", "disk")
     * @param percentage the utilization percentage (0-100)
     */
    public void setResourceUtilization(String resource, double percentage) {
        resourceUtilization.labels(resource).set(percentage);
    }

    /**
     * Records a health check execution.
     *
     * @param type the type of health check (e.g., "liveness", "readiness", "startup")
     */
    public void recordHealthCheckExecution(String type) {
        healthCheckExecutions.labels(type).inc();
    }

    /**
     * Records a health check failure.
     *
     * @param type the type of health check that failed
     */
    public void recordHealthCheckFailure(String type) {
        healthCheckFailures.labels(type).inc();
    }

    /**
     * Records a message processing health check with drop ratio calculation.
     *
     * @param currentPeriodMessages number of messages processed in the current period
     * @param lastPeriodMessages number of messages processed in the last period
     * @param dropThreshold the threshold below which a drop is considered a failure
     * @return true if the health check passed, false if it failed
     */
    public boolean recordMessageHealthCheck(int currentPeriodMessages, int lastPeriodMessages, double dropThreshold) {
        recordHealthCheckExecution("message");
        
        if (lastPeriodMessages > 0 && currentPeriodMessages > 0) {
            double dropRatio = currentPeriodMessages / (double) lastPeriodMessages;
            
            // Set the current message processing drop ratio as a resource metric
            setResourceUtilization("message_drop_ratio", dropRatio * 100);
            
            if (dropRatio < dropThreshold) {
                recordHealthCheckFailure("message");
                return false;
            }
        }
        
        return true;
    }

    /**
     * Records a web service health check.
     *
     * @param statusCode the HTTP status code returned by the web service
     * @return true if the health check passed, false if it failed
     */
    public boolean recordWebHealthCheck(int statusCode) {
        recordHealthCheckExecution("web");
        
        if (statusCode != 200) {
            recordHealthCheckFailure("web");
            return false;
        }
        
        return true;
    }
}