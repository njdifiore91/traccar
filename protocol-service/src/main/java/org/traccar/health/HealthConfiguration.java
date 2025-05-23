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
package org.traccar.health;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.HealthEndpointGroups;
import org.springframework.boot.actuate.health.HealthEndpointGroupsRegistrar;
import org.springframework.boot.actuate.health.Status;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides configuration for the Protocol Service health check system, including thresholds, intervals, and
 * component-specific settings. This class centralizes health check configuration and enables dynamic adjustment
 * of health check parameters.
 */
@Configuration
public class HealthConfiguration {

    /**
     * Message drop ratio threshold for health checks. If the ratio of dropped messages exceeds this value,
     * the health check will report an unhealthy status.
     */
    @Value("${message.processing.drop-threshold:0.1}")
    private double messageDropThreshold;

    /**
     * Health check interval in milliseconds. This is the frequency at which health checks are performed.
     */
    @Value("${health.check.interval:30000}")
    private long healthCheckInterval;

    /**
     * Health check timeout in milliseconds. This is the maximum time allowed for a health check to complete.
     */
    @Value("${health.check.timeout:5000}")
    private long healthCheckTimeout;

    /**
     * Flag to enable or disable systemd watchdog integration for traditional deployments.
     */
    @Value("${health.systemd.watchdog.enabled:true}")
    private boolean systemdWatchdogEnabled;

    /**
     * Grace period in milliseconds after startup during which health checks will always report healthy status.
     * This allows the service time to initialize and stabilize before health checks become strict.
     */
    @Value("${health.grace-period:300000}")
    private long gracePeriod;

    /**
     * Configures health endpoint groups for Kubernetes probes and other health check consumers.
     * This defines which health indicators are included in each health group (liveness, readiness, etc.)
     * and sets the status mapping for different health states.
     *
     * @return Configured health endpoint groups
     */
    @Bean
    public HealthEndpointGroups healthEndpointGroups() {
        return HealthEndpointGroupsRegistrar.builder()
                .withGroup("liveness")
                    .includeHealthIndicator("livenessState")
                    .includeHealthIndicator("diskSpace")
                    .statusAggregator(statuses -> {  
                        // DOWN takes precedence over OUT_OF_SERVICE
                        if (statuses.contains(Status.DOWN)) {
                            return Status.DOWN;
                        }
                        return Status.UP;
                    })
                    .build()
                .withGroup("readiness")
                    .includeHealthIndicator("readinessState")
                    .includeHealthIndicator("messageProcessing")
                    .includeHealthIndicator("discoveryClient")
                    .includeHealthIndicator("messageBroker")
                    .statusAggregator(statuses -> {
                        // Any non-UP status results in OUT_OF_SERVICE for readiness
                        if (statuses.stream().anyMatch(status -> !Status.UP.equals(status))) {
                            return Status.OUT_OF_SERVICE;
                        }
                        return Status.UP;
                    })
                    .build()
                .withGroup("startup")
                    .includeHealthIndicator("startupState")
                    .statusAggregator(statuses -> {
                        // Service is considered started when all indicators are UP
                        if (statuses.stream().allMatch(Status.UP::equals)) {
                            return Status.UP;
                        }
                        return Status.DOWN;
                    })
                    .build()
                .build();
    }

    /**
     * Gets the message drop threshold for health checks.
     *
     * @return The message drop threshold as a ratio (0.0 to 1.0)
     */
    public double getMessageDropThreshold() {
        return messageDropThreshold;
    }

    /**
     * Gets the health check interval in milliseconds.
     *
     * @return The health check interval
     */
    public long getHealthCheckInterval() {
        return healthCheckInterval;
    }

    /**
     * Gets the health check timeout in milliseconds.
     *
     * @return The health check timeout
     */
    public long getHealthCheckTimeout() {
        return healthCheckTimeout;
    }

    /**
     * Checks if systemd watchdog integration is enabled.
     *
     * @return True if systemd watchdog integration is enabled, false otherwise
     */
    public boolean isSystemdWatchdogEnabled() {
        return systemdWatchdogEnabled;
    }

    /**
     * Gets the grace period in milliseconds after startup during which health checks will always report healthy status.
     *
     * @return The grace period in milliseconds
     */
    public long getGracePeriod() {
        return gracePeriod;
    }
}