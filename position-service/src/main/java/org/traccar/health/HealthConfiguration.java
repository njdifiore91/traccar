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
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * Configuration class for Position Processing Service health check settings.
 * This class centralizes health check configuration and enables dynamic adjustment
 * of health check parameters for different environments.
 */
@Configuration
public class HealthConfiguration {

    private final Environment environment;

    /**
     * Constructor with environment injection for profile-specific configuration.
     * 
     * @param environment Spring environment for accessing profiles and properties
     */
    public HealthConfiguration(Environment environment) {
        this.environment = environment;
    }

    /**
     * Message drop ratio threshold for health checks.
     * If the ratio of dropped messages exceeds this value, the service will be considered unhealthy.
     * Default value is 0.1 (10%).
     */
    @Value("${message.processing.drop-threshold:0.1}")
    private double messageDropThreshold;

    /**
     * Health check interval in milliseconds.
     * This is the frequency at which health checks are performed.
     * Default value is 30000 (30 seconds).
     */
    @Value("${health.check.interval:30000}")
    private long healthCheckInterval;

    /**
     * Health check timeout in milliseconds.
     * If a health check takes longer than this value, it will be considered failed.
     * Default value is 5000 (5 seconds).
     */
    @Value("${health.check.timeout:5000}")
    private long healthCheckTimeout;

    /**
     * Database health check timeout in milliseconds.
     * If a database health check takes longer than this value, it will be considered failed.
     * Default value is 3000 (3 seconds).
     */
    @Value("${health.check.database.timeout:3000}")
    private long databaseHealthCheckTimeout;

    /**
     * Geocoder health check timeout in milliseconds.
     * If a geocoder health check takes longer than this value, it will be considered failed.
     * Default value is 5000 (5 seconds).
     */
    @Value("${health.check.geocoder.timeout:5000}")
    private long geocoderHealthCheckTimeout;

    /**
     * Geofence health check timeout in milliseconds.
     * If a geofence health check takes longer than this value, it will be considered failed.
     * Default value is 3000 (3 seconds).
     */
    @Value("${health.check.geofence.timeout:3000}")
    private long geofenceHealthCheckTimeout;

    /**
     * Message broker health check timeout in milliseconds.
     * If a message broker health check takes longer than this value, it will be considered failed.
     * Default value is 5000 (5 seconds).
     */
    @Value("${health.check.message-broker.timeout:5000}")
    private long messageBrokerHealthCheckTimeout;

    /**
     * Enable or disable systemd watchdog integration.
     * When enabled, the service will notify systemd of its health status.
     * Default value is true.
     */
    @Value("${health.check.systemd-watchdog.enabled:true}")
    private boolean systemdWatchdogEnabled;

    /**
     * Systemd watchdog notification interval in milliseconds.
     * This is the frequency at which the service notifies systemd of its health status.
     * Default value is 15000 (15 seconds).
     */
    @Value("${health.check.systemd-watchdog.interval:15000}")
    private long systemdWatchdogInterval;

    /**
     * Grace period for health checks in milliseconds.
     * During this period after startup, health checks will always return healthy status.
     * Default value is 300000 (5 minutes).
     */
    @Value("${health.check.grace-period:300000}")
    private long healthCheckGracePeriod;

    /**
     * Creates a custom health indicator for message processing.
     * This indicator monitors the message drop ratio and reports unhealthy status
     * if the ratio exceeds the configured threshold.
     *
     * @return MessageProcessingHealthIndicator bean
     */
    @Bean
    public MessageProcessingHealthIndicator messageProcessingHealthIndicator() {
        double threshold = messageDropThreshold;
        
        // Apply different thresholds based on environment
        if (environment.acceptsProfiles(profiles -> profiles.contains("production"))) {
            // More strict threshold for production
            threshold = Math.min(threshold, 0.05); // 5% max for production
        } else if (environment.acceptsProfiles(profiles -> profiles.contains("development"))) {
            // More lenient threshold for development
            threshold = Math.max(threshold, 0.2); // 20% min for development
        }
        
        return new MessageProcessingHealthIndicator(threshold);
    }

    /**
     * Creates a custom health indicator for database connectivity.
     * This indicator checks if the database connection is working properly.
     *
     * @return DatabaseHealthIndicator bean
     */
    @Bean
    public DatabaseHealthIndicator databaseHealthIndicator() {
        DatabaseHealthIndicator indicator = new DatabaseHealthIndicator(databaseHealthCheckTimeout);
        // Configure database health check based on environment
        if (environment.acceptsProfiles(profiles -> profiles.contains("kubernetes"))) {
            // In Kubernetes, we want to fail fast to trigger pod restart
            indicator.setFailFast(true);
        } else {
            // In traditional deployment, we want to be more resilient
            indicator.setFailFast(false);
            indicator.setRetryAttempts(3);
        }
        return indicator;
    }

    /**
     * Creates a custom health indicator for geocoder service.
     * This indicator checks if the geocoder service is available and responding.
     *
     * @return GeocoderHealthIndicator bean
     */
    @Bean
    public GeocoderHealthIndicator geocoderHealthIndicator() {
        return new GeocoderHealthIndicator(geocoderHealthCheckTimeout);
    }

    /**
     * Creates a custom health indicator for geofence service.
     * This indicator checks if the geofence service is functioning correctly.
     *
     * @return GeofenceHealthIndicator bean
     */
    @Bean
    public GeofenceHealthIndicator geofenceHealthIndicator() {
        return new GeofenceHealthIndicator(geofenceHealthCheckTimeout);
    }

    /**
     * Creates a custom health indicator for message broker connectivity.
     * This indicator checks if the message broker is available and responding.
     *
     * @return MessageBrokerHealthIndicator bean
     */
    @Bean
    public MessageBrokerHealthIndicator messageBrokerHealthIndicator() {
        return new MessageBrokerHealthIndicator(messageBrokerHealthCheckTimeout);
    }

    /**
     * Creates a systemd watchdog notifier if enabled.
     * This component periodically notifies systemd of the service's health status.
     *
     * @param healthEndpoint Spring Boot health endpoint to check overall health status
     * @return SystemdWatchdogNotifier bean if enabled, null otherwise
     */
    @Bean
    public SystemdWatchdogNotifier systemdWatchdogNotifier(HealthEndpoint healthEndpoint) {
        if (systemdWatchdogEnabled) {
            SystemdWatchdogNotifier notifier = new SystemdWatchdogNotifier(healthEndpoint, systemdWatchdogInterval, healthCheckGracePeriod);
            notifier.setHealthyStatus(Status.UP);
            notifier.setUnhealthyStatus(Status.DOWN);
            return notifier;
        }
        return null;
    }

    /**
     * Gets the message drop threshold for health checks.
     *
     * @return the message drop threshold
     */
    public double getMessageDropThreshold() {
        return messageDropThreshold;
    }

    /**
     * Gets the health check interval in milliseconds.
     *
     * @return the health check interval
     */
    public long getHealthCheckInterval() {
        return healthCheckInterval;
    }

    /**
     * Gets the health check timeout in milliseconds.
     *
     * @return the health check timeout
     */
    public long getHealthCheckTimeout() {
        return healthCheckTimeout;
    }

    /**
     * Gets the database health check timeout in milliseconds.
     *
     * @return the database health check timeout
     */
    public long getDatabaseHealthCheckTimeout() {
        return databaseHealthCheckTimeout;
    }

    /**
     * Gets the geocoder health check timeout in milliseconds.
     *
     * @return the geocoder health check timeout
     */
    public long getGeocoderHealthCheckTimeout() {
        return geocoderHealthCheckTimeout;
    }

    /**
     * Gets the geofence health check timeout in milliseconds.
     *
     * @return the geofence health check timeout
     */
    public long getGeofenceHealthCheckTimeout() {
        return geofenceHealthCheckTimeout;
    }

    /**
     * Gets the message broker health check timeout in milliseconds.
     *
     * @return the message broker health check timeout
     */
    public long getMessageBrokerHealthCheckTimeout() {
        return messageBrokerHealthCheckTimeout;
    }

    /**
     * Checks if systemd watchdog integration is enabled.
     *
     * @return true if systemd watchdog integration is enabled, false otherwise
     */
    public boolean isSystemdWatchdogEnabled() {
        return systemdWatchdogEnabled;
    }

    /**
     * Gets the systemd watchdog notification interval in milliseconds.
     *
     * @return the systemd watchdog notification interval
     */
    public long getSystemdWatchdogInterval() {
        return systemdWatchdogInterval;
    }

    /**
     * Gets the grace period for health checks in milliseconds.
     *
     * @return the health check grace period
     */
    public long getHealthCheckGracePeriod() {
        return healthCheckGracePeriod;
    }
    
    /**
     * Gets the current environment profiles.
     * 
     * @return array of active profiles
     */
    public String[] getActiveProfiles() {
        return environment.getActiveProfiles();
    }
    
    /**
     * Checks if a specific profile is active.
     * 
     * @param profile the profile to check
     * @return true if the profile is active, false otherwise
     */
    public boolean isProfileActive(String profile) {
        return environment.acceptsProfiles(profiles -> profiles.contains(profile));
    }
}