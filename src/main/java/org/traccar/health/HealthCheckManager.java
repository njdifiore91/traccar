/*
 * Copyright 2022 - 2023 Anton Tananaev (anton@traccar.org)
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

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
 import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Manager for health checks that can be used by service discovery systems
 * and monitoring tools to determine the health of the application.
 */
@Singleton
public class HealthCheckManager {

    private static final Logger LOGGER = Logger.getLogger(HealthCheckManager.class.getName());

    private final Map<String, Supplier<Boolean>> healthChecks = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executorService;
    private final int healthCheckInterval;

    /**
     * Constructs a new HealthCheckManager.
     *
     * @param config The configuration
     */
    @Inject
    public HealthCheckManager(Config config) {
        this.healthCheckInterval = config.getInteger(Keys.HEALTH_CHECK_INTERVAL, 30);
        
        // Create a scheduled executor service for running health checks
        this.executorService = new ScheduledThreadPoolExecutor(
                1, new DefaultThreadFactory("health-check-manager"));
        
        // Schedule periodic health check logging
        if (config.getBoolean(Keys.HEALTH_CHECK_LOGGING_ENABLED, false)) {
            executorService.scheduleAtFixedRate(this::logHealthStatus, 
                    healthCheckInterval, healthCheckInterval, TimeUnit.SECONDS);
        }
        
        LOGGER.info("Initialized HealthCheckManager with interval: " + healthCheckInterval + " seconds");
    }

    /**
     * Register a health check.
     *
     * @param name The name of the health check
     * @param check The health check function that returns true if healthy, false otherwise
     */
    public void register(String name, Supplier<Boolean> check) {
        healthChecks.put(name, check);
        LOGGER.info("Registered health check: " + name);
    }

    /**
     * Deregister a health check.
     *
     * @param name The name of the health check
     */
    public void deregister(String name) {
        healthChecks.remove(name);
        LOGGER.info("Deregistered health check: " + name);
    }

    /**
     * Check if the application is healthy.
     *
     * @return true if all health checks pass, false otherwise
     */
    public boolean isHealthy() {
        if (healthChecks.isEmpty()) {
            return true; // If no health checks are registered, assume healthy
        }
        
        // Check all registered health checks
        for (Map.Entry<String, Supplier<Boolean>> entry : healthChecks.entrySet()) {
            try {
                if (!entry.getValue().get()) {
                    LOGGER.warning("Health check failed: " + entry.getKey());
                    return false;
                }
            } catch (Exception e) {
                LOGGER.warning("Health check threw exception: " + entry.getKey() + ": " + e.getMessage());
                return false;
            }
        }
        
        return true;
    }

    /**
     * Get a map of all health checks and their status.
     *
     * @return A map of health check names to their status (true if healthy, false otherwise)
     */
    public Map<String, Boolean> getHealthChecks() {
        Map<String, Boolean> result = new ConcurrentHashMap<>();
        
        for (Map.Entry<String, Supplier<Boolean>> entry : healthChecks.entrySet()) {
            try {
                result.put(entry.getKey(), entry.getValue().get());
            } catch (Exception e) {
                result.put(entry.getKey(), false);
            }
        }
        
        return result;
    }
    
    /**
     * Log the current health status of all checks.
     */
    private void logHealthStatus() {
        Map<String, Boolean> checks = getHealthChecks();
        
        if (checks.isEmpty()) {
            LOGGER.info("Health status: No health checks registered");
            return;
        }
        
        StringBuilder sb = new StringBuilder("Health status: ");
        boolean allHealthy = true;
        
        for (Map.Entry<String, Boolean> entry : checks.entrySet()) {
            if (!entry.getValue()) {
                allHealthy = false;
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue() ? "UP" : "DOWN").append(", ");
        }
        
        // Remove trailing comma and space
        if (!checks.isEmpty()) {
            sb.setLength(sb.length() - 2);
        }
        
        if (allHealthy) {
            LOGGER.info(sb.toString());
        } else {
            LOGGER.warning(sb.toString());
        }
    }

    /**
     * Shutdown the health check manager.
     */
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}