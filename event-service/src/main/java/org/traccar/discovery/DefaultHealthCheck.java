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

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.handler.events.BaseEventHandler;
import org.traccar.messaging.MessageBroker;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default implementation of the HealthCheck interface for the Event Processing Service.
 * Provides health status reporting specific to event processing.
 * Checks the status of critical dependencies like database connections, message brokers,
 * and event handlers, and reports the overall health of the service based on these checks.
 */
@Singleton
public class DefaultHealthCheck implements HealthCheck {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultHealthCheck.class);

    private final Config config;
    private final Storage storage;
    private final MessageBroker messageBroker;
    private final List<BaseEventHandler> eventHandlers;
    private final Map<String, HealthIndicator> customIndicators;

    /**
     * Constructs a new DefaultHealthCheck with the specified dependencies.
     *
     * @param config Configuration for health check thresholds
     * @param storage Storage instance for database connectivity check
     * @param messageBroker Message broker for messaging system check
     * @param eventHandlers List of event handlers to check their status
     */
    @Inject
    public DefaultHealthCheck(
            Config config,
            Storage storage,
            MessageBroker messageBroker,
            List<BaseEventHandler> eventHandlers) {
        this.config = config;
        this.storage = storage;
        this.messageBroker = messageBroker;
        this.eventHandlers = eventHandlers;
        this.customIndicators = new HashMap<>();
    }

    /**
     * Registers a custom health indicator with a specific name.
     *
     * @param name Name of the health indicator
     * @param indicator The health indicator implementation
     */
    public void registerHealthIndicator(String name, HealthIndicator indicator) {
        customIndicators.put(name, indicator);
    }

    /**
     * Checks if the service is healthy by verifying all critical dependencies.
     *
     * @return true if the service is healthy, false otherwise
     */
    @Override
    public boolean isHealthy() {
        try {
            return checkDatabaseConnection() && 
                   checkMessageBroker() && 
                   checkEventHandlers() && 
                   checkResourceUtilization() &&
                   checkCustomIndicators();
        } catch (Exception e) {
            LOGGER.warn("Health check failed", e);
            return false;
        }
    }

    /**
     * Checks if the service is ready to serve requests.
     *
     * @return true if the service is ready, false otherwise
     */
    @Override
    public boolean isReady() {
        return isHealthy();
    }

    /**
     * Gets detailed health status information for all components.
     *
     * @return Map containing health status details for each component
     */
    @Override
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("status", isHealthy() ? "UP" : "DOWN");

        Map<String, Object> components = new HashMap<>();
        components.put("database", getDatabaseStatus());
        components.put("messageBroker", getMessageBrokerStatus());
        components.put("eventHandlers", getEventHandlersStatus());
        components.put("resources", getResourceStatus());
        
        // Add custom indicators
        for (Map.Entry<String, HealthIndicator> entry : customIndicators.entrySet()) {
            components.put(entry.getKey(), entry.getValue().getStatus());
        }
        
        status.put("components", components);
        return status;
    }

    /**
     * Checks database connection health.
     *
     * @return true if database connection is healthy, false otherwise
     */
    private boolean checkDatabaseConnection() {
        try {
            // Simple validation query to check database connectivity
            storage.validateConnection();
            return true;
        } catch (StorageException e) {
            LOGGER.warn("Database connection check failed", e);
            return false;
        }
    }

    /**
     * Gets detailed status information for the database connection.
     *
     * @return Map containing database health status details
     */
    private Map<String, Object> getDatabaseStatus() {
        Map<String, Object> status = new HashMap<>();
        try {
            storage.validateConnection();
            status.put("status", "UP");
            status.put("details", Map.of(
                "validationQuery", "Connection validated successfully"
            ));
        } catch (StorageException e) {
            status.put("status", "DOWN");
            status.put("details", Map.of(
                "error", e.getMessage(),
                "errorType", e.getClass().getSimpleName()
            ));
        }
        return status;
    }

    /**
     * Checks message broker health.
     *
     * @return true if message broker is healthy, false otherwise
     */
    private boolean checkMessageBroker() {
        try {
            return messageBroker.isConnected();
        } catch (Exception e) {
            LOGGER.warn("Message broker check failed", e);
            return false;
        }
    }

    /**
     * Gets detailed status information for the message broker.
     *
     * @return Map containing message broker health status details
     */
    private Map<String, Object> getMessageBrokerStatus() {
        Map<String, Object> status = new HashMap<>();
        try {
            boolean connected = messageBroker.isConnected();
            status.put("status", connected ? "UP" : "DOWN");
            status.put("details", Map.of(
                "connected", connected,
                "type", messageBroker.getClass().getSimpleName()
            ));
        } catch (Exception e) {
            status.put("status", "DOWN");
            status.put("details", Map.of(
                "error", e.getMessage(),
                "errorType", e.getClass().getSimpleName()
            ));
        }
        return status;
    }

    /**
     * Checks event handlers health.
     *
     * @return true if all event handlers are healthy, false otherwise
     */
    private boolean checkEventHandlers() {
        return !eventHandlers.isEmpty();
    }

    /**
     * Gets detailed status information for the event handlers.
     *
     * @return Map containing event handlers health status details
     */
    private Map<String, Object> getEventHandlersStatus() {
        Map<String, Object> status = new HashMap<>();
        if (eventHandlers.isEmpty()) {
            status.put("status", "DOWN");
            status.put("details", Map.of("error", "No event handlers registered"));
        } else {
            status.put("status", "UP");
            status.put("details", Map.of(
                "count", eventHandlers.size(),
                "handlers", eventHandlers.stream()
                    .map(handler -> handler.getClass().getSimpleName())
                    .toList()
            ));
        }
        return status;
    }

    /**
     * Checks system resource utilization (memory, CPU).
     *
     * @return true if resource utilization is within acceptable thresholds, false otherwise
     */
    private boolean checkResourceUtilization() {
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        long usedHeapMemory = memoryBean.getHeapMemoryUsage().getUsed();
        long maxHeapMemory = memoryBean.getHeapMemoryUsage().getMax();
        double memoryUtilization = (double) usedHeapMemory / maxHeapMemory;

        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        double cpuLoad = osBean.getSystemLoadAverage();
        int availableProcessors = osBean.getAvailableProcessors();
        double normalizedCpuLoad = cpuLoad / availableProcessors;

        double memoryThreshold = config.getDouble(Keys.EVENT_PROCESSOR_MEMORY_THRESHOLD, 0.9);
        double cpuThreshold = config.getDouble(Keys.EVENT_PROCESSOR_CPU_THRESHOLD, 0.9);

        return memoryUtilization < memoryThreshold && (normalizedCpuLoad < 0 || normalizedCpuLoad < cpuThreshold);
    }

    /**
     * Gets detailed status information for system resources.
     *
     * @return Map containing resource utilization health status details
     */
    private Map<String, Object> getResourceStatus() {
        Map<String, Object> status = new HashMap<>();
        try {
            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
            long usedHeapMemory = memoryBean.getHeapMemoryUsage().getUsed();
            long maxHeapMemory = memoryBean.getHeapMemoryUsage().getMax();
            double memoryUtilization = (double) usedHeapMemory / maxHeapMemory;

            OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
            double cpuLoad = osBean.getSystemLoadAverage();
            int availableProcessors = osBean.getAvailableProcessors();
            double normalizedCpuLoad = cpuLoad / availableProcessors;

            double memoryThreshold = config.getDouble(Keys.EVENT_PROCESSOR_MEMORY_THRESHOLD, 0.9);
            double cpuThreshold = config.getDouble(Keys.EVENT_PROCESSOR_CPU_THRESHOLD, 0.9);

            boolean memoryHealthy = memoryUtilization < memoryThreshold;
            boolean cpuHealthy = normalizedCpuLoad < 0 || normalizedCpuLoad < cpuThreshold;

            status.put("status", (memoryHealthy && cpuHealthy) ? "UP" : "DOWN");
            status.put("details", Map.of(
                "memoryUtilization", String.format("%.2f%%", memoryUtilization * 100),
                "memoryThreshold", String.format("%.2f%%", memoryThreshold * 100),
                "memoryUsed", usedHeapMemory,
                "memoryMax", maxHeapMemory,
                "cpuLoad", normalizedCpuLoad >= 0 ? String.format("%.2f", normalizedCpuLoad) : "N/A",
                "cpuThreshold", cpuThreshold,
                "availableProcessors", availableProcessors
            ));
        } catch (Exception e) {
            status.put("status", "UNKNOWN");
            status.put("details", Map.of(
                "error", e.getMessage(),
                "errorType", e.getClass().getSimpleName()
            ));
        }
        return status;
    }

    /**
     * Checks all custom health indicators.
     *
     * @return true if all custom indicators are healthy, false otherwise
     */
    private boolean checkCustomIndicators() {
        for (HealthIndicator indicator : customIndicators.values()) {
            if (!indicator.isHealthy()) {
                return false;
            }
        }
        return true;
    }
}