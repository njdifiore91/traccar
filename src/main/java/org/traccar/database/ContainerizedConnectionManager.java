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
package org.traccar.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.metrics.prometheus.PrometheusMetricsTrackerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import javax.sql.DataSource;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Manages database connections in containerized environments, optimizing connection management
 * for Kubernetes deployments. This class handles container lifecycle events (preStop, postStart),
 * dynamically sizes connection pools based on container resources, and implements graceful
 * connection handling during container shutdown.
 */
@Singleton
public class ContainerizedConnectionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerizedConnectionManager.class);

    private final Config config;
    private HikariDataSource dataSource;
    private boolean containerized;
    private boolean shuttingDown;
    private final Object shutdownLock = new Object();

    /**
     * Constructs a new ContainerizedConnectionManager with the specified configuration.
     *
     * @param config The application configuration
     */
    @Inject
    public ContainerizedConnectionManager(Config config) {
        this.config = config;
        this.containerized = detectContainerEnvironment();
        this.shuttingDown = false;
        initializeDataSource();
        registerShutdownHook();
    }

    /**
     * Detects if the application is running in a containerized environment.
     * 
     * @return true if running in a container, false otherwise
     */
    private boolean detectContainerEnvironment() {
        // Check for container environment variables
        if (System.getenv("KUBERNETES_SERVICE_HOST") != null) {
            LOGGER.info("Kubernetes environment detected");
            return true;
        }
        
        // Check for container cgroup indicators
        File cgroupFile = new File("/proc/1/cgroup");
        if (cgroupFile.exists()) {
            LOGGER.info("Container environment detected via cgroups");
            return true;
        }
        
        // Check for container-specific files
        File dockerFile = new File("/.dockerenv");
        if (dockerFile.exists()) {
            LOGGER.info("Docker environment detected");
            return true;
        }
        
        LOGGER.info("No container environment detected, using standard connection management");
        return false;
    }

    /**
     * Initializes the HikariCP data source with container-optimized settings.
     */
    private void initializeDataSource() {
        HikariConfig hikariConfig = new HikariConfig();
        
        // Basic database connection settings
        hikariConfig.setDriverClassName(config.getString(Keys.DATABASE_DRIVER));
        hikariConfig.setJdbcUrl(config.getString(Keys.DATABASE_URL));
        hikariConfig.setUsername(config.getString(Keys.DATABASE_USER));
        hikariConfig.setPassword(config.getString(Keys.DATABASE_PASSWORD));
        
        // Connection pool settings
        if (containerized) {
            // Dynamic pool sizing based on container resources
            int maxPoolSize = calculateOptimalPoolSize();
            hikariConfig.setMaximumPoolSize(maxPoolSize);
            LOGGER.info("Container-optimized connection pool size: {}", maxPoolSize);
            
            // Faster connection acquisition for containerized environments
            hikariConfig.setConnectionTimeout(TimeUnit.SECONDS.toMillis(5));
            hikariConfig.setInitializationFailTimeout(TimeUnit.SECONDS.toMillis(10));
            
            // Enable metrics for monitoring in container orchestration
            hikariConfig.setMetricsTrackerFactory(new PrometheusMetricsTrackerFactory());
            
            // Optimize for container lifecycle
            hikariConfig.setMinimumIdle(2); // Keep minimum connections ready
            hikariConfig.setIdleTimeout(TimeUnit.MINUTES.toMillis(3));
        } else {
            // Standard settings for non-containerized environments
            hikariConfig.setMaximumPoolSize(config.getInteger(Keys.DATABASE_MAX_POOL_SIZE, 10));
            hikariConfig.setConnectionTimeout(TimeUnit.SECONDS.toMillis(30));
        }
        
        // Common settings
        hikariConfig.setAutoCommit(true);
        hikariConfig.setPoolName("TraccarDB");
        hikariConfig.setRegisterMbeans(true);
        
        // Connection validation
        hikariConfig.setValidationTimeout(TimeUnit.SECONDS.toMillis(3));
        if (config.getBoolean(Keys.DATABASE_CHECK_CONNECTION)) {
            hikariConfig.setConnectionTestQuery("SELECT 1");
        }
        
        dataSource = new HikariDataSource(hikariConfig);
    }

    /**
     * Calculates the optimal connection pool size based on available container resources.
     * 
     * @return The optimal connection pool size
     */
    private int calculateOptimalPoolSize() {
        // Get available processors (CPU cores)
        int availableCores = Runtime.getRuntime().availableProcessors();
        
        // Get available memory
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        long maxMemory = Runtime.getRuntime().maxMemory();
        
        // Calculate based on resources
        // Formula: min(cores * 2 + 1, memory_based_connections, config_max_pool_size)
        int coreBased = availableCores * 2 + 1;
        
        // Memory-based calculation (rough estimate: 10MB per connection)
        int memoryBased = (int) (maxMemory / (10 * 1024 * 1024));
        
        // Get configured maximum if available
        int configMax = config.getInteger(Keys.DATABASE_MAX_POOL_SIZE, 20);
        
        // Use the minimum of the three values to avoid over-allocation
        int optimalSize = Math.min(coreBased, Math.min(memoryBased, configMax));
        
        // Ensure at least 3 connections for small containers
        return Math.max(optimalSize, 3);
    }

    /**
     * Registers a JVM shutdown hook to handle graceful connection pool shutdown.
     */
    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown hook triggered, closing database connections gracefully");
            shutdownGracefully();
        }));
    }

    /**
     * Handles the preStop lifecycle hook for Kubernetes.
     * This should be called when the container receives a termination signal.
     */
    public void handlePreStop() {
        if (containerized) {
            LOGGER.info("Container preStop hook received, preparing for shutdown");
            synchronized (shutdownLock) {
                shuttingDown = true;
            }
            
            // Start graceful connection draining
            dataSource.setMaximumPoolSize(1); // Reduce to minimum
            
            // Allow time for in-flight transactions to complete
            try {
                // Wait for active connections to reduce
                int timeout = 0;
                while (dataSource.getHikariPoolMXBean().getActiveConnections() > 1 && timeout < 10) {
                    LOGGER.info("Waiting for active connections to drain: {}", 
                            dataSource.getHikariPoolMXBean().getActiveConnections());
                    Thread.sleep(500);
                    timeout++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Interrupted during preStop connection draining", e);
            }
        }
    }

    /**
     * Handles the postStart lifecycle hook for Kubernetes.
     * This should be called when the container has started.
     */
    public void handlePostStart() {
        if (containerized) {
            LOGGER.info("Container postStart hook received, optimizing connection pool");
            
            // Warm up the connection pool
            try (Connection connection = dataSource.getConnection()) {
                LOGGER.debug("Connection pool initialized successfully");
            } catch (SQLException e) {
                LOGGER.warn("Failed to initialize connection pool during postStart", e);
            }
            
            // Reset shutdown flag if it was previously set
            synchronized (shutdownLock) {
                shuttingDown = false;
            }
        }
    }

    /**
     * Performs a health check on the database connection.
     * 
     * @return true if the database is accessible, false otherwise
     */
    public boolean performHealthCheck() {
        synchronized (shutdownLock) {
            if (shuttingDown) {
                LOGGER.info("Health check skipped - system is shutting down");
                return false;
            }
        }
        
        try (Connection connection = dataSource.getConnection()) {
            if (connection.isValid(3)) { // 3 second timeout
                return true;
            }
        } catch (SQLException e) {
            LOGGER.warn("Database health check failed", e);
        }
        return false;
    }

    /**
     * Shuts down the connection pool gracefully.
     */
    public void shutdownGracefully() {
        synchronized (shutdownLock) {
            if (shuttingDown) {
                return; // Already shutting down
            }
            shuttingDown = true;
        }
        
        LOGGER.info("Shutting down database connection pool gracefully");
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    /**
     * Gets the managed data source.
     * 
     * @return The HikariCP data source
     */
    public DataSource getDataSource() {
        synchronized (shutdownLock) {
            if (shuttingDown) {
                LOGGER.warn("Attempted to get data source during shutdown");
            }
        }
        return dataSource;
    }

    /**
     * Checks if the system is currently shutting down.
     * 
     * @return true if the system is shutting down, false otherwise
     */
    public boolean isShuttingDown() {
        synchronized (shutdownLock) {
            return shuttingDown;
        }
    }

    /**
     * Checks if the application is running in a containerized environment.
     * 
     * @return true if running in a container, false otherwise
     */
    public boolean isContainerized() {
        return containerized;
    }
}