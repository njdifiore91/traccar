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
package org.traccar.health;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;
import org.traccar.config.Config;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Health indicator for database connectivity and performance.
 * Monitors the health of the database used by the Position Processing Service.
 */
@Component
public class DatabaseHealthIndicator implements HealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseHealthIndicator.class);

    private static final String CONNECTION_POOL_KEY = "connectionPool";
    private static final String ACTIVE_CONNECTIONS_KEY = "activeConnections";
    private static final String IDLE_CONNECTIONS_KEY = "idleConnections";
    private static final String TOTAL_CONNECTIONS_KEY = "totalConnections";
    private static final String MAX_CONNECTIONS_KEY = "maxConnections";
    private static final String MIN_CONNECTIONS_KEY = "minConnections";
    private static final String CONNECTION_TIMEOUT_KEY = "connectionTimeoutMs";
    private static final String CONNECTION_WAIT_COUNT_KEY = "connectionWaitCount";
    private static final String CONNECTION_STATUS_KEY = "connectionStatus";
    private static final String QUERY_RESPONSE_TIME_KEY = "queryResponseTimeMs";
    private static final String DATABASE_TYPE_KEY = "databaseType";
    private static final String DATABASE_VERSION_KEY = "databaseVersion";
    private static final String LAST_ERROR_KEY = "lastError";
    private static final String LAST_SUCCESS_TIME_KEY = "lastSuccessTime";
    
    private static final int TIMEOUT_SECONDS = 5;
    private static final long MAX_RESPONSE_TIME_MS = 1000; // 1 second max response time
    private static final String TEST_QUERY = "SELECT 1";

    @Autowired
    private Config config;
    
    @Autowired
    private DataSource dataSource;
    
    @Autowired
    private Storage storage;
    
    @Autowired(required = false)
    private MeterRegistry meterRegistry;

    // Metrics for tracking database performance
    private final AtomicInteger totalQueries = new AtomicInteger(0);
    private final AtomicInteger failedQueries = new AtomicInteger(0);
    private final AtomicLong lastSuccessTimestamp = new AtomicLong(0);
    private String lastErrorMessage = null;

    // Metrics for Prometheus/Micrometer
    private Timer queryResponseTimeTimer;

    @Autowired
    public void init() {
        if (meterRegistry != null) {
            queryResponseTimeTimer = Timer.builder("database.query.response.time")
                    .description("Database query response time")
                    .register(meterRegistry);
        }
    }

    @Override
    public Health health() {
        if (dataSource == null) {
            return Health.down()
                    .withDetail("error", "DataSource not configured")
                    .build();
        }

        Health.Builder builder = new Health.Builder();
        
        try {
            // Check if the dataSource is a HikariDataSource to get connection pool metrics
            if (dataSource instanceof HikariDataSource) {
                addHikariPoolMetrics(builder, (HikariDataSource) dataSource);
            }
            
            // Test database connectivity with a simple query
            long startTime = System.currentTimeMillis();
            totalQueries.incrementAndGet();
            
            try {
                // First approach: Use the Storage abstraction
                checkDatabaseWithStorage();
                
                // If that succeeds, record success
                long responseTime = System.currentTimeMillis() - startTime;
                if (queryResponseTimeTimer != null) {
                    queryResponseTimeTimer.record(responseTime, TimeUnit.MILLISECONDS);
                }
                lastSuccessTimestamp.set(System.currentTimeMillis());
                
                // Add metrics to health check
                builder.withDetail(CONNECTION_STATUS_KEY, "connected")
                       .withDetail(QUERY_RESPONSE_TIME_KEY, responseTime);
                
                // Determine health status based on response time
                if (responseTime > MAX_RESPONSE_TIME_MS) {
                    builder.down()
                           .withDetail("error", "Response time exceeds threshold: " + responseTime + "ms");
                } else {
                    builder.up();
                }
                
                // Add database metadata if available
                try (Connection connection = dataSource.getConnection()) {
                    builder.withDetail(DATABASE_TYPE_KEY, connection.getMetaData().getDatabaseProductName())
                           .withDetail(DATABASE_VERSION_KEY, connection.getMetaData().getDatabaseProductVersion());
                }
                
            } catch (StorageException e) {
                // If Storage abstraction fails, try direct JDBC approach as fallback
                try (Connection connection = dataSource.getConnection();
                     PreparedStatement statement = connection.prepareStatement(TEST_QUERY);
                     ResultSet resultSet = statement.executeQuery()) {
                    
                    if (resultSet.next()) {
                        long responseTime = System.currentTimeMillis() - startTime;
                        if (queryResponseTimeTimer != null) {
                            queryResponseTimeTimer.record(responseTime, TimeUnit.MILLISECONDS);
                        }
                        lastSuccessTimestamp.set(System.currentTimeMillis());
                        
                        builder.withDetail(CONNECTION_STATUS_KEY, "connected")
                               .withDetail(QUERY_RESPONSE_TIME_KEY, responseTime);
                        
                        if (responseTime > MAX_RESPONSE_TIME_MS) {
                            builder.down()
                                   .withDetail("error", "Response time exceeds threshold: " + responseTime + "ms");
                        } else {
                            builder.up();
                        }
                        
                        builder.withDetail(DATABASE_TYPE_KEY, connection.getMetaData().getDatabaseProductName())
                               .withDetail(DATABASE_VERSION_KEY, connection.getMetaData().getDatabaseProductVersion());
                    } else {
                        return handleDatabaseError(builder, "Database test query returned no results", null);
                    }
                } catch (SQLException e2) {
                    return handleDatabaseError(builder, "Database connectivity error", e2);
                }
            }
            
            // Add error rate statistics
            double errorRate = totalQueries.get() > 0 
                    ? (double) failedQueries.get() / totalQueries.get() 
                    : 0.0;
            
            builder.withDetail("errorRate", errorRate)
                   .withDetail("queriesTotal", totalQueries.get())
                   .withDetail("errorsTotal", failedQueries.get())
                   .withDetail(LAST_SUCCESS_TIME_KEY, lastSuccessTimestamp.get());
            
        } catch (Exception e) {
            return handleDatabaseError(builder, "Unexpected error checking database health", e);
        }
        
        return builder.build();
    }
    
    private void checkDatabaseWithStorage() throws StorageException {
        // Use the Storage abstraction to perform a simple query
        // This is preferred as it uses the same code path as the application
        storage.getObjects(Object.class, new Request(
                new Columns.All(), new Condition.Raw(TEST_QUERY)));
    }
    
    private void addHikariPoolMetrics(Health.Builder builder, HikariDataSource hikariDataSource) {
        try {
            HikariPoolMXBean poolMXBean = hikariDataSource.getHikariPoolMXBean();
            
            if (poolMXBean != null) {
                builder.withDetail(CONNECTION_POOL_KEY, "HikariCP")
                       .withDetail(ACTIVE_CONNECTIONS_KEY, poolMXBean.getActiveConnections())
                       .withDetail(IDLE_CONNECTIONS_KEY, poolMXBean.getIdleConnections())
                       .withDetail(TOTAL_CONNECTIONS_KEY, poolMXBean.getTotalConnections())
                       .withDetail(CONNECTION_WAIT_COUNT_KEY, poolMXBean.getThreadsAwaitingConnection())
                       .withDetail(MAX_CONNECTIONS_KEY, hikariDataSource.getMaximumPoolSize())
                       .withDetail(MIN_CONNECTIONS_KEY, hikariDataSource.getMinimumIdle())
                       .withDetail(CONNECTION_TIMEOUT_KEY, hikariDataSource.getConnectionTimeout());
                
                // Check if connection pool is exhausted or close to it
                if (poolMXBean.getThreadsAwaitingConnection() > 0) {
                    builder.down()
                           .withDetail("error", "Connection pool has threads waiting for connections: " 
                                   + poolMXBean.getThreadsAwaitingConnection());
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Error getting HikariCP metrics", e);
            builder.withDetail(CONNECTION_POOL_KEY, "HikariCP (metrics unavailable)");
        }
    }

    private Health handleDatabaseError(Health.Builder builder, String message, Throwable e) {
        if (e != null) {
            LOGGER.error(message, e);
            lastErrorMessage = e.getMessage();
        } else {
            LOGGER.error(message);
            lastErrorMessage = message;
        }
        
        failedQueries.incrementAndGet();
        
        // Calculate error rate
        double errorRate = totalQueries.get() > 0 
                ? (double) failedQueries.get() / totalQueries.get() 
                : 0.0;

        return builder.down()
                .withDetail(CONNECTION_STATUS_KEY, "error")
                .withDetail("errorRate", errorRate)
                .withDetail("queriesTotal", totalQueries.get())
                .withDetail("errorsTotal", failedQueries.get())
                .withDetail(LAST_ERROR_KEY, lastErrorMessage)
                .withDetail(LAST_SUCCESS_TIME_KEY, lastSuccessTimestamp.get())
                .withDetail("error", message + (e != null ? ": " + e.getMessage() : ""))
                .build();
    }
}