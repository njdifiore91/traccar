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
package org.traccar.database;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.concurrent.TimeUnit;

/**
 * Collects and exposes database metrics for monitoring database performance and health.
 * This class integrates with Micrometer to provide metrics for Prometheus scraping.
 * 
 * Metrics collected include:
 * - Query execution times
 * - Connection pool utilization
 * - Error rates
 * - Query throughput
 */
@Singleton
public class DatabaseMetricsCollector implements MeterBinder {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseMetricsCollector.class);

    private final HikariDataSource dataSource;
    private MeterRegistry registry;

    // Counters for tracking query statistics
    private Counter queriesExecuted;
    private Counter queriesSucceeded;
    private Counter queriesFailed;
    
    // Timer for measuring query execution time
    private Timer queryTimer;

    /**
     * Constructs a new DatabaseMetricsCollector.
     *
     * @param dataSource The HikariDataSource to monitor
     */
    @Inject
    public DatabaseMetricsCollector(HikariDataSource dataSource) {
        this.dataSource = dataSource;
        LOGGER.info("Database metrics collector initialized");
    }

    /**
     * Binds all database metrics to the provided MeterRegistry.
     * This method is called automatically by Spring Boot when using the @MeterBinder interface.
     *
     * @param registry The MeterRegistry to bind metrics to
     */
    @Override
    public void bindTo(MeterRegistry registry) {
        this.registry = registry;
        
        // Initialize counters
        queriesExecuted = Counter.builder("database.queries.total")
                .description("Total number of database queries executed")
                .register(registry);
        
        queriesSucceeded = Counter.builder("database.queries.succeeded")
                .description("Number of database queries that succeeded")
                .register(registry);
        
        queriesFailed = Counter.builder("database.queries.failed")
                .description("Number of database queries that failed")
                .register(registry);
        
        // Initialize timer for query execution time
        queryTimer = Timer.builder("database.query.time")
                .description("Database query execution time")
                .publishPercentiles(0.5, 0.95, 0.99) // Publish 50th, 95th, and 99th percentiles
                .publishPercentileHistogram()
                .register(registry);
        
        // Register connection pool metrics if available
        registerPoolMetrics();
        
        LOGGER.debug("Database metrics bound to registry");
    }

    /**
     * Registers metrics related to the connection pool.
     */
    private void registerPoolMetrics() {
        try {
            HikariPoolMXBean poolProxy = dataSource.getHikariPoolMXBean();
            
            if (poolProxy != null) {
                // Active connections gauge
                Gauge.builder("database.connections.active", poolProxy, HikariPoolMXBean::getActiveConnections)
                        .description("Number of active connections in the pool")
                        .register(registry);
                
                // Idle connections gauge
                Gauge.builder("database.connections.idle", poolProxy, HikariPoolMXBean::getIdleConnections)
                        .description("Number of idle connections in the pool")
                        .register(registry);
                
                // Total connections gauge
                Gauge.builder("database.connections.total", poolProxy, 
                        p -> p.getActiveConnections() + p.getIdleConnections())
                        .description("Total number of connections in the pool")
                        .register(registry);
                
                // Waiting threads gauge
                Gauge.builder("database.connections.waiting", poolProxy, HikariPoolMXBean::getThreadsAwaitingConnection)
                        .description("Number of threads waiting for a connection")
                        .register(registry);
                
                // Connection timeout counter
                Counter connectionTimeouts = Counter.builder("database.connections.timeouts")
                        .description("Number of connection timeouts")
                        .register(registry);
                
                LOGGER.debug("Connection pool metrics registered");
            } else {
                LOGGER.warn("HikariPoolMXBean not available, connection pool metrics will not be collected");
            }
        } catch (Exception e) {
            LOGGER.error("Failed to register connection pool metrics", e);
        }
    }

    /**
     * Records the execution of a database query.
     * This method should be called before executing a query.
     *
     * @return A Timer.Sample that can be used to record the query execution time
     */
    public Timer.Sample startQueryTimer() {
        queriesExecuted.increment();
        return Timer.start(registry);
    }

    /**
     * Records the successful completion of a database query.
     * This method should be called after a query completes successfully.
     *
     * @param sample The Timer.Sample returned by startQueryTimer()
     */
    public void recordQuerySuccess(Timer.Sample sample) {
        queriesSucceeded.increment();
        sample.stop(queryTimer);
    }

    /**
     * Records a failed database query.
     * This method should be called when a query fails.
     *
     * @param sample The Timer.Sample returned by startQueryTimer()
     * @param exception The exception that caused the failure
     */
    public void recordQueryFailure(Timer.Sample sample, Exception exception) {
        queriesFailed.increment();
        sample.stop(queryTimer);
        LOGGER.debug("Database query failed", exception);
    }

    /**
     * Records the execution time of a database query directly.
     * This method can be used when the timing was measured externally.
     *
     * @param executionTime The execution time
     * @param unit The time unit of the executionTime parameter
     * @param success Whether the query was successful
     */
    public void recordQueryExecutionTime(long executionTime, TimeUnit unit, boolean success) {
        queriesExecuted.increment();
        queryTimer.record(executionTime, unit);
        
        if (success) {
            queriesSucceeded.increment();
        } else {
            queriesFailed.increment();
        }
    }

    /**
     * Records a connection timeout.
     * This method should be called when a connection timeout occurs.
     */
    public void recordConnectionTimeout() {
        Counter connectionTimeouts = registry.counter("database.connections.timeouts");
        connectionTimeouts.increment();
        LOGGER.warn("Database connection timeout occurred");
    }
}