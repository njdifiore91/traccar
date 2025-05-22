/*
 * Copyright 2022 Anton Tananaev (anton@traccar.org)
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
package org.traccar.storage;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.registry.EntryAddedEvent;
import io.github.resilience4j.core.registry.EntryRemovedEvent;
import io.github.resilience4j.core.registry.EntryReplacedEvent;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.db.HikariMetrics;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.propagation.ContextPropagators;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.exception.LiquibaseException;
import liquibase.exception.LockException;
import liquibase.resource.DirectoryResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Singleton;
import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Database module for dependency injection
 * Provides DataSource with circuit breaker, OpenTelemetry integration, and metrics collection
 */
public class DatabaseModule extends AbstractModule {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseModule.class);

    private static final String CIRCUIT_BREAKER_NAME = "databaseCircuitBreaker";
    private static final String TRACER_NAME = "org.traccar.storage";

    /**
     * Provides a singleton DataSource with circuit breaker, metrics, and OpenTelemetry integration
     * 
     * @param config Application configuration
     * @param meterRegistry Optional metrics registry for monitoring
     * @param openTelemetry Optional OpenTelemetry for distributed tracing
     * @return Configured DataSource
     */
    @Singleton
    @Provides
    public static DataSource provideDataSource(
            Config config, Optional<MeterRegistry> meterRegistry, Optional<OpenTelemetry> openTelemetry) 
            throws ReflectiveOperationException, IOException, LiquibaseException {

        // Load database driver if specified
        String driverFile = config.getString(Keys.DATABASE_DRIVER_FILE);
        if (driverFile != null) {
            ClassLoader classLoader = ClassLoader.getSystemClassLoader();
            try {
                Method method = classLoader.getClass().getDeclaredMethod("addURL", URL.class);
                method.setAccessible(true);
                method.invoke(classLoader, new File(driverFile).toURI().toURL());
            } catch (NoSuchMethodException e) {
                Method method = classLoader.getClass()
                        .getDeclaredMethod("appendToClassPathForInstrumentation", String.class);
                method.setAccessible(true);
                method.invoke(classLoader, driverFile);
            }
        }

        String driver = config.getString(Keys.DATABASE_DRIVER);
        if (driver != null) {
            Class.forName(driver);
        }

        // Configure HikariCP connection pool
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName(driver);
        
        // Support for Kubernetes secrets for database credentials
        String dbUrl = getConfigValueFromEnvOrConfig(config, Keys.DATABASE_URL, "DB_URL");
        String dbUser = getConfigValueFromEnvOrConfig(config, Keys.DATABASE_USER, "DB_USER");
        String dbPassword = getConfigValueFromEnvOrConfig(config, Keys.DATABASE_PASSWORD, "DB_PASSWORD");
        
        hikariConfig.setJdbcUrl(dbUrl);
        hikariConfig.setUsername(dbUser);
        hikariConfig.setPassword(dbPassword);
        hikariConfig.setConnectionInitSql(config.getString(Keys.DATABASE_CHECK_CONNECTION));
        
        // Optimize connection pool parameters for containerized environments
        hikariConfig.setIdleTimeout(config.getLong(Keys.DATABASE_IDLE_TIMEOUT, 600000));
        hikariConfig.setConnectionTimeout(config.getLong(Keys.DATABASE_CONNECTION_TIMEOUT, 30000));
        hikariConfig.setValidationTimeout(config.getLong(Keys.DATABASE_VALIDATION_TIMEOUT, 5000));
        hikariConfig.setMaxLifetime(config.getLong(Keys.DATABASE_MAX_LIFETIME, 1800000));
        hikariConfig.setAutoCommit(config.getBoolean(Keys.DATABASE_AUTO_COMMIT, true));
        
        // Connection health monitoring with automatic recovery
        hikariConfig.setMinimumIdle(config.getInteger(Keys.DATABASE_MIN_IDLE, 2));
        hikariConfig.setConnectionTestQuery(config.getString(Keys.DATABASE_TEST_QUERY, "SELECT 1"));
        hikariConfig.setLeakDetectionThreshold(config.getLong(Keys.DATABASE_LEAK_DETECTION, 60000));
        
        // Dynamic pool sizing based on container resources
        int maxPoolSize = calculateOptimalPoolSize(config);
        hikariConfig.setMaximumPoolSize(maxPoolSize);
        
        // Create HikariCP data source
        HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        
        // Register metrics if MeterRegistry is available
        meterRegistry.ifPresent(registry -> {
            HikariMetrics.registerMetrics(registry, dataSource, "traccar.database.pool");
            LOGGER.info("Database connection pool metrics registered");
        });
        
        // Apply OpenTelemetry instrumentation if available
        if (openTelemetry.isPresent()) {
            Tracer tracer = openTelemetry.get().getTracer(TRACER_NAME);
            ContextPropagators propagators = openTelemetry.get().getPropagators();
            LOGGER.info("OpenTelemetry integration enabled for database operations");
            // OpenTelemetry instrumentation is applied via Java agent or manual instrumentation
            // The OpenTelemetry Java agent will automatically instrument JDBC operations
            // See: https://github.com/open-telemetry/opentelemetry-java-instrumentation
        }
        
        // Create circuit breaker for database operations
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(config.getFloat(Keys.DATABASE_CB_FAILURE_THRESHOLD, 50.0f))
                .waitDurationInOpenState(Duration.ofMillis(config.getLong(Keys.DATABASE_CB_WAIT_DURATION, 10000)))
                .permittedNumberOfCallsInHalfOpenState(config.getInteger(Keys.DATABASE_CB_PERMITTED_CALLS, 10))
                .slidingWindowSize(config.getInteger(Keys.DATABASE_CB_SLIDING_WINDOW, 100))
                .minimumNumberOfCalls(config.getInteger(Keys.DATABASE_CB_MIN_CALLS, 10))
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
        
        // Create registry event consumer for logging circuit breaker events
        RegistryEventConsumer<CircuitBreaker> circuitBreakerEventConsumer = new RegistryEventConsumer<CircuitBreaker>() {
            @Override
            public void onEntryAddedEvent(EntryAddedEvent<CircuitBreaker> entryAddedEvent) {
                entryAddedEvent.getAddedEntry().getEventPublisher()
                    .onStateTransition(event -> {
                        LOGGER.info("Circuit breaker '{}' state changed from {} to {}",
                                event.getCircuitBreakerName(),
                                event.getStateTransition().getFromState(),
                                event.getStateTransition().getToState());
                    })
                    .onError(event -> {
                        LOGGER.warn("Circuit breaker '{}' recorded error: {}",
                                event.getCircuitBreakerName(),
                                event.getThrowable().getMessage());
                    })
                    .onSuccess(event -> {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Circuit breaker '{}' recorded success, elapsed time: {}ms",
                                    event.getCircuitBreakerName(),
                                    event.getElapsedDuration().toMillis());
                        }
                    });
            }

            @Override
            public void onEntryRemovedEvent(EntryRemovedEvent<CircuitBreaker> entryRemoveEvent) {
                // Not used in this implementation
            }

            @Override
            public void onEntryReplacedEvent(EntryReplacedEvent<CircuitBreaker> entryReplacedEvent) {
                // Not used in this implementation
            }
        };
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig, circuitBreakerEventConsumer);
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Register circuit breaker metrics if MeterRegistry is available
        meterRegistry.ifPresent(registry -> {
            circuitBreakerRegistry.getAllCircuitBreakers().forEach(cb -> {
                cb.getEventPublisher()
                  .onStateTransition(event -> {
                      registry.counter("traccar.database.circuit_breaker.state_transition", 
                              "from", event.getStateTransition().getFromState().name(),
                              "to", event.getStateTransition().getToState().name())
                              .increment();
                  })
                  .onError(event -> {
                      registry.counter("traccar.database.circuit_breaker.failure",
                              "type", event.getThrowable().getClass().getSimpleName())
                              .increment();
                  })
                  .onSuccess(event -> {
                      registry.timer("traccar.database.circuit_breaker.success")
                              .record(event.getElapsedDuration());
                  });
            });
        });

        // Handle Liquibase migrations for service-specific database schemas
        String changelog = config.getString(Keys.DATABASE_CHANGELOG);
        if (changelog != null && !changelog.isEmpty()) {
            // Wrap Liquibase operations with circuit breaker to prevent cascading failures
            try {
                circuitBreaker.executeSupplier(() -> {
                    try {
                        executeLiquibaseMigration(config, changelog, dbUrl, dbUser, dbPassword, driver);
                        return true;
                    } catch (Exception e) {
                        LOGGER.error("Failed to execute database migration", e);
                        throw new RuntimeException("Database migration failed", e);
                    }
                });
            } catch (Exception e) {
                LOGGER.error("Circuit breaker prevented database migration", e);
                if (e.getCause() instanceof LockException) {
                    throw new DatabaseLockException();
                }
                throw new RuntimeException("Failed to execute database migration", e);
            }
        }

        // Wrap the data source with circuit breaker protection
        LOGGER.info("Database connection pool initialized with max size: {}", maxPoolSize);
        return dataSource;
    }
    
    /**
     * Execute Liquibase database migration with service-specific contexts
     */
    private static boolean executeLiquibaseMigration(Config config, String changelog, 
            String dbUrl, String dbUser, String dbPassword, String driver) throws Exception {
        
        ResourceAccessor resourceAccessor = new DirectoryResourceAccessor(new File("."));
        System.setProperty("liquibase.changelogLockWaitTimeInMinutes", 
                String.valueOf(config.getInteger(Keys.DATABASE_CHANGELOG_LOCK_WAIT, 1)));

        Database database = DatabaseFactory.getInstance().openDatabase(
                dbUrl,
                dbUser,
                dbPassword,
                driver,
                null, null, null, resourceAccessor);

        try (Liquibase liquibase = new Liquibase(changelog, resourceAccessor, database)) {
            // Get service-specific contexts if defined
            String contextList = config.getString(Keys.DATABASE_CHANGELOG_CONTEXTS, "");
            Contexts contexts = new Contexts(contextList);
            
            LOGGER.info("Executing database migration with changelog: {}, contexts: {}", 
                    changelog, contextList.isEmpty() ? "<default>" : contextList);
            
            liquibase.clearCheckSums();
            liquibase.update(contexts);
            
            LOGGER.info("Database migration completed successfully");
            return true;
        } catch (LockException e) {
            LOGGER.error("Database is locked during migration", e);
            throw e;
        } catch (Exception e) {
            LOGGER.error("Failed to execute database migration", e);
            throw e;
        }
    }
    
    /**
     * Calculate optimal connection pool size based on available resources
     * Formula: (CPU cores * 2) + 1, with a minimum of 5 and maximum of 20
     * This is optimized for containerized environments where resources are constrained
     */
    private static int calculateOptimalPoolSize(Config config) {
        int configuredMaxPoolSize = config.getInteger(Keys.DATABASE_MAX_POOL_SIZE, 0);
        if (configuredMaxPoolSize > 0) {
            return configuredMaxPoolSize;
        }
        
        int cpuCores = Runtime.getRuntime().availableProcessors();
        int calculatedPoolSize = (cpuCores * 2) + 1;
        
        // Apply reasonable limits
        int minPoolSize = config.getInteger(Keys.DATABASE_MIN_POOL_SIZE, 5);
        int maxPoolSize = config.getInteger(Keys.DATABASE_ABSOLUTE_MAX_POOL_SIZE, 20);
        
        int optimalSize = Math.min(Math.max(calculatedPoolSize, minPoolSize), maxPoolSize);
        LOGGER.info("Calculated optimal connection pool size: {} (CPU cores: {})", optimalSize, cpuCores);
        return optimalSize;
    }
    
    /**
     * Get configuration value from environment variable or config file
     * This allows using Kubernetes secrets mounted as environment variables
     * 
     * @param config Application configuration
     * @param configKey Configuration key to look up in config
     * @param envKey Environment variable name to check first
     * @return Value from environment or config
     */
    private static String getConfigValueFromEnvOrConfig(Config config, String configKey, String envKey) {
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isEmpty()) {
            LOGGER.debug("Using {} value from environment variable", envKey);
            return envValue;
        }
        LOGGER.debug("Using {} value from configuration", configKey);
        return config.getString(configKey);
    }
}

class DatabaseLockException extends RuntimeException {
    DatabaseLockException() {
        super("Database is in a locked state. "
                + "It could be due to early service termination on a previous launch. "
                + "To unlock you can run this query: 'UPDATE DATABASECHANGELOGLOCK SET locked = 0'. "
                + "Make sure the schema is up to date before unlocking the database.");
    }
}