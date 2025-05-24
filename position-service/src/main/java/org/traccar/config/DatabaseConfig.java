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
package org.traccar.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import liquibase.integration.spring.SpringLiquibase;
import org.springframework.boot.actuate.jdbc.DataSourceHealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

/**
 * Database configuration for the Position Processing Service.
 * Configures database connections, connection pooling, transaction management,
 * health indicators, and database migrations.
 */
@Configuration
@EnableTransactionManagement
public class DatabaseConfig {

    private final Config config;

    public DatabaseConfig(Config config) {
        this.config = config;
    }

    /**
     * Configures and provides a HikariCP data source.
     * Supports multiple database types (MySQL, PostgreSQL, H2, MS SQL Server) through JDBC.
     *
     * @return Configured HikariDataSource
     */
    @Bean
    public DataSource dataSource() {
        HikariConfig hikariConfig = new HikariConfig();
        
        // Set database connection properties from configuration
        hikariConfig.setJdbcUrl(config.getString(Keys.DATABASE_URL));
        hikariConfig.setDriverClassName(config.getString(Keys.DATABASE_DRIVER));
        hikariConfig.setUsername(config.getString(Keys.DATABASE_USER));
        hikariConfig.setPassword(config.getString(Keys.DATABASE_PASSWORD));
        
        // Set connection pool properties
        if (config.hasKey(Keys.DATABASE_MAX_POOL_SIZE)) {
            hikariConfig.setMaximumPoolSize(config.getInteger(Keys.DATABASE_MAX_POOL_SIZE));
        }
        
        // Configure connection validation
        hikariConfig.setConnectionTestQuery(config.getString(Keys.DATABASE_CHECK_CONNECTION));
        
        // Configure pool metrics
        hikariConfig.setRegisterMbeans(true);
        hikariConfig.setMetricRegistry(new com.codahale.metrics.MetricRegistry());
        
        // Set pool name for identification in metrics and logs
        hikariConfig.setPoolName("position-service-db-pool");
        
        return new HikariDataSource(hikariConfig);
    }

    /**
     * Provides a JdbcTemplate for database operations.
     *
     * @param dataSource The configured data source
     * @return JdbcTemplate instance
     */
    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * Configures transaction management for database operations.
     *
     * @param dataSource The configured data source
     * @return Transaction manager
     */
    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    /**
     * Configures health indicator for database connections.
     * Used by Spring Actuator for health monitoring.
     *
     * @param dataSource The configured data source
     * @return Health indicator for database connections
     */
    @Bean
    public DataSourceHealthIndicator dataSourceHealthIndicator(DataSource dataSource) {
        return new DataSourceHealthIndicator(dataSource, config.getString(Keys.DATABASE_CHECK_CONNECTION));
    }

    /**
     * Configures Liquibase for database migrations.
     * Applies database schema changes automatically on startup.
     *
     * @param dataSource The configured data source
     * @return Configured Liquibase bean
     */
    @Bean
    public SpringLiquibase liquibase(DataSource dataSource) {
        SpringLiquibase liquibase = new SpringLiquibase();
        liquibase.setDataSource(dataSource);
        liquibase.setChangeLog(config.getString(Keys.DATABASE_CHANGELOG));
        liquibase.setContexts("position-service");
        return liquibase;
    }
}