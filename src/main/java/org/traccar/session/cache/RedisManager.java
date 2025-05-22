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
package org.traccar.session.cache;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

/**
 * RedisManager provides a simplified interface for Redis operations used for
 * distributed state management. It handles connection pooling and basic Redis
 * operations with metrics and error handling.
 */
@Singleton
public class RedisManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisManager.class);

    private final JedisPool jedisPool;
    private final MeterRegistry meterRegistry;

    /**
     * Constructs a new RedisManager with the required dependencies.
     *
     * @param config System configuration
     * @param meterRegistry For metrics collection
     */
    @Inject
    public RedisManager(Config config, MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;

        // Configure Redis connection pool
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(config.getInteger("redis.pool.maxTotal", 128));
        poolConfig.setMaxIdle(config.getInteger("redis.pool.maxIdle", 16));
        poolConfig.setMinIdle(config.getInteger("redis.pool.minIdle", 8));
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTime(Duration.ofSeconds(60));
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);

        String host = config.getString("redis.host", "localhost");
        int port = config.getInteger("redis.port", 6379);
        String password = config.getString("redis.password");
        int database = config.getInteger("redis.database", 0);
        int timeout = config.getInteger("redis.timeout", 2000);

        if (password != null && password.isEmpty()) {
            password = null;
        }

        this.jedisPool = new JedisPool(poolConfig, host, port, timeout, password, database);
        LOGGER.info("Redis connection pool initialized: {}:{}/{}", host, port, database);
    }

    /**
     * Sets a key-value pair in Redis.
     *
     * @param key The key
     * @param value The value
     */
    public void set(String key, String value) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(key, value);
            sample.stop(meterRegistry.timer("redis.operation", "operation", "set", "result", "success"));
        } catch (Exception e) {
            sample.stop(meterRegistry.timer("redis.operation", "operation", "set", "result", "failure"));
            LOGGER.error("Redis set operation failed for key {}: {}", key, e.getMessage());
            throw e;
        }
    }

    /**
     * Gets a value from Redis by key.
     *
     * @param key The key
     * @return The value, or null if not found
     */
    public String get(String key) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(key);
            sample.stop(meterRegistry.timer("redis.operation", "operation", "get", "result", "success"));
            return value;
        } catch (Exception e) {
            sample.stop(meterRegistry.timer("redis.operation", "operation", "get", "result", "failure"));
            LOGGER.error("Redis get operation failed for key {}: {}", key, e.getMessage());
            throw e;
        }
    }

    /**
     * Sets an expiration time on a key.
     *
     * @param key The key
     * @param seconds Time to live in seconds
     */
    public void expire(String key, int seconds) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.expire(key, seconds);
            sample.stop(meterRegistry.timer("redis.operation", "operation", "expire", "result", "success"));
        } catch (Exception e) {
            sample.stop(meterRegistry.timer("redis.operation", "operation", "expire", "result", "failure"));
            LOGGER.error("Redis expire operation failed for key {}: {}", key, e.getMessage());
            throw e;
        }
    }

    /**
     * Deletes a key from Redis.
     *
     * @param key The key to delete
     */
    public void delete(String key) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key);
            sample.stop(meterRegistry.timer("redis.operation", "operation", "delete", "result", "success"));
        } catch (Exception e) {
            sample.stop(meterRegistry.timer("redis.operation", "operation", "delete", "result", "failure"));
            LOGGER.error("Redis delete operation failed for key {}: {}", key, e.getMessage());
            throw e;
        }
    }

    /**
     * Checks if Redis is available by pinging the server.
     *
     * @return true if Redis is available, false otherwise
     */
    public boolean isAvailable() {
        try (Jedis jedis = jedisPool.getResource()) {
            return "PONG".equals(jedis.ping());
        } catch (Exception e) {
            LOGGER.warn("Redis ping failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Closes the Redis connection pool.
     */
    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
            LOGGER.info("Redis connection pool closed");
        }
    }
}