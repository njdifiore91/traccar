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
package org.traccar.session.cache;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.config.ConfigKey;

import java.time.Duration;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Redis cache configuration for distributed caching across microservices.
 * This class configures Redis connection settings, serialization strategies,
 * and cache expiration policies.
 */
@Singleton
public class RedisCacheConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisCacheConfig.class);

    private final Config config;
    private JedisPool jedisPool;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService executorService;
    private JedisPubSub pubSub;

    /**
     * Default cache expiration time in seconds
     */
    private static final int DEFAULT_EXPIRATION = 3600; // 1 hour

    /**
     * Default maximum idle connections in the pool
     */
    private static final int DEFAULT_MAX_IDLE = 10;

    /**
     * Default minimum idle connections in the pool
     */
    private static final int DEFAULT_MIN_IDLE = 2;

    /**
     * Default maximum total connections in the pool
     */
    private static final int DEFAULT_MAX_TOTAL = 20;

    /**
     * Default connection timeout in milliseconds
     */
    private static final int DEFAULT_TIMEOUT = 2000;

    /**
     * Default Redis database index
     */
    private static final int DEFAULT_DATABASE = 0;

    /**
     * Initializes Redis cache configuration with application config.
     * 
     * @param config Application configuration
     */
    @Inject
    public RedisCacheConfig(Config config) {
        this.config = config;
        this.objectMapper = createObjectMapper();
        this.executorService = Executors.newScheduledThreadPool(1);
        initializeJedisPool();
        initializeCacheInvalidation();
    }

    /**
     * Initializes the Jedis connection pool with configuration from application settings.
     * Falls back to default values if specific settings are not provided.
     */
    /**
     * Creates and configures the ObjectMapper for JSON serialization/deserialization.
     * 
     * @return Configured ObjectMapper instance
     */
    private ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        return mapper;
    }

    /**
     * Initializes the Jedis connection pool with configuration from application settings.
     * Falls back to default values if specific settings are not provided.
     */
    private void initializeJedisPool() {
        try {
            String host = config.getString("redis.host", Protocol.DEFAULT_HOST);
            int port = config.getInteger("redis.port", Protocol.DEFAULT_PORT);
            String password = config.getString("redis.password");
            int database = config.getInteger("redis.database", DEFAULT_DATABASE);
            int timeout = config.getInteger("redis.timeout", DEFAULT_TIMEOUT);

            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(config.getInteger("redis.pool.maxTotal", DEFAULT_MAX_TOTAL));
            poolConfig.setMaxIdle(config.getInteger("redis.pool.maxIdle", DEFAULT_MAX_IDLE));
            poolConfig.setMinIdle(config.getInteger("redis.pool.minIdle", DEFAULT_MIN_IDLE));
            poolConfig.setTestOnBorrow(true);
            poolConfig.setTestOnReturn(true);
            poolConfig.setTestWhileIdle(true);
            poolConfig.setNumTestsPerEvictionRun(3);
            poolConfig.setBlockWhenExhausted(true);

            if (password != null && !password.isEmpty()) {
                jedisPool = new JedisPool(poolConfig, host, port, timeout, password, database);
            } else {
                jedisPool = new JedisPool(poolConfig, host, port, timeout, null, database);
            }

            LOGGER.info("Redis cache initialized with host: {}, port: {}, database: {}", host, port, database);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize Redis cache", e);
            throw new RuntimeException("Redis cache initialization failed", e);
        }
    }
    
    /**
     * Initializes the Redis Pub/Sub mechanism for cache invalidation notifications.
     */
    private void initializeCacheInvalidation() {
        String channel = config.getString("redis.invalidationChannel", "traccar:cache:invalidation");
        
        pubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                LOGGER.debug("Received cache invalidation message: {}", message);
                // Cache invalidation logic will be handled by subscribers
            }
        };
        
        // Start the subscription in a separate thread to avoid blocking
        executorService.execute(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                LOGGER.info("Subscribing to cache invalidation channel: {}", channel);
                jedis.subscribe(pubSub, channel);
            } catch (Exception e) {
                LOGGER.error("Failed to subscribe to cache invalidation channel", e);
            }
        });
        
        // Schedule periodic connection check
        executorService.scheduleAtFixedRate(this::checkConnection, 30, 30, TimeUnit.SECONDS);
    }
    
    /**
     * Checks the Redis connection and reconnects if necessary.
     */
    private void checkConnection() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.ping();
        } catch (Exception e) {
            LOGGER.warn("Redis connection check failed, attempting to reconnect", e);
            try {
                if (pubSub != null) {
                    pubSub.unsubscribe();
                }
                if (jedisPool != null && !jedisPool.isClosed()) {
                    jedisPool.close();
                }
                initializeJedisPool();
                initializeCacheInvalidation();
            } catch (Exception reconnectError) {
                LOGGER.error("Failed to reconnect to Redis", reconnectError);
            }
        }
    }

    /**
     * Gets the Jedis connection pool.
     * 
     * @return JedisPool instance
     */
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    /**
     * Gets the default cache expiration time in seconds.
     * 
     * @return Default expiration time
     */
    public int getDefaultExpiration() {
        return config.getInteger("redis.expiration", DEFAULT_EXPIRATION);
    }

    /**
     * Gets the cache key prefix to avoid key collisions in shared Redis instances.
     * 
     * @return Cache key prefix
     */
    public String getKeyPrefix() {
        return config.getString("redis.keyPrefix", "traccar:");
    }

    /**
     * Gets the Redis Pub/Sub channel name for cache invalidation notifications.
     * 
     * @return Cache invalidation channel name
     */
    public String getInvalidationChannel() {
        return config.getString("redis.invalidationChannel", "traccar:cache:invalidation");
    }
    
    /**
     * Serializes an object to JSON string.
     * 
     * @param object Object to serialize
     * @return JSON string representation
     * @throws IOException If serialization fails
     */
    public String serialize(Object object) throws IOException {
        return objectMapper.writeValueAsString(object);
    }
    
    /**
     * Deserializes a JSON string to an object of the specified class.
     * 
     * @param <T> Type of the object
     * @param json JSON string to deserialize
     * @param clazz Class of the object
     * @return Deserialized object
     * @throws IOException If deserialization fails
     */
    public <T> T deserialize(String json, Class<T> clazz) throws IOException {
        return objectMapper.readValue(json, clazz);
    }
    
    /**
     * Publishes a cache invalidation message to notify other instances.
     * 
     * @param message Invalidation message
     */
    public void publishInvalidationMessage(String message) {
        try (Jedis jedis = jedisPool.getResource()) {
            String channel = getInvalidationChannel();
            jedis.publish(channel, message);
            LOGGER.debug("Published cache invalidation message: {}", message);
        } catch (Exception e) {
            LOGGER.error("Failed to publish cache invalidation message", e);
        }
    }
    
    /**
     * Stores a value in the Redis cache with the default expiration time.
     * 
     * @param key Cache key
     * @param value Value to store
     * @throws IOException If serialization fails
     */
    public void put(String key, Object value) throws IOException {
        put(key, value, getDefaultExpiration());
    }
    
    /**
     * Stores a value in the Redis cache with a specific expiration time.
     * 
     * @param key Cache key
     * @param value Value to store
     * @param expiration Expiration time in seconds
     * @throws IOException If serialization fails
     */
    public void put(String key, Object value, int expiration) throws IOException {
        String prefixedKey = getKeyPrefix() + key;
        String serializedValue = serialize(value);
        
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(prefixedKey, expiration, serializedValue);
            LOGGER.debug("Stored value in Redis cache with key: {}", prefixedKey);
        } catch (Exception e) {
            LOGGER.error("Failed to store value in Redis cache", e);
            throw e;
        }
    }
    
    /**
     * Retrieves a value from the Redis cache.
     * 
     * @param <T> Type of the value
     * @param key Cache key
     * @param clazz Class of the value
     * @return Retrieved value or null if not found
     * @throws IOException If deserialization fails
     */
    public <T> T get(String key, Class<T> clazz) throws IOException {
        String prefixedKey = getKeyPrefix() + key;
        
        try (Jedis jedis = jedisPool.getResource()) {
            String serializedValue = jedis.get(prefixedKey);
            if (serializedValue == null) {
                return null;
            }
            return deserialize(serializedValue, clazz);
        } catch (Exception e) {
            LOGGER.error("Failed to retrieve value from Redis cache", e);
            throw e;
        }
    }
    
    /**
     * Removes a value from the Redis cache.
     * 
     * @param key Cache key
     */
    public void remove(String key) {
        String prefixedKey = getKeyPrefix() + key;
        
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(prefixedKey);
            LOGGER.debug("Removed value from Redis cache with key: {}", prefixedKey);
        } catch (Exception e) {
            LOGGER.error("Failed to remove value from Redis cache", e);
        }
    }
    
    /**
     * Checks if a key exists in the Redis cache.
     * 
     * @param key Cache key
     * @return true if the key exists, false otherwise
     */
    public boolean exists(String key) {
        String prefixedKey = getKeyPrefix() + key;
        
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(prefixedKey);
        } catch (Exception e) {
            LOGGER.error("Failed to check key existence in Redis cache", e);
            return false;
        }
    }

    /**
     * Closes the Jedis connection pool and executor service when the application shuts down.
     */
    public void close() {
        try {
            if (pubSub != null) {
                pubSub.unsubscribe();
            }
            
            if (executorService != null && !executorService.isShutdown()) {
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
            
            if (jedisPool != null && !jedisPool.isClosed()) {
                jedisPool.close();
                LOGGER.info("Redis connection pool closed");
            }
        } catch (Exception e) {
            LOGGER.error("Error while closing Redis resources", e);
        }
    }
}