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
package org.traccar.geocoder;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;

/**
 * Redis implementation of the DistributedCacheManager interface.
 * Provides distributed caching for geocoder results using Redis.
 */
@Singleton
public class RedisCacheManager implements JsonGeocoder.DistributedCacheManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisCacheManager.class);
    private static final String CACHE_PREFIX = "geocoder:";
    private static final Duration CACHE_TTL = Duration.ofDays(30); // 30 days TTL

    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final Timer getCacheTimer;
    private final Timer putCacheTimer;

    /**
     * Creates a new RedisCacheManager with the specified Redis client and metrics registry.
     *
     * @param redisClient Redis client for connecting to the Redis server
     * @param objectMapper Jackson object mapper for serialization/deserialization
     * @param meterRegistry Registry for metrics collection
     */
    @Inject
    public RedisCacheManager(RedisClient redisClient, ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.redisClient = redisClient;
        this.connection = redisClient.connect();
        this.commands = connection.sync();
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.getCacheTimer = Timer.builder("geocoder.cache.get")
                .description("Time taken to retrieve from distributed cache")
                .register(meterRegistry);
        this.putCacheTimer = Timer.builder("geocoder.cache.put")
                .description("Time taken to store in distributed cache")
                .register(meterRegistry);
        
        LOGGER.info("Initialized Redis cache manager for geocoder");
    }

    /**
     * Retrieves an address from the Redis cache.
     *
     * @param key Cache key
     * @return Address object or null if not found
     */
    @Override
    public Address getAddress(String key) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            String cacheKey = CACHE_PREFIX + key;
            String json = commands.get(cacheKey);
            
            if (json != null) {
                meterRegistry.counter("geocoder.cache.hit").increment();
                return objectMapper.readValue(json, Address.class);
            } else {
                meterRegistry.counter("geocoder.cache.miss").increment();
                return null;
            }
        } catch (Exception e) {
            LOGGER.warn("Error retrieving address from Redis cache", e);
            meterRegistry.counter("geocoder.cache.error", "operation", "get").increment();
            return null;
        } finally {
            sample.stop(getCacheTimer);
        }
    }

    /**
     * Stores an address in the Redis cache with TTL.
     *
     * @param key Cache key
     * @param address Address to store
     */
    @Override
    public void putAddress(String key, Address address) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            String cacheKey = CACHE_PREFIX + key;
            String json = objectMapper.writeValueAsString(address);
            
            commands.setex(cacheKey, CACHE_TTL.getSeconds(), json);
            meterRegistry.counter("geocoder.cache.put").increment();
        } catch (Exception e) {
            LOGGER.warn("Error storing address in Redis cache", e);
            meterRegistry.counter("geocoder.cache.error", "operation", "put").increment();
        } finally {
            sample.stop(putCacheTimer);
        }
    }

    /**
     * Closes the Redis connection when the application shuts down.
     */
    public void close() {
        if (connection != null) {
            connection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }
}