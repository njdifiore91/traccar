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
package org.traccar.session.cache;

import com.google.inject.AbstractModule;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;

import java.time.Duration;

/**
 * Configuration module for Redis cache integration.
 * This module configures the Redis client and provides the CacheManager implementation
 * that works with a distributed Redis cache.
 */
public class RedisCacheConfig extends AbstractModule {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisCacheConfig.class);

    @Override
    protected void configure() {
        bind(CacheManager.class).toProvider(RedisCacheManagerProvider.class).in(Singleton.class);
    }

    /**
     * Provider for Redis-backed CacheManager implementation.
     */
    public static class RedisCacheManagerProvider implements Provider<CacheManager> {

        private final Config config;

        @Inject
        public RedisCacheManagerProvider(Config config) {
            this.config = config;
        }

        @Override
        public CacheManager get() {
            String redisHost = config.getString("redis.host", "localhost");
            int redisPort = config.getInteger("redis.port", 6379);
            String redisPassword = config.getString("redis.password");
            int redisDatabase = config.getInteger("redis.database", 0);
            long redisTtl = config.getLong("redis.ttl", 300); // Default TTL: 5 minutes

            LOGGER.info("Initializing Redis cache connection to {}:{} (db: {})", redisHost, redisPort, redisDatabase);

            RedisURI.Builder uriBuilder = RedisURI.builder()
                    .withHost(redisHost)
                    .withPort(redisPort)
                    .withDatabase(redisDatabase)
                    .withTimeout(Duration.ofSeconds(config.getLong("redis.timeout", 5)));

            if (redisPassword != null && !redisPassword.isEmpty()) {
                uriBuilder.withPassword(redisPassword.toCharArray());
            }

            RedisURI redisURI = uriBuilder.build();
            RedisClient redisClient = RedisClient.create(redisURI);
            StatefulRedisConnection<String, String> connection = redisClient.connect();
            RedisCommands<String, String> syncCommands = connection.sync();

            // Test connection
            try {
                String pingResponse = syncCommands.ping();
                LOGGER.info("Redis connection test: {}", pingResponse);
            } catch (Exception e) {
                LOGGER.warn("Redis connection test failed", e);
            }

            return new RedisCacheManager(connection, redisTtl);
        }
    }
}