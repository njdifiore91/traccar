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
package org.traccar.session;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Redis implementation of the DistributedSessionStore interface.
 * Uses Redis for storing and retrieving device sessions across multiple instances.
 */
@Singleton
public class RedisSessionStore implements DistributedSessionStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSessionStore.class);
    private static final String KEY_PREFIX = "session:";
    private static final int DEFAULT_RETRY_ATTEMPTS = 3;
    private static final int DEFAULT_RETRY_DELAY_MS = 200;

    private final ObjectMapper objectMapper;
    private final boolean clusterMode;
    private final int maxRetryAttempts;
    private final int retryDelayMs;
    private final long defaultTtlSeconds;

    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> connection;
    private RedisCommands<String, String> syncCommands;

    private RedisClusterClient redisClusterClient;
    private StatefulRedisClusterConnection<String, String> clusterConnection;
    private RedisAdvancedClusterCommands<String, String> clusterCommands;

    /**
     * Constructs a new RedisSessionStore with the provided configuration.
     *
     * @param config The application configuration
     */
    @Inject
    public RedisSessionStore(Config config) {
        this.objectMapper = createObjectMapper();
        this.clusterMode = config.getBoolean(Keys.REDIS_CLUSTER_MODE);
        this.maxRetryAttempts = config.getInteger(Keys.REDIS_RETRY_ATTEMPTS, DEFAULT_RETRY_ATTEMPTS);
        this.retryDelayMs = config.getInteger(Keys.REDIS_RETRY_DELAY, DEFAULT_RETRY_DELAY_MS);
        this.defaultTtlSeconds = config.getLong(Keys.STATUS_TIMEOUT);

        initializeRedisConnection(config);
    }

    private ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        return mapper;
    }

    private void initializeRedisConnection(Config config) {
        try {
            String redisUrl = config.getString(Keys.REDIS_URL, "redis://localhost:6379");
            int connectionTimeout = config.getInteger(Keys.REDIS_CONNECTION_TIMEOUT, 5000);
            int operationTimeout = config.getInteger(Keys.REDIS_OPERATION_TIMEOUT, 3000);

            RedisURI redisUri = RedisURI.create(redisUrl);
            redisUri.setTimeout(Duration.ofMillis(connectionTimeout));

            TimeoutOptions timeoutOptions = TimeoutOptions.builder()
                    .timeoutCommands(true)
                    .fixedTimeout(Duration.ofMillis(operationTimeout))
                    .build();

            if (clusterMode) {
                initializeClusterConnection(redisUri, timeoutOptions);
            } else {
                initializeStandaloneConnection(redisUri, timeoutOptions);
            }

            LOGGER.info("Redis session store initialized successfully");
        } catch (Exception e) {
            LOGGER.error("Failed to initialize Redis connection", e);
            throw new RuntimeException("Failed to initialize Redis connection", e);
        }
    }

    private void initializeStandaloneConnection(RedisURI redisUri, TimeoutOptions timeoutOptions) {
        redisClient = RedisClient.create(redisUri);
        redisClient.setOptions(ClientOptions.builder()
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .autoReconnect(true)
                .timeoutOptions(timeoutOptions)
                .build());

        connection = redisClient.connect();
        syncCommands = connection.sync();
    }

    private void initializeClusterConnection(RedisURI redisUri, TimeoutOptions timeoutOptions) {
        redisClusterClient = RedisClusterClient.create(redisUri);
        redisClusterClient.setOptions(ClusterClientOptions.builder()
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .autoReconnect(true)
                .timeoutOptions(timeoutOptions)
                .build());

        clusterConnection = redisClusterClient.connect();
        clusterCommands = clusterConnection.sync();
    }

    @Override
    public void putSession(long deviceId, DeviceSession session, long ttlSeconds) {
        String key = KEY_PREFIX + deviceId;
        try {
            String serializedSession = objectMapper.writeValueAsString(session);
            executeWithRetry(() -> {
                if (clusterMode) {
                    clusterCommands.setex(key, ttlSeconds, serializedSession);
                } else {
                    syncCommands.setex(key, ttlSeconds, serializedSession);
                }
                return null;
            });
        } catch (Exception e) {
            LOGGER.error("Failed to store session for device {}", deviceId, e);
        }
    }

    @Override
    public DeviceSession getSession(long deviceId) {
        String key = KEY_PREFIX + deviceId;
        try {
            String serializedSession = executeWithRetry(() -> {
                if (clusterMode) {
                    return clusterCommands.get(key);
                } else {
                    return syncCommands.get(key);
                }
            });

            if (serializedSession != null) {
                return objectMapper.readValue(serializedSession, DeviceSession.class);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to retrieve session for device {}", deviceId, e);
        }
        return null;
    }

    @Override
    public void removeSession(long deviceId) {
        String key = KEY_PREFIX + deviceId;
        try {
            executeWithRetry(() -> {
                if (clusterMode) {
                    clusterCommands.del(key);
                } else {
                    syncCommands.del(key);
                }
                return null;
            });
        } catch (Exception e) {
            LOGGER.error("Failed to remove session for device {}", deviceId, e);
        }
    }

    @Override
    public boolean containsSession(long deviceId) {
        String key = KEY_PREFIX + deviceId;
        try {
            return executeWithRetry(() -> {
                if (clusterMode) {
                    return clusterCommands.exists(key) > 0;
                } else {
                    return syncCommands.exists(key) > 0;
                }
            });
        } catch (Exception e) {
            LOGGER.error("Failed to check session existence for device {}", deviceId, e);
            return false;
        }
    }

    @Override
    public boolean updateSessionExpiration(long deviceId, long ttlSeconds) {
        String key = KEY_PREFIX + deviceId;
        try {
            return executeWithRetry(() -> {
                if (clusterMode) {
                    return clusterCommands.expire(key, ttlSeconds);
                } else {
                    return syncCommands.expire(key, ttlSeconds);
                }
            });
        } catch (Exception e) {
            LOGGER.error("Failed to update expiration for device {}", deviceId, e);
            return false;
        }
    }

    @Override
    public void close() {
        try {
            if (connection != null) {
                connection.close();
            }
            if (redisClient != null) {
                redisClient.shutdown();
            }
            if (clusterConnection != null) {
                clusterConnection.close();
            }
            if (redisClusterClient != null) {
                redisClusterClient.shutdown();
            }
        } catch (Exception e) {
            LOGGER.error("Error closing Redis connections", e);
        }
    }

    /**
     * Execute a Redis operation with retry logic.
     *
     * @param operation The operation to execute
     * @param <T> The return type of the operation
     * @return The result of the operation
     * @throws Exception If the operation fails after all retry attempts
     */
    private <T> T executeWithRetry(RedisOperation<T> operation) throws Exception {
        Exception lastException = null;
        for (int attempt = 0; attempt < maxRetryAttempts; attempt++) {
            try {
                return operation.execute();
            } catch (RedisCommandTimeoutException | RedisConnectionException e) {
                lastException = e;
                LOGGER.debug("Redis operation failed (attempt {}/{}), retrying...", attempt + 1, maxRetryAttempts, e);
                if (attempt < maxRetryAttempts - 1) {
                    TimeUnit.MILLISECONDS.sleep(retryDelayMs * (attempt + 1));
                }
            } catch (RedisException e) {
                lastException = e;
                LOGGER.error("Redis operation failed with non-retryable error", e);
                break;
            }
        }
        throw lastException != null ? lastException : new IOException("Redis operation failed");
    }

    /**
     * Functional interface for Redis operations that can be retried.
     *
     * @param <T> The return type of the operation
     */
    @FunctionalInterface
    private interface RedisOperation<T> {
        T execute() throws Exception;
    }
}