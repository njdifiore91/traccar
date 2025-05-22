/*
 * Copyright 2019 - 2024 Anton Tananaev (anton@traccar.org)
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

import java.util.List;

public final class Keys {

    private Keys() {
    }

    /**
     * If no data is reported by a device for the given amount of time, status changes from online to unknown. Value is
     * in seconds. Default timeout is 10 minutes.
     */
    public static final ConfigKey<Long> STATUS_TIMEOUT = new LongConfigKey(
            "status.timeout",
            List.of(KeyType.CONFIG),
            600L);
            
    /**
     * Redis URL for distributed session storage.
     */
    public static final ConfigKey<String> REDIS_URL = new StringConfigKey(
            "redis.url",
            List.of(KeyType.CONFIG));
            
    /**
     * Redis cluster mode enabled.
     */
    public static final ConfigKey<Boolean> REDIS_CLUSTER_MODE = new BooleanConfigKey(
            "redis.cluster",
            List.of(KeyType.CONFIG));
            
    /**
     * Redis connection timeout in milliseconds.
     */
    public static final ConfigKey<Integer> REDIS_CONNECTION_TIMEOUT = new IntegerConfigKey(
            "redis.connectionTimeout",
            List.of(KeyType.CONFIG));
            
    /**
     * Redis operation timeout in milliseconds.
     */
    public static final ConfigKey<Integer> REDIS_OPERATION_TIMEOUT = new IntegerConfigKey(
            "redis.operationTimeout",
            List.of(KeyType.CONFIG));
            
    /**
     * Redis retry attempts for failed operations.
     */
    public static final ConfigKey<Integer> REDIS_RETRY_ATTEMPTS = new IntegerConfigKey(
            "redis.retryAttempts",
            List.of(KeyType.CONFIG));
            
    /**
     * Redis retry delay in milliseconds.
     */
    public static final ConfigKey<Integer> REDIS_RETRY_DELAY = new IntegerConfigKey(
            "redis.retryDelay",
            List.of(KeyType.CONFIG));
}