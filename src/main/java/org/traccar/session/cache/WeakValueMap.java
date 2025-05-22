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

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

/**
 * A cache implementation that combines local weak references with a distributed Redis cache.
 * Provides automatic synchronization across service instances and metrics collection.
 */
public class WeakValueMap<K, V> {

    private final Map<K, WeakReference<V>> localMap = new HashMap<>();
    private final RedisTemplate<K, V> redisTemplate;
    private final String cachePrefix;
    private final Duration ttl;
    private final RedisMessageListenerContainer listenerContainer;
    private final String invalidationChannel;
    
    // Metrics
    private final Counter localHits;
    private final Counter localMisses;
    private final Counter redisHits;
    private final Counter redisMisses;
    private final Counter puts;
    private final Counter removes;
    private final Timer getTimer;
    private final Timer putTimer;
    private final Timer removeTimer;

    /**
     * Creates a new WeakValueMap with default TTL of 1 hour.
     * 
     * @param redisTemplate Redis template for distributed cache operations
     * @param cachePrefix Prefix for Redis keys to avoid collisions
     * @param meterRegistry Registry for metrics collection
     */
    public WeakValueMap(RedisTemplate<K, V> redisTemplate, String cachePrefix, MeterRegistry meterRegistry) {
        this(redisTemplate, cachePrefix, Duration.ofHours(1), meterRegistry);
    }

    /**
     * Creates a new WeakValueMap with specified TTL.
     * 
     * @param redisTemplate Redis template for distributed cache operations
     * @param cachePrefix Prefix for Redis keys to avoid collisions
     * @param ttl Time-to-live for cached entries
     * @param meterRegistry Registry for metrics collection
     */
    public WeakValueMap(RedisTemplate<K, V> redisTemplate, String cachePrefix, Duration ttl, MeterRegistry meterRegistry) {
        this.redisTemplate = redisTemplate;
        this.cachePrefix = cachePrefix;
        this.ttl = ttl;
        this.invalidationChannel = cachePrefix + "-invalidation";
        
        // Setup cache invalidation listener
        MessageListenerAdapter messageListener = new MessageListenerAdapter(new CacheInvalidationListener());
        this.listenerContainer = new RedisMessageListenerContainer();
        this.listenerContainer.setConnectionFactory(redisTemplate.getConnectionFactory());
        this.listenerContainer.addMessageListener(messageListener, new ChannelTopic(invalidationChannel));
        this.listenerContainer.afterPropertiesSet();
        this.listenerContainer.start();
        
        // Initialize metrics
        String metricsPrefix = "cache." + cachePrefix + ".";
        this.localHits = meterRegistry.counter(metricsPrefix + "local.hits");
        this.localMisses = meterRegistry.counter(metricsPrefix + "local.misses");
        this.redisHits = meterRegistry.counter(metricsPrefix + "redis.hits");
        this.redisMisses = meterRegistry.counter(metricsPrefix + "redis.misses");
        this.puts = meterRegistry.counter(metricsPrefix + "puts");
        this.removes = meterRegistry.counter(metricsPrefix + "removes");
        this.getTimer = meterRegistry.timer(metricsPrefix + "get.time");
        this.putTimer = meterRegistry.timer(metricsPrefix + "put.time");
        this.removeTimer = meterRegistry.timer(metricsPrefix + "remove.time");
    }

    /**
     * Stores a value in both local and distributed cache.
     * 
     * @param key The key to store the value under
     * @param value The value to store
     */
    public void put(K key, V value) {
        putTimer.record(() -> {
            // Store in local weak reference map
            localMap.put(key, new WeakReference<>(value));
            
            // Store in Redis with TTL
            String redisKey = getRedisKey(key);
            redisTemplate.opsForValue().set(redisKey, value, ttl);
            
            // Track metrics
            puts.increment();
        });
    }

    /**
     * Retrieves a value from the cache, checking local cache first then Redis.
     * 
     * @param key The key to retrieve
     * @return The value, or null if not found
     */
    public V get(K key) {
        return getTimer.record(() -> {
            // Try local cache first
            WeakReference<V> weakReference = localMap.get(key);
            V value = (weakReference != null) ? weakReference.get() : null;
            
            if (value != null) {
                // Local cache hit
                localHits.increment();
                return value;
            }
            
            // Local cache miss, try Redis
            localMisses.increment();
            String redisKey = getRedisKey(key);
            value = redisTemplate.opsForValue().get(redisKey);
            
            if (value != null) {
                // Redis cache hit, update local cache
                redisHits.increment();
                localMap.put(key, new WeakReference<>(value));
                // Reset TTL on access to implement time-to-idle behavior
                redisTemplate.expire(redisKey, ttl);
                return value;
            }
            
            // Redis cache miss
            redisMisses.increment();
            return null;
        });
    }

    /**
     * Removes a value from both local and distributed cache.
     * 
     * @param key The key to remove
     * @return The removed value, or null if not found
     */
    public V remove(K key) {
        return removeTimer.record(() -> {
            // Remove from local cache
            WeakReference<V> weakReference = localMap.remove(key);
            V value = (weakReference != null) ? weakReference.get() : null;
            
            // Remove from Redis and notify other instances
            String redisKey = getRedisKey(key);
            redisTemplate.delete(redisKey);
            redisTemplate.convertAndSend(invalidationChannel, key.toString());
            
            // Track metrics
            removes.increment();
            
            return value;
        });
    }

    /**
     * Cleans up expired weak references from the local cache.
     * This method is optimized to run periodically without affecting performance.
     */
    public void clean() {
        // Clean local map of null references
        localMap.entrySet().removeIf(entry -> entry.getValue().get() == null);
        
        // No need to clean Redis as TTL handles expiration automatically
    }
    
    /**
     * Generates a Redis key with the cache prefix to avoid collisions.
     * 
     * @param key The original key
     * @return The prefixed Redis key
     */
    private String getRedisKey(K key) {
        return cachePrefix + ":" + key.toString();
    }
    
    /**
     * Listener for cache invalidation events from other service instances.
     */
    private class CacheInvalidationListener {
        
        /**
         * Handles invalidation messages from other service instances.
         * 
         * @param message The key to invalidate as a string
         */
        @SuppressWarnings("unchecked")
        public void handleMessage(String message) {
            try {
                // Remove the invalidated key from local cache only
                // This assumes the key can be parsed from the message
                K key = (K) message;
                localMap.remove(key);
            } catch (Exception e) {
                // Log error but don't propagate
                System.err.println("Error handling cache invalidation: " + e.getMessage());
            }
        }
    }
    
    /**
     * Destroys the cache and releases resources.
     * Should be called when the cache is no longer needed.
     */
    public void destroy() {
        if (listenerContainer != null) {
            listenerContainer.stop();
        }
    }
}