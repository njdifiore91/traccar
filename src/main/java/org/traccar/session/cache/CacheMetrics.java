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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Collects and exposes cache performance metrics for monitoring and optimization.
 * This class implements counters and gauges for hit rates, miss rates, eviction counts,
 * and latency measurements for both local and distributed cache operations.
 */
@Singleton
public class CacheMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheMetrics.class);

    private final MeterRegistry meterRegistry;
    private final Config config;
    private final CacheManager cacheManager;

    // Cache operation counters
    private final Counter localCacheHits;
    private final Counter localCacheMisses;
    private final Counter redisCacheHits;
    private final Counter redisCacheMisses;
    private final Counter cacheEvictions;
    private final Counter cacheInvalidations;
    private final Counter distributedSyncEvents;
    private final Counter distributedSyncErrors;

    // Cache operation timers
    private final Timer localCacheGetTimer;
    private final Timer localCachePutTimer;
    private final Timer redisCacheGetTimer;
    private final Timer redisCachePutTimer;
    private final Timer cacheInvalidationTimer;
    private final Timer cacheSynchronizationTimer;

    // Cache size gauges
    private final Map<String, Integer> cacheSizes = new ConcurrentHashMap<>();

    /**
     * Constructs a new CacheMetrics instance.
     *
     * @param meterRegistry The Micrometer registry for registering metrics
     * @param config        The application configuration
     * @param cacheManager The cache manager instance for size measurements
     */
    @Inject
    public CacheMetrics(MeterRegistry meterRegistry, Config config, CacheManager cacheManager) {
        this.meterRegistry = meterRegistry;
        this.config = config;
        this.cacheManager = cacheManager;

        // Initialize counters
        localCacheHits = createCounter("cache.hits", "type", "local", "Cache hits in the local cache");
        localCacheMisses = createCounter("cache.misses", "type", "local", "Cache misses in the local cache");
        redisCacheHits = createCounter("cache.hits", "type", "redis", "Cache hits in the Redis distributed cache");
        redisCacheMisses = createCounter("cache.misses", "type", "redis", "Cache misses in the Redis distributed cache");
        cacheEvictions = createCounter("cache.evictions", null, null, "Number of entries evicted from cache");
        cacheInvalidations = createCounter("cache.invalidations", null, null, "Number of cache invalidation operations");
        distributedSyncEvents = createCounter("cache.sync.events", null, null, "Number of distributed cache synchronization events");
        distributedSyncErrors = createCounter("cache.sync.errors", null, null, "Number of errors during distributed cache synchronization");

        // Initialize timers
        localCacheGetTimer = createTimer("cache.get.time", "type", "local", "Time taken for local cache get operations");
        localCachePutTimer = createTimer("cache.put.time", "type", "local", "Time taken for local cache put operations");
        redisCacheGetTimer = createTimer("cache.get.time", "type", "redis", "Time taken for Redis cache get operations");
        redisCachePutTimer = createTimer("cache.put.time", "type", "redis", "Time taken for Redis cache put operations");
        cacheInvalidationTimer = createTimer("cache.invalidation.time", null, null, "Time taken for cache invalidation operations");
        cacheSynchronizationTimer = createTimer("cache.sync.time", null, null, "Time taken for distributed cache synchronization");

        // Initialize gauges
        registerCacheSizeGauge("local", () -> getCacheSize("local"));
        registerCacheSizeGauge("redis", () -> getCacheSize("redis"));
        registerMemoryUsageGauge();
        registerHitRatioGauge("local");
        registerHitRatioGauge("redis");

        LOGGER.info("Cache metrics initialized");
    }

    /**
     * Records a cache hit in the specified cache type.
     *
     * @param cacheType The type of cache ("local" or "redis")
     */
    public void recordCacheHit(String cacheType) {
        if ("local".equals(cacheType)) {
            localCacheHits.increment();
        } else if ("redis".equals(cacheType)) {
            redisCacheHits.increment();
        }
    }

    /**
     * Records a cache miss in the specified cache type.
     *
     * @param cacheType The type of cache ("local" or "redis")
     */
    public void recordCacheMiss(String cacheType) {
        if ("local".equals(cacheType)) {
            localCacheMisses.increment();
        } else if ("redis".equals(cacheType)) {
            redisCacheMisses.increment();
        }
    }

    /**
     * Records a cache eviction event.
     */
    public void recordEviction() {
        cacheEvictions.increment();
    }

    /**
     * Records a cache invalidation event.
     */
    public void recordInvalidation() {
        cacheInvalidations.increment();
    }

    /**
     * Records a distributed cache synchronization event.
     *
     * @param success Whether the synchronization was successful
     */
    public void recordSyncEvent(boolean success) {
        distributedSyncEvents.increment();
        if (!success) {
            distributedSyncErrors.increment();
        }
    }

    /**
     * Records the time taken for a local cache get operation.
     *
     * @param timeNanos The time taken in nanoseconds
     */
    public void recordLocalCacheGetTime(long timeNanos) {
        localCacheGetTimer.record(timeNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Records the time taken for a local cache put operation.
     *
     * @param timeNanos The time taken in nanoseconds
     */
    public void recordLocalCachePutTime(long timeNanos) {
        localCachePutTimer.record(timeNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Records the time taken for a Redis cache get operation.
     *
     * @param timeNanos The time taken in nanoseconds
     */
    public void recordRedisCacheGetTime(long timeNanos) {
        redisCacheGetTimer.record(timeNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Records the time taken for a Redis cache put operation.
     *
     * @param timeNanos The time taken in nanoseconds
     */
    public void recordRedisCachePutTime(long timeNanos) {
        redisCachePutTimer.record(timeNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Records the time taken for a cache invalidation operation.
     *
     * @param timeNanos The time taken in nanoseconds
     */
    public void recordInvalidationTime(long timeNanos) {
        cacheInvalidationTimer.record(timeNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Records the time taken for a distributed cache synchronization operation.
     *
     * @param timeNanos The time taken in nanoseconds
     */
    public void recordSyncTime(long timeNanos) {
        cacheSynchronizationTimer.record(timeNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Updates the size of the specified cache type.
     *
     * @param cacheType The type of cache ("local" or "redis")
     * @param size      The current size of the cache
     */
    public void updateCacheSize(String cacheType, int size) {
        cacheSizes.put(cacheType, size);
    }

    /**
     * Creates a counter with the specified name and tags.
     *
     * @param name        The name of the counter
     * @param tagKey      The key for the tag (can be null)
     * @param tagValue    The value for the tag (can be null)
     * @param description The description of the counter
     * @return The created counter
     */
    private Counter createCounter(String name, String tagKey, String tagValue, String description) {
        Tags tags = Tags.empty();
        if (tagKey != null && tagValue != null) {
            tags = Tags.of(tagKey, tagValue);
        }
        return Counter.builder(name)
                .tags(tags)
                .description(description)
                .register(meterRegistry);
    }

    /**
     * Creates a timer with the specified name and tags.
     *
     * @param name        The name of the timer
     * @param tagKey      The key for the tag (can be null)
     * @param tagValue    The value for the tag (can be null)
     * @param description The description of the timer
     * @return The created timer
     */
    private Timer createTimer(String name, String tagKey, String tagValue, String description) {
        Tags tags = Tags.empty();
        if (tagKey != null && tagValue != null) {
            tags = Tags.of(tagKey, tagValue);
        }
        return Timer.builder(name)
                .tags(tags)
                .description(description)
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(meterRegistry);
    }

    /**
     * Registers a gauge for measuring cache size.
     *
     * @param cacheType The type of cache ("local" or "redis")
     * @param supplier  The supplier function that provides the current cache size
     */
    private void registerCacheSizeGauge(String cacheType, Supplier<Number> supplier) {
        Gauge.builder("cache.size", supplier)
                .tags("type", cacheType)
                .description("Current number of entries in the " + cacheType + " cache")
                .register(meterRegistry);
    }

    /**
     * Registers gauges for measuring cache memory usage.
     */
    private void registerMemoryUsageGauge() {
        // Register memory usage gauge for local cache
        Gauge.builder("cache.memory.used", () -> getMemoryUsage("local"))
                .tags("type", "local")
                .description("Estimated memory usage of the local cache in bytes")
                .register(meterRegistry);

        // Register memory usage gauge for Redis cache
        Gauge.builder("cache.memory.used", () -> getMemoryUsage("redis"))
                .tags("type", "redis")
                .description("Estimated memory usage of the Redis cache in bytes")
                .register(meterRegistry);

        // Register memory usage percentage gauge
        Gauge.builder("cache.memory.usage_percent", () -> getMemoryUsagePercent())
                .description("Percentage of allocated memory used by the cache")
                .register(meterRegistry);
    }

    /**
     * Registers a gauge for measuring cache hit ratio.
     *
     * @param cacheType The type of cache ("local" or "redis")
     */
    private void registerHitRatioGauge(String cacheType) {
        Gauge.builder("cache.hit_ratio", () -> calculateHitRatio(cacheType))
                .tags("type", cacheType)
                .description("Ratio of cache hits to total cache accesses for " + cacheType + " cache")
                .register(meterRegistry);
    }

    /**
     * Gets the current size of the specified cache type.
     *
     * @param cacheType The type of cache ("local" or "redis")
     * @return The current size of the cache
     */
    private int getCacheSize(String cacheType) {
        return cacheSizes.getOrDefault(cacheType, 0);
    }

    /**
     * Gets the estimated memory usage of the specified cache type.
     *
     * @param cacheType The type of cache ("local" or "redis")
     * @return The estimated memory usage in bytes
     */
    private long getMemoryUsage(String cacheType) {
        // For local cache, estimate based on cache size and average entry size
        if ("local".equals(cacheType)) {
            int size = getCacheSize("local");
            int avgEntrySize = config.getInteger("cache.local.avgEntrySize", 1024); // Default 1KB per entry
            return (long) size * avgEntrySize;
        }
        // For Redis, this would ideally come from Redis INFO command
        // For now, return a placeholder value
        return 0L;
    }

    /**
     * Gets the percentage of allocated memory used by the cache.
     *
     * @return The memory usage percentage
     */
    private double getMemoryUsagePercent() {
        long maxMemory = config.getLong("cache.maxMemory", Runtime.getRuntime().maxMemory());
        if (maxMemory <= 0) {
            return 0.0;
        }
        long usedMemory = getMemoryUsage("local") + getMemoryUsage("redis");
        return (double) usedMemory / maxMemory * 100.0;
    }

    /**
     * Calculates the hit ratio for the specified cache type.
     *
     * @param cacheType The type of cache ("local" or "redis")
     * @return The hit ratio (between 0.0 and 1.0)
     */
    private double calculateHitRatio(String cacheType) {
        double hits;
        double misses;

        if ("local".equals(cacheType)) {
            hits = localCacheHits.count();
            misses = localCacheMisses.count();
        } else if ("redis".equals(cacheType)) {
            hits = redisCacheHits.count();
            misses = redisCacheMisses.count();
        } else {
            return 0.0;
        }

        double total = hits + misses;
        if (total <= 0) {
            return 0.0;
        }
        return hits / total;
    }

    /**
     * Records a cache operation with timing.
     *
     * @param cacheType The type of cache ("local" or "redis")
     * @param operation The operation being performed ("get" or "put")
     * @param success   Whether the operation was successful (hit or miss for get operations)
     * @param timeNanos The time taken in nanoseconds
     */
    public void recordCacheOperation(String cacheType, String operation, boolean success, long timeNanos) {
        if ("local".equals(cacheType)) {
            if ("get".equals(operation)) {
                if (success) {
                    localCacheHits.increment();
                } else {
                    localCacheMisses.increment();
                }
                localCacheGetTimer.record(timeNanos, TimeUnit.NANOSECONDS);
            } else if ("put".equals(operation)) {
                localCachePutTimer.record(timeNanos, TimeUnit.NANOSECONDS);
            }
        } else if ("redis".equals(cacheType)) {
            if ("get".equals(operation)) {
                if (success) {
                    redisCacheHits.increment();
                } else {
                    redisCacheMisses.increment();
                }
                redisCacheGetTimer.record(timeNanos, TimeUnit.NANOSECONDS);
            } else if ("put".equals(operation)) {
                redisCachePutTimer.record(timeNanos, TimeUnit.NANOSECONDS);
            }
        }
    }

    /**
     * Records a distributed cache synchronization operation with timing and success status.
     *
     * @param success   Whether the synchronization was successful
     * @param timeNanos The time taken in nanoseconds
     */
    public void recordSynchronization(boolean success, long timeNanos) {
        distributedSyncEvents.increment();
        if (!success) {
            distributedSyncErrors.increment();
        }
        cacheSynchronizationTimer.record(timeNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Records a cache invalidation operation with timing.
     *
     * @param timeNanos The time taken in nanoseconds
     */
    public void recordInvalidationOperation(long timeNanos) {
        cacheInvalidations.increment();
        cacheInvalidationTimer.record(timeNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Updates all cache metrics based on the current state of the cache manager.
     * This method should be called periodically to update gauges that don't
     * automatically track values.
     */
    public void updateMetrics() {
        try {
            // Update local cache size based on CacheManager's graph size
            int localSize = estimateLocalCacheSize();
            updateCacheSize("local", localSize);

            // Update Redis cache size if Redis is configured
            if (isRedisConfigured()) {
                int redisSize = estimateRedisCacheSize();
                updateCacheSize("redis", redisSize);
            }

            LOGGER.debug("Updated cache metrics: local={}, redis={}", 
                    getCacheSize("local"), getCacheSize("redis"));
        } catch (Exception e) {
            LOGGER.warn("Error updating cache metrics", e);
        }
    }

    /**
     * Estimates the size of the local cache based on the CacheManager's state.
     *
     * @return The estimated size of the local cache
     */
    private int estimateLocalCacheSize() {
        // This is a simplified estimation - in a real implementation,
        // we would get the actual size from the CacheManager's graph
        return 0; // Placeholder - implement actual size calculation
    }

    /**
     * Estimates the size of the Redis cache.
     *
     * @return The estimated size of the Redis cache
     */
    private int estimateRedisCacheSize() {
        // This would typically query Redis for its size
        // For now, return a placeholder value
        return 0;
    }

    /**
     * Checks if Redis is configured for distributed caching.
     *
     * @return true if Redis is configured, false otherwise
     */
    private boolean isRedisConfigured() {
        return config.getString("cache.redis.url") != null;
    }
}