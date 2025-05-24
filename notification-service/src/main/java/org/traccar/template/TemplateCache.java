package org.traccar.template;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Provides distributed caching of notification templates to improve performance and reduce
 * filesystem or database access. Implements a two-level caching strategy with local in-memory
 * cache for frequently used templates and distributed Redis cache for sharing across service instances.
 * <p>
 * This component is critical for optimizing template processing performance in a high-volume
 * notification system, ensuring that templates are loaded only once and then cached for subsequent use.
 * <p>
 * Key features:
 * <ul>
 *   <li>Two-level caching strategy: Local Caffeine cache + distributed Redis cache</li>
 *   <li>Circuit breaker pattern for resilient Redis access using Resilience4j</li>
 *   <li>Comprehensive cache statistics for monitoring hit/miss rates</li>
 *   <li>Configurable TTL-based expiration for both cache levels</li>
 *   <li>Cache invalidation mechanisms for template updates</li>
 * </ul>
 * <p>
 * The cache is designed to gracefully handle Redis failures by falling back to the local cache,
 * preventing cascading failures in the notification service during Redis outages.
 * <p>
 * Cache metrics are exposed via Micrometer for monitoring and alerting purposes.
 */
@Component
public class TemplateCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateCache.class);
    private static final String CIRCUIT_BREAKER_NAME = "templateCacheRedis";
    private static final String REDIS_CACHE_PREFIX = "template:";

    private final RedisTemplate<String, String> redisTemplate;
    private final CircuitBreaker circuitBreaker;
    private final MeterRegistry meterRegistry;

    // Local cache using Caffeine
    private Cache<String, String> localCache;

    // Cache metrics
    private Counter localCacheHits;
    private Counter localCacheMisses;
    private Counter redisCacheHits;
    private Counter redisCacheMisses;
    private Counter redisErrors;
    private Timer cacheAccessTimer;

    @Value("${template.cache.local.maxSize:1000}")
    private int localCacheMaxSize;

    @Value("${template.cache.local.expireAfterWrite:3600}")
    private int localCacheExpireAfterWriteSeconds;

    @Value("${template.cache.redis.expireAfterWrite:86400}")
    private int redisCacheExpireAfterWriteSeconds;

    @Value("${template.cache.circuitBreaker.failureRateThreshold:50}")
    private float failureRateThreshold;

    @Value("${template.cache.circuitBreaker.waitDurationInOpenState:30000}")
    private long waitDurationInOpenStateMs;

    @Value("${template.cache.circuitBreaker.slidingWindowSize:100}")
    private int slidingWindowSize;

    @Value("${template.cache.circuitBreaker.minimumNumberOfCalls:10}")
    private int minimumNumberOfCalls;

    @Autowired
    public TemplateCache(RedisTemplate<String, String> redisTemplate, 
                         CircuitBreakerRegistry circuitBreakerRegistry,
                         MeterRegistry meterRegistry) {
        this.redisTemplate = redisTemplate;
        this.meterRegistry = meterRegistry;
        this.circuitBreaker = createCircuitBreaker(circuitBreakerRegistry);
    }

    @PostConstruct
    public void init() {
        // Initialize local cache
        localCache = Caffeine.newBuilder()
                .maximumSize(localCacheMaxSize)
                .expireAfterWrite(Duration.ofSeconds(localCacheExpireAfterWriteSeconds))
                .recordStats()
                .build();

        // Initialize metrics
        localCacheHits = meterRegistry.counter("cache.template.local.hits");
        localCacheMisses = meterRegistry.counter("cache.template.local.misses");
        redisCacheHits = meterRegistry.counter("cache.template.redis.hits");
        redisCacheMisses = meterRegistry.counter("cache.template.redis.misses");
        redisErrors = meterRegistry.counter("cache.template.redis.errors");
        cacheAccessTimer = meterRegistry.timer("cache.template.access.time");

        LOGGER.info("Template cache initialized with local cache max size: {}, local TTL: {}s, Redis TTL: {}s",
                localCacheMaxSize, localCacheExpireAfterWriteSeconds, redisCacheExpireAfterWriteSeconds);
    }

    /**
     * Gets a template from the cache, first checking the local cache, then the Redis cache.
     * If the template is not found in either cache, the templateLoader is called to load the template,
     * and the result is stored in both caches.
     * <p>
     * The method implements a multi-level caching strategy with the following flow:
     * <ol>
     *   <li>Check local Caffeine cache first (fastest)</li>
     *   <li>If not found locally, check Redis cache with circuit breaker protection</li>
     *   <li>If found in Redis but not locally, store in local cache for future requests</li>
     *   <li>If not found in either cache, load template using the provided supplier</li>
     *   <li>Store newly loaded template in both local and Redis caches</li>
     * </ol>
     * <p>
     * All cache operations are timed and recorded as metrics for monitoring.
     * Redis failures are handled gracefully via the circuit breaker pattern.
     *
     * @param templateKey    The unique key for the template
     * @param templateLoader A supplier function that loads the template if not found in cache
     * @return The template content or null if the template could not be loaded
     */
    public String getTemplate(String templateKey, Supplier<String> templateLoader) {
        return cacheAccessTimer.record(() -> {
            // First, try to get from local cache
            String template = localCache.getIfPresent(templateKey);
            if (template != null) {
                localCacheHits.increment();
                LOGGER.debug("Template '{}' found in local cache", templateKey);
                return template;
            }

            localCacheMisses.increment();
            LOGGER.debug("Template '{}' not found in local cache, checking Redis", templateKey);

            // If not in local cache, try to get from Redis with circuit breaker
            try {
                template = circuitBreaker.executeSupplier(() -> getFromRedis(templateKey));
                if (template != null) {
                    // Found in Redis, store in local cache
                    localCache.put(templateKey, template);
                    redisCacheHits.increment();
                    LOGGER.debug("Template '{}' found in Redis cache", templateKey);
                    return template;
                }
                redisCacheMisses.increment();
            } catch (Exception e) {
                redisErrors.increment();
                LOGGER.warn("Error accessing Redis cache for template '{}': {}", templateKey, e.getMessage());
                // Circuit breaker will handle Redis failures, continue to template loading
            }

            // Not found in any cache, load the template
            LOGGER.debug("Template '{}' not found in any cache, loading from source", templateKey);
            template = templateLoader.get();
            if (template != null) {
                // Store in both caches
                localCache.put(templateKey, template);
                try {
                    circuitBreaker.executeRunnable(() -> storeInRedis(templateKey, template));
                } catch (Exception e) {
                    redisErrors.increment();
                    LOGGER.warn("Error storing template '{}' in Redis: {}", templateKey, e.getMessage());
                    // Continue even if Redis storage fails
                }
            }

            return template;
        });
    }

    /**
     * Invalidates a template in both local and distributed caches.
     * <p>
     * This method should be called when a template is updated or deleted to ensure
     * that all service instances receive the updated template on their next request.
     * The invalidation is performed in both the local Caffeine cache and the distributed
     * Redis cache to maintain consistency across all service instances.
     * <p>
     * Redis failures during invalidation are handled gracefully via the circuit breaker pattern
     * and will not prevent the local cache from being invalidated.
     *
     * @param templateKey The key of the template to invalidate
     */
    public void invalidateTemplate(String templateKey) {
        LOGGER.debug("Invalidating template '{}' from caches", templateKey);
        // Remove from local cache
        localCache.invalidate(templateKey);

        // Remove from Redis cache with circuit breaker
        try {
            circuitBreaker.executeRunnable(() -> {
                String redisKey = REDIS_CACHE_PREFIX + templateKey;
                redisTemplate.delete(redisKey);
            });
        } catch (Exception e) {
            redisErrors.increment();
            LOGGER.warn("Error invalidating template '{}' from Redis: {}", templateKey, e.getMessage());
        }
    }

    /**
     * Invalidates all templates in both local and distributed caches.
     * <p>
     * This method should be called when multiple templates are updated simultaneously
     * or when a system-wide cache refresh is needed. It clears all templates from both
     * the local Caffeine cache and the distributed Redis cache.
     * <p>
     * In Redis, only keys with the template prefix are deleted to avoid affecting other
     * data that might be stored in the same Redis instance. Redis failures during invalidation
     * are handled gracefully via the circuit breaker pattern and will not prevent the local
     * cache from being invalidated.
     * <p>
     * Note: This operation can be expensive in a large system with many templates and should
     * be used judiciously.
     */
    public void invalidateAllTemplates() {
        LOGGER.info("Invalidating all templates from caches");
        // Clear local cache
        localCache.invalidateAll();

        // Clear Redis cache with circuit breaker (only template keys)
        try {
            circuitBreaker.executeRunnable(() -> {
                // Find all keys with the template prefix and delete them
                Set<String> keys = redisTemplate.keys(REDIS_CACHE_PREFIX + "*");
                if (keys != null && !keys.isEmpty()) {
                    redisTemplate.delete(keys);
                }
            });
        } catch (Exception e) {
            redisErrors.increment();
            LOGGER.warn("Error invalidating all templates from Redis: {}", e.getMessage());
        }
    }

    /**
     * Gets cache statistics for monitoring and diagnostics.
     * <p>
     * This method returns a comprehensive set of statistics about the cache's performance
     * and current state, including hit/miss counts for both cache levels, error counts,
     * current cache size, and circuit breaker state. These statistics can be used for:
     * <ul>
     *   <li>Monitoring cache efficiency and performance</li>
     *   <li>Diagnosing cache-related issues</li>
     *   <li>Tuning cache parameters for optimal performance</li>
     *   <li>Alerting on abnormal cache behavior</li>
     * </ul>
     * <p>
     * The statistics are collected from both the Caffeine cache's built-in stats and
     * the custom Micrometer metrics maintained by this class.
     *
     * @return A map of cache statistics with string keys and object values
     */
    public Map<String, Object> getCacheStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("localCacheSize", localCache.estimatedSize());
        stats.put("localCacheHits", localCacheHits.count());
        stats.put("localCacheMisses", localCacheMisses.count());
        stats.put("redisCacheHits", redisCacheHits.count());
        stats.put("redisCacheMisses", redisCacheMisses.count());
        stats.put("redisErrors", redisErrors.count());
        stats.put("circuitBreakerState", circuitBreaker.getState().name());
        return stats;
    }

    /**
     * Creates and configures the circuit breaker for Redis operations.
     * <p>
     * The circuit breaker pattern prevents cascading failures by detecting when Redis
     * is unavailable or experiencing high error rates, and temporarily suspending Redis
     * operations to allow the system to recover. During this time, the cache falls back
     * to using only the local cache or loading templates directly from the source.
     * <p>
     * The circuit breaker is configured with the following parameters:
     * <ul>
     *   <li>Failure rate threshold: Percentage of failed calls that triggers the circuit to open</li>
     *   <li>Wait duration in open state: How long the circuit stays open before trying Redis again</li>
     *   <li>Sliding window size: Number of calls considered when calculating the failure rate</li>
     *   <li>Minimum number of calls: Minimum calls required before the circuit can open</li>
     * </ul>
     * <p>
     * The circuit breaker also publishes events for state transitions and errors, which
     * are logged for monitoring and diagnostics.
     *
     * @param circuitBreakerRegistry The registry to create and register the circuit breaker
     * @return A configured CircuitBreaker instance
     */
    private CircuitBreaker createCircuitBreaker(CircuitBreakerRegistry circuitBreakerRegistry) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(failureRateThreshold)
                .waitDurationInOpenState(Duration.ofMillis(waitDurationInOpenStateMs))
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(slidingWindowSize)
                .minimumNumberOfCalls(minimumNumberOfCalls)
                .build();

        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME, config);
        
        // Register event listeners for monitoring
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker '{}' state changed from {} to {}",
                        event.getCircuitBreakerName(), event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()))
                .onError(event -> LOGGER.debug("Circuit breaker '{}' recorded error: {}",
                        event.getCircuitBreakerName(), event.getThrowable().getMessage()));

        return circuitBreaker;
    }

    /**
     * Gets a template from Redis.
     * <p>
     * This method retrieves a template from the Redis distributed cache using the template key.
     * The key is prefixed with the Redis cache prefix to avoid key collisions with other data
     * that might be stored in the same Redis instance.
     * <p>
     * This method is called within the circuit breaker's context, so Redis failures will be
     * handled by the circuit breaker pattern.
     *
     * @param templateKey The key of the template to retrieve
     * @return The template content or null if not found in Redis
     */
    private String getFromRedis(String templateKey) {
        String redisKey = REDIS_CACHE_PREFIX + templateKey;
        return redisTemplate.opsForValue().get(redisKey);
    }

    /**
     * Stores a template in Redis with expiration.
     * <p>
     * This method stores a template in the Redis distributed cache with an expiration time.
     * The key is prefixed with the Redis cache prefix to avoid key collisions with other data
     * that might be stored in the same Redis instance.
     * <p>
     * The expiration time is configured via the redisCacheExpireAfterWriteSeconds property
     * and ensures that templates are eventually refreshed from the source even if they are
     * not explicitly invalidated.
     * <p>
     * This method is called within the circuit breaker's context, so Redis failures will be
     * handled by the circuit breaker pattern.
     *
     * @param templateKey The key of the template to store
     * @param template The template content to store
     */
    private void storeInRedis(String templateKey, String template) {
        String redisKey = REDIS_CACHE_PREFIX + templateKey;
        redisTemplate.opsForValue().set(redisKey, template, redisCacheExpireAfterWriteSeconds, TimeUnit.SECONDS);
    }
}