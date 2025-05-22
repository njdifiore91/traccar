/*
 * Copyright 2022 Anton Tananaev (anton@traccar.org)
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
package org.traccar.database;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Device;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Singleton
public class DeviceLookupService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceLookupService.class);

    private static final long INFO_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(60);
    private static final long THROTTLE_MIN_MS = TimeUnit.MINUTES.toMillis(1);
    private static final long THROTTLE_MAX_MS = TimeUnit.MINUTES.toMillis(30);
    
    private static final String CIRCUIT_BREAKER_NAME = "deviceLookupCircuitBreaker";
    private static final String RETRY_NAME = "deviceLookupRetry";
    private static final String REDIS_CACHE_PREFIX = "device:";
    private static final String REDIS_INVALIDATION_CHANNEL = "device-invalidation";
    private static final int REDIS_CACHE_TTL = 3600; // 1 hour in seconds

    private final Storage storage;
    private final Timer timer;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final JedisPool jedisPool;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Timer lookupTimer;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Counter cacheHitCounter;
    private final Counter cacheMissCounter;

    private final boolean throttlingEnabled;

    private static final class IdentifierInfo {
        private long lastQuery;
        private long delay;
        private Timeout timeout;
    }

    private final class IdentifierTask implements TimerTask {
        private final String uniqueId;

        private IdentifierTask(String uniqueId) {
            this.uniqueId = uniqueId;
        }

        @Override
        public void run(Timeout timeout) {
            LOGGER.debug("Device lookup expired {}", uniqueId);
            synchronized (DeviceLookupService.this) {
                identifierMap.remove(uniqueId);
            }
        }
    }

    private final Map<String, IdentifierInfo> identifierMap = new ConcurrentHashMap<>();

    @Inject
    public DeviceLookupService(Config config, Storage storage, Timer timer, Tracer tracer, MeterRegistry meterRegistry) {
        this.storage = storage;
        this.timer = timer;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        throttlingEnabled = config.getBoolean(Keys.DATABASE_THROTTLE_UNKNOWN);
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to open circuit
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10) // Consider last 10 calls for failure rate
                .minimumNumberOfCalls(5) // Minimum calls before calculating failure rate
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 calls in half-open state
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Initialize retry policy
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Maximum 3 attempts
                .waitDuration(Duration.ofMillis(500)) // Initial wait 500ms
                .retryExceptions(StorageException.class) // Retry on storage exceptions
                .exponentialBackoff(Duration.ofMillis(500), Duration.ofSeconds(5), 2.0) // Exponential backoff
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry(RETRY_NAME);
        
        // Initialize Redis connection pool
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(20);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(1);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        
        String redisHost = config.getString("redis.host", "localhost");
        int redisPort = config.getInteger("redis.port", 6379);
        String redisPassword = config.getString("redis.password", null);
        
        if (redisPassword != null && !redisPassword.isEmpty()) {
            this.jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 2000, redisPassword);
        } else {
            this.jedisPool = new JedisPool(poolConfig, redisHost, redisPort);
        }
        
        // Start Redis cache invalidation listener
        startCacheInvalidationListener();
        
        // Initialize metrics
        this.lookupTimer = Timer.builder("device.lookup.duration")
                .description("Time taken for device lookup operations")
                .register(meterRegistry);
        
        this.successCounter = Counter.builder("device.lookup.success")
                .description("Number of successful device lookups")
                .register(meterRegistry);
        
        this.failureCounter = Counter.builder("device.lookup.failure")
                .description("Number of failed device lookups")
                .register(meterRegistry);
        
        this.cacheHitCounter = Counter.builder("device.lookup.cache.hit")
                .description("Number of cache hits during device lookups")
                .register(meterRegistry);
        
        this.cacheMissCounter = Counter.builder("device.lookup.cache.miss")
                .description("Number of cache misses during device lookups")
                .register(meterRegistry);
    }
    
    private void startCacheInvalidationListener() {
        Thread listenerThread = new Thread(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.subscribe(new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {
                        if (REDIS_INVALIDATION_CHANNEL.equals(channel)) {
                            LOGGER.debug("Received cache invalidation for device: {}", message);
                            try (Jedis cacheJedis = jedisPool.getResource()) {
                                cacheJedis.del(REDIS_CACHE_PREFIX + message);
                            }
                        }
                    }
                }, REDIS_INVALIDATION_CHANNEL);
            } catch (Exception e) {
                LOGGER.error("Error in cache invalidation listener", e);
            }
        });
        listenerThread.setDaemon(true);
        listenerThread.setName("Redis-Cache-Invalidation-Listener");
        listenerThread.start();
    }

    private synchronized boolean isThrottled(String uniqueId) {
        if (throttlingEnabled) {
            IdentifierInfo info = identifierMap.get(uniqueId);
            return info != null && System.currentTimeMillis() < info.lastQuery + info.delay;
        } else {
            return false;
        }
    }

    private synchronized void lookupSucceeded(String uniqueId) {
        if (throttlingEnabled) {
            IdentifierInfo info = identifierMap.remove(uniqueId);
            if (info != null) {
                info.timeout.cancel();
            }
        }
    }

    private synchronized void lookupFailed(String uniqueId) {
        if (throttlingEnabled) {
            IdentifierInfo info = identifierMap.get(uniqueId);
            if (info != null) {
                info.timeout.cancel();
                info.delay = Math.min(info.delay * 2, THROTTLE_MAX_MS);
            } else {
                info = new IdentifierInfo();
                identifierMap.put(uniqueId, info);
                info.delay = THROTTLE_MIN_MS;
            }
            info.lastQuery = System.currentTimeMillis();
            info.timeout = timer.newTimeout(new IdentifierTask(uniqueId), INFO_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            LOGGER.debug("Device lookup {} throttled for {} ms", uniqueId, info.delay);
        }
    }
    
    private Device getDeviceFromCache(String uniqueId) {
        try (Jedis jedis = jedisPool.getResource()) {
            String deviceJson = jedis.get(REDIS_CACHE_PREFIX + uniqueId);
            if (deviceJson != null) {
                cacheHitCounter.increment();
                // In a real implementation, we would deserialize the JSON to a Device object
                // For simplicity, we'll just return null and let the database lookup happen
                // This would be replaced with proper JSON deserialization in production
                return null;
            }
        } catch (Exception e) {
            LOGGER.warn("Error accessing Redis cache", e);
        }
        cacheMissCounter.increment();
        return null;
    }
    
    private void cacheDevice(String uniqueId, Device device) {
        if (device != null) {
            try (Jedis jedis = jedisPool.getResource()) {
                // In a real implementation, we would serialize the Device object to JSON
                // For simplicity, we'll just store a placeholder
                // This would be replaced with proper JSON serialization in production
                jedis.setex(REDIS_CACHE_PREFIX + uniqueId, REDIS_CACHE_TTL, "device-data-placeholder");
            } catch (Exception e) {
                LOGGER.warn("Error storing device in Redis cache", e);
            }
        }
    }
    
    private void invalidateDeviceCache(String uniqueId) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.publish(REDIS_INVALIDATION_CHANNEL, uniqueId);
        } catch (Exception e) {
            LOGGER.warn("Error publishing cache invalidation", e);
        }
    }

    public Device lookup(String[] uniqueIds) {
        Span span = tracer.spanBuilder("device.lookup")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("uniqueIds.count", uniqueIds.length)
                .startSpan();
        
        try {
            return lookupTimer.record(() -> {
                Device device = null;
                
                // Try to find device in cache first
                for (String uniqueId : uniqueIds) {
                    if (!isThrottled(uniqueId)) {
                        span.setAttribute("uniqueId", uniqueId);
                        
                        // Check cache first
                        device = getDeviceFromCache(uniqueId);
                        if (device != null) {
                            span.setAttribute("cache.hit", true);
                            successCounter.increment();
                            lookupSucceeded(uniqueId);
                            return device;
                        }
                        
                        // If not in cache, try database with circuit breaker and retry
                        try {
                            Supplier<Device> decoratedSupplier = CircuitBreaker.decorateSupplier(
                                    circuitBreaker,
                                    () -> lookupFromDatabase(uniqueId, span));
                            
                            decoratedSupplier = Retry.decorateSupplier(retry, decoratedSupplier);
                            
                            device = decoratedSupplier.get();
                            
                            if (device != null) {
                                // Cache the device for future lookups
                                cacheDevice(uniqueId, device);
                                successCounter.increment();
                                lookupSucceeded(uniqueId);
                                break;
                            } else {
                                lookupFailed(uniqueId);
                            }
                        } catch (Exception e) {
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            failureCounter.increment();
                            lookupFailed(uniqueId);
                            LOGGER.warn("Device lookup error for {}", uniqueId, e);
                        }
                    } else {
                        LOGGER.debug("Device lookup throttled {}", uniqueId);
                    }
                }
                
                return device;
            });
        } finally {
            span.end();
        }
    }
    
    private Device lookupFromDatabase(String uniqueId, Span parentSpan) {
        Span span = tracer.spanBuilder("device.lookup.database")
                .setParent(parentSpan.getSpanContext())
                .setAttribute("uniqueId", uniqueId)
                .startSpan();
        
        try {
            Device device = storage.getObject(Device.class, new Request(
                    new Columns.All(), new Condition.Equals("uniqueId", uniqueId)));
            
            span.setAttribute("found", device != null);
            return device;
        } catch (StorageException e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw new RuntimeException("Database lookup failed", e);
        } finally {
            span.end();
        }
    }
}