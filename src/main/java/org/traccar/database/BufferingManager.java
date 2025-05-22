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
package org.traccar.database;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Position;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * BufferingManager is responsible for managing position buffering across multiple instances.
 * It uses Redis for distributed buffering and implements circuit breaker pattern for database operations.
 * OpenTelemetry is used for distributed tracing and Micrometer for metrics collection.
 */
@Singleton
public class BufferingManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferingManager.class);
    private static final String CIRCUIT_BREAKER_NAME = "bufferingManager";
    private static final String REDIS_CHANNEL_PREFIX = "traccar:buffer:";
    private static final String REDIS_KEY_PREFIX = "traccar:buffer:device:";

    /**
     * Callback interface for position release events.
     */
    public interface Callback {
        void onReleased(ChannelHandlerContext context, Position position);
    }

    /**
     * Holder class for buffered positions with comparable interface for ordering.
     */
    private static final class Holder implements Comparable<Holder> {

        private final ChannelHandlerContext context;
        private final Position position;
        private Timeout timeout;
        private final String instanceId;

        private Holder(ChannelHandlerContext context, Position position, String instanceId) {
            this.context = context;
            this.position = position;
            this.instanceId = instanceId;
        }

        private int compareTime(Date left, Date right) {
            if (left != null && right != null) {
                return left.compareTo(right);
            }
            return 0;
        }

        @Override
        public int compareTo(Holder other) {
            int fixTimeResult = compareTime(position.getFixTime(), other.position.getFixTime());
            if (fixTimeResult != 0) {
                return fixTimeResult;
            }

            int deviceTimeResult = compareTime(position.getDeviceTime(), other.position.getDeviceTime());
            if (deviceTimeResult != 0) {
                return deviceTimeResult;
            }

            int serverTimeResult = position.getServerTime().compareTo(other.position.getServerTime());
            if (serverTimeResult != 0) {
                return serverTimeResult;
            }
            
            // If all times are equal, use instance ID to break ties
            return instanceId.compareTo(other.instanceId);
        }
    }

    private final Timer timer = new HashedWheelTimer();
    private final Callback callback;
    private final long threshold;
    private final String instanceId;
    private final boolean redisEnabled;
    private final JedisPool jedisPool;
    private final Map<Long, TreeSet<Holder>> buffer = new ConcurrentHashMap<>();
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    
    // Metrics
    private final Counter acceptedCounter;
    private final Counter releasedCounter;
    private final Counter failedCounter;
    private final Timer processingTimer;
    private final AtomicInteger bufferSize = new AtomicInteger(0);

    /**
     * Constructs a BufferingManager with the specified configuration and dependencies.
     *
     * @param config Configuration for the buffering manager
     * @param callback Callback for position release events
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     */
    @Inject
    public BufferingManager(Config config, Callback callback, Tracer tracer, MeterRegistry meterRegistry) {
        this.callback = callback;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.threshold = config.getLong(Keys.SERVER_BUFFERING_THRESHOLD);
        this.instanceId = config.getString("server.instance.id", java.util.UUID.randomUUID().toString());
        this.redisEnabled = config.getBoolean("server.buffer.redis.enabled", false);
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Register event listeners for circuit breaker state changes
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker state changed from {} to {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));
        
        // Initialize Redis connection pool if enabled
        if (redisEnabled) {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(config.getInteger("server.buffer.redis.pool.maxTotal", 128));
            poolConfig.setMaxIdle(config.getInteger("server.buffer.redis.pool.maxIdle", 128));
            poolConfig.setMinIdle(config.getInteger("server.buffer.redis.pool.minIdle", 16));
            poolConfig.setTestOnBorrow(true);
            poolConfig.setTestOnReturn(true);
            poolConfig.setTestWhileIdle(true);
            poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
            poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
            poolConfig.setNumTestsPerEvictionRun(3);
            poolConfig.setBlockWhenExhausted(true);
            
            String redisHost = config.getString("server.buffer.redis.host", "localhost");
            int redisPort = config.getInteger("server.buffer.redis.port", 6379);
            String redisPassword = config.getString("server.buffer.redis.password", null);
            int redisDatabase = config.getInteger("server.buffer.redis.database", 0);
            
            if (redisPassword != null && !redisPassword.isEmpty()) {
                jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 2000, redisPassword, redisDatabase);
            } else {
                jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 2000, null, redisDatabase);
            }
            
            // Start Redis subscription in a separate thread
            startRedisSubscription();
            
            LOGGER.info("Redis-based distributed buffering enabled with host: {}, port: {}", redisHost, redisPort);
        } else {
            jedisPool = null;
            LOGGER.info("Local buffering enabled");
        }
        
        // Register metrics
        acceptedCounter = Counter.builder("traccar.buffer.positions.accepted")
                .description("Number of positions accepted into the buffer")
                .register(meterRegistry);
        
        releasedCounter = Counter.builder("traccar.buffer.positions.released")
                .description("Number of positions released from the buffer")
                .register(meterRegistry);
        
        failedCounter = Counter.builder("traccar.buffer.positions.failed")
                .description("Number of positions that failed to be processed")
                .register(meterRegistry);
        
        processingTimer = Timer.builder("traccar.buffer.processing.time")
                .description("Time taken to process positions in the buffer")
                .register(meterRegistry);
        
        Gauge.builder("traccar.buffer.size", bufferSize, AtomicInteger::get)
                .description("Current size of the position buffer")
                .register(meterRegistry);
        
        LOGGER.info("BufferingManager initialized with instance ID: {}", instanceId);
    }

    /**
     * Starts a Redis subscription for buffer synchronization across instances.
     */
    private void startRedisSubscription() {
        if (!redisEnabled) {
            return;
        }
        
        Thread subscriptionThread = new Thread(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.subscribe(new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {
                        if (!message.startsWith(instanceId)) { // Ignore own messages
                            String[] parts = message.split(":", 3);
                            if (parts.length == 3) {
                                String sourceInstanceId = parts[0];
                                long deviceId = Long.parseLong(parts[1]);
                                String action = parts[2];
                                
                                if ("released".equals(action)) {
                                    LOGGER.debug("Received release notification for device {} from instance {}",
                                            deviceId, sourceInstanceId);
                                    synchronized (buffer) {
                                        TreeSet<Holder> queue = buffer.get(deviceId);
                                        if (queue != null && !queue.isEmpty()) {
                                            // Cancel all timeouts for this device as another instance has processed it
                                            for (Holder holder : queue) {
                                                if (holder.timeout != null) {
                                                    holder.timeout.cancel();
                                                }
                                            }
                                            queue.clear();
                                            bufferSize.addAndGet(-queue.size());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }, REDIS_CHANNEL_PREFIX + "*");
            } catch (Exception e) {
                LOGGER.error("Redis subscription error", e);
            }
        });
        
        subscriptionThread.setName("Redis-Subscription-Thread");
        subscriptionThread.setDaemon(true);
        subscriptionThread.start();
    }

    /**
     * Schedules a timeout for a buffered position.
     *
     * @param holder The position holder to schedule a timeout for
     * @return The scheduled timeout
     */
    private Timeout scheduleTimeout(Holder holder) {
        Span span = tracer.spanBuilder("BufferingManager.scheduleTimeout")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", holder.position.getDeviceId())
                .setAttribute("positionId", holder.position.getId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            return timer.newTimeout(
                    timeout -> {
                        Span releaseSpan = tracer.spanBuilder("BufferingManager.releasePosition")
                                .setSpanKind(SpanKind.INTERNAL)
                                .setAttribute("deviceId", holder.position.getDeviceId())
                                .setAttribute("positionId", holder.position.getId())
                                .startSpan();
                        
                        try (Scope releaseScope = releaseSpan.makeCurrent()) {
                            Timer.Sample sample = Timer.start(meterRegistry);
                            
                            LOGGER.debug("Releasing position with fix time {} for device {}",
                                    holder.position.getFixTime(), holder.position.getDeviceId());
                            
                            synchronized (buffer) {
                                TreeSet<Holder> queue = buffer.get(holder.position.getDeviceId());
                                if (queue != null) {
                                    queue.remove(holder);
                                    if (queue.isEmpty()) {
                                        buffer.remove(holder.position.getDeviceId());
                                    }
                                    bufferSize.decrementAndGet();
                                }
                            }
                            
                            // Execute callback with circuit breaker protection
                            executeWithCircuitBreaker(() -> {
                                callback.onReleased(holder.context, holder.position);
                                return null;
                            });
                            
                            // Publish release event to Redis if enabled
                            if (redisEnabled) {
                                try (Jedis jedis = jedisPool.getResource()) {
                                    String message = instanceId + ":" + holder.position.getDeviceId() + ":released";
                                    jedis.publish(REDIS_CHANNEL_PREFIX + holder.position.getDeviceId(), message);
                                } catch (Exception e) {
                                    LOGGER.warn("Failed to publish release event to Redis", e);
                                }
                            }
                            
                            releasedCounter.increment();
                            sample.stop(processingTimer);
                        } catch (Exception e) {
                            LOGGER.error("Error releasing position", e);
                            failedCounter.increment();
                        } finally {
                            releaseSpan.end();
                        }
                    },
                    threshold, TimeUnit.MILLISECONDS);
        } finally {
            span.end();
        }
    }

    /**
     * Executes a function with circuit breaker protection.
     *
     * @param supplier The function to execute
     * @param <T> The return type of the function
     * @return The result of the function
     */
    private <T> T executeWithCircuitBreaker(Supplier<T> supplier) {
        return circuitBreaker.executeSupplier(supplier);
    }

    /**
     * Accepts a position into the buffer for processing.
     *
     * @param context The channel handler context
     * @param position The position to buffer
     */
    public void accept(ChannelHandlerContext context, Position position) {
        Span span = tracer.spanBuilder("BufferingManager.accept")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", position.getDeviceId())
                .setAttribute("positionId", position.getId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            if (threshold > 0) {
                acceptedCounter.increment();
                
                synchronized (buffer) {
                    LOGGER.debug("Queuing position with fix time {} for device {}",
                            position.getFixTime(), position.getDeviceId());
                    
                    var queue = buffer.computeIfAbsent(position.getDeviceId(), k -> new TreeSet<>());
                    Holder holder = new Holder(context, position, instanceId);
                    holder.timeout = scheduleTimeout(holder);
                    queue.add(holder);
                    bufferSize.incrementAndGet();
                    
                    // Reset timeouts for all positions in the queue for this device
                    queue.tailSet(holder).forEach(h -> {
                        h.timeout.cancel();
                        h.timeout = scheduleTimeout(h);
                    });
                }
                
                // Store in Redis if enabled
                if (redisEnabled) {
                    try (Jedis jedis = jedisPool.getResource()) {
                        String key = REDIS_KEY_PREFIX + position.getDeviceId();
                        Map<String, String> positionData = new HashMap<>();
                        positionData.put("id", String.valueOf(position.getId()));
                        positionData.put("deviceId", String.valueOf(position.getDeviceId()));
                        positionData.put("fixTime", position.getFixTime() != null ? 
                                String.valueOf(position.getFixTime().getTime()) : "");
                        positionData.put("instanceId", instanceId);
                        
                        jedis.hset(key, positionData);
                        jedis.expire(key, threshold * 2 / 1000); // Set expiry to twice the threshold in seconds
                    } catch (Exception e) {
                        LOGGER.warn("Failed to store position data in Redis", e);
                    }
                }
            } else {
                // If buffering is disabled, directly release the position
                executeWithCircuitBreaker(() -> {
                    callback.onReleased(context, position);
                    return null;
                });
            }
        } catch (Exception e) {
            LOGGER.error("Error accepting position", e);
            failedCounter.increment();
        } finally {
            span.end();
        }
    }

    /**
     * Cleans up resources when the application is shutting down.
     */
    @PreDestroy
    public void shutdown() {
        LOGGER.info("Shutting down BufferingManager");
        
        // Cancel all scheduled timeouts
        timer.stop();
        
        // Process any remaining items in the buffer
        synchronized (buffer) {
            buffer.values().forEach(queue -> {
                queue.forEach(holder -> {
                    try {
                        callback.onReleased(holder.context, holder.position);
                    } catch (Exception e) {
                        LOGGER.warn("Error releasing position during shutdown", e);
                    }
                });
            });
            buffer.clear();
        }
        
        // Close Redis connection pool if enabled
        if (redisEnabled && jedisPool != null) {
            jedisPool.close();
        }
        
        LOGGER.info("BufferingManager shutdown complete");
    }
}