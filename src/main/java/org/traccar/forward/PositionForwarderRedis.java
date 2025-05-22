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
package org.traccar.forward;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class PositionForwarderRedis implements PositionForwarder {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionForwarderRedis.class);

    private final String url;
    private final ObjectMapper objectMapper;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Timer operationTimer;

    // Redis connection objects
    private JedisPool jedisPool;
    private JedisSentinelPool jedisSentinelPool;
    private JedisCluster jedisCluster;
    private final boolean useCluster;
    private final boolean useSentinel;

    public PositionForwarderRedis(Config config, ObjectMapper objectMapper, Tracer tracer, MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.url = config.getString(Keys.FORWARD_URL);
        
        // Initialize metrics
        this.successCounter = Counter.builder("redis.operations")
                .tag("result", "success")
                .tag("operation", "forward")
                .description("Number of successful Redis forward operations")
                .register(meterRegistry);
        
        this.failureCounter = Counter.builder("redis.operations")
                .tag("result", "failure")
                .tag("operation", "forward")
                .description("Number of failed Redis forward operations")
                .register(meterRegistry);
        
        this.operationTimer = Timer.builder("redis.operation.duration")
                .tag("operation", "forward")
                .description("Time taken for Redis forward operations")
                .register(meterRegistry);
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("redisForwarder");
        
        // Determine Redis connection type
        this.useCluster = config.getBoolean(Keys.FORWARD_REDIS_CLUSTER_ENABLED, false);
        this.useSentinel = config.getBoolean(Keys.FORWARD_REDIS_SENTINEL_ENABLED, false);
        
        // Initialize Redis connection based on configuration
        initializeRedisConnection(config);
    }

    private void initializeRedisConnection(Config config) {
        // Configure connection pooling
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(config.getInteger(Keys.FORWARD_REDIS_MAX_CONNECTIONS, 10));
        poolConfig.setMaxIdle(config.getInteger(Keys.FORWARD_REDIS_MAX_IDLE, 5));
        poolConfig.setMinIdle(config.getInteger(Keys.FORWARD_REDIS_MIN_IDLE, 1));
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMaxWaitMillis(config.getLong(Keys.FORWARD_REDIS_MAX_WAIT_MILLIS, 3000));
        
        if (useCluster) {
            // Initialize Redis Cluster
            String[] nodes = config.getString(Keys.FORWARD_REDIS_CLUSTER_NODES).split(",");
            Set<HostAndPort> clusterNodes = new HashSet<>();
            for (String node : nodes) {
                String[] hostPort = node.split(":");
                clusterNodes.add(new HostAndPort(hostPort[0], Integer.parseInt(hostPort[1])));
            }
            jedisCluster = new JedisCluster(clusterNodes, poolConfig);
            LOGGER.info("Initialized Redis Cluster connection with {} nodes", nodes.length);
        } else if (useSentinel) {
            // Initialize Redis Sentinel
            String masterName = config.getString(Keys.FORWARD_REDIS_SENTINEL_MASTER);
            String[] sentinels = config.getString(Keys.FORWARD_REDIS_SENTINEL_NODES).split(",");
            Set<String> sentinelSet = new HashSet<>();
            for (String sentinel : sentinels) {
                sentinelSet.add(sentinel);
            }
            String password = config.getString(Keys.FORWARD_REDIS_PASSWORD, null);
            jedisSentinelPool = new JedisSentinelPool(masterName, sentinelSet, poolConfig, 2000, password, 0);
            LOGGER.info("Initialized Redis Sentinel connection with master {} and {} sentinels", masterName, sentinels.length);
        } else {
            // Initialize standard Redis connection pool
            String[] hostPort = url.split(":");
            String host = hostPort[0];
            int port = hostPort.length > 1 ? Integer.parseInt(hostPort[1]) : 6379;
            String password = config.getString(Keys.FORWARD_REDIS_PASSWORD, null);
            int database = config.getInteger(Keys.FORWARD_REDIS_DATABASE, 0);
            
            if (password != null && !password.isEmpty()) {
                jedisPool = new JedisPool(poolConfig, host, port, 2000, password, database);
            } else {
                jedisPool = new JedisPool(poolConfig, host, port, 2000, null, database);
            }
            LOGGER.info("Initialized Redis connection pool to {}:{}", host, port);
        }
    }

    @Override
    public void forward(PositionData positionData, ResultHandler resultHandler) {
        // Create OpenTelemetry span for Redis operation
        Span span = tracer.spanBuilder("redis.forward")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("redis.operation", "lpush")
                .setAttribute("redis.device.id", positionData.getDevice().getUniqueId())
                .startSpan();
        
        // Use timer to measure operation duration
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            String key = "positions." + positionData.getDevice().getUniqueId();
            String value = objectMapper.writeValueAsString(positionData.getPosition());
            
            // Use circuit breaker to handle Redis failures
            Supplier<Boolean> redisOperation = () -> {
                try {
                    if (useCluster) {
                        jedisCluster.lpush(key, value);
                    } else if (useSentinel) {
                        try (Jedis jedis = jedisSentinelPool.getResource()) {
                            jedis.lpush(key, value);
                        }
                    } else {
                        try (Jedis jedis = jedisPool.getResource()) {
                            jedis.lpush(key, value);
                        }
                    }
                    return true;
                } catch (JedisConnectionException e) {
                    LOGGER.warn("Redis connection error: {}", e.getMessage());
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, "Redis connection error");
                    throw e;
                }
            };
            
            boolean result = circuitBreaker.executeSupplier(redisOperation);
            
            // Record metrics
            successCounter.increment();
            sample.stop(operationTimer);
            
            // Complete the span
            span.setStatus(StatusCode.OK);
            span.end();
            
            resultHandler.onResult(true, null);
        } catch (Exception e) {
            // Record metrics for failure
            failureCounter.increment();
            sample.stop(operationTimer);
            
            // Complete the span with error
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.end();
            
            resultHandler.onResult(false, e);
        }
    }

    /**
     * Closes all Redis connections when the forwarder is no longer needed.
     * Should be called during application shutdown.
     */
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
        if (jedisSentinelPool != null) {
            jedisSentinelPool.close();
        }
        if (jedisCluster != null) {
            try {
                jedisCluster.close();
            } catch (Exception e) {
                LOGGER.warn("Error closing Redis cluster connection", e);
            }
        }
    }
}