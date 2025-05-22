/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.broadcast;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Redis implementation of the BroadcastService that uses Redis pub/sub for message distribution.
 * This implementation includes service discovery integration, distributed tracing,
 * metrics collection, and circuit breaker patterns for resilience.
 */
public class RedisBroadcastService extends BaseBroadcastService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisBroadcastService.class);
    
    private static final String CIRCUIT_BREAKER_NAME = "redisBroadcast";
    private static final String DEFAULT_CHANNEL = "traccar";
    
    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;
    private final String channel;
    
    private Jedis subscriber;
    private Jedis publisher;
    
    private final String id = UUID.randomUUID().toString();
    private final CircuitBreaker circuitBreaker;
    private final ServiceDiscovery serviceDiscovery;
    private final Tracer tracer;
    private final TextMapPropagator propagator;
    
    // Metrics
    private final Counter messagesSentCounter;
    private final Counter messagesReceivedCounter;
    private final Counter errorsCounter;
    private final Timer sendMessageTimer;
    
    /**
     * Custom TextMapSetter for propagating trace context in Redis messages.
     */
    private static class MessageTextMapSetter implements TextMapSetter<StringBuilder> {
        @Override
        public void set(@Nullable StringBuilder carrier, String key, String value) {
            if (carrier != null) {
                carrier.append(key).append("=").append(value).append(";");
            }
        }
    }
    
    /**
     * Custom TextMapGetter for extracting trace context from Redis messages.
     */
    private static class MessageTextMapGetter implements TextMapGetter<String> {
        @Override
        public Iterable<String> keys(String carrier) {
            if (carrier == null || !carrier.contains(";")) {
                return Collections.emptyList();
            }
            String[] parts = carrier.split(";");
            List<String> keys = new java.util.ArrayList<>(parts.length);
            for (String part : parts) {
                if (part.contains("=")) {
                    keys.add(part.substring(0, part.indexOf('=')));
                }
            }
            return keys;
        }

        @Nullable
        @Override
        public String get(@Nullable String carrier, String key) {
            if (carrier == null || !carrier.contains(";")) {
                return null;
            }
            String[] parts = carrier.split(";");
            for (String part : parts) {
                if (part.startsWith(key + "=")) {
                    return part.substring(key.length() + 1);
                }
            }
            return null;
        }
    }

    /**
     * Constructs a new RedisBroadcastService with the specified dependencies.
     *
     * @param config Configuration for Redis connection
     * @param executorService Executor service for async operations
     * @param objectMapper Object mapper for serialization
     * @param meterRegistry Registry for metrics collection
     * @param serviceDiscovery Service discovery for Redis endpoint resolution
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param propagator OpenTelemetry context propagator
     * @throws IOException If connection to Redis fails
     */
    public RedisBroadcastService(
            Config config, 
            ExecutorService executorService, 
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            ServiceDiscovery serviceDiscovery,
            Tracer tracer,
            TextMapPropagator propagator) throws IOException {
        
        this.executorService = executorService;
        this.objectMapper = objectMapper;
        this.serviceDiscovery = serviceDiscovery;
        this.tracer = tracer;
        this.propagator = propagator;
        this.channel = config.getString(Keys.BROADCAST_CHANNEL, DEFAULT_CHANNEL);
        
        // Initialize metrics
        this.messagesSentCounter = meterRegistry.counter("redis.broadcast.messages.sent");
        this.messagesReceivedCounter = meterRegistry.counter("redis.broadcast.messages.received");
        this.errorsCounter = meterRegistry.counter("redis.broadcast.errors");
        this.sendMessageTimer = meterRegistry.timer("redis.broadcast.send.time");
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .recordExceptions(JedisConnectionException.class, IOException.class)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Register circuit breaker events for monitoring
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker state changed from {} to {}", 
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));
        
        // Connect to Redis using service discovery if available
        String redisUrl = resolveRedisUrl(config.getString(Keys.BROADCAST_ADDRESS));
        
        try {
            subscriber = new Jedis(redisUrl);
            publisher = new Jedis(redisUrl);
            subscriber.connect();
            LOGGER.info("Connected to Redis broadcast service at {}", redisUrl);
        } catch (JedisConnectionException e) {
            errorsCounter.increment();
            LOGGER.error("Failed to connect to Redis at {}", redisUrl, e);
            throw new IOException(e);
        }
    }
    
    /**
     * Resolves the Redis URL using service discovery if available.
     *
     * @param configuredUrl The URL configured in the application
     * @return The resolved Redis URL
     */
    private String resolveRedisUrl(String configuredUrl) {
        if (serviceDiscovery == null || configuredUrl == null) {
            return configuredUrl;
        }
        
        try {
            // Try to resolve Redis service from service discovery
            List<ServiceInstance> instances = serviceDiscovery.findServiceInstances("redis");
            if (!instances.isEmpty()) {
                ServiceInstance instance = instances.get(0);
                String discoveredUrl = instance.getHost() + ":" + instance.getPort();
                LOGGER.info("Discovered Redis service at {}", discoveredUrl);
                return discoveredUrl;
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to discover Redis service, falling back to configured URL", e);
        }
        
        return configuredUrl;
    }

    @Override
    public boolean singleInstance() {
        return false;
    }

    @Override
    protected void sendMessage(BroadcastMessage message) {
        // Create a span for the send operation
        Span span = tracer.spanBuilder("RedisBroadcast.sendMessage").startSpan();
        try (Scope scope = span.makeCurrent()) {
            // Add attributes to the span
            span.setAttribute("redis.channel", channel);
            span.setAttribute("broadcast.message.type", message.getClass().getSimpleName());
            
            // Execute with circuit breaker and timer
            sendMessageTimer.record(() -> {
                try {
                    // Serialize the message with trace context
                    String serializedMessage = serializeWithContext(message, span.getSpanContext());
                    
                    // Use circuit breaker to protect against Redis failures
                    circuitBreaker.executeRunnable(() -> {
                        publisher.publish(channel, serializedMessage);
                    });
                    
                    messagesSentCounter.increment();
                } catch (IOException e) {
                    span.recordException(e);
                    errorsCounter.increment();
                    LOGGER.warn("Failed to serialize broadcast message", e);
                } catch (JedisConnectionException e) {
                    span.recordException(e);
                    errorsCounter.increment();
                    LOGGER.warn("Failed to publish broadcast message", e);
                } catch (Exception e) {
                    span.recordException(e);
                    errorsCounter.increment();
                    LOGGER.warn("Broadcast failed", e);
                }
            });
        } finally {
            span.end();
        }
    }
    
    /**
     * Serializes a broadcast message with trace context for distributed tracing.
     *
     * @param message The message to serialize
     * @param spanContext The current span context to propagate
     * @return The serialized message with trace context
     * @throws IOException If serialization fails
     */
    private String serializeWithContext(BroadcastMessage message, SpanContext spanContext) throws IOException {
        // Serialize the message
        String messageJson = objectMapper.writeValueAsString(message);
        
        // Add trace context
        StringBuilder contextCarrier = new StringBuilder();
        propagator.inject(Context.current(), contextCarrier, new MessageTextMapSetter());
        
        // Format: instanceId:contextData:messageJson
        return id + ":" + contextCarrier + ":" + messageJson;
    }

    @Override
    public void start() throws IOException {
        executorService.submit(receiver);
        LOGGER.info("Redis broadcast service started on channel {}", channel);
    }

    @Override
    public void stop() {
        LOGGER.info("Shutting down Redis broadcast service");
        
        // Create a span for the shutdown operation
        Span span = tracer.spanBuilder("RedisBroadcast.shutdown").startSpan();
        try (Scope scope = span.makeCurrent()) {
            try {
                if (subscriber != null) {
                    subscriber.close();
                    subscriber = null;
                }
            } catch (JedisException e) {
                span.recordException(e);
                errorsCounter.increment();
                LOGGER.warn("Subscriber close failed", e);
            }
            
            try {
                if (publisher != null) {
                    publisher.close();
                    publisher = null;
                }
            } catch (JedisException e) {
                span.recordException(e);
                errorsCounter.increment();
                LOGGER.warn("Publisher close failed", e);
            }
        } finally {
            span.end();
        }
    }

    private final Runnable receiver = new Runnable() {
        @Override
        public void run() {
            try {
                subscriber.subscribe(new JedisPubSub() {
                    @Override
                    public void onMessage(String messageChannel, String message) {
                        // Create a span for the receive operation
                        Span span = tracer.spanBuilder("RedisBroadcast.receiveMessage").startSpan();
                        try (Scope scope = span.makeCurrent()) {
                            span.setAttribute("redis.channel", messageChannel);
                            
                            try {
                                // Parse message parts: instanceId:contextData:messageJson
                                String[] parts = message.split(":", 3);
                                if (messageChannel.equals(channel) && parts.length == 3 && !id.equals(parts[0])) {
                                    // Extract trace context
                                    String contextData = parts[1];
                                    Context extractedContext = propagator.extract(Context.current(), contextData, new MessageTextMapGetter());
                                    
                                    // Process message with the extracted context
                                    try (Scope extractedScope = extractedContext.makeCurrent()) {
                                        // Deserialize and handle the message
                                        BroadcastMessage broadcastMessage = objectMapper.readValue(parts[2], BroadcastMessage.class);
                                        span.setAttribute("broadcast.message.type", broadcastMessage.getClass().getSimpleName());
                                        
                                        handleMessage(broadcastMessage);
                                        messagesReceivedCounter.increment();
                                    }
                                }
                            } catch (Exception e) {
                                span.recordException(e);
                                errorsCounter.increment();
                                LOGGER.warn("Broadcast handleMessage failed", e);
                            }
                        } finally {
                            span.end();
                        }
                    }
                }, channel);
            } catch (JedisException e) {
                errorsCounter.increment();
                LOGGER.error("Redis subscription failed", e);
                throw new RuntimeException(e);
            }
        }
    };
}