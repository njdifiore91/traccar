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
package org.traccar.messaging;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Centralized manager for message broker connections.
 * Provides connection management, circuit breaking, and metrics for both Kafka and RabbitMQ.
 */
@Singleton
public class MessageBrokerManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBrokerManager.class);
    
    private final MessagingConfig config;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final ScheduledExecutorService reconnectExecutor;
    
    private final AtomicReference<Connection> amqpConnection = new AtomicReference<>();
    private final AtomicReference<AdminClient> kafkaAdminClient = new AtomicReference<>();
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    
    private final Map<String, CircuitBreaker> circuitBreakers = new ConcurrentHashMap<>();
    
    /**
     * Constructs a MessageBrokerManager with the specified dependencies.
     *
     * @param config The messaging configuration
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param meterRegistry The Micrometer registry for metrics collection
     */
    @Inject
    public MessageBrokerManager(MessagingConfig config, Tracer tracer, MeterRegistry meterRegistry) {
        this.config = config;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.reconnectExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "message-broker-reconnect");
            thread.setDaemon(true);
            return thread;
        });
        
        // Configure circuit breaker registry
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        
        // Register metrics for connection status
        Gauge.builder("messaging.connection.status", () -> {
            if (amqpConnection.get() != null && amqpConnection.get().isOpen()) {
                return 1.0;
            } else {
                return 0.0;
            }
        })
        .tag("broker", "rabbitmq")
        .description("Connection status for RabbitMQ (1=connected, 0=disconnected)")
        .register(meterRegistry);
        
        Gauge.builder("messaging.connection.status", () -> {
            if (kafkaAdminClient.get() != null) {
                return 1.0;
            } else {
                return 0.0;
            }
        })
        .tag("broker", "kafka")
        .description("Connection status for Kafka (1=connected, 0=disconnected)")
        .register(meterRegistry);
        
        // Initialize connections
        initializeConnections();
        
        // Schedule periodic connection checks
        reconnectExecutor.scheduleAtFixedRate(this::checkConnections, 30, 30, TimeUnit.SECONDS);
        
        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }
    
    /**
     * Initializes connections to message brokers.
     */
    private void initializeConnections() {
        // Initialize RabbitMQ connection if configured
        if (config.isRabbitMqEnabled()) {
            initializeAmqpConnection();
        }
        
        // Initialize Kafka connection if configured
        if (config.isKafkaEnabled()) {
            initializeKafkaConnection();
        }
    }
    
    /**
     * Initializes the AMQP connection to RabbitMQ.
     */
    private void initializeAmqpConnection() {
        Span span = tracer.spanBuilder("amqp.connect")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("messaging.system", "rabbitmq")
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            CircuitBreaker circuitBreaker = getCircuitBreaker("amqp-connection");
            
            circuitBreaker.executeRunnable(() -> {
                try {
                    ConnectionFactory factory = new ConnectionFactory();
                    factory.setUri(config.getRabbitMqUri());
                    factory.setAutomaticRecoveryEnabled(true);
                    factory.setTopologyRecoveryEnabled(true);
                    factory.setNetworkRecoveryInterval(5000);
                    
                    Connection connection = factory.newConnection();
                    amqpConnection.set(connection);
                    
                    LOGGER.info("Successfully connected to RabbitMQ at {}", config.getRabbitMqUri());
                    span.setStatus(StatusCode.OK);
                } catch (IOException | TimeoutException | NoSuchAlgorithmException | KeyManagementException | URISyntaxException e) {
                    LOGGER.error("Failed to connect to RabbitMQ", e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    throw new RuntimeException("Failed to connect to RabbitMQ", e);
                }
            });
        } finally {
            span.end();
        }
    }
    
    /**
     * Initializes the Kafka connection.
     */
    private void initializeKafkaConnection() {
        Span span = tracer.spanBuilder("kafka.connect")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("messaging.system", "kafka")
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            CircuitBreaker circuitBreaker = getCircuitBreaker("kafka-connection");
            
            circuitBreaker.executeRunnable(() -> {
                try {
                    Properties props = new Properties();
                    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
                    
                    AdminClient adminClient = AdminClient.create(props);
                    kafkaAdminClient.set(adminClient);
                    
                    // Test connection by listing topics
                    adminClient.listTopics().names().get(5, TimeUnit.SECONDS);
                    
                    LOGGER.info("Successfully connected to Kafka at {}", config.getKafkaBootstrapServers());
                    span.setStatus(StatusCode.OK);
                } catch (Exception e) {
                    LOGGER.error("Failed to connect to Kafka", e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    throw new RuntimeException("Failed to connect to Kafka", e);
                }
            });
        } finally {
            span.end();
        }
    }
    
    /**
     * Gets or creates a circuit breaker for the specified key.
     *
     * @param key The circuit breaker key
     * @return The circuit breaker instance
     */
    private CircuitBreaker getCircuitBreaker(String key) {
        return circuitBreakers.computeIfAbsent(key, k -> circuitBreakerRegistry.circuitBreaker(k));
    }
    
    /**
     * Checks and attempts to recover connections if needed.
     */
    private void checkConnections() {
        if (shuttingDown.get()) {
            return;
        }
        
        // Check and recover RabbitMQ connection if needed
        if (config.isRabbitMqEnabled() && (amqpConnection.get() == null || !amqpConnection.get().isOpen())) {
            LOGGER.info("RabbitMQ connection is closed or null, attempting to reconnect");
            try {
                initializeAmqpConnection();
            } catch (Exception e) {
                LOGGER.error("Failed to reconnect to RabbitMQ", e);
            }
        }
        
        // Check and recover Kafka connection if needed
        if (config.isKafkaEnabled() && kafkaAdminClient.get() == null) {
            LOGGER.info("Kafka connection is null, attempting to reconnect");
            try {
                initializeKafkaConnection();
            } catch (Exception e) {
                LOGGER.error("Failed to reconnect to Kafka", e);
            }
        }
    }
    
    /**
     * Gets the AMQP connection to RabbitMQ.
     * If the connection is not available, attempts to initialize it.
     *
     * @return The AMQP connection
     * @throws IOException If an error occurs during connection initialization
     * @throws TimeoutException If the connection times out
     */
    public Connection getAmqpConnection() throws IOException, TimeoutException {
        if (amqpConnection.get() == null || !amqpConnection.get().isOpen()) {
            synchronized (this) {
                if (amqpConnection.get() == null || !amqpConnection.get().isOpen()) {
                    initializeAmqpConnection();
                }
            }
        }
        
        Connection connection = amqpConnection.get();
        if (connection == null) {
            throw new IOException("Failed to obtain AMQP connection");
        }
        
        return connection;
    }
    
    /**
     * Gets the Kafka admin client.
     * If the client is not available, attempts to initialize it.
     *
     * @return The Kafka admin client
     * @throws IOException If an error occurs during client initialization
     */
    public AdminClient getKafkaAdminClient() throws IOException {
        if (kafkaAdminClient.get() == null) {
            synchronized (this) {
                if (kafkaAdminClient.get() == null) {
                    initializeKafkaConnection();
                }
            }
        }
        
        AdminClient client = kafkaAdminClient.get();
        if (client == null) {
            throw new IOException("Failed to obtain Kafka admin client");
        }
        
        return client;
    }
    
    /**
     * Shuts down the message broker manager and releases all resources.
     */
    public void shutdown() {
        if (shuttingDown.compareAndSet(false, true)) {
            LOGGER.info("Shutting down MessageBrokerManager");
            
            // Shutdown reconnect executor
            reconnectExecutor.shutdownNow();
            
            // Close RabbitMQ connection
            Connection connection = amqpConnection.getAndSet(null);
            if (connection != null && connection.isOpen()) {
                try {
                    connection.close();
                    LOGGER.info("Closed RabbitMQ connection");
                } catch (IOException e) {
                    LOGGER.error("Error closing RabbitMQ connection", e);
                }
            }
            
            // Close Kafka admin client
            AdminClient adminClient = kafkaAdminClient.getAndSet(null);
            if (adminClient != null) {
                try {
                    adminClient.close();
                    LOGGER.info("Closed Kafka admin client");
                } catch (Exception e) {
                    LOGGER.error("Error closing Kafka admin client", e);
                }
            }
        }
    }
}