package org.traccar;

import io.netty.channel.Channel;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.traccar.config.Config;
import org.traccar.database.CommandsManager;
import org.traccar.database.MediaManager;
import org.traccar.database.StatisticsManager;
import org.traccar.model.Device;
import org.traccar.session.ConnectionManager;
import org.traccar.session.DeviceSession;
import org.traccar.session.cache.CacheManager;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Base test class providing utilities for both monolithic and microservices testing.
 * Includes methods for dependency injection, mocking service clients, message brokers,
 * service discovery, and resilience patterns.
 */
public class BaseTest {

    // OpenTelemetry extension for distributed tracing in tests
    @RegisterExtension
    static final OpenTelemetryExtension OTEL = OpenTelemetryExtension.create();
    
    // Meter registry for metrics testing
    private static final MeterRegistry METER_REGISTRY = new SimpleMeterRegistry();
    
    // Cache for mocked service clients
    private final Map<String, Object> mockedClients = new HashMap<>();
    
    /**
     * Injects dependencies into a protocol decoder.
     * Compatible with both monolithic and microservices architectures.
     *
     * @param decoder The protocol decoder to inject dependencies into
     * @return The decoder with injected dependencies
     * @throws Exception If an error occurs during injection
     */
    protected <T extends BaseProtocolDecoder> T inject(T decoder) throws Exception {
        var config = new Config();
        decoder.setConfig(config);
        var device = mock(Device.class);
        when(device.getId()).thenReturn(1L);
        var cacheManager = mock(CacheManager.class);
        when(cacheManager.getConfig()).thenReturn(config);
        when(cacheManager.getObject(eq(Device.class), anyLong())).thenReturn(device);
        decoder.setCacheManager(cacheManager);
        var connectionManager = mock(ConnectionManager.class);
        var uniqueIdsProvided = new HashSet<Boolean>();
        when(connectionManager.getDeviceSession(any(), any(), any(), any(String[].class))).thenAnswer(invocation -> {
            var mock = new DeviceSession(
                    1L, "", null, mock(Protocol.class), mock(Channel.class), mock(SocketAddress.class));
            if (uniqueIdsProvided.isEmpty()) {
                if (invocation.getArguments().length > 3) {
                    uniqueIdsProvided.add(true);
                    return mock;
                }
                return null;
            } else {
                return mock;
            }
        });
        decoder.setConnectionManager(connectionManager);
        decoder.setStatisticsManager(mock(StatisticsManager.class));
        decoder.setMediaManager(mock(MediaManager.class));
        decoder.setCommandsManager(mock(CommandsManager.class));
        return decoder;
    }

    /**
     * Injects dependencies into a frame decoder.
     * Compatible with both monolithic and microservices architectures.
     *
     * @param decoder The frame decoder to inject dependencies into
     * @return The decoder with injected dependencies
     * @throws Exception If an error occurs during injection
     */
    protected <T extends BaseFrameDecoder> T inject(T decoder) throws Exception {
        return decoder;
    }

    /**
     * Injects dependencies into a protocol encoder.
     * Compatible with both monolithic and microservices architectures.
     *
     * @param encoder The protocol encoder to inject dependencies into
     * @return The encoder with injected dependencies
     * @throws Exception If an error occurs during injection
     */
    protected <T extends BaseProtocolEncoder> T inject(T encoder) throws Exception {
        var device = mock(Device.class);
        when(device.getId()).thenReturn(1L);
        when(device.getUniqueId()).thenReturn("123456789012345");
        var cacheManager = mock(CacheManager.class);
        when(cacheManager.getConfig()).thenReturn(mock(Config.class));
        when(cacheManager.getObject(eq(Device.class), anyLong())).thenReturn(device);
        encoder.setCacheManager(cacheManager);
        return encoder;
    }
    
    /**
     * Creates a mock gRPC service client.
     *
     * @param clientClass The class of the client to mock
     * @param <T> The type of the client
     * @return A mocked client instance
     */
    @SuppressWarnings("unchecked")
    protected <T> T mockGrpcClient(Class<T> clientClass) {
        if (mockedClients.containsKey(clientClass.getName())) {
            return (T) mockedClients.get(clientClass.getName());
        }
        T mockedClient = mock(clientClass);
        mockedClients.put(clientClass.getName(), mockedClient);
        return mockedClient;
    }
    
    /**
     * Creates a mock REST service client.
     *
     * @param clientClass The class of the client to mock
     * @param <T> The type of the client
     * @return A mocked client instance
     */
    @SuppressWarnings("unchecked")
    protected <T> T mockRestClient(Class<T> clientClass) {
        if (mockedClients.containsKey(clientClass.getName())) {
            return (T) mockedClients.get(clientClass.getName());
        }
        T mockedClient = mock(clientClass);
        mockedClients.put(clientClass.getName(), mockedClient);
        return mockedClient;
    }
    
    /**
     * Creates a mock Kafka producer.
     *
     * @param <K> The type of the message key
     * @param <V> The type of the message value
     * @return A mocked Kafka producer
     */
    @SuppressWarnings("unchecked")
    protected <K, V> org.apache.kafka.clients.producer.Producer<K, V> mockKafkaProducer() {
        String key = "kafka-producer";
        if (mockedClients.containsKey(key)) {
            return (org.apache.kafka.clients.producer.Producer<K, V>) mockedClients.get(key);
        }
        org.apache.kafka.clients.producer.Producer<K, V> producer = mock(org.apache.kafka.clients.producer.Producer.class);
        mockedClients.put(key, producer);
        return producer;
    }
    
    /**
     * Creates a mock Kafka consumer.
     *
     * @param <K> The type of the message key
     * @param <V> The type of the message value
     * @return A mocked Kafka consumer
     */
    @SuppressWarnings("unchecked")
    protected <K, V> org.apache.kafka.clients.consumer.Consumer<K, V> mockKafkaConsumer() {
        String key = "kafka-consumer";
        if (mockedClients.containsKey(key)) {
            return (org.apache.kafka.clients.consumer.Consumer<K, V>) mockedClients.get(key);
        }
        org.apache.kafka.clients.consumer.Consumer<K, V> consumer = mock(org.apache.kafka.clients.consumer.Consumer.class);
        mockedClients.put(key, consumer);
        return consumer;
    }
    
    /**
     * Creates a mock RabbitMQ channel.
     *
     * @return A mocked RabbitMQ channel
     */
    protected com.rabbitmq.client.Channel mockRabbitMqChannel() {
        String key = "rabbitmq-channel";
        if (mockedClients.containsKey(key)) {
            return (com.rabbitmq.client.Channel) mockedClients.get(key);
        }
        com.rabbitmq.client.Channel channel = mock(com.rabbitmq.client.Channel.class);
        mockedClients.put(key, channel);
        return channel;
    }
    
    /**
     * Creates a mock RabbitMQ connection.
     *
     * @return A mocked RabbitMQ connection
     */
    protected com.rabbitmq.client.Connection mockRabbitMqConnection() {
        String key = "rabbitmq-connection";
        if (mockedClients.containsKey(key)) {
            return (com.rabbitmq.client.Connection) mockedClients.get(key);
        }
        com.rabbitmq.client.Connection connection = mock(com.rabbitmq.client.Connection.class);
        try {
            when(connection.createChannel()).thenReturn(mockRabbitMqChannel());
        } catch (Exception e) {
            // Ignore exception in mock setup
        }
        mockedClients.put(key, connection);
        return connection;
    }
    
    /**
     * Creates a mock Consul client for service discovery.
     *
     * @return A mocked Consul client
     */
    protected com.orbitz.consul.Consul mockConsulClient() {
        String key = "consul-client";
        if (mockedClients.containsKey(key)) {
            return (com.orbitz.consul.Consul) mockedClients.get(key);
        }
        com.orbitz.consul.Consul consul = mock(com.orbitz.consul.Consul.class);
        // Mock common Consul API calls
        com.orbitz.consul.AgentClient agentClient = mock(com.orbitz.consul.AgentClient.class);
        when(consul.agentClient()).thenReturn(agentClient);
        com.orbitz.consul.HealthClient healthClient = mock(com.orbitz.consul.HealthClient.class);
        when(consul.healthClient()).thenReturn(healthClient);
        mockedClients.put(key, consul);
        return consul;
    }
    
    /**
     * Creates a mock Kubernetes client for service discovery.
     *
     * @return A mocked Kubernetes client
     */
    protected io.fabric8.kubernetes.client.KubernetesClient mockKubernetesClient() {
        String key = "kubernetes-client";
        if (mockedClients.containsKey(key)) {
            return (io.fabric8.kubernetes.client.KubernetesClient) mockedClients.get(key);
        }
        io.fabric8.kubernetes.client.KubernetesClient client = mock(io.fabric8.kubernetes.client.KubernetesClient.class);
        mockedClients.put(key, client);
        return client;
    }
    
    /**
     * Gets a tracer for distributed tracing in tests.
     *
     * @return A tracer instance
     */
    protected Tracer getTracer() {
        return OTEL.getOpenTelemetry().getTracer("test");
    }
    
    /**
     * Gets a meter registry for metrics testing.
     *
     * @return A meter registry instance
     */
    protected MeterRegistry getMeterRegistry() {
        return METER_REGISTRY;
    }
    
    /**
     * Creates a circuit breaker for resilience testing.
     *
     * @param name The name of the circuit breaker
     * @return A configured circuit breaker instance
     */
    protected CircuitBreaker createCircuitBreaker(String name) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(10)
                .minimumNumberOfCalls(5)
                .waitDurationInOpenState(Duration.ofMillis(1000))
                .permittedNumberOfCallsInHalfOpenState(3)
                .build();
        return CircuitBreaker.of(name, config);
    }
    
    /**
     * Creates a retry mechanism for resilience testing.
     *
     * @param name The name of the retry
     * @return A configured retry instance
     */
    protected Retry createRetry(String name) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(100))
                .retryExceptions(Exception.class)
                .build();
        return Retry.of(name, config);
    }
    
    /**
     * Creates a time limiter for resilience testing.
     *
     * @param name The name of the time limiter
     * @return A configured time limiter instance
     */
    protected TimeLimiter createTimeLimiter(String name) {
        TimeLimiterConfig config = TimeLimiterConfig.custom()
                .timeoutDuration(Duration.ofMillis(500))
                .cancelRunningFuture(true)
                .build();
        return TimeLimiter.of(name, config);
    }
    
    /**
     * Sets up a test environment with Testcontainers.
     * This is a base implementation that can be extended in subclasses.
     */
    @Testcontainers
    public static class TestEnvironment {
        
        @Container
        public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:14-alpine")
                .withDatabaseName("traccar_test")
                .withUsername("test")
                .withPassword("test");
        
        @Container
        public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.0"));
        
        @Container
        public static RabbitMQContainer rabbitmq = new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.12-management"));
        
        @Container
        public static GenericContainer<?> consul = new GenericContainer<>(DockerImageName.parse("consul:1.15"))
                .withExposedPorts(8500);
        
        /**
         * Gets the JDBC URL for the PostgreSQL container.
         *
         * @return The JDBC URL
         */
        public static String getJdbcUrl() {
            return postgres.getJdbcUrl();
        }
        
        /**
         * Gets the bootstrap servers string for the Kafka container.
         *
         * @return The bootstrap servers string
         */
        public static String getKafkaBootstrapServers() {
            return kafka.getBootstrapServers();
        }
        
        /**
         * Gets the AMQP URL for the RabbitMQ container.
         *
         * @return The AMQP URL
         */
        public static String getRabbitMqUrl() {
            return rabbitmq.getAmqpUrl();
        }
        
        /**
         * Gets the Consul URL.
         *
         * @return The Consul URL
         */
        public static String getConsulUrl() {
            return String.format("http://%s:%d", consul.getHost(), consul.getMappedPort(8500));
        }
    }
    
    /**
     * Executes a function with resilience patterns applied (circuit breaker, retry, timeout).
     *
     * @param supplier The function to execute
     * @param <T> The return type of the function
     * @return The result of the function execution
     */
    protected <T> T executeWithResilience(java.util.function.Supplier<T> supplier) {
        CircuitBreaker circuitBreaker = createCircuitBreaker("test-circuit-breaker");
        Retry retry = createRetry("test-retry");
        TimeLimiter timeLimiter = createTimeLimiter("test-time-limiter");
        
        java.util.function.Supplier<CompletableFuture<T>> futureSupplier = 
                () -> CompletableFuture.supplyAsync(supplier);
        
        java.util.function.Supplier<CompletableFuture<T>> decoratedSupplier = 
                TimeLimiter.decorateFutureSupplier(timeLimiter, futureSupplier);
        
        try {
            return circuitBreaker.executeSupplier(
                    Retry.decorateSupplier(retry, () -> {
                        try {
                            return decoratedSupplier.get().get();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute with resilience patterns", e);
        }
    }
    
    /**
     * Resets all mocked clients to their initial state.
     * Useful for cleaning up between tests.
     */
    protected void resetMocks() {
        mockedClients.values().forEach(Mockito::reset);
    }
    
    /**
     * Clears all mocked clients.
     * Useful for cleaning up after tests.
     */
    protected void clearMocks() {
        mockedClients.clear();
    }
}