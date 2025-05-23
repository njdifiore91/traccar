package org.traccar;

import io.netty.channel.Channel;
import org.mockito.Mockito;
import org.traccar.config.Config;
import org.traccar.model.Device;
import org.traccar.session.ConnectionManager;
import org.traccar.session.DeviceSession;
import org.traccar.session.cache.CacheManager;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.kafka.KafkaProducer;
import org.traccar.messaging.rabbitmq.RabbitMQProducer;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ConsulServiceDiscovery;
import org.traccar.discovery.KubernetesServiceDiscovery;
import org.traccar.metrics.MetricsManager;
import org.traccar.health.HealthCheckManager;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.testing.GrpcCleanupRule;

import java.net.SocketAddress;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base test class for protocol-service tests.
 * Provides utilities for mocking dependencies and setting up test environments.
 */
public class BaseTest {

    /**
     * Injects dependencies into a protocol decoder.
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
        
        // Set up message producer for position publishing
        var messageProducer = mockMessageProducer();
        decoder.setMessageProducer(messageProducer);
        
        // Set up service discovery
        var serviceDiscovery = mockServiceDiscovery();
        decoder.setServiceDiscovery(serviceDiscovery);
        
        // Set up metrics manager
        var metricsManager = mock(MetricsManager.class);
        decoder.setMetricsManager(metricsManager);
        
        // Set up health check manager
        var healthCheckManager = mock(HealthCheckManager.class);
        decoder.setHealthCheckManager(healthCheckManager);
        
        return decoder;
    }

    /**
     * Injects dependencies into a frame decoder.
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
        
        // Set up message producer for command responses
        var messageProducer = mockMessageProducer();
        encoder.setMessageProducer(messageProducer);
        
        // Set up service discovery
        var serviceDiscovery = mockServiceDiscovery();
        encoder.setServiceDiscovery(serviceDiscovery);
        
        return encoder;
    }
    
    /**
     * Creates a mock message producer for testing.
     * 
     * @return A mocked MessageProducer instance
     */
    protected MessageProducer mockMessageProducer() {
        var messageProducer = mock(MessageProducer.class);
        // Configure default behavior for the message producer
        when(messageProducer.send(any(), any())).thenReturn(true);
        return messageProducer;
    }
    
    /**
     * Creates a mock Kafka producer for testing.
     * 
     * @return A mocked KafkaProducer instance
     */
    protected KafkaProducer mockKafkaProducer() {
        var kafkaProducer = mock(KafkaProducer.class);
        // Configure default behavior for the Kafka producer
        when(kafkaProducer.send(any(), any())).thenReturn(true);
        return kafkaProducer;
    }
    
    /**
     * Creates a mock RabbitMQ producer for testing.
     * 
     * @return A mocked RabbitMQProducer instance
     */
    protected RabbitMQProducer mockRabbitMQProducer() {
        var rabbitMQProducer = mock(RabbitMQProducer.class);
        // Configure default behavior for the RabbitMQ producer
        when(rabbitMQProducer.send(any(), any())).thenReturn(true);
        return rabbitMQProducer;
    }
    
    /**
     * Creates a mock service discovery client for testing.
     * 
     * @return A mocked ServiceDiscovery instance
     */
    protected ServiceDiscovery mockServiceDiscovery() {
        var serviceDiscovery = mock(ServiceDiscovery.class);
        // Configure default behavior for service discovery
        when(serviceDiscovery.getServiceUrl(any())).thenReturn("http://localhost:8080");
        return serviceDiscovery;
    }
    
    /**
     * Creates a mock Consul service discovery client for testing.
     * 
     * @return A mocked ConsulServiceDiscovery instance
     */
    protected ConsulServiceDiscovery mockConsulServiceDiscovery() {
        var consulServiceDiscovery = mock(ConsulServiceDiscovery.class);
        // Configure default behavior for Consul service discovery
        when(consulServiceDiscovery.getServiceUrl(any())).thenReturn("http://localhost:8080");
        return consulServiceDiscovery;
    }
    
    /**
     * Creates a mock Kubernetes service discovery client for testing.
     * 
     * @return A mocked KubernetesServiceDiscovery instance
     */
    protected KubernetesServiceDiscovery mockKubernetesServiceDiscovery() {
        var k8sServiceDiscovery = mock(KubernetesServiceDiscovery.class);
        // Configure default behavior for Kubernetes service discovery
        when(k8sServiceDiscovery.getServiceUrl(any())).thenReturn("http://localhost:8080");
        return k8sServiceDiscovery;
    }
    
    /**
     * Creates a mock gRPC channel for testing.
     * Uses in-process channel for fast and reliable testing.
     * 
     * @param serverName The name of the in-process server
     * @return A ManagedChannel instance for gRPC testing
     */
    protected ManagedChannel createTestChannel(String serverName) {
        return InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();
    }
    
    /**
     * Creates a gRPC server builder for testing.
     * Uses in-process server for fast and reliable testing.
     * 
     * @param serverName The name of the in-process server
     * @return An InProcessServerBuilder instance for gRPC testing
     */
    protected InProcessServerBuilder createTestServerBuilder(String serverName) {
        return InProcessServerBuilder.forName(serverName)
                .directExecutor();
    }
    
    /**
     * Creates a mock gRPC client stub for testing.
     * 
     * @param <T> The type of the gRPC stub
     * @param stubClass The class of the gRPC stub
     * @return A mocked gRPC stub instance
     */
    protected <T extends AbstractStub<T>> T mockGrpcStub(Class<T> stubClass) {
        return Mockito.mock(stubClass);
    }
    
    /**
     * Shuts down a gRPC channel gracefully.
     * 
     * @param channel The channel to shut down
     * @throws InterruptedException If the shutdown is interrupted
     */
    protected void shutdownChannel(ManagedChannel channel) throws InterruptedException {
        channel.shutdown();
        if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
            channel.shutdownNow();
        }
    }
}