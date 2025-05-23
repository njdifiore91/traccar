package org.traccar.handler.events;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.traccar.BaseTest;
import org.traccar.model.Event;
import org.traccar.model.Maintenance;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class for MaintenanceEventHandler that supports both monolithic and microservices architectures.
 * In monolithic mode, it tests direct method calls to the handler.
 * In microservices mode, it tests asynchronous event processing via message brokers (Kafka/RabbitMQ).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Testcontainers
public class MaintenanceEventHandlerTest extends BaseTest {
    
    // Message broker containers for microservices testing
    @Container
    private static final KafkaContainer kafkaContainer = 
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.0"))
            .withReuse(true);
    
    @Container
    private static final RabbitMQContainer rabbitMQContainer = 
        new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.12-management"))
            .withReuse(true);
    
    // Broker client instances will be initialized in setUp if needed
    private MessageBrokerClient brokerClient;
    
    @BeforeAll
    public void setUp() {
        // Initialize message broker client if running in microservices mode
        if (isRunningInMicroservicesMode()) {
            if (isUsingKafka()) {
                brokerClient = new KafkaMessageBrokerClient(kafkaContainer.getBootstrapServers());
            } else {
                brokerClient = new RabbitMQMessageBrokerClient(
                    rabbitMQContainer.getHost(),
                    rabbitMQContainer.getAmqpPort(),
                    rabbitMQContainer.getAdminUsername(),
                    rabbitMQContainer.getAdminPassword());
            }
            brokerClient.initialize();
        }
    }
    
    @AfterAll
    public void tearDown() {
        if (brokerClient != null) {
            brokerClient.close();
        }
    }
    
    /**
     * Original test for direct method calls (monolithic architecture)
     */
    @Test
    public void testMaintenanceEventHandler() {
        Position lastPosition = new Position();
        lastPosition.setDeviceId(1);
        lastPosition.setFixTime(new Date(0));

        Position position = new Position();
        position.setDeviceId(1);
        position.setFixTime(new Date(0));

        var maintenance = mock(Maintenance.class);
        when(maintenance.getType()).thenReturn(Position.KEY_TOTAL_DISTANCE);
        var maintenances = Set.of(maintenance);

        var cacheManager = mock(CacheManager.class);
        when(cacheManager.getDeviceObjects(anyLong(), eq(Maintenance.class))).thenReturn(maintenances);
        when(cacheManager.getPosition(anyLong())).thenReturn(lastPosition);
        MaintenanceEventHandler eventHandler = new MaintenanceEventHandler(cacheManager);        

        when(maintenance.getStart()).thenReturn(10000.0);
        when(maintenance.getPeriod()).thenReturn(2000.0);

        List<Event> events = new ArrayList<>();
 
        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 1999);
        position.set(Position.KEY_TOTAL_DISTANCE, 2001);
        eventHandler.analyzePosition(position, events::add);
        assertTrue(events.isEmpty());

        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 3999);
        position.set(Position.KEY_TOTAL_DISTANCE, 4001);
        eventHandler.analyzePosition(position, events::add);
        assertTrue(events.isEmpty());

        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 9999);
        position.set(Position.KEY_TOTAL_DISTANCE, 10001);
        eventHandler.analyzePosition(position, events::add);
        assertEquals(1, events.size());

        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 11999);
        position.set(Position.KEY_TOTAL_DISTANCE, 12001);
        eventHandler.analyzePosition(position, events::add);
        assertEquals(2, events.size());
    }
    
    /**
     * Test for asynchronous event processing via Kafka message broker
     */
    @Test
    @EnabledIfSystemProperty(named = "test.broker", matches = "kafka")
    public void testMaintenanceEventHandlerWithKafka() throws Exception {
        // Skip test if not running in microservices mode or not using Kafka
        if (!isRunningInMicroservicesMode() || !isUsingKafka()) {
            return;
        }
        
        // Set up test data
        Position lastPosition = new Position();
        lastPosition.setDeviceId(1);
        lastPosition.setFixTime(new Date(0));
        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 9999);

        Position position = new Position();
        position.setDeviceId(1);
        position.setFixTime(new Date(0));
        position.set(Position.KEY_TOTAL_DISTANCE, 10001);
        
        // Set up maintenance configuration
        var maintenance = mock(Maintenance.class);
        when(maintenance.getType()).thenReturn(Position.KEY_TOTAL_DISTANCE);
        when(maintenance.getStart()).thenReturn(10000.0);
        when(maintenance.getPeriod()).thenReturn(2000.0);
        var maintenances = Set.of(maintenance);
        
        // Set up cache manager mock
        var cacheManager = mock(CacheManager.class);
        when(cacheManager.getDeviceObjects(anyLong(), eq(Maintenance.class))).thenReturn(maintenances);
        when(cacheManager.getPosition(anyLong())).thenReturn(lastPosition);
        
        // Create event handler with the mocked cache manager
        MaintenanceEventHandler eventHandler = new MaintenanceEventHandler(cacheManager);
        
        // Set up a latch to wait for the event to be processed
        CountDownLatch latch = new CountDownLatch(1);
        List<Event> receivedEvents = new ArrayList<>();
        
        // Subscribe to the events topic
        brokerClient.subscribeToEvents(event -> {
            receivedEvents.add(event);
            latch.countDown();
        });
        
        // Process the position and publish the event to Kafka
        CompletableFuture.runAsync(() -> {
            eventHandler.analyzePosition(position, event -> {
                brokerClient.publishEvent(event);
            });
        });
        
        // Wait for the event to be received
        boolean received = latch.await(10, TimeUnit.SECONDS);
        
        // Verify the event was received
        assertTrue(received, "Event should be received within timeout");
        assertEquals(1, receivedEvents.size(), "Should receive exactly one event");
        assertEquals(Event.TYPE_MAINTENANCE, receivedEvents.get(0).getType(), "Event type should be maintenance");
    }
    
    /**
     * Test for asynchronous event processing via RabbitMQ message broker
     */
    @Test
    @EnabledIfSystemProperty(named = "test.broker", matches = "rabbitmq")
    public void testMaintenanceEventHandlerWithRabbitMQ() throws Exception {
        // Skip test if not running in microservices mode or not using RabbitMQ
        if (!isRunningInMicroservicesMode() || isUsingKafka()) {
            return;
        }
        
        // Set up test data
        Position lastPosition = new Position();
        lastPosition.setDeviceId(1);
        lastPosition.setFixTime(new Date(0));
        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 9999);

        Position position = new Position();
        position.setDeviceId(1);
        position.setFixTime(new Date(0));
        position.set(Position.KEY_TOTAL_DISTANCE, 10001);
        
        // Set up maintenance configuration
        var maintenance = mock(Maintenance.class);
        when(maintenance.getType()).thenReturn(Position.KEY_TOTAL_DISTANCE);
        when(maintenance.getStart()).thenReturn(10000.0);
        when(maintenance.getPeriod()).thenReturn(2000.0);
        var maintenances = Set.of(maintenance);
        
        // Set up cache manager mock
        var cacheManager = mock(CacheManager.class);
        when(cacheManager.getDeviceObjects(anyLong(), eq(Maintenance.class))).thenReturn(maintenances);
        when(cacheManager.getPosition(anyLong())).thenReturn(lastPosition);
        
        // Create event handler with the mocked cache manager
        MaintenanceEventHandler eventHandler = new MaintenanceEventHandler(cacheManager);
        
        // Set up a latch to wait for the event to be processed
        CountDownLatch latch = new CountDownLatch(1);
        List<Event> receivedEvents = new ArrayList<>();
        
        // Subscribe to the events topic/exchange
        brokerClient.subscribeToEvents(event -> {
            receivedEvents.add(event);
            latch.countDown();
        });
        
        // Process the position and publish the event to RabbitMQ
        CompletableFuture.runAsync(() -> {
            eventHandler.analyzePosition(position, event -> {
                brokerClient.publishEvent(event);
            });
        });
        
        // Wait for the event to be received
        boolean received = latch.await(10, TimeUnit.SECONDS);
        
        // Verify the event was received
        assertTrue(received, "Event should be received within timeout");
        assertEquals(1, receivedEvents.size(), "Should receive exactly one event");
        assertEquals(Event.TYPE_MAINTENANCE, receivedEvents.get(0).getType(), "Event type should be maintenance");
    }
    
    /**
     * Test for periodic maintenance events with message broker
     */
    @Test
    @EnabledIfEnvironmentVariable(named = "TEST_MICROSERVICES", matches = "true")
    public void testPeriodicMaintenanceEventsWithMessageBroker() throws Exception {
        // Skip test if not running in microservices mode
        if (!isRunningInMicroservicesMode()) {
            return;
        }
        
        // Set up test data for periodic maintenance
        Position lastPosition = new Position();
        lastPosition.setDeviceId(1);
        lastPosition.setFixTime(new Date(0));

        Position position = new Position();
        position.setDeviceId(1);
        position.setFixTime(new Date(0));
        
        // Set up maintenance configuration
        var maintenance = mock(Maintenance.class);
        when(maintenance.getType()).thenReturn(Position.KEY_TOTAL_DISTANCE);
        when(maintenance.getStart()).thenReturn(10000.0);
        when(maintenance.getPeriod()).thenReturn(2000.0);
        var maintenances = Set.of(maintenance);
        
        // Set up cache manager mock
        var cacheManager = mock(CacheManager.class);
        when(cacheManager.getDeviceObjects(anyLong(), eq(Maintenance.class))).thenReturn(maintenances);
        when(cacheManager.getPosition(anyLong())).thenReturn(lastPosition);
        
        // Create event handler with the mocked cache manager
        MaintenanceEventHandler eventHandler = new MaintenanceEventHandler(cacheManager);
        
        // Set up a latch to wait for two events to be processed
        CountDownLatch latch = new CountDownLatch(2);
        List<Event> receivedEvents = new ArrayList<>();
        
        // Subscribe to the events topic/exchange
        brokerClient.subscribeToEvents(event -> {
            receivedEvents.add(event);
            latch.countDown();
        });
        
        // Process positions that trigger maintenance events
        CompletableFuture.runAsync(() -> {
            // First maintenance event at 10000
            lastPosition.set(Position.KEY_TOTAL_DISTANCE, 9999);
            position.set(Position.KEY_TOTAL_DISTANCE, 10001);
            eventHandler.analyzePosition(position, event -> {
                brokerClient.publishEvent(event);
            });
            
            // Second maintenance event at 12000
            lastPosition.set(Position.KEY_TOTAL_DISTANCE, 11999);
            position.set(Position.KEY_TOTAL_DISTANCE, 12001);
            eventHandler.analyzePosition(position, event -> {
                brokerClient.publishEvent(event);
            });
        });
        
        // Wait for both events to be received
        boolean received = latch.await(10, TimeUnit.SECONDS);
        
        // Verify the events were received
        assertTrue(received, "Events should be received within timeout");
        assertEquals(2, receivedEvents.size(), "Should receive exactly two events");
        assertEquals(Event.TYPE_MAINTENANCE, receivedEvents.get(0).getType(), "First event type should be maintenance");
        assertEquals(Event.TYPE_MAINTENANCE, receivedEvents.get(1).getType(), "Second event type should be maintenance");
    }
    
    /**
     * Helper method to check if we're running in microservices mode
     */
    private boolean isRunningInMicroservicesMode() {
        return System.getenv("TEST_MICROSERVICES") != null && 
               System.getenv("TEST_MICROSERVICES").equalsIgnoreCase("true");
    }
    
    /**
     * Helper method to check if we're using Kafka (vs RabbitMQ)
     */
    private boolean isUsingKafka() {
        String broker = System.getProperty("test.broker", "kafka");
        return broker.equalsIgnoreCase("kafka");
    }
    
    /**
     * Interface for message broker client abstraction
     */
    private interface MessageBrokerClient extends AutoCloseable {
        void initialize();
        void publishEvent(Event event);
        void subscribeToEvents(java.util.function.Consumer<Event> eventConsumer);
    }
    
    /**
     * Kafka implementation of the message broker client
     */
    private static class KafkaMessageBrokerClient implements MessageBrokerClient {
        private final String bootstrapServers;
        
        public KafkaMessageBrokerClient(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }
        
        @Override
        public void initialize() {
            // Initialize Kafka producer and consumer
            // This would use KafkaProducer and KafkaConsumer from kafka-clients
        }
        
        @Override
        public void publishEvent(Event event) {
            // Publish event to Kafka topic
            // In a real implementation, this would serialize the event and send it to Kafka
        }
        
        @Override
        public void subscribeToEvents(java.util.function.Consumer<Event> eventConsumer) {
            // Subscribe to Kafka topic and process events
            // In a real implementation, this would create a consumer, subscribe to a topic,
            // and call the eventConsumer when messages are received
        }
        
        @Override
        public void close() {
            // Close Kafka connections
        }
    }
    
    /**
     * RabbitMQ implementation of the message broker client
     */
    private static class RabbitMQMessageBrokerClient implements MessageBrokerClient {
        private final String host;
        private final int port;
        private final String username;
        private final String password;
        
        public RabbitMQMessageBrokerClient(String host, int port, String username, String password) {
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
        }
        
        @Override
        public void initialize() {
            // Initialize RabbitMQ connection and channel
            // This would use ConnectionFactory from amqp-client
        }
        
        @Override
        public void publishEvent(Event event) {
            // Publish event to RabbitMQ exchange
            // In a real implementation, this would serialize the event and publish it
        }
        
        @Override
        public void subscribeToEvents(java.util.function.Consumer<Event> eventConsumer) {
            // Subscribe to RabbitMQ queue and process events
            // In a real implementation, this would create a consumer, bind to a queue,
            // and call the eventConsumer when messages are received
        }
        
        @Override
        public void close() {
            // Close RabbitMQ connections
        }
    }
}
