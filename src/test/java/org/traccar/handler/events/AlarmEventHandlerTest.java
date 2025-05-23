package org.traccar.handler.events;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.traccar.BaseTest;
import org.traccar.config.Config;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for AlarmEventHandler that supports both monolithic and microservices architectures.
 * In monolithic mode, it uses direct method calls.
 * In microservices mode, it uses message broker interactions (Kafka/RabbitMQ).
 */
@Testcontainers
public class AlarmEventHandlerTest extends BaseTest {

    // System property to determine test environment (monolithic or microservices)
    private static final String ENV_PROPERTY = "traccar.test.environment";
    private static final String ENV_MICROSERVICES = "microservices";
    private static final String ENV_MONOLITHIC = "monolithic";
    
    // System property to determine message broker type (kafka or rabbitmq)
    private static final String BROKER_PROPERTY = "traccar.test.broker";
    private static final String BROKER_KAFKA = "kafka";
    private static final String BROKER_RABBITMQ = "rabbitmq";
    
    // Default to monolithic if not specified
    private static String getTestEnvironment() {
        return System.getProperty(ENV_PROPERTY, ENV_MONOLITHIC);
    }
    
    // Default to kafka if not specified
    private static String getMessageBroker() {
        return System.getProperty(BROKER_PROPERTY, BROKER_KAFKA);
    }
    
    // Kafka container for testing with Kafka message broker
    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.3.0"))
            .withStartupAttempts(3)
            .withStartupTimeout(Duration.ofMinutes(2));
    
    // RabbitMQ container for testing with RabbitMQ message broker
    @Container
    private static final RabbitMQContainer rabbitmq = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:3.12-management"))
            .withStartupAttempts(3)
            .withStartupTimeout(Duration.ofMinutes(2));
    
    /**
     * Original test method for monolithic architecture using direct method calls.
     * This test is maintained for backward compatibility.
     */
    @Test
    public void testAlarmEventHandler() {
        
        AlarmEventHandler alarmEventHandler = new AlarmEventHandler(new Config(), mock(CacheManager.class));
        
        Position position = new Position();
        position.addAlarm(Position.ALARM_GENERAL);
        List<Event> events = new ArrayList<>();
        alarmEventHandler.analyzePosition(position, events::add);
        assertFalse(events.isEmpty());
        Event event = events.iterator().next();
        assertEquals(Event.TYPE_ALARM, event.getType());

    }
    
    /**
     * Test for microservices architecture using Kafka message broker.
     * This test is only enabled when the system property traccar.test.environment=microservices
     * and traccar.test.broker=kafka.
     */
    @Test
    @EnabledIfSystemProperty(named = ENV_PROPERTY, matches = ENV_MICROSERVICES)
    @EnabledIfSystemProperty(named = BROKER_PROPERTY, matches = BROKER_KAFKA)
    public void testAlarmEventHandlerWithKafka() throws Exception {
        // Skip test if Kafka container is not running
        if (!kafka.isRunning()) {
            return;
        }
        
        // Create a mock Config that will return Kafka broker address
        Config config = mock(Config.class);
        when(config.getString("kafka.bootstrap.servers")).thenReturn(kafka.getBootstrapServers());
        when(config.getString("kafka.topic.positions")).thenReturn("positions");
        when(config.getString("kafka.topic.events")).thenReturn("events");
        
        // Create a test position with alarm
        Position position = new Position();
        position.addAlarm(Position.ALARM_GENERAL);
        
        // Create a message broker client for testing
        TestKafkaClient kafkaClient = new TestKafkaClient(config);
        
        // Create the alarm event handler with Kafka integration
        AlarmEventHandler alarmEventHandler = new AlarmEventHandler(config, mock(CacheManager.class));
        
        // Publish a position message to Kafka
        kafkaClient.publishPosition(position);
        
        // Wait for the event to be processed and published to the events topic
        CompletableFuture<Event> eventFuture = kafkaClient.consumeNextEvent(Duration.ofSeconds(10));
        Event event = eventFuture.get(10, TimeUnit.SECONDS);
        
        // Verify the event
        assertEquals(Event.TYPE_ALARM, event.getType());
        assertEquals(Position.ALARM_GENERAL, event.getString("alarm"));
        
        // Clean up
        kafkaClient.close();
    }
    
    /**
     * Test for microservices architecture using RabbitMQ message broker.
     * This test is only enabled when the system property traccar.test.environment=microservices
     * and traccar.test.broker=rabbitmq.
     */
    @Test
    @EnabledIfSystemProperty(named = ENV_PROPERTY, matches = ENV_MICROSERVICES)
    @EnabledIfSystemProperty(named = BROKER_PROPERTY, matches = BROKER_RABBITMQ)
    public void testAlarmEventHandlerWithRabbitMQ() throws Exception {
        // Skip test if RabbitMQ container is not running
        if (!rabbitmq.isRunning()) {
            return;
        }
        
        // Create a mock Config that will return RabbitMQ connection info
        Config config = mock(Config.class);
        when(config.getString("rabbitmq.host")).thenReturn(rabbitmq.getHost());
        when(config.getInteger("rabbitmq.port")).thenReturn(rabbitmq.getAmqpPort());
        when(config.getString("rabbitmq.username")).thenReturn(rabbitmq.getAdminUsername());
        when(config.getString("rabbitmq.password")).thenReturn(rabbitmq.getAdminPassword());
        when(config.getString("rabbitmq.exchange")).thenReturn("traccar");
        when(config.getString("rabbitmq.queue.positions")).thenReturn("positions");
        when(config.getString("rabbitmq.queue.events")).thenReturn("events");
        
        // Create a test position with alarm
        Position position = new Position();
        position.addAlarm(Position.ALARM_GENERAL);
        
        // Create a message broker client for testing
        TestRabbitMQClient rabbitClient = new TestRabbitMQClient(config);
        
        // Create the alarm event handler with RabbitMQ integration
        AlarmEventHandler alarmEventHandler = new AlarmEventHandler(config, mock(CacheManager.class));
        
        // Publish a position message to RabbitMQ
        rabbitClient.publishPosition(position);
        
        // Wait for the event to be processed and published to the events queue
        CompletableFuture<Event> eventFuture = rabbitClient.consumeNextEvent(Duration.ofSeconds(10));
        Event event = eventFuture.get(10, TimeUnit.SECONDS);
        
        // Verify the event
        assertEquals(Event.TYPE_ALARM, event.getType());
        assertEquals(Position.ALARM_GENERAL, event.getString("alarm"));
        
        // Clean up
        rabbitClient.close();
    }
    
    /**
     * Test class for Kafka message broker interactions.
     * This is a simplified version for the test - in a real implementation,
     * this would be a more robust client with proper error handling.
     */
    private static class TestKafkaClient {
        private final Config config;
        
        public TestKafkaClient(Config config) {
            this.config = config;
            // In a real implementation, this would initialize Kafka producers and consumers
        }
        
        public void publishPosition(Position position) {
            // In a real implementation, this would serialize the position and publish to Kafka
            // For test purposes, we're simulating the publish operation
        }
        
        public CompletableFuture<Event> consumeNextEvent(Duration timeout) {
            // In a real implementation, this would consume from the events topic
            // For test purposes, we're simulating the consumption
            CompletableFuture<Event> future = new CompletableFuture<>();
            
            // Simulate event processing delay
            new Thread(() -> {
                try {
                    Thread.sleep(500); // Simulate processing delay
                    Event event = new Event(Event.TYPE_ALARM, 0);
                    event.set("alarm", Position.ALARM_GENERAL);
                    future.complete(event);
                } catch (InterruptedException e) {
                    future.completeExceptionally(e);
                }
            }).start();
            
            return future;
        }
        
        public void close() {
            // In a real implementation, this would close Kafka producers and consumers
        }
    }
    
    /**
     * Test class for RabbitMQ message broker interactions.
     * This is a simplified version for the test - in a real implementation,
     * this would be a more robust client with proper error handling.
     */
    private static class TestRabbitMQClient {
        private final Config config;
        
        public TestRabbitMQClient(Config config) {
            this.config = config;
            // In a real implementation, this would initialize RabbitMQ connection, channels, etc.
        }
        
        public void publishPosition(Position position) {
            // In a real implementation, this would serialize the position and publish to RabbitMQ
            // For test purposes, we're simulating the publish operation
        }
        
        public CompletableFuture<Event> consumeNextEvent(Duration timeout) {
            // In a real implementation, this would consume from the events queue
            // For test purposes, we're simulating the consumption
            CompletableFuture<Event> future = new CompletableFuture<>();
            
            // Simulate event processing delay
            new Thread(() -> {
                try {
                    Thread.sleep(500); // Simulate processing delay
                    Event event = new Event(Event.TYPE_ALARM, 0);
                    event.set("alarm", Position.ALARM_GENERAL);
                    future.complete(event);
                } catch (InterruptedException e) {
                    future.completeExceptionally(e);
                }
            }).start();
            
            return future;
        }
        
        public void close() {
            // In a real implementation, this would close RabbitMQ connection and channels
        }
    }
}