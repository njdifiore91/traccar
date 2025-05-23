package org.traccar.handler.events;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.traccar.BaseTest;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.test.MessageBrokerTestUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class CommandResultEventHandlerTest extends BaseTest {

    private static final String TEST_RESULT = "Test Result";
    private static final String MONOLITHIC_MODE = "traccar.test.mode.monolithic";
    private static final String KAFKA_MODE = "traccar.test.mode.kafka";
    private static final String RABBITMQ_MODE = "traccar.test.mode.rabbitmq";
    
    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.3.0"))
            .withLabel("test-resource", "CommandResultEventHandlerTest");
    
    @Container
    private static final RabbitMQContainer rabbitMQContainer = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:3.12.12-management"))
            .withLabel("test-resource", "CommandResultEventHandlerTest");
    
    private CommandResultEventHandler commandResultEventHandler;
    private MessageBrokerTestUtil messageBrokerTestUtil;
    
    @BeforeEach
    public void setUp() {
        commandResultEventHandler = new CommandResultEventHandler();
        
        // Initialize message broker test utility if running in broker mode
        if (System.getProperty(KAFKA_MODE) != null) {
            messageBrokerTestUtil = MessageBrokerTestUtil.createKafkaInstance(
                    kafkaContainer.getBootstrapServers());
        } else if (System.getProperty(RABBITMQ_MODE) != null) {
            messageBrokerTestUtil = MessageBrokerTestUtil.createRabbitMQInstance(
                    rabbitMQContainer.getAmqpUrl());
        }
    }

    /**
     * Original test method for backward compatibility with monolithic mode
     */
    @Test
    @EnabledIfSystemProperty(named = MONOLITHIC_MODE, matches = "true|yes|y|1")
    public void testCommandResultEventHandler() throws Exception {
        Position position = new Position();
        position.set(Position.KEY_RESULT, TEST_RESULT);
        List<Event> events = new ArrayList<>();
        commandResultEventHandler.analyzePosition(position, events::add);
        assertFalse(events.isEmpty());
        Event event = events.iterator().next();
        assertEquals(Event.TYPE_COMMAND_RESULT, event.getType());
    }
    
    /**
     * Test method for Kafka message broker integration
     */
    @Test
    @EnabledIfSystemProperty(named = KAFKA_MODE, matches = "true|yes|y|1")
    public void testCommandResultEventHandlerWithKafka() throws Exception {
        // Setup test position with command result
        Position position = new Position();
        position.set(Position.KEY_RESULT, TEST_RESULT);
        position.setDeviceId(1L);
        
        // Setup message broker consumer to capture events
        CompletableFuture<Event> eventFuture = new CompletableFuture<>();
        messageBrokerTestUtil.subscribeToEvents(event -> {
            if (Event.TYPE_COMMAND_RESULT.equals(event.getType())) {
                eventFuture.complete(event);
            }
        });
        
        // Process position through handler with broker integration
        messageBrokerTestUtil.publishPosition(position);
        
        // Wait for and verify the event
        Event event = eventFuture.get(10, TimeUnit.SECONDS);
        assertNotNull(event);
        assertEquals(Event.TYPE_COMMAND_RESULT, event.getType());
        assertEquals(1L, event.getDeviceId());
    }
    
    /**
     * Test method for RabbitMQ message broker integration
     */
    @Test
    @EnabledIfSystemProperty(named = RABBITMQ_MODE, matches = "true|yes|y|1")
    public void testCommandResultEventHandlerWithRabbitMQ() throws Exception {
        // Setup test position with command result
        Position position = new Position();
        position.set(Position.KEY_RESULT, TEST_RESULT);
        position.setDeviceId(2L);
        
        // Setup message broker consumer to capture events
        CompletableFuture<Event> eventFuture = new CompletableFuture<>();
        messageBrokerTestUtil.subscribeToEvents(event -> {
            if (Event.TYPE_COMMAND_RESULT.equals(event.getType())) {
                eventFuture.complete(event);
            }
        });
        
        // Process position through handler with broker integration
        messageBrokerTestUtil.publishPosition(position);
        
        // Wait for and verify the event
        Event event = eventFuture.get(10, TimeUnit.SECONDS);
        assertNotNull(event);
        assertEquals(Event.TYPE_COMMAND_RESULT, event.getType());
        assertEquals(2L, event.getDeviceId());
    }
    
    /**
     * Test asynchronous event processing with message broker
     */
    @Test
    @EnabledIfSystemProperty(named = KAFKA_MODE + "|" + RABBITMQ_MODE, matches = "true|yes|y|1")
    public void testAsynchronousEventProcessing() throws Exception {
        // Setup multiple test positions with command results
        Position position1 = new Position();
        position1.set(Position.KEY_RESULT, TEST_RESULT + " 1");
        position1.setDeviceId(3L);
        
        Position position2 = new Position();
        position2.set(Position.KEY_RESULT, TEST_RESULT + " 2");
        position2.setDeviceId(4L);
        
        // Setup message broker consumer to capture events
        List<Event> capturedEvents = new ArrayList<>();
        CompletableFuture<Boolean> eventsFuture = new CompletableFuture<>();
        
        messageBrokerTestUtil.subscribeToEvents(event -> {
            if (Event.TYPE_COMMAND_RESULT.equals(event.getType())) {
                synchronized (capturedEvents) {
                    capturedEvents.add(event);
                    if (capturedEvents.size() >= 2) {
                        eventsFuture.complete(true);
                    }
                }
            }
        });
        
        // Process positions through handler with broker integration
        messageBrokerTestUtil.publishPosition(position1);
        messageBrokerTestUtil.publishPosition(position2);
        
        // Wait for and verify the events
        assertTrue(eventsFuture.get(10, TimeUnit.SECONDS));
        assertEquals(2, capturedEvents.size());
        
        // Verify both events were processed correctly
        boolean foundDevice3 = false;
        boolean foundDevice4 = false;
        
        for (Event event : capturedEvents) {
            assertEquals(Event.TYPE_COMMAND_RESULT, event.getType());
            if (event.getDeviceId() == 3L) {
                foundDevice3 = true;
            } else if (event.getDeviceId() == 4L) {
                foundDevice4 = true;
            }
        }
        
        assertTrue(foundDevice3, "Event for device 3 not found");
        assertTrue(foundDevice4, "Event for device 4 not found");
    }
}