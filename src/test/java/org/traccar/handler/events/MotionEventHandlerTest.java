package org.traccar.handler.events;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.condition.DisabledIf;
import org.traccar.BaseTest;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.reports.common.TripsConfig;
import org.traccar.session.state.MotionProcessor;
import org.traccar.session.state.MotionState;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

// Kafka imports
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

// RabbitMQ imports
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.AMQP;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for MotionEventHandler that supports both monolithic and microservices testing environments.
 * This class has been updated to support testing with message brokers (Kafka/RabbitMQ).
 */
public class MotionEventHandlerTest extends BaseTest {

    // Common constants
    private static final String MOTION_EVENTS_TOPIC = "motion-events";
    private static final String POSITION_TOPIC = "positions";
    private static final String CONSUMER_GROUP_ID = "motion-event-test-group";
    
    // Kafka related fields
    private KafkaProducer<String, String> kafkaProducer;
    private KafkaConsumer<String, String> kafkaConsumer;
    
    // RabbitMQ related fields
    private Connection rabbitConnection;
    private Channel rabbitChannel;
    
    private TripsConfig tripsConfig;
    private String messageBrokerType; // "kafka" or "rabbitmq"
    
    @BeforeEach
    public void setUp() {
        tripsConfig = new TripsConfig(500, 300000, 300000, 0, false, false);
        
        // Setup message broker only in microservices environment
        if (isMicroservicesEnvironment()) {
            // Determine which message broker to use based on system property
            // Default to Kafka if not specified
            messageBrokerType = System.getProperty("test.messagebroker", "kafka").toLowerCase();
            setupMessageBroker();
        }
    }
    
    /**
     * Sets up the message broker for testing in microservices environment.
     * Supports both Kafka and RabbitMQ based on the messageBrokerType property.
     */
    private void setupMessageBroker() {
        if ("kafka".equals(messageBrokerType)) {
            setupKafka();
        } else if ("rabbitmq".equals(messageBrokerType)) {
            setupRabbitMQ();
        } else {
            throw new IllegalArgumentException("Unsupported message broker type: " + messageBrokerType);
        }
    }
    
    /**
     * Sets up Kafka producer and consumer for testing.
     */
    private void setupKafka() {
        // Producer configuration
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducer = new KafkaProducer<>(producerProps);
        
        // Consumer configuration
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumer = new KafkaConsumer<>(consumerProps);
        kafkaConsumer.subscribe(java.util.Arrays.asList(MOTION_EVENTS_TOPIC));
    }
    
    /**
     * Sets up RabbitMQ connection and channel for testing.
     */
    private void setupRabbitMQ() {
        try {
            // Create connection factory
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            factory.setPort(5672);
            factory.setUsername("guest");
            factory.setPassword("guest");
            
            // Create connection and channel
            rabbitConnection = factory.newConnection();
            rabbitChannel = rabbitConnection.createChannel();
            
            // Declare queues
            rabbitChannel.queueDeclare(POSITION_TOPIC, true, false, false, null);
            rabbitChannel.queueDeclare(MOTION_EVENTS_TOPIC, true, false, false, null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup RabbitMQ", e);
        }
    }

    private Position position(String time, boolean motion, double distance, Boolean ignition) throws ParseException {
        Position position = new Position();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        position.setTime(dateFormat.parse(time));
        position.set(Position.KEY_MOTION, motion);
        position.set(Position.KEY_TOTAL_DISTANCE, distance);
        position.set(Position.KEY_IGNITION, ignition);
        return position;
    }

    private void verifyState(MotionState motionState, boolean state, long distance) {
        assertEquals(state, motionState.getMotionState());
        assertEquals(distance, motionState.getMotionDistance(), 0.1);
    }

    /**
     * Test motion with position in monolithic environment.
     * This is the original test method maintained for backward compatibility.
     */
    @Test
    public void testMotionWithPosition() throws ParseException {
        TripsConfig tripsConfig = new TripsConfig(500, 300000, 300000, 0, false, false);

        MotionState state = new MotionState();

        MotionProcessor.updateState(state, position("2017-01-01 00:00:00", false, 0, null), false, tripsConfig);
        assertNull(state.getEvent());
        verifyState(state, false, 0);

        MotionProcessor.updateState(state, position("2017-01-01 00:02:00", true, 100, null), true, tripsConfig);
        assertNull(state.getEvent());
        verifyState(state, true, 100);

        MotionProcessor.updateState(state, position("2017-01-01 00:02:00", true, 700, null), true, tripsConfig);
        assertEquals(Event.TYPE_DEVICE_MOVING, state.getEvent().getType());
        verifyState(state, true, 0);

        MotionProcessor.updateState(state, position("2017-01-01 00:03:00", false, 700, null), false, tripsConfig);
        assertNull(state.getEvent());
        verifyState(state, false, 700);

        MotionProcessor.updateState(state, position("2017-01-01 00:10:00", false, 700, null), false, tripsConfig);
        assertEquals(Event.TYPE_DEVICE_STOPPED, state.getEvent().getType());
        verifyState(state, false, 0);
    }

    /**
     * Test motion fluctuation in monolithic environment.
     * This is the original test method maintained for backward compatibility.
     */
    @Test
    public void testMotionFluctuation() throws ParseException {
        TripsConfig tripsConfig = new TripsConfig(500, 300000, 300000, 0, false, false);

        MotionState state = new MotionState();

        MotionProcessor.updateState(state, position("2017-01-01 00:00:00", false, 0, null), false, tripsConfig);
        assertNull(state.getEvent());
        verifyState(state, false, 0);

        MotionProcessor.updateState(state, position("2017-01-01 00:02:00", true, 100, null), true, tripsConfig);
        assertNull(state.getEvent());
        verifyState(state, true, 100);

        MotionProcessor.updateState(state, position("2017-01-01 00:02:00", true, 700, null), true, tripsConfig);
        assertEquals(Event.TYPE_DEVICE_MOVING, state.getEvent().getType());
        verifyState(state, true, 0);

        MotionProcessor.updateState(state, position("2017-01-01 00:03:00", false, 700, null), false, tripsConfig);
        assertNull(state.getEvent());
        verifyState(state, false, 700);

        MotionProcessor.updateState(state, position("2017-01-01 00:04:00", true, 1000, null), true, tripsConfig);
        assertNull(state.getEvent());
        verifyState(state, true, 0);

        MotionProcessor.updateState(state, position("2017-01-01 00:06:00", true, 2000, null), true, tripsConfig);
        assertNull(state.getEvent());
        verifyState(state, true, 0);
    }

    /**
     * Test stop with position ignition in monolithic environment.
     * This is the original test method maintained for backward compatibility.
     */
    @Test
    public void testStopWithPositionIgnition() throws ParseException {
        TripsConfig tripsConfig = new TripsConfig(500, 300000, 300000, 0, true, false);

        MotionState state = new MotionState();
        state.setMotionStreak(true);
        state.setMotionState(true);

        MotionProcessor.updateState(state, position("2017-01-01 00:00:00", false, 100, true), false, tripsConfig);
        assertNull(state.getEvent());
        verifyState(state, false, 100);

        MotionProcessor.updateState(state, position("2017-01-01 00:02:00", false, 100, false), false, tripsConfig);
        assertEquals(Event.TYPE_DEVICE_STOPPED, state.getEvent().getType());
        verifyState(state, false, 0);
    }
    
    /**
     * Test motion with position in microservices environment using message broker.
     * This test only runs when in microservices environment.
     */
    @Test
    @EnabledIf("isMicroservicesEnvironment")
    public void testMotionWithPositionAsync() throws Exception {
        // Create a position that will trigger a motion event
        Position position = position("2017-01-01 00:02:00", true, 700, null);
        
        // Send position to message broker
        sendPositionToMessageBroker(position);
        
        // Wait for and verify the motion event
        String eventMessage = waitForEventMessage(Event.TYPE_DEVICE_MOVING, 10000);
        assertNotNull(eventMessage, "Did not receive motion event within timeout");
        assertTrue(eventMessage.contains(Event.TYPE_DEVICE_MOVING));
    }
    
    /**
     * Test stop event in microservices environment using message broker.
     * This test only runs when in microservices environment.
     */
    @Test
    @EnabledIf("isMicroservicesEnvironment")
    public void testStopEventAsync() throws Exception {
        // Create positions that will trigger a stop event
        Position position1 = position("2017-01-01 00:00:00", false, 700, null);
        Position position2 = position("2017-01-01 00:10:00", false, 700, null);
        
        // Setup motion state with motion streak
        MotionState state = new MotionState();
        state.setMotionStreak(true);
        state.setMotionState(true);
        
        // Send positions to message broker
        sendPositionToMessageBroker(position1);
        
        // Wait a bit before sending the second position
        Thread.sleep(1000);
        
        sendPositionToMessageBroker(position2);
        
        // Wait for and verify the stop event
        String eventMessage = waitForEventMessage(Event.TYPE_DEVICE_STOPPED, 10000);
        assertNotNull(eventMessage, "Did not receive stop event within timeout");
        assertTrue(eventMessage.contains(Event.TYPE_DEVICE_STOPPED));
    }
    
    /**
     * Test motion fluctuation in microservices environment using message broker.
     * This test only runs when in microservices environment.
     */
    @Test
    @EnabledIf("isMicroservicesEnvironment")
    public void testMotionFluctuationAsync() throws Exception {
        // Create a sequence of positions that will trigger motion fluctuation
        Position position1 = position("2017-01-01 00:00:00", false, 0, null);
        Position position2 = position("2017-01-01 00:02:00", true, 100, null);
        Position position3 = position("2017-01-01 00:02:00", true, 700, null);
        Position position4 = position("2017-01-01 00:03:00", false, 700, null);
        Position position5 = position("2017-01-01 00:04:00", true, 1000, null);
        
        // Send positions to message broker in sequence
        sendPositionToMessageBroker(position1);
        Thread.sleep(100);
        
        sendPositionToMessageBroker(position2);
        Thread.sleep(100);
        
        sendPositionToMessageBroker(position3);
        Thread.sleep(100);
        
        sendPositionToMessageBroker(position4);
        Thread.sleep(100);
        
        sendPositionToMessageBroker(position5);
        
        // Wait for and verify the moving event
        String eventMessage = waitForEventMessage(Event.TYPE_DEVICE_MOVING, 10000);
        assertNotNull(eventMessage, "Did not receive moving event within timeout");
        assertTrue(eventMessage.contains(Event.TYPE_DEVICE_MOVING));
    }
    
    /**
     * Test with RabbitMQ specifically.
     * This test only runs when in microservices environment with RabbitMQ.
     */
    @Test
    @EnabledIf("isRabbitMQEnvironment")
    public void testMotionWithRabbitMQ() throws Exception {
        // Create a position that will trigger a motion event
        Position position = position("2017-01-01 00:02:00", true, 700, null);
        
        // Send position to RabbitMQ
        sendPositionToMessageBroker(position);
        
        // Wait for and verify the motion event
        String eventMessage = waitForEventMessage(Event.TYPE_DEVICE_MOVING, 10000);
        assertNotNull(eventMessage, "Did not receive motion event within timeout");
        assertTrue(eventMessage.contains(Event.TYPE_DEVICE_MOVING));
    }
    
    /**
     * Helper method to determine if we're in a RabbitMQ environment.
     */
    private boolean isRabbitMQEnvironment() {
        return isMicroservicesEnvironment() && "rabbitmq".equals(messageBrokerType);
    }
    
    /**
     * Sends a position to the appropriate message broker.
     */
    private void sendPositionToMessageBroker(Position position) throws Exception {
        if ("kafka".equals(messageBrokerType)) {
            // Send to Kafka
            ProducerRecord<String, String> record = new ProducerRecord<>(POSITION_TOPIC, 
                    String.valueOf(position.getDeviceId()), position.toString());
            kafkaProducer.send(record).get(5, TimeUnit.SECONDS);
        } else if ("rabbitmq".equals(messageBrokerType)) {
            // Send to RabbitMQ
            rabbitChannel.basicPublish("", POSITION_TOPIC, 
                    new AMQP.BasicProperties.Builder().contentType("text/plain").build(), 
                    position.toString().getBytes());
        }
    }
    
    /**
     * Waits for an event message containing the specified event type.
     * 
     * @param eventType The event type to wait for
     * @param timeoutMs Maximum time to wait in milliseconds
     * @return The event message if found, null otherwise
     */
    private String waitForEventMessage(String eventType, long timeoutMs) throws Exception {
        if ("kafka".equals(messageBrokerType)) {
            return waitForKafkaEventMessage(eventType, timeoutMs);
        } else if ("rabbitmq".equals(messageBrokerType)) {
            return waitForRabbitMQEventMessage(eventType, timeoutMs);
        }
        return null;
    }
    
    /**
     * Waits for a Kafka event message containing the specified event type.
     */
    private String waitForKafkaEventMessage(String eventType, long timeoutMs) {
        CompletableFuture<String> future = new CompletableFuture<>();
        long endTime = System.currentTimeMillis() + timeoutMs;
        
        try {
            while (System.currentTimeMillis() < endTime) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(java.time.Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value().contains(eventType)) {
                        future.complete(record.value());
                        break;
                    }
                }
                
                if (future.isDone()) {
                    break;
                }
            }
            
            return future.isDone() ? future.get(1, TimeUnit.SECONDS) : null;
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Waits for a RabbitMQ event message containing the specified event type.
     */
    private String waitForRabbitMQEventMessage(String eventType, long timeoutMs) {
        CompletableFuture<String> future = new CompletableFuture<>();
        
        try {
            // Create a temporary queue for receiving messages
            String queueName = rabbitChannel.queueDeclare().getQueue();
            rabbitChannel.queueBind(queueName, MOTION_EVENTS_TOPIC, "");
            
            // Set up a consumer
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                if (message.contains(eventType)) {
                    future.complete(message);
                }
            };
            
            String consumerTag = rabbitChannel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
            
            // Wait for the message
            String result = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            
            // Clean up
            rabbitChannel.basicCancel(consumerTag);
            rabbitChannel.queueDelete(queueName);
            
            return result;
        } catch (Exception e) {
            return null;
        }
    }
}
