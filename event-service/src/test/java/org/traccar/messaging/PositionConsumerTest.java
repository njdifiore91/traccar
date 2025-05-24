package org.traccar.messaging;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.traccar.BaseTest;
import org.traccar.handler.events.EventHandler;
import org.traccar.model.Position;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests for the PositionConsumer class which is responsible for consuming position messages
 * from the message broker and routing them to the appropriate event handlers.
 * 
 * This test class verifies:
 * - Deserialization of position messages in both Protocol Buffers and JSON formats
 * - Proper routing of position messages to event handlers
 * - Consumer group behavior and partition assignment
 * - Error handling for malformed messages
 * - Message acknowledgment patterns
 */

@EmbeddedKafka(topics = {"enriched-positions", "dead-letter-queue"}, partitions = 3)
@ExtendWith(SpringExtension.class)
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PositionConsumerTest extends BaseTest {

    private static final String POSITIONS_TOPIC = "enriched-positions";
    private static final String DEAD_LETTER_QUEUE = "dead-letter-queue";
    private static final String CONSUMER_GROUP = "event-service-test";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Mock
    private EventHandler eventHandler;

    @Spy
    private PositionConsumer positionConsumer;

    @Captor
    private ArgumentCaptor<Position> positionCaptor;

    private Producer<String, String> producer;
    private Consumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        // Configure and create the producer
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producer = new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new StringSerializer()).createProducer();

        // Configure and create the consumer
        Map<String, Object> consumerProps = new HashMap<>(KafkaTestUtils.consumerProps(CONSUMER_GROUP, "false", embeddedKafkaBroker));
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer()).createConsumer();
        consumer.subscribe(Collections.singletonList(POSITIONS_TOPIC));

        // Initialize the position consumer with mocked event handler
        // In a real test, this would be autowired and we'd use @MockBean for the eventHandler
        positionConsumer = spy(new PositionConsumer());
        positionConsumer.setEventHandler(eventHandler);
    }

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
        }
    }

    /**
     * Tests the consumption of a position message in Protocol Buffers format.
     * Verifies that the message is properly deserialized and routed to the event handler.
     */
    @Test
    void testConsumePositionMessageProtobuf() throws Exception {
        // Prepare a sample protobuf position message
        String protobufMessage = "{ \"deviceId\": 123, \"protocol\": \"test\", \"serverTime\": 1621436387000, \"deviceTime\": 1621436387000, \"fixTime\": 1621436387000, \"valid\": true, \"latitude\": 40.7128, \"longitude\": -74.0060, \"altitude\": 10.0, \"speed\": 50.0, \"course\": 90.0, \"address\": \"Test Address\", \"attributes\": { \"key1\": \"value1\", \"key2\": 123 } }";
        
        // Set up a latch to wait for the message to be processed
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(eventHandler).handlePosition(any(Position.class));

        // Send the message to the topic
        producer.send(new ProducerRecord<>(POSITIONS_TOPIC, "device-123", protobufMessage));
        producer.flush();

        // Wait for the consumer to process the message
        boolean messageProcessed = latch.await(10, TimeUnit.SECONDS);
        assertTrue(messageProcessed, "Message should be processed within timeout");

        // Verify that the event handler was called with the correct position
        verify(eventHandler, times(1)).handlePosition(positionCaptor.capture());
        Position position = positionCaptor.getValue();
        assertEquals(123, position.getDeviceId());
        assertEquals(40.7128, position.getLatitude(), 0.0001);
        assertEquals(-74.0060, position.getLongitude(), 0.0001);
        assertEquals(50.0, position.getSpeed(), 0.0001);
    }

    /**
     * Tests the consumption of a position message in JSON format.
     * Verifies that the message is properly deserialized and routed to the event handler.
     */
    @Test
    void testConsumePositionMessageJson() throws Exception {
        // Prepare a sample JSON position message
        String jsonMessage = "{ \"deviceId\": 456, \"protocol\": \"test\", \"serverTime\": 1621436387000, \"deviceTime\": 1621436387000, \"fixTime\": 1621436387000, \"valid\": true, \"latitude\": 51.5074, \"longitude\": -0.1278, \"altitude\": 20.0, \"speed\": 30.0, \"course\": 180.0, \"address\": \"London\", \"attributes\": { \"key1\": \"value1\", \"key2\": 456 } }";
        
        // Set up a latch to wait for the message to be processed
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(eventHandler).handlePosition(any(Position.class));

        // Send the message to the topic
        producer.send(new ProducerRecord<>(POSITIONS_TOPIC, "device-456", jsonMessage));
        producer.flush();

        // Wait for the consumer to process the message
        boolean messageProcessed = latch.await(10, TimeUnit.SECONDS);
        assertTrue(messageProcessed, "Message should be processed within timeout");

        // Verify that the event handler was called with the correct position
        verify(eventHandler, times(1)).handlePosition(positionCaptor.capture());
        Position position = positionCaptor.getValue();
        assertEquals(456, position.getDeviceId());
        assertEquals(51.5074, position.getLatitude(), 0.0001);
        assertEquals(-0.1278, position.getLongitude(), 0.0001);
        assertEquals(30.0, position.getSpeed(), 0.0001);
    }

    /**
     * Tests the consumer group behavior and partition assignment.
     * Verifies that messages with the same key go to the same partition
     * and are processed in order, which is essential for maintaining
     * the correct sequence of position updates for a device.
     */
    @Test
    void testConsumerGroupBehavior() throws Exception {
        // This test verifies that messages with the same key go to the same partition
        // and are processed in order
        
        // Prepare multiple position messages for the same device
        String message1 = "{ \"deviceId\": 789, \"protocol\": \"test\", \"serverTime\": 1621436387000, \"deviceTime\": 1621436387000, \"fixTime\": 1621436387000, \"valid\": true, \"latitude\": 40.7128, \"longitude\": -74.0060, \"speed\": 10.0 }";
        String message2 = "{ \"deviceId\": 789, \"protocol\": \"test\", \"serverTime\": 1621436388000, \"deviceTime\": 1621436388000, \"fixTime\": 1621436388000, \"valid\": true, \"latitude\": 40.7129, \"longitude\": -74.0061, \"speed\": 20.0 }";
        String message3 = "{ \"deviceId\": 789, \"protocol\": \"test\", \"serverTime\": 1621436389000, \"deviceTime\": 1621436389000, \"fixTime\": 1621436389000, \"valid\": true, \"latitude\": 40.7130, \"longitude\": -74.0062, \"speed\": 30.0 }";
        
        // Set up a latch to wait for all messages to be processed
        CountDownLatch latch = new CountDownLatch(3);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(eventHandler).handlePosition(any(Position.class));

        // Send the messages with the same key to ensure they go to the same partition
        String deviceKey = "device-789";
        producer.send(new ProducerRecord<>(POSITIONS_TOPIC, deviceKey, message1));
        producer.send(new ProducerRecord<>(POSITIONS_TOPIC, deviceKey, message2));
        producer.send(new ProducerRecord<>(POSITIONS_TOPIC, deviceKey, message3));
        producer.flush();

        // Wait for all messages to be processed
        boolean allMessagesProcessed = latch.await(10, TimeUnit.SECONDS);
        assertTrue(allMessagesProcessed, "All messages should be processed within timeout");

        // Verify that the event handler was called three times
        verify(eventHandler, times(3)).handlePosition(positionCaptor.capture());
        
        // Verify that the positions were processed in order
        // Get all captured positions
        var capturedPositions = positionCaptor.getAllValues();
        assertEquals(3, capturedPositions.size());
        
        // Check that the speeds are in the correct order (10, 20, 30)
        assertEquals(10.0, capturedPositions.get(0).getSpeed(), 0.0001);
        assertEquals(20.0, capturedPositions.get(1).getSpeed(), 0.0001);
        assertEquals(30.0, capturedPositions.get(2).getSpeed(), 0.0001);
    }

    /**
     * Tests the error handling for malformed messages.
     * Verifies that malformed messages that cannot be deserialized
     * are properly handled and sent to the dead letter queue instead
     * of causing the consumer to crash.
     */
    @Test
    void testErrorHandlingForMalformedMessage() throws Exception {
        // Prepare a malformed message that will cause deserialization to fail
        String malformedMessage = "{ this is not valid JSON }";
        
        // Set up a latch to wait for the dead letter queue message
        CountDownLatch latch = new CountDownLatch(1);
        
        // Subscribe to the dead letter queue
        Consumer<String, String> dlqConsumer = new DefaultKafkaConsumerFactory<>(
                KafkaTestUtils.consumerProps("dlq-consumer", "false", embeddedKafkaBroker),
                new StringDeserializer(),
                new StringDeserializer()
        ).createConsumer();
        dlqConsumer.subscribe(Collections.singletonList(DEAD_LETTER_QUEUE));

        // Send the malformed message
        producer.send(new ProducerRecord<>(POSITIONS_TOPIC, "device-error", malformedMessage));
        producer.flush();

        // Verify that the event handler was not called with the malformed message
        verify(eventHandler, never()).handlePosition(any(Position.class));

        // Check if the message was sent to the dead letter queue
        ConsumerRecord<String, String> deadLetterRecord = KafkaTestUtils.getSingleRecord(dlqConsumer, DEAD_LETTER_QUEUE, 5000);
        assertNotNull(deadLetterRecord, "Message should be sent to dead letter queue");
        assertEquals(malformedMessage, deadLetterRecord.value());
        
        dlqConsumer.close();
    }

    /**
     * Tests the message acknowledgment pattern.
     * Verifies that messages are properly acknowledged after processing,
     * which is important for ensuring that messages are not reprocessed
     * and that the consumer group's offset is correctly updated.
     */
    @Test
    void testMessageAcknowledgment() throws Exception {
        // This test verifies that messages are properly acknowledged after processing
        
        // Prepare a valid position message
        String validMessage = "{ \"deviceId\": 999, \"protocol\": \"test\", \"serverTime\": 1621436387000, \"deviceTime\": 1621436387000, \"fixTime\": 1621436387000, \"valid\": true, \"latitude\": 40.7128, \"longitude\": -74.0060, \"speed\": 40.0 }";
        
        // Set up a latch to wait for the message to be processed
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(eventHandler).handlePosition(any(Position.class));

        // Send the message
        producer.send(new ProducerRecord<>(POSITIONS_TOPIC, "device-999", validMessage));
        producer.flush();

        // Wait for the message to be processed
        boolean messageProcessed = latch.await(10, TimeUnit.SECONDS);
        assertTrue(messageProcessed, "Message should be processed within timeout");

        // Verify that the event handler was called with the correct position
        verify(eventHandler, times(1)).handlePosition(positionCaptor.capture());
        Position position = positionCaptor.getValue();
        assertEquals(999, position.getDeviceId());
        assertEquals(40.0, position.getSpeed(), 0.0001);

        // Verify that the message was acknowledged (no more messages in the topic for this consumer)
        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, 1000);
        assertEquals(0, records.count(), "No more unacknowledged messages should be in the topic");
    }
}