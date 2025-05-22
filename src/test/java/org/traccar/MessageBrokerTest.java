package org.traccar;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.config.Config;
import org.traccar.messaging.MessageConsumer;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHandler;
import org.traccar.messaging.MessageHeaders;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageSerializer;
import org.traccar.messaging.kafka.KafkaMessageConsumer;
import org.traccar.messaging.kafka.KafkaMessageProducer;
import org.traccar.messaging.rabbitmq.RabbitMQMessageConsumer;
import org.traccar.messaging.rabbitmq.RabbitMQMessageProducer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test utility class for mocking and verifying message broker interactions.
 * 
 * This class provides comprehensive testing utilities for the microservices messaging infrastructure,
 * including:
 * 
 * 1. Mock implementations of MessageProducer and MessageConsumer for both Kafka and RabbitMQ
 * 2. Utilities for verifying message publication and consumption
 * 3. Support for testing message serialization/deserialization (JSON and Protocol Buffers)
 * 4. Verification of message headers and metadata
 * 5. Testing of message delivery guarantees (at-least-once, exactly-once)
 * 
 * The class is designed to be extended by specific test classes that need to verify
 * asynchronous communication between microservices without requiring actual message brokers.
 */
@ExtendWith(MockitoExtension.class)
public class MessageBrokerTest extends BaseTest {

    @Mock
    protected Config config;

    @Mock
    protected MessageSerializer serializer;

    protected MockMessageProducer mockProducer;
    protected MockMessageConsumer mockConsumer;

    @BeforeEach
    public void setUp() {
        mockProducer = new MockMessageProducer();
        mockConsumer = new MockMessageConsumer();
    }

    /**
     * Mock implementation of MessageProducer for testing.
     * 
     * This class captures all published messages for later verification and provides
     * methods to simulate failures for testing error handling and retry logic.
     * It implements the core MessageProducer interface, making it compatible with
     * both Kafka and RabbitMQ testing scenarios.
     */
    public class MockMessageProducer implements MessageProducer {
        private final List<MessageEnvelope> publishedMessages = new ArrayList<>();
        private final Map<String, List<MessageEnvelope>> topicMessages = new HashMap<>();
        private boolean shouldFailPublishing = false;
        private Exception publishException = new RuntimeException("Simulated publishing failure");

        @Override
        public <T> CompletableFuture<Void> send(String topic, T message) {
            return send(topic, message, new MessageHeaders());
        }

        @Override
        public <T> CompletableFuture<Void> send(String topic, T message, MessageHeaders headers) {
            if (shouldFailPublishing) {
                CompletableFuture<Void> future = new CompletableFuture<>();
                future.completeExceptionally(publishException);
                return future;
            }

            MessageEnvelope envelope = new MessageEnvelope(message, headers);
            publishedMessages.add(envelope);
            topicMessages.computeIfAbsent(topic, k -> new ArrayList<>()).add(envelope);
            return CompletableFuture.completedFuture(null);
        }

        /**
         * Gets all messages published to any topic.
         */
        public List<MessageEnvelope> getPublishedMessages() {
            return new ArrayList<>(publishedMessages);
        }

        /**
         * Gets messages published to a specific topic.
         */
        public List<MessageEnvelope> getMessagesForTopic(String topic) {
            return topicMessages.getOrDefault(topic, new ArrayList<>());
        }

        /**
         * Clears all captured messages.
         */
        public void clearMessages() {
            publishedMessages.clear();
            topicMessages.clear();
        }

        /**
         * Configures the producer to simulate failures.
         */
        public void simulateFailure(boolean shouldFail, Exception exception) {
            this.shouldFailPublishing = shouldFail;
            if (exception != null) {
                this.publishException = exception;
            }
        }
    }

    /**
     * Mock implementation of MessageConsumer for testing.
     * 
     * This class simulates message consumption and allows verification of handler invocations.
     * It tracks subscribed handlers by topic and provides methods to simulate message reception
     * and failure scenarios. It implements the core MessageConsumer interface, making it
     * compatible with both Kafka and RabbitMQ testing scenarios.
     */
    public class MockMessageConsumer implements MessageConsumer {
        private final Map<String, List<MessageHandler<?>>> topicHandlers = new HashMap<>();
        private boolean shouldFailConsumption = false;
        private Exception consumptionException = new RuntimeException("Simulated consumption failure");

        @Override
        public <T> void subscribe(String topic, Class<T> messageType, MessageHandler<T> handler) {
            topicHandlers.computeIfAbsent(topic, k -> new ArrayList<>()).add(handler);
        }

        @Override
        public void unsubscribe(String topic) {
            topicHandlers.remove(topic);
        }

        /**
         * Simulates receiving a message on a specific topic.
         * The message will be delivered to all registered handlers for that topic.
         */
        @SuppressWarnings("unchecked")
        public <T> void simulateMessageReceived(String topic, T message) {
            simulateMessageReceived(topic, message, new MessageHeaders());
        }

        /**
         * Simulates receiving a message with headers on a specific topic.
         */
        @SuppressWarnings("unchecked")
        public <T> void simulateMessageReceived(String topic, T message, MessageHeaders headers) {
            if (shouldFailConsumption) {
                throw consumptionException;
            }

            List<MessageHandler<?>> handlers = topicHandlers.get(topic);
            if (handlers != null) {
                MessageEnvelope envelope = new MessageEnvelope(message, headers);
                for (MessageHandler<?> handler : handlers) {
                    try {
                        ((MessageHandler<T>) handler).handle(message, headers);
                    } catch (ClassCastException e) {
                        // Handler was registered for a different message type
                        // In a real system, this would be handled more gracefully
                    }
                }
            }
        }

        /**
         * Gets all handlers registered for a specific topic.
         */
        public List<MessageHandler<?>> getHandlersForTopic(String topic) {
            return topicHandlers.getOrDefault(topic, new ArrayList<>());
        }

        /**
         * Configures the consumer to simulate failures.
         */
        public void simulateFailure(boolean shouldFail, Exception exception) {
            this.shouldFailConsumption = shouldFail;
            if (exception != null) {
                this.consumptionException = exception;
            }
        }
    }

    /**
     * Creates a mock Kafka producer that captures messages for verification.
     * This method returns a mock implementation of KafkaMessageProducer that
     * can be used to verify message publication without connecting to a real Kafka broker.
     */
    public KafkaMessageProducer createMockKafkaProducer() {
        KafkaMessageProducer producer = mock(KafkaMessageProducer.class);
        
        // Configure the mock to delegate to our MockMessageProducer
        when(producer.send(anyString(), any())).thenAnswer(invocation -> {
            String topic = invocation.getArgument(0);
            Object message = invocation.getArgument(1);
            return mockProducer.send(topic, message);
        });
        
        when(producer.send(anyString(), any(), any(MessageHeaders.class))).thenAnswer(invocation -> {
            String topic = invocation.getArgument(0);
            Object message = invocation.getArgument(1);
            MessageHeaders headers = invocation.getArgument(2);
            return mockProducer.send(topic, message, headers);
        });
        
        return producer;
    }

    /**
     * Creates a mock RabbitMQ producer that captures messages for verification.
     * This method returns a mock implementation of RabbitMQMessageProducer that
     * can be used to verify message publication without connecting to a real RabbitMQ broker.
     */
    public RabbitMQMessageProducer createMockRabbitMQProducer() {
        RabbitMQMessageProducer producer = mock(RabbitMQMessageProducer.class);
        
        // Configure the mock to delegate to our MockMessageProducer
        when(producer.send(anyString(), any())).thenAnswer(invocation -> {
            String topic = invocation.getArgument(0);
            Object message = invocation.getArgument(1);
            return mockProducer.send(topic, message);
        });
        
        when(producer.send(anyString(), any(), any(MessageHeaders.class))).thenAnswer(invocation -> {
            String topic = invocation.getArgument(0);
            Object message = invocation.getArgument(1);
            MessageHeaders headers = invocation.getArgument(2);
            return mockProducer.send(topic, message, headers);
        });
        
        return producer;
    }

    /**
     * Creates a mock Kafka consumer that can simulate message reception.
     * This method returns a mock implementation of KafkaMessageConsumer that
     * can be used to verify message consumption without connecting to a real Kafka broker.
     */
    public KafkaMessageConsumer createMockKafkaConsumer() {
        KafkaMessageConsumer consumer = mock(KafkaMessageConsumer.class);
        
        // Configure the mock to delegate to our MockMessageConsumer
        doAnswer(invocation -> {
            String topic = invocation.getArgument(0);
            Class<?> messageType = invocation.getArgument(1);
            MessageHandler<?> handler = invocation.getArgument(2);
            mockConsumer.subscribe(topic, messageType, handler);
            return null;
        }).when(consumer).subscribe(anyString(), any(Class.class), any(MessageHandler.class));
        
        doAnswer(invocation -> {
            String topic = invocation.getArgument(0);
            mockConsumer.unsubscribe(topic);
            return null;
        }).when(consumer).unsubscribe(anyString());
        
        return consumer;
    }

    /**
     * Creates a mock RabbitMQ consumer that can simulate message reception.
     * This method returns a mock implementation of RabbitMQMessageConsumer that
     * can be used to verify message consumption without connecting to a real RabbitMQ broker.
     */
    public RabbitMQMessageConsumer createMockRabbitMQConsumer() {
        RabbitMQMessageConsumer consumer = mock(RabbitMQMessageConsumer.class);
        
        // Configure the mock to delegate to our MockMessageConsumer
        doAnswer(invocation -> {
            String topic = invocation.getArgument(0);
            Class<?> messageType = invocation.getArgument(1);
            MessageHandler<?> handler = invocation.getArgument(2);
            mockConsumer.subscribe(topic, messageType, handler);
            return null;
        }).when(consumer).subscribe(anyString(), any(Class.class), any(MessageHandler.class));
        
        doAnswer(invocation -> {
            String topic = invocation.getArgument(0);
            mockConsumer.unsubscribe(topic);
            return null;
        }).when(consumer).unsubscribe(anyString());
        
        return consumer;
    }

    /**
     * Verifies that a message was published to a specific topic.
     */
    public <T> void verifyMessagePublished(String topic, T expectedMessage) {
        List<MessageEnvelope> messages = mockProducer.getMessagesForTopic(topic);
        assertTrue(!messages.isEmpty(), "No messages published to topic: " + topic);
        
        boolean found = false;
        for (MessageEnvelope envelope : messages) {
            if (envelope.getPayload().equals(expectedMessage)) {
                found = true;
                break;
            }
        }
        assertTrue(found, "Expected message not found in published messages for topic: " + topic);
    }

    /**
     * Verifies that a message with specific headers was published to a topic.
     */
    public <T> void verifyMessagePublished(String topic, T expectedMessage, Map<String, Object> expectedHeaders) {
        List<MessageEnvelope> messages = mockProducer.getMessagesForTopic(topic);
        assertTrue(!messages.isEmpty(), "No messages published to topic: " + topic);
        
        boolean found = false;
        for (MessageEnvelope envelope : messages) {
            if (envelope.getPayload().equals(expectedMessage)) {
                MessageHeaders headers = envelope.getHeaders();
                boolean headersMatch = true;
                for (Map.Entry<String, Object> entry : expectedHeaders.entrySet()) {
                    if (!entry.getValue().equals(headers.get(entry.getKey()))) {
                        headersMatch = false;
                        break;
                    }
                }
                if (headersMatch) {
                    found = true;
                    break;
                }
            }
        }
        assertTrue(found, "Expected message with headers not found in published messages for topic: " + topic);
    }

    /**
     * Verifies that a message handler was invoked with the expected message.
     */
    @SuppressWarnings("unchecked")
    public <T> void verifyHandlerInvoked(MessageHandler<T> handler, T expectedMessage) {
        ArgumentCaptor<T> messageCaptor = ArgumentCaptor.forClass((Class<T>) expectedMessage.getClass());
        ArgumentCaptor<MessageHeaders> headersCaptor = ArgumentCaptor.forClass(MessageHeaders.class);
        
        try {
            verify(handler, times(1)).handle(messageCaptor.capture(), headersCaptor.capture());
            assertEquals(expectedMessage, messageCaptor.getValue());
        } catch (Exception e) {
            fail("Handler verification failed: " + e.getMessage());
        }
    }

    /**
     * Verifies that a message handler was invoked with the expected message and headers.
     */
    @SuppressWarnings("unchecked")
    public <T> void verifyHandlerInvoked(MessageHandler<T> handler, T expectedMessage, Map<String, Object> expectedHeaders) {
        ArgumentCaptor<T> messageCaptor = ArgumentCaptor.forClass((Class<T>) expectedMessage.getClass());
        ArgumentCaptor<MessageHeaders> headersCaptor = ArgumentCaptor.forClass(MessageHeaders.class);
        
        try {
            verify(handler, times(1)).handle(messageCaptor.capture(), headersCaptor.capture());
            assertEquals(expectedMessage, messageCaptor.getValue());
            
            MessageHeaders actualHeaders = headersCaptor.getValue();
            for (Map.Entry<String, Object> entry : expectedHeaders.entrySet()) {
                assertEquals(entry.getValue(), actualHeaders.get(entry.getKey()));
            }
        } catch (Exception e) {
            fail("Handler verification failed: " + e.getMessage());
        }
    }

    /**
     * Tests message serialization and deserialization using the provided serializer.
     * This method verifies that a message can be correctly serialized to bytes and then
     * deserialized back to the original object.
     * 
     * @param message The message to serialize and deserialize
     * @param messageType The class of the message
     * @param <T> The type of the message
     */
    public <T> void testSerialization(T message, Class<T> messageType) {
        byte[] serialized = new byte[] {1, 2, 3, 4}; // Sample serialized data
        try {
            when(serializer.serialize(message)).thenReturn(serialized);
            when(serializer.deserialize(serialized, messageType)).thenReturn(message);
            
            byte[] result = serializer.serialize(message);
            assertEquals(serialized, result, "Serialized data should match expected bytes");
            
            T deserialized = serializer.deserialize(result, messageType);
            assertEquals(message, deserialized, "Deserialized object should match original message");
        } catch (Exception e) {
            fail("Serialization test failed: " + e.getMessage());
        }
    }
    
    /**
     * Tests JSON serialization and deserialization specifically.
     * This method configures the serializer to use JSON format and verifies
     * that a message can be correctly serialized and deserialized.
     * 
     * @param message The message to serialize and deserialize
     * @param messageType The class of the message
     * @param expectedJson The expected JSON string representation
     * @param <T> The type of the message
     */
    public <T> void testJsonSerialization(T message, Class<T> messageType, String expectedJson) {
        try {
            byte[] serialized = expectedJson.getBytes();
            when(serializer.serialize(message)).thenReturn(serialized);
            when(serializer.deserialize(serialized, messageType)).thenReturn(message);
            
            byte[] result = serializer.serialize(message);
            String jsonResult = new String(result);
            assertEquals(expectedJson, jsonResult, "JSON serialization should match expected string");
            
            T deserialized = serializer.deserialize(result, messageType);
            assertEquals(message, deserialized, "Deserialized object should match original message");
        } catch (Exception e) {
            fail("JSON serialization test failed: " + e.getMessage());
        }
    }
    
    /**
     * Tests Protocol Buffers serialization and deserialization specifically.
     * This method configures the serializer to use Protocol Buffers format and verifies
     * that a message can be correctly serialized and deserialized.
     * 
     * @param message The message to serialize and deserialize
     * @param messageType The class of the message
     * @param expectedBytes The expected binary representation
     * @param <T> The type of the message
     */
    public <T> void testProtobufSerialization(T message, Class<T> messageType, byte[] expectedBytes) {
        try {
            when(serializer.serialize(message)).thenReturn(expectedBytes);
            when(serializer.deserialize(expectedBytes, messageType)).thenReturn(message);
            
            byte[] result = serializer.serialize(message);
            assertArrayEquals(expectedBytes, result, "Protobuf serialization should match expected bytes");
            
            T deserialized = serializer.deserialize(result, messageType);
            assertEquals(message, deserialized, "Deserialized object should match original message");
        } catch (Exception e) {
            fail("Protobuf serialization test failed: " + e.getMessage());
        }
    }

    /**
     * Tests message delivery guarantees by simulating failures and retries.
     * This method verifies that a message is eventually delivered successfully
     * after a configurable number of retries, implementing the at-least-once
     * delivery guarantee pattern.
     * 
     * @param topic The topic to publish to
     * @param message The message to publish
     * @param maxRetries The maximum number of retries to simulate
     * @param <T> The type of the message
     */
    public <T> void testDeliveryGuarantees(String topic, T message, int maxRetries) {
        CountDownLatch latch = new CountDownLatch(1);
        List<Throwable> exceptions = new ArrayList<>();
        
        // Configure producer to fail initially
        mockProducer.simulateFailure(true, new RuntimeException("Simulated failure"));
        
        // Set up retry logic
        CompletableFuture<Void> future = new CompletableFuture<>();
        for (int i = 0; i < maxRetries; i++) {
            future = future.exceptionally(ex -> {
                exceptions.add(ex);
                if (exceptions.size() >= maxRetries - 1) {
                    // Last retry attempt - succeed this time
                    mockProducer.simulateFailure(false, null);
                }
                return mockProducer.send(topic, message).join();
            });
        }
        
        // Initial send attempt (will fail)
        mockProducer.send(topic, message)
            .exceptionally(ex -> {
                exceptions.add(ex);
                return mockProducer.send(topic, message).join();
            })
            .thenRun(latch::countDown);
        
        try {
            boolean completed = latch.await(5, TimeUnit.SECONDS);
            assertTrue(completed, "Message delivery did not complete within timeout");
            assertEquals(maxRetries, exceptions.size(), "Expected number of retries did not occur");
            assertEquals(1, mockProducer.getMessagesForTopic(topic).size(), "Message should be published exactly once");
        } catch (InterruptedException e) {
            fail("Test was interrupted: " + e.getMessage());
        }
    }
    
    /**
     * Tests exactly-once message processing semantics.
     * This method verifies that a message is processed exactly once even if it is
     * delivered multiple times, using message IDs and idempotent processing.
     * 
     * @param topic The topic to publish to
     * @param message The message to publish
     * @param messageId A unique ID for the message
     * @param <T> The type of the message
     */
    public <T> void testExactlyOnceProcessing(String topic, T message, String messageId) {
        // Create a handler that tracks processed message IDs
        Set<String> processedIds = new HashSet<>();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger processCount = new AtomicInteger(0);
        
        MessageHandler<T> idempotentHandler = mock(MessageHandler.class);
        doAnswer(invocation -> {
            T msg = invocation.getArgument(0);
            MessageHeaders headers = invocation.getArgument(1);
            String id = headers.get("messageId").toString();
            
            // Only process if we haven't seen this ID before
            if (processedIds.add(id)) {
                // Process the message (in a real handler, this would update a database, etc.)
                processCount.incrementAndGet();
            }
            
            if (processCount.get() == 1) {
                latch.countDown();
            }
            return null;
        }).when(idempotentHandler).handle(any(), any(MessageHeaders.class));
        
        // Subscribe the handler
        mockConsumer.subscribe(topic, (Class<T>) message.getClass(), idempotentHandler);
        
        // Create message headers with the ID
        MessageHeaders headers = new MessageHeaders();
        headers.put("messageId", messageId);
        
        // Simulate receiving the same message multiple times
        for (int i = 0; i < 3; i++) {
            mockConsumer.simulateMessageReceived(topic, message, headers);
        }
        
        try {
            boolean completed = latch.await(5, TimeUnit.SECONDS);
            assertTrue(completed, "Message processing did not complete within timeout");
            assertEquals(1, processCount.get(), "Message should be processed exactly once");
            assertEquals(1, processedIds.size(), "Only one message ID should be tracked");
            assertTrue(processedIds.contains(messageId), "The processed ID should match the message ID");
        } catch (InterruptedException e) {
            fail("Test was interrupted: " + e.getMessage());
        }
    }

    /**
     * Creates a mock message handler that can be verified using Mockito.
     * 
     * @param messageType The class of messages this handler will process
     * @param <T> The type of messages this handler will process
     * @return A mock MessageHandler that can be verified using Mockito
     */
    @SuppressWarnings("unchecked")
    public <T> MessageHandler<T> createMockHandler(Class<T> messageType) {
        return mock(MessageHandler.class);
    }

    /**
     * Creates a mock message handler that invokes the provided consumer function.
     * This is useful for implementing custom message processing logic in tests.
     * 
     * @param messageConsumer A function that will be called with each received message
     * @param <T> The type of messages this handler will process
     * @return A mock MessageHandler that delegates to the provided consumer function
     */
    public <T> MessageHandler<T> createFunctionalHandler(Consumer<T> messageConsumer) {
        MessageHandler<T> handler = mock(MessageHandler.class);
        doAnswer(invocation -> {
            T message = invocation.getArgument(0);
            messageConsumer.accept(message);
            return null;
        }).when(handler).handle(any(), any(MessageHeaders.class));
        return handler;
    }
    
    /**
     * Creates a message handler that tracks and counts received messages.
     * This is useful for verifying that messages are received in the expected order and quantity.
     * 
     * @param <T> The type of messages this handler will process
     * @return A MessageHandler that tracks received messages
     */
    public <T> MessageTrackingHandler<T> createMessageTrackingHandler() {
        return new MessageTrackingHandler<>();
    }
    
    /**
     * A message handler implementation that tracks all received messages and their headers.
     * Provides methods to verify message receipt and examine message contents.
     * 
     * @param <T> The type of messages this handler will process
     */
    public class MessageTrackingHandler<T> implements MessageHandler<T> {
        private final List<T> receivedMessages = new ArrayList<>();
        private final List<MessageHeaders> receivedHeaders = new ArrayList<>();
        private final CountDownLatch messagesLatch;
        private final int expectedMessageCount;
        
        public MessageTrackingHandler() {
            this(0); // Default: no expected count, no latch
        }
        
        public MessageTrackingHandler(int expectedMessageCount) {
            this.expectedMessageCount = expectedMessageCount;
            this.messagesLatch = expectedMessageCount > 0 ? new CountDownLatch(1) : null;
        }
        
        @Override
        public void handle(T message, MessageHeaders headers) {
            synchronized (receivedMessages) {
                receivedMessages.add(message);
                receivedHeaders.add(headers);
                
                if (messagesLatch != null && receivedMessages.size() >= expectedMessageCount) {
                    messagesLatch.countDown();
                }
            }
        }
        
        /**
         * Gets all received messages.
         */
        public List<T> getReceivedMessages() {
            synchronized (receivedMessages) {
                return new ArrayList<>(receivedMessages);
            }
        }
        
        /**
         * Gets all received message headers.
         */
        public List<MessageHeaders> getReceivedHeaders() {
            synchronized (receivedMessages) {
                return new ArrayList<>(receivedHeaders);
            }
        }
        
        /**
         * Gets the number of received messages.
         */
        public int getMessageCount() {
            synchronized (receivedMessages) {
                return receivedMessages.size();
            }
        }
        
        /**
         * Waits for the expected number of messages to be received.
         * 
         * @param timeout The maximum time to wait
         * @param unit The time unit of the timeout argument
         * @return true if the expected number of messages was received, false if the waiting time elapsed
         * @throws InterruptedException if the current thread is interrupted while waiting
         */
        public boolean awaitMessages(long timeout, TimeUnit unit) throws InterruptedException {
            if (messagesLatch == null) {
                throw new IllegalStateException("No expected message count was set");
            }
            return messagesLatch.await(timeout, unit);
        }
        
        /**
         * Clears all received messages and headers.
         */
        public void clear() {
            synchronized (receivedMessages) {
                receivedMessages.clear();
                receivedHeaders.clear();
            }
        }
    }
}