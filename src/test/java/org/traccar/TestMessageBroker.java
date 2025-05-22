package org.traccar;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A test implementation of a message broker for testing protocol integration with microservices.
 * This class simulates the behavior of a real message broker like Kafka or RabbitMQ for testing purposes.
 */
public class TestMessageBroker {

    private final Map<String, List<Object>> publishedMessages = new HashMap<>();
    private final Map<String, List<String>> subscribedTopics = new HashMap<>();
    private final List<String> acknowledgedMessages = new ArrayList<>();
    
    /**
     * Publishes a message to the specified topic.
     * 
     * @param topic The topic to publish to
     * @param message The message to publish
     */
    public void publish(String topic, Object message) {
        publishedMessages.computeIfAbsent(topic, k -> new ArrayList<>()).add(message);
    }
    
    /**
     * Simulates a downstream service by registering it as a subscriber to relevant topics.
     * 
     * @param serviceName The name of the service to simulate
     */
    public void simulateDownstreamService(String serviceName) {
        // In a real implementation, different services would subscribe to different topics
        // For testing purposes, we'll use a simple naming convention
        if ("position-service".equals(serviceName)) {
            subscribedTopics.computeIfAbsent(serviceName, k -> new ArrayList<>())
                    .add("protocol.position.raw");
        } else if ("event-service".equals(serviceName)) {
            subscribedTopics.computeIfAbsent(serviceName, k -> new ArrayList<>())
                    .add("position.processed");
        } else if ("notification-service".equals(serviceName)) {
            subscribedTopics.computeIfAbsent(serviceName, k -> new ArrayList<>())
                    .add("event.detected");
        }
    }
    
    /**
     * Simulates a service acknowledging receipt of a message.
     * 
     * @param serviceName The name of the service acknowledging the message
     * @param messageId The ID of the message being acknowledged (optional)
     */
    public void acknowledgeMessage(String serviceName, String messageId) {
        acknowledgedMessages.add(serviceName + ":" + (messageId != null ? messageId : "default"));
    }
    
    /**
     * Verifies that a message was published to the specified topic.
     * 
     * @param topic The topic to check
     * @param expectedMessage The expected message
     * @throws AssertionError if the message was not published to the topic
     */
    public void verifyMessagePublished(String topic, Object expectedMessage) {
        List<Object> messages = publishedMessages.get(topic);
        if (messages == null || !messages.contains(expectedMessage)) {
            throw new AssertionError("Expected message was not published to topic: " + topic);
        }
    }
    
    /**
     * Verifies that a message was received by the specified service from the specified topic.
     * 
     * @param serviceName The name of the service
     * @param topic The topic to check
     * @throws AssertionError if the service is not subscribed to the topic or no messages were published
     */
    public void verifyMessageReceived(String serviceName, String topic) {
        List<String> topics = subscribedTopics.get(serviceName);
        if (topics == null || !topics.contains(topic)) {
            throw new AssertionError("Service " + serviceName + " is not subscribed to topic: " + topic);
        }
        
        List<Object> messages = publishedMessages.get(topic);
        if (messages == null || messages.isEmpty()) {
            throw new AssertionError("No messages were published to topic: " + topic);
        }
    }
    
    /**
     * Verifies that a service acknowledged receipt of a message.
     * 
     * @param serviceName The name of the service
     * @throws AssertionError if the service did not acknowledge any messages
     */
    public void verifyServiceAcknowledgement(String serviceName) {
        boolean found = false;
        for (String ack : acknowledgedMessages) {
            if (ack.startsWith(serviceName + ":")) {
                found = true;
                break;
            }
        }
        
        if (!found) {
            throw new AssertionError("Service " + serviceName + " did not acknowledge any messages");
        }
    }
    
    /**
     * Clears all recorded messages and subscriptions.
     * Useful for resetting the state between tests.
     */
    public void reset() {
        publishedMessages.clear();
        subscribedTopics.clear();
        acknowledgedMessages.clear();
    }
}