package org.traccar.protocol;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test for MxtProtocolDecoder
 * 
 * This test class supports both monolithic and microservices testing approaches:
 * - Traditional tests verify direct decoding of binary messages to Position objects
 * - Microservices tests verify integration with message brokers for cross-service communication
 */
public class MxtProtocolDecoderTest extends ProtocolTest {

    /**
     * Test direct decoding of binary messages to Position objects
     * This approach is used in the monolithic architecture
     */
    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new MxtProtocolDecoder(null));

        verifyPosition(decoder, binary(
                "01a631a7627b00087dc41c40850006aab70affecdf23fd32200080000600000000000000000000001b2ff03b1bb9c4c60214f40100050000006c2d0000f427600051051101de0704"));

        verifyPosition(decoder, binary(
                "01a631144c7e0008643ad2f456fb2d49747cfe4cbe0ffd002008800000001021000fd43d3f1403000000ff300000f42760001031102445a81fda04"));

        verifyPosition(decoder, binary(
                "01a631361e7a00082471418b052a2c46b587ffc01ae3fd000008800000000000003345422203000000f000f00000000000ea1e04"));

        verifyPosition(decoder, binary(
                "01a63118787d00086440628d226e2bc26a97feac8a3afd10210010308000000000000018003d2b10240000005e2f0000f427f21031feff0000593804"));

        verifyPosition(decoder, binary(
                "01a631bd777d0008646e319e17292ce86798fed4cd3afd102110211030800000102403001f15003e2b102400000034300000f4271021007b175535a7be04"));

        verifyPosition(decoder, binary(
                "01a631e3f97e00087cf40a98151c2cc46898fee0ce3afd1021001030c0000006102116072e003829bb00000036102100001024000000062b0000f42730004b06a6384b4304"));

        verifyPosition(decoder, binary(
                "01a63118787d00086468457a466a2bc26a97feac8a3afd10212010308000000000001fe1053d291024000000922f0000f4271021007b17553599bb04"));

        verifyPosition(decoder, binary(
                "01a63118787d0008648645ec486a2bc26a97feac8a3afd1021001030c0000000001419eb05372b1024000000982a0000f4271021007b17000010308c04"));

        verifyPosition(decoder, binary(
                "01a631e3f97e00087cfa0af3151c2c126798febace3afd1021801030c0000006102122082f003e29bb00000037102100001024000000ab2f0000f42730004b060000488c04"));

        verifyPosition(decoder, binary(
                "01a631e3f97e00087cfe0a4b161c2c126798febace3afd1021801030800000071021240731003e2abb00000038102100001024000000c12f0000f42730004b06a638633104"));

        verifyPosition(decoder, binary(
                "01a63118787d0008648645ec486a2bc26a97feac8a3afd1021001030c0000000001419eb05372b1024000000982a0000f4271021007b17000010308c04"));

    }
    
    /**
     * Test protocol integration with message broker
     * This approach is used in the microservices architecture where the Protocol Service
     * publishes decoded positions to a message broker for consumption by other services
     */
    @Test
    public void testMessageBrokerIntegration() throws Exception {
        // Create a mock Kafka producer to verify message publishing
        MockProducer<String, String> mockProducer = new MockProducer<>(
                true, new StringSerializer(), new StringSerializer());
        
        // Create and inject the protocol decoder with the mock producer
        MxtProtocolDecoder decoder = new MxtProtocolDecoder(null);
        decoder = inject(decoder);
        
        // Set the mock producer in the decoder (in real implementation, this would be injected)
        // In a real implementation, the message producer would be injected
        // This is a simplified approach for testing purposes
        try {
            // Use reflection to set the message producer if the method exists
            decoder.getClass().getMethod("setMessageProducer", Object.class)
                   .invoke(decoder, mockProducer);
        } catch (Exception e) {
            // Method doesn't exist yet, which is expected during migration
            // In a real test, we would use a proper mock or test double
            System.out.println("Message producer injection not yet implemented");
        }
        
        // Decode a binary message
        Object result = decoder.decode(null, null, binary(
                "01a631a7627b00087dc41c40850006aab70affecdf23fd32200080000600000000000000000000001b2ff03b1bb9c4c60214f40100050000006c2d0000f427600051051101de0704"));
        
        // Verify the position was decoded correctly
        assertNotNull(result);
        Position position = (Position) result;
        
        // Verify the position was published to the message broker
        List<ProducerRecord<String, String>> history = mockProducer.history();
        assertEquals(1, history.size(), "Should have published one message to the broker");
        
        // Verify the message was published to the correct topic
        ProducerRecord<String, String> record = history.get(0);
        assertEquals("positions", record.topic(), "Should publish to the positions topic");
        
        // Verify the device ID is used as the message key for proper partitioning
        assertEquals(String.valueOf(position.getDeviceId()), record.key(), 
                "Device ID should be used as the message key");
        
        // Verify the message value contains the position data
        assertNotNull(record.value(), "Message value should not be null");
    }
    
    /**
     * Test protocol handling across service boundaries by simulating the complete flow
     * from protocol decoding to position processing and event detection
     */
    @Test
    public void testCrossServiceIntegration() throws Exception {
        // Create mock producers for each service communication channel
        MockProducer<String, String> positionProducer = new MockProducer<>(
                true, new StringSerializer(), new StringSerializer());
        MockProducer<String, String> eventProducer = new MockProducer<>(
                true, new StringSerializer(), new StringSerializer());
        
        // Create and inject the protocol decoder with the position producer
        MxtProtocolDecoder decoder = new MxtProtocolDecoder(null);
        decoder = inject(decoder);
        // In a real implementation, the message producer would be injected
        // This is a simplified approach for testing purposes
        try {
            // Use reflection to set the message producer if the method exists
            decoder.getClass().getMethod("setMessageProducer", Object.class)
                   .invoke(decoder, positionProducer);
        } catch (Exception e) {
            // Method doesn't exist yet, which is expected during migration
            // In a real test, we would use a proper mock or test double
            System.out.println("Message producer injection not yet implemented");
        }
        
        // Create a mock position handler that would consume from the position topic
        // and publish to the event topic in a real microservices environment
        MockPositionHandler positionHandler = new MockPositionHandler(eventProducer);
        
        // Decode a binary message
        Object result = decoder.decode(null, null, binary(
                "01a631bd777d0008646e319e17292ce86798fed4cd3afd102110211030800000102403001f15003e2b102400000034300000f4271021007b175535a7be04"));
        
        // Verify the position was decoded correctly
        assertNotNull(result);
        Position position = (Position) result;
        
        // Verify the position was published to the position topic
        List<ProducerRecord<String, String>> positionHistory = positionProducer.history();
        assertEquals(1, positionHistory.size(), "Should have published one message to the position topic");
        
        // Simulate the position service consuming the position and processing it
        positionHandler.process(position);
        
        // Verify an event was published to the event topic
        List<ProducerRecord<String, String>> eventHistory = eventProducer.history();
        assertEquals(1, eventHistory.size(), "Should have published one message to the event topic");
        
        // Verify the event message contains the correct device ID
        ProducerRecord<String, String> eventRecord = eventHistory.get(0);
        assertEquals(String.valueOf(position.getDeviceId()), eventRecord.key(),
                "Event message should have the same device ID as the position");
    }
    
    /**
     * Mock position handler that simulates the Position Service in a microservices architecture
     * This would consume positions from the message broker and publish events
     */
    private static class MockPositionHandler {
        private final MockProducer<String, String> eventProducer;
        
        public MockPositionHandler(MockProducer<String, String> eventProducer) {
            this.eventProducer = eventProducer;
        }
        
        public void process(Position position) {
            // Simulate position processing and event detection
            // In a real implementation, this would analyze the position and generate events
            
            // Publish a simulated event to the event topic
            eventProducer.send(new ProducerRecord<>(
                    "events",
                    String.valueOf(position.getDeviceId()),
                    String.format("{\"type\":\"deviceMoving\",\"deviceId\":%d,\"position\":%d}", 
                            position.getDeviceId(), position.getId()));
        }
    }
}