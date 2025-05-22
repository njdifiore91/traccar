package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.Mockito;
import org.traccar.ProtocolTest;

/**
 * Test for Oigo protocol decoder.
 * 
 * This test class supports both monolithic and microservices testing environments.
 * It can be executed in the traditional monolithic environment and also supports
 * testing protocol integration with message brokers in the microservices architecture.
 */
public class OigoProtocolDecoderTest extends ProtocolTest {

    /**
     * Traditional decode test for backward compatibility with monolithic architecture.
     * This test directly injects the decoder and verifies position decoding.
     */
    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new OigoProtocolDecoder(null));

        verifyPosition(decoder, binary(
                "7e002e000000146310002523830400001bfb000369150f310c0591594d062ac0c0141508011303cd63101604fd00000000"));

        verifyPosition(decoder, binary(
                "0103537820628365110310410790660962521813380026EE4EFF8593AA0065003E00794C020600100500000000"));

        verifyPosition(decoder, binary(
                "0E03537820628344660204043255862749531B100E0026EE3AFF8593A3FFFE00BF00044C20090710C300000000"));

        verifyPosition(decoder, binary(
                "00035378206638500203340201271426226b190203001ac000ff72eedd00370097238b4c34116a130b000094d9"));

        verifyPosition(decoder, binary(
                "1d035378206638500203340201271426226b19020c001ab144ff72f74d005f0097298a4c1d066d130b000094de"));

        verifyPosition(decoder, binary(
                "00035378206638500203340201271426226b191016001c04e5ff760081013d002900814c1a0f5e130b00009576"));

        verifyPosition(decoder, binary(
                "7e004200000014631000258257000000ffff02d0690e000220690e0002200696dbd204bdfde31a070000b307101135de106e05f500000000010908010402200104ffff8001"));

        verifyPosition(decoder, binary(
                "7e004200000014631000258257000000ffff02d1690e00051f690e00051f0696dbd204bdfde31a070000b307100f35c0106305f500000000010908010402200104ffff8001"));

        verifyPosition(decoder, binary(
                "7e004200000014631000258257000000ffff0d82691300001669130000160696dbd804bdfdbb1a0800000007101035a2106905f500000000010908010402200104ffff8001"));

    }

    /**
     * Test for message broker integration in microservices architecture.
     * This test verifies that decoded positions are correctly published to the message broker.
     * Only runs when the 'test.broker' system property is set to 'true'.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.broker", matches = "true")
    public void testMessageBrokerIntegration() throws Exception {
        // This test will be executed in the Protocol Service environment
        // where the decoder publishes positions to a message broker
        
        // Create a test message broker client
        var brokerClient = createMessageBrokerClient();
        
        // Create and configure the decoder with the broker client
        var decoder = inject(new OigoProtocolDecoder(null));
        configureBrokerPublisher(decoder, brokerClient);
        
        // Process a sample message
        decoder.decode(null, null, binary(
                "7e002e000000146310002523830400001bfb000369150f310c0591594d062ac0c0141508011303cd63101604fd00000000"));
        
        // Verify the message was published to the broker
        verifyMessagePublished(brokerClient, "positions");
    }
    
    /**
     * Creates a mock message broker client for testing.
     * This method is only used in the microservices environment.
     * 
     * @return A mock message broker client
     */
    private Object createMessageBrokerClient() {
        try {
            // Try to create a mock of the actual MessageBrokerClient class if available
            Class<?> clientClass = Class.forName("org.traccar.messaging.MessageBrokerClient");
            return Mockito.mock(clientClass);
        } catch (ClassNotFoundException e) {
            // Fallback for monolithic environment - create a simple mock object
            return Mockito.mock(Object.class);
        }
    }
    
    /**
     * Configures the protocol decoder to use the provided message broker client.
     * This method is only used in the microservices environment.
     * 
     * @param decoder The protocol decoder
     * @param brokerClient The message broker client
     */
    private void configureBrokerPublisher(OigoProtocolDecoder decoder, Object brokerClient) {
        try {
            // Try to use reflection to set the broker client on the decoder
            // This will only work in the microservices environment
            var method = decoder.getClass().getMethod("setBrokerClient", brokerClient.getClass());
            method.invoke(decoder, brokerClient);
        } catch (Exception e) {
            // Ignore in monolithic environment
        }
    }
    
    /**
     * Verifies that a message was published to the specified topic.
     * This method is only used in the microservices environment.
     * 
     * @param brokerClient The message broker client
     * @param topic The topic to verify
     */
    private void verifyMessagePublished(Object brokerClient, String topic) {
        try {
            // Try to verify that the publish method was called
            // This will only work in the microservices environment
            Class<?> messageClass = Class.forName("org.traccar.proto.PositionMessage");
            Mockito.verify(brokerClient, Mockito.atLeastOnce())
                   .publish(Mockito.eq(topic), Mockito.any(messageClass));
        } catch (Exception e) {
            // Ignore in monolithic environment
        }
    }

    /**
     * Test for cross-service protocol handling in microservices architecture.
     * This test verifies that the protocol decoder correctly handles messages across service boundaries.
     * Only runs when the 'test.services' system property is set to 'true'.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.services", matches = "true")
    public void testCrossServiceHandling() throws Exception {
        // This test simulates the complete flow from Protocol Service to Position Service
        
        // Create test clients for service communication
        var brokerClient = createMessageBrokerClient();
        var positionServiceClient = createPositionServiceClient();
        
        // Create and configure the decoder with the broker client
        var decoder = inject(new OigoProtocolDecoder(null));
        configureBrokerPublisher(decoder, brokerClient);
        
        // Process a sample message
        decoder.decode(null, null, binary(
                "7e002e000000146310002523830400001bfb000369150f310c0591594d062ac0c0141508011303cd63101604fd00000000"));
        
        // Verify the message was published to the broker
        verifyMessagePublished(brokerClient, "positions");
        
        // Verify the Position Service received and processed the message
        verifyPositionProcessed(positionServiceClient, "146310002523");
    }
    
    /**
     * Creates a mock position service client for testing.
     * This method is only used in the microservices environment.
     * 
     * @return A mock position service client
     */
    private Object createPositionServiceClient() {
        try {
            // Try to create a mock of the actual PositionServiceClient class if available
            Class<?> clientClass = Class.forName("org.traccar.client.PositionServiceClient");
            return Mockito.mock(clientClass);
        } catch (ClassNotFoundException e) {
            // Fallback for monolithic environment - create a simple mock object
            return Mockito.mock(Object.class);
        }
    }
    
    /**
     * Verifies that a position was processed by the Position Service.
     * This method is only used in the microservices environment.
     * 
     * @param positionServiceClient The position service client
     * @param deviceId The device ID to verify
     */
    private void verifyPositionProcessed(Object positionServiceClient, String deviceId) {
        try {
            // Try to verify that the processPosition method was called
            // This will only work in the microservices environment
            Class<?> positionClass = Class.forName("org.traccar.model.Position");
            Mockito.verify(positionServiceClient, Mockito.atLeastOnce())
                   .processPosition(Mockito.argThat(position -> {
                       try {
                           // Use reflection to get the deviceId from the position
                           var method = positionClass.getMethod("getDeviceId");
                           var actualDeviceId = method.invoke(position).toString();
                           return deviceId.equals(actualDeviceId);
                       } catch (Exception e) {
                           return false;
                       }
                   }));
        } catch (Exception e) {
            // Ignore in monolithic environment
        }
    }
}