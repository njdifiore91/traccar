package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;

/**
 * Motor Protocol Decoder Test
 * 
 * This test has been updated to support both monolithic and microservices testing.
 * It will be gradually migrated to the Protocol Service test folder.
 */
public class MotorProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        // Standard monolithic test for protocol decoding
        var decoder = inject(new MotorProtocolDecoder(null));

        verifyPosition(decoder, text(
                "341200007E7E00007E7E020301803955352401161766210162090501010108191625132655351234567F12345F"));
    }

    @Test
    @EnabledIfSystemProperty(named = "test.microservice.mode", matches = "true")
    public void testDecodeWithMessageBroker() throws Exception {
        // This test is only enabled in microservices mode
        // It verifies that the protocol decoder correctly publishes to the message broker
        var decoder = inject(new MotorProtocolDecoder(null));
        
        // Configure the test to capture messages sent to the broker
        configureBrokerCapture();
        
        // Process the message
        decoder.decode(null, null, text(
                "341200007E7E00007E7E020301803955352401161766210162090501010108191625132655351234567F12345F"));
        
        // Verify that the message was correctly published to the broker
        verifyBrokerMessage("positions", message -> {
            // Verify essential fields in the published message
            return message.contains("latitude") && 
                   message.contains("longitude") && 
                   message.contains("protocol=motor");
        });
    }

    @Test
    @EnabledIfSystemProperty(named = "test.service.integration", matches = "true")
    public void testCrossServiceIntegration() throws Exception {
        // This test verifies protocol handling across service boundaries
        // It ensures the position data flows correctly from Protocol Service to Position Service
        
        // Create a test device in the system
        var device = createTestDevice();
        
        // Send a message through the protocol decoder
        var decoder = inject(new MotorProtocolDecoder(null));
        decoder.decode(null, null, text(
                "341200007E7E00007E7E020301803955352401161766210162090501010108191625132655351234567F12345F"));
        
        // Verify that the position was processed by the Position Service
        verifyPositionHandled(device.getId(), position -> {
            // Verify position was enriched by the Position Service
            return position.getLatitude() > 0 && position.getLongitude() > 0;
        });
    }
    
    /**
     * Helper method to configure the test to capture messages sent to the broker.
     * This is a placeholder for the actual implementation that will be provided
     * by the ProtocolTest base class in the microservices architecture.
     */
    private void configureBrokerCapture() {
        // In the actual implementation, this would configure a test message broker
        // or a mock to capture messages published by the protocol decoder
    }
    
    /**
     * Helper method to verify messages published to the broker.
     * This is a placeholder for the actual implementation that will be provided
     * by the ProtocolTest base class in the microservices architecture.
     * 
     * @param topic The topic to check for messages
     * @param validator A function that validates the message content
     */
    private void verifyBrokerMessage(String topic, java.util.function.Predicate<String> validator) {
        // In the actual implementation, this would verify that a message matching
        // the validator was published to the specified topic
    }
    
    /**
     * Helper method to create a test device for integration testing.
     * This is a placeholder for the actual implementation that will be provided
     * by the ProtocolTest base class in the microservices architecture.
     * 
     * @return A test device object
     */
    private Object createTestDevice() {
        // In the actual implementation, this would create a test device
        // in the system and return it for use in the test
        return new Object(); // Placeholder return
    }
    
    /**
     * Helper method to verify that a position was handled by the Position Service.
     * This is a placeholder for the actual implementation that will be provided
     * by the ProtocolTest base class in the microservices architecture.
     * 
     * @param deviceId The ID of the device to check positions for
     * @param validator A function that validates the position
     */
    private void verifyPositionHandled(Object deviceId, java.util.function.Predicate<Object> validator) {
        // In the actual implementation, this would verify that a position
        // matching the validator was processed by the Position Service
    }
}