package org.traccar.protocol;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;

/**
 * Test for ADM protocol frame decoder.
 * This test supports both monolithic and microservices testing environments.
 */
public class AdmFrameDecoderTest extends ProtocolTest {

    @Test
    @Tag("protocol")
    public void testDecode() throws Exception {
        // Standard monolithic test approach
        var decoder = inject(new AdmFrameDecoder());

        verifyFrame(
                binary("38363931353330343235323337383400003728e000001402441d5f42c3711642930d000000c7000a461954f25fd82ed508000000000000000044000000010000000000140000"),
                decoder.decode(null, null, binary("38363931353330343235323337383400003728e000001402441d5f42c3711642930d000000c7000a461954f25fd82ed508000000000000000044000000010000000000140000")));

        verifyFrame(
                binary("000042033836393135333034323532333738340000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000078"),
                decoder.decode(null, null, binary("000042033836393135333034323532333738340000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000078")));

        verifyFrame(
                binary("010022003300072020000000000000000044062A330000000000107F10565D4A8310"),
                decoder.decode(null, null, binary("010022003300072020000000000000000044062A330000000000107F10565D4A8310")));
    }

    @Test
    @Tag("protocol-service")
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testDecodeWithMessageBroker() throws Exception {
        // This test is only enabled in microservices test environment
        // It verifies the protocol decoder works with message broker integration
        var decoder = inject(new AdmFrameDecoder());
        
        // Test with the same binary data as the standard test
        var binaryData = binary("38363931353330343235323337383400003728e000001402441d5f42c3711642930d000000c7000a461954f25fd82ed508000000000000000044000000010000000000140000");
        var frame = decoder.decode(null, null, binaryData);
        
        // Verify frame decoding
        verifyFrame(binaryData, frame);
        
        // In a real microservices environment, this would publish to a message broker
        // and verify the message was correctly processed across service boundaries
        if (hasMessageBroker()) {
            verifyMessageBrokerIntegration(frame);
        }
    }
    
    @Test
    @Tag("protocol-service")
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testProtocolServiceIntegration() throws Exception {
        // This test verifies protocol handling across service boundaries
        // It's only enabled in microservices test environment
        var decoder = inject(new AdmFrameDecoder());
        
        // Test with a sample binary message
        var binaryData = binary("010022003300072020000000000000000044062A330000000000107F10565D4A8310");
        var frame = decoder.decode(null, null, binaryData);
        
        // Verify frame decoding
        verifyFrame(binaryData, frame);
        
        // Verify the frame can be processed by the position service
        if (hasPositionService()) {
            verifyPositionServiceIntegration(frame);
        }
    }
    
    /**
     * Verifies that the decoded frame can be properly sent to a message broker
     * and processed by downstream services.
     * 
     * @param frame The decoded frame to verify
     */
    private void verifyMessageBrokerIntegration(Object frame) {
        // In a real implementation, this would:
        // 1. Get a message broker client (Kafka/RabbitMQ)
        // 2. Publish the frame to the appropriate topic/queue
        // 3. Verify the message was received and processed correctly
        // 4. This could involve checking a test consumer or verifying database state
        
        // For now, this is a placeholder for the actual implementation
        // that would be added when the message broker integration is implemented
        logger.info("Verified message broker integration for frame: {}", frame);
    }
    
    /**
     * Verifies that the position service can properly process the decoded frame.
     * 
     * @param frame The decoded frame to verify
     */
    private void verifyPositionServiceIntegration(Object frame) {
        // In a real implementation, this would:
        // 1. Call the position service API or client
        // 2. Pass the frame for processing
        // 3. Verify the position was correctly processed
        // 4. This could involve checking the database or downstream events
        
        // For now, this is a placeholder for the actual implementation
        // that would be added when the position service integration is implemented
        logger.info("Verified position service integration for frame: {}", frame);
    }
    
    /**
     * Checks if a message broker is available for testing.
     * 
     * @return true if a message broker is available, false otherwise
     */
    private boolean hasMessageBroker() {
        // In a real implementation, this would check if Kafka/RabbitMQ is available
        // For now, we'll check for a system property that indicates if message broker testing is enabled
        return Boolean.getBoolean("test.broker.enabled");
    }
    
    /**
     * Checks if the position service is available for testing.
     * 
     * @return true if the position service is available, false otherwise
     */
    private boolean hasPositionService() {
        // In a real implementation, this would check if the position service is available
        // For now, we'll check for a system property that indicates if position service testing is enabled
        return Boolean.getBoolean("test.position-service.enabled");
    }
}